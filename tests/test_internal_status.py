"""internal 상태 조회원 — bjobs 대신 앱 콜백으로 상태를 얻는 경로.

job_status_fetcher를 주면 LsfCommand가 subprocess 대신 InternalStatusSource를
쓴다. 검증 축은 넷:
  1) 파싱   — REST payload → JobStatus (dataId/stat/시각/cluster)
  2) 계약   — 조회 장애는 '판단 보류'이지 LOST가 아니다 (bjobs 경로와 동일)
  3) 동시성 — 폴링/killer verify/detect_lost가 겹쳐도 콜백은 1회 (single-flight)
  4) 누적/만료 — 증분 병합으로 쌓이되 종료 후 보존 기간이 지나면 버린다
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand
from lsfmgr.internal_status import (
    InternalStatusSource, parse_internal_jobs, parse_time,
)
from tests.conftest import submit_cmds


def _job(job_id, stat="RUN", **kw):
    """예시 payload 1행 — 실제 REST 응답 필드 그대로."""
    out = {
        "queue": "normal", "updateTime": "2026-08-19T00:12:12", "stat": stat,
        "app": "default", "submitTime": "2026-08-08T10:10:00",
        "subcwd": "/user/jekai", "startTime": "2026-08-08T12:00:01",
        "finishTime": None, "dataId": f"{job_id}.cluster1",
        "cluster": "cluster1", "userName": "jekai",
    }
    out.update(kw)
    return out


def _payload(*jobs):
    return {"jobs": list(jobs), "count": len(jobs), "updateFrom": None}


def _source(fetcher, **kw):
    kw.setdefault("refresh_min_s", 0.0)
    kw.setdefault("wait_timeout_s", 5.0)
    return InternalStatusSource(fetcher, **kw)


# ----------------------------------------------------------------------
# 1) 파싱
# ----------------------------------------------------------------------
def test_parses_example_payload():
    """질의 예시 payload 그대로 — dataId/stat/시각/cluster가 다 살아난다."""
    (st,) = parse_internal_jobs(_payload(_job(1432342)))
    assert st.job_id == 1432342 and st.array_index is None
    assert st.state is JobState.RUN
    assert st.start_time == datetime(2026, 8, 8, 12, 0, 1)
    assert st.finish_time is None            # RUN 중 종료시각은 노출 안 함
    assert st.source_cluster == "cluster1"


def test_array_element_and_bare_data_id():
    """dataId는 "id[idx].cluster" / "id" 양쪽 표기를 받는다."""
    a, b = parse_internal_jobs(_payload(
        _job(1, dataId="500[3].cl2", cluster=None),   # cluster 필드 없는 사이트
        _job(2, dataId="777")))
    # cluster 필드가 없으면 dataId 접미사에서 뽑는다
    assert (a.job_id, a.array_index, a.source_cluster) == (500, 3, "cl2")
    # 있으면 명시 필드가 이긴다 (dataId에 접미사가 없어도 된다)
    assert (b.job_id, b.array_index, b.source_cluster) == (777, None, "cluster1")


def test_finish_time_only_on_terminal_and_run_time_derived():
    """run_time은 payload에 없다 — 시각 두 개로 유도하고, finish_time은
    종료 상태에서만 노출한다(bjobs 파서와 같은 규칙)."""
    done, run = parse_internal_jobs(
        _payload(_job(1, stat="DONE", startTime="2026-08-08T12:00:00",
                      finishTime="2026-08-08T12:01:40"),
                 _job(2, stat="RUN", startTime="2026-08-08T12:00:00",
                      finishTime="2026-08-08T13:00:00")),
        now=datetime(2026, 8, 8, 12, 0, 30))
    assert done.finish_time == datetime(2026, 8, 8, 12, 1, 40)
    assert done.run_time_s == 100
    assert run.finish_time is None           # 예상치일 수 있어 버린다
    assert run.run_time_s == 30              # 스냅샷 시각까지의 경과


def test_run_time_is_stable_between_fetches():
    """run_time을 읽을 때마다 now()로 계산하면 조회 없이도 값이 흔들려
    monitor가 전 job을 매 사이클 재전이시킨다 — 받은 시각으로 고정한다."""
    src = _source(lambda: _payload(_job(1, stat="RUN")))
    (first,), _ = src.statuses_by_ids([1])
    time.sleep(0.05)
    (again,), _ = src.statuses_by_ids([1])   # refresh_min_s=0이라 콜백은 다시 돌지만
    assert first.run_time_s is not None
    # 같은 초 안이면 값이 같아야 한다(읽기마다 증가하지 않는다)
    assert abs(again.run_time_s - first.run_time_s) <= 1


@pytest.mark.parametrize("text,expected", [
    ("2026-08-19T00:12:12", datetime(2026, 8, 19, 0, 12, 12)),
    ("2026-08-19 00:12:12", datetime(2026, 8, 19, 0, 12, 12)),
    # 실환경 표기 흔들림 — 날짜 구분자가 ':'로 오는 사례
    ("2026:08:08T12:00:01", datetime(2026, 8, 8, 12, 0, 1)),
    ("2026-08-19T00:12:12.345", datetime(2026, 8, 19, 0, 12, 12)),
    (None, None), ("", None), ("null", None), ("깨진값", None),
])
def test_parse_time_variants(text, expected):
    assert parse_time(text) == expected


def test_unknown_stat_becomes_unkwn_not_dropped():
    """모르는 상태 문자열에 행을 통째로 버리면 그 job이 미발견 → LOST가 된다."""
    (st,) = parse_internal_jobs(_payload(_job(1, stat="WEIRD")))
    assert st.state is JobState.UNKWN


# ----------------------------------------------------------------------
# 2) 계약 — 조회 장애 ≠ job 없음
# ----------------------------------------------------------------------
def test_fetch_failure_defers_all_ids():
    """콜백 예외는 '판단 보류'로 나간다 — monitor가 LOST로 확정하면 안 된다."""
    def boom():
        raise RuntimeError("REST 500")

    found, failed = _source(boom).statuses_by_ids([1, 2, 3])
    assert found == [] and failed == {1, 2, 3}


def test_malformed_payload_is_a_failure_not_an_empty_result():
    """'jobs' 키가 없는 응답을 0건으로 접으면 전 job이 LOST로 몰린다."""
    found, failed = _source(lambda: {"result": "ok"}).statuses_by_ids([7])
    assert found == [] and failed == {7}


def test_empty_jobs_list_is_a_valid_empty_answer():
    """빈 목록은 정상 응답 — 보류가 아니라 '없음'이다(LOST 판정으로 넘어간다)."""
    found, failed = _source(lambda: _payload()).statuses_by_ids([7])
    assert found == [] and failed == set()


def test_no_ids_does_not_call_fetcher():
    calls = []
    src = _source(lambda: calls.append(1) or _payload())
    assert src.statuses_by_ids([]) == ([], set())
    assert not calls


# ----------------------------------------------------------------------
# 3) 동시성 — single-flight / TTL / fresh
# ----------------------------------------------------------------------
def test_concurrent_queries_share_one_fetch():
    """폴링·killer verify·detect_lost가 겹쳐도 콜백은 한 번만 돈다."""
    calls = []
    gate = threading.Event()

    def slow():
        calls.append(1)
        gate.wait(5.0)                       # 리더가 도는 동안 나머지가 몰려든다
        return _payload(_job(1))

    src = _source(slow)
    results = []
    threads = [threading.Thread(
        target=lambda: results.append(src.statuses_by_ids([1])))
        for _ in range(8)]
    for t in threads:
        t.start()
    time.sleep(0.2)                          # 전원이 대기열에 들어갈 시간
    gate.set()
    for t in threads:
        t.join(10)
    assert len(calls) == 1, f"콜백이 {len(calls)}회 실행됨 — single-flight 깨짐"
    assert all(len(found) == 1 and not failed for found, failed in results)
    assert len(results) == 8


def test_refresh_min_s_reuses_snapshot():
    """TTL 안의 조회는 콜백을 다시 돌리지 않는다 — 폴링 1사이클 = 콜백 1회."""
    calls = []
    src = _source(lambda: calls.append(1) or _payload(_job(1), _job(2)),
                  refresh_min_s=30.0)
    for _ in range(5):
        found, _f = src.statuses_by_ids([1, 2])
        assert len(found) == 2
    assert len(calls) == 1


def test_fresh_bypasses_the_cache():
    """kill verify는 '방금'을 봐야 한다 — 캐시된 원장으로 답하면 안 된다."""
    calls = []
    src = _source(lambda: calls.append(1) or _payload(_job(1)),
                  refresh_min_s=30.0)
    src.statuses_by_ids([1])
    src.statuses_by_ids([1])                     # 캐시
    assert len(calls) == 1
    src.statuses_by_ids([1], fresh=True)         # 강제 갱신
    assert len(calls) == 2


def test_failed_fetch_does_not_stampede_waiters():
    """콜백이 죽어 있을 때 대기자마다 재시도하면 죽은 서버를 연타한다."""
    calls = []
    gate = threading.Event()

    def failing():
        calls.append(1)
        gate.wait(5.0)
        raise RuntimeError("down")

    src = _source(failing)
    out = []
    threads = [threading.Thread(
        target=lambda: out.append(src.statuses_by_ids([1])))
        for _ in range(6)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    gate.set()
    for t in threads:
        t.join(10)
    assert len(calls) == 1
    assert all(failed == {1} for _found, failed in out)   # 전원 보류


def test_invalidate_forces_next_fetch():
    calls = []
    src = _source(lambda: calls.append(1) or _payload(_job(1)),
                  refresh_min_s=30.0)
    src.statuses_by_ids([1])
    src.invalidate()
    src.statuses_by_ids([1])
    assert len(calls) == 2


# ----------------------------------------------------------------------
# 4) 누적 병합 / 만료
# ----------------------------------------------------------------------
def test_incremental_payload_merges_instead_of_replacing():
    """updatefrom 증분 조회에서 '이번에 안 온 job'은 사라진 게 아니라
    안 바뀐 것이다 — 통째 교체하면 멀쩡한 job이 미발견 → LOST가 된다."""
    payloads = [_payload(_job(1, stat="RUN"), _job(2, stat="RUN")),
                _payload(_job(2, stat="DONE"))]     # 2번만 갱신됨
    src = _source(lambda: payloads.pop(0))
    src.statuses_by_ids([1, 2])
    found, failed = src.statuses_by_ids([1, 2])
    assert not failed
    states = {st.job_id: st.state for st in found}
    assert states == {1: JobState.RUN, 2: JobState.DONE}


def test_expired_terminal_jobs_are_dropped():
    """종료 후 보존 기간이 지난 job은 원장에서 버린다(무한 누적 방지)."""
    old = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%S")
    recent = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S")
    src = _source(lambda: _payload(
        _job(1, stat="DONE", finishTime=old),        # 20일 전 종료 → 만료
        _job(2, stat="DONE", finishTime=recent),     # 3일 전 종료 → 유지
        _job(3, stat="RUN", finishTime=None)),       # 진행 중 → 유지
        retention_days=14.0)
    found, failed = src.statuses_by_ids([1, 2, 3])
    assert not failed
    assert {st.job_id for st in found} == {2, 3}
    assert src.stats()["entries"] == 2


def test_long_running_job_is_never_expired():
    """2주 넘게 도는 job을 나이로 버리면 추적이 끊긴다 — 종료분만 버린다."""
    old = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%S")
    src = _source(lambda: _payload(
        _job(1, stat="RUN", startTime=old, finishTime=None)),
        retention_days=14.0)
    found, _f = src.statuses_by_ids([1])
    assert [st.job_id for st in found] == [1]


def test_terminal_without_finish_time_expires_by_seen_at():
    """finishTime을 안 주는 payload에서도 종료 job이 영원히 쌓이면 안 된다."""
    src = _source(lambda: _payload(_job(1, stat="EXIT", finishTime=None)),
                  retention_days=14.0)
    src.statuses_by_ids([1])
    assert src.stats()["entries"] == 1
    # 받은 지 20일 지난 상태를 만든다 (청소 최소 간격도 함께 푼다 —
    # 매 폴링 전수 스캔을 피하려고 60초 게이트가 걸려 있다)
    with src._cv:
        entry = src._ledger[1][None]
        src._ledger[1][None] = type(entry)(
            status=entry.status, seen_at=datetime.now() - timedelta(days=20))
        src._last_prune_at = float("-inf")
    src.invalidate()
    src.statuses_by_ids([2])                 # 아무 조회나 — 갱신 시 청소된다
    assert src.stats()["entries"] == 1       # 방금 다시 받은 1건만 남음


def test_retention_zero_disables_expiry():
    old = (datetime.now() - timedelta(days=900)).strftime("%Y-%m-%dT%H:%M:%S")
    src = _source(lambda: _payload(_job(1, stat="DONE", finishTime=old)),
                  retention_days=0.0)
    found, _f = src.statuses_by_ids([1])
    assert [st.job_id for st in found] == [1]


# ----------------------------------------------------------------------
# 5) 배선 — LsfCommand / LsfJobManager
# ----------------------------------------------------------------------
def test_fetcher_presence_alone_selects_the_callback_source():
    """모드 전환의 스위치는 job_status_fetcher 하나뿐이다."""
    assert LsfCommand(LsfConfig()).internal_status is None
    cmd = LsfCommand(LsfConfig(job_status_fetcher=lambda: _payload()))
    assert cmd.internal_status is not None


def test_explicit_bjobs_path_is_warned_as_ignored(caplog):
    """콜백을 주면 bjobs_path는 아무 데도 안 쓰인다 — mock bjobs를 가리켜
    놓고 '왜 안 불리지' 하는 것을 막는다."""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        cmd = LsfCommand(LsfConfig(bjobs_path="/opt/mock/bjobs",
                                   job_status_fetcher=lambda: _payload()))
    assert cmd.internal_status is not None
    warns = [r for r in caplog.records if "무시됩니다" in r.message]
    assert len(warns) == 1, f"경고 {len(warns)}회 (생성 시 1회여야 함)"


def test_poll_runtime_updates_reaches_the_source():
    """monitor가 run_time 변화를 버리는 설정(기본)이면 원장도 그 값을
    안 만들어야 한다 — 두 곳이 어긋나면 조회마다 전수 스캔이 그냥 낭비다."""
    def cmd(runtime):
        return LsfCommand(LsfConfig(poll_runtime_updates=runtime,
                                    job_status_fetcher=lambda: _payload()))
    assert cmd(False).internal_status._track_runtime is False
    assert cmd(True).internal_status._track_runtime is True


def test_removed_jobs_are_dropped_from_the_ledger(qtbot, fake_lsf):
    """삭제된 job이 원장에 남으면 jobset을 만들고 지우기를 반복하는 장수
    세션에서 계속 커진다 — 만료는 종료(DONE/EXIT) 항목만 걷어낸다."""
    ids = []

    def fetch():
        return _payload(*[_job(i, stat="RUN") for i in ids])

    mgr = _internal_mgr(fake_lsf, fetch)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp", "mytool b.sp"],
                             auto_poll=False)
        ids.extend(r.job_id for r in js.jobs())
        mgr.query_once(js.id)
        src = mgr.command.internal_status
        qtbot.waitUntil(lambda: src.stats()["entries"] == 2, timeout=10000)

        mgr.kill(js)                          # 지우려면 먼저 비활성으로
        qtbot.waitUntil(lambda: js.is_inactive, timeout=10000)
        removed = mgr.remove_jobs(js, [js.jobs()[0].job_key])
        assert len(removed) == 1
        assert src.stats()["entries"] == 1, src.stats()

        mgr.remove_jobset(js.id)
        st = src.stats()
        assert (st["job_ids"], st["entries"], st["tracked_ids"],
                st["inflight"]) == (0, 0, 0, 0), st
    finally:
        mgr.shutdown()


def test_default_bjobs_path_is_not_warned(caplog):
    """안 건드린 기본값까지 경고하면 정상 사용에 잡음만 남는다."""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        LsfCommand(LsfConfig(job_status_fetcher=lambda: _payload()))
    assert not [r for r in caplog.records if "무시됩니다" in r.message]


def test_ignore_warning_fires_once_per_manager(qtbot, fake_lsf, caplog):
    """경고는 LsfJobManager 생성 시 1회 — 조회마다 반복되면 안 된다."""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                            bjobs_path="/opt/mock/bjobs")
        try:
            with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
                js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
            mgr.start_polling(js, 0.1)
            qtbot.wait(500)                  # 여러 조회 사이클
        finally:
            mgr.shutdown()
    warns = [r for r in caplog.records if "무시됩니다" in r.message]
    assert len(warns) == 1, f"경고 {len(warns)}회 — 생성 시 1회여야 함"


def test_default_refresh_interval_is_half_the_poll_interval():
    """갱신 간격의 **소유자는 조회원 하나**다 — 설정에서 유도한 값과
    실행 중 자동 추종한 값이 따로 있으면 어느 쪽이 진짜인지 알 수 없다.
    자동/고정 판정도 소스가 스스로 한다(예전엔 호출자가 같은 필드에서
    값과 플래그를 각각 유도해 넘겨 어긋날 수 있었다)."""
    def refresh_of(**cfg):
        cmd = LsfCommand(LsfConfig(job_status_fetcher=lambda: _payload(),
                                   **cfg))
        return cmd.internal_status.stats()["refresh_min_s"]

    assert refresh_of(poll_interval_s=10.0) == 5.0        # 자동 = 절반
    assert refresh_of(internal_refresh_min_s=0.0) == 0.0  # 명시 = 그대로
    assert refresh_of(internal_refresh_min_s=7.0,
                      poll_interval_s=10.0) == 7.0        # 명시가 이긴다


def test_polling_updates_state_without_running_bjobs(qtbot, fake_lsf):
    """E2E — 제출은 wrapper 그대로, 상태는 콜백으로. bjobs는 한 번도 안 나간다."""
    def fetch():
        with fake_lsf.lock:
            return _payload(*[_job(fj.job_id, stat="RUN")
                              for fj in fake_lsf.jobs.values()])

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(retry_delay_s=0.05,
                         internal_refresh_min_s=0.0,
                         job_status_fetcher=fetch),
        runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"mytool r{i}.sp" for i in range(5)],
                             auto_poll=False)
        assert mgr.summary(js.id)["PEND"] == 5      # bsub 성공 = 로컬 PEND
        mgr.start_polling(js, 0.15)
        qtbot.waitUntil(lambda: mgr.summary(js.id).get("RUN") == 5,
                        timeout=10000)
        assert not [c for c in fake_lsf.calls
                    if c[0].rsplit("/", 1)[-1] == "bjobs"], "bjobs가 실행됨"
    finally:
        mgr.shutdown()


def test_lost_is_deferred_when_callback_is_down(qtbot, fake_lsf):
    """콜백 장애에도 LOST로 확정하지 않는다 — bjobs chunk 실패와 같은 계약."""
    down = {"on": False}

    def fetch():
        if down["on"]:
            raise RuntimeError("REST down")
        with fake_lsf.lock:
            return _payload(*[_job(fj.job_id, stat="RUN")
                              for fj in fake_lsf.jobs.values()])

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(retry_delay_s=0.05,
                         internal_refresh_min_s=0.0,
                         lost_after_missing_polls=1,
                         job_status_fetcher=fetch),
        runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.start_polling(js, 0.1)
        qtbot.waitUntil(lambda: mgr.summary(js.id).get("RUN") == 1,
                        timeout=10000)
        down["on"] = True
        qtbot.wait(600)                      # 여러 사이클 실패시킨다
        assert mgr.summary(js.id).get("LOST", 0) == 0
        assert mgr.summary(js.id)["RUN"] == 1
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 6) 제출 직후 LOST 유예 — 상태 원본(REST 집계)이 아직 job을 모를 때
# ----------------------------------------------------------------------
def _internal_mgr(fake_lsf, fetch, **cfg):
    cfg.setdefault("internal_refresh_min_s", 0.0)
    cfg.setdefault("lost_after_missing_polls", 1)   # 유예가 없으면 즉시 LOST
    return LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(retry_delay_s=0.05,
                         job_status_fetcher=fetch, **cfg),
        runner=fake_lsf)


def test_unseen_job_is_deferred_within_submit_grace(qtbot, fake_lsf):
    """집계가 늦어 원장에 없는 job을 LOST로 죽이면 안 된다 — 되돌릴 수 없다."""
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),   # 서버가 아직 모른다
                        internal_lost_grace_s=30.0)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.start_polling(js, 0.1)
        qtbot.wait(700)                      # 여러 사이클 미발견
        summary = mgr.summary(js.id)
        assert summary.get("LOST", 0) == 0, "유예 중에 LOST 확정됨"
        assert summary["PEND"] == 1
    finally:
        mgr.shutdown()


def test_lost_is_confirmed_after_the_grace_expires(qtbot, fake_lsf):
    """유예는 미루기지 면제가 아니다 — 지나도 안 보이면 진짜 소실로 확정한다."""
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                        internal_lost_grace_s=0.2)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.start_polling(js, 0.1)
        qtbot.waitUntil(lambda: mgr.summary(js.id).get("LOST") == 1,
                        timeout=10000)
    finally:
        mgr.shutdown()


def test_late_registration_is_picked_up_during_grace(qtbot, fake_lsf):
    """유예 중에 집계가 따라잡으면 그대로 정상 반영된다."""
    ready = {"on": False}

    def fetch():
        if not ready["on"]:
            return _payload()
        with fake_lsf.lock:
            return _payload(*[_job(fj.job_id, stat="RUN")
                              for fj in fake_lsf.jobs.values()])

    mgr = _internal_mgr(fake_lsf, fetch, internal_lost_grace_s=30.0)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.start_polling(js, 0.1)
        qtbot.wait(300)
        assert mgr.summary(js.id)["PEND"] == 1
        ready["on"] = True                   # 서버가 이제 안다
        qtbot.waitUntil(lambda: mgr.summary(js.id).get("RUN") == 1,
                        timeout=10000)
        assert mgr.summary(js.id).get("LOST", 0) == 0
    finally:
        mgr.shutdown()


def test_grace_does_not_reset_the_streak_after_it_expires(qtbot, fake_lsf):
    """유예 중에 스트릭을 올려두면 유예가 끝나는 순간 즉시 LOST가 된다 —
    유예 뒤에도 lost_after_missing_polls 만큼은 더 봐야 한다."""
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                        internal_lost_grace_s=0.25,
                        lost_after_missing_polls=3,
                        poll_interval_s=10.0)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.start_polling(js, 0.1)
        qtbot.wait(450)                      # 유예는 끝났지만 스트릭은 1~2회
        assert mgr.summary(js.id).get("LOST", 0) == 0
        qtbot.waitUntil(lambda: mgr.summary(js.id).get("LOST") == 1,
                        timeout=10000)       # 3회 채우면 확정
    finally:
        mgr.shutdown()


def test_submit_grace_is_off_on_the_bjobs_path(qtbot, manager):
    """bjobs 경로의 판정은 건드리지 않는다 — 지연의 성격이 다르다."""
    assert manager.querier._submit_grace_s() == 0.0


def test_submit_grace_zero_matches_the_bjobs_judgement(fake_lsf):
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                        internal_lost_grace_s=0.0)
    try:
        assert mgr.querier._submit_grace_s() == 0.0
    finally:
        mgr.shutdown()


def test_record_without_timestamps_is_not_deferred_forever():
    """나이를 모르는 레코드를 유예하면 영영 LOST가 안 돼 미발견에 고착된다."""
    from lsfmgr.monitor import _within_submit_grace
    from lsfmgr.states import JobRecord

    rec = JobRecord(job_id=1, array_index=None, jobset_id="js",
                    job_key="k", state=JobState.PEND, command="r")
    assert rec.submit_time is None and rec.updated_at is None
    assert _within_submit_grace(rec, datetime.now(), 300.0) is False


# ----------------------------------------------------------------------
# 7) 진단에서 나온 결함 9건 회귀 가드
#    (자세한 배경은 각 테스트 docstring — 전부 "조용히 망가지던" 경로다)
# ----------------------------------------------------------------------
def test_hung_callback_does_not_hold_the_caller_thread():
    """timeout 없는 콜백 하나가 폴링 스레드를 영구히 잡으면 상태 갱신이
    통째로 멈추고 shutdown까지 막힌다 — 콜백은 전용 스레드에서 돈다."""
    hang = threading.Event()                 # 절대 set 안 함
    src = _source(lambda: hang.wait() or _payload(), wait_timeout_s=1.0)
    t0 = time.monotonic()
    found, failed = src.statuses_by_ids([1])
    elapsed = time.monotonic() - t0
    assert failed == {1} and not found       # 판단 보류
    assert elapsed < 2.0, f"호출자가 {elapsed:.1f}s 붙잡힘"


def test_hung_callback_recovers_when_the_server_comes_back():
    """상한을 넘긴 조회를 인계하지 않으면 아무도 리더가 못 돼 서버가
    회복돼도 영원히 '조회 장애'로 남는다."""
    hang = threading.Event()
    state = {"fetch": lambda: hang.wait() or _payload()}
    src = _source(lambda: state["fetch"](), wait_timeout_s=0.3)
    assert src.statuses_by_ids([9])[1] == {9}        # 장애
    state["fetch"] = lambda: _payload(_job(9))       # 서버 회복
    for _ in range(5):                               # 인계 후 정상화
        found, failed = src.statuses_by_ids([9])
        if found:
            break
    assert [st.job_id for st in found] == [9] and not failed


def test_inflight_fetches_are_capped():
    """콜백이 영영 안 돌아오는데 인계를 무한 허용하면 스레드가 쌓인다."""
    hang = threading.Event()
    src = _source(lambda: hang.wait() or _payload(), wait_timeout_s=0.1)
    for _ in range(10):
        src.statuses_by_ids([1])
    assert src.stats()["inflight"] <= src.MAX_INFLIGHT


def test_shutdown_releases_waiting_callers():
    """폴링 스레드가 조회를 기다리는 중 shutdown이 걸리면 wait_timeout
    (기본 120초)만큼 종료가 밀린다."""
    hang = threading.Event()
    src = _source(lambda: hang.wait() or _payload(), wait_timeout_s=60.0)
    threading.Thread(target=lambda: src.statuses_by_ids([1]),
                     daemon=True).start()
    time.sleep(0.2)
    done = []
    t = threading.Thread(
        target=lambda: done.append(src.statuses_by_ids([2])), daemon=True)
    t.start()
    time.sleep(0.1)
    src.shutdown()
    t.join(3.0)
    assert done, "shutdown이 대기 중 호출자를 풀어 주지 않았다"


@pytest.mark.parametrize("stat,expected", [
    ("RUN", JobState.RUN), ("Run", JobState.RUN), ("run", JobState.RUN),
    ("RUNNING", JobState.RUN), ("pending", JobState.PEND),
    ("EXITED", JobState.EXIT), (" done ", JobState.DONE),
])
def test_stat_is_normalized(stat, expected):
    """대소문자 하나에 전 job이 UNKWN이 되면 UNKWN은 terminal이 아니라서
    폴링이 안 멈추고 jobset_finished/post_process가 영영 발화하지 않는다."""
    (st,) = parse_internal_jobs(_payload(_job(1, stat=stat)))
    assert st.state is expected


def test_unknown_stat_warns_once(caplog):
    """모르는 상태는 UNKWN으로 두되 **보이게** 남긴다(매 폴링 반복은 금지)."""
    import lsfmgr.internal_status as mod
    mod._warned_stats.discard("MYSTERY")
    with caplog.at_level("WARNING", logger="lsfmgr.internal_status"):
        for _ in range(3):
            parse_internal_jobs(_payload(_job(1, stat="MYSTERY")))
    warns = [r for r in caplog.records if "MYSTERY" in r.message]
    assert len(warns) == 1, f"경고 {len(warns)}회"


def test_all_rows_unparsable_is_a_failure_not_an_empty_answer():
    """dataId 표기가 다른 사이트면 전 행이 조용히 버려지고 빈 목록이 나간다 —
    정상 '없음'과 구별되지 않아 유예가 끝나는 대로 전 job이 LOST가 된다."""
    src = _source(lambda: _payload(_job(1, dataId="cluster1.1432342"),
                                   _job(2, dataId="job_777")))
    found, failed = src.statuses_by_ids([1, 2])
    assert found == [] and failed == {1, 2}, "판단 보류가 아니라 '없음'으로 나감"


def test_untracked_jobs_are_not_kept():
    """콜백은 유저의 전 job을 주는데 lsfmgr가 추적하는 건 그중 일부다 —
    나머지까지 2주 보관하면 원장이 무관한 job으로 부풀어 오른다."""
    src = _source(lambda: _payload(*[_job(i) for i in range(100, 200)]))
    found, _f = src.statuses_by_ids([150])
    assert [st.job_id for st in found] == [150]
    assert src.stats()["entries"] == 1, src.stats()


def test_interest_is_registered_before_the_merge():
    """등록이 병합보다 늦으면 갓 제출된 job이 첫 조회에서 통째로 누락된다."""
    src = _source(lambda: _payload(_job(4242)))
    found, failed = src.statuses_by_ids([4242])       # 첫 조회
    assert [st.job_id for st in found] == [4242] and not failed


def test_interest_registered_during_a_fetch_is_not_dropped():
    """조회가 **이미 떠 있는 동안** 새 job이 조회에 들어오면(제출 직후가
    정확히 그 순간이다), 그 job은 payload에 있어도 파싱 필터에 걸려 버려질
    수 있다. 결과가 '조회 장애'가 아니라 **미발견**이라 monitor의 LOST
    스트릭이 올라간다 — internal_lost_grace_s를 0으로 둔 앱은 곧장 LOST."""
    gate = threading.Event()

    def fetcher():
        gate.wait(5.0)                       # 콜백이 도는 동안 등록이 들어온다
        return _payload(_job(100), _job(200))

    src = _source(fetcher)
    out = {}
    first = threading.Thread(
        target=lambda: out.__setitem__("a", src.statuses_by_ids([100])))
    first.start()
    time.sleep(0.3)                          # 조회가 확실히 떠 있는 상태
    second = threading.Thread(
        target=lambda: out.__setitem__("b", src.statuses_by_ids([200])))
    second.start()
    time.sleep(0.3)
    gate.set()
    first.join(5.0)
    second.join(5.0)
    src.shutdown()

    found_b, failed_b = out["b"]
    assert [st.job_id for st in found_b] == [200], (
        f"조회 중 등록된 job이 누락됨: {out['b']}")
    assert not failed_b


def test_runtime_refresh_is_skipped_when_the_app_does_not_want_it():
    """poll_runtime_updates가 꺼져 있으면 monitor가 run_time 변화를 갱신
    대상에서 뺀다 — 그런데도 원장이 매 조회마다 전수 스캔으로 값을 다시
    만들면(2만 건 기준 조회당 37ms, 그동안 원장 lock 점유) 그 일이 통째로
    버려진다."""
    started = (datetime.now() - timedelta(seconds=100)).strftime(
        "%Y-%m-%dT%H:%M:%S")
    payloads = [_payload(_job(1, stat="RUN", startTime=started)),
                _payload()]                  # 2회차: 갱신분 없음
    src = _source(lambda: payloads.pop(0), track_runtime=False)
    (first,), _f = src.statuses_by_ids([1])
    time.sleep(1.1)
    (again,), _f = src.statuses_by_ids([1])
    assert again.run_time_s == first.run_time_s


def test_forget_drops_the_ledger_and_the_interest():
    """추적이 끝난 job(remove_jobs/clear_jobs/remove_jobset)은 만료를 기다릴
    수 없다 — 만료는 종료(DONE/EXIT) 항목만 보고, 원장에 오른 적 없는
    job(LOST/CANCELLED)은 관심 집합에만 남는다."""
    src = _source(lambda: _payload(_job(1), _job(2)))
    src.statuses_by_ids([1, 2, 3])           # 3은 payload에 없다(관심에만 등록)
    assert src.stats()["entries"] == 2 and src.stats()["tracked_ids"] == 3
    src.forget([1, 3])
    st = src.stats()
    assert st["entries"] == 1 and st["tracked_ids"] == 1, st


def test_run_time_advances_for_jobs_absent_from_an_incremental_payload():
    """증분 조회에서 상태가 안 바뀐 RUN job은 payload에 안 온다 — 그대로
    두면 경과시간이 옛 값에 멈춰 UI의 타이머가 정지한다."""
    started = (datetime.now() - timedelta(seconds=100)).strftime(
        "%Y-%m-%dT%H:%M:%S")
    payloads = [_payload(_job(1, stat="RUN", startTime=started)),
                _payload()]                  # 2회차: 갱신분 없음
    src = _source(lambda: payloads.pop(0))
    (first,), _f = src.statuses_by_ids([1])
    time.sleep(1.1)
    (again,), _f = src.statuses_by_ids([1])
    assert again.run_time_s > first.run_time_s, (
        f"경과시간 정지: {first.run_time_s} → {again.run_time_s}")


def test_terminal_job_run_time_is_not_recomputed():
    """끝난 job의 실행시간은 실측이다 — 나중 시각으로 늘리면 안 된다."""
    payloads = [_payload(_job(1, stat="DONE",
                              startTime="2026-08-08T12:00:00",
                              finishTime="2026-08-08T12:01:40")),
                _payload()]
    # 만료는 이 테스트의 관심사가 아니다 — 절대 날짜라 보존기간(14일)이
    # 지나면 첫 조회의 청소가 항목을 지워 버린다. 만료를 끈다.
    src = _source(lambda: payloads.pop(0), retention_days=0.0)
    (first,), _f = src.statuses_by_ids([1])
    time.sleep(1.1)
    (again,), _f = src.statuses_by_ids([1])
    assert first.run_time_s == again.run_time_s == 100


@pytest.mark.parametrize("value,expected", [
    (1723118400, datetime.fromtimestamp(1723118400)),
    ("1723118400", datetime.fromtimestamp(1723118400)),
    (1723118400000, datetime.fromtimestamp(1723118400)),
])
def test_epoch_timestamps_are_parsed(value, expected):
    """숫자 시각을 조용히 None으로 버리면 start_time/run_time이 통째로 빈다."""
    assert parse_time(value) == expected


def test_refresh_interval_follows_the_actual_polling_rate(qtbot, fake_lsf):
    """갱신 간격 기본값은 LsfConfig.poll_interval_s에서 유도되는데, 앱은
    start_polling으로 더 빠르게 돌릴 수 있다 — 그때 캐시가 폴링보다 느리면
    갱신이 밀린다."""
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                        internal_refresh_min_s=None)
    try:
        src = mgr.command.internal_status
        assert src._refresh_min_s == 5.0            # config 10초의 절반
        mgr.start_polling(mgr.create_jobset([], job_keys=[]).id, 2.0)
        assert src._refresh_min_s == 1.0            # 실제 주기 2초의 절반
    finally:
        mgr.shutdown()


def test_explicit_refresh_interval_is_not_overridden(qtbot, fake_lsf):
    """앱이 값을 명시했으면 라이브러리가 마음대로 낮추지 않는다."""
    mgr = _internal_mgr(fake_lsf, lambda: _payload(),
                        internal_refresh_min_s=8.0)
    try:
        src = mgr.command.internal_status
        mgr.start_polling(mgr.create_jobset([], job_keys=[]).id, 2.0)
        assert src._refresh_min_s == 8.0
    finally:
        mgr.shutdown()


def test_manager_kwarg_poll_interval_reaches_the_source(qtbot, fake_lsf):
    """poll_interval_s는 MANAGER_ONLY가 아니라 _defaults로 가서 config에
    안 실린다 — 그대로 두면 앱 설정이 갱신 간격에 반영되지 않는다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(
                                          retry_delay_s=0.05,
                                          job_status_fetcher=lambda: _payload()),
                        runner=fake_lsf, poll_interval_s=30.0)
    try:
        assert mgr.command.internal_status._refresh_min_s == 15.0
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 8) README §5.8 "받아들이는 JSON" 표의 계약 — 문서와 구현의 대조
# ----------------------------------------------------------------------
@pytest.mark.parametrize("payload", [
    {"jobs": [{"dataId": "1.c", "stat": "RUN"}], "count": 1,
     "updateFrom": None},                       # ① 응답 원문
    {"jobs": [{"dataId": "1.c", "stat": "RUN"}], "count": 99},   # count 불일치
    [{"dataId": "1.c", "stat": "RUN"}],         # ② job 목록만
])
def test_accepted_envelopes(payload):
    (st,) = parse_internal_jobs(payload)
    assert st.job_id == 1 and st.state is JobState.RUN


def test_null_jobs_is_an_empty_list():
    assert parse_internal_jobs({"jobs": None}) == []


@pytest.mark.parametrize("key", ["dataId", "dataid", "jobId", "jobid", "id"])
def test_id_key_aliases(key):
    (st,) = parse_internal_jobs({"jobs": [{key: "1432342.c1", "stat": "RUN"}]})
    assert st.job_id == 1432342 and st.source_cluster == "c1"


@pytest.mark.parametrize("key", ["stat", "status", "state"])
def test_stat_key_aliases(key):
    (st,) = parse_internal_jobs({"jobs": [{"dataId": "1", key: "DONE"}]})
    assert st.state is JobState.DONE


@pytest.mark.parametrize("key", ["finishTime", "finish_time",
                                 "endTime", "end_time"])
def test_finish_key_aliases(key):
    (st,) = parse_internal_jobs({"jobs": [
        {"dataId": "1", "stat": "DONE", key: "2026-08-08T12:01:40"}]})
    assert st.finish_time == datetime(2026, 8, 8, 12, 1, 40)


@pytest.mark.parametrize("key", ["exitStatus", "exitCode",
                                 "exit_code", "exit_status"])
def test_exit_key_aliases(key):
    (st,) = parse_internal_jobs({"jobs": [
        {"dataId": "1", "stat": "EXIT", key: "137"}]})
    assert st.exit_code == 137


@pytest.mark.parametrize("key", ["cluster", "clusterName", "cluster_name"])
def test_cluster_key_aliases(key):
    (st,) = parse_internal_jobs({"jobs": [
        {"dataId": "1", "stat": "RUN", key: "cl9"}]})
    assert st.source_cluster == "cl9"


@pytest.mark.parametrize("blank", [None, "", "-", "null", "NULL",
                                   "none", "nil", "n/a"])
def test_blank_markers_are_all_none(blank):
    (st,) = parse_internal_jobs({"jobs": [
        {"dataId": "1", "stat": "RUN", "startTime": blank,
         "cluster": blank}]})
    assert st.start_time is None and st.source_cluster is None


@pytest.mark.parametrize("text", [
    "2026-08-19T00:12:12", "2026-08-19 00:12:12",
    "2026-08-19T00:12:12.345", "2026:08:08T12:00:01",
])
def test_documented_time_shapes(text):
    assert parse_time(text) is not None


def test_timezone_offsets_are_converted_to_local():
    """aware/naive를 섞으면 뺄셈이 TypeError다 — 로컬 naive로 맞춘다."""
    from datetime import timezone as tz
    utc = parse_time("2026-08-19T00:00:00Z")
    plus9 = parse_time("2026-08-19T09:00:00+09:00")
    plus9b = parse_time("2026-08-19T09:00:00+0900")
    assert utc.tzinfo is None
    assert utc == plus9 == plus9b            # 같은 순간 → 같은 로컬 시각
    assert utc == datetime(2026, 8, 19, 0, 0, 0, tzinfo=tz.utc
                           ).astimezone().replace(tzinfo=None)


def test_unparsable_time_keeps_the_row():
    """시각 하나 때문에 행을 버리면 그 job이 미발견 → LOST가 된다."""
    (st,) = parse_internal_jobs({"jobs": [
        {"dataId": "1", "stat": "RUN", "startTime": "깨진값"}]})
    assert st.job_id == 1 and st.start_time is None


# ----------------------------------------------------------------------
# 8) 예비 콜백 (job_status_fetcher_failover)
# ----------------------------------------------------------------------
def test_failover_serves_when_primary_raises():
    """주 콜백이 예외를 던지면 같은 조회를 예비 콜백으로 다시 시도한다 —
    판단 보류가 아니라 결과가 나와야 failover다."""
    def primary():
        raise RuntimeError("primary down")
    src = _source(primary, failover=lambda: _payload(_job(7)))
    found, failed = src.statuses_by_ids([7])
    assert [st.job_id for st in found] == [7] and not failed
    assert src.stats()["served_by_failover"] is True


def test_failover_serves_when_primary_payload_is_garbage():
    """'동작하지 않는다'에는 해석 불가 응답도 들어간다 — 형식이 깨진 주
    콜백 응답은 조회 장애인데, 예비가 있으면 거기서 받는다."""
    src = _source(lambda: {"nope": 1},          # 'jobs' 키 없음 → ValueError
                  failover=lambda: _payload(_job(3)))
    found, failed = src.statuses_by_ids([3])
    assert [st.job_id for st in found] == [3] and not failed


def test_both_callbacks_failing_defers_judgment():
    """예비까지 실패하면 종전대로 '조회 장애' — 전원 보류, LOST 확정 없음."""
    def bad():
        raise RuntimeError("down")
    src = _source(bad, failover=bad)
    found, failed = src.statuses_by_ids([1, 2])
    assert not found and failed == {1, 2}


def test_primary_is_retried_every_fetch_and_recovers():
    """매 조회는 항상 주 콜백부터다 — 주가 회복되면 예비 사용이 자동으로
    끝나야 한다(한 번 넘어갔다고 예비에 눌러앉으면 안 된다)."""
    calls = []
    state = {"fail": True}

    def primary():
        calls.append("p")
        if state["fail"]:
            raise RuntimeError("down")
        return _payload(_job(1))

    def failover():
        calls.append("b")
        return _payload(_job(1))

    src = _source(primary, failover=failover)
    found, _f = src.statuses_by_ids([1])
    assert found and src.stats()["served_by_failover"] is True
    state["fail"] = False                        # 주 콜백 회복
    found, _f = src.statuses_by_ids([1])
    assert found and src.stats()["served_by_failover"] is False
    assert calls[-1] == "p", calls               # 회복 후엔 예비가 안 불린다


def test_hung_primary_fails_over_without_repaying_the_timeout():
    """주 콜백이 안 돌아오면(미회수) 인계된 조회는 **처음부터** 예비로
    간다 — 인계마다 다시 주부터 걸면 사이클마다 wait_timeout_s를 통째로
    날리고, 예비는 영영 차례가 안 온다."""
    hang = threading.Event()                     # 절대 set 안 함
    src = _source(lambda: hang.wait() or _payload(),
                  failover=lambda: _payload(_job(9)),
                  wait_timeout_s=0.3)
    found, failed = src.statuses_by_ids([9])     # 첫 사이클: 주에 붙잡힘
    for _ in range(5):                           # 인계 후 예비로 정상화
        if found:
            break
        found, failed = src.statuses_by_ids([9])
    assert [st.job_id for st in found] == [9] and not failed
    stats = src.stats()
    assert stats["served_by_failover"] is True
    assert stats["primary_unreturned"] >= 1      # 주 콜백은 아직 갇혀 있다
    # 주가 갇혀 있는 동안의 후속 조회는 예비로 즉시 답한다 — 타임아웃을
    # 다시 물지 않는다.
    t0 = time.monotonic()
    found, failed = src.statuses_by_ids([9])
    assert found and not failed
    assert time.monotonic() - t0 < 0.3, "미회수 주 콜백을 또 기다렸다"
    hang.set()                                   # 갇힌 스레드 정리


def test_failover_results_merge_into_the_same_ledger():
    """예비 응답도 같은 원장에 병합된다 — 증분 payload에서 주가 준 job이
    예비로 넘어갔다고 사라지면 안 된다."""
    state = {"fail": False}

    def primary():
        if state["fail"]:
            raise RuntimeError("down")
        return _payload(_job(1, stat="RUN"))

    src = _source(primary, failover=lambda: _payload(_job(2, stat="RUN")))
    src.statuses_by_ids([1, 2])                  # 주: job 1만 옴
    state["fail"] = True
    found, failed = src.statuses_by_ids([1, 2])  # 예비: job 2만 옴
    assert not failed
    assert sorted(st.job_id for st in found) == [1, 2]   # 1은 원장에서 유지


def test_failover_config_requires_primary_and_callable():
    """예비만 주면 조회가 bjobs로 가서 아무 데도 안 쓰인다 — 조용히 무시하지
    않고 생성 시점에 막는다. 호출 불가 값도 같은 취급."""
    with pytest.raises(ValueError, match="job_status_fetcher_failover"):
        LsfConfig(job_status_fetcher_failover=lambda: _payload())
    with pytest.raises(ValueError, match="호출 가능"):
        LsfConfig(job_status_fetcher=lambda: _payload(),
                  job_status_fetcher_failover="not-callable")
    with pytest.raises(ValueError, match="호출 가능"):
        _source(lambda: _payload(), failover="not-callable")


def test_manager_uses_failover_when_primary_is_down(qtbot, fake_lsf):
    """manager 통합 — 주 콜백이 죽어 있어도 예비가 상태를 공급해 jobset이
    끝까지 간다(폴링·완료 신호 flow는 조회원 내부 사정을 모른다)."""
    ids = []

    def failover():
        return _payload(*[_job(i, stat="DONE",
                               startTime="2026-08-08T12:00:00",
                               finishTime=None) for i in ids])

    def primary():
        raise RuntimeError("REST down")

    mgr = _internal_mgr(fake_lsf, primary,
                        job_status_fetcher_failover=failover)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        ids.extend(r.job_id for r in js.jobs())
        with qtbot.waitSignal(mgr.jobset_finished, timeout=10000):
            mgr.start_polling(js.id, 0.1)
        assert [r.state.value for r in js.jobs()] == ["DONE"]
        assert mgr.command.internal_status.stats()["served_by_failover"] is True
    finally:
        mgr.shutdown()


def test_failover_switch_warns_once_then_stays_quiet(caplog):
    """주 콜백이 오래 죽어 있으면 매 조회가 failover 경로다 — 전환 순간만
    warning이고 반복은 debug여야 로그가 폭주하지 않는다. 회복은 info 1회."""
    state = {"fail": True}

    def primary():
        if state["fail"]:
            raise RuntimeError("down")
        return _payload(_job(1))

    src = _source(primary, failover=lambda: _payload(_job(1)))
    with caplog.at_level("INFO", logger="lsfmgr.internal_status"):
        for _ in range(4):                       # 실패 4사이클 — 전환은 1회
            src.statuses_by_ids([1])
        state["fail"] = False                    # 회복
        src.statuses_by_ids([1])
    switch = [r for r in caplog.records
              if "예비 콜백으로 다시" in r.message and r.levelname == "WARNING"]
    recover = [r for r in caplog.records if "회복" in r.message]
    assert len(switch) == 1, [r.message for r in caplog.records]
    assert len(recover) == 1


def test_fresh_query_is_served_by_failover_when_primary_is_down():
    """kill verify는 fresh 조회다('방금'을 봐야 한다) — 주 콜백이 죽어 있어도
    예비가 그 계약(이 호출 이후 시작된 조회)을 그대로 채워야 한다."""
    def primary():
        raise RuntimeError("down")

    src = _source(primary, failover=lambda: _payload(_job(5, stat="EXIT")),
                  refresh_min_s=30.0)          # 캐시가 있어도 fresh는 우회
    src.statuses_by_ids([5], fresh=True)       # 원장 채움 + 관심 등록
    found, failed = src.statuses_by_ids([5], fresh=True)
    assert [st.state.value for st in found] == ["EXIT"] and not failed


def test_bare_none_payload_is_a_failure_not_an_empty_answer():
    """콜백이 return을 빼먹으면 payload가 None이다 — '0건'으로 접으면 전
    job이 미발견 → LOST로 몰린다. 형식 오류(조회 장애)로 올려야 하고,
    예비가 있으면 예비가 받는다. ({"jobs": None}은 빈 결과의 서버 표기라
    정상 — 구분이 이 테스트의 요점이다.)"""
    found, failed = _source(lambda: None).statuses_by_ids([1])
    assert not found and failed == {1}           # 보류, LOST 아님
    found, failed = _source(lambda: None,
                            failover=lambda: _payload(_job(1))
                            ).statuses_by_ids([1])
    assert [st.job_id for st in found] == [1] and not failed
    found, failed = _source(lambda: {"jobs": None}).statuses_by_ids([1])
    assert not found and not failed              # 봉투 있는 null = 정상 0건


# ----------------------------------------------------------------------
# 9) 건강 판정 API (fetcher_state / mgr.status_fetcher_state)
# ----------------------------------------------------------------------
def test_fetcher_state_reports_who_is_serving():
    """앱이 '지금 주가 정상인가, 예비로 버티는 중인가, 둘 다 죽었나'를
    물을 수 있어야 한다 — 상태 전이 전체를 한 바퀴 돈다."""
    from lsfmgr import FetcherState

    health = {"p": True, "f": True}

    def primary():
        if not health["p"]:
            raise RuntimeError("p down")
        return _payload(_job(1))

    def failover():
        if not health["f"]:
            raise RuntimeError("f down")
        return _payload(_job(1))

    src = _source(primary, failover=failover)
    assert src.fetcher_state() is FetcherState.IDLE       # 조회 전
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.PRIMARY    # 주 정상
    health["p"] = False
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.FAILOVER   # 예비가 동작
    health["f"] = False
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.DOWN       # 둘 다 실패
    health["f"] = True
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.FAILOVER   # 예비만 회복
    health["p"] = True
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.PRIMARY    # 완전 회복
    assert src.stats()["fetcher_state"] == "PRIMARY"      # stats에도 노출


def test_fetcher_state_without_failover():
    """예비가 없어도 유효한 API다 — 주 콜백 하나의 생사를 답한다."""
    from lsfmgr import FetcherState

    health = {"p": False}

    def primary():
        if not health["p"]:
            raise RuntimeError("down")
        return _payload(_job(1))

    src = _source(primary)
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.DOWN
    health["p"] = True
    src.statuses_by_ids([1])
    assert src.fetcher_state() is FetcherState.PRIMARY


def test_manager_status_fetcher_state(qtbot, fake_lsf):
    """manager 공개 API — bjobs 조회면 None, 콜백 조회원이면 판정."""
    from lsfmgr import FetcherState

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    try:
        assert mgr.status_fetcher_state() is None         # bjobs 모드
    finally:
        mgr.shutdown()

    mgr = _internal_mgr(fake_lsf, lambda: _payload())
    try:
        assert mgr.status_fetcher_state() is FetcherState.IDLE
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        mgr.query_once(js.id)
        qtbot.waitUntil(
            lambda: mgr.status_fetcher_state() is FetcherState.PRIMARY,
            timeout=10000)
    finally:
        mgr.shutdown()
