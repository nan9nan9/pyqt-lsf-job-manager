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
    cmd = LsfCommand(LsfConfig(), job_status_fetcher=lambda: _payload())
    assert cmd.internal_status is not None


def test_explicit_bjobs_path_is_warned_as_ignored(caplog):
    """콜백을 주면 bjobs_path는 아무 데도 안 쓰인다 — mock bjobs를 가리켜
    놓고 '왜 안 불리지' 하는 것을 막는다."""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        cmd = LsfCommand(LsfConfig(bjobs_path="/opt/mock/bjobs"),
                         job_status_fetcher=lambda: _payload())
    assert cmd.internal_status is not None
    warns = [r for r in caplog.records if "무시됩니다" in r.message]
    assert len(warns) == 1, f"경고 {len(warns)}회 (생성 시 1회여야 함)"


def test_default_bjobs_path_is_not_warned(caplog):
    """안 건드린 기본값까지 경고하면 정상 사용에 잡음만 남는다."""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        LsfCommand(LsfConfig(), job_status_fetcher=lambda: _payload())
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
    assert LsfConfig(poll_interval_s=10.0).effective_internal_refresh_min_s == 5.0
    assert LsfConfig(internal_refresh_min_s=0.0
                     ).effective_internal_refresh_min_s == 0.0


def test_polling_updates_state_without_running_bjobs(qtbot, fake_lsf):
    """E2E — 제출은 wrapper 그대로, 상태는 콜백으로. bjobs는 한 번도 안 나간다."""
    def fetch():
        with fake_lsf.lock:
            return _payload(*[_job(fj.job_id, stat="RUN")
                              for fj in fake_lsf.jobs.values()])

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(retry_delay_s=0.05,
                         internal_refresh_min_s=0.0),
        runner=fake_lsf, job_status_fetcher=fetch)
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
                         lost_after_missing_polls=1),
        runner=fake_lsf, job_status_fetcher=fetch)
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
        config=LsfConfig(retry_delay_s=0.05, **cfg),
        runner=fake_lsf, job_status_fetcher=fetch)


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
