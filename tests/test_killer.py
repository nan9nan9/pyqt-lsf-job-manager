"""kill 전략 / 부분 kill / verify 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobState
from tests.conftest import mk_jobset, submit_cmds


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [f"r {i}" for i in range(30)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs).id
    return jsid


# ----------------------------------------------------------------------
# 전략 ① group 1회 호출 (수용 기준 2)
# ----------------------------------------------------------------------
def test_kill_jobs_survives_pool_lookup_failure(qtbot, manager, fake_lsf):
    """리팩토링 회귀 F1: 개별 id kill 도중 jobset이 소실(close/merge 경합)
    되거나 비수치 id가 와도 **bkill 자체는 실행**돼야 한다 — 레코드 풀
    조회는 kill 이후의 마킹 단계에서만 쓰이고, 실패하면 마킹만 포기한다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["g 0", "g 1"], auto_poll=False)
    fake_lsf.set_all("RUN")
    ids = [r.job_id for r in js.jobs()]
    manager.store.store_delete_jobset(js.id)     # jobset 소실 경합 재현
    with qtbot.waitSignal(manager.killer.finished, timeout=10000) as blk:
        manager.killer.kill_jobs(ids, jobset_id=js.id)
    report = blk.args[1]
    assert not any("internal" in e for e in report.errors), report.errors
    assert report.unconfirmed == 0               # bkill은 정상 수행됨
    assert fake_lsf.alive_jobs() == []           # LSF job은 죽었다

    # 비수치 id (jobset 컨텍스트 없음) — bkill 전달은 되고 예외는 없어야
    with qtbot.waitSignal(manager.killer.finished, timeout=10000) as blk:
        manager.killer.kill_jobs(["weird-id"])
    assert not any("internal" in e for e in blk.args[1].errors)


def test_whole_kill_survives_chunk_failure_with_retry(qtbot, fake_lsf):
    """v10.1: 전체 kill도 confirm 경로 — 첫 bkill chunk가 순단(rc=255)이어도
    나머지 chunk를 계속 시도하고, 미확인분은 재시도로 살려낸다 (이전에는
    첫 장애에 kill 전체가 무위였다 — 리뷰 H2 회귀)."""
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
    cfg = LsfConfig(rate_limit_per_s=None, chunk_size=10, kill_retry_delay_s=0.05)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"k {i}" for i in range(30)],
                             auto_poll=False)
        fake_lsf.set_all("RUN")
        fake_lsf.fail_next_bkill = 1             # 첫 chunk만 장애
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as blk:
            mgr.kill(js)
        report = blk.args[1]
        assert report.requested == 30
        assert report.unconfirmed == 0           # 재시도로 전원 확인
        assert report.kill_retries >= 1
        assert fake_lsf.alive_jobs() == []       # 유출 0
        assert all(r.state is JobState.EXIT for r in js.jobs())
    finally:
        mgr.shutdown()


def test_kill_whole_jobset_chunked(qtbot, manager, fake_lsf, submitted):
    """전체 kill — job_id chunk 단일 경로 (v10). 호출 수는 kill_chunk_size로
    갈린다(조회용 chunk_size와 별개 — bkill은 쓰기라 건당 비용이 크다)."""
    import math

    from lsfmgr import LsfConfig
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(submitted)
    jsid, report = blocker.args
    assert jsid == submitted
    assert report.requested == 30
    expected = math.ceil(30 / LsfConfig().kill_chunk_size)
    assert report.command_calls == expected, (
        f"bkill {report.command_calls}회 (기대 {expected})")
    assert report.strategies == ["chunk"]
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 전략 ② array
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 전략 ④ chunking (부착물 전부 유실, 수용 기준 3)
# ----------------------------------------------------------------------
def test_kill_chunk_fallback(qtbot, manager, fake_lsf, submitted, config):
    from dataclasses import replace
    js = manager.store.get_jobset(submitted)
    manager.store.update_jobset(replace(
        js))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(submitted)
    _, report = blocker.args
    assert report.strategies == ["chunk"]
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 부분 kill
# ----------------------------------------------------------------------
def test_partial_kill_by_state(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    # 절반만 RUN으로 (store에도 반영)
    for r in recs[:15]:
        fake_lsf.set_job(r.job_id, "RUN")
        manager.store.transition(submitted, r.job_key, JobState.RUN)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(submitted, only_state=JobState.PEND)
    _, report = blocker.args
    assert report.requested == 15
    run_alive = [j for j in fake_lsf.alive_jobs() if j.stat == "RUN"]
    assert len(run_alive) == 15                       # RUN은 살아있음


def test_kill_individual_ids(qtbot, manager, fake_lsf, submitted):
    ids = [r.job_id for r in manager.get_jobs(submitted)][:5]
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(ids)
    _, report = blocker.args
    assert report.requested == 5
    assert report.unconfirmed == 0                # 전부 'is being terminated' 확인
    assert len(fake_lsf.alive_jobs()) == 25


def test_kill_progress_signal(qtbot, fake_lsf, config):
    """대량 chunk kill 시 kill_progress(done, total)가 발화되고, 마지막은
    반드시 (total, total)로 끝난다 (submit_progress와 대칭)."""
    from dataclasses import replace
    from lsfmgr import InMemoryStore, LsfJobManager
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=replace(config, chunk_size=10),  # 여러 chunk
                        runner=fake_lsf)
    try:
        jobs = [f"r {i}" for i in range(60)]
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            jsid = submit_cmds(mgr, jobs).id
        ids = [r.job_id for r in mgr.get_jobs(jsid)]
        seen = []
        mgr.kill_progress.connect(
            lambda j, d, t: seen.append((d, t)) if j == jsid else None)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs(ids, jobset_id=jsid)
        assert seen, "kill_progress가 한 번도 오지 않음"
        assert seen[-1] == (60, 60)                # 마지막은 100%
        assert all(0 <= d <= t == 60 for d, t in seen)
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# kill 확인 + 재시도
# ----------------------------------------------------------------------
def test_kill_retries_until_confirmed(qtbot, manager, fake_lsf, submitted):
    """bkill이 일시 장애(rc≠0, 확인 문구 없음)면 submit처럼 재시도해서,
    'is being terminated' 확인이 뜰 때까지 반복한다."""
    ids = [r.job_id for r in manager.get_jobs(submitted)][:3]
    fake_lsf.fail_next_bkill = 2                  # 처음 2번 bkill은 장애
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(ids)
    _, report = blocker.args
    assert report.kill_retries >= 1              # 재시도 발생
    assert report.unconfirmed == 0               # 결국 전부 확인됨
    assert all(j.job_id not in ids for j in fake_lsf.alive_jobs())


# ----------------------------------------------------------------------
# kill 상태 정책 — optimistic(기본) vs actual
# ----------------------------------------------------------------------
def test_kill_jobs_optimistic_without_jobset(qtbot, manager, fake_lsf,
                                             submitted):
    """kill_jobs([ids])를 jobset_id 없이 불러도 optimistic EXIT가 전역 검색으로
    적용된다 — store가 즉시 EXIT라 폴링이 RUN으로 되돌리는 깜빡임이 없다."""
    ids = [r.job_id for r in manager.get_jobs(submitted)][:5]
    per_job = []
    manager.jobs_updated.connect(lambda j, recs: per_job.append((j, recs)))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(ids)                       # jobset_id 없음
    _, report = blocker.args
    assert len(report.changed) == 5                  # 전역 검색으로 EXIT 전이
    # store가 즉시 EXIT (수동 추론 불필요)
    exited = manager.get_jobs(submitted, states={JobState.EXIT})
    assert {r.job_id for r in exited} == set(ids)
    # jobs_updated가 해당 jobset으로 EXIT 발화
    assert any(j == submitted and all(r.state is JobState.EXIT for r in recs)
               for j, recs in per_job)


def test_js_kill_jobs_by_key(qtbot, manager, fake_lsf, submitted):
    """manager.kill_jobs(js, job_keys) — JobSet의 선택 job만 kill, jobset 컨텍스트라
    optimistic EXIT + killed Signal 정상."""
    js = manager.jobset(submitted)
    keys = [r.job_key for r in manager.get_jobs(submitted)][:3]
    with qtbot.waitSignal(js.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(js, keys)
    report = blocker.args[0]
    assert len(report.changed) == 3
    exited = manager.get_jobs(submitted, states={JobState.EXIT})
    assert len(exited) == 3
    # 안 죽인 나머지는 그대로
    assert manager.summary(submitted).get("PEND", 0) == 27


def test_kill_optimistic_marks_exit_immediately(qtbot, manager, fake_lsf,
                                                submitted):
    """기본 정책(optimistic): terminated 확인 시 폴링/verify 없이 즉시 EXIT.
    jobs_updated(EXIT 레코드) + jobset_updated(요약)로 UI에 바로 반영."""
    per_job = []
    manager.jobs_updated.connect(lambda j, recs: per_job.append(recs))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(submitted)               # verify 없음
    _, report = blocker.args
    assert len(report.changed) == 30                 # 즉시 EXIT 전이
    s = manager.summary(submitted)
    assert s.get("EXIT", 0) == 30 and s.get("PEND", 0) == 0
    assert per_job and all(r.state is JobState.EXIT for r in per_job[-1])


def test_kill_actual_waits_for_lsf(qtbot, fake_lsf, config):
    """actual 정책: terminated 확인만으론 상태를 안 바꾸고, 실제 LSF 상태
    (verify/폴링)로만 EXIT를 반영한다."""
    from lsfmgr import LsfJobManager, InMemoryStore
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        kill_status_policy="actual")
    try:
        assert mgr.config.kill_status_policy == "actual"
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            jsid = submit_cmds(mgr, [f"r {i}"
                                    for i in range(5)]).id
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as blocker:
            mgr.kill(jsid)                     # verify 없음
        _, report = blocker.args
        assert report.changed == []                  # optimistic 전이 없음
        # store는 아직 초기 PEND — 실제 LSF 상태를 안 당겨옴
        assert mgr.summary(jsid).get("PEND", 0) == 5
        assert mgr.summary(jsid).get("EXIT", 0) == 0
        # verify=True면 재조회로 실제 EXIT 반영
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill(jsid, verify=True)
        assert mgr.summary(jsid).get("EXIT", 0) == 5
    finally:
        mgr.shutdown()


def test_kill_status_policy_validation(fake_lsf):
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
    with pytest.raises(ValueError):
        LsfConfig(rate_limit_per_s=None, kill_status_policy="bogus")
    with pytest.raises(ValueError):                  # manager kwarg 경로
        LsfJobManager(store=InMemoryStore(), runner=fake_lsf,
                      kill_status_policy="nope")


def test_kill_unconfirmed_reported(qtbot, manager, fake_lsf, submitted):
    """확인이 끝내 안 되면(장애 지속) unconfirmed로 보고하고 error에 남긴다."""
    ids = [r.job_id for r in manager.get_jobs(submitted)][:3]
    fake_lsf.fail_next_bkill = 99                # 계속 장애 → 확인 불가
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(ids)
    _, report = blocker.args
    assert report.unconfirmed == 3               # 재시도 후에도 미확인
    assert report.kill_retries == 2              # kill_max_retry 기본 2회
    assert report.errors                         # 실패 메시지 기록


# ----------------------------------------------------------------------
# verify
# ----------------------------------------------------------------------
def test_kill_verify(qtbot, manager, fake_lsf, submitted):
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(submitted, verify=True)
    _, report = blocker.args
    assert report.still_alive == 0
    # verify 조회가 store에도 반영됨 (killed → EXIT)
    s = manager.summary(submitted)
    assert s.get("EXIT", 0) == 30


# ----------------------------------------------------------------------
# verify는 kill 대상만 잔존으로 센다 (부분/개별 kill에서 대상 아닌 job 제외)
# ----------------------------------------------------------------------
def test_partial_kill_verify_counts_only_targets(qtbot, fake_lsf, config):
    """PEND만 kill + verify — 남은 RUN job은 still_alive에 세지 않아야 한다
    (예전엔 jobset 전체 alive를 세 kill이 실패한 것처럼 보였다)."""
    from lsfmgr import InMemoryStore, LsfJobManager
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        kill_status_policy="actual")
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"echo {i}" for i in range(4)],
                           auto_poll=False)
        recs = sorted(js.jobs(), key=lambda r: r.job_key)
        fake_lsf.set_job(recs[0].job_id, "RUN")
        fake_lsf.set_job(recs[1].job_id, "RUN")
        mgr.querier.query(js.id)                    # 2 RUN, 2 PEND
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill(js, only_state=JobState.PEND, verify=True)
        assert b.args[1].still_alive == 0           # RUN 2개는 대상 아님
    finally:
        mgr.shutdown()


def test_individual_kill_verify_counts_only_targets(qtbot, fake_lsf, config):
    """kill_jobs(선택 job) + verify — 선택 안 한 RUN job은 제외."""
    from lsfmgr import InMemoryStore, LsfJobManager
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        kill_status_policy="actual")
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a", "echo b", "echo c"],
                           auto_poll=False)
        fake_lsf.set_all("RUN")
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill_jobs(js, keys[:1], verify=True)     # 1개만 kill
        assert b.args[1].still_alive == 0           # 나머지 2개는 대상 아님
        assert len(fake_lsf.alive_jobs()) == 2      # 실제로 2개 살아있음
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 전체 kill은 대기 중 submit 재시도도 포기 확정 — job 부활 방지
# ----------------------------------------------------------------------
def test_whole_kill_aborts_pending_retries(qtbot, manager, fake_lsf):
    """RETRY_WAIT 중 manager.kill(js) 후 재시도 QTimer가 발화해도 job이 부활하지
    않는다 — 예전엔 kill 뒤 타이머가 재제출해 PEND로 되살아났다."""
    import time
    fake_lsf.fail_next_bsub = 1              # 첫 bsub 실패 → RETRY_WAIT
    # 재시도 지연을 길게 — kill이 타이머 발화보다 먼저 도는 것을 보장
    js = submit_cmds(manager, ["echo a"], auto_poll=False, max_retry=3,
                        retry_backoff="fixed:2")
    deadline = time.time() + 5
    while time.time() < deadline:
        recs = js.jobs()
        if recs and recs[0].state is JobState.RETRY_WAIT:
            break
        qtbot.wait(10)
    assert js.jobs()[0].state is JobState.RETRY_WAIT

    reports = []
    manager.submit_finished.connect(lambda _js, r: reports.append(r))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)      # 전체 kill — 재시도 포기 확정 (submit_finished도 이때 발행)
    qtbot.wait(400)                          # 재시도 타이머 발화 시간 경과
    rec = js.jobs()[0]
    assert rec.state is JobState.CANCELLED       # 부활 없음(실패가 아닌 취소)
    assert fake_lsf.alive_jobs() == []
    assert reports and reports[0].cancelled == 1 and reports[0].failed == 0


def test_partial_kill_keeps_pending_retries(qtbot, manager, fake_lsf):
    """부분 kill(only_state)은 재시도를 건드리지 않는다 — RETRY_WAIT job은
    타이머 발화 후 정상 재제출된다."""
    import time
    fake_lsf.fail_next_bsub = 1
    js = submit_cmds(manager, ["echo a", "echo b"], auto_poll=False,
                        max_retry=3)
    deadline = time.time() + 5
    while time.time() < deadline:
        sts = {r.state for r in js.jobs()}
        if JobState.RETRY_WAIT in sts and JobState.PEND in sts:
            break
        qtbot.wait(10)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js, only_state=JobState.PEND)    # PEND만 kill — 재시도 유지
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    # RETRY_WAIT였던 job은 재시도로 PEND 복귀
    assert any(r.state is JobState.PEND for r in js.jobs())


# ----------------------------------------------------------------------
# jobset 핸들 없이 kill (v10.3) — GUI가 행의 job_key만 들고 죽인다
# ----------------------------------------------------------------------
def test_kill_by_keys_without_handle_infers_jobset(qtbot, manager, fake_lsf):
    """kill_jobs(job_keys=[...])는 핸들 없이도 소유 jobset을 역추적해
    컨텍스트로 채택한다 — verify·kill_started가 그대로 동작."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["k 0", "k 1", "k 2"], auto_poll=False)
    fake_lsf.set_all("RUN")
    keys = [r.job_key for r in js.jobs()][:2]
    started = []
    manager.kill_started.connect(lambda jsid: started.append(jsid))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill_jobs(job_keys=keys, verify=True)      # js 없이
    jsid, report = blk.args
    assert jsid == js.id and started == [js.id]     # jobset 자동 채택
    assert report.still_alive == 0                  # verify 동작
    assert len(fake_lsf.alive_jobs()) == 1          # 선택한 2건만 죽음
    states = sorted(r.state.name for r in js.jobs())
    assert states == ["EXIT", "EXIT", "RUN"], states


def test_global_kill_verify_without_jobset(qtbot, manager, fake_lsf):
    """jobset 컨텍스트가 없어도 verify는 대상 id 직접 조회로 수행된다
    (예전에는 조용히 무시돼 still_alive=None이었다)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["v 0", "v 1"], auto_poll=False)
    fake_lsf.set_all("RUN")
    ids = [r.job_id for r in js.jobs()]
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill_jobs(ids, verify=True)         # jobset_id 없음
    jsid, report = blk.args
    assert jsid == ""                               # 전역 kill
    assert report.still_alive == 0                  # 미검증(None)이 아니다
    assert fake_lsf.alive_jobs() == []


def test_kill_jobs_requires_target(manager):
    """대상 없이 호출하면 조용한 no-op 대신 TypeError."""
    with pytest.raises(TypeError):
        manager.kill_jobs()


def test_kill_by_keys_spanning_jobsets_splits_per_jobset(qtbot, manager,
                                                         fake_lsf):
    """여러 jobset에 걸친 key를 핸들 없이 넘기면 jobset별로 나눠 각각 정식
    kill을 건다 — 컨텍스트를 잃고 전역으로 뭉개지 않는다(신호도 jobset별)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = submit_cmds(manager, ["a 0", "a 1"], auto_poll=False)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = submit_cmds(manager, ["b 0", "b 1"], auto_poll=False)
    fake_lsf.set_all("RUN")
    keys = [a.jobs()[0].job_key, b.jobs()[0].job_key]
    seen = []
    manager.kill_finished.connect(lambda jsid, rep: seen.append(jsid))
    with qtbot.waitSignals([manager.kill_finished, manager.kill_finished],
                           timeout=10000):
        manager.kill_jobs(job_keys=keys, verify=True)
    assert sorted(seen) == sorted([a.id, b.id])     # jobset별 kill_finished
    assert "" not in seen                           # 전역으로 뭉개지 않음
    assert len(fake_lsf.alive_jobs()) == 2          # 선택한 2건만 죽음


def test_kill_jobs_rejects_jobset_without_keys(manager):
    """선택 kill인데 선택이 없으면 조용한 no-op 대신 TypeError — 호출자는
    뭔가 죽었다고 믿는데 실제로는 0건이라 위험하다."""
    js = mk_jobset(manager, ["x"])
    with pytest.raises(TypeError):
        manager.kill_jobs(js)
    with pytest.raises(TypeError):
        manager.kill_jobs(js.id)
    manager.kill_jobs(js, [])            # 빈 선택은 명시하면 정상 no-op


def test_kill_jobs_accepts_jobset_id_string_with_keys(qtbot, manager,
                                                      fake_lsf):
    """핸들 대신 jobset_id 문자열 + keys 조합(mgr.kill(js.id)과 같은 관용).
    회귀: 원시 id 경로로 새면 jsid 문자열이 한 글자씩 bkill 대상이 된다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["s 0", "s 1"], auto_poll=False)
    fake_lsf.set_all("RUN")
    key = js.jobs()[0].job_key
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill_jobs(js.id, [key])
    jsid, report = blk.args
    assert jsid == js.id and report.requested == 1
    assert len(fake_lsf.alive_jobs()) == 1
    # jsid 문자 분해가 일어나면 무관한 한 자리 id가 bkill로 나간다
    for call in fake_lsf.calls_of("bkill"):
        assert all(len(t) > 1 for t in call[1:]), call


def test_global_verify_does_not_mark_surviving_array_exited(qtbot, manager,
                                                            fake_lsf):
    """전역 verify가 실측한 생존 array를 optimistic EXIT가 덮으면 안 된다 —
    접힌 array 레코드(job_id, None)는 parent id로 역매핑해야 한다."""
    from tests.fake_lsf import FakeJob
    from lsfmgr import JobRecord

    js = mk_jobset(manager, intended_count=1)
    aid = 7700
    manager.store.store_add_job(JobRecord(
        job_id=aid, array_index=None, jobset_id=js.id,
        job_key=f"{js.id}_arr", state=JobState.RUN, command="r"))
    fake_lsf.forward_needs_env = True
    for i in (1, 2):
        fake_lsf.jobs[f"{aid}[{i}]"] = FakeJob(
            job_id=aid, array_index=i, name=f"n{i}", group=None, queue="q",
            command="r", stat="RUN",
            forward_cluster="busan" if i == 1 else None)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill_jobs([aid], verify=True)        # jobset 컨텍스트 없음
    report = blk.args[1]
    assert report.still_alive == 1                   # element 1이 살아남음
    assert manager.get_jobs(js.id)[0].state is JobState.RUN   # EXIT 금지
