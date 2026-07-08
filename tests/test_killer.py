"""kill 전략 / 부분 kill / verify 테스트 (FR-3)."""
from __future__ import annotations

import pytest

from lsfmgr import ArrayJobSpec, JobSpec, JobState


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"r {i}") for i in range(30)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs)
    return jsid


# ----------------------------------------------------------------------
# 전략 ① group 1회 호출 (수용 기준 2)
# ----------------------------------------------------------------------
def test_kill_by_group_single_call(qtbot, manager, fake_lsf, submitted):
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted)
    jsid, report = blocker.args
    assert jsid == submitted
    assert report.requested == 30
    assert report.command_calls == 1                  # bkill 1회
    assert any(s.startswith("group:") for s in report.strategies)
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 전략 ② array
# ----------------------------------------------------------------------
def test_kill_array(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_array(ArrayJobSpec(command="r", count=25))
    # array jobset은 group도 있으므로 group이 먼저 시도됨 — group 제거하여
    # array 전략 검증
    from dataclasses import replace
    js = manager.store.get_jobset(jsid)
    manager.store.update_jobset(replace(js, lsf_group_paths=[]))
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(jsid)
    _, report = blocker.args
    assert report.command_calls == 1
    assert any(s.startswith("array:") for s in report.strategies)
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 전략 ④ chunking (부착물 전부 유실, 수용 기준 3)
# ----------------------------------------------------------------------
def test_kill_chunk_fallback(qtbot, manager, fake_lsf, submitted, config):
    from dataclasses import replace
    js = manager.store.get_jobset(submitted)
    manager.store.update_jobset(replace(
        js, lsf_group_paths=[], name_patterns=[], array_job_ids=[]))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted)
    _, report = blocker.args
    assert report.strategies == ["chunk"]
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 부분 kill (FR-3.2)
# ----------------------------------------------------------------------
def test_partial_kill_by_state(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    # 절반만 RUN으로 (store에도 반영)
    for r in recs[:15]:
        fake_lsf.set_job(r.job_id, "RUN")
        manager.store.transition(submitted, r.job_key, JobState.RUN)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted, only_state=JobState.PEND)
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
        jobs = [JobSpec(command=f"r {i}") for i in range(60)]
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            jsid = mgr.submit_bulk(jobs)
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
# kill 확인 + 재시도 (FR-3.4)
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
# kill 상태 정책 (FR-3.5) — optimistic(기본) vs actual
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
    """js.kill_jobs(job_keys) — JobSet의 선택 job만 kill, jobset 컨텍스트라
    optimistic EXIT + killed Signal 정상."""
    js = manager.jobset(submitted)
    keys = [r.job_key for r in manager.get_jobs(submitted)][:3]
    with qtbot.waitSignal(js.kill_finished, timeout=10000) as blocker:
        js.kill_jobs(keys)
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
        manager.kill_jobset(submitted)               # verify 없음
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
            jsid = mgr.submit_bulk([JobSpec(command=f"r {i}")
                                    for i in range(5)])
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as blocker:
            mgr.kill_jobset(jsid)                     # verify 없음
        _, report = blocker.args
        assert report.changed == []                  # optimistic 전이 없음
        # store는 아직 초기 PEND — 실제 LSF 상태를 안 당겨옴
        assert mgr.summary(jsid).get("PEND", 0) == 5
        assert mgr.summary(jsid).get("EXIT", 0) == 0
        # verify=True면 재조회로 실제 EXIT 반영
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobset(jsid, verify=True)
        assert mgr.summary(jsid).get("EXIT", 0) == 5
    finally:
        mgr.shutdown()


def test_kill_status_policy_validation(fake_lsf):
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
    with pytest.raises(ValueError):
        LsfConfig(kill_status_policy="bogus")
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
# verify (FR-3.3)
# ----------------------------------------------------------------------
def test_kill_verify(qtbot, manager, fake_lsf, submitted):
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted, verify=True)
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
            js = mgr.submit([f"echo {i}" for i in range(4)], mode="bulk",
                           auto_poll=False)
        recs = sorted(js.jobs(), key=lambda r: r.job_key)
        fake_lsf.set_job(recs[0].job_id, "RUN")
        fake_lsf.set_job(recs[1].job_id, "RUN")
        mgr.querier.query(js.id)                    # 2 RUN, 2 PEND
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            js.kill(only_state=JobState.PEND, verify=True)
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
            js = mgr.submit(["echo a", "echo b", "echo c"], mode="bulk",
                           auto_poll=False)
        fake_lsf.set_all("RUN")
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            js.kill_jobs(keys[:1], verify=True)     # 1개만 kill
        assert b.args[1].still_alive == 0           # 나머지 2개는 대상 아님
        assert len(fake_lsf.alive_jobs()) == 2      # 실제로 2개 살아있음
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 전체 kill은 대기 중 submit 재시도도 포기 확정 — job 부활 방지
# ----------------------------------------------------------------------
def test_whole_kill_aborts_pending_retries(qtbot, manager, fake_lsf):
    """RETRY_WAIT 중 js.kill() 후 재시도 QTimer가 발화해도 job이 부활하지
    않는다 — 예전엔 kill 뒤 타이머가 재제출해 PEND로 되살아났다."""
    import time
    fake_lsf.fail_next_bsub = 1              # 첫 bsub 실패 → RETRY_WAIT
    # 재시도 지연을 길게 — kill이 타이머 발화보다 먼저 도는 것을 보장
    js = manager.submit(["echo a"], mode="bulk", auto_poll=False, max_retry=3,
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
        js.kill()      # 전체 kill — 재시도 포기 확정 (submit_finished도 이때 발행)
    qtbot.wait(400)                          # 재시도 타이머 발화 시간 경과
    rec = js.jobs()[0]
    assert rec.state is JobState.SUBMIT_FAILED   # 부활 없음
    assert fake_lsf.alive_jobs() == []
    assert reports and reports[0].failed == 1


def test_partial_kill_keeps_pending_retries(qtbot, manager, fake_lsf):
    """부분 kill(only_state)은 재시도를 건드리지 않는다 — RETRY_WAIT job은
    타이머 발화 후 정상 재제출된다."""
    import time
    fake_lsf.fail_next_bsub = 1
    js = manager.submit(["echo a", "echo b"], mode="bulk", auto_poll=False,
                        max_retry=3)
    deadline = time.time() + 5
    while time.time() < deadline:
        sts = {r.state for r in js.jobs()}
        if JobState.RETRY_WAIT in sts and JobState.PEND in sts:
            break
        qtbot.wait(10)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        js.kill(only_state=JobState.PEND)    # PEND만 kill — 재시도 유지
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    # RETRY_WAIT였던 job은 재시도로 PEND 복귀
    assert any(r.state is JobState.PEND for r in js.jobs())
