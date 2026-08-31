"""submit / retry / progress / cancel 테스트 (pytest-qt)."""
from __future__ import annotations

import pytest

from lsfmgr import JobState
from tests.conftest import mk_jobset, submit_cmds


def wait_submit_finished(qtbot, mgr, timeout=10000):
    with qtbot.waitSignal(mgr.submit_finished, timeout=timeout) as blocker:
        pass
    return blocker.args          # (jobset_id, SubmitReport)


# ----------------------------------------------------------------------
# 대량 submit
# ----------------------------------------------------------------------
def test_bulk_submit_parallel(qtbot, manager, fake_lsf):
    jobs = [f"run {i}" for i in range(100)]
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = submit_cmds(manager, jobs, workers=8).id
    rpt_jsid, report = blocker.args
    assert rpt_jsid == jsid
    assert report.succeeded == 100
    assert report.failed == 0

    s = manager.summary(jsid)
    assert s["total"] == 100
    assert s["PEND"] == 100
    # 전 job이 ID 확보 (수용 기준 1)
    assert all(r.job_id is not None for r in manager.get_jobs(jsid))
    # v10.1: 부착물 필드는 스키마에서 삭제됨 — jobset 메타만 확인
    assert manager.store.get_jobset(jsid).intended_count == 100


def test_bulk_submit_sequential(qtbot, manager, fake_lsf):
    jobs = [f"run {i}" for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs, workers=1).id
    assert manager.summary(jsid)["PEND"] == 10


def test_submit_emits_jobset_updated_with_initial_pend(qtbot, manager,
                                                       fake_lsf):
    """submit 완료 시 초기 PEND 상태가 jobset_updated로 즉시 발화된다 —
    폴링(첫 조회)이나 상태 변화 없이도 js.jobset_updated가 PEND를 받아야 한다.
    (auto_poll 없이 제출하면 이 발화가 없으면 갱신이 영영 안 옴)"""
    updates = []
    manager.jobset_updated.connect(lambda jsid, s: updates.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, [f"r {i}"
                                    for i in range(5)]).id
    assert updates, "submit 완료 후 jobset_updated 미발화"
    assert updates[-1]["PEND"] == 5 and updates[-1]["total"] == 5


def test_submit_emits_submitting_immediately(qtbot, manager, fake_lsf):
    """v9: create_jobset가 CREATED를 즉시 발행해 표를 채우고, submit 착수가
    SUBMITTING 리셋을 완료 전에 발행한다 — 대량 submit이 오래 걸려도
    표가 바로 갱신된다."""
    batches = []
    manager.jobs_updated.connect(
        lambda jsid, recs: batches.append([r.state for r in recs]))
    js = mk_jobset(manager, [f"r {i}" for i in range(3)])
    assert batches and batches[0] == [JobState.CREATED] * 3   # 생성 즉시
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js)
    assert batches[1] == [JobState.SUBMITTING] * 3            # 착수 즉시 리셋


def test_submit_emits_jobs_updated_progressively(qtbot, manager, fake_lsf):
    """submit 진행 중 jobs_updated가 점진 발행되어, 완료를 안 기다리고 각 job이
    SUBMITTING→PEND로 갱신된다. 최종적으로 전 job이 PEND(job_id 확보)."""
    seen = {}    # job_key → 마지막 상태
    manager.jobs_updated.connect(
        lambda jsid, recs: seen.update({r.job_key: r for r in recs}))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, [f"r {i}" for i in range(4)])
    qtbot.wait(50)                       # 마지막 배치 소진
    assert len(seen) == 4
    assert all(r.job_id is not None and r.state is JobState.PEND
               for r in seen.values())


def test_submit_failure_emits_failed_once(qtbot, manager, fake_lsf):
    """제출 실패 시 js.jobs_failed가 정확히 1회만 발화 (완료 emit과 _h_finished의
    이중 발행 제거 확인)."""
    fake_lsf.fail_next_bsub = 99
    js = submit_cmds(manager, ["x"], max_retry=0, auto_poll=False)
    failed_batches = []
    js.jobs_failed.connect(failed_batches.append)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    qtbot.wait(50)                       # 후속 큐 신호 소진
    assert len(failed_batches) == 1      # 이중 아님
    assert failed_batches[0][0].state is JobState.SUBMIT_FAILED


def test_submit_updated_relayed_to_handle(qtbot, manager, fake_lsf):
    """핸들 js.jobset_updated로도 초기 PEND 요약이 온다 (사용자 예제 경로)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, ["x"]).id
    js = manager.jobset(jsid)
    got = []
    js.jobset_updated.connect(lambda s: got.append(s))
    # 완료 후 재조회 없이도 이미 발화됐으므로, refresh로 한 번 더 확인
    with qtbot.waitSignal(js.jobset_updated, timeout=10000):
        manager.query_once(js)
    assert got and got[-1]["total"] == 1


def test_submit_started_signal(qtbot, manager):
    with qtbot.waitSignal(manager.submit_started, timeout=5000) as blocker:
        jsid = submit_cmds(manager, ["x"]).id
    assert blocker.args == [jsid]
    qtbot.waitSignal(manager.submit_finished, timeout=5000)


def test_progress_throttle_option_reduces_emits(qtbot, fake_lsf, config):
    """progress throttle 옵션을 성기게 하면 jobs_updated 발화 수가 준다."""
    from dataclasses import replace
    from lsfmgr import InMemoryStore, LsfJobManager

    def count_emits(**cfgkw):
        mgr = LsfJobManager(store=InMemoryStore(),
                            config=replace(config, **cfgkw), runner=fake_lsf)
        c = [0]
        mgr.jobs_updated.connect(lambda j, rs: c.__setitem__(0, c[0] + 1))
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            submit_cmds(mgr, [f"r {i}" for i in range(300)],
                            workers=32)
        mgr.shutdown()
        return c[0]

    default = count_emits()
    coarse = count_emits(progress_min_interval_s=0.5, progress_min_step_ratio=0.1)
    assert coarse < default                    # 성긴 설정이 덜 발화


def test_progress_signal(qtbot, manager):
    seen = []
    manager.submit_progress.connect(lambda j, d, t: seen.append((d, t)))
    jobs = [f"r {i}" for i in range(50)]
    with qtbot.waitSignal(manager.submit_finished, timeout=15000):
        submit_cmds(manager, jobs)
    assert seen, "progress Signal이 한 번도 오지 않음"
    assert seen[-1] == (50, 50)          # 마지막 통지는 반드시 (total, total)
    assert all(d <= t for d, t in seen)


# ----------------------------------------------------------------------
# retry
# ----------------------------------------------------------------------
def test_retry_then_success(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 2          # 처음 2회 실패 → 재시도로 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = submit_cmds(manager, ["x"], max_retry=3).id
    _, report = blocker.args
    assert report.succeeded == 1
    assert report.retried == 1
    rec = manager.get_jobs(jsid)[0]
    assert rec.state is JobState.PEND
    assert rec.retry_count == 2


def test_submit_failed_after_max_retry(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 99
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = submit_cmds(manager, ["x"], max_retry=2).id
    _, report = blocker.args
    assert report.failed == 1
    rec = manager.get_jobs(jsid)[0]
    assert rec.state is JobState.SUBMIT_FAILED
    assert rec.fail_reason == "BSUB_EXIT_1"
    assert report.fail_reasons == {"BSUB_EXIT_1": 1}


def test_no_jobid_parse_failure_classified(qtbot, manager, fake_lsf):
    fake_lsf.no_jobid_next_bsub = 99
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        submit_cmds(manager, ["x"], max_retry=1)
    _, report = blocker.args
    assert report.fail_reasons == {"NO_JOBID_PARSED": 1}


# ----------------------------------------------------------------------
# cancel
# ----------------------------------------------------------------------
def test_cancel_submit(qtbot, manager, fake_lsf):
    # rate limit으로 느리게 만들어 중간 취소 여지를 확보
    jobs = [f"r {i}" for i in range(50)]
    with qtbot.waitSignal(manager.submit_finished, timeout=30000) as blocker:
        jsid = submit_cmds(manager, jobs, workers=1).id
        manager.cancel_submit(jsid)
    _, report = blocker.args
    assert report.total == 50
    assert report.succeeded + report.failed + report.cancelled == 50
    assert report.cancelled > 0
    # 이미 submit된 job은 JobSet에 정상 기록
    pend = manager.get_jobs(jsid, states={JobState.PEND})
    assert len(pend) == report.succeeded


def test_cancel_submit_aborts_pending_retries_now(qtbot, manager, fake_lsf):
    """RETRY_WAIT로 backoff를 기다리는 job이 있어도 cancel_submit은 **즉시**
    사이클을 끝내야 한다 — 각 backoff 타이머의 만료를 기다리면 긴 expo
    backoff 하나에 submit_finished가 분 단위로 밀리고, 그동안
    is_active=True라 편집/재제출/remove_jobset이 전부 막힌다."""
    fake_lsf.fail_next_bsub = 99
    js = submit_cmds(manager, ["mytool a.sp"], max_retry=5,
                     retry_backoff="fixed:600", auto_poll=False)
    qtbot.waitUntil(
        lambda: manager.get_jobs(js.id)[0].state is JobState.RETRY_WAIT,
        timeout=5000)
    with qtbot.waitSignal(manager.submit_finished, timeout=3000) as blk:
        manager.cancel_submit(js)                # 600초 타이머를 안 기다린다
    rpt = blk.args[1]
    assert rpt.cancelled == 1 and rpt.failed == 0
    rec = manager.get_jobs(js.id)[0]
    assert rec.state is JobState.CANCELLED and rec.killed
    assert not manager.is_submitting(js)


def test_internal_error_transition_reaches_the_ui(qtbot, fake_lsf, config):
    """분류 불가 예외(_task_crashed)의 SUBMIT_FAILED 전이도 jobs_updated로
    나가야 한다 — SUBMIT_FAILED는 폴링 대상(is_on_lsf)이 아니라서 여기서
    안 알리면 요약 배지는 SUBMIT_FAILED인데 행은 SUBMITTING에 영구
    고착된다."""
    from lsfmgr import InMemoryStore, LsfJobManager

    def crashing_runner(argv, timeout, cwd=None):
        if any("boom" in a for a in argv):
            raise RuntimeError("runner 폭발 주입")
        return fake_lsf(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(), config=config,
                        runner=crashing_runner)
    try:
        seen = []
        mgr.jobs_updated.connect(
            lambda jsid, recs: seen.extend(r.state for r in recs))
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blk:
            js = submit_cmds(mgr, ["boom a.sp"], auto_poll=False)
        assert blk.args[1].failed == 1
        qtbot.wait(50)                       # 큐에 남은 배치 소진
        assert JobState.SUBMIT_FAILED in seen, seen
        rec = mgr.get_jobs(js.id)[0]
        assert rec.state is JobState.SUBMIT_FAILED
        assert rec.fail_reason == "INTERNAL_ERROR"
    finally:
        mgr.shutdown()


def test_selection_kill_during_failing_attempt_skips_retry_wait(
        qtbot, fake_lsf, config):
    """선택 kill이 겨냥한 job의 제출 시도가 kill **뒤에** 실패로 돌아오면
    재시도(RETRY_WAIT backoff)로 눕지 말고 곧장 CANCELLED로 확정돼야 한다 —
    _cancel_scope의 재시도 원장 비우기는 등록 전이라 이 재시도를 못 잡고,
    backoff 동안 활성으로 남아 can_submit이 막히고 kill이 덜 끝난 것처럼
    보인다. 취소 판정을 조금만 먼저 통과한 경우와 같은 결과여야 한다."""
    import threading
    from lsfmgr import InMemoryStore, LsfJobManager
    from lsfmgr.command import CommandResult

    started = threading.Event()
    release = threading.Event()

    def slow_failing_runner(argv, timeout, cwd=None):
        if any("slowboom" in a for a in argv):
            started.set()
            release.wait(10)
            return CommandResult(1, "", "LSF error: queue unavailable")
        return fake_lsf(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(), config=config,
                        runner=slow_failing_runner)
    try:
        js = mk_jobset(mgr, ["slowboom a.sp"])
        mgr.submit(js, max_retry=5, retry_backoff="fixed:600",
                   auto_poll=False)
        assert started.wait(5)               # 시도가 도는 중이다
        mgr.kill_jobs(js, ["k0"])            # 선택 kill — 취소 지시(동기)
        release.set()                        # 이제 시도가 실패로 돌아온다
        with qtbot.waitSignal(mgr.submit_finished, timeout=5000) as blk:
            pass                             # 600초 backoff를 안 기다린다
        rpt = blk.args[1]
        assert rpt.cancelled == 1 and rpt.failed == 0
        rec = mgr.get_jobs(js.id)[0]
        assert rec.state is JobState.CANCELLED and rec.killed
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# array
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# 재시도 중 상태가 UI로 나가는가 (실환경 신고: bsub가 몇 번 실패했다 성공하는
# 동안 표가 SUBMITTING에 고착 — store엔 RETRY_WAIT가 있는데 신호가 없었다)
# ----------------------------------------------------------------------
def _states_of(manager, key="k0"):
    seen = []
    manager.jobs_updated.connect(
        lambda jsid, recs: seen.extend(
            r.state.name for r in recs if r.job_key == key))
    return seen


def test_retry_wait_reaches_the_ui(qtbot, manager, fake_lsf):
    """재시도 중인 job이 표에서 SUBMITTING으로 보이면 몇 분짜리 재시도가
    '멈춘 것'으로 읽힌다 — RETRY_WAIT가 신호로 나가야 한다."""
    seen = _states_of(manager)
    fake_lsf.fail_next_bsub = 2
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, ["mytool a.sp"], auto_poll=False).id
    assert "RETRY_WAIT" in seen, f"RETRY_WAIT 미발행: {seen}"
    assert seen[-1] == "PEND"                 # 최종적으로는 제출 성공
    rec = manager.get_jobs(jsid)[0]
    assert rec.state is JobState.PEND and rec.retry_count == 2


def test_retry_cycle_is_visible_in_order(qtbot, manager, fake_lsf):
    """재시도마다 SUBMITTING↔RETRY_WAIT가 순서대로 보인다 — 몇 번째 시도인지
    표에서 읽을 수 있어야 한다."""
    seen = _states_of(manager)
    fake_lsf.fail_next_bsub = 2
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    # 중복 인접분을 접어 전이 시퀀스만 본다
    seq = [s for i, s in enumerate(seen) if i == 0 or s != seen[i - 1]]
    assert seq == ["CREATED", "SUBMITTING", "RETRY_WAIT", "SUBMITTING",
                   "RETRY_WAIT", "SUBMITTING", "PEND"], seq


def test_retry_does_not_inflate_progress(qtbot, manager, fake_lsf):
    """RETRY_WAIT 발행이 완료 계상에 얹히면 done이 부풀어 submit_finished가
    조기 발화한다 — 계상 없는 발행 경로여야 한다."""
    progress = []
    manager.submit_progress.connect(
        lambda jsid, done, total: progress.append((done, total)))
    fake_lsf.fail_next_bsub = 2
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    rpt = blk.args[1]
    assert (rpt.ok, rpt.failed, rpt.total) == (1, 0, 1)
    assert all(done <= total for done, total in progress), progress


def test_retry_wait_carries_the_failure_message(qtbot, manager, fake_lsf):
    """왜 재시도 중인지 표에서 보여야 한다 — 마지막 시도의 터미널 원문이
    RETRY_WAIT 레코드에 실려 나간다."""
    waits = []
    manager.jobs_updated.connect(
        lambda jsid, recs: waits.extend(
            r for r in recs if r.state is JobState.RETRY_WAIT))
    fake_lsf.fail_next_bsub = 1
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    assert waits, "RETRY_WAIT 미발행"
    assert waits[0].retry_count == 1
    assert waits[0].fail_reason and waits[0].fail_message


def test_first_attempt_submitting_is_not_emitted_twice(qtbot, manager,
                                                       fake_lsf):
    """최초 시도의 SUBMITTING은 착수 리셋 배치가 이미 발행한다 — task에서
    또 얹으면 대량 제출에서 job 수만큼 중복 레코드가 흐른다."""
    seen = _states_of(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    assert seen.count("SUBMITTING") == 1, seen


def test_final_progress_survives_the_finish_race(qtbot, manager):
    """마지막 1건을 계상한 worker가 _emit_progress에 들어가기 전에 다른
    worker가 finished를 세우면 최종 (total,total)이 유실됐다 — 진행바가
    49/50에서 멈춘 채 완료된다. _finish_if_done이 직접 낸다."""
    from lsfmgr.options import Options
    from lsfmgr.submitter import _SubmitContext
    from lsfmgr.util import WorkerSlots

    seen = []
    manager.submitter.progress.connect(lambda j, d, t: seen.append((d, t)))
    ctx = _SubmitContext(jobset_id="js1", total=50,
                         slots=WorkerSlots(8),
                         options=Options())
    ctx.done = 50
    # throttle 창을 소진시켜 _emit_progress가 못 내는 상황을 만든다
    ctx.throttler.should_emit(49, 50)
    manager.submitter._emit_progress(ctx)
    manager.submitter._finish_if_done(ctx)
    qtbot.waitUntil(lambda: bool(seen), timeout=5000)
    assert seen[-1] == (50, 50), seen


def test_forced_finish_does_not_fake_a_complete_progress(qtbot, manager):
    """게이트 거부처럼 done<total로 끝나는 경로에서 (total,total)을 지어내면
    '전부 처리됨'으로 오보된다."""
    from lsfmgr.options import Options
    from lsfmgr.submitter import _SubmitContext
    from lsfmgr.util import WorkerSlots

    seen = []
    manager.submitter.progress.connect(lambda j, d, t: seen.append((d, t)))
    ctx = _SubmitContext(jobset_id="js2", total=50,
                         slots=WorkerSlots(8),
                         options=Options())
    ctx.done = 3
    manager.submitter._finish_if_done(ctx, force=True)
    qtbot.wait(200)
    assert (50, 50) not in seen, seen


def test_store_failure_during_reset_is_reported_as_error(qtbot, fake_lsf):
    """store 장애로 제출을 못 한 job이 report.cancelled로만 잡히면 앱은
    kill로 취소된 것과 구별할 수 없다 — error_occurred로 따로 알린다."""
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager

    class FlakyStore(InMemoryStore):
        def __init__(self):
            super().__init__()
            self.boom_keys = set()

        def transition(self, jobset_id, job_key, new_state, *a, **kw):
            if job_key in self.boom_keys:
                self.boom_keys.discard(job_key)
                raise RuntimeError("store 장애 주입")
            return super().transition(jobset_id, job_key, new_state, *a, **kw)

    store = FlakyStore()
    mgr = LsfJobManager(store=store, runner=fake_lsf,
                        config=LsfConfig(
                                         retry_delay_s=0.05))
    try:
        errs = []
        mgr.error_occurred.connect(lambda j, e: errs.append(e))
        js = mgr.create_jobset(["a", "b"], job_keys=["ka", "kb"])
        store.boom_keys = {"ka"}
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blk:
            mgr.submit(js, auto_poll=False)
        rpt = blk.args[1]
        assert rpt.cancelled == 1 and rpt.succeeded == 1
        assert errs and "ka" in errs[0], errs
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# rate limit — 지속 처리량은 누르되 소량 제출은 통과시킨다
# ----------------------------------------------------------------------
def test_worker_slots_bound_concurrency():
    """동시 제출 슬롯 — 상한만큼만 동시에 통과한다."""
    import threading
    from lsfmgr.util import WorkerSlots

    slots = WorkerSlots(2)
    assert slots.acquire() and slots.acquire()
    got = []
    t = threading.Thread(target=lambda: got.append(slots.acquire()))
    t.start()
    t.join(0.2)
    assert not got, "상한을 넘겨 통과했다"
    slots.release()
    t.join(2.0)
    assert got == [True]
    slots.release(); slots.release()


def test_worker_slot_wait_is_cancellable():
    """슬롯 대기 중에도 kill이 즉시 먹혀야 한다 — 대량 제출 도중 kill이
    앞선 제출이 끝날 때까지 밀리면 '멈추라고 눌렀는데 안 멈춘다'가 된다."""
    import time
    from lsfmgr.util import WorkerSlots

    slots = WorkerSlots(1)
    assert slots.acquire()                       # 슬롯 소진
    t0 = time.monotonic()
    assert slots.acquire(lambda: True) is False  # 취소로 빠져나온다
    assert time.monotonic() - t0 < 0.5
