"""kill 우선권 (FR-3) — submit 진행 중 kill이 와도 유출/스킵이 없어야 한다.

정책:
  - kill 시점에 아직 제출 안 된 job(SUBMITTING, worker 미착수) → submit 취소,
    CREATED 복귀 (제출 자체를 안 함)
  - 이미 bsub에 들어간 job → killer가 submit 정지(quiesce)를 기다린 뒤
    job_id로 kill (SUBMITTING이 is_on_lsf 스냅샷에서 스킵돼 몇 초 뒤 PEND로
    살아남던 구멍을 막는다)
"""
from __future__ import annotations

import threading

from lsfmgr import InMemoryStore, LsfJobManager
from lsfmgr.states import JobState


class GatedBsub:
    """bsub 호출만 gate에서 블록하는 runner 래퍼 — 'submit 진행 중'을 결정적으로
    재현한다. FakeLsf lock 밖에서 대기하므로 bjobs/bkill은 병행 진행된다."""

    def __init__(self, fake):
        self.fake = fake
        self.gate = threading.Event()        # set 전까지 bsub 블록
        self.entered = threading.Event()     # 첫 bsub 진입 통지

    def __call__(self, argv, timeout):
        if argv[0].rsplit("/", 1)[-1] == "bsub":
            self.entered.set()
            self.gate.wait(10)
        return self.fake(argv, timeout)


def test_kill_during_submit_cancels_unsubmitted_and_kills_submitted(
        qtbot, config, fake_lsf):
    """핵심 시나리오: worker 1개가 bsub 진행 중 + 2개 미착수일 때 kill —
    진행 중이던 1개는 제출 완료 후 kill(EXIT), 미착수 2개는 CREATED 복귀."""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = mgr.submit(["echo 1", "echo 2", "echo 3"], mode="bulk",
                        workers=1, auto_poll=False)
        assert runner.entered.wait(5)        # worker가 bsub 진입 (제출 중)

        with qtbot.waitSignals([mgr.submit_finished, mgr.kill_finished],
                               timeout=15000):
            mgr.kill_jobset(js.id)           # kill 우선권 발동
            runner.gate.set()                # 진행 중이던 bsub 완료 허용

        states = sorted(r.state.name for r in js.jobs())
        assert states == ["CREATED", "CREATED", "EXIT"], states
        killed = [r for r in js.jobs() if r.state is JobState.EXIT]
        assert killed[0].fail_reason == "KILLED"
        assert killed[0].job_id is not None  # 제출돼 버린 그 job이 kill됨
        assert len(fake_lsf.jobs) == 1       # LSF에 실제 제출된 건 1개뿐
        assert fake_lsf.alive_jobs() == []   # 그리고 그것도 죽었다 (유출 0)
    finally:
        mgr.shutdown()


def test_kill_during_submit_invariant_no_survivors(qtbot, manager, fake_lsf):
    """레이스 불변식: submit과 kill이 어떤 순서로 겹치든, 두 작업이 끝난 뒤
    LSF 생존자(on-lsf)는 없고 각 job은 EXIT/CREATED/SUBMIT_FAILED 중 하나다."""
    with qtbot.waitSignals([manager.submit_finished, manager.kill_finished],
                           timeout=15000):
        js = manager.submit([f"echo {i}" for i in range(30)], mode="bulk",
                            auto_poll=False)
        manager.kill_jobset(js.id)

    assert fake_lsf.alive_jobs() == []
    allowed = {JobState.EXIT, JobState.CREATED, JobState.SUBMIT_FAILED}
    got = {r.state for r in js.jobs()}
    assert got <= allowed, got               # SUBMITTING/PEND 잔존 금지


def test_kill_during_array_submit_no_leak(qtbot, config, fake_lsf):
    """array submit(bsub 1회) 진행 중 kill — quiesce가 제출 완료를 기다린 뒤
    array_id로 죽여 전 element가 EXIT로 정리된다 (유출 없음)."""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = mgr.submit("echo x", count=3, mode="array", auto_poll=False)
        assert runner.entered.wait(5)        # array bsub 진입

        with qtbot.waitSignals([mgr.submit_finished, mgr.kill_finished],
                               timeout=15000):
            mgr.kill_jobset(js.id)
            runner.gate.set()

        assert all(r.state is JobState.EXIT for r in js.jobs())
        assert fake_lsf.alive_jobs() == []
    finally:
        mgr.shutdown()


def test_array_cancel_before_bsub_returns_created(qtbot, manager, fake_lsf):
    """cancel이 bsub 이전에 걸린 array task는 제출 없이 전 element CREATED
    복귀 (bulk _task_cancelled와 대칭인 안전 지점 중단)."""
    from lsfmgr.config import ArrayJobSpec
    from lsfmgr.options import Options
    from lsfmgr.qt import QThreadPool
    from lsfmgr.states import JobRecord
    from lsfmgr.submitter import _ArraySubmitTask, _SubmitContext
    from lsfmgr.util import TokenBucketLimiter

    jsid = manager.create_jobset(2)
    manager.store.add_jobs([
        JobRecord(job_id=None, array_index=i, jobset_id=jsid,
                  lsf_job_name=f"{jsid}[{i}]", state=JobState.SUBMITTING,
                  command="echo x")
        for i in (1, 2)])
    ctx = _SubmitContext(jobset_id=jsid, total=1, max_retry=0,
                         pool=QThreadPool(), limiter=TokenBucketLimiter(None),
                         options=Options())
    ctx.cancel_event.set()                   # kill/cancel이 먼저 도착한 상황

    _ArraySubmitTask(manager.submitter, ctx,
                     ArrayJobSpec(command="echo x", count=2), 0).run()

    assert all(r.state is JobState.CREATED
               for r in manager.store.get_jobs(jsid))
    assert fake_lsf.calls_of("bsub") == []   # 제출 자체가 안 나감
