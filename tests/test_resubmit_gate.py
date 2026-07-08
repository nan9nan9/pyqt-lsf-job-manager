"""resubmit_jobs의 pre_submit 게이트 (FR-9) — kill 이전에 검사.

submit 게이트와 동일한 신호 순서/옵션. 단 resubmit는 kill-phase가 있어
게이트가 그 **이전**에 돌고, 거부/예외 시 돌던 job을 죽이지 않고 레코드도
건드리지 않는다(현재 상태 유지).
"""
from __future__ import annotations


from lsfmgr import InMemoryStore, LsfJobManager
from lsfmgr.states import JobState


def _running_job(qtbot, mgr, fake_lsf):
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = mgr.submit(["echo a"], mode="bulk", auto_poll=False)
    fake_lsf.set_all("RUN")
    mgr.querier.query(js.id)
    return js, js.jobs()[0].job_key


def _record(mgr, log):
    mgr.ready_started.connect(lambda j: log.append("ready_started"))
    mgr.ready_finished.connect(lambda j, ok: log.append(("ready_finished", ok)))
    mgr.submit_started.connect(lambda j: log.append("submit_started"))
    mgr.submit_finished.connect(
        lambda j, r: log.append(("submit_finished", r.succeeded,
                                 r.cancelled, r.failed)))


# ----------------------------------------------------------------------
# 통과 — kill 후 재제출, 신호 순서
# ----------------------------------------------------------------------
def test_resubmit_gate_pass(qtbot, manager, fake_lsf):
    js, key = _running_job(qtbot, manager, fake_lsf)
    log, got = [], {}
    _record(manager, log)

    def gate(cmds):
        got["c"] = list(cmds)
        return True

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([key], pre_submit=gate)
    assert got["c"] == ["echo a"]
    assert log[:3] == ["ready_started", ("ready_finished", True),
                       "submit_started"]
    assert fake_lsf.calls_of("bkill")            # 통과 → kill 됨
    assert js.jobs()[0].state is JobState.PEND   # 재제출됨


# ----------------------------------------------------------------------
# 거부 — kill 안 함, 재제출 안 함, 돌던 job 유지
# ----------------------------------------------------------------------
def test_resubmit_gate_reject_keeps_job(qtbot, manager, fake_lsf):
    js, key = _running_job(qtbot, manager, fake_lsf)
    log = []
    _record(manager, log)

    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as b:
        js.resubmit_jobs([key], pre_submit=lambda c: False)
    assert "submit_started" not in log
    assert log[1] == ("ready_finished", False)
    assert b.args[1].cancelled == 1
    assert fake_lsf.calls_of("bkill") == []      # 안 죽임
    assert js.jobs()[0].state is JobState.RUN    # 돌던 상태 유지


def test_resubmit_gate_reject_option_off(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        submit_finished_on_gate_reject=False)
    try:
        js, key = _running_job(qtbot, mgr, fake_lsf)
        log = []
        _record(mgr, log)
        with qtbot.waitSignal(mgr.ready_finished, timeout=10000):
            js.resubmit_jobs([key], pre_submit=lambda c: False)
        qtbot.wait(150)
        assert "submit_finished" not in [e if isinstance(e, str) else e[0]
                                         for e in log]
        assert fake_lsf.calls_of("bkill") == []
        assert js.jobs()[0].state is JobState.RUN
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 예외 — error + submit_finished(failed), 돌던 job은 그대로(SUBMIT_FAILED 아님)
# ----------------------------------------------------------------------
def test_resubmit_gate_exception(qtbot, manager, fake_lsf):
    js, key = _running_job(qtbot, manager, fake_lsf)
    log, errs = [], []
    _record(manager, log)
    manager.error_occurred.connect(lambda j, m: errs.append(m))

    def boom(cmds):
        raise RuntimeError("전처리 실패")

    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as b:
        js.resubmit_jobs([key], pre_submit=boom)
    assert b.args[1].failed == 1
    assert any("전처리 실패" in m for m in errs)
    assert fake_lsf.calls_of("bkill") == []
    # 돌던 job은 SUBMIT_FAILED로 오염되지 않고 RUN 유지
    assert js.jobs()[0].state is JobState.RUN


# ----------------------------------------------------------------------
# terminal job 재제출 + 게이트 (kill 없음 경로)
# ----------------------------------------------------------------------
def test_resubmit_gate_terminal_job(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], mode="bulk", auto_poll=False)
    key = js.jobs()[0].job_key
    manager.store.transition(js.id, key, JobState.EXIT, exit_code=1)  # terminal
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([key], pre_submit=lambda c: True)
    assert js.jobs()[0].state is JobState.PEND   # 게이트 통과 → 재제출


# ----------------------------------------------------------------------
# 게이트 없으면 기존 동작 (ready 신호 없음)
# ----------------------------------------------------------------------
def test_resubmit_no_gate(qtbot, manager, fake_lsf):
    js, key = _running_job(qtbot, manager, fake_lsf)
    log = []
    _record(manager, log)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([key])
    assert "ready_started" not in log
    assert log[0] == "submit_started"


# ----------------------------------------------------------------------
# 게이트 실행 중 cancel → kill·재제출 안 함, finished(cancelled)
# ----------------------------------------------------------------------
def test_resubmit_gate_cancel_during(qtbot, fake_lsf, config):
    import threading
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    started, release = threading.Event(), threading.Event()

    def slow(cmds):
        started.set()
        release.wait(5)
        return True

    try:
        js, key = _running_job(qtbot, mgr, fake_lsf)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js.resubmit_jobs([key], pre_submit=slow)
            assert started.wait(3)
            js.cancel()               # 게이트 실행 중 취소
            release.set()
        assert fake_lsf.calls_of("bkill") == []   # 안 죽임
        assert js.jobs()[0].state is JobState.RUN
    finally:
        release.set(); mgr.shutdown()
