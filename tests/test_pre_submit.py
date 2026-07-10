"""pre_submit 게이트 (FR-9) — 제출 전 커맨드 리스트 전체를 단일 워커에서 검사.

신호 순서: pre_submit_started → pre_submit_finished(ok) → (ok일 때만) submit_started →
submit_finished. False/예외 처리와 옵션(submit_finished_on_gate_reject) 검증.
"""
from __future__ import annotations


from lsfmgr import InMemoryStore, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.states import JobState


def _record(mgr, log):
    mgr.pre_submit_started.connect(lambda j: log.append(("pre_submit_started", j)))
    mgr.pre_submit_finished.connect(lambda j, ok: log.append(("pre_submit_finished", ok)))
    mgr.submit_started.connect(lambda j: log.append(("submit_started",)))
    mgr.submit_finished.connect(
        lambda j, r: log.append(("submit_finished", r.succeeded,
                                 r.cancelled, r.failed)))


# ----------------------------------------------------------------------
# 통과 (True) — 신호 순서 + 콜백이 커맨드 리스트 수신
# ----------------------------------------------------------------------
def test_gate_pass_order_and_commands(qtbot, manager, fake_lsf):
    log, got = [], {}
    _record(manager, log)

    def gate(cmds):
        got["cmds"] = list(cmds)
        return True

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a", "echo b"],
                            auto_poll=False, pre_submit=gate)
    assert got["cmds"] == ["echo a", "echo b"]
    kinds = [e[0] for e in log]
    assert kinds == ["pre_submit_started", "pre_submit_finished", "submit_started",
                     "submit_finished"]
    assert log[1] == ("pre_submit_finished", True)
    assert all(r.state is JobState.PEND for r in js.jobs())


# ----------------------------------------------------------------------
# 거부 (False) — 기본: submit_finished(cancelled=N) 발화, job은 CREATED
# ----------------------------------------------------------------------
def test_gate_reject_default_emits_finished(qtbot, manager, fake_lsf):
    log = []
    _record(manager, log)

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a", "echo b"],
                            auto_poll=False, pre_submit=lambda c: False)
    kinds = [e[0] for e in log]
    assert "submit_started" not in kinds          # 게이트 미통과 → 제출 시작 안 함
    assert log[0][0] == "pre_submit_started"
    assert log[1] == ("pre_submit_finished", False)
    fin = [e for e in log if e[0] == "submit_finished"][0]
    assert fin == ("submit_finished", 0, 2, 0)    # cancelled=2
    # 레코드 미생성 → 요약은 N CREATED
    assert manager.summary(js.id)["total"] == 2
    assert manager.summary(js.id).get("CREATED") == 2
    assert fake_lsf.calls_of("bsub") == []        # 실제 제출 안 됨


# ----------------------------------------------------------------------
# 거부 (False) + 옵션 off — submit_finished 미발화, pre_submit_finished(False)만
# ----------------------------------------------------------------------
def test_gate_reject_option_suppresses_finished(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        submit_finished_on_gate_reject=False)
    try:
        log = []
        _record(mgr, log)
        with qtbot.waitSignal(mgr.pre_submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a"], auto_poll=False,
                           pre_submit=lambda c: False)
        qtbot.wait(150)
        kinds = [e[0] for e in log]
        assert kinds == ["pre_submit_started", "pre_submit_finished"]   # finished 없음
        assert "submit_finished" not in kinds
        assert fake_lsf.calls_of("bsub") == []
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 예외 — 옵션과 무관하게 error + submit_finished(failed=N), job SUBMIT_FAILED
# ----------------------------------------------------------------------
def test_gate_exception_always_reports(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        submit_finished_on_gate_reject=False)   # off여도 예외는 보고
    try:
        log, errs = [], []
        _record(mgr, log)
        mgr.error_occurred.connect(lambda j, m: errs.append(m))

        def boom(cmds):
            raise RuntimeError("전처리 실패!")

        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a", "echo b"],
                           auto_poll=False, pre_submit=boom)
        assert log[1] == ("pre_submit_finished", False)
        fin = [e for e in log if e[0] == "submit_finished"][0]
        assert fin == ("submit_finished", 0, 0, 2)    # failed=2
        assert any("전처리 실패" in m for m in errs)
        recs = js.jobs()
        # v9: 게이트 예외도 레코드 원상 유지 — 리셋 이전에 검사하므로
        # CREATED 그대로 남고, 실패는 finished(failed=N)+error로만 보고
        assert all(r.state is JobState.CREATED for r in recs)
        assert all(r.fail_reason is None for r in recs)
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# wrapper 경로 — 게이트가 argv 조합 문자열을 받음
# ----------------------------------------------------------------------
def test_gate_wrapper_path(qtbot, manager, fake_lsf):
    got = {}

    def gate(cmds):
        got["cmds"] = list(cmds)
        return True

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, 
            ["customwrapper_sub -i a.sp", ["customwrapper_sub", "-q", "long", "tb.v"]],
            auto_poll=False, pre_submit=gate, wrapper=True)
    assert got["cmds"] == ["customwrapper_sub -i a.sp", "customwrapper_sub -q long tb.v"]
    assert all(r.state is JobState.PEND for r in js.jobs())


def test_gate_wrapper_reject(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blocker:
        js = submit_cmds(manager, ["customwrapper_sub -i a.sp"], wrapper=True, auto_poll=False,
                                    pre_submit=lambda c: False)
    assert blocker.args[1].cancelled == 1
    assert fake_lsf.calls_of("customwrapper_sub") == []


# ----------------------------------------------------------------------
# array 경로 — 게이트 통과 후 array 제출
# ----------------------------------------------------------------------
def test_gate_array_pass(qtbot, manager, fake_lsf):
    got = {}
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, "run_task", count=3, auto_poll=False,
                            pre_submit=lambda c: got.setdefault("c", c) or True)
    assert all(r.state is JobState.PEND for r in js.jobs())
    assert len(js.jobs()) == 3


def test_gate_array_reject(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, "run_task", count=3, auto_poll=False,
                            pre_submit=lambda c: False)
    assert manager.summary(js.id).get("CREATED") == 3
    assert fake_lsf.calls_of("bsub") == []


# ----------------------------------------------------------------------
# 핸들 신호 — js.pre_submit_started / js.pre_submit_finished 중계
# ----------------------------------------------------------------------
def test_handle_ready_signals(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, ["echo a"], auto_poll=False,
                        pre_submit=lambda c: True)
    got = []
    js.pre_submit_finished.connect(lambda ok: got.append(ok))
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert got == [True]


# ----------------------------------------------------------------------
# pre_submit 없으면 기존 동작 그대로 (ready 신호 없음)
# ----------------------------------------------------------------------
def test_no_gate_no_ready_signals(qtbot, manager, fake_lsf):
    log = []
    _record(manager, log)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, ["echo a"], auto_poll=False)
    kinds = [e[0] for e in log]
    assert "pre_submit_started" not in kinds
    assert kinds[0] == "submit_started"


# ----------------------------------------------------------------------
# auto_poll — 게이트 통과 후에만 polling 시작 (거부 시 미시작)
# ----------------------------------------------------------------------
def test_gate_autopoll_deferred_until_pass(qtbot, manager, fake_lsf):
    """게이트 통과 시 미뤄둔 auto-poll이 시작되고, 이후 query가 RUN을 반영."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=True,
                            pre_submit=lambda c: True)
    qtbot.wait(50)
    assert js.id in manager._poll_intervals          # 통과 후 start_polling됨
    assert js.id not in manager._pending_autopoll     # pending 소진
    # 폴링 워커 경유 1회 조회로 RUN 반영 확인
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(js.id)
    assert js.jobs()[0].state is JobState.RUN
    manager.stop_polling(js)


def test_gate_reject_no_autopoll(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=True,
                            pre_submit=lambda c: False)
    qtbot.wait(100)
    # 거부됐으므로 polling이 켜지지 않아 pending/interval 모두 비어야 함
    assert js.id not in manager._pending_autopoll
    assert js.id not in manager._poll_intervals


# ----------------------------------------------------------------------
# 게이트 통과 후 do_launch(레코드 생성)가 store 장애로 실패 → finished 보장
# (미방어 시 게이트 워커가 죽어 submit_finished 미발화 → jobset 잠김)
# ----------------------------------------------------------------------
def test_gate_do_launch_failure_still_finishes(qtbot, manager, fake_lsf):
    """게이트 통과 후 착수(리셋) 단계의 store 장애 — jobset이 잠기지 않고
    finished가 반드시 발화돼야 한다 (v9: 리셋 실패 키는 건너뛰고 취소 계상)."""
    def gate(cmds):
        def boom(*a, **k):
            raise RuntimeError("store down")
        manager.store.transition = boom          # do_launch의 리셋 사보타주
        return True
    with qtbot.waitSignal(manager.submit_finished, timeout=5000) as b:
        js = submit_cmds(manager, ["echo a"], auto_poll=False,
                            pre_submit=gate)
    rpt = b.args[1]
    assert rpt.succeeded == 0 and rpt.cancelled == 1   # 잠기지 않고 마무리
    assert not manager.submitter.is_active(js.id)


# ----------------------------------------------------------------------
# 게이트 실행 중 shutdown → 통과해도 제출 안 함 (좀비 없음)
# ----------------------------------------------------------------------
def test_gate_shutdown_during_callback(qtbot, fake_lsf, config):
    import threading
    import time
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    started, release = threading.Event(), threading.Event()

    def slow(cmds):
        started.set()
        release.wait(5)
        return True

    js = submit_cmds(mgr, ["echo a"], auto_poll=False, pre_submit=slow)
    assert started.wait(3)
    # shutdown이 취소 플래그를 세운 뒤 게이트를 풀어준다 — 통과 직후
    # 재확인(cancel/shutdown 체크)에서 반드시 걸리게 (결정적 순서)
    t = threading.Thread(target=mgr.shutdown)
    t.start()
    for _ in range(300):
        if mgr.submitter._shutdown:
            break
        time.sleep(0.01)
    release.set()
    t.join(10)
    assert not t.is_alive(), "shutdown이 게이트 대기에서 행"
    assert fake_lsf.calls_of("bsub") == []
