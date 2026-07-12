"""전체 정독 리뷰 사이클 2에서 확정된 6건의 회귀 테스트.

사이클 1 수정이 만든 2차 결함: rearm↔리셋 경합(records_reset 시점 무장),
낡은 게이트 신호의 보류분 파괴(사이클 token), 술어-명령 불일치(closed),
close force 계약, script_dir 하위 호환, kill finished-last.
"""
from __future__ import annotations

import threading

import pytest

from lsfmgr import InMemoryStore, JobState, LsfJobManager


def _finish(manager, fake_lsf, js, state="DONE", code=0):
    fake_lsf.set_all(state, code)
    manager.querier.query(js.id)


def _submit_done(qtbot, manager, fake_lsf, cmds):
    js = manager.create_jobset(cmds)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish(manager, fake_lsf, js)
    return js


# ----------------------------------------------------------------------
# C2-1: rearm/무장은 레코드 리셋 완료(records_reset) 후 — 게이트 통과 시에도
#       end 핸들러가 옛 terminal 레코드로 중복 발화하지 않는다
# ----------------------------------------------------------------------
def test_gate_resubmit_no_duplicate_final_handler(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    finals = []
    manager.add_handler(js, "h", lambda ctx: "x",
                        start_states={JobState.RUN},
                        end_states={JobState.DONE})
    manager.handler_finished.connect(
        lambda j, n, res: finals.append(res.final) if res.final else None)

    # 1차 실행 완료분의 final을 폴링으로 소진
    manager.start_polling(js, 5.0)
    qtbot.waitUntil(lambda: len(finals) == 1, timeout=10000)
    manager.stop_polling(js)

    # 게이트 재제출 — 통과 시 rearm은 리셋 완료 후(records_reset)라, 그 사이
    # 폴링 tick이 와도 옛 DONE 레코드에 final이 중복 발화하지 않는다
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=lambda c: True, auto_poll=False)
    manager.query_once(js)                       # 리셋 직후 tick 재현
    qtbot.wait(150)
    assert finals == [True]                      # 아직 1회뿐 (중복 없음)

    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(js)                   # 새 실행 완료 → final 2회째
    qtbot.waitUntil(lambda: len(finals) == 2, timeout=10000)


# ----------------------------------------------------------------------
# C2-2: 낡은 게이트 신호(token 불일치)는 새 사이클의 보류분을 파괴하지 못한다
# ----------------------------------------------------------------------
def test_stale_gate_rejected_does_not_destroy_new_cycle(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    gate_entered, release = threading.Event(), threading.Event()

    def slow_gate(cmds):
        gate_entered.set()
        release.wait(5)
        return True

    manager.submit(js, pre_submit=slow_gate,
                   post_process=lambda r: "R", auto_poll=False)
    assert gate_entered.wait(3)
    assert js.id in manager._pending_arm
    # 이전 사이클의 낡은 거부 신호가 늦게 배달된 상황 — token이 다르므로 무시
    manager.submitter.gate_rejected.emit(js.id, object())
    qtbot.wait(50)
    assert js.id in manager._pending_arm         # 보류분 생존

    release.set()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    assert js.id in manager._post_process        # 무장 정상 승격
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        manager.query_once(js)                   # 후처리 정상 발화


# ----------------------------------------------------------------------
# C2-3: closed jobset — can_submit/can_merge도 False (술어-명령 일치)
# ----------------------------------------------------------------------
def test_predicates_false_for_closed_jobset(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    jsid = js.id
    other = manager.create_jobset(["customwrapper_sub b.sp"])
    manager.close(js)
    assert manager.can_submit(jsid) is False     # 명령은 예외 — 술어는 False
    assert manager.can_merge(jsid, other) is False
    assert manager.can_merge(other, jsid) is False


# ----------------------------------------------------------------------
# C2-4: 진행 중(게이트 대기)에도 close(force=True)는 강제 종결 — 제출은 취소됨
# ----------------------------------------------------------------------
def test_force_close_during_gate_cancels_submit(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    jsid = js.id
    n_lsf = len(fake_lsf.jobs)
    gate_entered, release = threading.Event(), threading.Event()

    def slow_gate(cmds):
        gate_entered.set()
        release.wait(5)
        return True

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=slow_gate, auto_poll=False)
        assert gate_entered.wait(3)
        manager.close(js, force=True)            # 강제 종결 — 제출 취소 예약
        release.set()

    assert manager.store.get_jobset(jsid).closed is True
    assert len(fake_lsf.jobs) == n_lsf           # 닫힌 jobset에 새 제출 없음


# ----------------------------------------------------------------------
# C2-5: 제거된 script_dir 옵션 — TypeError 대신 경고 후 무시 (하위 호환)
# ----------------------------------------------------------------------
def test_removed_script_dir_option_ignored(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        script_dir="/scratch/lsf")     # 구버전 앱 코드
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)            # 정상 기동·동작
        assert js.jobs()[0].state is JobState.PEND
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# C2-6: kill finished-last — 핸들 kill_finished 시점에 EXIT 전이분이 이미 도착
# ----------------------------------------------------------------------
def test_kill_finished_arrives_after_exit_updates(qtbot, manager, fake_lsf):
    js = manager.create_jobset([f"customwrapper_sub r{i}.sp" for i in range(4)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    seen_exit = []                                # kill_finished 수신 '이전'의
    got = {}                                      # EXIT jobs_updated 기록

    js.jobs_updated.connect(
        lambda recs: seen_exit.extend(
            r.job_key for r in recs if r.state is JobState.EXIT))
    js.kill_finished.connect(
        lambda rep: got.setdefault("exit_at_finish", list(seen_exit)))

    with qtbot.waitSignal(js.kill_finished, timeout=10000):
        manager.kill(js)

    # finished-last: 핸들 kill_finished가 왔을 때 EXIT 전이 배치는 이미 수신됨
    assert len(got["exit_at_finish"]) == 4
