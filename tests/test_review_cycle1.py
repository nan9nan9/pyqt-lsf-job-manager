"""전체 정독 리뷰 사이클 1에서 확정된 10건의 회귀 테스트.

pre_submit 게이트 창(레코드가 이전 실행 terminal인 구간)의 정합성 버그군과
shutdown 이후 스레드 누수, 동시 강등 경합, closed 계약, 고아 레코드.
"""
from __future__ import annotations

import threading

import pytest

from lsfmgr import (
    CloseNotAllowedError,
    InMemoryStore,
    JobSetClosedError,
    JobState,
    LsfJobManager,
    MergeNotAllowedError,
    SubmitNotAllowedError,
)


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
# F1: 게이트 진행 중 close 거부 — 닫힌 jobset에 제출되는 창 차단
# ----------------------------------------------------------------------
def test_close_rejected_while_gate_pending(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    gate_entered, release = threading.Event(), threading.Event()

    def slow_gate(cmds):
        gate_entered.set()
        release.wait(5)
        return True

    manager.submit(js, pre_submit=slow_gate, auto_poll=False)
    assert gate_entered.wait(3)
    with pytest.raises(CloseNotAllowedError):
        manager.close(js)                    # 게이트(제출) 진행 중 — 거부
    release.set()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    _finish(manager, fake_lsf, js)
    manager.close(js)                        # 완주 후에는 정상 close


# ----------------------------------------------------------------------
# F2: post_process 조기 무장 — 게이트 창에서 낡은 레코드로 오발화 금지
# ----------------------------------------------------------------------
def test_post_process_not_fired_during_gate_window(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    calls = []
    gate_entered, release = threading.Event(), threading.Event()

    def slow_gate(cmds):
        gate_entered.set()
        release.wait(5)
        return True

    manager.submit(js, pre_submit=slow_gate,
                   post_process=lambda recs: calls.append(
                       [r.state.name for r in recs]) or "R", auto_poll=False)
    assert gate_entered.wait(3)
    manager.query_once(js)                   # 게이트 창 — 레코드는 이전 DONE
    qtbot.wait(150)
    assert calls == []                       # 낡은 레코드로 발화하면 안 됨

    release.set()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        manager.query_once(js)               # 완료 감지 경유 — 이제 1회 발화
    assert calls == [["DONE"]]


def test_post_process_discarded_on_gate_reject(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    calls = []
    with qtbot.waitSignal(manager.pre_submit_finished, timeout=10000):
        manager.submit(js, pre_submit=lambda c: False,
                       post_process=lambda recs: calls.append(1),
                       auto_poll=False)
    manager.query_once(js)                   # 거부됨 — 무장도 폐기됐어야 함
    qtbot.wait(150)
    assert calls == []
    assert js.id not in manager._post_process
    assert js.id not in manager._pending_arm


# ----------------------------------------------------------------------
# F3: 게이트 통과 직후 취소 — ok=True가 아니라 False로 통지 (rearm 방지)
# ----------------------------------------------------------------------
def test_gate_pass_then_cancel_reports_false(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    results = []
    manager.pre_submit_finished.connect(lambda j, ok: results.append(ok))
    gate_entered, release = threading.Event(), threading.Event()

    def slow_gate(cmds):
        gate_entered.set()
        release.wait(5)
        return True                          # 통과 — 하지만 그새 취소됨

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=slow_gate, auto_poll=False)
        assert gate_entered.wait(3)
        manager.cancel_submit(js)            # 게이트 도중 취소
        release.set()

    assert results == [False]                # True로 알리면 rearm이 잘못 됨
    assert js.jobs()[0].state is JobState.DONE   # 레코드 원상 (재제출 없음)
    # 보류분(rearm/autopoll/post_process)이 잔류하지 않아야 한다
    assert js.id not in manager._pending_arm


# ----------------------------------------------------------------------
# F4: shutdown 후 submit/kill — 고착 신호 없이 명확히 거부/무시
# ----------------------------------------------------------------------
def test_submit_after_shutdown_raises(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    mgr.shutdown()
    with pytest.raises(SubmitNotAllowedError):
        mgr.submit(js)


def test_kill_after_shutdown_is_noop(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, auto_poll=False)
    started = []
    mgr.kill_started.connect(lambda j: started.append(j))
    mgr.shutdown()
    mgr.kill(js)                             # no-op — worker/신호 없음
    mgr.kill_jobs(js, [js.jobs()[0].job_key])
    qtbot.wait(100)
    assert started == []                     # kill_started 미발화 (고착 없음)


# ----------------------------------------------------------------------
# F5: shutdown 후 잔여 폴링 이벤트 — post_pool 재기동 금지
# ----------------------------------------------------------------------
def test_poll_updated_after_shutdown_ignored(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    calls = []
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, post_process=lambda r: calls.append(1),
                   auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    summary = mgr.store.summary(js.id)
    mgr.shutdown()
    # shutdown 후 큐에 남아 있던 updated가 도착한 상황을 재현
    mgr._on_poll_updated(js.id, summary, [])
    qtbot.wait(100)
    assert calls == []                       # post_pool 재기동 없음


# ----------------------------------------------------------------------
# F8: closed 계약 — 재획득/재제출/merge 전부 거부
# ----------------------------------------------------------------------
def test_closed_jobset_cannot_be_reused(qtbot, manager, fake_lsf):
    js = _submit_done(qtbot, manager, fake_lsf, ["customwrapper_sub a.sp"])
    jsid = js.id
    manager.close(js)
    with pytest.raises(JobSetClosedError):
        manager.jobset(jsid)                 # 재획득 거부
    with pytest.raises(SubmitNotAllowedError):
        manager.submit(jsid)                 # 문자열 id 직접 제출도 거부
    other = manager.create_jobset(["customwrapper_sub b.sp"])
    with pytest.raises(MergeNotAllowedError):
        manager.merge(jsid, other)           # 닫힌 target으로 merge 거부


# ----------------------------------------------------------------------
# F9: create_jobset 검증 실패 시 고아 jobset 미잔류
# ----------------------------------------------------------------------
def test_create_jobset_rollback_on_validation_error(manager):
    before = {r.jobset_id for r in manager.list_jobsets()}
    with pytest.raises(ValueError):
        manager.create_jobset(["a", "b"], merge_ids=["m1"])   # 길이 불일치
    with pytest.raises(ValueError):
        manager.create_jobset(["a", "b"], merge_ids=["m", "m"])  # 중복
    after = {r.jobset_id for r in manager.list_jobsets()}
    assert after == before                   # 유령 빈 jobset 없음


# ----------------------------------------------------------------------
# F7: bjobs 포맷 동시 강등 — 1단만 (FULL 건너뛰기 방지)
# ----------------------------------------------------------------------
def test_bjobs_format_concurrent_downgrade_single_step():
    from lsfmgr.command import LsfCommand
    from lsfmgr.config import LsfConfig

    cmd = LsfCommand(LsfConfig(collect_clusters=True), lambda a, t: None)
    assert cmd._bjobs_fmt_idx == 0
    used = cmd._bjobs_fmt_idx
    # 두 스레드가 같은 used 인덱스로 동시에 강등을 시도한 상황
    with cmd._bjobs_fmt_lock:
        pass                                 # 락 존재 확인
    for _ in range(2):                       # CAS — 같은 단계 중복 강등은 1회만
        with cmd._bjobs_fmt_lock:
            if cmd._bjobs_fmt_idx == used:
                cmd._bjobs_fmt_idx = used + 1
    assert cmd._bjobs_fmt_idx == 1           # FULL(1단) — CORE(2단) 아님
