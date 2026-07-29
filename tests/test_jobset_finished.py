"""jobset_finished — jobset의 전 job이 terminal에 도달하면 1회 발화.

post_process 등록과 무관하게 LSF job 상태만 보고 판정한다(전원 terminal이면
성공/실패 혼재 무관). 재제출로 다시 활성이 되면 재무장돼 다음 완료에 또 발화.
"""
from __future__ import annotations


def _finish(manager, fake_lsf, js, state="DONE", code=0):
    fake_lsf.set_all(state, code)
    manager.query_once(js)          # 완료 감지 지점


# ----------------------------------------------------------------------
# 기본 — post_process 없이도 전원 terminal 시 발화, 인자는 최종 요약
# ----------------------------------------------------------------------
def test_jobset_finished_without_post_process(qtbot, manager, fake_lsf):
    js = manager.create_jobset([f"customwrapper_sub r{i}.sp" for i in range(3)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    with qtbot.waitSignal(js.jobset_finished, timeout=10000) as blk:
        _finish(manager, fake_lsf, js)

    assert blk.args[0]["total"] == 3
    assert blk.args[0]["DONE"] == 3
    assert js.is_done


# ----------------------------------------------------------------------
# 전역 계층도 같은 이벤트를 jobset_id와 함께 발행
# ----------------------------------------------------------------------
def test_manager_signal_carries_jobset_id(qtbot, manager, fake_lsf):
    seen = []
    manager.jobset_finished.connect(lambda j, s: seen.append((j, s)))

    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(manager.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, js)

    assert [j for j, _ in seen] == [js.id]


# ----------------------------------------------------------------------
# 실패 혼재여도 전원 terminal이면 발화 (성공 여부와 무관)
# ----------------------------------------------------------------------
def test_fires_on_mixed_terminal(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)
    fake_lsf.set_job(recs[1].job_id, "EXIT", 1)
    with qtbot.waitSignal(js.jobset_finished, timeout=10000) as blk:
        manager.query_once(js)

    assert blk.args[0]["DONE"] == 1 and blk.args[0]["EXIT"] == 1


# ----------------------------------------------------------------------
# 일부만 종료면 발화 없음
# ----------------------------------------------------------------------
def test_not_fired_while_active(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)
    fake_lsf.set_job(recs[1].job_id, "RUN")
    manager.query_once(js)
    qtbot.wait(100)
    assert fired == []


# ----------------------------------------------------------------------
# 완료 상태에서 폴링이 더 돌아도 재발화 없음 (1회 latch)
# ----------------------------------------------------------------------
def test_fires_once(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    with qtbot.waitSignal(js.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    manager.query_once(js)
    manager.query_once(js)
    qtbot.wait(150)
    assert len(fired) == 1


# ----------------------------------------------------------------------
# 재제출하면 재무장 — 다음 완료에 다시 발화
# ----------------------------------------------------------------------
def test_rearms_on_resubmit(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(js.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    assert len(fired) == 1

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(js.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    assert len(fired) == 2


# ----------------------------------------------------------------------
# post_process와 함께 쓰면 jobset_finished가 먼저
# ----------------------------------------------------------------------
def test_order_before_post_processing(qtbot, manager, fake_lsf):
    order = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    js.jobset_finished.connect(lambda s: order.append("finished"))
    js.post_processing_started.connect(lambda: order.append("post_started"))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: len(r), auto_poll=False)

    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        _finish(manager, fake_lsf, js)

    assert order == ["finished", "post_started"]


# ----------------------------------------------------------------------
# job이 없는 빈 jobset은 발화 없음 ("완료할 것"이 없음)
# ----------------------------------------------------------------------
def test_empty_jobset_never_fires(qtbot, manager, fake_lsf):
    fired = []
    manager.jobset_finished.connect(lambda j, s: fired.append(j))
    js = manager.create_jobset([])
    manager.query_once(js)
    qtbot.wait(150)
    assert fired == []


# ----------------------------------------------------------------------
# 제출 자체가 전량 실패해 폴링 없이 곧장 terminal이 된 경우에도 발화
# ----------------------------------------------------------------------
def test_fires_when_all_submit_failed(qtbot, manager, fake_lsf):
    fired = []
    manager.jobset_finished.connect(lambda j, s: fired.append(s))
    fake_lsf.fail_next_bsub = 1                 # 유일한 job의 제출이 거부됨
    js = manager.create_jobset(["customwrapper_sub a.sp"])

    with qtbot.waitSignal(js.jobset_finished, timeout=10000) as blk:
        manager.submit(js, auto_poll=False, max_retry=0)

    assert blk.args[0]["SUBMIT_FAILED"] == 1    # 폴링 없이 submit_finished에서
    assert len(fired) == 1
