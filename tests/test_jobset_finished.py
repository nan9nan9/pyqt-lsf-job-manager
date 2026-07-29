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
# 내가 건 kill로 끝났으면 통지 없음 — 사용자가 스스로 끝낸 완료
# ----------------------------------------------------------------------
def test_muted_when_user_killed_whole_jobset(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)
    assert js.summary["EXIT"] == 2          # 전원 EXIT (optimistic)
    manager.query_once(js)                  # 폴링이 돌아도
    qtbot.wait(200)
    assert fired == []                      # 통지 없음


# ----------------------------------------------------------------------
# kill로 끝나도 post_process는 실행 — 별개 계약(결과 수집)
# ----------------------------------------------------------------------
def test_post_process_still_runs_after_kill(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: len(r), auto_poll=False)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000) as blk:
        manager.query_once(js)

    assert blk.args[0] == 1
    assert fired == []                      # 후처리는 돌되 완료 통지는 없음


# ----------------------------------------------------------------------
# 부분 kill은 억제 대상이 아님 — 남은 job이 끝나면 통지된다
# ----------------------------------------------------------------------
def test_partial_kill_still_notifies(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    recs = js.jobs()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill_jobs(js, [recs[0].job_key])     # 1건만 kill
    qtbot.wait(100)
    assert fired == []                      # 아직 나머지가 살아 있다

    with qtbot.waitSignal(js.jobset_finished, timeout=10000) as blk:
        fake_lsf.set_job(recs[1].job_id, "DONE", 0)  # 나머지는 자연 종료
        manager.query_once(js)
    assert blk.args[0]["total"] == 2        # 내가 안 죽인 job의 완료는 통지


# ----------------------------------------------------------------------
# kill 억제 후 재제출하면 다음 완료는 정상 통지 (재무장)
# ----------------------------------------------------------------------
def test_rearms_after_killed_jobset_resubmitted(qtbot, manager, fake_lsf):
    fired = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    js.jobset_finished.connect(lambda s: fired.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)
    manager.query_once(js)
    qtbot.wait(150)
    assert fired == []

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(js.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    assert len(fired) == 1


# ----------------------------------------------------------------------
# 완료본 둘을 merge하면 재발화 없음 — 아무 job도 전이하지 않았다
# ----------------------------------------------------------------------
def test_merge_of_finished_jobsets_does_not_refire(qtbot, manager, fake_lsf):
    fired = []
    manager.jobset_finished.connect(lambda j, s: fired.append(j))

    a = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)
    with qtbot.waitSignal(a.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, a)

    b = manager.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(b, auto_poll=False)
    with qtbot.waitSignal(b.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, b)
    assert fired == [a.id, b.id]

    manager.merge(a, b)                 # 완료본 흡수 — 전이 없음
    manager.query_once(a)
    qtbot.wait(200)
    assert fired == [a.id, b.id]        # a 재발화 없음


# ----------------------------------------------------------------------
# merge로 **미완료** job이 들어오면 재무장 — 그 job이 끝날 때 다시 발화
# ----------------------------------------------------------------------
def test_merge_of_created_jobs_rearms(qtbot, manager, fake_lsf):
    fired = []
    manager.jobset_finished.connect(lambda j, s: fired.append(j))

    a = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)
    with qtbot.waitSignal(a.jobset_finished, timeout=10000):
        _finish(manager, fake_lsf, a)

    add = manager.create_jobset(["customwrapper_sub c.sp"], merge_ids=["c"])
    manager.merge(a, add)               # CREATED 1건 추가 — 다시 미완료
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)
    with qtbot.waitSignal(a.jobset_finished, timeout=10000) as blk:
        _finish(manager, fake_lsf, a)

    assert fired == [a.id, a.id]
    assert blk.args[0]["total"] == 2


# ----------------------------------------------------------------------
# jobset_finished slot이 그 자리에서 재제출해도 이번 실행의 post_process는 실행
# ----------------------------------------------------------------------
def test_post_process_survives_resubmit_from_slot(qtbot, manager, fake_lsf):
    calls = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: calls.append(len(r)),
                       auto_poll=False)

    # 완료 통지를 받자마자 재제출하는 GUI 패턴 (한 번만)
    manager.jobset_finished.connect(
        lambda j, s: manager.submit(js, auto_poll=False)
        if not calls and not manager.is_submitting(j) else None)

    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    assert calls == [1]                 # 도달한 완료의 후처리는 유실되지 않음


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
