"""전체 정독 리뷰 사이클 3에서 확정된 항목의 회귀 테스트.

array element 개별 kill의 verify가 parent job_id로 형제 element를 잔존으로
오집계하던 버그 — target 문자열(id[idx]) 기준 매칭으로 수정.
"""
from __future__ import annotations

from lsfmgr import JobRecord, JobState


def _array_jobset(manager, fake_lsf, n=10):
    from tests.fake_lsf import FakeJob

    js = manager.create_jobset(intended_count=n)
    jsid, parent = js.id, 9500
    manager.store.store_add_jobs([JobRecord(
        job_id=parent, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]", state=JobState.RUN, command="r")
        for i in range(n)])
    for i in range(n):
        fake_lsf.jobs[f"{parent}[{i}]"] = FakeJob(
            job_id=parent, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat="RUN")
    return js, parent


# ----------------------------------------------------------------------
# C3-1: 단일 element verify — 형제 element를 잔존으로 세지 않는다
# ----------------------------------------------------------------------
def test_verify_single_element_no_sibling_still_alive(qtbot, manager, fake_lsf):
    js, _parent = _array_jobset(manager, fake_lsf, n=10)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill_jobs(js, [f"{js.id}[3]"], verify=True)

    report = blk.args[1]
    # element 3만 죽었고 verify는 그 element만 본다 — 형제 9개는 대상 아님
    assert report.still_alive == 0, f"형제 오집계: still_alive={report.still_alive}"
    alive_idx = sorted(j.array_index for j in fake_lsf.alive_jobs())
    assert alive_idx == [0, 1, 2, 4, 5, 6, 7, 8, 9]


# ----------------------------------------------------------------------
# C3-2: _verify 단위 — bare id는 job_id 전체, element 지정은 (id,idx) 정확
# ----------------------------------------------------------------------
def test_verify_target_matching_unit(qtbot, manager, fake_lsf):
    js, parent = _array_jobset(manager, fake_lsf, n=4)   # 전원 RUN, store 등록
    from lsfmgr.killer import _KillTask

    t = _KillTask(manager.killer, jobset_id=js.id)
    # element 지정 — 그 element(1개)만
    assert t._verify({f"{parent}[1]"})[0] == 1
    assert t._verify({f"{parent}[1]", f"{parent}[3]"})[0] == 2
    # bare parent id — 같은 job_id 전 element(4개)
    assert t._verify({str(parent)})[0] == 4
    # 대상 없음
    assert t._verify(set())[0] == 0


# ----------------------------------------------------------------------
# C3-3: 부분 kill(only_state) verify도 대상 element만
# ----------------------------------------------------------------------
def test_verify_partial_kill_counts_only_targeted(qtbot, manager, fake_lsf):
    from tests.fake_lsf import FakeJob

    js = manager.create_jobset(intended_count=4)
    jsid, parent = js.id, 9600
    states = {0: JobState.PEND, 1: JobState.RUN,
              2: JobState.PEND, 3: JobState.RUN}
    manager.store.store_add_jobs([JobRecord(
        job_id=parent, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]", state=st, command="r")
        for i, st in states.items()])
    for i, st in states.items():
        fake_lsf.jobs[f"{parent}[{i}]"] = FakeJob(
            job_id=parent, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat=st.value)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill(js, only_state=JobState.PEND, verify=True)

    report = blk.args[1]
    # PEND(0,2)만 대상 — RUN(1,3)은 verify 집계 대상 아님
    assert report.still_alive == 0
    alive_idx = sorted(j.array_index for j in fake_lsf.alive_jobs())
    assert alive_idx == [1, 3]                    # RUN 생존(대상 아님)


# ======================================================================
# 사이클 3 재실행 — 사이클 2 리워크의 2차 결함 (do_launch/gate/post_process)
# ======================================================================
import threading



# ----------------------------------------------------------------------
# C3-4 (fix 3): 제출 시점에 전원 terminal(전량 bsub 실패)이어도 post_process
#               가 유실되지 않고 발화한다 — submit_finished 시점 재확인
# ----------------------------------------------------------------------
def test_post_process_fires_when_all_terminal_at_submit(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 100                # 전 job bsub 실패
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    got = {}
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000) as blk:
        manager.submit(js, post_process=lambda recs: {
            "failed": sum(1 for r in recs
                          if r.state.name == "SUBMIT_FAILED")},
            max_retry=0, auto_poll=True)
    assert blk.args[0] == {"failed": 2}          # 전원 SUBMIT_FAILED → 후처리 1회


# ----------------------------------------------------------------------
# C3-5 (fix 1/5): 게이트 예외 → jobset 잠기지 않고 _pending_arm도 정리된다
# ----------------------------------------------------------------------
def test_gate_exception_unlocks_and_clears_pending(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)

    def boom(cmds):
        raise RuntimeError("gate blew up")

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=boom,
                       post_process=lambda r: 1, auto_poll=False)
    qtbot.wait(50)
    # 예외 후: ctx 미완 고착 없음(재제출 가능), 보류 무장분 정리됨
    assert not manager.submitter.is_active(js.id)
    assert js.id not in manager._pending_arm
    assert js.id not in manager._post_process
    # 실제로 다시 제출 가능해야 한다 (잠기지 않음)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    assert js.jobs()[0].state.name == "PEND"


# ----------------------------------------------------------------------
# C3-6 (fix 1): 비게이트 do_launch 예외(store 리셋 실패) → 잠기지 않고
#               SUBMITTING 고착 없이 SUBMIT_FAILED로 마무리
# ----------------------------------------------------------------------
def test_nongate_launch_failure_finalizes(qtbot, manager, fake_lsf, monkeypatch):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)

    # do_launch 내부에서 pool.start가 죽는 상황을 흉내 — 전 job 리셋 후 예외
    def boom(*a, **k):
        raise RuntimeError("pool spawn failed")

    monkeypatch.setattr(manager.submitter, "_make_resubmit_task", boom)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, auto_poll=False)
    assert blk.args[1].failed == 1               # 실패로 마무리 (미완 고착 아님)
    assert not manager.submitter.is_active(js.id)
    # SUBMITTING 고착 없이 SUBMIT_FAILED (재제출 가드에 안 걸림)
    assert js.jobs()[0].state.name == "SUBMIT_FAILED"
    monkeypatch.undo()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)      # 다시 제출 가능
    assert js.jobs()[0].state.name == "PEND"


# ----------------------------------------------------------------------
# C3-7 (fix 4): 게이트(제출) 진행 중 close(force=True) — 예외 없이 종결되고
#               bgdel이 제출 정지 후 수행된다 (고아 방지)
# ----------------------------------------------------------------------
def test_force_close_during_active_submit_quiesces(qtbot, fake_lsf, config):
    from lsfmgr import InMemoryStore, LsfJobManager

    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake_lsf.set_all("DONE", 0)
        mgr.querier.query(js.id)
        jsid = js.id

        gate_entered, release = threading.Event(), threading.Event()

        def slow_gate(cmds):
            gate_entered.set()
            release.wait(5)
            return True

        mgr.submit(js, pre_submit=slow_gate, auto_poll=False)
        assert gate_entered.wait(3)
        mgr.close(js, force=True)                # 진행 중 강제 종결 — 예외 없이
        assert mgr.store.get_jobset(jsid).closed is True
        release.set()
        qtbot.wait(300)                          # quiesce+bgdel worker 완료 대기
    finally:
        mgr.shutdown()
