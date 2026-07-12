"""전체 정독 리뷰 사이클 5에서 확정된 회귀 테스트.

사이클 4 수정의 4차 결함: _gate_fail un-stick 루프의 부분 실패 격리,
shutdown 경합의 started/finished 짝(kill/submit), kill verify의 array_index=None
레코드 매칭, 죽은 import.
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, JobRecord, JobState, LsfJobManager


def _finish(manager, fake_lsf, js, state="DONE", code=0):
    fake_lsf.set_all(state, code)
    manager.querier.query(js.id)


# ----------------------------------------------------------------------
# C5-1: _gate_fail un-stick 루프가 한 key의 예외에 통째로 죽지 않고 나머지
#       SUBMITTING 레코드를 전부 SUBMIT_FAILED로 정리 (jobset 안 잠김)
# ----------------------------------------------------------------------
def test_gate_fail_unstick_isolates_per_key(qtbot, manager, fake_lsf, monkeypatch):
    js = manager.create_jobset([f"customwrapper_sub r{i}.sp" for i in range(3)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish(manager, fake_lsf, js)

    # do_launch를 강제 실패시켜 _gate_fail로 진입 (task 생성 실패)
    def boom(*a, **k):
        raise RuntimeError("build fail")
    monkeypatch.setattr(manager.submitter, "_make_resubmit_task", boom)

    # store.transition이 특정 key에서만 예외 — 나머지는 정상 정리돼야 함
    real_tr = manager.store.transition
    keys = [r.job_key for r in js.jobs()]
    bad_key = keys[1]

    def flaky_tr(jsid, key, state, **kw):
        if key == bad_key and state is JobState.SUBMIT_FAILED:
            from lsfmgr.errors import JobNotFoundError
            raise JobNotFoundError(f"{jsid}/{key}")
        return real_tr(jsid, key, state, **kw)
    monkeypatch.setattr(manager.store, "transition", flaky_tr)

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    qtbot.wait(30)
    monkeypatch.undo()
    # bad_key만 SUBMITTING 잔류 가능하나, 나머지는 SUBMIT_FAILED로 정리돼야 함
    states = {r.job_key: r.state for r in js.jobs()}
    others = [k for k in keys if k != bad_key]
    assert all(states[k] is JobState.SUBMIT_FAILED for k in others), states
    assert not manager.submitter.is_active(js.id)


# ----------------------------------------------------------------------
# C5-2 (shutdown 짝): shutdown 후 submit은 예외 — started 고아 없음
#       (started는 착수 지점에서만 발화되므로 shutdown 경로에선 안 나감)
# ----------------------------------------------------------------------
def test_submit_started_only_at_launch(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    starts = []
    mgr.submit_started.connect(lambda j: starts.append(j))
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, auto_poll=False)
    assert starts == [js.id]                        # 정상 착수 1회
    mgr.shutdown()
    from lsfmgr import SubmitNotAllowedError
    import pytest
    with pytest.raises(SubmitNotAllowedError):
        mgr.submit(js)                              # shutdown 후 — 예외
    assert starts == [js.id]                        # started 추가 발화 없음


# ----------------------------------------------------------------------
# C5-3 (shutdown 짝): shutdown 후 kill은 kill_started 미발화 (finished 없음)
# ----------------------------------------------------------------------
def test_kill_started_only_when_queued(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, auto_poll=False)
    starts = []
    mgr.kill_started.connect(lambda j: starts.append(j))
    mgr.killer._shutdown = True                     # killer만 shutdown 상태로
    mgr.kill(js)                                    # no-op — started 안 나감
    mgr.kill_jobs(js, [js.jobs()[0].job_key])
    qtbot.wait(50)
    assert starts == []                            # kill_started 미발화
    mgr.killer._shutdown = False
    mgr.shutdown()


# ----------------------------------------------------------------------
# C5-4 (kill verify): array_index=None 레코드는 element target으로 판정하지
#       않는다 — 집계/비array 레코드를 특정 element로 잔존 집계하면 형제를
#       과대집계한다(사이클 6에서 되돌림). 전체 kill(bare id)만 집계.
#       (사이클 6 C6-1이 실제 회귀 시나리오를 추가로 검증)
# ----------------------------------------------------------------------
def test_verify_none_array_index_not_element_matched(qtbot, manager, fake_lsf):
    from lsfmgr.killer import _KillTask
    from tests.fake_lsf import FakeJob
    js = manager.create_jobset(intended_count=1)
    jsid = js.id
    # array_index=None인 RUN 레코드 (비array/collapsed)
    manager.store.store_add_jobs([JobRecord(
        job_id=500, array_index=None, jobset_id=jsid,
        lsf_job_name=f"{jsid}_0", state=JobState.RUN, command="r")])
    fake_lsf.jobs["500"] = FakeJob(
        job_id=500, array_index=None, name=f"{jsid}_0", group=None,
        queue="q", command="r", stat="RUN")   # verify 재조회에서 RUN 유지
    t = _KillTask(manager.killer, jobset_id=jsid)
    assert t._verify({"500[3]"})[0] == 0              # element target — 집계 레코드 판정 불가
    assert t._verify({"500[3-5]"})[0] == 0            # 범위 target — 동일
    assert t._verify({"500"})[0] == 1                 # 전체 kill(bare id)만 집계
    assert t._verify({"999[1]"})[0] == 0              # 다른 job은 안 셈
