"""전체 정독 리뷰 사이클 10에서 확정된 회귀 테스트.

동시성/lifecycle 각도. 사이클 8 lookup 캐시의 형제-staleness 회귀와
killer shutdown TOCTOU.

- C10-1: monitor lookup 캐시 — 형제 element가 by-id 재조회를 유발하면, 첫 probe로
  이미 resolved된 형제도 최신(by-id) 데이터를 반영해야 한다(stale 금지).
- C10-2: killer shutdown 후 kill은 task를 띄우지 않는다(체크+start 원자화로
  join 안 되는 worker 방지).
"""
from __future__ import annotations

from lsfmgr import JobRecord, JobState
from lsfmgr.command import JobStatus
from lsfmgr.states import JobSetRecord


# ----------------------------------------------------------------------
# C10-1: 형제 element가 유발한 by-id 재조회의 최신 상태를, 첫 probe로 resolved된
#        형제도 반영한다 (캐시가 stale probe값을 store에 쓰지 않는다)
# ----------------------------------------------------------------------
def test_sibling_refresh_not_stale_from_cache(qtbot, manager):
    jsid = "JS-STALE"
    # name probe가 도는 jobset (group/array 부착물 없음 → element1은 leftover)
    manager.store.store_insert_jobset(JobSetRecord(
        jobset_id=jsid, intended_count=2))
    manager.store.store_add_jobs([
        JobRecord(job_id=900, array_index=0, jobset_id=jsid,
                  job_key=f"{jsid}_0", state=JobState.RUN, command="r"),
        JobRecord(job_id=900, array_index=1, jobset_id=jsid,
                  job_key=f"{jsid}_1", state=JobState.RUN, command="r")])

    cmd = manager.querier.command
    # (v10: probe/leftover 2단이 사라져 원래의 'stale probe' 시나리오는 구조적으로
    # 불가능해졌다 — by-id 단일 조회 결과가 전 element 레코드에 반영되는지만 검증)
    def fake_by_ids(ids):
        if 900 in set(ids):
            return ([JobStatus(900, 0, JobState.DONE, 0, f"{jsid}_0"),
                     JobStatus(900, 1, JobState.DONE, 0, f"{jsid}_1")], set())
        return ([], set())
    cmd.bjobs_by_ids = fake_by_ids

    manager.querier.query(jsid)

    states = {r.array_index: r.state for r in manager.get_jobs(jsid)}
    assert states[0] is JobState.DONE, f"element0 미반영: {states}"
    assert states[1] is JobState.DONE


# ----------------------------------------------------------------------
# C10-2: shutdown 후 kill은 task를 띄우지 않고 False (join 안 되는 worker 방지)
# ----------------------------------------------------------------------
def test_kill_after_shutdown_starts_no_task(qtbot, fake_lsf, config):
    from lsfmgr import InMemoryStore, LsfJobManager
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    js = mgr.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, auto_poll=False)
    key = js.jobs()[0].job_key

    mgr.killer.shutdown()                       # killer만 shutdown (플래그+drain)
    # 체크+start가 원자화돼 shutdown 이후엔 task가 안 뜬다 → False 반환
    assert mgr.killer.kill_jobset(js.id) is False
    assert mgr.killer.kill_jobs([js.jobs()[0].job_id], jobset_id=js.id) is False
    assert mgr.killer._pool.activeThreadCount() == 0
    mgr.shutdown()
