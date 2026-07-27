"""전체 정독 리뷰 사이클 11에서 확정된 회귀 테스트.

사이클 10 refreshed_ids 재lookup의 2차 결함: folded(array_index=None) 레코드를
by-id 재조회 뒤 다시 집계할 때, collect()가 by_id에 append하면 stale(첫 probe)+
fresh(재조회) element 행이 섞여 _aggregate_elements가 옛 상태로 잘못 확정할 수
있다. by_id를 array_index별 최신값으로 dedup해 최신만 집계하도록 방어.
"""
from __future__ import annotations

from lsfmgr import JobRecord, JobState
from lsfmgr.command import JobStatus
from lsfmgr.states import JobSetRecord


# ----------------------------------------------------------------------
# C11-1: folded 레코드 재집계가 stale element 행을 섞지 않는다 (dedup)
#        — 형제 element가 유발한 by-id 재조회의 최신값으로 집계
# ----------------------------------------------------------------------
def test_folded_reaggregation_uses_latest_not_stale(qtbot, manager):
    jsid = "JS-DEDUP"
    manager.store.store_insert_jobset(JobSetRecord(
        jobset_id=jsid, intended_count=2))
    # folded 레코드 R=(910,None)와 형제 per-element S=(910,5) — 같은 job_id.
    manager.store.store_add_jobs([
        JobRecord(job_id=910, array_index=None, jobset_id=jsid,
                  job_key=f"{jsid}_r", state=JobState.RUN, command="r"),
        JobRecord(job_id=910, array_index=5, jobset_id=jsid,
                  job_key=f"{jsid}_5", state=JobState.RUN, command="r")])

    cmd = manager.querier.command
    # (v10: probe/leftover 2단이 사라져 원래의 'stale probe 행 혼입' 시나리오는
    # 구조적으로 불가능해졌다 — by-id 단일 조회의 element 행들로 folded 레코드
    # R=(910,None)이 _aggregate_elements 집계되는지만 검증)
    def fake_by_ids(ids):
        if 910 in set(ids):
            return ([JobStatus(910, 0, JobState.DONE, 0, f"{jsid}_0"),
                     JobStatus(910, 5, JobState.DONE, 0, f"{jsid}_5")], set())
        return ([], set())
    cmd.bjobs_by_ids = fake_by_ids

    manager.querier.query(jsid)

    states = {r.array_index: r.state for r in manager.get_jobs(jsid)}
    # R(folded)은 element 집계(전원 DONE)로 DONE, S=(910,5)는 자기 행으로 DONE
    assert states[None] is JobState.DONE, f"folded 집계 실패: {states}"
    assert states[5] is JobState.DONE
