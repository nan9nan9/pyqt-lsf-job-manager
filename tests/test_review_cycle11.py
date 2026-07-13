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
        jobset_id=jsid, intended_count=2, name_patterns=[f"{jsid}_*"]))
    # folded 레코드 R=(910,None)와 형제 per-element S=(910,5) — 같은 job_id.
    manager.store.store_add_jobs([
        JobRecord(job_id=910, array_index=None, jobset_id=jsid,
                  lsf_job_name=f"{jsid}_r", state=JobState.RUN, command="r"),
        JobRecord(job_id=910, array_index=5, jobset_id=jsid,
                  lsf_job_name=f"{jsid}_5", state=JobState.RUN, command="r")])

    cmd = manager.querier.command
    # 첫 probe(name)는 element0을 **STALE EXIT**로 반환 → by_id[910]={0:EXIT},
    # R은 첫 pass에서 EXIT로 집계된다(resolved 캐시). S(element5)는 미포함 → leftover.
    def fake_by_name(pattern):
        return [JobStatus(910, 0, JobState.EXIT, 7, f"{jsid}_0")]
    def fake_by_group(path):
        return []
    # S의 leftover by-id 재조회는 job_id=910 전 element를 **최신(DONE)**으로 반환.
    # element0도 여기서 DONE으로 갱신된다(stale EXIT를 덮어야 한다).
    def fake_by_ids(ids):
        if 910 in set(ids):
            return ([JobStatus(910, 0, JobState.DONE, 0, f"{jsid}_0"),
                     JobStatus(910, 5, JobState.DONE, 0, f"{jsid}_5")], set())
        return ([], set())
    cmd.bjobs_by_name = fake_by_name
    cmd.bjobs_by_group = fake_by_group
    cmd.bjobs_by_ids = fake_by_ids

    manager.querier.query(jsid)

    states = {r.array_index: r.state for r in manager.get_jobs(jsid)}
    # R(folded)은 최신 element(전원 DONE)로 집계돼 DONE — stale EXIT 혼입 금지.
    # (dedup 없으면 by_id[910]=[EXIT(stale),DONE,DONE]로 EXIT 오확정 → 영구 terminal)
    assert states[None] is JobState.DONE, f"folded stale 혼입: {states}"
    assert states[5] is JobState.DONE
