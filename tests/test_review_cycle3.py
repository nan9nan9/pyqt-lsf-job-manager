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
    assert t._verify({f"{parent}[1]"}) == 1
    assert t._verify({f"{parent}[1]", f"{parent}[3]"}) == 2
    # bare parent id — 같은 job_id 전 element(4개)
    assert t._verify({str(parent)}) == 4
    # 대상 없음
    assert t._verify(set()) == 0


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
