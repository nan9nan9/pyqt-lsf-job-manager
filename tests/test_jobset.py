"""JobSet 관리 테스트 — 손실 감지 / merge / close / add_job (FR-5)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState
from lsfmgr.errors import LsfmgrError
from lsfmgr.states import JobRecord


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"r {i}") for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs)
    return jsid


# ----------------------------------------------------------------------
# 손실 감지 (FR-5.3)
# ----------------------------------------------------------------------
def test_detect_lost_recovers_by_name(qtbot, manager, fake_lsf, submitted):
    # ID를 잃어버린 상황 재현: 레코드의 job_id를 지우고 SUBMITTING으로 되돌림
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    lost = manager.detect_lost(submitted)
    assert lost == []                          # name 패턴으로 ID 복구 성공
    recovered = manager.store.get_job(submitted, rec.job_key)
    assert recovered.job_id == rec.job_id
    assert recovered.state is JobState.PEND


def test_detect_lost_marks_lost(qtbot, manager, fake_lsf, submitted):
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    fake_lsf.vanish_job(rec.job_id)            # LSF에서도 소멸
    lost = manager.detect_lost(submitted)
    assert len(lost) == 1
    assert lost[0].state is JobState.LOST


# ----------------------------------------------------------------------
# merge (FR-5.5)
# ----------------------------------------------------------------------
def test_merge_jobsets(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command=f"a {i}") for i in range(5)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command=f"b {i}") for i in range(7)])

    merged = manager.merge_jobsets([a, b], keep_originals=False)
    js = manager.store.get_jobset(merged)
    assert js.intended_count == 12
    assert js.merged_from == [a, b]
    assert len(js.lsf_group_paths) == 2        # 부착물 누적
    assert len(js.name_patterns) == 2
    assert len(manager.get_jobs(merged)) == 12
    # 원본 삭제 확인
    from lsfmgr.errors import JobSetNotFoundError
    with pytest.raises(JobSetNotFoundError):
        manager.store.get_jobset(a)
    # merge된 jobset 요약 불변식
    s = manager.summary(merged)
    assert sum(v for k, v in s.items() if k != "total") == 12


def test_merge_keep_originals(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command="a")])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command="b")])
    merged = manager.merge_jobsets([a, b], keep_originals=True)
    assert manager.store.get_jobset(a) is not None
    assert len(manager.get_jobs(merged)) == 2


# ----------------------------------------------------------------------
# merge된 jobset kill — 부착물 전부 순회 (§1.1)
# ----------------------------------------------------------------------
def test_merged_jobset_kill_iterates_attachments(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command=f"a {i}") for i in range(5)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command=f"b {i}") for i in range(5)])
    merged = manager.merge_jobsets([a, b])
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(merged)
    _, report = blocker.args
    assert report.command_calls == 2           # group 2개 순회
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# close (FR-5.7)
# ----------------------------------------------------------------------
def test_close_requires_all_terminal(qtbot, manager, fake_lsf, submitted):
    with pytest.raises(LsfmgrError):
        manager.close_jobset(submitted)        # 전원 PEND — 불가


def test_close_after_terminal(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    manager.close_jobset(submitted)
    assert manager.store.get_jobset(submitted).closed is True
    assert len(fake_lsf.calls_of("bgdel")) == 1


# ----------------------------------------------------------------------
# add_job (FR-5.4)
# ----------------------------------------------------------------------
def test_add_job_with_lsf_sync(qtbot, manager, fake_lsf, submitted):
    # 외부에서 submit된 job을 편입
    cmd = manager.command
    ext_id = cmd.bsub("external job", job_name="ext_1")
    rec = JobRecord(job_id=ext_id, array_index=None, jobset_id=submitted,
                    lsf_job_name="ext_1", state=JobState.PEND,
                    command="external job")
    manager.add_job(submitted, rec, sync_lsf=True)

    js = manager.store.get_jobset(submitted)
    assert js.intended_count == 11             # 불변식 유지 위해 증가
    assert len(manager.get_jobs(submitted)) == 11
    # bmod -g 호출됨
    assert any("-g" in c for c in fake_lsf.calls_of("bmod"))
    assert fake_lsf.jobs[str(ext_id)].group == js.lsf_group_paths[0]


# ----------------------------------------------------------------------
# 메타데이터/검색 (FR-5.6)
# ----------------------------------------------------------------------
def test_search_by_tag(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command="x")],
                                label="tt_sweep", tags=["sweep", "tt"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit_bulk([JobSpec(command="y")], tags=["other"])
    hits = manager.search_jobsets(tag="sweep")
    assert [j.jobset_id for j in hits] == [a]
    assert manager.search_jobsets(label="tt_sweep")[0].jobset_id == a
