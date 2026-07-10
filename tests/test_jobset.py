"""JobSet 관리 테스트 — 손실 감지 / merge / close / add_job (FR-5)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState
from tests.conftest import submit_cmds
from lsfmgr.errors import LsfmgrError
from lsfmgr.states import JobRecord


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"r {i}") for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs).id
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
# ----------------------------------------------------------------------
# merge된 jobset kill — 부착물 전부 순회 (§1.1)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# close (FR-5.7)
# ----------------------------------------------------------------------
def test_close_requires_all_terminal(qtbot, manager, fake_lsf, submitted):
    with pytest.raises(LsfmgrError):
        manager.close(submitted)        # 전원 PEND — 불가


def test_close_after_terminal(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    manager.close(submitted)
    assert manager.store.get_jobset(submitted).closed is True
    # bgdel은 worker 스레드에서 비동기 수행 (main 스레드 LSF 호출 금지)
    qtbot.waitUntil(lambda: len(fake_lsf.calls_of("bgdel")) == 1,
                    timeout=10000)


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


def test_remove_job_decrements_intended_count(qtbot, manager, fake_lsf, submitted):
    # 10건 중 1건 제거 → intended_count 감소, 유령 CREATED 없이 합계 유지
    victim = manager.get_jobs(submitted)[0]
    before = manager.summary(submitted)
    assert before["total"] == 10

    # victim은 PEND(활성) — v9 가드상 force로 레코드만 제거
    recs = manager.remove_job(submitted, job_key=victim.job_key,
                              force=True)
    rec = recs[0]
    assert rec.job_key == victim.job_key       # 제거된 레코드 반환

    s = manager.summary(submitted)
    assert s["total"] == 9                      # add_job의 역연산 — intended 감소
    assert len(manager.get_jobs(submitted)) == 9
    assert sum(v for k, v in s.items() if k != "total") == 9  # 유령 CREATED 없음

    # 제거 후 재추가 → 왕복 일관성 (다시 10)
    manager.add_job(submitted, victim, sync_lsf=False)
    s2 = manager.summary(submitted)
    assert s2["total"] == 10
    assert sum(v for k, v in s2.items() if k != "total") == 10


# ----------------------------------------------------------------------
# resubmit_jobs — 상태 기반 재실행 (kill 후 재제출, 레코드 재사용)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 메타데이터/검색 (FR-5.6)
# ----------------------------------------------------------------------
def test_search_by_tag(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = submit_cmds(manager, [JobSpec(command="x")],
                                label="tt_sweep", tags=["sweep", "tt"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, [JobSpec(command="y")], tags=["other"])
    hits = manager.search_jobsets(tag="sweep")
    assert [j.jobset_id for j in hits] == [a.id]
    assert manager.search_jobsets(label="tt_sweep")[0].jobset_id == a.id
