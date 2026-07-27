"""JobSet 관리 테스트 — 손실 감지 / merge / close / add_job (FR-5)."""
from __future__ import annotations

import pytest

from lsfmgr import JobState
from tests.conftest import submit_cmds
from lsfmgr.errors import LsfmgrError


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [f"r {i}" for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs).id
    return jsid


# ----------------------------------------------------------------------
# 손실 감지 (FR-5.3)
# ----------------------------------------------------------------------
def test_detect_lost_no_name_recovery(qtbot, manager, fake_lsf, submitted):
    """v10: name 역조회 복구 제거 — ID 미확보 SUBMITTING은 조회 없이 바로
    LOST 확정한다 (LSF에 job이 살아있어도)."""
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    fake_lsf.calls.clear()
    lost = manager.detect_lost(submitted)
    assert len(lost) == 1
    assert lost[0].state is JobState.LOST
    assert not fake_lsf.calls_of("bjobs")      # 역조회 자체를 안 한다


def test_detect_lost_emits_signals(qtbot, manager, fake_lsf, submitted):
    """리뷰 M3 회귀 — LOST는 terminal이라 폴링이 재보고하지 않으므로
    detect_lost 자체가 job_lost/jobs_updated/jobset_updated를 발행해야 한다."""
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    got = {"lost": [], "jobs": [], "summary": []}
    manager.job_lost.connect(lambda j, r: got["lost"].append(r))
    manager.jobs_updated.connect(lambda j, rs: got["jobs"].append(rs))
    manager.jobset_updated.connect(lambda j, s: got["summary"].append(s))
    lost = manager.detect_lost(submitted)
    assert len(lost) == 1
    assert [r.job_key for r in got["lost"]] == [rec.job_key]
    assert got["jobs"] and got["jobs"][0][0].state is JobState.LOST
    assert got["summary"] and got["summary"][-1]["LOST"] == 1


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
def test_merge_name_collision_is_atomic(qtbot, manager, fake_lsf):
    """이름 충돌 merge는 **아무것도 반영하지 않고** 실패한다 (리뷰 H1 회귀 —
    이전엔 충돌 전 job이 이미 target에 들어가 중복+summary 불변식 파손)."""
    import pytest
    from lsfmgr import JobRecord
    tgt = manager.create_jobset(intended_count=0)
    src = manager.create_jobset(intended_count=0)
    manager.store.store_add_jobs([JobRecord(
        job_id=None, array_index=None, jobset_id=tgt.id,
        job_key="shared", state=JobState.CREATED, command="x")])
    manager.store.store_add_jobs([
        JobRecord(job_id=None, array_index=None, jobset_id=src.id,
                  job_key="aaa_first", state=JobState.CREATED,
                  command="a"),
        JobRecord(job_id=None, array_index=None, jobset_id=src.id,
                  job_key="shared", state=JobState.CREATED,
                  command="b")])
    with pytest.raises(ValueError, match="이름 충돌"):
        manager.jobsets.merge_from(tgt.id, src.id)
    # 원자성 — target/source 모두 원상 그대로
    assert sorted(r.job_key for r in manager.store.get_jobs(tgt.id))         == ["shared"]
    assert sorted(r.job_key for r in manager.store.get_jobs(src.id))         == ["aaa_first", "shared"]
    s = manager.store.summary(tgt.id)
    assert sum(v for k, v in s.items() if k != "total") <= max(s["total"], 1)



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
    # v10: 부착물이 없으므로 bgdel 정리도 없다
    assert not fake_lsf.calls_of("bgdel")


# ----------------------------------------------------------------------
# remove_job — intended_count 정합 (유령 CREATED 방지)
# ----------------------------------------------------------------------
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
    assert s["total"] == 9                      # intended 함께 감소
    assert len(manager.get_jobs(submitted)) == 9
    assert sum(v for k, v in s.items() if k != "total") == 9  # 유령 CREATED 없음


# ----------------------------------------------------------------------
# resubmit_jobs — 상태 기반 재실행 (kill 후 재제출, 레코드 재사용)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 메타데이터/검색 (FR-5.6)
# ----------------------------------------------------------------------
def test_search_by_tag(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = submit_cmds(manager, ["x"],
                                label="tt_sweep", tags=["sweep", "tt"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        submit_cmds(manager, ["y"], tags=["other"])
    hits = manager.search_jobsets(tag="sweep")
    assert [j.jobset_id for j in hits] == [a.id]
    assert manager.search_jobsets(label="tt_sweep")[0].jobset_id == a.id
