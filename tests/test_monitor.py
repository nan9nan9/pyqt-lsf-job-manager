"""polling / 조회 전략 / LOST 전이 테스트 (FR-4)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    """job 20개 submit 완료된 jobset."""
    jobs = [JobSpec(command=f"r {i}") for i in range(20)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs)
    return jsid


# ----------------------------------------------------------------------
# 조회 전략 (FR-4.1) — group 기반 1회 호출
# ----------------------------------------------------------------------
def test_query_uses_group_first(qtbot, manager, fake_lsf, submitted):
    fake_lsf.calls.clear()
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, summary = blocker.args
    assert summary["RUN"] == 20
    # 부착물(group/name) 기반 조회 — job 수에 비례하지 않음 (수용 기준 5)
    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert 1 <= len(bjobs_calls) <= 3
    assert any("-g" in c for c in bjobs_calls)


def test_jobs_updated_carries_only_changes(qtbot, manager, fake_lsf,
                                           submitted):
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobs_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, changed = blocker.args
    assert len(changed) == 20                 # PEND → RUN 전부 변경

    # 변화 없으면 jobs_updated는 안 오고 jobset_updated만 온다 (QT-4)
    got_jobs_updated = []
    manager.jobs_updated.connect(lambda *a: got_jobs_updated.append(a))
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    assert not got_jobs_updated


def test_done_exit_transition(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.set_all("DONE", 0)
    fake_lsf.set_job(recs[0].job_id, "EXIT", 3)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    _, summary = blocker.args
    assert summary["DONE"] == 19
    assert summary["EXIT"] == 1
    exited = manager.get_jobs(submitted, states={JobState.EXIT})
    assert exited[0].exit_code == 3


# ----------------------------------------------------------------------
# bhist fallback → LOST (FR-4.3, 수용 기준 6)
# ----------------------------------------------------------------------
def test_bhist_fallback(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)
    fake_lsf.vanish_job(recs[0].job_id, in_bhist=True)   # bjobs엔 없음
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    rec = manager.store.get_job(submitted, recs[0].lsf_job_name)
    assert rec.state is JobState.DONE                    # bhist로 복구


def test_lost_when_bjobs_and_bhist_both_miss(qtbot, manager, fake_lsf,
                                             submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.vanish_job(recs[0].job_id, in_bhist=False)
    with qtbot.waitSignal(manager.job_lost, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, lost_rec = blocker.args
    assert jsid == submitted
    assert lost_rec.state is JobState.LOST
    assert lost_rec.fail_reason == "NOT_FOUND_IN_LSF"
    s = manager.summary(submitted)
    assert s["LOST"] == 1
    assert sum(v for k, v in s.items() if k != "total") == s["total"]


# ----------------------------------------------------------------------
# 주기 polling (FR-4.4)
# ----------------------------------------------------------------------
def test_periodic_polling(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    updates = []
    manager.jobset_updated.connect(lambda j, s: updates.append(s))
    manager.start_polling(submitted, interval_s=0.2)
    qtbot.waitUntil(lambda: len(updates) >= 2, timeout=10000)
    manager.stop_polling(submitted)
    assert updates[0]["RUN"] == 20


def test_polling_autostops_when_all_terminal(qtbot, manager, fake_lsf,
                                             submitted):
    fake_lsf.set_all("DONE", 0)
    updates = []
    manager.jobset_updated.connect(lambda j, s: updates.append(s))
    manager.start_polling(submitted, interval_s=0.1)
    qtbot.waitUntil(lambda: len(updates) >= 1, timeout=10000)
    qtbot.wait(400)                     # 자동 중지 후 추가 polling 없어야 함
    n = len(updates)
    qtbot.wait(400)
    assert len(updates) == n


# ----------------------------------------------------------------------
# 부착물 전부 유실 → chunking으로 동작 (수용 기준 3)
# ----------------------------------------------------------------------
def test_graceful_degradation_without_attachments(qtbot, manager, fake_lsf,
                                                  submitted):
    from dataclasses import replace
    js = manager.store.get_jobset(submitted)
    manager.store.update_jobset(replace(
        js, lsf_group_paths=[], name_patterns=[], array_job_ids=[]))
    fake_lsf.set_all("RUN")
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    _, summary = blocker.args
    assert summary["RUN"] == 20
    assert all("-g" not in c and "-J" not in c
               for c in fake_lsf.calls_of("bjobs"))
