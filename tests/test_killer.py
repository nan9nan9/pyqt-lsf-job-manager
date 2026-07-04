"""kill 전략 / 부분 kill / verify 테스트 (FR-3)."""
from __future__ import annotations

import pytest

from lsfmgr import ArrayJobSpec, JobSpec, JobState


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"r {i}") for i in range(30)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs)
    return jsid


# ----------------------------------------------------------------------
# 전략 ① group 1회 호출 (수용 기준 2)
# ----------------------------------------------------------------------
def test_kill_by_group_single_call(qtbot, manager, fake_lsf, submitted):
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted)
    jsid, report = blocker.args
    assert jsid == submitted
    assert report.requested == 30
    assert report.command_calls == 1                  # bkill 1회
    assert any(s.startswith("group:") for s in report.strategies)
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 전략 ② array
# ----------------------------------------------------------------------
def test_kill_array(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_array(ArrayJobSpec(command="r", count=25))
    # array jobset은 group도 있으므로 group이 먼저 시도됨 — group 제거하여
    # array 전략 검증
    from dataclasses import replace
    js = manager.store.get_jobset(jsid)
    manager.store.update_jobset(replace(js, lsf_group_paths=[]))
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(jsid)
    _, report = blocker.args
    assert report.command_calls == 1
    assert any(s.startswith("array:") for s in report.strategies)
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 전략 ④ chunking (부착물 전부 유실, 수용 기준 3)
# ----------------------------------------------------------------------
def test_kill_chunk_fallback(qtbot, manager, fake_lsf, submitted, config):
    from dataclasses import replace
    js = manager.store.get_jobset(submitted)
    manager.store.update_jobset(replace(
        js, lsf_group_paths=[], name_patterns=[], array_job_ids=[]))
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted)
    _, report = blocker.args
    assert report.strategies == ["chunk"]
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 부분 kill (FR-3.2)
# ----------------------------------------------------------------------
def test_partial_kill_by_state(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    # 절반만 RUN으로 (store에도 반영)
    for r in recs[:15]:
        fake_lsf.set_job(r.job_id, "RUN")
        manager.store.transition(submitted, r.job_key, JobState.RUN)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted, only_state=JobState.PEND)
    _, report = blocker.args
    assert report.requested == 15
    run_alive = [j for j in fake_lsf.alive_jobs() if j.stat == "RUN"]
    assert len(run_alive) == 15                       # RUN은 살아있음


def test_kill_individual_ids(qtbot, manager, fake_lsf, submitted):
    ids = [r.job_id for r in manager.get_jobs(submitted)][:5]
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobs(ids)
    _, report = blocker.args
    assert report.requested == 5
    assert len(fake_lsf.alive_jobs()) == 25


# ----------------------------------------------------------------------
# verify (FR-3.3)
# ----------------------------------------------------------------------
def test_kill_verify(qtbot, manager, fake_lsf, submitted):
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(submitted, verify=True)
    _, report = blocker.args
    assert report.still_alive == 0
    # verify 조회가 store에도 반영됨 (killed → EXIT)
    s = manager.summary(submitted)
    assert s.get("EXIT", 0) == 30
