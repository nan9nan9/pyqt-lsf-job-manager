"""submit_finished 순서 계약 — 완료 통지 시점에 전 job의 jobs_updated가
이미 도착해 있어야 한다.

과거 버그: _emit_progress가 버퍼를 lock 안에서 비우고 emit은 lock 밖에서 해,
다른 worker 스레드의 _finish_if_done가 그 사이에 finished를 먼저 post할 수
있었다 → 마지막 job의 jobs_updated(PEND)가 submit_finished보다 늦게 도착.
전체 스위트에서 test_resubmit_jobs_pipeline_stages_emitted가 ~1/3 확률로
'PEND 미관측'으로 실패하던 원인. 발화를 전부 ctx.lock 안에서 직렬화해 수정.
"""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState


@pytest.mark.parametrize("trial", range(8))
def test_all_jobs_updated_before_finished(qtbot, manager, fake_lsf, trial):
    """병렬 submit 완료 통지 시점에 전 job이 jobs_updated로 관측돼야 한다."""
    n = 40
    seen_pend = set()
    snapshot_at_finish = {}

    manager.jobs_updated.connect(lambda j, rs: [
        seen_pend.add(r.job_key) for r in rs if r.state is JobState.PEND])

    def on_finished(j, rep):
        snapshot_at_finish["keys"] = set(seen_pend)
        snapshot_at_finish["report"] = rep

    manager.submit_finished.connect(on_finished)

    with qtbot.waitSignal(manager.submit_finished, timeout=15000):
        jsid = manager.submit_bulk(
            [JobSpec(command=f"run {i}") for i in range(n)],
            parallel=True, workers=8)

    # 완료 통지 시점 스냅샷: 전 job의 PEND가 이미 jobs_updated로 나갔어야 함
    assert snapshot_at_finish["report"].succeeded == n
    expected = {f"{jsid}_{i}" for i in range(n)}
    assert snapshot_at_finish["keys"] == expected, (
        f"submit_finished 시점에 누락된 jobs_updated: "
        f"{expected - snapshot_at_finish['keys']}")


def test_resubmit_finished_after_all_updates(qtbot, manager, fake_lsf):
    """resubmit도 동일 — 완료 시점에 전 대상 job의 PEND가 관측돼 있어야 한다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(
            [JobSpec(command=f"r {i}") for i in range(12)], workers=6)
    keys = [f"{jsid}_{i}" for i in range(12)]

    seen = set()
    snap = {}
    manager.jobs_updated.connect(lambda j, rs: [
        seen.add(r.job_key) for r in rs if r.state is JobState.PEND])
    manager.submit_finished.connect(
        lambda j, rep: snap.setdefault("keys", set(seen)))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, keys)

    assert snap["keys"] == set(keys), (
        f"resubmit 완료 시점 누락: {set(keys) - snap['keys']}")
