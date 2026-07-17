"""min_state_dwell_s — 상태 전이 표시 최소 간격 (pacer.StatePacer).

순식간에 지나가는 전이(SUBMITTING→PEND, EXIT→SUBMITTING)를 GUI가 볼 수
있도록 jobs_updated 발화를 job별로 띄우는 기능. store는 늦추지 않는다.
"""
from __future__ import annotations

import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds

DWELL = 0.4          # 테스트는 짧은 dwell로 — 계약은 값과 무관


@pytest.fixture
def paced_manager(qtbot, fake_lsf):
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0,
                    kill_retry_delay_s=0.05, min_state_dwell_s=DWELL)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    yield mgr
    mgr.shutdown()


def _states_of(seen, job_key):
    """수신한 jobs_updated 배치들에서 해당 job의 상태 전이 순서만 뽑는다."""
    return [rec.state for _t, recs in seen for rec in recs
            if rec.job_key == job_key]


def _connect(mgr):
    seen = []      # [(수신 시각, [JobRecord])]
    mgr.jobs_updated.connect(
        lambda _jsid, recs: seen.append((time.monotonic(), list(recs))))
    return seen


# ----------------------------------------------------------------------
# 핵심 — SUBMITTING이 표에 dwell만큼 머문 뒤에야 PEND가 나간다
# ----------------------------------------------------------------------
def test_submitting_visible_before_pend(qtbot, paced_manager):
    mgr = paced_manager
    seen = _connect(mgr)
    js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
    key = mgr.get_jobs(js._jobset_id)[0].job_key

    qtbot.waitUntil(lambda: JobState.PEND in _states_of(seen, key),
                    timeout=5000)
    # create_jobset의 CREATED(표에 행 추가)도 dwell을 갖는 전이다
    assert _states_of(seen, key) == [JobState.CREATED, JobState.SUBMITTING,
                                     JobState.PEND]

    t_submitting = next(t for t, recs in seen
                        if any(r.state is JobState.SUBMITTING for r in recs))
    t_pend = next(t for t, recs in seen
                  if any(r.state is JobState.PEND for r in recs))
    assert t_pend - t_submitting >= DWELL * 0.9    # 화면에 머문 시간


def test_store_is_not_delayed(qtbot, paced_manager):
    """pacer는 표시만 늦춘다 — store는 신호보다 앞서 이미 PEND."""
    mgr = paced_manager
    seen = _connect(mgr)
    js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)

    # store가 PEND에 도달한 시점에도 표는 아직 앞선 상태에 머물러 있다
    qtbot.waitUntil(
        lambda: mgr.get_jobs(js._jobset_id)[0].state is JobState.PEND,
        timeout=5000)
    key = mgr.get_jobs(js._jobset_id)[0].job_key
    assert JobState.PEND not in _states_of(seen, key)


# ----------------------------------------------------------------------
# 전이는 버리지 않고 순서대로 — EXIT → SUBMITTING → PEND
# ----------------------------------------------------------------------
def test_resubmit_shows_every_state_in_order(qtbot, paced_manager):
    mgr = paced_manager
    seen = _connect(mgr)
    js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
    qtbot.waitUntil(
        lambda: mgr.get_jobs(js._jobset_id)[0].state is JobState.PEND,
        timeout=5000)
    key = mgr.get_jobs(js._jobset_id)[0].job_key

    with qtbot.waitSignal(mgr.kill_finished, timeout=5000):
        mgr.kill(js)
    qtbot.waitUntil(lambda: mgr.can_submit(js._jobset_id), timeout=5000)
    mgr.submit(js, auto_poll=False)

    qtbot.waitUntil(lambda: len(_states_of(seen, key)) >= 6, timeout=10000)
    # EXIT(kill)이 순식간에 SUBMITTING으로 덮이지 않고 전부 순서대로 보인다
    assert _states_of(seen, key) == [
        JobState.CREATED, JobState.SUBMITTING, JobState.PEND,
        JobState.EXIT, JobState.SUBMITTING, JobState.PEND]
    times = [t for t, recs in seen if any(r.job_key == key for r in recs)]
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert all(g >= DWELL * 0.9 for g in gaps), gaps


def test_batches_stay_batched(qtbot, paced_manager):
    """대량이어도 같은 tick의 보류분은 jobset당 한 배치로 합쳐 발화 —
    job 수만큼 신호가 쪼개지지 않는다."""
    mgr = paced_manager
    seen = _connect(mgr)
    js = submit_cmds(mgr, [f"mytool r_{i}.sp" for i in range(40)],
                     auto_poll=False)

    qtbot.waitUntil(
        lambda: all(r.state is JobState.PEND
                    for r in mgr.get_jobs(js._jobset_id)), timeout=10000)
    qtbot.waitUntil(
        lambda: sum(len(recs) for _t, recs in seen
                    if any(r.state is JobState.PEND for r in recs)) == 40,
        timeout=10000)
    pend_batches = [recs for _t, recs in seen
                    if any(r.state is JobState.PEND for r in recs)]
    assert len(pend_batches) <= 4          # 40건이 40개 신호로 쪼개지지 않음


# ----------------------------------------------------------------------
# 기본값(0) — 발화 경로 무변경
# ----------------------------------------------------------------------
def test_off_by_default(qtbot, manager):
    """기본값(0)이면 pacer를 아예 만들지 않는다 — 발화 경로가 종전과 동일."""
    assert manager._pacer is None
    seen = _connect(manager)
    js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    qtbot.waitUntil(
        lambda: manager.get_jobs(js._jobset_id)[0].state is JobState.PEND,
        timeout=5000)
    key = manager.get_jobs(js._jobset_id)[0].job_key
    # 전이는 전부 오되 dwell 큐를 거치지 않는다. store가 PEND인 시점에 신호가
    # 이미 도착했다고 단정하면 안 된다 — store-first(전이 후 발화)에 발화는
    # worker→main queued라, 배달은 항상 store보다 뒤다.
    qtbot.waitUntil(
        lambda: _states_of(seen, key) == [JobState.CREATED,
                                          JobState.SUBMITTING,
                                          JobState.PEND], timeout=5000)


def test_shutdown_flushes_pending(qtbot, fake_lsf):
    """종료 시 밀린 전이는 유실되지 않고 즉시 발화된다."""
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0,
                    min_state_dwell_s=30.0)      # 타이머로는 절대 안 빠질 dwell
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    try:
        seen = _connect(mgr)
        js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        qtbot.waitUntil(
            lambda: mgr.get_jobs(js._jobset_id)[0].state is JobState.PEND,
            timeout=5000)
        key = mgr.get_jobs(js._jobset_id)[0].job_key
        assert JobState.PEND not in _states_of(seen, key)     # 보류 중
    finally:
        mgr.shutdown()
    # 종료 시엔 중간 과정을 보여줄 시간이 없다 — 최종 상태가 남는 것만 보장
    assert _states_of(seen, key)[-1] is JobState.PEND


def test_removed_job_does_not_resurrect(qtbot, paced_manager):
    """dwell 창 안에 지워진 job의 보류 전이는 버려진다 — 안 버리면 뒤늦은
    jobs_updated가 표에서 지운 행을 되살린다."""
    mgr = paced_manager
    js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
    qtbot.waitUntil(
        lambda: mgr.get_jobs(js._jobset_id)[0].state is JobState.PEND,
        timeout=5000)
    with qtbot.waitSignal(mgr.kill_finished, timeout=5000):
        mgr.kill(js)                       # EXIT — 표시 대기열에 밀린다
    key = mgr.get_jobs(js._jobset_id)[0].job_key

    seen = _connect(mgr)
    mgr.remove_job(js._jobset_id, job_key=key)     # dwell 창 안에 삭제
    qtbot.wait(int(DWELL * 1000 * 3))
    assert _states_of(seen, key) == []


def test_negative_dwell_rejected():
    with pytest.raises(ValueError):
        LsfConfig(min_state_dwell_s=-1.0)
    with pytest.raises(ValueError):
        LsfJobManager(min_state_dwell_s=-1.0)
