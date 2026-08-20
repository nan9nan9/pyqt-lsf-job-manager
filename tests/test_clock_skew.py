"""시스템 시계가 뒤로 갈 때 — NTP 보정 / 수동 변경 / DST.

라이브러리 곳곳이 datetime.now() 차이로 판정한다(제출 후 LOST 유예, 원장
만료, run_time 유도). 시계가 뒤로 가면 그 차이가 음수가 되는데, 어느 쪽으로
틀리느냐가 중요하다 — **되돌릴 수 없는 쪽(LOST 확정 / 원장 삭제)으로 틀리면
안 된다**. 전부 보수적인지 고정한다.
"""
from datetime import datetime, timedelta

import pytest

from lsfmgr import JobState
from lsfmgr.internal_status import InternalStatusSource, _Entry
from lsfmgr.command import JobStatus
from lsfmgr.monitor import _within_submit_grace
from lsfmgr.states import JobRecord


def _rec(**kw):
    base = dict(job_id=1000, array_index=None, jobset_id="js", job_key="k",
                state=JobState.PEND, command="x")
    base.update(kw)
    return JobRecord(**base)


def test_submit_grace_with_a_future_submit_time():
    """시계가 뒤로 가면 submit_time이 '미래'가 된다 — 유예 판정이 깨지나?"""
    now = datetime(2026, 8, 21, 10, 0, 0)
    future = now + timedelta(hours=1)              # 시계가 1시간 뒤로 감
    got = _within_submit_grace(_rec(submit_time=future), now, 60.0)
    print(f"\nsubmit_time이 1시간 미래 → 유예중={got}")
    # 미래면 (now - marker)가 음수라 유예로 판정된다 — LOST를 늦출 뿐이라 안전
    assert got is True


def test_submit_grace_with_a_very_old_time():
    now = datetime(2026, 8, 21, 10, 0, 0)
    old = now - timedelta(days=365)
    assert _within_submit_grace(_rec(submit_time=old), now, 60.0) is False


def test_ledger_expiry_with_a_future_finish_time():
    """finish_time이 미래면 만료 판정이 어떻게 되나."""
    src = InternalStatusSource(lambda: {"jobs": []}, refresh_min_s=0,
                               wait_timeout_s=1, retention_days=14)
    now = datetime(2026, 8, 21, 10, 0, 0)
    future = now + timedelta(days=30)
    e = _Entry(status=JobStatus(job_id=1, array_index=None,
                                state=JobState.DONE, exit_code=0,
                                finish_time=future), seen_at=now)
    print(f"finish_time 30일 미래 → 만료={src._expired(e, now)}")
    assert src._expired(e, now) is False           # 안 버린다(보수적)
    src.shutdown()


def test_ledger_expiry_with_a_far_past_seen_at():
    src = InternalStatusSource(lambda: {"jobs": []}, refresh_min_s=0,
                               wait_timeout_s=1, retention_days=14)
    now = datetime(2026, 8, 21, 10, 0, 0)
    e = _Entry(status=JobStatus(job_id=1, array_index=None,
                                state=JobState.DONE, exit_code=0,
                                finish_time=None),
               seen_at=now - timedelta(days=100))
    assert src._expired(e, now) is True
    src.shutdown()


def test_run_time_is_not_negative_when_clock_goes_back(qtbot):
    """start_time이 미래면 run_time이 음수가 될 수 있다."""
    from lsfmgr.internal_status import job_status_from_dict
    now = datetime(2026, 8, 21, 10, 0, 0)
    st = job_status_from_dict(
        {"dataId": "1", "stat": "RUN",
         "startTime": (now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")},
        now)
    print(f"start_time 2시간 미래 → run_time_s={st.run_time_s}")
    assert st.run_time_s is None or st.run_time_s >= 0


def test_lost_streak_is_time_independent(qtbot, manager, fake_lsf):
    """LOST 스트릭은 '연속 횟수'라 시계와 무관해야 한다."""
    from tests.conftest import submit_cmds
    js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    qtbot.wait(300)
    rec = js.jobs()[0]
    for j in fake_lsf.jobs.values():
        if j.job_id == rec.job_id:
            j.vanished = True
    for _ in range(5):
        manager.query_once(js)
        qtbot.wait(150)
    assert js.jobs()[0].state is JobState.LOST
