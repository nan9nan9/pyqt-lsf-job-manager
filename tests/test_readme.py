"""README 예제 그대로 동작 검증 — 문서와 구현의 계약 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import (
    JobState,
    LsfJobManager,
)


# ----------------------------------------------------------------------
# §1 Quick Start — 3줄 그대로
# ----------------------------------------------------------------------
def test_quickstart_verbatim(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf)
    try:
        lines = []
        js = mgr.submit([f"mytool run_{i}.sp" for i in range(50)])
        js.jobset_updated.connect(
            lambda s: lines.append(
                f"RUN={s.get('RUN', 0)} DONE={s.get('DONE', 0)}/{s['total']}"))
        qtbot.waitUntil(lambda: len(lines) >= 1, timeout=10000)
        assert lines[0].endswith("/50")
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# §4.1 SubmitReport — rpt.ok / rpt.total / rpt.failed 표기
# ----------------------------------------------------------------------
def test_report_ok_alias(qtbot, manager, fake_lsf):
    msgs = []
    js = manager.submit(["a x", "b y"], auto_poll=False, mode="bulk")
    js.submit_finished.connect(lambda rpt: msgs.append(
        f"submitted {rpt.ok}/{rpt.total} (failed {rpt.failed})"))
    qtbot.waitUntil(lambda: bool(msgs), timeout=10000)
    assert msgs == ["submitted 2/2 (failed 0)"]


# ----------------------------------------------------------------------
# §4.2 Array — 단일 command 문자열 + count
# ----------------------------------------------------------------------
def test_submit_single_command_with_count(qtbot, manager, fake_lsf):
    js = manager.submit("run_sim.sh $LSB_JOBINDEX", count=100,
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 1        # array 1회
    recs = js.jobs()
    assert len(recs) == 100
    assert len({r.job_id for r in recs}) == 1
    assert js.summary["PEND"] == 100


def test_submit_single_command_without_count(qtbot, manager, fake_lsf):
    js = manager.submit("lone.sh", auto_poll=False)   # 단일 job 취급
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(js.jobs()) == 1


def test_count_with_list_rejected(manager):
    with pytest.raises(ValueError):
        manager.submit(["a", "b"], count=5)


def test_count_invalid(manager):
    with pytest.raises(ValueError):
        manager.submit("x", count=0)


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# §3.3 스냅샷 조회 계약
# ----------------------------------------------------------------------
def test_snapshot_queries_do_not_call_lsf(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(5)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    fake_lsf.calls.clear()
    _ = js.summary
    _ = js.is_done
    _ = js.failed_jobs
    _ = js.jobs(states={JobState.PEND})
    _ = js.id
    assert fake_lsf.calls == []                       # LSF 호출 없음
