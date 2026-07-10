"""README 예제 그대로 동작 검증 — 문서와 구현의 계약 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import (
    JobState,
    LsfJobManager,
)
from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# §1 Quick Start — 3줄 그대로
# ----------------------------------------------------------------------
def test_quickstart_verbatim(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf)
    try:
        lines = []
        js = submit_cmds(mgr, [f"mytool run_{i}.sp" for i in range(50)])
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
    js = submit_cmds(manager, ["a x", "b y"], auto_poll=False)
    js.submit_finished.connect(lambda rpt: msgs.append(
        f"submitted {rpt.ok}/{rpt.total} (failed {rpt.failed})"))
    qtbot.waitUntil(lambda: bool(msgs), timeout=10000)
    assert msgs == ["submitted 2/2 (failed 0)"]


# ----------------------------------------------------------------------
# §4.2 동일 command 반복 제출 — v9: array 제출 제거, job N건 개별 제출
# ----------------------------------------------------------------------
def test_submit_same_command_repeated(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, "run_sim.sh", count=100, auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    recs = js.jobs()
    assert len(recs) == 100
    assert len({r.job_id for r in recs}) == 100       # 각자 개별 job
    assert js.summary["PEND"] == 100


def test_submit_single_command_without_count(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, "lone.sh", auto_poll=False)   # 단일 job 취급
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(js.jobs()) == 1


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# §3.3 스냅샷 조회 계약
# ----------------------------------------------------------------------
def test_snapshot_queries_do_not_call_lsf(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, [f"r {i}" for i in range(5)],
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
