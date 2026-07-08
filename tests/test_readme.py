"""README 예제 그대로 동작 검증 — 문서와 구현의 계약 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import (
    JobState,
    LsfJobManager,
    PersistenceNotSupportedError,
    SqliteStore,
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
# §6 세션 복원 — js.reconcile() 핸들 메서드 + 이후 자동 polling
# ----------------------------------------------------------------------
def test_readme_restore_flow(qtbot, fake_lsf, config, tmp_path):
    db = str(tmp_path / "restore.db")

    # 세션 1: submit 후 프로세스 kill 가정
    m1 = LsfJobManager(store=SqliteStore(db), config=config, runner=fake_lsf)
    js1 = m1.submit([f"r {i}" for i in range(6)], mode="bulk",
                    auto_poll=False)
    with qtbot.waitSignal(js1.submit_finished, timeout=10000):
        pass
    m1.shutdown()

    # 죽어있는 동안: 절반 DONE, 나머지는 계속 RUN
    recs = m1.store.get_jobs(js1.id)
    for r in recs[:3]:
        fake_lsf.set_job(r.job_id, "DONE", 0)
    for r in recs[3:]:
        fake_lsf.set_job(r.job_id, "RUN")

    # 세션 2: README §6 흐름 그대로
    m2 = LsfJobManager(store=SqliteStore(db), config=config, runner=fake_lsf)
    try:
        summaries = []
        for rec in m2.list_orphan_jobsets():
            js = m2.recover_jobset(rec.jobset_id)      # 핸들 반환
            js.jobset_updated.connect(summaries.append)
            with qtbot.waitSignal(js.jobset_updated, timeout=10000):
                js.reconcile()                         # 비동기
        assert summaries[-1]["DONE"] == 3
        assert summaries[-1]["RUN"] == 3
        # 미종결(RUN) job이 남았으므로 polling 자동 시작 → 추가 갱신 도착
        fake_lsf.set_all("DONE", 0)
        qtbot.waitUntil(lambda: summaries[-1].get("DONE", 0) == 6,
                        timeout=15000)
    finally:
        m2.shutdown()


def test_reconcile_on_inmemory_handle_raises(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], auto_poll=False)
    with pytest.raises(PersistenceNotSupportedError):
        js.reconcile()


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
