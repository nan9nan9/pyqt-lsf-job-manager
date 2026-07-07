"""RUN 중 run_time_s(경과 실행시간)가 jobs_updated로 live 발행되는지 (기본 켬)
+ poll_runtime_updates=False로 끌 수 있는지."""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.states import JobState
from tests.fake_lsf import FakeLsf


def _submit_one_running(qtbot, mgr, fake_lsf):
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = mgr.submit(["echo a"], mode="bulk", auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_job(rec.job_id, "RUN")
    fake_lsf.jobs[str(rec.job_id)].run_time_s = 10
    mgr.querier.query(js.id)          # RUN 진입 (runtime=10)
    return js, rec.job_id


def test_runtime_emitted_on_change(qtbot, manager, fake_lsf):
    """RUN 중 runtime만 늘어도 그 레코드가 changed(→ jobs_updated)에 실린다."""
    js, jid = _submit_one_running(qtbot, manager, fake_lsf)
    fake_lsf.jobs[str(jid)].run_time_s = 45   # 경과시간 증가 (상태는 그대로 RUN)
    result = manager.querier.query(js.id)

    runtimes = [r.run_time_s for r in result.changed if r.state is JobState.RUN]
    assert 45 in runtimes, f"live runtime 미발행: {runtimes}"
    assert js.jobs()[0].run_time_s == 45       # store에도 반영


def test_runtime_emitted_via_polling_signal(qtbot, manager, fake_lsf):
    """실제 polling 경로에서도 runtime 변화가 jobs_updated Signal로 온다."""
    js, jid = _submit_one_running(qtbot, manager, fake_lsf)
    fake_lsf.jobs[str(jid)].run_time_s = 77
    with qtbot.waitSignal(manager.jobs_updated, timeout=10000) as blocker:
        manager.query_once(js.id)             # 폴링 워커 경유 → Signal 발화
    _jsid, recs = blocker.args
    assert any(r.run_time_s == 77 for r in recs), [r.run_time_s for r in recs]


def test_runtime_updates_disabled(qtbot, fake_lsf, config):
    """poll_runtime_updates=False면 runtime만 변한 사이클엔 발행/전이 없음."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        poll_runtime_updates=False)
    try:
        js, jid = _submit_one_running(qtbot, mgr, fake_lsf)
        fake_lsf.jobs[str(jid)].run_time_s = 99
        result = mgr.querier.query(js.id)
        assert result.changed == ()            # runtime만 바뀜 → 전이 없음
    finally:
        mgr.shutdown()


def test_runtime_still_set_on_terminal_when_disabled(qtbot, fake_lsf, config):
    """끈 상태여도 상태 전이(RUN→DONE) 시점엔 최종 runtime이 반영된다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        poll_runtime_updates=False)
    try:
        js, jid = _submit_one_running(qtbot, mgr, fake_lsf)
        fake_lsf.jobs[str(jid)].run_time_s = 120
        fake_lsf.set_job(jid, "DONE")
        mgr.querier.query(js.id)
        rec = js.jobs()[0]
        assert rec.state is JobState.DONE and rec.run_time_s == 120
    finally:
        mgr.shutdown()
