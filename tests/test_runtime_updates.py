"""RUN 중 run_time_s(경과 실행시간)의 live 발행 — poll_runtime_updates.

기본은 **꺼짐**(False)이다: 켜면 RUN job 전원이 매 폴링 재전이돼 5000건
규모에서 사이클당 5000레코드 배치가 앱으로 간다. 아래 두 테스트는 그 기능을
검증하므로 명시로 켠다."""
from __future__ import annotations


import pytest

from lsfmgr import InMemoryStore, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.states import JobState


@pytest.fixture
def manager(qtbot, fake_lsf, config):
    """이 파일 전용 — run_time live 발행을 켠 manager (기본은 꺼짐)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        poll_runtime_updates=True)
    yield mgr
    mgr.shutdown()


def _submit_one_running(qtbot, mgr, fake_lsf):
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = submit_cmds(mgr, ["echo a"], auto_poll=False)
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


def test_runtime_updates_is_off_by_default(qtbot, fake_lsf, config):
    """기본값이 켜져 있으면 RUN job 전원이 매 폴링 재전이돼, 5000건 규모에서
    사이클마다 5000레코드짜리 배치가 앱의 표 갱신을 때린다 — 기본은 꺼짐."""
    from lsfmgr import LsfConfig

    assert LsfConfig().poll_runtime_updates is False
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        js, jid = _submit_one_running(qtbot, mgr, fake_lsf)
        fake_lsf.jobs[str(jid)].run_time_s = 999      # 경과시간만 변화
        result = mgr.querier.query(js.id)
        assert result.changed == (), result.changed   # 재전이 없음
    finally:
        mgr.shutdown()


def test_runtime_lands_on_the_next_real_transition(qtbot, fake_lsf, config):
    """꺼져 있어도 값이 유실되면 안 된다 — 상태가 바뀌는 순간 최신 경과시간이
    함께 반영돼야 종료 job의 실행시간이 정확하다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        js, jid = _submit_one_running(qtbot, mgr, fake_lsf)
        fake_lsf.jobs[str(jid)].run_time_s = 123
        mgr.querier.query(js.id)                      # 발행 없음
        assert js.jobs()[0].run_time_s == 10          # 진입 시점 값 유지
        fake_lsf.set_job(jid, "DONE", 0)
        fake_lsf.jobs[str(jid)].run_time_s = 200
        mgr.querier.query(js.id)                      # 상태 전이 → 함께 반영
        rec = js.jobs()[0]
        assert rec.state is JobState.DONE and rec.run_time_s == 200
    finally:
        mgr.shutdown()
