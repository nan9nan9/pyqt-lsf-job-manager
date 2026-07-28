"""조회 수단 장애 시 status 판단 보류 (LOST 오확정 방지, FR-4.3).

핵심 계약: bjobs에서 사라진 job이라도 **그 job이 속한 bjobs chunk가 실패**한
사이클에서는 LOST로 확정하지 않고 '보류'한다 — LSF 순단 1회에 멀쩡한 job이
전원 실패로 확정되는 것을 막는 graceful degradation. 순단이 풀리면 다음
사이클에 자동으로 따라잡는다.

(v10.3: bhist fallback 삭제 — 조회 수단은 bjobs뿐이라 '사라졌고 조회는
정상'이면 곧장 LOST 확정이다.)
"""
from __future__ import annotations

import logging
from dataclasses import replace

from lsfmgr import InMemoryStore, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.states import JobState


def _submit_running(qtbot, mgr, fake_lsf):
    """job 1건을 제출해 RUN 상태로 만든 뒤 (jobset, job_id) 반환."""
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = submit_cmds(mgr, ["echo a"], auto_poll=False)
    jid = js.jobs()[0].job_id
    fake_lsf.set_job(jid, "RUN")
    mgr.querier.query(js.id)                 # RUN 진입 반영
    assert js.jobs()[0].state is JobState.RUN
    return js, jid


def test_query_failure_defers_not_lost(qtbot, manager, fake_lsf):
    """bjobs에서 사라졌지만 그 chunk 조회가 실패 → LOST 확정 안 함(RUN 유지)."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)

    fake_lsf.vanish_job(jid)                 # bjobs에서 사라짐 (missing)
    fake_lsf.bjobs_fail_ids = {jid}          # 그 chunk는 rc=255 (LSF 순단)

    result = manager.querier.query(js.id)

    assert result.lost == ()                 # LOST 확정 없음
    assert js.jobs()[0].state is JobState.RUN  # 직전 상태 그대로 보류(얼어붙음)


def test_defer_logs_hold_message(qtbot, manager, fake_lsf, caplog):
    """보류 로그가 남는지 — 어떤 job이 왜 안 바뀌었는지 추적 가능해야 한다."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)
    fake_lsf.bjobs_fail_ids = {jid}

    with caplog.at_level(logging.WARNING, logger="lsfmgr.monitor"):
        manager.querier.query(js.id)

    assert "판단 보류" in caplog.text
    assert js.jobs()[0].job_key in caplog.text       # 어떤 job인지 식별됨


def test_lost_confirmed_after_query_restored(qtbot, manager, fake_lsf):
    """순단이 풀린 사이클에서 여전히 안 보이면 그때 LOST로 확정한다."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)
    fake_lsf.bjobs_fail_ids = {jid}
    manager.querier.query(js.id)                     # 보류 사이클
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.bjobs_fail_ids = set()                  # 조회 정상 복구
    result = manager.querier.query(js.id)

    assert [r.job_id for r in result.lost] == [jid]
    assert js.jobs()[0].state is JobState.LOST
    assert js.jobs()[0].fail_reason == "NOT_FOUND_IN_LSF"


def test_chunk_failure_isolated(qtbot, config, fake_lsf):
    """실패는 chunk 단위로 격리 — 실패 chunk의 job만 보류되고 성공 chunk의
    job은 정상 갱신된다. chunk_size=1로 job마다 별도 chunk가 되게 한다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=replace(config, chunk_size=1), runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["customwrapper_sub a.sp",
                                   "customwrapper_sub b.sp"])
        bad, good = (r.job_id for r in js.jobs())
        mgr.querier.query(js.id)                  # 둘 다 PEND 반영
        fake_lsf.set_job(good, "RUN")
        fake_lsf.bjobs_fail_ids = {bad}           # bad의 chunk만 rc=255

        result = mgr.querier.query(js.id)

        states = {r.job_id: r.state for r in js.jobs()}
        assert states[bad] is JobState.PEND       # 실패 chunk → 보류(직전 유지)
        assert states[good] is JobState.RUN       # 성공 chunk → 정상 갱신
        assert result.lost == ()                  # LOST 오확정 없음
        assert bad not in {r.job_id for r in result.changed}
    finally:
        mgr.shutdown()


def test_jobid_none_deferred_when_query_failing(qtbot, manager, fake_lsf):
    """job_id 없는 missing 레코드는 id 기반 조회로 확인 자체가 불가 —
    조회 장애가 섞인 사이클엔 LOST 확정하지 않고 보류한다 (FR-4.3)."""
    from lsfmgr.states import JobRecord

    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)
    manager.store.store_add_job(JobRecord(
        job_id=None, array_index=None, jobset_id=js.id,
        job_key="manual_1", state=JobState.PEND, command=""))
    fake_lsf.bjobs_fail_ids = {jid}              # 조회 장애 사이클

    result = manager.querier.query(js.id)

    assert result.lost == ()                     # 아무도 LOST 확정 안 됨
    states = {r.job_key: r.state for r in js.jobs()}
    assert states["manual_1"] is JobState.PEND   # 보류
