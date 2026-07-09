"""working dir(파티션) full → bhist가 exit 1로 죽는 상황의 status 갱신 동작.

관측 로그:
    조회 실패(bhist): bhist exit 1: ...
    조회 실패로 <job_key> 판단 보류 (LOST 확정 안 함)

핵심 계약: bjobs에서 사라진 job을 bhist로도 확인 못 하면(조회 수단 장애)
LOST로 확정하지 않고 '보류'한다 — LSF/디스크 순단 1회에 멀쩡한 job이
전원 실패로 확정되는 것을 막는 graceful degradation (monitor.py:177-182).
디스크가 풀리면 다음 사이클에 자동으로 따라잡는다.
"""
from __future__ import annotations

import logging

from lsfmgr import InMemoryStore, LsfJobManager
from lsfmgr.states import JobState


def _submit_running(qtbot, mgr, fake_lsf):
    """job 1건을 제출해 RUN 상태로 만든 뒤 (jobset, job_id) 반환."""
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = mgr.submit(["echo a"], mode="bulk", auto_poll=False)
    jid = js.jobs()[0].job_id
    fake_lsf.set_job(jid, "RUN")
    mgr.querier.query(js.id)                 # RUN 진입 반영
    assert js.jobs()[0].state is JobState.RUN
    return js, jid


def test_bhist_failure_defers_not_lost(qtbot, manager, fake_lsf):
    """bjobs에서 사라졌지만 bhist가 exit 1 → LOST 확정 안 함(RUN 유지)."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)

    fake_lsf.vanish_job(jid)                 # bjobs에서 사라짐 (missing)
    fake_lsf.fail_bhist = True               # working dir full → bhist exit 1

    result = manager.querier.query(js.id)

    assert result.lost == ()                 # LOST 확정 없음
    assert js.jobs()[0].state is JobState.RUN  # 직전 상태 그대로 보류(얼어붙음)


def test_defer_logs_hold_message(qtbot, manager, fake_lsf, caplog):
    """관측된 로그('조회 실패' + '판단 보류')가 실제로 남는지 확인."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)
    fake_lsf.fail_bhist = True

    with caplog.at_level(logging.WARNING, logger="lsfmgr.monitor"):
        manager.querier.query(js.id)

    text = caplog.text
    assert "bhist" in text and "exit 1" in text      # 조회 실패(bhist): bhist exit 1
    assert "판단 보류" in text
    assert js.jobs()[0].job_key in text              # 어떤 job인지 식별됨


def test_recovers_after_bhist_restored(qtbot, manager, fake_lsf):
    """디스크가 풀려 bhist가 살아나면 다음 사이클에 최종 상태로 수렴한다."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    # job은 실제로는 종료됐다(성공). bjobs엔 없고 bhist엔 DONE으로 남음.
    fake_lsf.set_job(jid, "DONE")
    fake_lsf.vanish_job(jid, in_bhist=True)

    fake_lsf.fail_bhist = True                # ① 디스크 full — 보류
    manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.fail_bhist = False               # ② 디스크 확보 — 복구
    result = manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.DONE
    assert js.jobs()[0].job_key in {r.job_key for r in result.changed}


def test_found_job_updates_while_sibling_defers(qtbot, config, fake_lsf):
    """bhist 장애 사이클에도 bjobs에 살아있는 job은 정상 갱신된다 —
    보류는 '확인 못 한' job에만 적용되고 나머지 전이를 막지 않는다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = mgr.submit(["echo a", "echo b"], mode="bulk", auto_poll=False)
        alive, gone = (r.job_id for r in js.jobs())
        fake_lsf.set_all("RUN")
        mgr.querier.query(js.id)

        fake_lsf.set_job(alive, "RUN")        # 하나는 bjobs에 그대로 (RUN 유지)
        fake_lsf.vanish_job(gone)             # 하나는 사라짐 → bhist 필요
        fake_lsf.fail_bhist = True            # bhist 장애

        by_id = {r.job_id: r for r in mgr.querier.query(js.id).changed}
        # gone: 보류 → changed에 없음 / store엔 RUN 유지
        states = {r.job_id: r.state for r in js.jobs()}
        assert states[gone] is JobState.RUN
        assert states[alive] is JobState.RUN  # 살아있는 쪽도 정상(장애 전파 없음)
        assert gone not in {r.job_id for r in mgr.querier.query(js.id).changed}
    finally:
        mgr.shutdown()
