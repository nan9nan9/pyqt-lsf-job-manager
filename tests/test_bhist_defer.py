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
from dataclasses import replace

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


def test_bhist_chunk_failure_isolated(qtbot, config, fake_lsf):
    """bhist를 chunk로 쪼갤 때, 한 chunk가 exit 1이어도 그 chunk의 job만
    보류되고 성공한 chunk의 job은 정상 해소된다 (chunk 단위 실패 격리)."""
    # chunk_size=1 → 각 job_id가 별도 bhist 호출(chunk)로 나간다.
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=replace(config, chunk_size=1), runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = mgr.submit(["echo a", "echo b"], mode="bulk", auto_poll=False)
        bad, good = (r.job_id for r in js.jobs())
        fake_lsf.set_all("RUN")
        mgr.querier.query(js.id)                  # 둘 다 RUN

        # 둘 다 bjobs에서 사라짐 → bhist fallback 필요.
        fake_lsf.set_job(good, "DONE", 0)
        fake_lsf.vanish_job(good, in_bhist=True)  # good: bhist엔 DONE으로 남음
        fake_lsf.vanish_job(bad, in_bhist=True)
        fake_lsf.bhist_fail_ids = {bad}           # bad의 chunk만 exit 1

        result = mgr.querier.query(js.id)
        states = {r.job_id: r.state for r in js.jobs()}
        assert states[bad] is JobState.RUN        # 실패 chunk → 보류(RUN 유지)
        assert states[good] is JobState.DONE       # 성공 chunk → 정상 해소
        changed = {r.job_id for r in result.changed}
        assert good in changed and bad not in changed
    finally:
        mgr.shutdown()


def test_bhist_circuit_breaker_on_consecutive_failures(fake_lsf):
    """bhist 전면 장애(모든 chunk 실패) 시 연속 2회 실패에서 회로를 끊는다 —
    나머지 chunk는 호출 없이 실패 처리되어, 데몬 hang일 때 chunk 수 ×
    timeout만큼 폴링 스레드가 직렬 블록되던 회귀를 막는다."""
    from lsfmgr import LsfConfig
    from lsfmgr.command import LsfCommand

    fake_lsf.fail_bhist = True
    cmd = LsfCommand(config=LsfConfig(chunk_size=1), runner=fake_lsf)

    hist, failed = cmd.bhist_states([1, 2, 3, 4, 5])   # chunk 5개

    assert hist == {}
    assert failed == {1, 2, 3, 4, 5}                   # 전원 실패 귀속
    assert len(fake_lsf.calls_of("bhist")) == 2        # 2회 후 차단


def test_bjobs_chunk_failure_isolated(qtbot, config, fake_lsf):
    """bjobs leftover chunk도 실패가 job 단위로 격리된다 — 실패 chunk의
    job만 보류되고 성공 chunk의 job은 정상 갱신된다 (bhist와 대칭).
    wrapper job은 부착물(group/name)이 없어 폴링이 곧장 id chunk 조회를
    탄다 — chunk_size=1로 job마다 별도 chunk가 되게 한다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=replace(config, chunk_size=1), runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = mgr.submit_wrapper(["customwrapper_sub a.sp",
                                     "customwrapper_sub b.sp"])
        bad, good = (r.job_id for r in js.jobs())
        mgr.querier.query(js.id)                  # 둘 다 PEND 반영
        fake_lsf.set_job(good, "RUN")
        fake_lsf.bjobs_fail_ids = {bad}           # bad의 chunk만 rc=255

        result = mgr.querier.query(js.id)

        states = {r.job_id: r.state for r in js.jobs()}
        assert states[bad] is JobState.PEND       # 실패 chunk → 보류(직전 유지)
        assert states[good] is JobState.RUN        # 성공 chunk → 정상 갱신
        assert result.lost == ()                  # LOST 오확정 없음
        assert bad not in {r.job_id for r in result.changed}
    finally:
        mgr.shutdown()


def test_jobid_none_deferred_when_bhist_failing(qtbot, manager, fake_lsf):
    """job_id 없는 missing 레코드는 bhist로 확인 자체가 불가 — bhist 장애가
    섞인 사이클엔 LOST 확정하지 않고 보류한다 (chunk 격리 전과 동일, FR-4.3)."""
    from lsfmgr.states import JobRecord

    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)                 # id 있는 job — bhist 경로를 연다
    # id 없는 on-LSF 레코드 (persistent 복구 행 등을 흉내)
    manager.store.add_job(JobRecord(
        job_id=None, array_index=None, jobset_id=js.id,
        lsf_job_name="manual_1", state=JobState.PEND, command=""))
    fake_lsf.fail_bhist = True               # bhist 장애 사이클

    result = manager.querier.query(js.id)

    assert result.lost == ()                 # 아무도 LOST 확정 안 됨
    states = {r.job_key: r.state for r in js.jobs()}
    assert states["manual_1"] is JobState.PEND   # 보류 (구 동작과 동일)
