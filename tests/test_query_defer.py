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
    """순단이 풀린 뒤 **연속 N회** 미발견이면 그때 LOST로 확정한다."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.vanish_job(jid)
    fake_lsf.bjobs_fail_ids = {jid}
    manager.querier.query(js.id)                     # 보류 사이클
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.bjobs_fail_ids = set()                  # 조회 정상 복구
    grace = manager.command.config.lost_after_missing_polls
    for _ in range(grace - 1):                       # 유예 중 — 아직 아님
        assert manager.querier.query(js.id).lost == ()
        assert js.jobs()[0].state is JobState.RUN
    result = manager.querier.query(js.id)            # N회째 — 확정

    assert [r.job_id for r in result.lost] == [jid]
    assert js.jobs()[0].state is JobState.LOST
    assert js.jobs()[0].fail_reason == "NOT_FOUND_IN_LSF"


def test_missing_streak_resets_when_job_reappears(qtbot, manager, fake_lsf):
    """한두 사이클 안 보이다 다시 보이면 유예 카운트가 리셋된다 — 제출 직후
    등록 지연이나 조회 클러스터 불일치로 깜빡이는 job을 죽이지 않는다."""
    js, jid = _submit_running(qtbot, manager, fake_lsf)
    grace = manager.command.config.lost_after_missing_polls
    assert grace >= 2, "이 테스트는 유예가 2회 이상일 때 의미가 있다"

    fake_lsf.vanish_job(jid)
    for _ in range(grace - 1):                       # 유예 소진 직전까지
        manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.jobs[str(jid)].vanished = False         # 다시 보임 → 리셋
    manager.querier.query(js.id)
    fake_lsf.vanish_job(jid)
    for _ in range(grace - 1):                       # 다시 유예 소진 직전
        assert manager.querier.query(js.id).lost == ()
    assert js.jobs()[0].state is JobState.RUN        # 아직 LOST 아님


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


def test_purged_id_in_chunk_does_not_lose_live_jobs(qtbot, manager, fake_lsf):
    """**실환경 버그 회귀**: 한 chunk에 purge된 id가 섞이면 LSF는 rc=255 +
    'No matching job found'를 내면서도 **찾은 job 행은 stdout에 출력**한다.
    그 stdout을 버리면 살아있는 job까지 전부 LOST로 확정된다 —
    ("LOST 확정 로그가 뜨는데 bjobs로 보면 job이 살아있다"는 증상).
    purge된 것만 LOST, 나머지는 정상 반영돼야 한다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, [f"m {i}" for i in range(4)],
                         auto_poll=False)
    recs = sorted(js.jobs(), key=lambda r: r.job_id)
    gone, done, run1, run2 = recs
    fake_lsf.set_job(gone.job_id, "DONE")
    fake_lsf.vanish_job(gone.job_id)          # CLEAN_PERIOD 밖 — purge
    fake_lsf.set_job(done.job_id, "DONE")
    fake_lsf.set_job(run1.job_id, "RUN")
    fake_lsf.set_job(run2.job_id, "RUN")

    grace = manager.command.config.lost_after_missing_polls
    for _ in range(grace):                    # 4건이 한 chunk
        result = manager.querier.query(js.id)

    states = {r.job_key: r.state for r in js.jobs()}
    assert states[gone.job_key] is JobState.LOST        # 진짜 없는 것만
    assert states[done.job_key] is JobState.DONE        # 같은 chunk의 종료분
    assert states[run1.job_key] is JobState.RUN         # 살아있는 job은
    assert states[run2.job_key] is JobState.RUN         # 절대 LOST 아님
    assert [r.job_key for r in result.lost] == [gone.job_key]


def test_circuit_breaker_stops_after_consecutive_chunk_failures(config,
                                                                fake_lsf):
    """전면 장애(데몬 hang)에서 연속 실패가 이어지면 남은 chunk는 호출 없이
    실패 처리하고 중단한다 — chunk 수 × timeout 만큼 폴링 스레드가 직렬로
    블록되는 것을 막는다. 중단된 chunk의 job도 failed에 들어가 보류된다."""
    from dataclasses import replace
    from lsfmgr.command import LsfCommand

    cmd = LsfCommand(replace(config, chunk_size=1), runner=fake_lsf)
    ids = list(range(1000, 1010))            # chunk 10개
    fake_lsf.fail_all_queries = True         # 전부 rc=255 (장애 문구)

    statuses, failed = cmd.bjobs_by_ids(ids)

    assert statuses == []
    assert failed == set(ids)                # 호출 안 한 chunk도 실패로 귀속
    calls = len(fake_lsf.calls_of("bjobs"))
    assert calls < len(ids), f"회로 차단 없이 전 chunk 호출됨 ({calls}회)"
