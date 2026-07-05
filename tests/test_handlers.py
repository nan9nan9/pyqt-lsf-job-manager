"""JobSet handler — 주기 실행 / start·end state / 최종 실행 / 에러 (FR-7)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="run 0")])
    return jsid


def _poll(qtbot, manager, jsid):
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(jsid)


# ----------------------------------------------------------------------
# 주기 실행 + start/end state + 최종 실행
# ----------------------------------------------------------------------
def test_handler_runs_on_start_state_and_finalizes(qtbot, manager, fake_lsf,
                                                   submitted):
    key = f"{submitted}_0"
    calls = []

    def handler(ctx):
        calls.append((ctx.job_key, ctx.job_id, ctx.record.state, ctx.final))
        return {"seen": ctx.record.state.value}

    # 아직 PEND — start_state(RUN) 아님 → 실행 안 됨
    manager.add_handler(submitted, "collect", handler, interval_s=0.05,
                        start_states={JobState.RUN},
                        end_states={JobState.DONE, JobState.EXIT})
    qtbot.wait(150)
    assert calls == []                                  # PEND에서는 미실행

    # RUN으로 전이 → 주기 실행 시작
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000) as blk:
        pass
    jsid, name, res = blk.args
    assert name == "collect" and res.job_key == key
    assert res.final is False and res.error is None
    assert res.data == {"seen": "RUN"}
    assert res.job_id is not None

    # DONE으로 전이 → 최종 실행(final=True) 후 자동 종료
    fake_lsf.set_all("DONE", 0)
    _poll(qtbot, manager, submitted)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000,
                          check_params_cb=lambda j, n, r: r.final) as blk2:
        pass
    assert blk2.args[2].final is True
    assert blk2.args[2].data == {"seen": "DONE"}        # 최종 실행은 DONE 시점

    # 모든 job 최종 실행 완료 → handler 휴면 (타이머 정지, 등록은 유지 —
    # resubmit_jobs 재실행 시 rearm이 재가동할 수 있게)
    h = manager.handlers._handlers[(submitted, "collect")]
    qtbot.waitUntil(lambda: not h.timer.isActive(), timeout=5000)


# ----------------------------------------------------------------------
# handler_finished는 JobSet 핸들로도 이중 발행 (name, result)
# ----------------------------------------------------------------------
def test_handler_finished_relayed_to_handle(qtbot, manager, fake_lsf,
                                            submitted):
    js = manager.jobset(submitted)
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)

    manager.jobset(submitted)      # 핸들 발급 확인
    with qtbot.waitSignal(js.handler_finished, timeout=10000) as blk:
        js.add_handler("h1", lambda ctx: ctx.job_id, interval_s=0.05)
    name, res = blk.args
    assert name == "h1" and res.data == res.job_id


# ----------------------------------------------------------------------
# 에러 격리 — 예외는 error 필드로 전달, 다른 tick을 죽이지 않음
# ----------------------------------------------------------------------
def test_handler_error_is_captured(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)

    def boom(ctx):
        raise RuntimeError("boom!")

    with qtbot.waitSignal(manager.handler_finished, timeout=10000) as blk:
        manager.add_handler(submitted, "bad", boom, interval_s=0.05)
    res = blk.args[2]
    assert res.error is not None and "boom!" in res.error
    assert res.data is None
    manager.remove_handler(submitted, "bad")


# ----------------------------------------------------------------------
# remove_handler — 이후 실행 없음
# ----------------------------------------------------------------------
def test_remove_handler_stops_execution(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)
    calls = []
    with qtbot.waitSignal(manager.handler_finished, timeout=10000):
        manager.add_handler(submitted, "c", lambda ctx: calls.append(1),
                            interval_s=0.05)
    manager.remove_handler(submitted, "c")
    n = len(calls)
    qtbot.wait(200)                                     # 여러 tick 분량 대기
    assert len(calls) == n                              # 더 이상 실행 안 됨


# ----------------------------------------------------------------------
# end_states가 terminal을 다 안 덮을 때 — 죽은 job에 무한 발화 금지
# ----------------------------------------------------------------------
def test_handler_terminal_outside_end_states_finishes_silently(
        qtbot, manager, fake_lsf, submitted):
    """end_states={DONE}인데 job이 EXIT로 죽으면 — 최종 실행 없이 종결되고
    handler는 자동 해제된다 (죽은 job에 매 tick 발화 방지)."""
    calls = []
    manager.add_handler(submitted, "h", lambda ctx: calls.append(ctx.final),
                        interval_s=0.05,
                        start_states={JobState.RUN},
                        end_states={JobState.DONE})   # EXIT 미포함
    fake_lsf.set_all("EXIT", 3)
    _poll(qtbot, manager, submitted)
    # handler 휴면까지 대기 — EXIT는 end에 없지만 terminal이라 종결
    h = manager.handlers._handlers[(submitted, "h")]
    qtbot.waitUntil(lambda: not h.timer.isActive(), timeout=5000)
    assert calls == []                       # 발화 없이 조용히 종결
    manager.remove_handler(submitted, "h")


# ----------------------------------------------------------------------
# resubmit_jobs 후 handler 재무장 — 새 실행에서 다시 돈다
# ----------------------------------------------------------------------
def test_handler_rearmed_after_resubmit(qtbot, manager, fake_lsf, submitted):
    key = f"{submitted}_0"
    results = []
    manager.handler_finished.connect(
        lambda j, n, r: results.append(r))

    # 1차 실행: RUN → DONE, final까지 수행 (_FINISHED 상태 도달)
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)
    manager.add_handler(submitted, "c", lambda ctx: ctx.record.state.value,
                        interval_s=0.05)
    fake_lsf.set_all("DONE", 0)
    _poll(qtbot, manager, submitted)
    qtbot.waitUntil(lambda: any(r.final for r in results), timeout=5000)

    # 1차 완료로 handler는 휴면 상태(등록 유지) — resubmit의 rearm이 재가동
    assert (submitted, "c") in manager.handlers._handlers
    n_before = len(results)
    manager.start_polling(submitted, interval_s=0.2)   # 재개 기준 interval
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(submitted, [key])

    # 새 실행이 RUN→DONE 되면 handler가 다시 최종 실행까지 돈다 (재무장)
    fake_lsf.set_all("RUN")
    qtbot.waitUntil(lambda: len(results) > n_before, timeout=5000)
    fake_lsf.set_all("DONE", 0)
    qtbot.waitUntil(
        lambda: any(r.final for r in results[n_before:]), timeout=5000)


# ----------------------------------------------------------------------
# 검증 — 이름 중복 / 없는 jobset / 잘못된 interval
# ----------------------------------------------------------------------
def test_handler_validation(qtbot, manager, submitted):
    manager.add_handler(submitted, "dup", lambda ctx: None, interval_s=1)
    with pytest.raises(ValueError):                     # 이름 중복
        manager.add_handler(submitted, "dup", lambda ctx: None, interval_s=1)
    with pytest.raises(ValueError):                     # interval <= 0
        manager.add_handler(submitted, "z", lambda ctx: None, interval_s=0)
    from lsfmgr.errors import JobSetNotFoundError
    with pytest.raises(JobSetNotFoundError):            # 없는 jobset
        manager.add_handler("nope", "x", lambda ctx: None, interval_s=1)
    manager.remove_handler(submitted, "dup")
