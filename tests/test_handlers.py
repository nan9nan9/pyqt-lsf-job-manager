"""JobSet handler — 폴링 구동 / start·end state / 최종 실행 / 에러 (FR-7)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="run 0")])
    return jsid


def _poll(qtbot, manager, jsid):
    """1회 폴링 — handler는 이 사이클 직후 평가된다."""
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(jsid)


# ----------------------------------------------------------------------
# 폴링 사이클 구동 + start/end state + 최종 실행
# ----------------------------------------------------------------------
def test_handler_runs_on_start_state_and_finalizes(qtbot, manager, fake_lsf,
                                                   submitted):
    key = f"{submitted}_0"
    calls = []

    def handler(ctx):
        calls.append((ctx.job_key, ctx.job_id, ctx.record.state, ctx.final))
        return {"seen": ctx.record.state.value}

    manager.add_handler(submitted, "collect", handler,
                        start_states={JobState.RUN},
                        end_states={JobState.DONE, JobState.EXIT})
    # 아직 PEND — start_state(RUN) 아님 → 폴링해도 실행 안 됨
    _poll(qtbot, manager, submitted)
    qtbot.wait(80)
    assert calls == []

    # RUN으로 전이 → 그 폴링 사이클에 실행
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.handler_finished, timeout=10000) as blk:
        manager.query_once(submitted)
    jsid, name, res = blk.args
    assert name == "collect" and res.job_key == key
    assert res.final is False and res.error is None
    assert res.data == {"seen": "RUN"} and res.job_id is not None

    # DONE으로 전이 → 최종 실행(final=True)
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000,
                          check_params_cb=lambda j, n, r: r.final) as blk2:
        manager.query_once(submitted)
    assert blk2.args[2].final is True
    assert blk2.args[2].data == {"seen": "DONE"}

    # 최종 실행 후 — 등록은 유지되고(resubmit rearm 대비), 추가 폴링에도 안 돎
    assert (submitted, "collect") in manager.handlers._handlers
    n = len(calls)
    _poll(qtbot, manager, submitted)
    qtbot.wait(80)
    assert len(calls) == n


# ----------------------------------------------------------------------
# handler_finished는 JobSet 핸들로도 이중 발행 (name, result)
# ----------------------------------------------------------------------
def test_handler_finished_relayed_to_handle(qtbot, manager, fake_lsf,
                                            submitted):
    js = manager.jobset(submitted)
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)          # store를 RUN으로

    js.add_handler("h1", lambda ctx: ctx.job_id)
    with qtbot.waitSignal(js.handler_finished, timeout=10000) as blk:
        manager.query_once(submitted)         # 폴링 사이클에 실행
    name, res = blk.args
    assert name == "h1" and res.data == res.job_id


# ----------------------------------------------------------------------
# 에러 격리 — 예외는 error 필드로 전달
# ----------------------------------------------------------------------
def test_handler_error_is_captured(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)

    def boom(ctx):
        raise RuntimeError("boom!")

    manager.add_handler(submitted, "bad", boom)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000) as blk:
        manager.query_once(submitted)
    res = blk.args[2]
    assert res.error is not None and "boom!" in res.error
    assert res.data is None
    manager.remove_handler(submitted, "bad")


# ----------------------------------------------------------------------
# remove_handler — 이후 폴링에서 실행 없음
# ----------------------------------------------------------------------
def test_remove_handler_stops_execution(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)
    calls = []
    manager.add_handler(submitted, "c", lambda ctx: calls.append(1))
    with qtbot.waitSignal(manager.handler_finished, timeout=10000):
        manager.query_once(submitted)         # 1회 실행
    manager.remove_handler(submitted, "c")
    n = len(calls)
    _poll(qtbot, manager, submitted)           # 제거 후 폴링
    qtbot.wait(80)
    assert len(calls) == n                     # 더 이상 실행 안 됨


# ----------------------------------------------------------------------
# end_states가 terminal을 다 안 덮을 때 — 죽은 job에 발화 금지
# ----------------------------------------------------------------------
def test_handler_terminal_outside_end_states_finishes_silently(
        qtbot, manager, fake_lsf, submitted):
    """end_states={DONE}인데 job이 EXIT로 죽으면 — 최종 실행 없이 종결된다."""
    calls = []
    manager.add_handler(submitted, "h", lambda ctx: calls.append(ctx.final),
                        start_states={JobState.RUN}, end_states={JobState.DONE})
    fake_lsf.set_all("EXIT", 3)
    _poll(qtbot, manager, submitted)           # EXIT는 end에 없지만 terminal
    _poll(qtbot, manager, submitted)           # 한 번 더 — 여전히 발화 없어야
    qtbot.wait(80)
    assert calls == []
    manager.remove_handler(submitted, "h")


# ----------------------------------------------------------------------
# 기본 state — 시작 RUN, 종료 DONE/EXIT
# ----------------------------------------------------------------------
def test_handler_default_states(qtbot, manager, fake_lsf, submitted):
    """start/end 미지정 시 기본값(RUN 시작, DONE/EXIT 종료)."""
    finals = []
    manager.handler_finished.connect(lambda j, n, r: finals.append(r.final))
    manager.add_handler(submitted, "d", lambda ctx: None)   # 기본 state
    _poll(qtbot, manager, submitted)                        # PEND → 미발화
    qtbot.wait(50)
    assert finals == []
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.handler_finished, timeout=10000):
        manager.query_once(submitted)                       # RUN → 발화(False)
    fake_lsf.set_all("EXIT", 1)                             # 기본 end에 EXIT 포함
    with qtbot.waitSignal(manager.handler_finished, timeout=10000,
                          check_params_cb=lambda j, n, r: r.final):
        manager.query_once(submitted)                       # EXIT → 최종
    assert True in finals


# ----------------------------------------------------------------------
# resubmit_jobs 후 handler 재무장 — 새 실행에서 다시 돈다
# ----------------------------------------------------------------------
def test_handler_rearmed_after_resubmit(qtbot, manager, fake_lsf, submitted):
    key = f"{submitted}_0"
    results = []
    manager.handler_finished.connect(lambda j, n, r: results.append(r))

    # 1차: RUN → DONE (final까지, _FINISHED 도달)
    fake_lsf.set_all("RUN")
    _poll(qtbot, manager, submitted)
    manager.add_handler(submitted, "c", lambda ctx: ctx.record.state.value)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000):
        manager.query_once(submitted)          # RUN 발화
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.handler_finished, timeout=10000,
                          check_params_cb=lambda j, n, r: r.final):
        manager.query_once(submitted)          # 최종

    # handler 등록 유지 (rearm 대비)
    assert (submitted, "c") in manager.handlers._handlers
    n_before = len(results)

    # resubmit → rearm이 status 리셋 → 새 실행에서 다시 돎
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(submitted, [key])
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.handler_finished, timeout=10000):
        manager.query_once(submitted)          # 재실행 RUN → 다시 발화
    assert len(results) > n_before


# ----------------------------------------------------------------------
# 검증 — 이름 중복 / 없는 jobset
# ----------------------------------------------------------------------
def test_handler_validation(qtbot, manager, submitted):
    manager.add_handler(submitted, "dup", lambda ctx: None)
    with pytest.raises(ValueError):                     # 이름 중복
        manager.add_handler(submitted, "dup", lambda ctx: None)
    from lsfmgr.errors import JobSetNotFoundError
    with pytest.raises(JobSetNotFoundError):            # 없는 jobset
        manager.add_handler("nope", "x", lambda ctx: None)
    manager.remove_handler(submitted, "dup")
