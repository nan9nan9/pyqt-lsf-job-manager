"""post_process 후처리 — jobset이 전원 terminal에 도달하면 worker에서 1회 실행.

신호: post_processing_started → post_processing_finished(result).
완료 감지(폴링/query_once) 시점에 발화하며, 성공/실패 혼재와 무관하게 전원
terminal이면 실행된다. pre_submit(pre_processing)과 대칭.
"""
from __future__ import annotations

from lsfmgr import JobState, LsfJobManager


def _finish(manager, fake_lsf, js, state="DONE", code=0):
    fake_lsf.set_all(state, code)
    manager.query_once(js)          # 완료 감지 → post_process 발화 지점


# ----------------------------------------------------------------------
# 기본 — 전원 DONE 시 콜백 1회, 최종 레코드 전달, 결과 신호
# ----------------------------------------------------------------------
def test_post_process_runs_on_all_terminal(qtbot, manager, fake_lsf):
    seen = {}

    def collect(records):
        seen["n"] = len(records)
        seen["states"] = {r.state for r in records}
        return {"ok": sum(1 for r in records if r.state is JobState.DONE)}

    js = manager.create_jobset([f"customwrapper_sub r{i}.sp" for i in range(3)])
    order = []
    js.post_processing_started.connect(lambda: order.append("started"))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=collect, auto_poll=False)

    with qtbot.waitSignal(js.post_processing_finished, timeout=10000) as blk:
        _finish(manager, fake_lsf, js)

    assert order == ["started"]                 # started가 finished보다 먼저
    assert seen["n"] == 3
    assert seen["states"] == {JobState.DONE}
    assert blk.args[0] == {"ok": 3}             # 반환값이 finished로 전달


# ----------------------------------------------------------------------
# 실패 혼재여도 전원 terminal이면 실행 (post-processing은 결과 무관)
# ----------------------------------------------------------------------
def test_post_process_runs_on_mixed_terminal(qtbot, manager, fake_lsf):
    got = {}

    def collect(records):
        got["failed"] = [r.job_key for r in records if r.state is JobState.EXIT]
        return len(records)

    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=collect, auto_poll=False)

    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)
    fake_lsf.set_job(recs[1].job_id, "EXIT", 1)
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        manager.query_once(js)

    assert got["failed"] == [recs[1].job_key]   # EXIT도 terminal — 실행됨


# ----------------------------------------------------------------------
# 미완료(일부 RUN)면 실행 안 됨
# ----------------------------------------------------------------------
def test_post_process_not_run_while_active(qtbot, manager, fake_lsf):
    calls = []
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: calls.append(1), auto_poll=False)

    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)   # 1건만 종료, 나머지 RUN
    fake_lsf.set_job(recs[1].job_id, "RUN")
    manager.query_once(js)
    qtbot.wait(100)
    assert calls == []                          # 아직 전원 terminal 아님


# ----------------------------------------------------------------------
# 딱 한 번만 — 완료 후 폴링이 더 돌아도 재발화 없음
# ----------------------------------------------------------------------
def test_post_process_fires_once(qtbot, manager, fake_lsf):
    calls = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: calls.append(1), auto_poll=False)

    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    manager.query_once(js)                      # 완료 상태에서 한 번 더 조회
    manager.query_once(js)
    qtbot.wait(150)
    assert calls == [1]                         # 1회만


# ----------------------------------------------------------------------
# 예외 격리 → error_occurred + post_processing_finished(None)
# ----------------------------------------------------------------------
def test_post_process_exception_reported(qtbot, manager, fake_lsf):
    errs = []
    manager.error_occurred.connect(lambda j, m: errs.append(m))

    def boom(records):
        raise RuntimeError("후처리 실패!")

    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=boom, auto_poll=False)

    with qtbot.waitSignal(js.post_processing_finished, timeout=10000) as blk:
        _finish(manager, fake_lsf, js)

    assert blk.args[0] is None                  # 예외 시 결과 None
    assert any("후처리 실패" in m for m in errs)


# ----------------------------------------------------------------------
# 재제출 — post_process 없이 다시 submit하면 이전 무장 해제
# ----------------------------------------------------------------------
def test_post_process_disarmed_on_resubmit_without_callback(qtbot, manager, fake_lsf):
    calls = []
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, post_process=lambda r: calls.append("first"),
                       auto_poll=False)
    # 완료 전에 재제출(콜백 없이) — 이전 무장 해제되어야 함
    # (완료 후 재제출은 별 의미라 여기선 활성 상태에서 못 하니, 먼저 종료)
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000):
        _finish(manager, fake_lsf, js)
    assert calls == ["first"]

    calls.clear()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)     # 콜백 없음 → 무장 해제
    _finish(manager, fake_lsf, js)
    qtbot.wait(150)
    assert calls == []                          # 재발화 없음
