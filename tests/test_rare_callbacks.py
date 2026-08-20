"""앱 콜백이 오작동할 때 / array element 집계.

handler가 예외를 던지거나 자기를 제거하거나 manager로 재진입해도 폴링이
계속 돌아야 한다. post_process의 재진입도 마찬가지.
array는 레코드가 (id, None) 하나인데 조회가 element 행을 주므로
_aggregate_elements가 대표 상태를 만든다.
"""
import threading
import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.command import JobStatus
from tests.conftest import submit_cmds


# ---------------------------------------------------------------- ①
def test_library_slots_run_before_app_slots(qtbot, fake_lsf):
    """앱 slot이 터져도 **라이브러리 내부 부기**는 이미 끝나 있어야 한다.

    PyQt는 slot 하나가 터지면 그 emit의 **뒤 slot을 건너뛴다**(그리고
    프로세스를 abort한다 — docs/gui.md §0-5). manager가 내부 slot을
    __init__에서 **먼저** 연결하는 것이 그 방어다: 앱 연결은 항상 뒤에 붙는다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    sig_t = type(vars(type(mgr))["submit_started"])
    names = [k for k, v in vars(type(mgr)).items()
             if isinstance(v, sig_t) and not k.startswith("_")]
    seen = []
    mgr.jobs_updated.connect(lambda j, recs: seen.append(("app", len(recs))))
    handle_seen = []
    try:
        js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(5)],
                         auto_poll=False)
        js.jobs_updated.connect(lambda recs: handle_seen.append(len(recs)))
        qtbot.wait(600)
        mgr.query_once(js)
        qtbot.wait(400)
        # 핸들 중계(_h_jobs_updated)는 manager가 __init__에서 연결한 내부 slot —
        # 앱 slot보다 먼저 돌아야 한다.
        assert handle_seen, "핸들 중계가 안 돌았다"
        assert seen, "앱 slot이 안 돌았다"
        assert mgr.summary(js.id)["total"] == 5
        assert len(names) > 10
    finally:
        mgr.shutdown()


# ---------------------------------------------------------------- ②
def test_handler_that_misbehaves(qtbot, fake_lsf):
    """handler가 예외/자기 제거/manager 재진입을 해도 폴링이 계속 돌아야 한다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(poll_interval_s=5.0), runner=fake_lsf)
    seen = []
    try:
        js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(6)],
                         auto_poll=False)
        qtbot.wait(300)

        def boom(ctx):
            seen.append(("boom", ctx.job_key))
            raise RuntimeError("handler 폭발")

        def suicidal(ctx):
            seen.append(("suicide", ctx.job_key))
            mgr.remove_handler(js, "suicidal")     # worker에서 자기 제거

        def reentrant(ctx):
            seen.append(("reentrant", ctx.job_key))
            mgr.summary(js.id)                     # worker에서 조회 재진입
            mgr.get_jobs(js.id)

        for name, fn in (("boom", boom), ("suicidal", suicidal),
                         ("reentrant", reentrant)):
            mgr.add_handler(js, name, fn, start_states={JobState.PEND})
        for _ in range(4):
            mgr.query_once(js)
            qtbot.wait(250)
        kinds = {k for k, _ in seen}
        print(f"\nhandler 호출 {len(seen)}회, 종류={sorted(kinds)}")
        assert kinds == {"boom", "suicide", "reentrant"}
        assert mgr.summary(js.id)["total"] == 6
    finally:
        mgr.shutdown()


# ---------------------------------------------------------------- ③
def test_post_process_that_reenters(qtbot, fake_lsf):
    """post_process에서 manager를 다시 호출해도 안전해야 한다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    out = []
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])

        def pp(records):
            out.append(len(records))
            mgr.summary(js.id)                     # worker에서 재진입
            return {"n": len(records)}

        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False, post_process=pp)
        mgr.kill(js)
        qtbot.wait(500)
        mgr.query_once(js)          # 완료 감지는 폴링/query_once의 몫(README §4.5)
        qtbot.waitUntil(lambda: bool(out), timeout=20000)
        qtbot.wait(300)
        assert out == [1]
    finally:
        mgr.shutdown()


# ---------------------------------------------------------------- ④
def test_array_elements_folded_into_one_record(qtbot, fake_lsf):
    """레코드는 (id, None) 하나인데 조회가 element 행을 준다 —
    _aggregate_elements가 대표 상태를 만든다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    try:
        js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        qtbot.wait(300)
        jid = js.jobs()[0].job_id

        cases = [
            ([("RUN", None), ("DONE", 0)], JobState.RUN, "하나라도 RUN이면 RUN"),
            ([("DONE", 0), ("DONE", 0)], JobState.DONE, "전원 DONE"),
            ([("DONE", 0), ("EXIT", 7)], JobState.EXIT, "하나라도 EXIT"),
            ([("PEND", None), ("PEND", None)], JobState.PEND, "전원 PEND"),
        ]
        for elems, want, desc in cases:
            sts = [JobStatus(job_id=jid, array_index=i, state=JobState[s],
                             exit_code=c)
                   for i, (s, c) in enumerate(elems)]
            mgr.command.bjobs_by_ids = lambda ids, fresh=False, _s=sts: (_s, set())
            mgr.store.transition(js.id, "k0", JobState.PEND)   # 되돌리고
            mgr.querier.query(js.id)
            got = js.jobs()[0]
            print(f"  {desc:22s} → {got.state.value} (exit={got.exit_code})")
            assert got.state is want, desc
    finally:
        mgr.shutdown()
