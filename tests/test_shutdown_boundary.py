"""shutdown 경계 — 종료 후 호출 / 진행 중 종료 / 최종 실행 보존.

GUI 종료 중에 뭔가 진행 중인 상황은 흔한데 진입점이 많다. 조용히 스레드를
만들거나, 정체불명 예외를 내거나, 종료가 하염없이 밀리면 안 된다.
"""
from __future__ import annotations

import threading
import time
import os
import sys
import subprocess
from datetime import datetime

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.errors import LsfmgrError
from lsfmgr.handlers import JobSetHandlerService
from lsfmgr.states import JobRecord, JobSetRecord
from tests.conftest import submit_cmds

EXPECTED = (LsfmgrError, ValueError, TypeError)


def test_shutdown_before_qapplication_exists():
    result = subprocess.run(
        [sys.executable, "-c",
         "from lsfmgr import LsfJobManager; "
         "mgr = LsfJobManager(); mgr.shutdown(); "
         "assert not mgr.polling._thread.isRunning()"],
        capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_shutdown_cancels_real_query_and_reaps_process(qtbot, tmp_path):
    """실제 bjobs 클라이언트가 오래 걸려도 종료 시 취소·회수한다."""
    marker = tmp_path / "query.pid"
    code = ("import os, pathlib, sys, time; "
            "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
            "time.sleep(60); print('123;RUN;-')")
    mgr = LsfJobManager(config=LsfConfig(
        bjobs_path=[sys.executable, "-c", code, str(marker)]))
    js = mgr.create_jobset(["wrapper"], job_keys=["a"])
    mgr.store.transition(js.id, "a", JobState.PEND, job_id=123)
    try:
        mgr.start_polling(js, 5)
        qtbot.waitUntil(lambda: marker.exists() and bool(marker.read_text()),
                        timeout=5000)
        pid = int(marker.read_text())
        t0 = time.monotonic()
        mgr.shutdown()
        assert time.monotonic() - t0 < 5
        assert not mgr.polling._thread.isRunning()
        assert mgr.polling._worker._timers == {}
        if os.name == "posix":
            with pytest.raises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)  # 종료만 한 좀비도 남아서는 안 된다
        # 취소를 job 부재로 해석하지 않고, 종료 뒤 새 프로세스도 띄우지 않는다.
        assert js.jobs()[0].state is JobState.PEND
        marker.unlink()
        assert mgr.command.bjobs_by_ids([123]) == ([], {123})
        assert not marker.exists()
    finally:
        mgr.shutdown()
        mgr.polling._thread.wait(65000)


def test_shutdown_waits_for_custom_runner_beyond_old_deadline(qtbot):
    """주입 Runner가 기존 15초 종료 예산보다 늦게 반환해도 반드시 join."""
    from lsfmgr.command import CommandResult

    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def runner(argv, timeout, cwd=None):
        entered.set()
        release.wait(20)
        finished.set()
        return CommandResult(0, "123;RUN;-", "")

    mgr = LsfJobManager(runner=runner)
    js = mgr.create_jobset(["wrapper"], job_keys=["a"])
    mgr.store.transition(js.id, "a", JobState.PEND, job_id=123)
    timer = threading.Timer(16, release.set)
    try:
        mgr.query_once(js)
        assert entered.wait(5)
        timer.start()
        mgr.shutdown()
        assert finished.is_set()
        assert not mgr.polling._thread.isRunning()
        assert mgr.polling._worker._timers == {}
    finally:
        release.set()
        timer.cancel()
        mgr.shutdown()
        mgr.polling._thread.wait(5000)


def test_no_public_api_misbehaves_after_shutdown(qtbot, fake_lsf):
    """정체불명 예외도, 새 스레드도 없어야 한다 (종료 후 큐에 남은 Qt
    이벤트가 이 API들을 부르는 것은 정상 상황이다)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    js = mgr.create_jobset(["mytool a.sp", "mytool b.sp"], job_keys=["a", "b"])
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(js, auto_poll=False)
    before = threading.active_count()
    mgr.shutdown()
    qtbot.wait(200)

    calls = [
        ("submit", lambda: mgr.submit(js, auto_poll=False)),
        ("kill", lambda: mgr.kill(js)),
        ("kill_jobs", lambda: mgr.kill_jobs(js, ["a"])),
        ("cancel_submit", lambda: mgr.cancel_submit(js)),
        ("start_polling", lambda: mgr.start_polling(js, 5.0)),
        ("stop_polling", lambda: mgr.stop_polling(js)),
        ("query_once", lambda: mgr.query_once(js)),
        ("summary", lambda: mgr.summary(js)),
        ("get_jobs", lambda: mgr.get_jobs(js)),
        ("can_submit", lambda: mgr.can_submit(js)),
        ("is_submitting", lambda: mgr.is_submitting(js)),
        ("is_killing", lambda: mgr.is_killing(js)),
        ("submit_state", lambda: mgr.submit_state(js)),
        ("kill_state", lambda: mgr.kill_state(js)),
        ("add_jobs", lambda: mgr.add_jobs(js, ["mytool c.sp"], job_keys=["c"])),
        ("set_user_data", lambda: mgr.set_user_data(js, "a", {"x": 1})),
        ("add_handler", lambda: mgr.add_handler(js, "h", lambda c: None)),
        ("remove_handler", lambda: mgr.remove_handler(js, "h")),
        ("detect_lost", lambda: mgr.detect_lost(js)),
        ("total_summary", lambda: mgr.total_summary()),
        ("list_jobsets", lambda: mgr.list_jobsets()),
        ("remove_jobs", lambda: mgr.remove_jobs(js, ["b"])),
        ("clear_jobs", lambda: mgr.clear_jobs(js)),
        ("remove_jobset", lambda: mgr.remove_jobset(js)),
        ("shutdown(재호출)", lambda: mgr.shutdown()),
    ]
    weird = []
    for name, fn in calls:
        try:
            fn()
        except EXPECTED:
            pass
        except Exception as e:                # noqa: BLE001
            weird.append(f"{name}: {type(e).__name__}: {e}")
    qtbot.wait(500)
    assert not weird, "\n".join(weird)
    assert threading.active_count() <= before, "종료 후 호출이 스레드를 만들었다"


def test_async_entrypoints_warn_instead_of_silently_doing_nothing(
        qtbot, fake_lsf, caplog):
    """폴링 스레드가 죽은 뒤의 start_polling/query_once, tick이 안 도는
    add_handler는 **조용히 무시되면 안 된다** — 앱은 켜진 줄 안다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
    mgr.shutdown()
    with caplog.at_level("WARNING", logger="lsfmgr.manager"):
        mgr.start_polling(js, 5.0)
        mgr.query_once(js)
        mgr.add_handler(js, "h", lambda c: None)
    msgs = [r.message for r in caplog.records if "shutdown 후" in r.message]
    assert len(msgs) == 3, msgs


@pytest.mark.parametrize("phase", ["gate", "submit", "kill", "handler",
                                   "post_process", "polling"])
def test_shutdown_during(qtbot, fake_lsf, phase):
    """무엇이 진행 중이든 종료가 끝나고 좀비 스레드가 없어야 한다."""
    def slow(argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] not in ("bjobs", "bkill"):
            time.sleep(0.05)
        return fake_lsf(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(poll_interval_s=5.0), runner=slow)
    base = {t.name for t in threading.enumerate()}
    js = mgr.create_jobset([f"mytool {i}.sp" for i in range(60)],
                           job_keys=[f"k{i}" for i in range(60)])
    if phase == "gate":
        mgr.submit(js, auto_poll=False,
                   pre_submit=lambda c: (time.sleep(2.0), True)[1])
    elif phase == "post_process":
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            mgr.submit(js, auto_poll=False,
                       post_process=lambda r: time.sleep(1.5))
        mgr.kill(js)
        qtbot.wait(300)
        mgr.query_once(js)
    elif phase == "handler":
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            mgr.submit(js, auto_poll=False)
        mgr.add_handler(js, "h", lambda c: time.sleep(0.5),
                        start_states={JobState.PEND})
        mgr.query_once(js)
    else:
        mgr.submit(js, auto_poll=(phase == "polling"))
        if phase == "kill":
            qtbot.wait(150)
            mgr.kill(js)
    qtbot.wait(200)
    t0 = time.perf_counter()
    mgr.shutdown()
    elapsed = time.perf_counter() - t0
    qtbot.wait(400)
    left = {n for n in ({t.name for t in threading.enumerate()} - base)
            if n.startswith("lsfmgr")}
    assert not left, f"[{phase}] 잔여 스레드 {left}"
    assert elapsed < 20.0, f"[{phase}] shutdown이 {elapsed:.0f}초"


# ----------------------------------------------------------------------
# handler 종료 — 중간 실행분은 버리고 최종 실행분은 지킨다
# ----------------------------------------------------------------------
def _handler_svc(n, state):
    store = InMemoryStore()
    store.store_insert_jobset(JobSetRecord(jobset_id="js", intended_count=n,
                                           created_at=datetime.now()))
    store.store_add_jobs([
        JobRecord(job_id=1000 + i, array_index=None, jobset_id="js",
                  job_key=f"k{i}", state=state, command="x")
        for i in range(n)])
    return store, JobSetHandlerService(store)


def test_shutdown_keeps_final_handler_runs(qtbot):
    """최종 실행은 '이 job은 이렇게 끝났다'는 마지막 수집이라 버리면 안 된다."""
    n = 40
    _store, svc = _handler_svc(n, JobState.DONE)
    ran, lock = [], threading.Lock()

    def fn(ctx):
        time.sleep(0.02)
        with lock:
            ran.append((ctx.job_key, ctx.final))

    svc.add_handler("js", "h", fn)
    svc.tick("js")
    svc.shutdown()
    assert len(ran) == n, f"최종 실행이 {len(ran)}건만 돌았다"
    assert all(final for _, final in ran)


def test_shutdown_drops_pending_intermediate_runs(qtbot):
    """중간 실행은 '이번 폴링 사이클의 수집'이라 종료 시점에 따라잡을 이유가
    없다 — 그대로 두면 job 60건 x 0.5초에 shutdown이 7.3초였다(실측)."""
    n = 40
    _store, svc = _handler_svc(n, JobState.RUN)
    ran, lock = [], threading.Lock()

    def fn(ctx):
        time.sleep(0.05)
        with lock:
            ran.append(ctx.job_key)

    svc.add_handler("js", "h", fn)
    svc.tick("js")
    t0 = time.perf_counter()
    svc.shutdown()
    elapsed = time.perf_counter() - t0
    assert len(ran) < n, f"중간 실행 {len(ran)}건을 다 돌았다(버려야 한다)"
    assert elapsed < 1.0, f"shutdown이 {elapsed:.1f}초"
