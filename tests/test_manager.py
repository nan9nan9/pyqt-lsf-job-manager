"""Facade 통합 테스트 — 수명 관리 / 세션 복원 / GUI 응답성 (수용 기준 7·12·13)."""
from __future__ import annotations

import threading
import time

import pytest

from lsfmgr import (
    InMemoryStore,
    JobSpec,
    JobState,
    LsfConfig,
    LsfJobManager,
)
from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
def test_shutdown_joins_threads(qtbot, fake_lsf, config):
    before = set(threading.enumerate())
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    jobs = [JobSpec(command=f"r {i}") for i in range(20)]
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        jsid = submit_cmds(mgr, jobs).id
    mgr.start_polling(jsid, interval_s=0.2)
    qtbot.wait(300)
    mgr.shutdown()
    assert not mgr.polling._thread.isRunning()
    # QThreadPool 스레드는 곧 소멸 — 새로 살아남은 non-daemon 스레드 없어야 함
    qtbot.waitUntil(
        lambda: all(t in before or not t.is_alive() or t.daemon
                    for t in threading.enumerate()), timeout=10000)


def test_shutdown_idempotent(manager):
    manager.shutdown()
    manager.shutdown()          # 2회 호출해도 안전


def test_no_coredump_on_exit_without_shutdown(tmp_path):
    """앱이 shutdown()을 안 부르고, 이벤트루프도 안 돌리고 종료해도 core dump가
    안 나야 한다 — atexit 안전망이 폴링 QThread/워커를 정리한다 (CS-8).
    실제 crash는 서브프로세스 종료 코드로만 잡히므로 별도 프로세스로 실행."""
    import os
    import subprocess
    import sys
    import textwrap

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = textwrap.dedent("""
        import os, sys
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        sys.path.insert(0, os.path.join(REPO, "tests"))
        from qtpy.QtWidgets import QApplication
        from lsfmgr import LsfJobManager, InMemoryStore, JobSpec, LsfConfig
        from fake_lsf import FakeLsf
        app = QApplication(sys.argv)
        mgr = LsfJobManager(store=InMemoryStore(),
                            config=LsfConfig(poll_interval_s=5), runner=FakeLsf())
        js = mgr.create_jobset([f"r {i}" for i in range(20)], wrapper=False)
        mgr.submit(js, auto_poll=False)
        mgr.start_polling(js.id, 0.2)    # 폴링 QThread 가동 중
        # shutdown() 미호출 + app.exec() 미실행 → 그냥 종료
    """).replace("REPO", repr(repo))
    r = subprocess.run([sys.executable, "-c", script],
                       capture_output=True, timeout=30)
    # 정상 종료(0)여야 한다. segfault면 음수(-11), abort면 -6.
    assert r.returncode == 0, (
        f"비정상 종료 rc={r.returncode}\n{r.stderr.decode(errors='replace')}")


def test_shutdown_during_submit_preserves_job_ids(qtbot, fake_lsf, config):
    """CS-8 — shutdown 시 진행 중 bsub의 job_id 유실 없음."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    jobs = [JobSpec(command=f"r {i}") for i in range(50)]
    jsid = submit_cmds(mgr, jobs, workers=2, rate_limit_per_s=30).id
    qtbot.wait(200)             # 일부만 submit된 시점
    mgr.shutdown()
    # submit 성공한 job 수 == store에 job_id 확보된 레코드 수
    submitted_in_lsf = len(fake_lsf.jobs)
    with_id = [r for r in mgr.store.get_jobs(jsid) if r.job_id is not None]
    assert len(with_id) == submitted_in_lsf


# ----------------------------------------------------------------------
# 세션 복원 end-to-end (수용 기준 12)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# GUI 응답성 — polling+submit 중 이벤트 루프 정지 없음 (수용 기준 7 축소판)
# ----------------------------------------------------------------------
def test_event_loop_not_blocked_during_bulk_submit(qtbot, fake_lsf):
    cfg = LsfConfig(retry_delay_s=0.05)

    # bsub마다 5ms 걸리는 느린 LSF 시뮬레이션
    original = fake_lsf.__call__

    def slow_runner(argv, timeout, cwd=None):
        time.sleep(0.005)
        return original(argv, timeout)

    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=slow_runner)
    try:
        ticks = []
        from lsfmgr.qt import QTimer
        timer = QTimer()
        timer.setInterval(20)
        timer.timeout.connect(lambda: ticks.append(time.monotonic()))
        timer.start()

        jobs = [JobSpec(command=f"r {i}") for i in range(300)]
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            submit_cmds(mgr, jobs, workers=8)
        timer.stop()

        # main 스레드 이벤트 루프가 100ms 이상 정지한 구간이 없어야 함
        gaps = [b - a for a, b in zip(ticks, ticks[1:])]
        assert gaps, "timer가 한 번도 tick하지 않음 — 이벤트 루프 정지"
        assert max(gaps) < 0.1, f"이벤트 루프 최대 정지 {max(gaps)*1000:.0f}ms"
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 동시 수행 — submit + polling + kill (수용 기준 9 축소판)
# ----------------------------------------------------------------------
def test_concurrent_submit_poll_kill(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
            a = submit_cmds(mgr, [JobSpec(command=f"a {i}") for i in range(30)])
        mgr.start_polling(a, interval_s=0.1)

        # polling 도중 새 submit + kill 동시 진행
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
            b = submit_cmds(mgr, [JobSpec(command=f"b {i}") for i in range(30)])
        with qtbot.waitSignal(mgr.kill_finished, timeout=15000):
            mgr.kill(a)

        qtbot.wait(300)                       # polling 몇 사이클 더
        for jsid in (a, b):
            s = mgr.summary(jsid)
            assert s["total"] == 30
            assert sum(v for k, v in s.items() if k != "total") == 30
    finally:
        mgr.shutdown()
