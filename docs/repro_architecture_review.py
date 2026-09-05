"""521db95에서 실패하는 리뷰 재현 사례. 실행 방법은 인접 리뷰 문서 참고."""

import subprocess
import sys
import threading
import time

import pytest

from lsfmgr import JobState, LsfJobManager
from lsfmgr.command import CommandResult
from tests.fake_lsf import FakeLsf


def submit_one(qtbot, mgr, command="wrapper task"):
    js = mgr.create_jobset([command], job_keys=["a"])
    with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
        mgr.submit(js, auto_poll=False)
    return js


def test_kill_state_snapshot_catches_submitting(qtbot):
    fake = FakeLsf()
    entered, release = threading.Event(), threading.Event()

    def runner(argv, timeout, cwd=None):
        if argv[0] == "wrapper":
            entered.set()
            assert release.wait(3)
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(runner=runner)
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        mgr.submit(js, auto_poll=False)
        assert entered.wait(3)
        assert js.jobs()[0].state is JobState.SUBMITTING
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as result:
            mgr.kill(js, only_state=JobState.SUBMITTING)
            release.set()
        assert not fake.alive_jobs(), (result.args[1], js.jobs(), fake.calls)
    finally:
        release.set()
        mgr.shutdown()


@pytest.mark.parametrize("policy", ["optimistic", "actual"])
def test_verify_preserves_kill_origin(qtbot, policy):
    mgr = LsfJobManager(runner=FakeLsf(), kill_status_policy=policy)
    try:
        js = submit_one(qtbot, mgr)
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            mgr.kill(js, verify=True)
        rec = js.jobs()[0]
        assert rec.state is JobState.EXIT
        assert rec.killed, rec
    finally:
        mgr.shutdown()


def test_partial_array_kill_is_not_success(qtbot):
    fake = FakeLsf()

    def runner(argv, timeout, cwd=None):
        if argv[0] == "bkill":
            pid = argv[1]
            fake.set_job(int(pid), "EXIT", 137, array_index=1)
            return CommandResult(
                255,
                f"Job <{pid}[1]> is being terminated\n",
                f"Job <{pid}[2]>: Failed to send signal: temporarily unavailable\n",
            )
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(runner=runner, kill_max_retry=0)
    try:
        js = submit_one(qtbot, mgr, "bsub -J arr[1-2] task")
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as result:
            mgr.kill(js)
        assert result.args[1].unconfirmed > 0, (
            result.args[1], fake.alive_jobs(), js.jobs()
        )
        assert js.jobs()[0].state.is_on_lsf
    finally:
        mgr.shutdown()


def test_stale_poll_cannot_override_replacement_signal(qtbot):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake)
    try:
        js = submit_one(qtbot, mgr)
        fake.set_all("DONE", 0)
        mgr.query_once(js)
        deadline = time.monotonic() + 3
        # GUI가 바쁜 동안 Store는 갱신되지만 조회 신호는 Qt 큐에 대기한다.
        while js.jobs()[0].state is not JobState.DONE:
            assert time.monotonic() < deadline
            time.sleep(.001)
        seen = []
        js.jobs_updated.connect(lambda recs: seen.extend(recs))
        mgr.replace_jobs(js, ["wrapper new-task"], job_keys=["a"])
        qtbot.wait(50)
        assert seen[-1].command == js.jobs()[0].command, [
            (r.state, r.command) for r in seen
        ]
    finally:
        mgr.shutdown()


def test_mock_daemon_stop_does_not_claim_success_while_alive(tmp_path, monkeypatch):
    from mocklsf import daemon

    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import signal,time; "
         "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
         "print('ready', flush=True); time.sleep(30)"],
        stdout=subprocess.PIPE, text=True,
    )
    try:
        assert proc.stdout.readline().strip() == "ready"
        pid_path = tmp_path / "mocklsfd.pid"
        pid_path.write_text(str(proc.pid))
        monkeypatch.setattr(daemon.config, "PID_PATH", str(pid_path))
        stopped = daemon.stop()
        assert not stopped or proc.poll() is not None, (
            stopped, proc.poll(), pid_path.exists()
        )
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()
