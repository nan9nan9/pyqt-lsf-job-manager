"""2026-09-05 아키텍처 리뷰에서 확인한 결함의 회귀 테스트."""

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
@pytest.mark.parametrize("selection", ["whole", "keys", "ids"])
@pytest.mark.parametrize("terminal,exit_code", [("EXIT", 137), ("DONE", 0)])
def test_verify_preserves_kill_origin(qtbot, policy, selection, terminal, exit_code):
    fake = FakeLsf()

    def runner(argv, timeout, cwd=None):
        result = fake(argv, timeout, cwd)
        if argv[0] == "bkill":
            fake.set_all(terminal, exit_code)
        return result

    mgr = LsfJobManager(runner=runner, kill_status_policy=policy)
    try:
        js = submit_one(qtbot, mgr)
        seen, finished = [], []
        js.jobs_updated.connect(lambda recs: seen.extend(recs))
        mgr.jobset_finished.connect(lambda *args: finished.append(args))
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as result:
            if selection == "whole":
                mgr.kill(js, verify=True)
            elif selection == "keys":
                mgr.kill_jobs(js, ["a"], verify=True)
            else:
                mgr.kill_jobs([js.jobs()[0].job_id], jobset_id=js, verify=True)
        rec = js.jobs()[0]
        assert rec.state is JobState[terminal]
        assert rec.exit_code == exit_code
        assert rec.killed, rec
        assert result.args[1].changed == [rec]
        assert seen == [rec]
        assert finished == []
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("retry", [False, True])
def test_partial_array_kill_is_not_success(qtbot, retry):
    fake = FakeLsf()
    kill_calls = []
    progress = []

    def runner(argv, timeout, cwd=None):
        if argv[0] == "bkill":
            kill_calls.append(argv)
            if len(kill_calls) > 1:
                return fake(argv, timeout, cwd)
            pid = argv[1]
            fake.set_job(int(pid), "EXIT", 137, array_index=1)
            return CommandResult(
                255,
                f"Job <{pid}[1]> is being terminated\n",
                f"Job <{pid}[2]>: Failed to send signal: temporarily unavailable\n",
            )
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(runner=runner, kill_max_retry=int(retry),
                        kill_retry_delay_s=.01)
    mgr.kill_progress.connect(
        lambda _jsid, done, total: progress.append((done, total)))
    try:
        js = submit_one(qtbot, mgr, "bsub -J arr[1-2] task")
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as result:
            mgr.kill(js)
        report = result.args[1]
        assert len(kill_calls) == 1 + int(retry)
        assert progress and all(0 <= done <= total for done, total in progress)
        if retry:
            assert report.unconfirmed == 0 and not report.errors
            assert not fake.alive_jobs()
            assert js.jobs()[0].state is JobState.EXIT
        else:
            assert report.unconfirmed == 1 and report.errors
            assert len(fake.alive_jobs()) == 1
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
        assert stopped is False
        assert proc.poll() is None
        assert pid_path.read_text() == str(proc.pid)
        assert daemon.is_running()
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()


def test_only_state_keeps_selected_keys_after_state_changes(qtbot, monkeypatch):
    from lsfmgr.killer import _KillTask

    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake)
    entered, release = threading.Event(), threading.Event()
    quiesce = _KillTask._quiesce

    def wait_before_plan(task, scope, errors):
        quiesce(task, scope, errors)
        entered.set()
        assert release.wait(3)

    monkeypatch.setattr(_KillTask, "_quiesce", wait_before_plan)
    try:
        js = mgr.create_jobset(["wrapper a", "wrapper b"], job_keys=["a", "b"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        a, b = js.jobs()
        fake.set_job(b.job_id, "RUN")
        mgr.querier.query(js.id)
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            mgr.kill(js, only_state=JobState.PEND)
            assert entered.wait(3)
            fake.set_job(a.job_id, "RUN")
            fake.set_job(b.job_id, "PEND")
            mgr.querier.query(js.id)
            release.set()
        recs = {r.job_key: r for r in js.jobs()}
        assert recs["a"].state is JobState.EXIT and recs["a"].killed
        assert recs["b"].state is JobState.PEND and not recs["b"].killed
        assert [call for call in fake.calls if call[0] == "bkill"] == [
            ["bkill", str(a.job_id)]]
    finally:
        release.set()
        mgr.shutdown()


@pytest.mark.parametrize("dwell", [0, .05])
@pytest.mark.parametrize("edit", ["replace", "upsert", "remove", "clear", "jobset"])
def test_queued_results_do_not_revive_old_jobs(qtbot, dwell, edit):
    """미제출 job도 같은 key/command로 교체할 수 있어 LSF ID만으로는 부족하다."""
    mgr = LsfJobManager(runner=FakeLsf(), min_state_dwell_s=dwell)
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        old = js.jobs()[0]
        seen, summaries, lost = [], [], []
        mgr.jobs_updated.connect(lambda _jsid, recs: seen.extend(recs))
        mgr.jobset_updated.connect(lambda _jsid, summary: summaries.append(summary))
        mgr.job_lost.connect(lambda _jsid, rec: lost.append(rec))

        def post_old_results():
            mgr.submitter.jobs_changed.emit(js.id, [old])
            mgr.polling.updated.emit(js.id, {"total": 99, "DONE": 99}, [old])
            mgr.polling.lost.emit(js.id, old)

        worker = threading.Thread(target=post_old_results)
        worker.start()
        worker.join()
        if edit in ("replace", "upsert"):
            getattr(mgr, edit + "_jobs")(js, [old.command], job_keys=["a"])
        elif edit == "remove":
            mgr.remove_jobs(js, ["a"])
        elif edit == "clear":
            mgr.clear_jobs(js)
        else:
            mgr.remove_jobset(js, force=True)
        seen.clear()
        summaries.clear()
        qtbot.wait(150)
        assert seen == []
        assert lost == []
        if edit == "jobset":
            assert summaries == []
        else:
            assert summaries and all(s == mgr.summary(js) for s in summaries)
    finally:
        mgr.shutdown()


def test_replacement_discards_already_paced_states(qtbot):
    mgr = LsfJobManager(runner=FakeLsf(), min_state_dwell_s=.05)
    try:
        js = submit_one(qtbot, mgr)
        seen = []
        mgr.jobs_updated.connect(lambda _jsid, recs: seen.extend(recs))
        replacement = mgr.replace_jobs(js, ["wrapper next"], job_keys=["a"], force=True)
        qtbot.wait(200)
        assert seen == replacement
    finally:
        mgr.shutdown()


def test_resubmit_discards_undelivered_previous_cycle(qtbot):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake)
    try:
        js = submit_one(qtbot, mgr)
        fake.set_all("DONE", 0)
        old = mgr.querier.query(js.id).changed[0]
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        seen = []
        js.jobs_updated.connect(lambda recs: seen.extend(recs))
        worker = threading.Thread(target=lambda: mgr.polling.updated.emit(
            js.id, {"total": 1, "DONE": 1}, [old]))
        worker.start()
        worker.join()
        qtbot.wait(30)
        assert seen == []
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("action", ["stop", "restart", "reset"])
def test_mock_cli_reports_stop_failure_without_restarting_or_resetting(
        tmp_path, monkeypatch, capsys, action):
    from mocklsf import cli

    db_path = tmp_path / "mocklsf.db"
    db_path.write_bytes(b"keep running state")
    monkeypatch.setattr(cli.config, "DB_PATH", str(db_path))
    monkeypatch.setattr(cli.daemon, "stop", lambda: False)
    monkeypatch.setattr(cli.daemon, "is_running", lambda: True)
    started = []
    monkeypatch.setattr(cli.daemon, "start", lambda: started.append(True))
    assert cli.cmd_mocklsfd([action]) == 1
    assert started == []
    assert db_path.read_bytes() == b"keep running state"
    output = capsys.readouterr()
    assert "still running" in output.err
    assert output.out == ""
