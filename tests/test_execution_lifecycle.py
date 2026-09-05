"""실행 식별·결과 반영·완료 통지 경계의 계약."""
import threading
from collections import Counter
from queue import Queue

import pytest

from lsfmgr import JobState, LsfJobManager
from lsfmgr.command import CommandResult
from lsfmgr.killer import _KillTask
from tests.fake_lsf import FakeLsf


@pytest.mark.parametrize("scope", ["jobset", "keys", "raw", "raw_jobset"])
@pytest.mark.parametrize("policy", ["optimistic", "actual"])
def test_kill_verification_cannot_publish_natural_completion(qtbot, monkeypatch, scope, policy):
    mgr = LsfJobManager(runner=FakeLsf(), kill_max_retry=0, kill_status_policy=policy)
    ready, release = threading.Event(), threading.Event()
    original = _KillTask._mark_killed
    completed, posts = [], []

    def before_mark(task, *args):
        ready.set()
        assert release.wait(5)
        return original(task, *args)

    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False, post_process=posts.append)
        mgr.stop_polling(js)
        js.jobset_finished.connect(lambda summary: completed.append(summary))
        monkeypatch.setattr(_KillTask, "_mark_killed", before_mark)
        if scope == "jobset":
            mgr.kill(js, verify=True)
        elif scope == "keys":
            mgr.kill_jobs(js, ["a"], verify=True)
        else:
            mgr.kill_jobs([js.jobs()[0].job_id], verify=True,
                          jobset_id=js.id if scope == "raw_jobset" else None)
        assert ready.wait(3)
        # A regular poll can observe verified EXIT before kill attribution.
        with qtbot.waitSignal(mgr.polling.updated, timeout=3000):
            mgr.query_once(js)
        rec = js.jobs()[0]
        assert rec.state is JobState.EXIT and not rec.killed
        assert not completed and not posts
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            release.set()
        assert js.jobs()[0].killed
        assert completed == [], completed
        qtbot.waitUntil(lambda: bool(posts), timeout=3000)
        assert all(rec.killed for rec in posts[0])
    finally:
        release.set()
        mgr.shutdown()


def test_scheduler_rejects_snapshot_from_before_stop_resume(tmp_path, monkeypatch):
    from mocklsf import cli, config
    from mocklsf.db import Database
    from mocklsf.models import Job, RUN
    from mocklsf.scheduler import Scheduler

    monkeypatch.setattr(config, "MOCKLSF_HOME", str(tmp_path))
    monkeypatch.setattr(config, "JOB_OUT_DIR", str(tmp_path / "jobout"))
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setattr(config, "FORWARD_CLUSTERS", [])
    db = Database()
    try:
        db.insert_jobs([Job(
            job_id=1000, user="test", command="simulate", queue="normal",
            from_host="mockmaster", job_name="a", submit_time=1, stat=RUN,
            exec_host="hostA", start_time=10, finish_time=110,
            suspend_at=5, suspend_secs=20)])
        original = db.jobs_in_states
        fired = []

        def snapshot_then_stop_resume(states):
            jobs = original(states)
            if RUN in states and not fired:
                fired.append(True)
                # Independent CLI connections execute legitimate mutations.
                monkeypatch.setattr(cli.time, "time", lambda: 20.0)
                assert cli.cmd_bstop(["1000"]) == 0
                monkeypatch.setattr(cli.time, "time", lambda: 30.0)
                assert cli.cmd_bresume(["1000"]) == 0
                assert db.one_element(1000, None).finish_time == 120.0
            return jobs

        monkeypatch.setattr(db, "jobs_in_states", snapshot_then_stop_resume)
        scheduler = Scheduler(db)
        scheduler.tick(now=20)
        after_race = db.one_element(1000, None)
        scheduler.tick(now=111)
        current = db.one_element(1000, None)
        assert (current.stat, current.finish_time) == (RUN, 120.0), (
            after_race.stat, after_race.finish_time, current.stat,
            current.finish_time)
    finally:
        db.close()


def test_global_kill_keeps_identical_keys_in_different_jobsets_separate(qtbot, monkeypatch):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, kill_max_retry=0)
    try:
        sets = [mgr.create_jobset(["wrapper task"], job_keys=["a"])
                for _ in range(2)]
        for js in sets:
            with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
                mgr.submit(js, auto_poll=False)
        first, second = [js.jobs()[0].job_id for js in sets]
        original = mgr.command.runner

        def partial_kill(argv, timeout, cwd=None):
            if argv[0] == "bkill":
                with fake.lock:
                    fake.jobs[str(first)].stat = "EXIT"
                    fake.jobs[str(first)].exit_code = 130
                return CommandResult(
                    255, f"Job <{first}> is being terminated\n",
                    f"Job <{second}>: Operation not permitted\n")
            return original(argv, timeout, cwd)

        monkeypatch.setattr(mgr.command, "runner", partial_kill)
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as signal:
            mgr.kill_jobs([first, second], verify=True)
        report = signal.args[1]
        assert report.still_alive == 1
        untouched = sets[1].jobs()[0]
        assert untouched.state is JobState.PEND and not untouched.killed
        killed = sets[0].jobs()[0]
        assert (killed.state, killed.killed) == (JobState.EXIT, True)
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("global_kill", [False, True])
@pytest.mark.parametrize("response", ["already finished", "Operation not permitted"])
def test_kill_rechecks_natural_completion_without_another_poll(
        qtbot, monkeypatch, global_kill, response):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, kill_max_retry=0)
    ready, release = threading.Event(), threading.Event()
    original_mark = _KillTask._mark_killed
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        jid = js.jobs()[0].job_id
        fake.set_all("DONE", 0)
        original_runner = mgr.command.runner

        def runner(argv, timeout, cwd=None):
            if argv[0] == "bkill":
                return CommandResult(255, "", f"Job <{jid}>: {response}\n")
            return original_runner(argv, timeout, cwd)

        def pause(task, *args):
            ready.set()
            assert release.wait(5)
            return original_mark(task, *args)

        monkeypatch.setattr(mgr.command, "runner", runner)
        monkeypatch.setattr(_KillTask, "_mark_killed", pause)
        completed = []
        js.jobset_finished.connect(completed.append)
        if global_kill:
            mgr.kill_jobs([jid], verify=True)
        else:
            mgr.kill(js, verify=True)
        assert ready.wait(3)
        with qtbot.waitSignal(mgr.polling.updated, timeout=3000):
            mgr.query_once(js)
        assert completed == []
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            release.set()
        assert completed == [{"total": 1, "DONE": 1}]
        assert not js.jobs()[0].killed
    finally:
        release.set()
        mgr.shutdown()


def test_overlapping_global_kills_keep_completion_blocked_until_both_deliver(qtbot, monkeypatch):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, kill_max_retry=0)
    pending = Queue()
    releases = []
    original_mark = _KillTask._mark_killed
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        jid = js.jobs()[0].job_id
        fake.set_all("DONE", 0)
        original_runner = mgr.command.runner

        def runner(argv, timeout, cwd=None):
            if argv[0] == "bkill":
                return CommandResult(255, "", f"Job <{jid}>: already finished\n")
            return original_runner(argv, timeout, cwd)

        def pause(task, *args):
            release = threading.Event()
            releases.append(release)
            pending.put(release)
            assert release.wait(5)
            return original_mark(task, *args)

        monkeypatch.setattr(mgr.command, "runner", runner)
        monkeypatch.setattr(_KillTask, "_mark_killed", pause)
        completed = []
        js.jobset_finished.connect(completed.append)
        mgr.kill_jobs([jid], verify=True)
        first = pending.get(timeout=3)
        mgr.kill_jobs([jid], verify=True)
        second = pending.get(timeout=3)
        with qtbot.waitSignal(mgr.polling.updated, timeout=3000):
            mgr.query_once(js)
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            first.set()
        assert not completed
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            second.set()
        assert completed == [{"total": 1, "DONE": 1}]
    finally:
        for release in releases:
            release.set()
        mgr.shutdown()


def test_resubmit_rearms_only_records_actually_reset(qtbot, monkeypatch):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake)
    try:
        js = mgr.create_jobset(["wrapper a", "wrapper b"], job_keys=["a", "b"])
        calls = []
        mgr.add_handler(js, "collect", lambda ctx: calls.append(ctx.job_id))
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        old = {r.job_key: r.job_id for r in js.jobs()}
        fake.set_all("DONE", 0)
        mgr.query_once(js)
        qtbot.waitUntil(lambda: len(calls) == 2, timeout=3000)
        original = mgr.store.transition

        def reject_one_reset(jsid, key, state, **fields):
            if key == "b" and state is JobState.SUBMITTING:
                raise RuntimeError("reset failed")
            return original(jsid, key, state, **fields)

        monkeypatch.setattr(mgr.store, "transition", reject_one_reset)
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            mgr.submit(js, auto_poll=False)
        current = {r.job_key: r.job_id for r in js.jobs()}
        fake.set_all("DONE", 0)
        with qtbot.waitSignal(mgr.handler_finished, timeout=3000):
            mgr.query_once(js)
        mgr.handlers._pool.waitForDone(3000)
        assert Counter(calls) == Counter([old["a"], old["b"], current["a"]])
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("edit", ["remove", "clear"])
def test_deleted_jobs_leave_no_handler_execution_bookkeeping(qtbot, edit):
    mgr = LsfJobManager(runner=FakeLsf())
    try:
        js = mgr.create_jobset(["wrapper a", "wrapper b"], job_keys=["a", "b"])
        mgr.add_handler(js, "collect", lambda ctx: None)
        for key in ("a", "b"):
            mgr.store.transition(js.id, key, JobState.DONE)
        mgr.handlers.tick(js.id)
        mgr.handlers._pool.waitForDone(3000)
        if edit == "remove":
            mgr.remove_jobs(js, ["a"])
        else:
            mgr.clear_jobs(js)
        handler = mgr.handlers._handlers[(js.id, "collect")]
        assert set(handler.status) == ({"b"} if edit == "remove" else set())
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("dwell", [0, 0.02])
def test_reset_delivery_preserves_new_run_metadata_already_published(qtbot, monkeypatch, dwell):
    mgr = LsfJobManager(runner=FakeLsf(), min_state_dwell_s=dwell)
    ready, release = threading.Event(), threading.Event()
    original = mgr.store.transition
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        seen = []
        js.jobs_updated.connect(lambda recs: seen.extend(recs))

        def pause_after_pend(jsid, key, state, **fields):
            result = original(jsid, key, state, **fields)
            if state is JobState.PEND:
                ready.set()
                assert release.wait(5)
            return result

        monkeypatch.setattr(mgr.store, "transition", pause_after_pend)
        mgr.submit(js, auto_poll=False)
        assert ready.wait(3)
        mgr.set_user_data(js, "a", {"new": 1})
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            release.set()
        if mgr._pacer is not None:
            mgr._pacer.stop()  # 남은 표시를 전부 전달한 결과 검사
        assert seen[-1].user_data == {"new": 1}
    finally:
        release.set()
        mgr.shutdown()


def test_reset_delivery_keeps_missing_observation_of_new_run(qtbot, monkeypatch):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, lost_after_missing_polls=2)
    ready, release, queried = (threading.Event() for _ in range(3))
    original_transition = mgr.store.transition
    original_streaks = mgr.querier._set_streaks
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])

        def pause_after_pend(jsid, key, state, **fields):
            result = original_transition(jsid, key, state, **fields)
            if state is JobState.PEND:
                ready.set()
                assert release.wait(5)
            return result

        def note_query(jsid, streaks):
            original_streaks(jsid, streaks)
            queried.set()

        monkeypatch.setattr(mgr.store, "transition", pause_after_pend)
        monkeypatch.setattr(mgr.querier, "_set_streaks", note_query)
        mgr.submit(js, auto_poll=False)
        assert ready.wait(3)
        with fake.lock:
            for job in fake.jobs.values():
                job.vanished = True
        mgr.query_once(js)
        assert queried.wait(3)  # main의 records_reset 전달 전에 poll worker가 조회
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            release.set()
        with qtbot.waitSignal(mgr.polling.updated, timeout=3000):
            mgr.query_once(js)
        assert js.jobs()[0].state is JobState.LOST
    finally:
        release.set()
        mgr.shutdown()
