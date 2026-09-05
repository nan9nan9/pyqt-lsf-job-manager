"""01e370c 추가 리뷰에서 확인한 상태 전달·완료·ID 정리 경합의 회귀 테스트."""
import threading
from dataclasses import replace

import pytest

from lsfmgr import JobState, LsfConfig, LsfJobManager
from lsfmgr.command import CommandResult
from tests.fake_lsf import FakeLsf


@pytest.fixture
def env(qtbot):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, config=LsfConfig(
        max_retry=0, kill_max_retry=0))
    try:
        yield mgr, fake
    finally:
        mgr.shutdown()


def _submit(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
        mgr.submit(js, auto_poll=False)


def _poll(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.jobset_updated, timeout=3000):
        mgr.query_once(js)


@pytest.mark.parametrize("dwell", [0, 0.03])
def test_delayed_submit_signal_does_not_regress_done(qtbot, monkeypatch, dwell):
    fake = FakeLsf()
    mgr = LsfJobManager(runner=fake, config=LsfConfig(min_state_dwell_s=dwell))
    ready, release = threading.Event(), threading.Event()
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        seen = []
        js.jobs_updated.connect(lambda recs: seen.extend(r.state for r in recs))
        original = mgr.store.transition

        def pause_after_pend(jobset_id, key, state, **fields):
            rec = original(jobset_id, key, state, **fields)
            if state is JobState.PEND:
                ready.set()  # Store 반영 후 제출 결과 배치에 넣기 전 선점
                assert release.wait(3)
            return rec

        monkeypatch.setattr(mgr.store, "transition", pause_after_pend)
        mgr.submit(js, auto_poll=False)
        assert ready.wait(3)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            release.set()
        _poll(qtbot, mgr, js)  # 이미 terminal이라 뒤의 조회도 변경분을 내지 않는다
        qtbot.wait(150)        # 표시 지연을 켠 경우 대기열까지 소진
        assert js.jobs()[0].state is JobState.DONE
        assert seen[-1] is JobState.DONE, seen
    finally:
        release.set()
        mgr.shutdown()


def test_previous_final_handler_does_not_lose_new_run_final(env, qtbot):
    mgr, fake = env
    js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
    _submit(qtbot, mgr, js)
    old_id = js.jobs()[0].job_id
    ready, release = threading.Event(), threading.Event()
    calls = []

    def handler(ctx):
        calls.append((ctx.job_id, ctx.final))
        if ctx.job_id == old_id:
            ready.set()
            assert release.wait(3)

    mgr.add_handler(js, "collector", handler)
    try:
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)
        assert ready.wait(3)
        _submit(qtbot, mgr, js)
        new_id = js.jobs()[0].job_id
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)  # 새 실행의 마지막 tick은 이전 final의 inflight에 막힘
        with qtbot.waitSignal(mgr.handler_finished, timeout=3000):
            release.set()
        qtbot.waitUntil(lambda: len(calls) == 2, timeout=3000)
        assert calls == [(old_id, True), (new_id, True)], calls
    finally:
        release.set()


def test_resubmit_forget_cannot_be_undone_before_id_reset(qtbot, monkeypatch):
    fake = FakeLsf()
    payload = {"jobs": []}
    mgr = LsfJobManager(runner=fake, config=LsfConfig(
        job_status_fetcher=lambda: payload, internal_refresh_min_s=0))
    ready, release = threading.Event(), threading.Event()
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        _submit(qtbot, mgr, js)
        old_id = js.jobs()[0].job_id
        payload["jobs"] = [{"dataId": str(old_id), "stat": "RUN"}]
        _poll(qtbot, mgr, js)
        # optimistic kill 뒤 조회원은 아직 RUN을 보고 있을 수 있다.
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            mgr.kill(js)
        original = mgr.command.forget_status

        def pause_after_forget(ids):
            original(ids)
            if old_id in ids and not ready.is_set():
                ready.set()
                assert release.wait(3)

        monkeypatch.setattr(mgr.command, "forget_status", pause_after_forget)
        mgr.submit(js, auto_poll=False)
        assert ready.wait(3)
        # forget 뒤 직접 조회를 끝낸다. Store의 옛 ID는 이때 이미 지워져야 한다.
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            mgr.kill_jobs([old_id], verify=True)
        with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
            release.set()
        assert js.jobs()[0].job_id != old_id
        assert old_id not in mgr.command.internal_status._interest
    finally:
        release.set()
        mgr.shutdown()


def test_already_finished_kill_preserves_natural_completion(env, qtbot, monkeypatch):
    mgr, fake = env
    js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
    _submit(qtbot, mgr, js)
    job_id = js.jobs()[0].job_id
    fake.set_all("DONE", 0)
    original = mgr.command.runner

    def already_finished(argv, timeout, cwd=None):
        if argv[0] == "bkill":
            return CommandResult(255, "", f"Job <{job_id}>: Job has already finished\n")
        return original(argv, timeout, cwd)

    monkeypatch.setattr(mgr.command, "runner", already_finished)
    completed = []
    js.jobset_finished.connect(lambda summary: completed.append(summary))
    with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
        mgr.kill(js)
    rec = js.jobs()[0]
    assert rec.state is JobState.DONE and not rec.killed
    _poll(qtbot, mgr, js)
    assert len(completed) == 1, completed


@pytest.mark.parametrize("initial,now,outcome,event", [
    ("RUN", 120, "DONE", "done"),
    ("RUN", 120, "EXIT", "exit"),
    ("PEND", 20, "DONE", "dispatch"),
    ("RUN", 20, "DONE", "suspend"),
    ("SSUSP", 40, "DONE", "resume"),
])
def test_mock_scheduler_does_not_publish_rejected_transition(
        tmp_path, monkeypatch, initial, now, outcome, event):
    from mocklsf import config
    from mocklsf.db import Database
    from mocklsf.models import EXIT, Job
    from mocklsf.scheduler import Scheduler

    monkeypatch.setattr(config, "MOCKLSF_HOME", str(tmp_path))
    monkeypatch.setattr(config, "JOB_OUT_DIR", str(tmp_path / "jobout"))
    monkeypatch.setattr(config, "FORWARD_CLUSTERS", [])
    db = Database(str(tmp_path / "state.db"))
    other = Database(str(tmp_path / "state.db"))
    try:
        db.insert_jobs([Job(
            job_id=1000, user="test", command="simulate", queue="normal",
            from_host="mockmaster", job_name="a", submit_time=1, stat=initial,
            exec_host="hostA", start_time=10, finish_time=110,
            suspend_at=5, suspend_secs=20, planned_outcome=outcome)])
        original = db.jobs_in_states
        fired = []

        def snapshot_then_kill(states):
            jobs = original(states)
            if initial in states and not fired:
                fired.append(True)
                killed = other.one_element(1000, None)
                killed.stat, killed.exit_code, killed.finish_time = EXIT, 130, 12
                assert other.update_if_stat_in(
                    killed, [initial], columns=("stat", "exit_code", "finish_time"))
                other.log_event(1000, None, "kill", "user", ts=12)
            return jobs

        monkeypatch.setattr(db, "jobs_in_states", snapshot_then_kill)
        Scheduler(db).tick(now=now)
        assert db.one_element(1000, None).stat == EXIT
        events = [row["kind"] for row in db.events_for(1000)]
        path = tmp_path / "jobout" / "1000.out"
        output = path.read_text() if path.exists() else ""
        assert event not in events and events == ["kill"]
        assert not output
    finally:
        other.close()
        db.close()


@pytest.mark.parametrize("dwell", [0, 0.03])
def test_ordered_intermediate_states_survive_store_advancing(qtbot, dwell):
    mgr = LsfJobManager(runner=FakeLsf(), min_state_dwell_s=dwell)
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        seen = []
        js.jobs_updated.connect(lambda recs: seen.extend(r.state for r in recs))
        states = [JobState.RUN, JobState.SSUSP, JobState.RUN, JobState.DONE]
        records = [mgr.store.transition(js.id, "a", state) for state in states]
        # Store는 이미 DONE이지만 순서대로 도착한 중간 전이는 표시해야 한다.
        for rec in records:
            mgr._emit_jobs(js.id, [rec])
        qtbot.waitUntil(lambda: len(seen) == len(states), timeout=3000)
        assert seen == states
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("write", ["transition", "batch", "replace"])
def test_late_signal_cannot_restore_old_metadata(env, write):
    mgr, _ = env
    js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
    old = mgr.store.transition(js.id, "a", JobState.RUN, user_data={"v": 1})
    if write == "transition":
        new = mgr.store.transition(js.id, "a", None, user_data={"v": 2})
    elif write == "batch":
        new, = mgr.store.transition_many(js.id, [("a", None, None, {"user_data": {"v": 2}})])
    else:
        new = mgr.store.update_job(replace(old, user_data={"v": 2}))
    seen = []
    js.jobs_updated.connect(lambda recs: seen.extend(recs))
    mgr._emit_jobs(js.id, [new])
    mgr._emit_jobs(js.id, [old])
    assert [r.user_data for r in seen] == [{"v": 2}]


@pytest.mark.parametrize("removal", ["remove", "clear"])
def test_new_record_reusing_deleted_key_is_delivered(env, removal):
    mgr, _ = env
    js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
    for state in (JobState.RUN, JobState.DONE):
        mgr._emit_jobs(js.id, [mgr.store.transition(js.id, "a", state)])
    if removal == "remove":
        mgr.remove_jobs(js, ["a"])
    else:
        mgr.clear_jobs(js)
    assert js.id not in mgr._emitted_revisions
    seen = []
    js.jobs_updated.connect(lambda recs: seen.extend(recs))
    added = mgr.add_jobs(js, ["wrapper next"], job_keys=["a"])
    assert seen == added


@pytest.mark.parametrize("first_done", [True, False])
def test_mixed_natural_and_killed_completion_is_independent_of_order(
        env, qtbot, first_done):
    mgr, fake = env
    js = mgr.create_jobset(["wrapper a", "wrapper b"], job_keys=["a", "b"])
    _submit(qtbot, mgr, js)
    a, _b = js.jobs()
    completed = []
    js.jobset_finished.connect(lambda summary: completed.append(summary))
    if first_done:
        fake.set_job(a.job_id, "DONE", 0)
        _poll(qtbot, mgr, js)
    with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
        mgr.kill_jobs(js, ["b"])
    if not first_done:
        fake.set_job(a.job_id, "DONE", 0)
    _poll(qtbot, mgr, js)
    _poll(qtbot, mgr, js)
    assert len(completed) == 1
    assert [r.killed for r in js.jobs()] == [False, True]


def test_resubmit_forgets_only_successfully_reset_ids(env, qtbot, monkeypatch):
    mgr, fake = env
    js = mgr.create_jobset(["wrapper a", "wrapper b"], job_keys=["a", "b"])
    _submit(qtbot, mgr, js)
    fake.set_all("DONE", 0)
    _poll(qtbot, mgr, js)
    old = {r.job_key: r.job_id for r in js.jobs()}
    forgotten = []
    original = mgr.store.transition

    def fail_one_reset(jsid, key, state, **fields):
        if key == "a" and state is JobState.SUBMITTING:
            raise RuntimeError("reset failed")
        return original(jsid, key, state, **fields)

    monkeypatch.setattr(mgr.store, "transition", fail_one_reset)
    monkeypatch.setattr(mgr.command, "forget_status", forgotten.extend)
    _submit(qtbot, mgr, js)
    assert forgotten == [old["b"]]
    current = {r.job_key: r.job_id for r in js.jobs()}
    assert current["a"] == old["a"] and current["b"] != old["b"]
