"""추가 점검: 미발견 횟수의 실행 수명, pacer 재진입, MockLSF 슬롯 정산."""
import threading
from types import SimpleNamespace

import pytest

from lsfmgr import JobState, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _submit(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
        mgr.submit(js, auto_poll=False)


def _poll(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.jobset_updated, timeout=3000):
        mgr.query_once(js)


@pytest.mark.parametrize("edit", ["resubmit", "replace", "remove", "clear"])
def test_old_query_cannot_restore_missing_streak_after_edit(qtbot, monkeypatch, edit):
    mgr = LsfJobManager(runner=FakeLsf(), config=LsfConfig(lost_after_missing_polls=2))
    entered, release = threading.Event(), threading.Event()
    try:
        js = mgr.create_jobset(["wrapper old"], job_keys=["a"])
        _submit(qtbot, mgr, js)
        monkeypatch.setattr(mgr.command, "bjobs_by_ids", lambda ids, fresh=False: ([], set()))
        _poll(qtbot, mgr, js)  # 이전 실행의 첫 미발견
        # 편집의 자동 폴링 재개를 제외해, 새 실행의 조회 횟수를 직접 제어한다.
        monkeypatch.setattr(mgr, "start_polling", lambda *args, **kwargs: None)
        original = mgr.querier._set_streaks

        def pause_before_save(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                assert release.wait(3)
            return original(*args, **kwargs)

        monkeypatch.setattr(mgr.querier, "_set_streaks", pause_before_save)
        mgr.query_once(js)
        assert entered.wait(3)
        # 조회는 두 번째 미발견을 계산했지만 저장 전에 실행을 교체한다.
        if edit == "resubmit":
            with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
                mgr.kill(js)
        elif edit == "replace":
            mgr.replace_jobs(js, ["wrapper next"], job_keys=["a"], force=True)
        else:
            if edit == "remove":
                mgr.remove_jobs(js, ["a"], force=True)
            else:
                mgr.clear_jobs(js, force=True)
            mgr.add_jobs(js, ["wrapper next"], job_keys=["a"])
        _submit(qtbot, mgr, js)
        with qtbot.waitSignal(mgr.jobset_updated, timeout=3000):
            release.set()
        # 새 실행은 첫 미발견이므로 LOST 유예가 남아 있어야 한다.
        _poll(qtbot, mgr, js)
        assert js.jobs()[0].state is JobState.PEND
        _poll(qtbot, mgr, js)
        assert js.jobs()[0].state is JobState.LOST
    finally:
        release.set()
        mgr.shutdown()


def test_new_query_before_rearm_forget_does_not_inherit_old_streak(qtbot, monkeypatch):
    mgr = LsfJobManager(runner=FakeLsf(), config=LsfConfig(lost_after_missing_polls=2))
    assigned = threading.Event()
    try:
        js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
        _submit(qtbot, mgr, js)
        monkeypatch.setattr(mgr.command, "bjobs_by_ids", lambda ids, fresh=False: ([], set()))
        _poll(qtbot, mgr, js)
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
            mgr.kill(js)
        original_transition = mgr.store.transition
        original_forget = mgr.querier.forget
        observed = []

        def note_id(jsid, key, state, **fields):
            rec = original_transition(jsid, key, state, **fields)
            if state is JobState.PEND:
                assigned.set()
            return rec

        def query_before_forget(jsid, keys=None):
            assert assigned.wait(3)
            mgr.querier.query(jsid)
            observed.append(js.jobs()[0].state)
            original_forget(jsid, keys)

        monkeypatch.setattr(mgr.store, "transition", note_id)
        monkeypatch.setattr(mgr.querier, "forget", query_before_forget)
        _submit(qtbot, mgr, js)
        assert observed == [JobState.PEND]
        assert js.jobs()[0].state is JobState.PEND
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("flush", [False, True])
@pytest.mark.parametrize("edit", ["replace", "remove", "jobset"])
def test_pacer_forget_from_another_jobset_slot_cancels_ready_output(
        qtbot, monkeypatch, flush, edit):
    from lsfmgr import pacer

    now = [0.0]
    monkeypatch.setattr(pacer, "time", SimpleNamespace(monotonic=lambda: now[0]))
    mgr = LsfJobManager(runner=FakeLsf(), min_state_dwell_s=10)
    try:
        first = mgr.create_jobset(["wrapper first"], job_keys=["a"])
        second = mgr.create_jobset(["wrapper old"], job_keys=["b"])
        for js, key in ((first, "a"), (second, "b")):
            mgr._emit_jobs(js.id, [mgr.store.transition(js.id, key, JobState.DONE)])
        seen = []
        mgr.jobs_updated.connect(lambda jsid, recs: seen.extend(
            recs if jsid == second.id else []))

        def edit_second(_records):
            if edit == "replace":
                mgr.replace_jobs(second, ["wrapper new"], job_keys=["b"])
            elif edit == "remove":
                mgr.remove_jobs(second, ["b"])
            else:
                mgr.remove_jobset(second)

        first.jobs_updated.connect(edit_second)
        now[0] = 20.0
        if flush:
            mgr._pacer.stop()
        else:
            mgr._pacer._drain()
        assert [r.state for r in seen] == ([JobState.CREATED] if edit == "replace" else [])
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("limit", ["host", "array"])
def test_mock_rejected_finish_does_not_release_occupied_slot(tmp_path, monkeypatch, limit):
    from mocklsf import config
    from mocklsf.db import Database
    from mocklsf.models import ACTIVE_STATES, DONE, PEND, RUN, USUSP, Job
    from mocklsf.scheduler import Scheduler

    monkeypatch.setattr(config, "MOCKLSF_HOME", str(tmp_path))
    monkeypatch.setattr(config, "JOB_OUT_DIR", str(tmp_path / "jobout"))
    monkeypatch.setattr(config, "HOSTS", {"hostA": 1 if limit == "host" else 2})
    db = Database(str(tmp_path / "state.db"))
    other = Database(str(tmp_path / "state.db"))
    try:
        common = dict(user="test", command="simulate", queue="normal", from_host="master",
                      job_name="a", submit_time=1, planned_outcome=DONE, run_secs=100)
        db.insert_jobs([
            Job(job_id=1000, array_index=1, array_limit=1, stat=RUN,
                exec_host="hostA", start_time=10, finish_time=11, **common),
            Job(job_id=1000 if limit == "array" else 1001,
                array_index=2, array_limit=1, stat=PEND, **common),
        ])
        original = db.jobs_in_states
        changed = []

        def snapshot_then_suspend(states):
            jobs = original(states)
            if RUN in states and not changed:
                changed.append(True)
                job = other.one_element(1000, 1)
                job.stat, job.susp_since = USUSP, 12
                assert other.update_if_stat_in(job, [RUN], columns=("stat", "susp_since"))
            return jobs

        monkeypatch.setattr(db, "jobs_in_states", snapshot_then_suspend)
        Scheduler(db).tick(now=20)
        assert db.one_element(1000, 1).stat == USUSP
        assert len(db.jobs_in_states(list(ACTIVE_STATES))) == 1
        assert db.all_jobs()[1].stat == PEND
        assert not list((tmp_path / "jobout").glob("*.out"))
    finally:
        other.close()
        db.close()
