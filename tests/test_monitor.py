"""polling / 조회 전략 / LOST 전이 테스트 (FR-4)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState
from tests.conftest import submit_cmds


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    """job 20개 submit 완료된 jobset."""
    jobs = [JobSpec(command=f"r {i}") for i in range(20)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs).id
    return jsid


# ----------------------------------------------------------------------
# 조회 전략 (FR-4.1) — group 기반 1회 호출
# ----------------------------------------------------------------------
def test_query_uses_group_first(qtbot, manager, fake_lsf, submitted):
    fake_lsf.calls.clear()
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, summary = blocker.args
    assert summary["RUN"] == 20
    # 부착물(group/name) 기반 조회 — job 수에 비례하지 않음 (수용 기준 5)
    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert 1 <= len(bjobs_calls) <= 3
    assert any("-g" in c for c in bjobs_calls)


def test_jobs_updated_carries_only_changes(qtbot, manager, fake_lsf,
                                           submitted):
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobs_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, changed = blocker.args
    assert len(changed) == 20                 # PEND → RUN 전부 변경

    # 변화 없으면 jobs_updated는 안 오고 jobset_updated만 온다 (QT-4)
    got_jobs_updated = []
    manager.jobs_updated.connect(lambda *a: got_jobs_updated.append(a))
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    assert not got_jobs_updated


def test_done_exit_transition(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.set_all("DONE", 0)
    fake_lsf.set_job(recs[0].job_id, "EXIT", 3)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    _, summary = blocker.args
    assert summary["DONE"] == 19
    assert summary["EXIT"] == 1
    exited = manager.get_jobs(submitted, states={JobState.EXIT})
    assert exited[0].exit_code == 3


# ----------------------------------------------------------------------
# 실행 시간 (run_time / start_time / finish_time) — LSF bjobs 기준
# ----------------------------------------------------------------------
def test_runtime_captured_from_bjobs(qtbot, manager, fake_lsf, submitted):
    from datetime import datetime
    rec0 = manager.get_jobs(submitted)[0]
    # 완료 job에 LSF 실행시간 필드를 실어 보냄
    j = fake_lsf.jobs[str(rec0.job_id)]
    j.stat, j.exit_code = "DONE", 0
    j.run_time_s = 125
    j.start_time = "2026-07-05 14:00:00"
    j.finish_time = "2026-07-05 14:02:05"
    j.working_dir = "/proj/run/job0"

    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)

    after = manager.store.get_job(submitted, rec0.job_key)
    assert after.state is JobState.DONE
    assert after.run_time_s == 125                       # LSF run_time(초)
    assert after.start_time == datetime(2026, 7, 5, 14, 0, 0)
    assert after.finish_time == datetime(2026, 7, 5, 14, 2, 5)
    assert after.working_dir == "/proj/run/job0"         # LSF exec_cwd
    # 실행시간이 안 실린 job은 None 유지 (파싱 실패 없음)
    others = [r for r in manager.get_jobs(submitted)
              if r.job_key != rec0.job_key]
    assert all(r.run_time_s is None for r in others)


def test_name_fallback_rejects_id_mismatch(qtbot, manager, fake_lsf,
                                           submitted):
    """이름은 같지만 job_id가 다른 인스턴스(name 재사용)는 fallback 매칭에서
    버린다 — 다른 job의 상태/exit_code가 이 레코드에 혼입되면 안 된다."""
    from tests.fake_lsf import FakeJob
    rec0 = manager.get_jobs(submitted)[0]
    # 원본 job은 bjobs에서 사라지고(bhist에도 없음),
    fake_lsf.vanish_job(rec0.job_id, in_bhist=False)
    # 같은 이름의 '다른' job(id 상이)이 group probe에 잡히도록 심는다
    js = manager.store.get_jobset(submitted)
    fake_lsf.jobs["99999"] = FakeJob(
        job_id=99999, array_index=None, name=rec0.lsf_job_name,
        group=js.lsf_group_paths[0], queue="normal", command="impostor",
        stat="DONE", exit_code=0)

    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)

    after = manager.store.get_job(submitted, rec0.job_key)
    # 사칭 job의 DONE이 혼입되지 않아야 한다 — id 불일치로 fallback 거부
    # (원본은 미발견 → bhist에도 없음 → LOST 경로)
    assert not (after.state is JobState.DONE and after.job_id == rec0.job_id)
    assert after.state in (JobState.LOST, rec0.state)


# ----------------------------------------------------------------------
# bhist fallback → LOST (FR-4.3, 수용 기준 6)
# ----------------------------------------------------------------------
def test_bhist_fallback(qtbot, manager, fake_lsf, submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.set_job(recs[0].job_id, "DONE", 0)
    fake_lsf.vanish_job(recs[0].job_id, in_bhist=True)   # bjobs엔 없음
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    rec = manager.store.get_job(submitted, recs[0].lsf_job_name)
    assert rec.state is JobState.DONE                    # bhist로 복구


def test_lost_when_bjobs_and_bhist_both_miss(qtbot, manager, fake_lsf,
                                             submitted):
    recs = manager.get_jobs(submitted)
    fake_lsf.vanish_job(recs[0].job_id, in_bhist=False)
    with qtbot.waitSignal(manager.job_lost, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, lost_rec = blocker.args
    assert jsid == submitted
    assert lost_rec.state is JobState.LOST
    assert lost_rec.fail_reason == "NOT_FOUND_IN_LSF"
    s = manager.summary(submitted)
    assert s["LOST"] == 1
    assert sum(v for k, v in s.items() if k != "total") == s["total"]


# ----------------------------------------------------------------------
# 주기 polling (FR-4.4)
# ----------------------------------------------------------------------
def test_periodic_polling(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("RUN")
    updates = []
    manager.jobset_updated.connect(lambda j, s: updates.append(s))
    manager.start_polling(submitted, interval_s=0.2)
    qtbot.waitUntil(lambda: len(updates) >= 2, timeout=10000)
    manager.stop_polling(submitted)
    assert updates[0]["RUN"] == 20


def test_polling_autostops_when_all_terminal(qtbot, manager, fake_lsf,
                                             submitted):
    fake_lsf.set_all("DONE", 0)
    updates = []
    manager.jobset_updated.connect(lambda j, s: updates.append(s))
    manager.start_polling(submitted, interval_s=0.1)
    qtbot.waitUntil(lambda: len(updates) >= 1, timeout=10000)
    qtbot.wait(400)                     # 자동 중지 후 추가 polling 없어야 함
    n = len(updates)
    qtbot.wait(400)
    assert len(updates) == n


# ----------------------------------------------------------------------
# 부착물 전부 유실 → chunking으로 동작 (수용 기준 3)
# ----------------------------------------------------------------------
def test_graceful_degradation_without_attachments(qtbot, manager, fake_lsf,
                                                  submitted):
    from dataclasses import replace
    js = manager.store.get_jobset(submitted)
    manager.store.update_jobset(replace(
        js, lsf_group_paths=[], name_patterns=[], array_job_ids=[]))
    fake_lsf.set_all("RUN")
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    _, summary = blocker.args
    assert summary["RUN"] == 20
    assert all("-g" not in c and "-J" not in c
               for c in fake_lsf.calls_of("bjobs"))


# ----------------------------------------------------------------------
# shutdown: polling 타이머는 폴링 스레드에서 정리돼야 한다 (cross-thread
# killTimer 위반 금지). start_polling 직후 즉시 shutdown 하는 경합에서도
# stop_all이 quit 전에 완료돼 타이머가 그 스레드에서 파괴된다.
# ----------------------------------------------------------------------
def test_polling_shutdown_cleans_timers_in_thread(qtbot, manager, fake_lsf,
                                                  capfd):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.start_polling(js, 5.0)          # 여유 없이 바로 shutdown
    worker = manager.polling._worker
    manager.shutdown()

    # stop_all이 실행돼 타이머를 폴링 스레드에서 정지·삭제했어야 한다
    assert worker.stopped_event.is_set()
    assert worker._timers == {}
    assert not manager.polling._thread.isRunning()
    # C 레벨(qWarning) stderr에 cross-thread 위반이 없어야 한다
    import gc
    gc.collect()
    err = capfd.readouterr().err
    assert "Timers cannot be stopped from another thread" not in err, err
