"""polling / 조회 전략 / LOST 전이 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobState
from tests.conftest import submit_cmds


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    """job 20개 submit 완료된 jobset."""
    jobs = [f"r {i}" for i in range(20)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, jobs).id
    return jsid


# ----------------------------------------------------------------------
# 조회 전략 — group 기반 1회 호출
# ----------------------------------------------------------------------
def test_query_uses_id_chunks_only(qtbot, manager, fake_lsf, submitted):
    """v10: 조회는 explicit id chunked bjobs뿐 — -g/-J를 쓰지 않는다."""
    fake_lsf.calls.clear()
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, summary = blocker.args
    assert summary["RUN"] == 20
    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert 1 <= len(bjobs_calls) <= 3          # 20 jobs / chunk 200 → 1회
    assert all("-g" not in c and "-J" not in c for c in bjobs_calls)


def test_jobs_updated_carries_only_changes(qtbot, manager, fake_lsf,
                                           submitted):
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobs_updated, timeout=10000) as blocker:
        manager.query_once(submitted)
    jsid, changed = blocker.args
    assert len(changed) == 20                 # PEND → RUN 전부 변경

    # 변화 없으면 jobs_updated는 안 오고 jobset_updated만 온다
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

    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)

    after = manager.store.get_job(submitted, rec0.job_key)
    assert after.state is JobState.DONE
    assert after.run_time_s == 125                       # LSF run_time(초)
    assert after.start_time == datetime(2026, 7, 5, 14, 0, 0)
    assert after.finish_time == datetime(2026, 7, 5, 14, 2, 5)
    # 실행시간이 안 실린 job은 None 유지 (파싱 실패 없음)
    others = [r for r in manager.get_jobs(submitted)
              if r.job_key != rec0.job_key]
    assert all(r.run_time_s is None for r in others)


def test_core_downgrade_preserves_runtime_fields(qtbot, manager, fake_lsf,
                                                  submitted):
    """리뷰 M6 회귀 — 포맷이 CORE로 강등돼 확장 필드가 안 오는 사이클이
    이미 저장된 run_time/start/finish를 None으로 덮지 않고, 무의미한
    재전이(jobs_updated 스팸)도 만들지 않는다."""
    from datetime import datetime
    rec0 = manager.get_jobs(submitted)[0]
    j = fake_lsf.jobs[str(rec0.job_id)]
    j.stat, j.run_time_s = "RUN", 42
    j.start_time = "2026-07-28 09:00:00"
    manager.query_once(submitted)
    qtbot.waitUntil(lambda: manager.store.get_job(
        submitted, rec0.job_key).run_time_s == 42, timeout=10000)

    # CORE 강등 재현 — 이후 조회는 확장 필드 없이 온다
    cmd = manager.querier.command
    cmd._bjobs_fmt_idx = len(cmd._bjobs_formats) - 1
    got = []
    manager.jobs_updated.connect(lambda _j, rs: got.append(rs))
    manager.query_once(submitted)
    qtbot.wait(50)

    after = manager.store.get_job(submitted, rec0.job_key)
    assert after.run_time_s == 42                       # 보존 (None 덮기 금지)
    assert after.start_time == datetime(2026, 7, 28, 9, 0, 0)
    # 상태·값 무변화 사이클 — 재전이/발행 없음 (스팸 방지)
    assert not any(any(r.job_key == rec0.job_key for r in rs) for rs in got)


# ----------------------------------------------------------------------
# bjobs 미발견 → LOST (수용 기준 6) — 본체는 test_query_defer.py
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 주기 polling
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
        js))
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
