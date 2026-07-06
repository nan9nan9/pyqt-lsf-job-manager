"""JobSet 핸들 / AUTO-1~4 / 옵션 계층 통합 테스트 (v7, 수용 기준 14·15·16)."""
from __future__ import annotations

import pytest

from lsfmgr import (
    InMemoryStore,
    JobSet,
    JobSetClosedError,
    JobSpec,
    JobState,
    LsfJobManager,
)
from lsfmgr.jobset_core import detect_array_template


# ----------------------------------------------------------------------
# 수용 기준 14 — 3줄 간소화 API (auto_poll 포함)
# ----------------------------------------------------------------------
def test_three_line_usage(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf)
    try:
        summaries = []
        js = mgr.submit([f"hspice run_{i}.sp" for i in range(10)])
        js.updated.connect(summaries.append)

        assert isinstance(js, JobSet)
        with qtbot.waitSignal(js.finished, timeout=10000):
            pass
        # AUTO-1: polling이 자동 시작되어 updated가 도착해야 함
        qtbot.waitUntil(lambda: len(summaries) >= 1, timeout=10000)
        assert summaries[-1]["total"] == 10
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# AUTO-4 — mode 자동 선택
# ----------------------------------------------------------------------
def test_auto4_identical_commands_use_array(qtbot, manager, fake_lsf):
    js = manager.submit(["run_sim.sh"] * 20, auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 1          # array 1회
    recs = js.jobs()
    assert len(recs) == 20
    assert len({r.job_id for r in recs}) == 1


def test_auto4_index_pattern_substituted_to_array(qtbot, manager, fake_lsf):
    js = manager.submit([f"hspice tt_{i}.sp" for i in range(1, 31)],
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    calls = fake_lsf.calls_of("bsub")
    assert len(calls) == 1                              # $LSB_JOBINDEX 치환
    assert "hspice tt_${LSB_JOBINDEX}.sp" in calls[0][-1]


def test_auto4_mixed_commands_use_bulk(qtbot, manager, fake_lsf):
    js = manager.submit(["make a", "run b", "sim c"], auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 3          # 개별 bsub


def test_auto4_forced_bulk(qtbot, manager, fake_lsf):
    js = manager.submit(["same.sh"] * 5, mode="bulk", auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 5


def test_auto4_forced_array_with_mixed_commands(qtbot, manager, fake_lsf):
    # 상이 command 강제 array → dispatch 스크립트 경로
    js = manager.submit(["make a", "run b", "sim c"], mode="array",
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 1
    assert [r.command for r in sorted(js.jobs(),
                                      key=lambda r: r.array_index)] \
        == ["make a", "run b", "sim c"]


def test_detect_array_template():
    assert detect_array_template(["a 1", "a 2", "a 3"]) == "a ${LSB_JOBINDEX}"
    assert detect_array_template(["x.sh"] * 3) == "x.sh"
    assert detect_array_template(["a 1", "b 2"]) is None       # 골격 상이
    assert detect_array_template(["a 5", "a 2", "a 9"]) is None  # 인덱스 아님
    assert detect_array_template(["v2 run_1", "v2 run_2"]) \
        == "v2 run_${LSB_JOBINDEX}"                    # 상수 숫자는 유지
    assert detect_array_template(["only"]) is None


# ----------------------------------------------------------------------
# 수용 기준 16 — 핸들 Signal 격리 + Facade 이중 발행 일치
# ----------------------------------------------------------------------
def test_handle_signal_isolation(qtbot, manager, fake_lsf):
    # submit 완료 시 jobset_updated가 발화되므로(초기 PEND), 리스너 연결 전에
    # 두 submit의 완료 Signal을 모두 소진해야 격리 카운트가 어긋나지 않는다
    finished = []
    manager.submit_finished.connect(lambda j, r: finished.append(j))
    a = manager.submit([f"a {i}" for i in range(3)], auto_poll=False,
                       mode="bulk")
    b = manager.submit(["b x", "b y"], auto_poll=False, mode="bulk")
    qtbot.waitUntil(lambda: {a.id, b.id} <= set(finished), timeout=10000)

    a_updates, b_updates, facade = [], [], []
    a.updated.connect(a_updates.append)
    b.updated.connect(b_updates.append)
    manager.jobset_updated.connect(lambda j, s: facade.append((j, s)))

    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(a.updated, timeout=10000):
        a.refresh()
    assert len(a_updates) == 1
    assert b_updates == []                     # 타 JobSet 이벤트 미수신
    # Facade와 이중 발행 일치
    assert (a.id, a_updates[0]) in facade


def test_handle_finished_and_progress(qtbot, manager, fake_lsf):
    progresses = []
    js = manager.submit([f"r {i}" for i in range(30)], mode="bulk",
                        auto_poll=False)
    js.progress.connect(lambda d, t: progresses.append((d, t)))
    with qtbot.waitSignal(js.finished, timeout=10000) as blocker:
        pass
    report = blocker.args[0]
    assert report.succeeded == 30
    assert progresses and progresses[-1] == (30, 30)


def test_handle_failed_signal_on_submit_failure(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 99
    js = manager.submit(["x"], max_retry=0, auto_poll=False)
    with qtbot.waitSignal(js.failed, timeout=10000) as blocker:
        pass
    failed = blocker.args[0]
    assert failed[0].state is JobState.SUBMIT_FAILED


def test_handle_kill(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(10)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    with qtbot.waitSignal(js.killed, timeout=10000) as blocker:
        js.kill()
    assert blocker.args[0].requested == 10
    assert fake_lsf.alive_jobs() == []


def test_handle_snapshot_properties(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(4)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert js.summary["PEND"] == 4
    assert js.is_done is False
    assert js.failed_jobs == []
    assert len(js.jobs({JobState.PEND})) == 4

    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.updated, timeout=10000):
        js.refresh()
    assert js.is_done is True


def test_handle_reacquire_same_instance(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], auto_poll=False)
    assert manager.jobset(js.id) is js


def test_closed_handle_raises(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], auto_poll=False, mode="bulk")
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.updated, timeout=10000):
        js.refresh()
    js.close()
    with pytest.raises(JobSetClosedError):
        _ = js.summary
    with pytest.raises(JobSetClosedError):
        js.kill()
    # 재획득하면 새 핸들 (store에는 closed 상태로 남아있음)
    js2 = manager.jobset(js.id)
    assert js2 is not js


def test_merge_with_invalidates_originals(qtbot, manager, fake_lsf):
    a = manager.submit(["a 1", "a 2"], auto_poll=False, mode="bulk")
    b = manager.submit(["b 1"], auto_poll=False, mode="bulk")
    qtbot.waitUntil(lambda: a.summary.get("PEND", 0) == 2
                    and b.summary.get("PEND", 0) == 1, timeout=10000)
    merged = a.merge_with(b)
    assert merged.summary["total"] == 3
    with pytest.raises(JobSetClosedError):
        _ = a.summary
    with pytest.raises(JobSetClosedError):
        _ = b.summary


# ----------------------------------------------------------------------
# 수용 기준 15 — manager kwargs 옵션 계층 (통합 동작)
# ----------------------------------------------------------------------
def test_manager_kwargs_default_queue(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf,
                        default_queue="priority", max_retry=0)
    try:
        js = mgr.submit(["x"], auto_poll=False)
        with qtbot.waitSignal(js.finished, timeout=10000):
            pass
        argv = fake_lsf.calls_of("bsub")[0]
        assert argv[argv.index("-q") + 1] == "priority"
        # call 계층이 manager 계층을 덮어씀
        js2 = mgr.submit(["y"], queue="short", auto_poll=False)
        with qtbot.waitSignal(js2.finished, timeout=10000):
            pass
        argv2 = fake_lsf.calls_of("bsub")[-1]
        assert argv2[argv2.index("-q") + 1] == "short"
    finally:
        mgr.shutdown()


def test_manager_kwargs_typo_typeerror(fake_lsf):
    with pytest.raises(TypeError):
        LsfJobManager(runner=fake_lsf, wokers=8)


def test_manager_kwargs_range_valueerror(fake_lsf):
    with pytest.raises(ValueError):
        LsfJobManager(runner=fake_lsf, workers=99)


def test_call_kwargs_typo_typeerror(qtbot, manager):
    with pytest.raises(TypeError):
        manager.submit(["x"], wokers=8)


def test_persistent_kwarg_selects_sqlite(tmp_path, fake_lsf, qtbot, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf, persistent=True,
                        db_path=str(tmp_path / "opt.db"))
    try:
        assert mgr.persistent is True
        assert (tmp_path / "opt.db").exists()
    finally:
        mgr.shutdown()


def test_verify_kill_manager_default(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf, verify_kill=True)
    try:
        js = mgr.submit([f"r {i}" for i in range(5)], mode="bulk",
                        auto_poll=False)
        with qtbot.waitSignal(js.finished, timeout=10000):
            pass
        with qtbot.waitSignal(js.killed, timeout=10000) as blocker:
            js.kill()                          # verify 미지정 → ② 적용
        assert blocker.args[0].still_alive == 0
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# AUTO-3 — aboutToQuit 자동 shutdown (수용 기준 12 확장)
# ----------------------------------------------------------------------
def test_auto3_shutdown_on_about_to_quit(qtbot, qapp, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    assert mgr._shutdown_done is False
    qapp.aboutToQuit.emit()
    assert mgr._shutdown_done is True
    mgr.shutdown()                             # 명시 호출도 중복 안전 (멱등)


# ----------------------------------------------------------------------
# 복원 → 핸들 반환 (v7 §5)
# ----------------------------------------------------------------------
def test_recover_returns_handle(qtbot, fake_lsf, config, tmp_path):
    from lsfmgr import SqliteStore
    db = str(tmp_path / "h.db")
    m1 = LsfJobManager(store=SqliteStore(db), config=config, runner=fake_lsf)
    jsid = m1.submit_bulk([JobSpec(command="x")])
    with qtbot.waitSignal(m1.submit_finished, timeout=10000):
        pass
    m1.shutdown()

    m2 = LsfJobManager(store=SqliteStore(db), config=config, runner=fake_lsf)
    try:
        orphan = m2.list_orphan_jobsets()[0]
        js = m2.recover_jobset(orphan.jobset_id)
        assert isinstance(js, JobSet)
        assert js.summary["total"] == 1
    finally:
        m2.shutdown()
