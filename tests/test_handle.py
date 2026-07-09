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
        js = mgr.submit([f"mytool run_{i}.sp" for i in range(10)])
        js.jobset_updated.connect(summaries.append)

        assert isinstance(js, JobSet)
        with qtbot.waitSignal(js.submit_finished, timeout=10000):
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
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 1          # array 1회
    recs = js.jobs()
    assert len(recs) == 20
    assert len({r.job_id for r in recs}) == 1


def test_auto4_index_pattern_substituted_to_array(qtbot, manager, fake_lsf):
    js = manager.submit([f"mytool tt_{i}.sp" for i in range(1, 31)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    calls = fake_lsf.calls_of("bsub")
    assert len(calls) == 1                              # $LSB_JOBINDEX 치환
    assert "mytool tt_${LSB_JOBINDEX}.sp" in calls[0][-1]


def test_auto4_mixed_commands_use_bulk(qtbot, manager, fake_lsf):
    js = manager.submit(["make a", "run b", "sim c"], auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 3          # 개별 bsub


def test_auto4_forced_bulk(qtbot, manager, fake_lsf):
    js = manager.submit(["same.sh"] * 5, mode="bulk", auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(fake_lsf.calls_of("bsub")) == 5


def test_auto4_forced_array_with_mixed_commands(qtbot, manager, fake_lsf):
    # 상이 command 강제 array → dispatch 스크립트 경로
    js = manager.submit(["make a", "run b", "sim c"], mode="array",
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
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
    a.jobset_updated.connect(a_updates.append)
    b.jobset_updated.connect(b_updates.append)
    manager.jobset_updated.connect(lambda j, s: facade.append((j, s)))

    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(a.jobset_updated, timeout=10000):
        a.refresh()
    assert len(a_updates) == 1
    assert b_updates == []                     # 타 JobSet 이벤트 미수신
    # Facade와 이중 발행 일치
    assert (a.id, a_updates[0]) in facade


def test_handle_finished_and_progress(qtbot, manager, fake_lsf):
    progresses = []
    js = manager.submit([f"r {i}" for i in range(30)], mode="bulk",
                        auto_poll=False)
    js.submit_progress.connect(lambda d, t: progresses.append((d, t)))
    with qtbot.waitSignal(js.submit_finished, timeout=10000) as blocker:
        pass
    report = blocker.args[0]
    assert report.succeeded == 30
    assert progresses and progresses[-1] == (30, 30)


def test_handle_failed_signal_on_submit_failure(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 99
    js = manager.submit(["x"], max_retry=0, auto_poll=False)
    with qtbot.waitSignal(js.jobs_failed, timeout=10000) as blocker:
        pass
    failed = blocker.args[0]
    assert failed[0].state is JobState.SUBMIT_FAILED


def test_handle_kill(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(10)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    with qtbot.waitSignal(js.kill_finished, timeout=10000) as blocker:
        js.kill()
    assert blocker.args[0].requested == 10
    assert fake_lsf.alive_jobs() == []


def test_handle_snapshot_properties(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(4)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert js.summary["PEND"] == 4
    assert js.is_done is False
    assert js.failed_jobs == []
    assert len(js.jobs({JobState.PEND})) == 4

    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.jobset_updated, timeout=10000):
        js.refresh()
    assert js.is_done is True


def test_handle_reacquire_same_instance(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], auto_poll=False)
    assert manager.jobset(js.id) is js


def test_closed_handle_raises(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], auto_poll=False, mode="bulk")
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(js.jobset_updated, timeout=10000):
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
    # merge는 submit 진행 중이면 거부된다 — summary(store)가 전원 PEND를
    # 보여도 ctx 마감(is_active=False) 전이라는 창이 있어, 가드 조건 자체를
    # 기다린다(신호 타이밍 무관, 결정적)
    qtbot.waitUntil(lambda: not manager.submitter.is_active(a.id)
                    and not manager.submitter.is_active(b.id), timeout=10000)
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
        with qtbot.waitSignal(js.submit_finished, timeout=10000):
            pass
        argv = fake_lsf.calls_of("bsub")[0]
        assert argv[argv.index("-q") + 1] == "priority"
        # call 계층이 manager 계층을 덮어씀
        js2 = mgr.submit(["y"], queue="short", auto_poll=False)
        with qtbot.waitSignal(js2.submit_finished, timeout=10000):
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
        with qtbot.waitSignal(js.submit_finished, timeout=10000):
            pass
        with qtbot.waitSignal(js.kill_finished, timeout=10000) as blocker:
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


# ----------------------------------------------------------------------
# is_active / is_inactive — 재수행 판단용
# inactive = 전원 terminal(DONE/EXIT/SUBMIT_FAILED/LOST), active = 그 반대
# ----------------------------------------------------------------------
def _submit_running(qtbot, manager, fake_lsf, n=3):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit([f"echo {i}" for i in range(n)],
                             mode="bulk", auto_poll=False)
    return js


def test_active_while_running(qtbot, manager, fake_lsf):
    """PEND/RUN 등 non-terminal이 하나라도 있으면 active."""
    js = _submit_running(qtbot, manager, fake_lsf)
    assert js.is_active                    # 전원 PEND
    assert not js.is_inactive
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    assert js.is_active and not js.is_inactive


def test_inactive_when_all_done(qtbot, manager, fake_lsf):
    js = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.set_all("DONE")
    manager.querier.query(js.id)
    assert js.is_inactive and not js.is_active


def test_inactive_when_all_exit(qtbot, manager, fake_lsf):
    js = _submit_running(qtbot, manager, fake_lsf)
    fake_lsf.set_all("EXIT", exit_code=1)
    manager.querier.query(js.id)
    assert js.is_inactive and not js.is_active


def test_inactive_when_mixed_terminal(qtbot, manager, fake_lsf):
    """DONE/EXIT 섞여도 전원 terminal이면 inactive."""
    js = _submit_running(qtbot, manager, fake_lsf, n=2)
    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE")
    fake_lsf.set_job(recs[1].job_id, "EXIT", exit_code=2)
    manager.querier.query(js.id)
    assert js.is_inactive and not js.is_active


def test_inactive_when_all_submit_failed(qtbot, manager, fake_lsf):
    """전원 SUBMIT_FAILED도 inactive (terminal)."""
    fake_lsf.fail_next_bsub = 100          # 모든 bsub 실패
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["a", "b"], mode="bulk", auto_poll=False, max_retry=0)
    assert all(r.state is JobState.SUBMIT_FAILED for r in js.jobs())
    assert js.is_inactive and not js.is_active


def test_inactive_when_all_lost(qtbot, manager, fake_lsf):
    """전원 LOST도 inactive (terminal)."""
    js = _submit_running(qtbot, manager, fake_lsf, n=1)
    # store에서 직접 LOST로 (조회 불가 시나리오)
    rec = js.jobs()[0]
    manager.store.transition(js.id, rec.job_key, JobState.LOST,
                             fail_reason="NO_JOBID_PARSED")
    assert js.is_inactive and not js.is_active


def test_active_when_one_terminal_rest_running(qtbot, manager, fake_lsf):
    """일부만 끝나고 하나라도 돌고 있으면 active — 재수행 안 함 근거."""
    js = _submit_running(qtbot, manager, fake_lsf, n=3)
    recs = js.jobs()
    fake_lsf.set_job(recs[0].job_id, "DONE")
    fake_lsf.set_job(recs[1].job_id, "DONE")
    fake_lsf.set_job(recs[2].job_id, "RUN")   # 하나는 아직 RUN
    manager.querier.query(js.id)
    assert js.is_active and not js.is_inactive


# ----------------------------------------------------------------------
# js.jobs_updated — 단일 JobSet 표 갱신용 파생 발행 (README §5.2)
# ----------------------------------------------------------------------
def test_handle_jobs_updated_derived(qtbot, manager, fake_lsf):
    """Manager.jobs_updated(jsid, ...)가 핸들 js.jobs_updated(records)로
    중계된다 — 단일 jobset GUI가 jsid 필터 없이 표를 갱신하는 근거."""
    batches = []
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a", "echo b"], mode="bulk",
                            auto_poll=False)
        js.jobs_updated.connect(batches.append)

    qtbot.waitUntil(lambda: any(batches), timeout=5000)
    keys = {r.job_key for batch in batches for r in batch}
    assert keys == {r.job_key for r in js.jobs()}   # 초기 배치가 전원 포함
