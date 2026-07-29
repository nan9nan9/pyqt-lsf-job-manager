"""JobRecord.killed — "이 EXIT은 내가 죽인 것인가"의 근거.

mgr.kill()/kill_jobs()가 bkill 수용을 확인한 대상에만 표시된다. 자연 종료·
외부 bkill·비정상 EXIT은 이 경로를 안 타므로 False로 남는다(exit_code로는
구분되지 않는다). 재제출 리셋에서 False로 되돌아간다.
"""
from __future__ import annotations

from dataclasses import replace as dc_replace

from lsfmgr import InMemoryStore, LsfJobManager


# ----------------------------------------------------------------------
# 내가 죽인 job — killed=True (기본 optimistic 정책: 즉시 EXIT 전이와 함께)
# ----------------------------------------------------------------------
def test_killed_flag_set_by_kill(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)

    recs = js.jobs()
    assert all(r.killed for r in recs)
    assert all(r.fail_reason == "KILLED" for r in recs)


# ----------------------------------------------------------------------
# 시스템/자연 종료로 EXIT된 job — killed=False (exit_code는 kill과 무구분)
# ----------------------------------------------------------------------
def test_system_exit_is_not_flagged(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    fake_lsf.set_all("EXIT", 137)        # 외부에서 죽은 것처럼 (SIGKILL 코드)
    with qtbot.waitSignal(js.jobset_finished, timeout=10000):
        manager.query_once(js)           # 내 kill이 아니므로 통지된다

    rec = js.jobs()[0]
    assert rec.exit_code == 137 and not rec.killed


# ----------------------------------------------------------------------
# 부분 kill — 죽인 것만 표시, 나머지는 그대로
# ----------------------------------------------------------------------
def test_partial_kill_flags_only_targets(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    recs = js.jobs()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill_jobs(js, [recs[0].job_key])

    by_key = {r.job_key: r for r in js.jobs()}
    assert by_key[recs[0].job_key].killed
    assert not by_key[recs[1].job_key].killed


# ----------------------------------------------------------------------
# 재제출하면 표식이 지워진다 — 새 실행의 결과는 새 근거로 판단해야 한다
# ----------------------------------------------------------------------
def test_flag_cleared_on_resubmit(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)
    assert js.jobs()[0].killed

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    assert not js.jobs()[0].killed


# ----------------------------------------------------------------------
# actual 정책 — kill 시점엔 EXIT가 아니지만 표식은 남고, 폴링이 EXIT를
# 확인하는 순간의 완료도 (표식 덕분에) 통지되지 않는다
# ----------------------------------------------------------------------
def test_actual_policy_flags_and_mutes(qtbot, fake_lsf, config):
    cfg = dc_replace(config, kill_status_policy="actual")
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    try:
        fired = []
        mgr.jobset_finished.connect(lambda j, s: fired.append(s))
        js = mgr.create_jobset(["customwrapper_sub a.sp"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)

        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill(js)
        rec = js.jobs()[0]
        assert rec.killed                        # 표식은 지금 남는다
        assert not rec.state.is_terminal         # 상태 전이는 폴링 몫

        fake_lsf.set_all("EXIT", 130)            # kill이 반영된 실측
        mgr.query_once(js)
        qtbot.wait(300)
        after = js.jobs()[0]
        assert after.state.name == "EXIT"
        assert after.killed                      # 폴링 전이가 표식을 보존
        assert fired == []                       # 내가 죽인 완료 — 통지 없음
    finally:
        mgr.shutdown()
