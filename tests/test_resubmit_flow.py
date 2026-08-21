"""probe: 재제출 — 리셋 / rearm / 무장 토큰 / 폴링 재개가 제대로 도나."""
import threading
import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds


def _mgr(fake_lsf, **cfg):
    return LsfJobManager(store=InMemoryStore(),
                         config=LsfConfig(retry_delay_s=0.02, **cfg),
                         runner=fake_lsf)


def test_resubmit_clears_previous_failure_residue(qtbot, fake_lsf):
    """이전 실행의 흔적(job_id/exit_code/fail_*/killed/retry_count)이 남으면
    표에 죽은 옛 정보가 보인다."""
    fake_lsf.fail_next_bsub = 99
    mgr = _mgr(fake_lsf, max_retry=0)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False)
        bad = js.jobs()[0]
        assert bad.state is JobState.SUBMIT_FAILED and bad.fail_message
        fake_lsf.fail_next_bsub = 0
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False)
        ok = js.jobs()[0]
        print(f"\n재제출 후: state={ok.state.value} fail_reason={ok.fail_reason} "
              f"fail_message={ok.fail_message!r} retry={ok.retry_count} "
              f"killed={ok.killed} exit={ok.exit_code}")
        assert ok.state is JobState.PEND
        assert (ok.fail_reason, ok.fail_message, ok.retry_count,
                ok.killed, ok.exit_code) == (None, None, 0, False, None)
        assert ok.job_id is not None and ok.job_id != bad.job_id or bad.job_id is None
    finally:
        mgr.shutdown()


def test_handler_reruns_after_resubmit(qtbot, fake_lsf):
    """_FINISHED로 남으면 재실행에서 handler가 영영 침묵한다."""
    hits = []
    mgr = _mgr(fake_lsf, poll_interval_s=5.0)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        mgr.add_handler(js, "h", lambda c: hits.append((c.job_key, c.final)),
                        start_states={JobState.PEND},
                        end_states={JobState.DONE, JobState.EXIT})
        for cycle in range(2):
            with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
                mgr.submit(js, auto_poll=False)
            mgr.query_once(js)
            qtbot.wait(300)
            with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
                mgr.kill(js)
            mgr.query_once(js)
            qtbot.wait(300)
            print(f"  사이클 {cycle}: handler 누적 {len(hits)}회 {hits}")
        assert len(hits) >= 2, f"재제출 후 handler가 안 돌았다: {hits}"
    finally:
        mgr.shutdown()


def test_post_process_rearms_on_resubmit(qtbot, fake_lsf):
    calls = []
    mgr = _mgr(fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        for cycle in range(2):
            with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
                mgr.submit(js, auto_poll=False,
                           post_process=lambda r, c=cycle: calls.append(c))
            with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
                mgr.kill(js)
            mgr.query_once(js)
            qtbot.waitUntil(lambda c=cycle: len(calls) > c, timeout=20000)
        print(f"\npost_process 호출: {calls}")
        assert calls == [0, 1]
    finally:
        mgr.shutdown()


def test_lost_streak_is_reset_on_resubmit(qtbot, fake_lsf):
    """옛 실행의 미발견 횟수를 물려받으면 새 실행이 한 번만 안 보여도
    LOST(되돌릴 수 없음)가 확정된다."""
    mgr = _mgr(fake_lsf, lost_after_missing_polls=2)
    try:
        js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        qtbot.wait(200)
        for j in fake_lsf.jobs.values():
            j.vanished = True
        mgr.query_once(js)                      # 미발견 1회 (스트릭 1)
        qtbot.wait(200)
        assert js.jobs()[0].state is not JobState.LOST
        for j in fake_lsf.jobs.values():
            j.vanished = False
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
            mgr.kill(js)
        qtbot.wait(200)
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False)     # 재제출 — 스트릭 리셋돼야
        for j in fake_lsf.jobs.values():
            j.vanished = True
        mgr.query_once(js)                      # 새 실행의 미발견 1회
        qtbot.wait(300)
        st = js.jobs()[0].state
        print(f"\n재제출 후 미발견 1회 → {st.value} (LOST면 스트릭을 물려받은 것)")
        assert st is not JobState.LOST
    finally:
        mgr.shutdown()


def test_stale_signal_from_previous_cycle_is_ignored(qtbot, fake_lsf):
    """이전 사이클의 낡은 records_reset이 새 사이클의 무장을 건드리면 안 된다."""
    mgr = _mgr(fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False, post_process=lambda r: None)
        # 낡은 토큰으로 무장/폐기 신호를 흉내낸다
        mgr._on_records_reset(js.id, object())
        mgr._on_gate_rejected(js.id, object())
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
            mgr.kill(js)
        mgr.query_once(js)
        qtbot.wait(400)
        assert mgr.summary(js.id)["total"] == 1
    finally:
        mgr.shutdown()
