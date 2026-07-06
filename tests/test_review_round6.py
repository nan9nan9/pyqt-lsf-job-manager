"""정독 리뷰 6차 — events 로그 오염 / stats 이중 집계 버그의 회귀 테스트.

SqliteStore.transition이 상태가 바뀌지 않는 재설정(worker의 SUBMITTING
재설정, RUN 중 working_dir/exit_code 갱신 등)에도 events를 기록해:
  1. get_history에 old_state==new_state인 가짜 전이가 쌓이고,
  2. stats()의 PEND→RUN 대기시간이 이중 집계됐다.
→ 실제 상태 전이(old.state != new_state)일 때만 event를 남기도록 수정.
"""
from __future__ import annotations

from lsfmgr import LsfJobManager, SqliteStore
from lsfmgr.states import JobState


def _sqlite_mgr(tmp_path, fake_lsf, config):
    return LsfJobManager(store=SqliteStore(str(tmp_path / "db.sqlite")),
                         config=config, runner=fake_lsf)


def test_no_same_state_events_in_history(qtbot, fake_lsf, config, tmp_path):
    """정상 submit → 폴링 전이에 same-state 이벤트가 없어야 한다."""
    mgr = _sqlite_mgr(tmp_path, fake_lsf, config)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = mgr.submit(["echo a"], mode="bulk", auto_poll=False)
        jid = js.jobs()[0].job_id
        fake_lsf.set_job(jid, "RUN")
        mgr.querier.query(js.id)
        fake_lsf.set_job(jid, "DONE")
        mgr.querier.query(js.id)

        hist = mgr.get_history(js.id)
        pairs = [(h["old_state"], h["new_state"]) for h in hist]
        assert all(o != n for o, n in pairs), f"same-state 이벤트: {pairs}"
        # 전체 생명주기는 old_state 흐름으로 온전히 추적된다
        assert pairs == [("SUBMITTING", "PEND"), ("PEND", "RUN"),
                         ("RUN", "DONE")], pairs
    finally:
        mgr.shutdown()


def test_stats_pend_wait_not_double_counted(qtbot, fake_lsf, config, tmp_path):
    """working_dir가 RUN 전이보다 늦은 폴링에서 채워져도 PEND→RUN 대기시간이
    이중 집계되지 않는다."""
    mgr = _sqlite_mgr(tmp_path, fake_lsf, config)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = mgr.submit(["echo a"], mode="bulk", auto_poll=False)
        jid = js.jobs()[0].job_id
        # PEND→RUN (start_time만 세팅)
        fake_lsf.set_job(jid, "RUN")
        fake_lsf.jobs[str(jid)].start_time = "2026-07-07 10:00:00"
        mgr.querier.query(js.id)
        # working_dir가 뒤늦게 채워짐 (state는 여전히 RUN) — 가짜 RUN→RUN 금지
        fake_lsf.jobs[str(jid)].working_dir = "/work/dir"
        mgr.querier.query(js.id)

        stats = mgr.stats()
        assert stats["pend_wait_count"] == 1, stats
        run_events = [h for h in mgr.get_history(js.id)
                      if h["new_state"] == JobState.RUN.value]
        assert len(run_events) == 1, run_events
    finally:
        mgr.shutdown()


def test_stats_submit_success_rate_unaffected(qtbot, fake_lsf, config,
                                              tmp_path):
    """submit 성공/실패 집계는 정확히 유지된다 (PEND/SUBMIT_FAILED 전이 기준)."""
    mgr = _sqlite_mgr(tmp_path, fake_lsf, config)
    try:
        fake_lsf.fail_next_bsub = 1        # 첫 job 1회 실패 후 재시도 성공
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(["echo a", "echo b"], mode="bulk", auto_poll=False,
                       max_retry=2)
        st = mgr.stats()
        assert st["submit_success"] == 2 and st["submit_failed"] == 0, st
        assert st["submit_success_rate"] == 1.0, st
    finally:
        mgr.shutdown()
