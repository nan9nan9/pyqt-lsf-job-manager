"""실패 진단 기능 테스트 — fail_message 저장 + job_detail 온디맨드 조회.

- SUBMIT_FAILED/RETRY_WAIT: bsub/wrapper 실행의 stderr/stdout(터미널 메시지)을
  JobRecord.fail_message에 저장한다.
- EXIT의 원인은 저장하지 않는다(폴링 오버헤드 0) — 앱에서 상태 클릭 시
  fetch_job_detail/job_detail로 bhist -l 원문을 온디맨드 조회한다.
"""
from __future__ import annotations

from lsfmgr import SqliteStore
from lsfmgr.states import JobState


# ----------------------------------------------------------------------
# SUBMIT_FAILED — 터미널 stderr 보존
# ----------------------------------------------------------------------
def test_submit_failed_keeps_terminal_stderr(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 10          # rc=1 + "LSF error: queue unavailable"
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False, max_retry=1)
    rec = js.jobs()[0]
    assert rec.state is JobState.SUBMIT_FAILED
    assert "queue unavailable" in rec.fail_message


def test_wrapper_parse_failure_keeps_stdout(qtbot, manager, fake_lsf):
    """NO_JOBID_PARSED는 stdout에 단서가 있다 — stdout도 담겨야 한다."""
    fake_lsf.no_jobid_next_bsub = 1
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(["primesim_sub -i a.sp"],
                                    auto_poll=False)
    rec = js.jobs()[0]
    assert rec.state is JobState.SUBMIT_FAILED
    assert "garbled output" in rec.fail_message


def test_retry_wait_carries_message_then_cleared_on_success(
        qtbot, manager, fake_lsf):
    """재시도 대기 중에도 마지막 시도의 메시지가 보이고, 최종 성공하면
    지워진다 (이전 실패 흔적 잔존 금지)."""
    fake_lsf.fail_next_bsub = 1           # 1회 실패 후 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False, max_retry=3)
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.fail_message is None


def test_array_submit_failure_keeps_message(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 10
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit("run_task", count=3, auto_poll=False,
                            max_retry=0)
    for rec in js.jobs():
        assert rec.state is JobState.SUBMIT_FAILED
        assert "queue unavailable" in rec.fail_message


def test_exit_does_not_trigger_extra_bhist(qtbot, manager, fake_lsf):
    """EXIT 감지는 자동 수집을 하지 않는다 — 폴링 사이클에 bhist 추가 호출 0."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=7)

    before = len(fake_lsf.calls_of("bhist"))
    manager.querier.query(js.id)          # 폴링 1사이클 (동기)
    rec = js.jobs()[0]
    assert rec.state is JobState.EXIT
    assert rec.fail_message is None       # 자동 저장 안 함
    assert len(fake_lsf.calls_of("bhist")) == before


# ----------------------------------------------------------------------
# 온디맨드 상세 조회 — job_detail / fetch_job_detail
# ----------------------------------------------------------------------
def test_job_detail_sync_exit(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=7)

    text = js.job_detail(rec.job_key)
    assert "Exited with exit code 7" in text
    assert f"Job <{rec.job_id}>" in text  # bhist -l 원문


def test_job_detail_submit_failed_falls_back(qtbot, manager, fake_lsf):
    """제출 실패 job(job_id 없음)은 저장된 터미널 메시지를 돌려준다."""
    fake_lsf.fail_next_bsub = 10
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False, max_retry=0)
    rec = js.jobs()[0]
    assert "queue unavailable" in js.job_detail(rec.job_key)


def test_job_detail_array_element(qtbot, manager, fake_lsf):
    """array element는 그 element의 bhist 블록만 온다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit("run_task", count=3, auto_poll=False)
    fake_lsf.set_all("DONE")
    rec = next(r for r in js.jobs() if r.array_index == 2)
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=9, array_index=2)

    text = js.job_detail(rec.job_key)
    assert "Exited with exit code 9" in text
    assert f"Job <{rec.job_id}[2]>" in text
    assert f"Job <{rec.job_id}[1]>" not in text   # 다른 element 미포함


def test_fetch_job_detail_async_signal(qtbot, manager, fake_lsf):
    """비동기 버전 — 결과가 js.job_detail_ready Signal로 온다 (GUI 클릭용)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=5)

    with qtbot.waitSignal(js.job_detail_ready, timeout=10000) as blocker:
        js.fetch_job_detail(rec.job_key)
    key, text = blocker.args
    assert key == rec.job_key
    assert "Exited with exit code 5" in text


def test_fetch_job_detail_error_reported_in_text(qtbot, manager, fake_lsf):
    """bhist 장애여도 Signal은 오고, 본문에 조회 실패가 담긴다 (UI 무한대기 방지)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.fail_all_queries = True

    with qtbot.waitSignal(manager.job_detail_ready, timeout=10000) as blocker:
        manager.fetch_job_detail(js.id, rec.job_key)
    assert "조회 실패" in blocker.args[2]


# ----------------------------------------------------------------------
# resubmit 리셋 / 영속화
# ----------------------------------------------------------------------
def test_resubmit_clears_fail_message(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 1           # 최초 1회만 실패 — 재제출은 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False, max_retry=0)
    rec = js.jobs()[0]
    assert rec.fail_message

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([rec.job_key])   # 이번엔 성공
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.fail_message is None


def test_fail_message_persisted_in_sqlite(qtbot, sqlite_manager, fake_lsf,
                                          tmp_path):
    fake_lsf.fail_next_bsub = 10
    with qtbot.waitSignal(sqlite_manager.submit_finished, timeout=10000):
        js = sqlite_manager.submit(["echo a"], auto_poll=False, max_retry=0)
    jsid = js.id

    reopened = SqliteStore(str(tmp_path / "db.sqlite"))   # 새 세션으로 재오픈
    try:
        rec = reopened.get_jobs(jsid)[0]
        assert rec.state is JobState.SUBMIT_FAILED
        assert "queue unavailable" in rec.fail_message
    finally:
        reopened.close()
