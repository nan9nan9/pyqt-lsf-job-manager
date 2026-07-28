"""실패 진단 기능 테스트 — fail_message 저장 + job_detail 온디맨드 조회.

- SUBMIT_FAILED/RETRY_WAIT: bsub/wrapper 실행의 stderr/stdout(터미널 메시지)을
  JobRecord.fail_message에 저장한다.
- EXIT의 원인은 저장하지 않는다(폴링 오버헤드 0) — 앱에서 상태 클릭 시
  EXIT 원인은 자동 수집하지 않는다 (v10.3: bhist 조회 삭제).
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.states import JobState


# ----------------------------------------------------------------------
# SUBMIT_FAILED — 터미널 stderr 보존
# ----------------------------------------------------------------------
def test_submit_failed_keeps_terminal_stderr(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 10          # rc=1 + "LSF error: queue unavailable"
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False, max_retry=1)
    rec = js.jobs()[0]
    assert rec.state is JobState.SUBMIT_FAILED
    assert "queue unavailable" in rec.fail_message


def test_wrapper_parse_failure_keeps_stdout(qtbot, manager, fake_lsf):
    """NO_JOBID_PARSED는 stdout에 단서가 있다 — stdout도 담겨야 한다."""
    fake_lsf.no_jobid_next_bsub = 1
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["customwrapper_sub -i a.sp"], wrapper=True,
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
        js = submit_cmds(manager, ["echo a"], auto_poll=False, max_retry=3)
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.fail_message is None


def test_array_submit_failure_keeps_message(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 10
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, "run_task", count=3, auto_poll=False,
                            max_retry=0)
    for rec in js.jobs():
        assert rec.state is JobState.SUBMIT_FAILED
        assert "queue unavailable" in rec.fail_message


def test_exit_does_not_trigger_extra_bhist(qtbot, manager, fake_lsf):
    """EXIT 감지는 추가 조회를 하지 않는다 — bhist는 v10.3에서 삭제됐고
    (fake는 rc=127 스텁) 되살아나면 이 회귀가 잡는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=7)

    before = len(fake_lsf.calls_of("bhist"))
    manager.querier.query(js.id)          # 폴링 1사이클 (동기)
    rec = js.jobs()[0]
    assert rec.state is JobState.EXIT
    assert rec.fail_message is None       # 자동 저장 안 함
    assert len(fake_lsf.calls_of("bhist")) == before


# ----------------------------------------------------------------------
# resubmit 리셋 / 영속화
# ----------------------------------------------------------------------
def test_full_resubmit_clears_fail_message(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 1           # 최초 1회만 실패 — 재제출은 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False, max_retry=0)
    rec = js.jobs()[0]
    assert rec.fail_message

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js)                       # 전체 재제출 — 이번엔 성공
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.fail_message is None


