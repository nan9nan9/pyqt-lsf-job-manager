"""제출 착수와 지연 요청은 자신이 속한 제출만 처리한다."""
import threading

from lsfmgr import JobState, Options
from lsfmgr.submitter import _PendingRetry


def test_pre_submit_failure_does_not_change_unselected_records(manager, qtbot):
    js = manager.create_jobset(["tool a", "tool b"], job_keys=["a", "b"])
    before = manager.store.transition(js.id, "b", JobState.SUBMITTING)

    def reject(commands):
        raise RuntimeError("pre-submit rejected")

    with qtbot.waitSignal(manager.submit_finished, timeout=3000):
        manager.submit(js, only=["a"], pre_submit=reject, auto_poll=False)
    after = manager.store.get_job(js.id, "b")
    assert after == before
    assert after._revision == before._revision


def test_delayed_retry_request_does_not_arm_next_submission(manager, qapp, monkeypatch):
    js = manager.create_jobset(["tool a"], job_keys=["a"])
    sub = manager.submitter
    old = sub._new_context(js.id, ["a"], Options())
    timers = []
    monkeypatch.setattr("lsfmgr.submitter.QTimer.singleShot", lambda ms, fn: timers.append(fn))
    thread = threading.Thread(target=lambda: sub._schedule_retry(old, "a", 1, lambda: None))
    thread.start()
    thread.join(3)
    assert not thread.is_alive()
    with old.retry_lock:
        old.pending_retries.clear()
    sub._count(old, cancelled=True)

    current = sub._new_context(js.id, ["a"], Options())
    current.pending_retries["a"] = _PendingRetry("a", 10, lambda: None)
    try:
        qapp.processEvents()  # 이전 cycle의 worker 신호가 새 cycle 접수 뒤 도착
        assert timers == []
        assert "a" in current.pending_retries
    finally:
        current.pending_retries.clear()
        sub._count(current, cancelled=True)
