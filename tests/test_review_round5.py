"""정독 리뷰 5차 — 동시성 경계/모니터링 엣지/극단 옵션 버그의 회귀 테스트.

1. submit/resubmit 진행 중 merge 허용: 소스 삭제로 worker가 크래시하고
   merged jobset에 SUBMITTING 좀비 레코드가 영구 잔존하던 버그 — 거부.
2. add_job 동일 job_key: 기존 레코드를 silent overwrite하던 버그 — 거부
   (merge의 충돌 선검사와 동일 원칙).
3. expo backoff 극단값: attempt 21부터 QTimer int32(ms) 한도를 초과해
   OverflowError → slot 예외 → PyQt abort로 이어지던 버그 — 1일로 clamp.
4. 부착물 없는 jobset kill 중 chunk bkill 장애: 예외 전파로 kill_finished가
   영영 발행되지 않던 버그 — errors에 담고 완료 (optimistic 오표시도 차단).
5. wrapper가 array job을 제출한 경우: 레코드(array_index=None)가 bjobs의
   element 행(id[idx])과 매칭되지 않아 RUN 중인데 LOST로 오판하던 버그 —
   같은 job_id의 element 상태를 집계해 대표 상태로 반영.
6. start_polling(0): 무검증으로 QTimer 핫루프(bjobs 연타)가 되던 버그 —
   양수 검증.
"""
from __future__ import annotations

import threading

import pytest

from lsfmgr import InMemoryStore, LsfJobManager
from lsfmgr.errors import LsfmgrError
from lsfmgr.options import MAX_RETRY_DELAY_S, Options
from lsfmgr.states import JobRecord, JobState
from tests.fake_lsf import FakeLsf


# ----------------------------------------------------------------------
# 1. submit 진행 중 merge 거부
# ----------------------------------------------------------------------
class _GatedLsf(FakeLsf):
    """bsub을 gate가 열릴 때까지 붙잡아 'submit 진행 중' 상태를 만든다."""

    def __init__(self):
        super().__init__()
        self.gate = threading.Event()

    def _do_bsub(self, args):
        self.gate.wait(10)
        return super()._do_bsub(args)


def test_merge_during_active_submit_rejected(qtbot, config):
    lsf = _GatedLsf()
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js_done = mgr.submit(["echo z"], auto_poll=False)
            lsf.gate.set()                    # 첫 jobset은 완료시킴
        lsf.gate.clear()
        js_active = mgr.submit(["echo a", "echo b"], mode="bulk",
                               workers=1, auto_poll=False)
        assert mgr.submitter.is_active(js_active.id)
        with pytest.raises(LsfmgrError):
            mgr.merge_jobsets([js_active.id, js_done.id])
        lsf.gate.set()
        qtbot.waitUntil(lambda: not mgr.submitter.is_active(js_active.id),
                        timeout=10000)
        # 소스가 온전해야 완료 후 merge는 정상 동작
        merged = mgr.merge_jobsets([js_active.id, js_done.id])
        assert mgr.summary(merged)["total"] == 3
    finally:
        lsf.gate.set()
        mgr.shutdown()


# ----------------------------------------------------------------------
# 2. add_job 동일 job_key 거부
# ----------------------------------------------------------------------
def test_add_job_duplicate_key_rejected(manager):
    jsid = manager.create_jobset(1, label="dup").id
    rec = JobRecord(job_id=111, array_index=None, jobset_id=jsid,
                    lsf_job_name="manual_1", state=JobState.RUN,
                    command="echo a")
    manager.add_job(jsid, rec, sync_lsf=False)
    dup = JobRecord(job_id=222, array_index=None, jobset_id=jsid,
                    lsf_job_name="manual_1", state=JobState.PEND,
                    command="echo b")
    with pytest.raises(ValueError):
        manager.add_job(jsid, dup, sync_lsf=False)
    kept = manager.get_jobs(jsid)[0]
    assert kept.job_id == 111 and kept.command == "echo a"
    # remove 후 재추가는 허용 (정상 교체 경로)
    manager.remove_job(jsid, "manual_1")
    manager.add_job(jsid, dup, sync_lsf=False)
    assert manager.get_jobs(jsid)[0].job_id == 222


# ----------------------------------------------------------------------
# 3. 재시도 대기 clamp — QTimer int32(ms) 한도
# ----------------------------------------------------------------------
def test_retry_delay_clamped_to_qtimer_range():
    o = Options(retry_backoff="expo:2")
    for attempt in (0, 5, 21, 35, 100, 10_000):
        delay = o.retry_delay_s(attempt)
        assert delay <= MAX_RETRY_DELAY_S
        assert int(delay * 1000) <= 2**31 - 1
    assert o.retry_delay_s(0) == 2.0                  # 정상 구간은 불변
    assert o.retry_delay_s(3) == 16.0
    assert Options(retry_backoff="fixed:99999999999").retry_delay_s(0) \
        == MAX_RETRY_DELAY_S
    assert Options(retry_backoff="expo:0").retry_delay_s(100) == 0.0


# ----------------------------------------------------------------------
# 4. chunk bkill 장애에도 kill_finished 발행
# ----------------------------------------------------------------------
def test_kill_chunk_failure_still_finishes(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(["customwrapper_sub -i a.sp"],
                                    auto_poll=False)
    fake_lsf.fail_next_bkill = 1          # mbatchd 장애 주입
    with qtbot.waitSignal(manager.kill_finished, timeout=5000) as blocker:
        js.kill()
    report = blocker.args[1]
    assert report.errors
    # 장애 상황이므로 optimistic 오표시(EXIT) 금지 — 재시도 여지 유지
    assert js.jobs(states={JobState.EXIT}) == []
    # 장애 해소 후 재kill은 정상 완료
    with qtbot.waitSignal(manager.kill_finished, timeout=5000) as blocker:
        js.kill()
    assert not blocker.args[1].errors
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# 5. wrapper가 제출한 array job — 집계 상태로 추적
# ----------------------------------------------------------------------
def test_wrapper_submitted_array_not_lost(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(
            [["customwrapper_sub", "-J", "arr[1-3]", "echo", "hi"]],
            auto_poll=False)
    fake_lsf.set_all("RUN")

    manager.querier.query(js.id)          # 폴링 1사이클 (동기)
    rec = js.jobs()[0]
    assert rec.state is JobState.RUN      # element 집계 — LOST 아님

    # 일부 실패로 종료 → EXIT(exit_code)로 집계
    fake_lsf.set_all("DONE")
    fake_lsf.set_job(rec.job_id, "EXIT", exit_code=7, array_index=2)
    manager.querier.query(js.id)
    rec = js.jobs()[0]
    assert rec.state is JobState.EXIT and rec.exit_code == 7


def test_wrapper_submitted_array_bhist_fallback(qtbot, manager, fake_lsf):
    """bjobs에서 사라진 wrapper-array job도 bhist element 블록 집계로 종결."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(
            [["customwrapper_sub", "-J", "arr[1-2]", "echo", "hi"]],
            auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_all("DONE")
    fake_lsf.vanish_job(rec.job_id, in_bhist=True)

    manager.querier.query(js.id)
    rec = js.jobs()[0]
    assert rec.state is JobState.DONE     # LOST가 아니라 bhist 집계로 DONE


# ----------------------------------------------------------------------
# 6. polling interval 검증
# ----------------------------------------------------------------------
def test_start_polling_rejects_nonpositive(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], auto_poll=False)
    with pytest.raises(ValueError):
        js.start_polling(0)
    with pytest.raises(ValueError):
        js.start_polling(-1.0)
    js.start_polling(5.0)                 # 정상 구간은 동작
    js.stop_polling()
