"""전체 정독 리뷰 사이클 15 — job 추가 시점 폴링 재개 조건.

C15-1 (live 결함): 당시 merge가 **source가 폴링 중이었을 때만** 폴링을
재개했다 — target 자신의 폴링 기억은 min()의 피연산자로만 쓰이고 재개의
근거로는 쓰이지 않았다. 그래서:

  ① jobset 제출 → 폴링 시작 (_poll_intervals에 interval 기억)
  ② 전원 terminal → monitor._maybe_auto_stop이 **서비스 타이머만** 끈다.
     manager의 기억은 남는다(재제출 시 되살리기 위한 설계).
  ③ CREATED job이 새로 들어옴 → 재개 판단이 엉뚱한 근거를 보고 건너뛴다.
  ④ 관찰할 일(비terminal job)이 다시 생겼는데 폴링은 죽은 채다.
     handler는 폴링 tick에 tie돼 있으므로(handlers.tick ← _on_poll_updated)
     새 job에 대해 **영영 침묵**한다.

판단 기준은 "이 jobset에 아직 관찰할 job이 있는가" 하나다
(manager._resume_polling_if_watchable). merge가 삭제되고 job 추가가
add_jobs/upsert_jobs로 바뀐 뒤에도 같은 지점이 그 역할을 한다.

※ 이 재개는 "사용자가 stop_polling으로 끈 폴링은 되살리지 않는다"는 계약보다
  우선한다 — 관찰 대상이 있는데 조용히 멈춰 있는 쪽이 더 나쁜 실패이기 때문.
"""
from __future__ import annotations

from tests.conftest import mk_jobset
from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _mgr(runner):
    return LsfJobManager(store=InMemoryStore(),
                         config=LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05), runner=runner)


def _poll(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.jobset_updated, timeout=10000):
        mgr.query_once(js)
    qtbot.wait(150)


def _polling_on(mgr, jsid):
    """PollingService에 살아있는 타이머가 있는지 (관찰용 내부 접근)."""
    return jsid in mgr.polling._worker._timers


def test_add_jobs_resumes_polling_after_auto_stop(qtbot):
    """C15-1: 자동 중지된 폴링이 job 추가로 되살아난다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=5)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)                    # 전원 terminal → 자동 중지
        assert not _polling_on(mgr, js.id)
        assert mgr._poll_intervals.get(js.id) == 5.0     # 기억은 남는다

        mgr.add_jobs(js, ["customwrapper_sub b.sp"], job_keys=["b"])
        qtbot.wait(150)
        assert _polling_on(mgr, js.id), "추가분이 있는데 폴링이 재개되지 않았다"
    finally:
        mgr.shutdown()


def test_merged_job_handler_runs(qtbot):
    """C15-1의 실제 증상 — 흡수된 신규 job의 handler가 돌아야 한다.

    폴링이 죽어 있으면 handlers.tick 자체가 안 불려 조용히 침묵한다.

    ⚠️ 이 테스트는 **폴링 타이머로만** 구동한다 — query_once를 끼우면
    그것이 tick을 대신 돌려서(poll_now → updated → handlers.tick) 타이머가
    죽어 있어도 handler가 돌아, 결함을 못 잡는 테스트가 된다.
    interval은 mgr.start_polling 직접 호출로 짧게 준다(옵션 경로의 5~60초
    검증은 직접 호출에는 적용되지 않는다).
    """
    fake = FakeLsf()
    mgr = _mgr(fake)
    seen = []
    mgr.handler_finished.connect(lambda j, n, r: seen.append(r.job_key))
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        mgr.add_handler(js.id, "h", lambda ctx: ctx.job_key)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        mgr.start_polling(js, 0.15)              # 빠른 타이머 + 기억 등록
        # start_polling은 queued(polling 전용 스레드) — 등록을 먼저 확인해야
        # 아래 "중지 대기"가 등록 전 빈 dict을 보고 즉시 통과하지 않는다
        qtbot.waitUntil(lambda: _polling_on(mgr, js.id), timeout=5000)
        fake.set_all("DONE", 0)
        qtbot.waitUntil(lambda: not _polling_on(mgr, js.id), timeout=5000)
        assert js.is_done                        # 자동 중지는 전원 terminal로

        added = mgr.add_jobs(js, ["customwrapper_sub b.sp"], job_keys=["b"])
        new_key = added[0].job_key

        # 재제출은 추가분 포함 전 job — 이후는 **타이머만** 관찰에 쓴다
        seen.clear()
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        qtbot.wait(400)
        fake.set_all("DONE", 0)
        qtbot.waitUntil(lambda: new_key in seen, timeout=5000)
    finally:
        mgr.shutdown()


def test_add_resumes_even_if_polling_was_explicitly_stopped(qtbot):
    """판단 기준은 "볼 것이 있는가" — 기억(interval) 유무가 아니다.

    사용자가 stop_polling으로 껐어도(= 기억까지 삭제) 추가 후 관찰 대상이
    남으면 기본 interval로 재개한다. "껐다"보다 "볼 것이 있다"가 우선한다 —
    관찰 대상이 있는데 조용히 멈춰 있으면 handler가 죽기 때문.
    """
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=5)
        mgr.stop_polling(js)                     # 사용자가 명시적으로 끔
        qtbot.wait(150)
        assert mgr._poll_intervals.get(js.id) is None   # 기억까지 삭제됨

        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)
        mgr.add_jobs(js, ["customwrapper_sub b.sp"],
                     job_keys=["b"])            # 추가분(CREATED)이 남는다
        qtbot.waitUntil(lambda: _polling_on(mgr, js.id), timeout=5000)
    finally:
        mgr.shutdown()


def test_replace_of_finished_jobs_does_not_start_polling(qtbot):
    """반대로 볼 것이 없으면 켜지 않는다 — 편집해도 결과가 전원 terminal이면
    쓸데없는 bjobs 호출이 나가면 안 된다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=5)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)                    # 전원 terminal → 자동 중지
        qtbot.waitUntil(lambda: not _polling_on(mgr, js.id), timeout=5000)

        # job을 지우면 남는 것도 없다 — 관찰 대상 0
        mgr.clear_jobs(js)
        qtbot.wait(300)
        assert not _polling_on(mgr, js.id), "볼 것이 없는데 폴링이 켜졌다"
    finally:
        mgr.shutdown()


def test_resume_uses_remembered_interval(qtbot):
    """재개 시 이 jobset의 기억된 interval을 쓴다 (없으면 기본값)."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=7)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)
        qtbot.waitUntil(lambda: not _polling_on(mgr, js.id), timeout=5000)
        assert mgr._poll_intervals.get(js.id) == 7.0     # 기억은 남는다

        mgr.add_jobs(js, ["customwrapper_sub b.sp"], job_keys=["b"])
        qtbot.wait(200)
        assert mgr._poll_intervals.get(js.id) == 7.0     # 그 값으로 재개
        assert _polling_on(mgr, js.id)
    finally:
        mgr.shutdown()
