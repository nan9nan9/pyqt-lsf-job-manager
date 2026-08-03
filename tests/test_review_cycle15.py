"""전체 정독 리뷰 사이클 15 — merge 시점 폴링 재개 조건의 결함.

C15-1 (live 결함): merge가 **source가 폴링 중이었을 때만** 폴링을 재개했다.

    tgt_iv = self._poll_intervals.get(tid)
    if src_iv is not None:                      # ← 결함
        self.start_polling(tid, min(src_iv, tgt_iv) if tgt_iv else src_iv)

target 자신의 폴링 기억(tgt_iv)이 min()의 피연산자로만 쓰이고 재개의 근거로는
쓰이지 않는다. 그래서:

  ① target 제출 → 폴링 시작 (_poll_intervals[tid]에 interval 기억)
  ② 전원 terminal → monitor._maybe_auto_stop이 **서비스 타이머만** 끈다.
     manager의 기억은 남는다(재제출 시 되살리기 위한 설계).
  ③ CREATED 상태의 신규 jobset을 merge → src_iv=None(그 batch는 폴링한 적
     없음)이라 start_polling이 아예 안 불린다.
  ④ target에 관찰할 일(비terminal job)이 다시 생겼는데 폴링은 죽은 채다.
     handler는 폴링 tick에 tie돼 있으므로(handlers.tick ← _on_poll_updated)
     흡수된 신규 job에 대해 **영영 침묵**한다.

사용자가 명시적으로 끈 폴링은 stop_polling이 기억까지 지우므로(둘 다 None)
이 수정으로도 되살아나지 않는다 — "일부러 끈 폴링은 merge 이관으로 마음대로
되살아나지 않는다"는 기존 계약은 그대로다.
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _mgr(runner):
    return LsfJobManager(store=InMemoryStore(),
                         config=LsfConfig(retry_delay_s=0.05), runner=runner)


def _poll(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.jobset_updated, timeout=10000):
        mgr.query_once(js)
    qtbot.wait(150)


def _polling_on(mgr, jsid):
    """PollingService에 살아있는 타이머가 있는지 (관찰용 내부 접근)."""
    return jsid in mgr.polling._worker._timers


def test_merge_resumes_target_polling_after_auto_stop(qtbot):
    """C15-1: 자동 중지된 target 폴링이 merge로 되살아난다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=5)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)                    # 전원 terminal → 자동 중지
        assert not _polling_on(mgr, js.id)
        assert mgr._poll_intervals.get(js.id) == 5.0     # 기억은 남는다

        batch = mgr.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
        mgr.merge(js, batch)                     # source는 폴링한 적 없음
        qtbot.wait(150)
        assert _polling_on(mgr, js.id), "흡수분이 있는데 폴링이 재개되지 않았다"
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
        js = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
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

        batch = mgr.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
        new_key = batch.jobs()[0].job_key
        mgr.merge(js, batch)

        # 재제출은 흡수분 포함 전 job — 이후는 **타이머만** 관찰에 쓴다
        seen.clear()
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        qtbot.wait(400)
        fake.set_all("DONE", 0)
        qtbot.waitUntil(lambda: new_key in seen, timeout=5000)
    finally:
        mgr.shutdown()


def test_merge_resumes_even_if_polling_was_explicitly_stopped(qtbot):
    """판단 기준은 "볼 것이 있는가" — 기억(interval) 유무가 아니다.

    사용자가 stop_polling으로 껐어도(= 기억까지 삭제) 흡수 후 관찰 대상이
    남으면 기본 interval로 재개한다. "껐다"보다 "볼 것이 있다"가 우선한다 —
    관찰 대상이 있는데 조용히 멈춰 있으면 handler가 죽기 때문.
    """
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=True, poll_interval_s=5)
        mgr.stop_polling(js)                     # 사용자가 명시적으로 끔
        qtbot.wait(150)
        assert mgr._poll_intervals.get(js.id) is None   # 기억까지 삭제됨

        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, js)
        batch = mgr.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
        mgr.merge(js, batch)                     # 흡수분(CREATED)이 남는다
        qtbot.waitUntil(lambda: _polling_on(mgr, js.id), timeout=5000)
    finally:
        mgr.shutdown()


def test_merge_of_finished_sets_does_not_start_polling(qtbot):
    """반대로 볼 것이 없으면 켜지 않는다 — 완료본 둘을 합친 경우
    쓸데없는 bjobs 호출이 나가면 안 된다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        tgt = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        src = mgr.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
        for js in (tgt, src):
            with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
                mgr.submit(js, auto_poll=True, poll_interval_s=5)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, tgt)
        _poll(qtbot, mgr, src)                   # 양쪽 다 전원 terminal

        mgr.merge(tgt, src)
        qtbot.wait(300)
        assert not _polling_on(mgr, tgt.id), "볼 것이 없는데 폴링이 켜졌다"
    finally:
        mgr.shutdown()


def test_source_polling_interval_still_transfers(qtbot):
    """기존 계약 유지 — target이 폴링을 안 쓰는데 source가 쓰고 있었다면
    그 interval을 target이 이어받는다. (재개 **여부**는 관찰 대상 유무로
    판단하지만, 재개할 때 쓰는 **interval**은 여전히 src/tgt 중 짧은 쪽.)"""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        tgt = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(tgt, auto_poll=False)     # target은 폴링 없음
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, tgt)
        assert mgr._poll_intervals.get(tgt.id) is None

        # source는 폴링 중(interval 7) + 아직 CREATED — 흡수 후 관찰 대상이 된다
        src = mgr.create_jobset(["customwrapper_sub b.sp"], merge_ids=["b"])
        mgr.start_polling(src, 7)
        qtbot.wait(150)

        mgr.merge(tgt, src)
        qtbot.wait(200)
        assert mgr._poll_intervals.get(tgt.id) == 7.0    # source에서 이관
        assert _polling_on(mgr, tgt.id)
    finally:
        mgr.shutdown()
