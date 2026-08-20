"""전체 정독 리뷰 7차 — 재시도 폭풍(실환경 eauth 과부하) 경로의 main 스레드.

- R7-1: 재시도 원장(ctx.pending_retries)이 ctx.lock으로 잠겨 있었다. ctx.lock은
  순서 보장을 위해 worker가 신호를 발화하는 동안에도 쥐는 lock이라 대량 제출
  중에는 상시 혼잡하다 — 거기에 원장까지 얹으니 main 스레드의
  _on_retry_requested가 재시도 1건마다 그 혼잡을 기다렸다.
  2000건 전량 실패→재시도 실측: main이 lock 대기로만 628ms를 썼고,
  main 이벤트 루프 응답 지연이 중앙값 36.6ms / p99 1164ms까지 벌어졌다.
  원장은 카운터와 원자적일 이유가 없다 → 전용 retry_lock으로 분리.
  (수정 후: 중앙값 0.2ms / p99 2.0ms, 처리 시간은 동일)
"""
from __future__ import annotations

import threading
import time

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.options import Options


def test_retry_ledger_does_not_wait_on_the_busy_context_lock(qtbot, fake_lsf):
    """ctx.lock을 다른 스레드가 쥐고 있어도 재시도 접수는 막히지 않는다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=fake_lsf)
    sub = mgr.submitter
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        ctx = sub._new_context(js.id, ["a"], Options())
        sub._schedule_retry(ctx, "a", 60.0, lambda: None)

        held = threading.Event()
        release = threading.Event()

        def hog():
            with ctx.lock:                   # worker가 발화 중인 상황 재현
                held.set()
                release.wait(5.0)

        t = threading.Thread(target=hog, daemon=True)
        t.start()
        assert held.wait(2.0)
        try:
            t0 = time.perf_counter()
            sub._on_retry_requested(js.id, "a")   # main 경로
            elapsed = time.perf_counter() - t0
        finally:
            release.set()
            t.join(5.0)
        assert elapsed < 0.2, (
            f"혼잡한 ctx.lock에 {elapsed*1000:.0f}ms 묶였다 "
            f"(재시도 원장은 전용 lock이어야 한다)")
    finally:
        mgr.shutdown()


def test_retry_storm_keeps_every_job(qtbot, fake_lsf):
    """lock을 나눠도 재시도 정확성은 그대로 — 전량 실패해도 전량 제출된다."""
    n = 300
    fake_lsf.fail_next_bsub = n              # 첫 시도 전량 실패
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(retry_delay_s=0.02,
                         retry_backoff=1.0),
        runner=fake_lsf)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(n)],
                               job_keys=[f"k{i}" for i in range(n)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000) as blk:
            mgr.submit(js, auto_poll=False, workers=8)
        rpt = blk.args[1]
        assert rpt.succeeded == n, f"{rpt.succeeded}/{n}만 성공"
        assert rpt.failed == 0 and rpt.cancelled == 0
        # 정확히 몇 건이 재시도됐는지는 fake의 실패 카운터를 재시도가 나눠
        # 쓰기 때문에 결정적이지 않다 — 재시도 경로를 실제로 탔다는 것만 본다.
        assert rpt.retried > n // 2, f"재시도 계상 {rpt.retried}"
        assert mgr.summary(js.id)["PEND"] == n
    finally:
        mgr.shutdown()
