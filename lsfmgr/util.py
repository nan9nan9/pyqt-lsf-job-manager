"""공용 유틸 — thread-safe rate limiter 등 (Qt 비의존)."""
from __future__ import annotations

import threading
import time
from typing import Optional


class TokenBucketLimiter:
    """token bucket 방식 rate limiter (CS-6).

    rate_per_s가 None이면 무제한. acquire()는 토큰 확보까지 짧게 대기하며,
    cancel_event가 set되면 False를 반환하고 즉시 빠져나온다.
    """

    def __init__(self, rate_per_s: Optional[float], burst: Optional[int] = None):
        self.rate = float(rate_per_s) if rate_per_s else 0.0
        self.capacity = float(burst if burst is not None
                              else max(1.0, self.rate))
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, cancel_event: Optional[threading.Event] = None) -> bool:
        if self.rate <= 0:
            return True
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity,
                                   self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                wait = (1.0 - self._tokens) / self.rate
            if cancel_event is not None and cancel_event.wait(min(wait, 0.05)):
                return False
            if cancel_event is None:
                time.sleep(min(wait, 0.05))


class EmitThrottler:
    """progress Signal emit 빈도 제한 (QT-5) — thread-safe.

    min_interval_s 경과 또는 진행률 min_step_ratio 이상 변화 시에만 True.
    마지막(done == total) 통지는 항상 True.
    """

    def __init__(self, min_interval_s: float = 0.5,
                 min_step_ratio: float = 0.01):
        self.min_interval_s = min_interval_s
        self.min_step_ratio = min_step_ratio
        self._last_t = 0.0
        self._last_done = -1
        self._lock = threading.Lock()

    def should_emit(self, done: int, total: int) -> bool:
        with self._lock:
            if done >= total:
                self._last_t = time.monotonic()
                self._last_done = done
                return True
            now = time.monotonic()
            step = max(1, int(total * self.min_step_ratio))
            if (now - self._last_t >= self.min_interval_s
                    or done - self._last_done >= step):
                self._last_t = now
                self._last_done = done
                return True
            return False


# ----------------------------------------------------------------------
# 활동 원장 헬퍼 — "jobset_id → 항목 리스트"의 identity 기준 추가/제거.
# killer(kill별 진행 slot)와 lifecycle.SubmitGate(submit 활동)가 공유한다.
# caller가 자신의 lock을 쥔 채 호출한다(각자 다른 lock/공유 상태라 lock은 주입
# 안 함). list.remove(equality)는 겹친 항목의 값이 우연히 같으면([0,0] 등)
# 남의 항목을 지우므로 반드시 identity(is)로 제거한다.
# ----------------------------------------------------------------------
def ledger_add(table: dict, key: str, item) -> None:
    """dict-of-lists에 항목 추가 (caller가 lock 보유)."""
    table.setdefault(key, []).append(item)


def ledger_remove(table: dict, key: str, item) -> None:
    """identity 기준 제거 + 빈 리스트가 되면 키 삭제 (caller가 lock 보유, 멱등)."""
    lst = table.get(key)
    if not lst:
        return
    for i, x in enumerate(lst):
        if x is item:
            del lst[i]
            break
    if not lst:
        del table[key]
