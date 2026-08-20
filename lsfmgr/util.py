"""공용 유틸 — thread-safe rate limiter 등 (Qt 비의존)."""
from __future__ import annotations

import threading
import time
from typing import Optional


#: 버킷 용량 = rate × 이 배수. eauth를 두들기는 건 **지속 부하**이지 짧은
#: 버스트가 아니므로, 소량 제출은 제한 없이 통과시키고 대량 제출만 초당
#: rate로 눌러야 한다. 용량이 rate와 같으면(옛 기본값) 30건짜리 소량 제출도
#: 5초씩 걸렸다(rate=5 기준 실측).
BURST_FACTOR = 10


class TokenBucketLimiter:
    """token bucket 방식 rate limiter.

    rate_per_s가 None이면 무제한. acquire()는 토큰 확보까지 짧게 대기하며,
    cancel_event가 set되면 False를 반환하고 즉시 빠져나온다.

    burst 미지정이면 rate × BURST_FACTOR — "버킷 용량만큼은 즉시, 그 뒤로는
    초당 rate". ※ limiter는 **제출 사이클 1건당 하나**라 버킷도 사이클마다
    새로 찬다: 소량 제출을 연달아 하면 매번 용량만큼 즉시 나간다. 동시 실행
    수는 그와 별개로 workers가 계속 제한한다.
    """

    def __init__(self, rate_per_s: Optional[float], burst: Optional[int] = None):
        self.rate = float(rate_per_s) if rate_per_s else 0.0
        self.capacity = float(burst if burst is not None
                              else max(1.0, self.rate * BURST_FACTOR))
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


class WorkerSlots:
    """**전역** 동시 제출 상한 — 제출 사이클이 몇 개든 동시에 도는 wrapper
    프로세스 총수가 이 값을 넘지 않게 한다.

    왜 사이클별 QThreadPool만으로는 부족한가: 풀은 제출 사이클마다 새로
    만들어지므로(submitter._new_context) jobset을 3개 동시에 제출하면
    wrapper가 8이 아니라 24개 뜬다. workers를 낮게 잡아 LSF 인증(eauth)/
    mbatchd 과부하를 막으려는 사이트에서 그 보호가 동시 제출 수만큼 무력화된다.

    풀을 하나로 합치지 않고 세마포어를 얹는 이유: 사이클별 pool.waitForDone이
    "내 사이클의 제출이 멎었는가"를 뜻해야 한다(kill 우선권의 quiesce가
    그 위에 서 있다). 풀을 공유하면 A jobset의 kill이 B jobset의 제출까지
    기다리게 된다.

    acquire()는 슬롯이 날 때까지 짧게 대기하며, should_stop()이 True면
    False를 반환하고 즉시 빠져나온다(취소가 슬롯 대기에 갇히지 않게).
    """

    def __init__(self, limit: int):
        self.limit = max(1, int(limit))
        self._sem = threading.Semaphore(self.limit)

    def acquire(self, should_stop=None) -> bool:
        while True:
            if self._sem.acquire(timeout=0.05):
                return True
            if should_stop is not None and should_stop():
                return False

    def release(self) -> None:
        self._sem.release()


class EmitThrottler:
    """progress Signal emit 빈도 제한 — thread-safe.

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
