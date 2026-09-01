"""공용 유틸 — 동시 제출 슬롯 / 발화 스로틀 / 활동 원장 (Qt 비의존)."""
from __future__ import annotations

import threading
import time


class WorkerSlots:
    """동시 제출 슬롯 — 사이클 하나의 동시 wrapper 수를 묶는다.

    전역 상한은 submitter의 **공용 QThreadPool 크기**가 잡는다. 이 클래스는
    호출별 workers가 그보다 **낮을 때** 그 사이클만 더 조이는 용도다
    (풀은 공용이라 사이클마다 크기를 달리 줄 수 없다).

    acquire()는 슬롯이 날 때까지 짧게 대기하며, should_stop()이 True면
    False를 반환하고 즉시 빠져나온다 — 취소(kill)가 슬롯 대기에 갇히면
    quiesce가 그만큼 밀린다.
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


class LogSampler:
    """부류별로 앞의 N건만 남기고 접는 로그 표본기 — thread-safe.

    job 하나마다 한 줄씩 남기는 로그는 실패가 몇 건일 때는 유용하지만,
    mbatchd 과부하로 5000건이 한꺼번에 떨어지면 그 자체가 부하가 된다
    (핸들러가 GUI 위젯이면 이벤트루프까지 막는다). 부류(fail_reason)마다
    앞 N건은 그대로 남기고, 그 뒤로는 접었다는 사실을 한 번만 알린다.

    사이클마다 새로 만들어 쓴다(제출 1회 = 표본기 1개) — 접힌 상태가
    다음 제출로 새지 않는다. 전체 내역은 완료 로그의 부류별 요약이 준다.

    kind는 hashable이면 뭐든 된다. **메시지 종류가 다르면 kind도 달라야
    한다** — 예산을 공유하면 각 메시지가 반씩만 나와 둘 다 잘린 것처럼 보인다.
    """

    def __init__(self, limit: int = 20):
        self.limit = max(1, int(limit))
        self._n: dict = {}
        #: 한도를 막 넘긴 부류 — 접힘 통지를 아직 안 낸 것. 카운트 값 비교
        #: (n == limit+1)로 판정하면 allow/just_folded 두 lock 획득 사이에
        #: 다른 스레드의 allow가 끼어들어(n이 limit+2로) 통지가 유실된다 —
        #: 넘긴 순간을 여기 적어 두고 읽는 쪽이 지우면 정확히 1회다.
        self._folded_pending: set = set()
        self._lock = threading.Lock()

    def allow(self, kind) -> bool:
        """이 부류를 아직 그대로 남길지. limit번째 직후 1회만 False 대신
        'folded'를 알리도록 just_folded()와 짝지어 쓴다."""
        with self._lock:
            n = self._n.get(kind, 0) + 1
            self._n[kind] = n
            if n == self.limit + 1:
                self._folded_pending.add(kind)
            return n <= self.limit

    def just_folded(self, kind) -> bool:
        """이 부류가 한도를 넘겼는데 접힘 통지가 아직 안 나갔는가 —
        True를 돌려주며 소진한다(정확히 1회, 어느 스레드가 받아도 무방)."""
        with self._lock:
            if kind in self._folded_pending:
                self._folded_pending.discard(kind)
                return True
            return False
