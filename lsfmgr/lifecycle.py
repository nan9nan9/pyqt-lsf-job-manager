"""SubmitGate — jobset별 submit 활동 게이트 (kill 우선권의 구조적 보장, FR-3).

모든 submit 사이클(제출/재제출/array)은 시작 전에 register()를 통과해야
하고, kill은 KillScope.acquire()로 barrier를 올리는 순간 그 시점의 활동
목록을 원자적으로 넘겨받아 취소·대기한다. barrier가 올라간 동안의
register()는 거부된다.

'나중에 쫓아가서 취소'(cancel 후 재확인 루프)가 아니라 '시작 자체를
원자적으로 막는' 방식이라, 취소와 시작 사이의 경합 창이 구조적으로
존재하지 않는다 — 한 lock 아래에서 barrier 확인과 등록이 원자적이므로:

    활동 등록이 barrier보다 먼저 → kill이 목록으로 받아 취소+대기
    활동 등록이 barrier보다 나중 → register 거부 → 활동은 born-cancelled

순수 Python(threading) — Qt 무관, 단위 테스트 직격 가능.
"""
from __future__ import annotations

import threading

from .util import ledger_add, ledger_remove
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

__all__ = ["SubmitGate", "KillScope"]


@dataclass
class _Activity:
    """진행 중 submit 사이클 1건 — 취소 신호와 종료 대기 수단."""
    cancel_event: threading.Event
    wait: Callable[[float], bool]      # timeout_s 대기 → 시간 내 종료 여부
    timeout_s: float                   # 이 활동의 정지 대기 상한


class SubmitGate:
    """jobset별 활동 원장 + kill barrier. manager가 1개를 소유하고
    submitter(등록)와 killer(kill_scope)가 공유한다."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._activities: Dict[str, List[_Activity]] = {}
        self._barriers: Dict[str, int] = {}     # 겹치는 kill 지원 (카운트)

    def register(self, jobset_id: str, cancel_event: threading.Event,
                 wait: Callable[[float], bool],
                 timeout_s: float) -> Optional[_Activity]:
        """활동 시작 등록. 반환 토큰은 unregister에 사용.
        None이면 kill barrier가 올라가 있음 — caller는 활동을
        born-cancelled(cancel_event set 상태로 시작)로 만들어야 한다."""
        act = _Activity(cancel_event, wait, timeout_s)
        with self._lock:
            if self._barriers.get(jobset_id):
                return None
            ledger_add(self._activities, jobset_id, act)
        return act

    def unregister(self, jobset_id: str, act: _Activity) -> None:
        """활동 종료 — 멱등(이미 제거됐으면 no-op). identity로 제거한다 —
        equality 매칭(remove)은 겹친 활동의 필드가 우연히 같으면 남의
        등록을 지울 수 있다."""
        with self._lock:
            ledger_remove(self._activities, jobset_id, act)

    def kill_scope(self, jobset_id: str) -> "KillScope":
        return KillScope(self, jobset_id)

    # --- barrier 조작 — lock 규율을 이 클래스 한 곳에 모은다 -------------
    def _barrier_up(self, jobset_id: str) -> List[_Activity]:
        """barrier ↑ + 그 시점 활동 목록 반환 (원자적)."""
        with self._lock:
            self._barriers[jobset_id] = self._barriers.get(jobset_id, 0) + 1
            return list(self._activities.get(jobset_id, ()))

    def _barrier_down(self, jobset_id: str) -> None:
        with self._lock:
            n = self._barriers.get(jobset_id, 0) - 1
            if n > 0:
                self._barriers[jobset_id] = n
            else:
                self._barriers.pop(jobset_id, None)


class KillScope:
    """kill 1건의 우선권 구간. killer worker 스레드에서:

        acquire() — barrier↑ + 그 시점 활동 전부 취소 + 멎을 때까지 대기
        release() — barrier↓ (kill 완료 후, finally에서)

    barrier가 유지되는 동안 새 submit 등록은 거부되므로 'kill 진행 중
    도착한 제출/재제출은 취소된다'가 타이밍이 아닌 규칙이 된다."""

    def __init__(self, gate: SubmitGate, jobset_id: str):
        self._gate = gate
        self.jobset_id = jobset_id

    def acquire(self) -> bool:
        """반환: 시간 내 전부 정지 여부 — False면 caller(killer)가
        KillReport.errors로 보고한다 (스냅샷 이후 제출 완료분 유출 가능)."""
        acts = self._gate._barrier_up(self.jobset_id)
        for a in acts:                       # 취소 신호는 전부 먼저 —
            a.cancel_event.set()             # 대기가 병렬로 짧아진다
        ok = True
        for a in acts:
            ok &= a.wait(a.timeout_s)
        return ok

    def release(self) -> None:
        self._gate._barrier_down(self.jobset_id)
