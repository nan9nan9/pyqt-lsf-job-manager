"""SubmitGate — submit 활동 게이트 (kill 우선권의 구조적 보장).

모든 submit 사이클(제출/재제출)은 시작 전에 register()를 통과해야 하고,
kill은 KillScope.acquire()로 barrier를 올리는 순간 그 시점의 활동 목록을
원자적으로 넘겨받아 취소·대기한다. barrier가 올라간 동안의 register()는
(그 barrier가 덮는 job에 한해) 거부된다.

'나중에 쫓아가서 취소'(cancel 후 재확인 루프)가 아니라 '시작 자체를
원자적으로 막는' 방식이라, 취소와 시작 사이의 경합 창이 구조적으로
존재하지 않는다 — 한 lock 아래에서 barrier 확인과 등록이 원자적이므로:

    활동 등록이 barrier보다 먼저 → kill이 목록으로 받아 취소+대기
    활동 등록이 barrier보다 나중 → register 거부 → 그 job은 born-cancelled

**범위(keys)**: barrier와 취소는 kill이 **실제로 겨냥한 job**에만 걸린다.
`keys=None`은 "이 jobset 전체"(= 전체 kill)를 뜻하고, key 집합을 주면 그
job들만 제출이 멈춘다 — 선택 kill이 행 하나를 죽이겠다고 jobset의 제출
전체를 무너뜨리지 않게 하면서도, **겨냥한 job은 반드시** 죽게 한다
(제출 중이라 job_id가 없던 대상이 kill을 빠져나가는 유출을 막는 지점).

순수 Python(threading) — Qt 무관, 단위 테스트 직격 가능.
"""
from __future__ import annotations

import threading

from .util import ledger_add, ledger_remove
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, Iterable, List, Optional

__all__ = ["SubmitGate", "KillScope", "Registration"]

#: kill/취소의 범위 — None이면 "그 jobset의 전 job"
Scope = Optional[FrozenSet[str]]


@dataclass
class _Activity:
    """진행 중 submit 사이클 1건 — 취소 신호와 종료 대기 수단.

    둘 다 범위(Scope)를 받는다: None이면 사이클 전체, key 집합이면 그
    job들만 취소/대기한다."""
    cancel: Callable[[Scope], None]
    wait: Callable[[Scope, float], bool]     # (범위, timeout_s) → 시간 내 정지 여부
    timeout_s: float                         # 이 활동의 정지 대기 상한


@dataclass
class Registration:
    """register() 결과.

    activity: 등록 토큰 — None이면 이 사이클의 **전 job**이 barrier에 걸려
        등록되지 않았다(caller는 사이클을 born-cancelled로 만든다).
    refused: barrier에 걸린 job_key — 일부만 걸렸을 때 그 key들만
        born-cancelled로 처리하고 나머지는 정상 제출한다."""
    activity: Optional[_Activity]
    refused: FrozenSet[str]


class SubmitGate:
    """jobset별 활동 원장 + kill barrier. manager가 1개를 소유하고
    submitter(등록)와 killer(kill_scope)가 공유한다."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._activities: Dict[str, List[_Activity]] = {}
        # 겹치는 kill 지원 — barrier 1건당 항목 1개(범위 포함). 항목은
        # identity로 제거하므로 KillScope가 자기 범위 객체를 계속 들고 있다.
        self._barriers: Dict[str, List[Scope]] = {}

    def register(self, jobset_id: str, keys: Iterable[str],
                 cancel: Callable[[Scope], None],
                 wait: Callable[[Scope, float], bool],
                 timeout_s: float) -> Registration:
        """활동 시작 등록. keys는 이 사이클이 제출할 job_key 전체.

        barrier에 걸린 key는 `refused`로 돌려준다 — caller는 그 key만
        제출하지 않고 취소로 계상해야 한다. **전부** 걸리면 등록 자체를
        하지 않고 `activity=None`을 준다(사이클 통째로 born-cancelled)."""
        want = frozenset(keys)
        act = _Activity(cancel, wait, timeout_s)
        with self._lock:
            refused: set = set()
            for scope in self._barriers.get(jobset_id, ()):
                if scope is None:            # 전체 barrier — 더 볼 것 없다
                    refused = set(want)
                    break
                refused |= (want & scope)
            if want and refused >= want:
                # 전 job이 막혔다 — 등록하지 않는다. 그래야 뒤이은 kill이
                # 이 사이클을 '취소하고 기다려야 할 활동'으로 넘겨받지 않는다
                # (제출이 없으니 기다릴 것도 없다).
                return Registration(None, frozenset(want))
            ledger_add(self._activities, jobset_id, act)
        return Registration(act, frozenset(refused))

    def unregister(self, jobset_id: str, act: Optional[_Activity]) -> None:
        """활동 종료 — 멱등(이미 제거됐으면 no-op). identity로 제거한다 —
        equality 매칭(remove)은 겹친 활동의 필드가 우연히 같으면 남의
        등록을 지울 수 있다."""
        if act is None:
            return
        with self._lock:
            ledger_remove(self._activities, jobset_id, act)

    def kill_scope(self, jobset_id: str,
                   keys: Optional[Iterable[str]] = None) -> "KillScope":
        """kill 1건의 우선권 구간. keys=None이면 jobset 전체(전체 kill)."""
        return KillScope(self, jobset_id, keys)

    # --- barrier 조작 — lock 규율을 이 클래스 한 곳에 모은다 -------------
    def _barrier_up(self, jobset_id: str, scope: Scope) -> List[_Activity]:
        """barrier ↑ + 그 시점 활동 목록 반환 (원자적)."""
        with self._lock:
            ledger_add(self._barriers, jobset_id, scope)
            return list(self._activities.get(jobset_id, ()))

    def _barrier_down(self, jobset_id: str, scope: Scope) -> None:
        with self._lock:
            ledger_remove(self._barriers, jobset_id, scope)


class KillScope:
    """kill 1건의 우선권 구간. killer worker 스레드에서:

        acquire() — barrier↑ + 그 시점 활동의 **범위 내 제출** 취소 + 정지 대기
        release() — barrier↓ (kill 완료 후, finally에서)

    barrier가 유지되는 동안 그 범위의 새 submit 등록은 거부되므로 'kill 진행
    중 도착한 제출/재제출은 취소된다'가 타이밍이 아닌 규칙이 된다.

    범위(keys)가 있으면 그 job만 멈추고 나머지 job의 제출은 계속된다 —
    선택 kill이 jobset 제출 전체를 무너뜨리지 않게 하는 지점이다.


    begin()과 acquire()가 나뉘어 있는 이유: barrier↑와 취소는 **논블로킹**
    이라 kill을 접수한 스레드(GUI)에서 곧바로 할 수 있고, 그래야 제출이
    즉시 멎는다. 정지 대기(blocking)만 killer worker로 미룬다 — 취소가
    worker 차례까지 밀리면 그동안 제출된 job이 전부 '제출됐다가 곧 죽는'
    낭비가 된다(대량 제출일수록 크다)."""

    def __init__(self, gate: SubmitGate, jobset_id: str,
                 keys: Optional[Iterable[str]] = None):
        self._gate = gate
        self.jobset_id = jobset_id
        # 범위 객체를 **한 번만** 만들어 계속 들고 다닌다 — barrier 원장은
        # identity로 제거하므로, release에서 새 frozenset을 만들면 자기
        # barrier를 못 내리고 영영 남는다(그 jobset의 제출이 영구 차단).
        self.keys: Scope = None if keys is None else frozenset(keys)
        self._lock = threading.Lock()
        #: begin()이 포착한 활동 — None이면 아직 barrier를 안 올렸다.
        #: 포착 이후 등록되는 활동은 barrier가 거부(또는 범위분만 거부)하므로
        #: 다시 훑을 필요가 없다 — 그것이 게이트의 원자성 계약이다.
        self._acts: Optional[List[_Activity]] = None

    def begin(self) -> None:
        """barrier↑ + 그 시점 활동의 범위 내 제출 즉시 취소 (논블로킹, 멱등).
        kill 접수 스레드에서 곧장 부른다."""
        with self._lock:
            if self._acts is not None:
                return
            self._acts = self._gate._barrier_up(self.jobset_id, self.keys)
            acts = self._acts
        for a in acts:
            a.cancel(self.keys)

    def acquire(self) -> bool:
        """begin(아직이면) + 범위 내 제출이 멎을 때까지 대기 (blocking).
        반환: 시간 내 전부 정지 여부 — False면 caller(killer)가
        KillReport.errors로 보고한다 (스냅샷 이후 제출 완료분 유출 가능)."""
        self.begin()
        ok = True
        for a in self._acts or ():
            ok &= a.wait(self.keys, a.timeout_s)
        return ok

    def release(self) -> None:
        """barrier↓ (멱등 — begin 전이면 올린 것이 없으므로 no-op)."""
        with self._lock:
            if self._acts is None:
                return
            self._acts = None
        self._gate._barrier_down(self.jobset_id, self.keys)
