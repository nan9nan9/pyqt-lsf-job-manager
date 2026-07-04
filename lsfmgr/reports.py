"""결과 리포트 (frozen dataclass — Signal 인자로 안전, CS-2)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class SubmitReport:
    """submit_finished Signal로 전달되는 최종 결과 (retry 포함)."""
    jobset_id: str
    total: int
    succeeded: int
    failed: int              # SUBMIT_FAILED로 최종 확정된 수
    cancelled: int           # cancel로 submit 자체를 안 한 수
    retried: int             # 재시도가 1회 이상 발생한 job 수
    duration_s: float
    fail_reasons: Dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> int:
        """succeeded 별칭 — README 표기(rpt.ok)와 일치."""
        return self.succeeded


@dataclass(frozen=True)
class KillReport:
    """kill_finished Signal로 전달되는 결과."""
    jobset_id: str
    requested: int                       # kill 대상 job 수
    strategies: List[str] = field(default_factory=list)   # 사용된 전략 순서
    command_calls: int = 0               # 실제 LSF 호출 횟수
    still_alive: Optional[int] = None    # verify=True일 때 재조회 후 잔존 수
    errors: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReconcileReport:
    """recover 후 저장 상태 vs LSF 실상태 대조 결과 (Sqlite 전용)."""
    jobset_id: str
    checked: int             # 조회 대상(is_on_lsf) job 수
    transitioned: int        # 상태가 갱신된 job 수
    lost: int                # LOST로 전이된 job 수
    summary: Dict[str, int] = field(default_factory=dict)
