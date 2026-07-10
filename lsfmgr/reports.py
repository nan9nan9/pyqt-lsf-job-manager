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
class SubmitProgress:
    """진행 중 submit/resubmit의 실시간 스냅샷 — 아무 때나 조회 가능(pull).

    submit_progress Signal(push)의 조회 버전이다. 대량 제출을 백그라운드로
    돌려놓고 진행 dialog를 닫은 뒤(딴 작업), 나중에 상태 패널을 다시 열어
    현재 진행을 그릴 때 쓴다. 제출이 끝나면 스냅샷은 None이 되고(핸들의
    submit_state가 None 반환) 최종 결과는 summary / SubmitReport로 본다.
    """
    jobset_id: str
    done: int                # 처리 완료 단위 수 (성공+실패+취소)
    total: int               # 전체 단위 수
    succeeded: int
    failed: int
    cancelled: int

    @property
    def remaining(self) -> int:
        return max(0, self.total - self.done)

    @property
    def fraction(self) -> float:
        """0.0~1.0 진행률 (total=0이면 1.0)."""
        return (self.done / self.total) if self.total else 1.0


@dataclass(frozen=True)
class KillProgress:
    """진행 중 kill의 실시간 스냅샷 — 아무 때나 조회 가능(pull).

    kill_progress Signal(push)의 조회 버전. 대량 chunked kill(특히 MC envpath는
    chunk마다 env source, verify는 재조회 루프)을 백그라운드로 돌려놓고 진행
    dialog를 닫은 뒤 나중에 다시 그릴 때 쓴다. 끝나면 None(핸들 kill_state가
    None), 최종 결과는 kill_finished(KillReport)로 본다.
    부착물 전략(group/‏name 1명령)만으로 끝나는 kill은 증분 진행이 없어
    done/total이 0으로 머물다 완료 시 채워질 수 있다(거의 즉시 끝나는 경로)."""
    jobset_id: str
    done: int                # 처리한 target 수(chunk 누적)
    total: int               # kill 대상 target 수

    @property
    def remaining(self) -> int:
        return max(0, self.total - self.done)

    @property
    def fraction(self) -> float:
        """0.0~1.0 진행률 (total=0이면 0.0 — 아직 증분 단계 전/부착물 전략)."""
        return (self.done / self.total) if self.total else 0.0


@dataclass(frozen=True)
class KillReport:
    """kill_finished Signal로 전달되는 결과."""
    jobset_id: str
    requested: int                       # kill 대상 job 수
    strategies: List[str] = field(default_factory=list)   # 사용된 전략 순서
    command_calls: int = 0               # 실제 LSF 호출 횟수
    still_alive: Optional[int] = None    # verify=True일 때 재조회 후 잔존 수
    unconfirmed: int = 0                 # 재시도 후에도 kill 확인 못 한 수 (FR-3.4)
    kill_retries: int = 0                # kill 재시도 라운드 수
    changed: List = field(default_factory=list)   # optimistic 정책에서 EXIT로
                                         # 전이된 JobRecord (FR-3.5)
    errors: List[str] = field(default_factory=list)


