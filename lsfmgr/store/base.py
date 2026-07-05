"""JobSetStore 추상 인터페이스 (Qt 비의존 순수 Python).

공통 API(§4.2)는 두 백엔드가 동일 계약으로 구현하고,
Sqlite 전용 API(§4.3)는 base에서 PersistenceNotSupportedError를 기본 제공한다.
모든 public 메서드는 thread-safe여야 한다 (CS-1).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any, Callable, Dict, Iterable, List, Optional, Sequence, Set,
)

from ..errors import PersistenceNotSupportedError
from ..states import JobRecord, JobSetRecord, JobState


class JobSetStore(ABC):
    """JobSet/JobRecord 저장소 계약."""

    #: SqliteStore=True — GUI에서 복원 메뉴 활성/비활성 분기용
    persistent: bool = False

    # ------------------------------------------------------------------
    # JobSet CRUD
    # ------------------------------------------------------------------
    @abstractmethod
    def create_jobset(self, record: JobSetRecord) -> JobSetRecord: ...

    @abstractmethod
    def get_jobset(self, jobset_id: str) -> JobSetRecord:
        """없으면 JobSetNotFoundError."""

    @abstractmethod
    def update_jobset(self, record: JobSetRecord) -> JobSetRecord: ...

    @abstractmethod
    def delete_jobset(self, jobset_id: str) -> None:
        """jobset과 소속 job 전부 삭제."""

    @abstractmethod
    def list_jobsets(self) -> List[JobSetRecord]:
        """현재 세션 범위의 jobset 목록."""

    # ------------------------------------------------------------------
    # JobRecord
    # ------------------------------------------------------------------
    @abstractmethod
    def add_job(self, record: JobRecord) -> JobRecord: ...

    def add_jobs(self, records: Sequence[JobRecord]) -> List[JobRecord]:
        """여러 JobRecord 일괄 추가 — 대량 submit의 CREATED 선생성용.
        백엔드는 단일 lock/트랜잭션으로 최적화할 것 (기본: 순차 add_job)."""
        return [self.add_job(r) for r in records]

    @abstractmethod
    def remove_job(self, jobset_id: str, job_key: str) -> JobRecord:
        """job 1건을 저장소에서 제거하고 제거된 레코드를 반환.
        없으면 JobNotFoundError. LSF의 실제 job은 건드리지 않는다 —
        저장소 추적에서만 제외한다(필요하면 호출 전에 kill할 것)."""

    @abstractmethod
    def update_job(self, record: JobRecord) -> JobRecord: ...

    @abstractmethod
    def get_job(self, jobset_id: str, job_key: str) -> JobRecord:
        """없으면 JobNotFoundError."""

    @abstractmethod
    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]: ...

    @abstractmethod
    def transition(self, jobset_id: str, job_key: str, new_state: JobState,
                   guard: Optional[Callable[[JobRecord], bool]] = None,
                   **fields: Any) -> Optional[JobRecord]:
        """원자적 상태 전이 (read-modify-write, CS-1).
        fields로 job_id/exit_code/fail_reason 등 동시 갱신.
        키 필드(lsf_job_name/jobset_id)는 변경 불가 — ValueError.
        guard가 주어지면 lock 안에서 현재 레코드로 평가해 False면 전이를
        건너뛰고 None 반환 (CAS) — 스냅샷 기반 갱신(polling)이 그 사이
        바뀐 레코드(재제출 등)를 덮어쓰는 것을 막는다.
        Sqlite 모드에서는 전이 이력 event를 기록한다 (§2.2)."""

    @staticmethod
    def _reject_key_fields(fields: Dict[str, Any]) -> None:
        """transition의 키 필드 변경 거부 — 허용하면 옛 키의 레코드가
        잔존해 한 job이 이중 계상되거나(sqlite) 키-레코드 불일치(memory)."""
        for key in ("lsf_job_name", "jobset_id"):
            if key in fields:
                raise ValueError(
                    f"transition으로 키 필드({key})는 변경할 수 없습니다")

    # ------------------------------------------------------------------
    # 조회/검색
    # ------------------------------------------------------------------
    @abstractmethod
    def summary(self, jobset_id: str) -> Dict[str, Any]:
        """상태별 카운트. 불변식: 상태 합계 == intended_count (FR-5.2).
        반환 예: {"total": 5000, "RUN": 2100, "PEND": 2800, ...}"""

    @abstractmethod
    def search(self, *, tag: Optional[str] = None, label: Optional[str] = None,
               since: Optional[datetime] = None) -> List[JobSetRecord]:
        """세션 범위 검색 (FR-5.6)."""

    # ------------------------------------------------------------------
    # 수명
    # ------------------------------------------------------------------
    def close(self) -> None:
        """저장소 정리 (connection close 등). 기본은 no-op."""

    # ------------------------------------------------------------------
    # Sqlite 전용 API (§4.3) — InMemoryStore에서는 예외
    # ------------------------------------------------------------------
    def _not_persistent(self) -> PersistenceNotSupportedError:
        return PersistenceNotSupportedError(
            "이 기능은 SqliteStore(persistent=True)에서만 지원됩니다")

    def list_orphan_jobsets(self) -> List[JobSetRecord]:
        raise self._not_persistent()

    def recover_jobset(self, jobset_id: str) -> JobSetRecord:
        raise self._not_persistent()

    def search_all_sessions(self, *, tag: Optional[str] = None,
                            label: Optional[str] = None,
                            since: Optional[datetime] = None
                            ) -> List[JobSetRecord]:
        raise self._not_persistent()

    def get_history(self, jobset_id: str) -> List[Dict[str, Any]]:
        raise self._not_persistent()

    def stats(self, since: Optional[datetime] = None,
              until: Optional[datetime] = None) -> Dict[str, Any]:
        raise self._not_persistent()

    def archive(self, older_than_days: int = 30) -> int:
        raise self._not_persistent()

    def vacuum(self) -> None:
        raise self._not_persistent()

    def export_jobset(self, jobset_id: str, path: str) -> None:
        raise self._not_persistent()


def make_summary(jobset: JobSetRecord,
                 jobs: Iterable[JobRecord]) -> Dict[str, Any]:
    """공통 요약 생성 — total은 intended_count (불변식 FR-5.2)."""
    counts: Dict[str, int] = {}
    n = 0
    for rec in jobs:
        counts[rec.state.value] = counts.get(rec.state.value, 0) + 1
        n += 1
    out: Dict[str, Any] = {"total": jobset.intended_count}
    # 아직 레코드가 생성되지 않은 몫은 CREATED로 계상 → 합계 == intended_count
    missing = jobset.intended_count - n
    if missing > 0:
        counts[JobState.CREATED.value] = (
            counts.get(JobState.CREATED.value, 0) + missing)
    out.update(counts)
    return out
