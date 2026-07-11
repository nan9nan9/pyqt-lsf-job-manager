"""JobSetStore 추상 인터페이스 (Qt 비의존 순수 Python).

공통 API(§4.2)는 두 백엔드가 동일 계약으로 구현하고,
모든 public 메서드는 thread-safe여야 한다 (CS-1).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple,
)

from ..errors import JobNotFoundError
from ..states import JobRecord, JobSetRecord, JobState

#: transition_many 입력 1건 — (job_key, new_state, guard, fields)
TransitionSpec = Tuple[
    str, JobState, Optional[Callable[[JobRecord], bool]], Dict[str, Any]]


class JobSetStore(ABC):
    """JobSet/JobRecord 저장소 계약."""


    # ------------------------------------------------------------------
    # JobSet CRUD
    # ------------------------------------------------------------------
    @abstractmethod
    def insert_jobset(self, record: JobSetRecord) -> JobSetRecord: ...

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
    def delete_job(self, jobset_id: str, job_key: str) -> JobRecord:
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

    def find_jobs(self, job_ids: Set[int]) -> List[JobRecord]:
        """job_id 집합에 해당하는 레코드를 jobset 무관 전역 검색 (kill_jobs
        optimistic 전이용 — 어느 jobset 소속인지 모를 때). 기본 구현은
        jobset 순회이고, 백엔드가 최적화(WHERE IN)해도 된다."""
        if not job_ids:
            return []
        out: List[JobRecord] = []
        for js in self.list_jobsets():
            out.extend(r for r in self.get_jobs(js.jobset_id)
                       if r.job_id in job_ids)
        return out

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
"""

    def transition_many(self, jobset_id: str,
                        specs: "Sequence[TransitionSpec]") -> List[JobRecord]:
        """여러 job을 한 번에 전이 — 대량 폴링 갱신의 트랜잭션 비용 절감용.

        specs: [(job_key, new_state, guard, fields_dict), ...].
        반환: 실제로 전이된 레코드 목록(guard 거부·키 소실분은 제외, 입력 순서).
        구현은 일괄 처리로 건당 오버헤드를 없앤다 — 수만 건
        전이가 한 사이클에 몰릴 때 폴링 스레드 블로킹/WAL 락 독점을 막는다.
        기본 구현은 건당 transition (계약 유지)."""
        out: List[JobRecord] = []
        for job_key, new_state, guard, fields in specs:
            try:
                rec = self.transition(jobset_id, job_key, new_state,
                                      guard=guard, **fields)
            except JobNotFoundError:
                continue                         # 사이클 도중 remove_job 등
            if rec is not None:
                out.append(rec)
        return out

    @staticmethod
    def _reject_key_fields(fields: Dict[str, Any]) -> None:
        """transition의 키 필드 변경 거부 — 허용하면 옛 키의 레코드가
        잔존해 키-레코드 불일치가 생긴다."""
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
    def dispose(self) -> None:
        """저장소 자원 해제 (connection close 등). 기본은 no-op."""


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
