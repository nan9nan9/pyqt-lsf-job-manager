"""상태 모델 — JobState / JobRecord / JobSetRecord (Qt 비의존 순수 Python).

frozen dataclass는 불변이므로 Qt Signal 인자로 스레드 간 안전하게 전달 가능 (CS-2).
갱신은 dataclasses.replace()로 새 객체를 만들어 Store를 통해서만 수행한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace  # noqa: F401  (replace는 외부 사용 편의 re-export)
from datetime import datetime
from enum import Enum
from typing import List, Optional


class JobState(Enum):
    # --- 내부 상태 (LSF 도달 전 / 추적 불가) ---
    CREATED = "CREATED"
    SUBMITTING = "SUBMITTING"
    RETRY_WAIT = "RETRY_WAIT"        # submit 실패 후 재시도 대기 (n/N회)
    SUBMIT_FAILED = "SUBMIT_FAILED"  # N회 재시도 모두 실패 (최종)
    LOST = "LOST"                    # ID 미확보/조회 불가 (최종)

    # --- LSF native 상태 ---
    PEND = "PEND"
    RUN = "RUN"
    DONE = "DONE"    # exit 0
    EXIT = "EXIT"    # exit != 0
    PSUSP = "PSUSP"
    USUSP = "USUSP"
    SSUSP = "SSUSP"
    UNKWN = "UNKWN"
    ZOMBI = "ZOMBI"

    @property
    def is_terminal(self) -> bool:
        """최종 상태 여부 — 더 이상 전이하지 않음."""
        return self in _TERMINAL

    @property
    def is_failed(self) -> bool:
        """실패로 분류되는 상태 여부."""
        return self in _FAILED

    @property
    def is_on_lsf(self) -> bool:
        """bjobs 조회 대상 여부 — LSF에 존재(했)다고 간주되는 상태."""
        return self in _ON_LSF


_TERMINAL = frozenset({
    JobState.DONE, JobState.EXIT, JobState.SUBMIT_FAILED, JobState.LOST,
})
_FAILED = frozenset({
    JobState.EXIT, JobState.SUBMIT_FAILED, JobState.LOST,
})
_ON_LSF = frozenset({
    JobState.PEND, JobState.RUN, JobState.PSUSP, JobState.USUSP,
    JobState.SSUSP, JobState.UNKWN, JobState.ZOMBI,
})

# LSF 문자열 상태 → JobState 매핑 (bjobs 출력 파싱용)
LSF_STAT_MAP = {s.value: s for s in _ON_LSF}
LSF_STAT_MAP["DONE"] = JobState.DONE
LSF_STAT_MAP["EXIT"] = JobState.EXIT


@dataclass(frozen=True)
class JobRecord:
    """job 1개의 추적 레코드. jobset 내에서 lsf_job_name이 유일 키."""
    job_id: Optional[int]            # SUBMIT_FAILED 등 미확보 시 None
    array_index: Optional[int]       # array element면 인덱스, 아니면 None
    jobset_id: str
    lsf_job_name: str                # "<jobset_id>_<idx>" 또는 "<jobset_id>[<idx>]"
    state: JobState
    fail_reason: Optional[str] = None    # "NO_JOBID_PARSED"|"BSUB_TIMEOUT"|...
    retry_count: int = 0
    exit_code: Optional[int] = None
    submit_time: Optional[datetime] = None
    command: str = ""                # retry 재submit용
    updated_at: Optional[datetime] = None
    # --- 실행 시간/위치 (LSF bjobs 기준) ---
    run_time_s: Optional[int] = None     # LSF run_time(초) — 종료 job은 최종 실행시간
    start_time: Optional[datetime] = None    # LSF start_time (실행 시작)
    finish_time: Optional[datetime] = None   # LSF finish_time (종료)
    working_dir: Optional[str] = None    # LSF exec_cwd (실제 실행 디렉토리)
    # 제출 경로 — wrapper(커맨드 그대로 실행) vs bsub(lsfmgr 인자 조립).
    # job 단위 속성이다: merge로 wrapper/bsub jobset이 섞여도 재제출 경로를
    # 레코드만 보고 정확히 고를 수 있어야 한다 (resubmit_jobs)
    via_wrapper: bool = False
    # bsub 경로의 제출 옵션 스냅샷(JobSpec 직렬화 JSON) — resubmit_jobs가
    # queue/resources/outfile/env 를 원본 그대로 복원하는 근거.
    # command 만 다시 만들면 이 옵션들이 조용히 기본값으로 소실된다
    spec_json: Optional[str] = None

    @property
    def job_key(self) -> str:
        """Store 내 job 식별 키."""
        return self.lsf_job_name


@dataclass(frozen=True)
class JobSetRecord:
    """논리적 job 묶음. LSF 부착물(group/name/array)은 실행 수단일 뿐이며
    전부 유실돼도 JobRecord의 job_id 목록만으로 동작한다 (graceful degradation)."""
    jobset_id: str
    intended_count: int                          # 손실 감지 기준
    lsf_group_paths: List[str] = field(default_factory=list)
    name_patterns: List[str] = field(default_factory=list)
    array_job_ids: List[int] = field(default_factory=list)
    label: str = ""
    tags: List[str] = field(default_factory=list)
    description: str = ""
    parent_jobset_id: Optional[str] = None
    created_by: str = ""
    created_at: Optional[datetime] = None
    merged_from: List[str] = field(default_factory=list)
    session_id: str = ""
    closed: bool = False
