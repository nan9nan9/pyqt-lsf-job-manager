"""상태 모델 — JobState / JobRecord / JobSetRecord (Qt 비의존 순수 Python).

frozen dataclass는 불변이므로 Qt Signal 인자로 스레드 간 안전하게 전달 가능.
갱신은 dataclasses.replace()로 새 객체를 만들어 Store를 통해서만 수행한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4


class JobState(Enum):
    # --- 내부 상태 (LSF 도달 전 / 추적 불가) ---
    CREATED = "CREATED"
    SUBMITTING = "SUBMITTING"
    RETRY_WAIT = "RETRY_WAIT"        # submit 실패 후 재시도 대기 (n/N회)
    SUBMIT_FAILED = "SUBMIT_FAILED"  # N회 재시도 모두 실패 (최종)
    CANCELLED = "CANCELLED"          # 제출 도중 kill/취소로 중단 (최종)
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

    @property
    def is_inactive(self) -> bool:
        """비활성(제출 전 CREATED 또는 최종) 여부 — submit/편집/remove
        가드의 공통 술어. 활성(SUBMITTING/RETRY_WAIT/on-LSF)이면 False."""
        return self is JobState.CREATED or self in _TERMINAL


_TERMINAL = frozenset({
    JobState.DONE, JobState.EXIT, JobState.SUBMIT_FAILED, JobState.LOST,
    # CANCELLED는 제출이 끝난 terminal 상태이며 재제출할 수 있다.
    JobState.CANCELLED,
})
#: 실패로 분류되는 상태. CANCELLED는 **의도한 중단**이라 여기 없다 —
#: js.jobs_failed/요약의 실패 집계에 취소분이 섞이면 "몇 건이 진짜 실패했나"를
#: 못 읽는다.
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
class JobStatus:
    """LSF **관측값** 1건 — bjobs 1행 또는 콜백 payload 1건의 파싱 결과.

    JobRecord(라이브러리가 소유하는 상태)와 짝이다: 이쪽은 "LSF가 지금
    뭐라고 하는가"이고 저쪽은 "우리가 아는 상태"다. 둘을 합쳐 전이를
    결정하는 것이 monitor.merge_fields다.
    (command.py에 있던 것을 옮겼다 — 조회원 두 곳(bjobs/콜백)이 다 쓰는
     데이터 타입이라 command 소유일 이유가 없었고, 그 배치가
     config→internal_status→command→config 순환을 만들었다.)
    """
    job_id: int
    array_index: Optional[int]
    state: JobState
    exit_code: Optional[int]
    run_time_s: Optional[int] = None       # LSF run_time(초)
    start_time: Optional[datetime] = None  # LSF start_time
    finish_time: Optional[datetime] = None # LSF finish_time
    # 작업 디렉토리는 조회하지 않는다 — JobRecord.submit_cwd(제출 요청값)가
    # 같은 경로를 가리켰고, exec_cwd는 RUN 이후에야 채워지면서 조회 포맷만
    # 무겁게 했다. submit_cwd가 None이면 부모 프로세스 cwd라는 뜻이다.
    source_cluster: Optional[str] = None   # MC: 제출(로컬) 클러스터
    forward_cluster: Optional[str] = None  # MC: 포워딩된 실행(원격) 클러스터


@dataclass(frozen=True)
class JobRecord:
    """job 1개의 추적 레코드. jobset 내에서 job_key가 유일 키."""
    job_id: Optional[int]            # SUBMIT_FAILED 등 미확보 시 None
    array_index: Optional[int]       # array element면 인덱스, 아니면 None
    jobset_id: str
    #: 앱이 지정하는 jobset 내 고유 키. 재제출·교체에도 유지되며 선택·편집의 기준이다.
    #: LSF의 -J 이름으로 전달하지 않는다.
    job_key: str
    state: JobState
    fail_reason: Optional[str] = None    # "NO_JOBID_PARSED"|"BSUB_TIMEOUT"|...
    # SUBMIT_FAILED/RETRY_WAIT의 wrapper stdout·stderr 진단 원문. EXIT 원인은 수집하지 않는다.
    fail_message: Optional[str] = None
    retry_count: int = 0
    exit_code: Optional[int] = None
    #: 이 manager의 bkill 수락 또는 제출 취소 표식. 외부 kill과 자연 종료에는 설정하지 않는다.
    #: 재제출 시 False로 리셋한다.
    killed: bool = False
    submit_time: Optional[datetime] = None
    command: str = ""                # retry 재submit용
    updated_at: Optional[datetime] = None
    # --- 실행 시간 (LSF bjobs 기준) ---
    run_time_s: Optional[int] = None     # LSF run_time(초) — 종료 job은 최종 실행시간
    start_time: Optional[datetime] = None    # LSF start_time (실행 시작)
    finish_time: Optional[datetime] = None   # LSF finish_time (종료)
    # LSF MultiCluster(job forwarding) — collect_clusters=True일 때 폴링이 채운다
    source_cluster: Optional[str] = None     # 제출(로컬) 클러스터
    forward_cluster: Optional[str] = None    # 포워딩된 실행(원격) 클러스터
    # 제출 subprocess의 작업 디렉토리. None이면 부모 cwd를 사용한다.
    # 프로세스 전역 os.chdir 대신 subprocess의 cwd 인자로 전달한다.
    submit_cwd: Optional[str] = None
    # 사용자 정의 JSON 직렬화 가능 dict. 라이브러리는 해석하지 않는다.
    # frozen 레코드 내부를 직접 수정하지 말고 set_user_data로 교체한다.
    user_data: Optional[dict] = None
    # 교체·새 제출마다 바뀌는 실행 식별자. 상태 전이에서는 유지한다.
    # job_key는 재사용되고 job_id는 제출 전에는 없으므로 늦은 신호를 구별할 수 없다.
    _generation: str = field(default_factory=lambda: uuid4().hex,
                             repr=False, compare=False)
    # Store가 같은 key의 쓰기마다 증가시킨다. worker 신호의 역순 도착 판정용.
    _revision: int = field(default=0, repr=False, compare=False)

    def same_execution(self, other: "JobRecord") -> bool:
        """같은 논리 작업의 같은 실행인가. 상태·revision은 실행 중 바뀔 수 있다."""
        return (self.jobset_id == other.jobset_id and self.job_key == other.job_key
                and self._generation == other._generation)


@dataclass(frozen=True)
class JobSetRecord:
    """논리적 job 묶음 — 추적은 JobRecord의 job_id 목록으로만 한다.

    쓰기만 되고 읽히지 않던 사장 필드(부착물/session_id/closed/merged_from/
    description 등)는 v10.1~v10.5에 걸쳐 일괄 삭제됐다 — 근거와 경위는
    git 이력. 되살리지 말 것(종결은 remove_jobset의 실제 삭제, job 추가는
    add_jobs/replace_jobs/upsert_jobs가 대신한다)."""
    jobset_id: str
    intended_count: int                          # 손실 감지 기준
    label: str = ""
    tags: List[str] = field(default_factory=list)   # search_jobsets(tag=) 필터
    created_at: Optional[datetime] = None        # search_jobsets(since=) 필터
