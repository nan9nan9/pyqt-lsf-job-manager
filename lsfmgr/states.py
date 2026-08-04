"""상태 모델 — JobState / JobRecord / JobSetRecord (Qt 비의존 순수 Python).

frozen dataclass는 불변이므로 Qt Signal 인자로 스레드 간 안전하게 전달 가능.
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

    @property
    def is_inactive(self) -> bool:
        """비활성(제출 전 CREATED 또는 최종) 여부 — submit/merge/remove
        가드의 공통 술어. 활성(SUBMITTING/RETRY_WAIT/on-LSF)이면 False."""
        return self is JobState.CREATED or self in _TERMINAL


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
    """job 1개의 추적 레코드. jobset 내에서 job_key가 유일 키."""
    job_id: Optional[int]            # SUBMIT_FAILED 등 미확보 시 None
    array_index: Optional[int]       # array element면 인덱스, 아니면 None
    jobset_id: str
    #: Store 내 물리 키 — "<jobset_id>_<idx>". (v10.1: 구명 lsf_job_name에서
    #: 개명 — v10부터 LSF에 -J로 부착되지 않는 순수 내부 키라 옛 이름이
    #: 오해를 유발했다. 구명 별칭 없음 — 호출부는 job_key만 쓴다.)
    job_key: str
    state: JobState
    fail_reason: Optional[str] = None    # "NO_JOBID_PARSED"|"BSUB_TIMEOUT"|...
    # 실패 진단 원문 — UI가 "왜 실패했나"를 그대로 보여주는 용도.
    # SUBMIT_FAILED/RETRY_WAIT에서 bsub/wrapper 실행의 stderr/stdout(터미널
    # 메시지)이 저장된다. EXIT의 원인은 저장하지 않는다 — 필요 시점에
    # (v10.3: bhist 원문 온디맨드 조회는 삭제 — 상세는 이 레코드 필드로)
    fail_message: Optional[str] = None
    retry_count: int = 0
    exit_code: Optional[int] = None
    #: 이 매니저의 kill 요청으로 종료된 job인지 — mgr.kill()/kill_jobs()가
    #: bkill 수용을 확인한 대상에 표시한다. 자연 종료·외부 bkill(관리자/다른
    #: 세션)·비정상 EXIT은 이 경로를 안 타므로 False로 남는다. "EXIT인데 내가
    #: 죽인 게 아니다"를 가르는 **유일한 근거** — exit_code(130/137/143)는
    #: 외부 kill과 구분되지 않는다. 재제출 리셋에서 False로 되돌아간다.
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
    # 제출 시 subprocess를 실행할 작업 디렉토리(create_jobset의 work_dir(s)
    # 요청값). None이면 부모(GUI) 프로세스의 cwd에서 실행(스레드 안전 —
    # os.chdir 같은 프로세스 전역 변경 금지). job 단위 속성이라 merge/재제출
    # 에도 보존된다.
    # v10.4: 관측값 working_dir(bjobs exec_cwd) 삭제 — RUN 이후에야 채워지는
    # 데다 이 값과 사실상 같은 경로를 가리켜 헷갈리기만 했다. 작업 디렉토리는
    # 이 필드 하나로 본다. exec_cwd 조회를 되살리지 말 것.
    submit_cwd: Optional[str] = None
    # --- 논리 정체성/사용자 데이터 (GUI 직접 제어용, v9) ---
    # merge_id: job의 논리 키 — merge 시 같은 merge_id의 기존 job을 이
    # 레코드 내용으로 replace한다(물리 키 job_key는 유지 → 테이블 행 연속).
    # None이면 merge에서 항상 신규 추가. jobset 내 유일해야 한다(None 제외).
    merge_id: Optional[str] = None
    # user_data: 사용자 정의 데이터(dict, JSON 직렬화 가능해야 함) — 실제
    # run command 등 GUI가 임의 정보를 싣는 용도. 라이브러리는 해석하지
    # 않고 보존만 한다. frozen 레코드 안의 dict이므로 내용을 제자리에서
    # 고치지 말고 set_user_data로 교체할 것.
    user_data: Optional[dict] = None


@dataclass(frozen=True)
class JobSetRecord:
    """논리적 job 묶음 — 추적은 JobRecord의 job_id 목록으로만 한다.
    (v10.1: 사장 필드 일괄 삭제 — 부착물(lsf_group_paths/name_patterns/
    array_job_ids)은 v10부터 생성 자체가 없고, session_id(세션복원)/
    parent_jobset_id/created_by는 쓰기만 되고 어디서도 읽지 않았다.
    영속 저장소가 v9에서 제거돼 과거 데이터 호환 부담도 없다.

    closed 플래그도 삭제됐다 — 종결은 mgr.remove_jobset()이 레코드를 실제로
    지우므로 '닫혔지만 목록에 남아있는' 중간 상태가 아예 없다.
    merged_from도 삭제됐다 — merge API가 사라지면서(job 추가는 add_jobs/
    replace_jobs/upsert_jobs로 직접) 기록할 대상 자체가 없어졌다.)"""
    jobset_id: str
    intended_count: int                          # 손실 감지 기준
    label: str = ""
    tags: List[str] = field(default_factory=list)   # search_jobsets(tag=) 필터
    description: str = ""
    created_at: Optional[datetime] = None        # search_jobsets(since=) 필터
