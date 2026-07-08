"""job 상태 상수와 Job 데이터 모델."""

from dataclasses import dataclass
from typing import Optional


# LSF job 상태값. bjobs STAT 컬럼에 그대로 노출된다.
PEND = "PEND"    # 스케줄 대기
RUN = "RUN"      # 실행 중
DONE = "DONE"    # 정상 종료 (exit code 0)
EXIT = "EXIT"    # 비정상 종료 (exit code != 0)
PSUSP = "PSUSP"  # PEND 상태에서 사용자가 suspend
USUSP = "USUSP"  # RUN 상태에서 사용자가 suspend (bstop)
SSUSP = "SSUSP"  # 시스템에 의해 suspend

# 더 이상 상태가 변하지 않는 종료 상태.
FINISHED_STATES = {DONE, EXIT}
# 실행 슬롯을 점유하는 상태 (suspend 되어도 슬롯은 잡고 있다).
ACTIVE_STATES = {RUN, SSUSP, USUSP}


@dataclass
class Job:
    """단일 job (array 의 경우 element 하나)에 대응.

    DB 의 한 row 와 1:1 로 매핑된다.
    """

    job_id: int                       # LSF job 번호 (array element 는 부모와 공유)
    user: str
    command: str
    queue: str
    from_host: str
    job_name: str
    submit_time: float                # epoch
    stat: str = PEND

    array_index: Optional[int] = None  # array element 번호 (일반 job 은 None)
    array_size: int = 0                # array 전체 element 수 (0 이면 일반 job)
    array_limit: int = 0              # %limit (동시 실행 제한, 0 이면 무제한)

    exec_host: Optional[str] = None
    start_time: Optional[float] = None
    finish_time: Optional[float] = None

    # 스케줄러가 참조하는 계획값.
    pend_secs: float = 0.0            # 최소 PEND 유지 시간
    run_secs: float = 0.0             # 계획된 실행 시간
    planned_outcome: str = DONE      # 종료 예정 상태 (DONE/EXIT)
    exit_code: int = 0

    # suspend 스케줄 (RUN 중 SSUSP 흉내). 0 이면 없음.
    suspend_at: float = 0.0          # start_time 기준 오프셋 초
    suspend_secs: float = 0.0
    # 사용자 suspend(bstop) 가 시작된 시각. 0 이면 suspend 아님.
    # resume 시 이 값으로 경과분만큼 finish/pend 시간을 밀어준다.
    susp_since: float = 0.0

    num_cpus: int = 1
    requested_hosts: str = ""        # -m 로 지정된 호스트 (공백 구분)
    proj: str = "default"
    job_group: str = ""              # bsub -g 로 지정된 job group 경로 (없으면 "")
    cwd: str = ""                    # bsub 실행 디렉토리 (exec_cwd 로 노출)
    # MultiCluster: 제출(로컬) 클러스터 / forward 된 실행(원격) 클러스터.
    # forward_cluster 가 "" 이면 로컬 실행(포워딩 안 됨).
    source_cluster: str = ""
    forward_cluster: str = ""
    row_id: int = 0                  # DB rowid (내부용)

    @property
    def display_id(self) -> str:
        """bjobs 등에 표시되는 job id 문자열. array 는 123[4] 형태."""
        if self.array_index is not None:
            return f"{self.job_id}[{self.array_index}]"
        return str(self.job_id)

    @property
    def is_array(self) -> bool:
        return self.array_index is not None
