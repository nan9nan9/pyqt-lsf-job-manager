"""설정 (LsfConfig) 및 job 명세 (JobSpec / ArrayJobSpec) — Qt 비의존."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import List, Optional, Sequence, Tuple, Union

#: LSF 명령 경로. 단일 프로그램은 str, bsub를 호출하는 wrapper처럼 고정 인자가
#: 붙는 명령은 토큰 목록으로 지정한다 (예: ["primesim_sub", "--proj", "X"]).
#: wrapper는 표준 bsub 옵션(-q/-J/-g/...)을 받아 bsub로 넘기고, bsub의 출력
#: ("Job <id> ...")을 그대로 뱉으면 된다 — 파싱/추적은 bsub와 동일하다.
CmdPath = Union[str, Sequence[str]]


@dataclass
class LsfConfig:
    """LSF 명령 경로/타임아웃/chunk 등 환경 설정 (NFR-7)."""
    bsub_path: CmdPath = "bsub"       # wrapper 지원: 토큰 목록도 허용
    bjobs_path: CmdPath = "bjobs"
    bkill_path: CmdPath = "bkill"
    bhist_path: CmdPath = "bhist"
    bmod_path: CmdPath = "bmod"
    bgdel_path: CmdPath = "bgdel"

    default_queue: str = ""              # 빈 문자열이면 -q 미지정
    submit_timeout_s: float = 30.0       # FR-2.1
    query_timeout_s: float = 120.0
    kill_timeout_s: float = 120.0

    chunk_size: int = 200                # chunking fallback 시 chunk당 job 수 (100~500)
    arg_max: int = 131072                # 명령줄 인자 총 길이 상한 (NFR-5, 보수적)

    lsf_group_root: str = "/lsfmgr"      # → /lsfmgr/<user>/<jobset_id> (CS-10)
    script_dir: str = ""                 # array dispatch 스크립트 저장 위치
                                         # 빈 문자열이면 ~/.lsfmgr/scripts

    workers: int = 16                    # 병렬 submit worker 수 (1~64)
                                         # 상한↑ 시 submit 호스트 CPU/RAM·master
                                         # 부하 주의 — rate_limit_per_s와 병행
    max_retry: int = 3                   # submit 재시도 횟수 (FR-2.2)
    retry_delay_s: float = 2.0           # 첫 재시도 대기 (v7 기본 "fixed:2")
    retry_backoff: float = 1.0           # >1.0이면 지수 backoff("expo")
    rate_limit_per_s: Optional[float] = None   # bsub 초당 호출 제한 (NFR-4)

    kill_max_retry: int = 2              # kill 확인 실패 시 재시도 (FR-3.4)
    kill_retry_delay_s: float = 3.0      # kill 재시도 간격 — bkill은 비동기라
                                         # 확인('is being terminated')까지 여유
    #: kill 상태 정책 (FR-3.5)
    #: "optimistic" — bkill 'is being terminated' 확인 시 즉시 EXIT로 간주(기본).
    #                 bkill이 비동기라 실제 종료 전이지만, kill 의도가 수락됐으니
    #                 EXIT로 낙관 표시하고 폴링은 이 job을 더 조회하지 않는다.
    #: "actual"     — terminated 확인만으론 상태를 안 바꾸고, 실제 LSF 상태
    #                 (bjobs verify/폴링)로만 EXIT를 반영한다.
    kill_status_policy: str = "optimistic"

    poll_interval_s: float = 10.0        # FR-4.4 기본 polling 주기

    def __post_init__(self):
        self.workers = max(1, min(64, int(self.workers)))
        if self.chunk_size < 1:
            self.chunk_size = 200
        if self.kill_status_policy not in ("optimistic", "actual"):
            raise ValueError(
                "kill_status_policy는 'optimistic' 또는 'actual' "
                f"(got {self.kill_status_policy!r})")

    def resolve_script_dir(self) -> str:
        path = self.script_dir or os.path.join(
            os.path.expanduser("~"), ".lsfmgr", "scripts")
        os.makedirs(path, exist_ok=True)
        return path


def cmd_tokens(path: CmdPath) -> List[str]:
    """CmdPath를 argv 앞부분 토큰 목록으로 정규화. str이면 프로그램 1개."""
    return [path] if isinstance(path, str) else list(path)


@dataclass(frozen=True)
class JobSpec:
    """개별 job submit 명세 (FR-1.5 옵션 템플릿)."""
    command: str
    queue: Optional[str] = None
    resources: Optional[str] = None          # bsub -R
    outfile: Optional[str] = None            # bsub -o
    errfile: Optional[str] = None            # bsub -e
    env: Optional[Tuple[Tuple[str, str], ...]] = None   # 추가 환경변수 (불변 tuple)
    extra_args: Tuple[str, ...] = ()         # 기타 bsub 인자


def spec_to_json(spec: JobSpec) -> str:
    """JobSpec → JSON — JobRecord.spec_json 저장용 (resubmit 옵션 보존)."""
    return json.dumps(asdict(spec), ensure_ascii=False)


def spec_from_json(s: str) -> JobSpec:
    """JobRecord.spec_json → JobSpec 복원 (JSON list → 불변 tuple 정규화)."""
    d = json.loads(s)
    if d.get("env") is not None:
        d["env"] = tuple((str(k), str(v)) for k, v in d["env"])
    d["extra_args"] = tuple(d.get("extra_args") or ())
    return JobSpec(**d)


@dataclass(frozen=True)
class ArrayJobSpec:
    """Array job submit 명세 (FR-1.3).

    - command 단일 + count: 동일 command, $LSB_JOBINDEX 활용
    - commands 리스트: element별 command 상이 → dispatch 스크립트 자동 생성
    """
    command: Optional[str] = None
    commands: Optional[Tuple[str, ...]] = None
    count: Optional[int] = None
    queue: Optional[str] = None
    resources: Optional[str] = None
    outfile: Optional[str] = None
    errfile: Optional[str] = None
    env: Optional[Tuple[Tuple[str, str], ...]] = None
    extra_args: Tuple[str, ...] = ()

    def __post_init__(self):
        if self.commands is not None and not isinstance(self.commands, tuple):
            object.__setattr__(self, "commands", tuple(self.commands))

    @property
    def size(self) -> int:
        if self.commands is not None:
            return len(self.commands)
        if self.count is None:
            raise ValueError("ArrayJobSpec: count 또는 commands 중 하나는 필수")
        return int(self.count)
