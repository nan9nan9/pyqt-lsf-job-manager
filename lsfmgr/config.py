"""설정 (LsfConfig) 및 job 명세 (JobSpec / ArrayJobSpec) — Qt 비의존."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class LsfConfig:
    """LSF 명령 경로/타임아웃/chunk 등 환경 설정 (NFR-7)."""
    bsub_path: str = "bsub"
    bjobs_path: str = "bjobs"
    bkill_path: str = "bkill"
    bhist_path: str = "bhist"
    bmod_path: str = "bmod"
    bgdel_path: str = "bgdel"

    default_queue: str = ""              # 빈 문자열이면 -q 미지정
    submit_timeout_s: float = 30.0       # FR-2.1
    query_timeout_s: float = 120.0
    kill_timeout_s: float = 120.0

    chunk_size: int = 200                # chunking fallback 시 chunk당 job 수 (100~500)
    arg_max: int = 131072                # 명령줄 인자 총 길이 상한 (NFR-5, 보수적)

    lsf_group_root: str = "/lsfmgr"      # → /lsfmgr/<user>/<jobset_id> (CS-10)
    script_dir: str = ""                 # array dispatch 스크립트 저장 위치
                                         # 빈 문자열이면 ~/.lsfmgr/scripts

    workers: int = 16                    # 병렬 submit worker 수 (1~32)
    max_retry: int = 3                   # submit 재시도 횟수 (FR-2.2)
    retry_delay_s: float = 2.0           # 첫 재시도 대기 (v7 기본 "fixed:2")
    retry_backoff: float = 1.0           # >1.0이면 지수 backoff("expo")
    rate_limit_per_s: Optional[float] = None   # bsub 초당 호출 제한 (NFR-4)

    poll_interval_s: float = 10.0        # FR-4.4 기본 polling 주기

    def __post_init__(self):
        self.workers = max(1, min(32, int(self.workers)))
        if self.chunk_size < 1:
            self.chunk_size = 200

    def resolve_script_dir(self) -> str:
        path = self.script_dir or os.path.join(
            os.path.expanduser("~"), ".lsfmgr", "scripts")
        os.makedirs(path, exist_ok=True)
        return path


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
