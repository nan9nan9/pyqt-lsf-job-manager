"""설정 (LsfConfig) 및 job 명세 (JobSpec) — Qt 비의존."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import List, Optional, Sequence, Tuple, Union

#: LSF 명령 경로. 단일 프로그램은 str, bsub를 호출하는 wrapper처럼 고정 인자가
#: 붙는 명령은 토큰 목록으로 지정한다 (예: ["customwrapper_sub", "--proj", "X"]).
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

    workers: int = 32                    # 병렬 submit worker 수 (1~64)
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

    #: LSF MultiCluster(job forwarding) 정보 수집 — bjobs -o 에 source_cluster·
    #: forward_cluster 필드를 추가해 JobRecord.source_cluster/forward_cluster 로
    #: 채운다. MC 환경에서만 켠다(기본 꺼짐) — 미지원 LSF면 그 필드만 자동
    #: 강등(FULL+cluster → FULL)돼 run_time 등 다른 확장 필드는 유지된다.
    collect_clusters: bool = False

    #: RUN 중 run_time_s(경과 실행시간) 변화도 폴링 갱신·jobs_updated 발행 대상에
    #: 포함할지. True면 UI가 매 폴링마다 살아있는 job의 runtime을 갱신받는다.
    #: 대신 RUN job 전원이 매 폴링 재전이돼(수만 개 규모에선 폴링 부하↑) 부담되면
    #: False로 끈다 — 그때 run_time_s는 상태 전이(RUN→DONE 등) 시점에만 반영된다.
    poll_runtime_updates: bool = True

    #: pre_submit 게이트가 False를 반환(제출 거부)했을 때 submit_finished를
    #: 발화할지. True(기본)면 게이트 거부도 submit_finished(cancelled=N)로
    #: 마무리해 기존 완료 핸들러 하나로 다 받는다. False면 발화하지 않고
    #: 종료 통지는 ready_finished(False)만으로 한다. (게이트 예외는 이 옵션과
    #: 무관하게 항상 error_occurred + submit_finished(failed=N)로 보고한다)
    submit_finished_on_gate_reject: bool = True

    #: progress/jobs_updated 발화 빈도 제한 (QT-5) — 이 간격 경과 OR 이 비율만큼
    #: 진행했을 때만 발화(배치). 값이 클수록 시그널이 성겨져 부하↓·반응성↓.
    #: submit progress·jobs_updated 점진 발행·kill progress에 공통 적용.
    progress_min_interval_s: float = 0.5   # 최소 발화 간격(초), 0이면 시간 제한 없음
    progress_min_step_ratio: float = 0.01  # 최소 진행 비율(0~1), 0이면 매번

    def __post_init__(self):
        self.workers = max(1, min(64, int(self.workers)))
        if self.chunk_size < 1:
            self.chunk_size = 200
        # retry_backoff는 여기선 숫자다(>1.0이면 지수 backoff). 같은 이름의
        # submit()/LsfJobManager() kwarg는 'fixed:N'/'expo:N' 문자열이라 헷갈려
        # LsfConfig에 문자열을 넘기면, 예전엔 조용히 통과하다 manager 생성 시
        # str<=float 크래시가 났다 — 이른 시점에 명확한 에러로 잡는다.
        try:
            self.retry_backoff = float(self.retry_backoff)
        except (TypeError, ValueError):
            raise ValueError(
                f"LsfConfig.retry_backoff는 숫자여야 합니다 "
                f"(>1.0이면 지수 backoff) — got {self.retry_backoff!r}. "
                f"'fixed:N'/'expo:N' 문자열 형식은 submit()/LsfJobManager() "
                f"kwargs 전용입니다 (예: LsfJobManager(retry_backoff='fixed:2'))"
            ) from None
        if self.kill_status_policy not in ("optimistic", "actual"):
            raise ValueError(
                "kill_status_policy는 'optimistic' 또는 'actual' "
                f"(got {self.kill_status_policy!r})")
        if self.progress_min_interval_s < 0:
            raise ValueError("progress_min_interval_s는 0 이상")
        if not (0.0 <= self.progress_min_step_ratio <= 1.0):
            raise ValueError("progress_min_step_ratio는 0~1")

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


