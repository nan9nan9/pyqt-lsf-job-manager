"""설정 (LsfConfig) — Qt 비의존."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Callable, List, Optional, Sequence, Tuple, Union


#: LSF 명령은 실행 파일 문자열 또는 고정 인자를 포함한 토큰 목록으로 지정한다.
CmdPath = Union[str, Sequence[str]]

#: 인자 없이 REST 응답 dict 또는 job dict 목록을 반환한다.
#: 예외는 조회 장애로 처리하며 해당 사이클의 상태 판정을 보류한다.
JobStatusFetcher = Callable[[], Any]

#: bjobs_path 기본값 — "앱이 명시로 지정했나"를 이 값과의 비교로 판정한다
#: (job_status_fetcher와 함께 주면 무시된다는 경고를 낼지 결정).
DEFAULT_BJOBS_PATH = "bjobs"

#: min_state_dwell_s 상한(초) = 1시간. 표시 지연이 그보다 길 이유가 없다.
MAX_STATE_DWELL_S = 3600.0

#: 공통 수치 범위: 이름 → (형변환, 하한, 상한, 하한 포함 여부). 상한 None은 제한 없음.
#: 범위 밖 값은 보정하지 않고 거부한다. poll_interval_s의 5~60초 정책은 options에서 적용한다.
NUMERIC_RANGES = {
    "workers":                  (int,   1, 64, True),
    "max_retry":                (int,   0, None, True),
    "retry_delay_s":            (float, 0.0, None, True),
    "chunk_size":               (int,   1, 5000, True),
    "arg_max":                  (int,   4096, None, True),
    "kill_chunk_size":          (int,   1, 5000, True),
    "kill_workers":             (int,   1, 32, True),
    "kill_max_retry":           (int,   0, None, True),
    "kill_retry_delay_s":       (float, 0.0, None, True),
    "lost_after_missing_polls": (int,   1, None, True),
    "progress_min_interval_s":  (float, 0.0, None, True),
    "progress_min_step_ratio":  (float, 0.0, 1.0, True),
    "min_state_dwell_s":        (float, 0.0, MAX_STATE_DWELL_S, True),
    "internal_retention_days":  (float, 0.0, None, True),
    "internal_lost_grace_s":    (float, 0.0, None, True),
    "poll_interval_s":          (float, 0.0, None, False),
    "submit_timeout_s":         (float, 0.0, None, False),
    "query_timeout_s":          (float, 0.0, None, False),
    "kill_timeout_s":           (float, 0.0, None, False),
}


def _in_range(value, name, cast, lo, hi, inclusive_lo):
    """범위 검증 + 형 정규화. 위반이면 ValueError (조용한 보정 금지)."""
    try:
        v = cast(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(
            f"{name}는 {cast.__name__} (got {value!r})") from None
    ok = ((cast is not float or math.isfinite(v))
          and (v >= lo if inclusive_lo else v > lo)
          and (hi is None or v <= hi))
    if not ok:
        bound = (f"{lo:g} 이상" if inclusive_lo else f"{lo:g} 초과")
        if hi is not None:
            bound = f"{lo:g}~{hi:g}"
        raise ValueError(f"{name}는 {bound} (got {value!r})")
    return v


@dataclass
class LsfConfig:
    """LSF 명령 경로/타임아웃/chunk 등 환경 설정."""
    # job_status_fetcher가 없을 때 사용할 조회 명령.
    bjobs_path: CmdPath = DEFAULT_BJOBS_PATH
    bkill_path: CmdPath = "bkill"

    #: bjobs 대신 사용할 상태 조회 콜백. 파싱·캐시·동시 호출 병합·만료는 라이브러리가 처리한다.
    #: URL·인증·타임아웃·재시도는 콜백이 담당한다. 상세 계약은 README §5.8 참고.
    job_status_fetcher: Optional[JobStatusFetcher] = None

    #: 주 콜백의 예외·파싱 실패·응답 지연 시 사용할 예비 콜백. 주 콜백과 계약이 같다.
    #: 주 콜백이 미회수 상태일 때만 예비를 먼저 호출하며, 회복하면 주 콜백으로 복귀한다.
    #: 양쪽 모두 실패하면 판정을 보류한다. 주 콜백 없이 단독 지정할 수 없다.
    job_status_fetcher_failover: Optional[JobStatusFetcher] = None

    #: 테스트용 실행 프로그램 치환: (argv[0] basename의 glob 패턴, 대체 CmdPath).
    #: 예: ("*_sub", "/mock/customwrapper_sub"). 나머지 인자는 보존한다.
    #: 토큰 목록으로 지정하면 고정 인자를 붙인다. JobRecord.command에는 원본을 보관한다.
    #: 환경에 따른 적용 여부는 호출자가 결정하며, None이면 치환하지 않는다.
    test_submit_wrapper_pattern_cmd: Optional[Tuple[str, CmdPath]] = None

    submit_timeout_s: float = 30.0
    query_timeout_s: float = 120.0
    kill_timeout_s: float = 120.0

    #: bjobs 호출당 job 수(100~500).
    chunk_size: int = 500
    arg_max: int = 131072                # 명령줄 인자 총 길이 상한 (보수적)

    #: 동시 wrapper 프로세스의 manager 전체 상한(1~64).
    #: 호출별 workers로 낮출 수 있지만, 공용 풀의 상한을 높일 수는 없다.
    workers: int = 8
    max_retry: int = 3                   # submit 재시도 횟수
    retry_delay_s: float = 2.0           # 첫 재시도 대기(초)
    retry_backoff: float = 1.0           # >1.0이면 지수 backoff("expo")

    #: bkill 1회에 전달할 target 수. timeout은 chunk 전체에 적용되므로
    #: 원격 왕복이 느린 환경에서는 chunk를 줄여 요청 도중의 timeout을 피한다.
    kill_chunk_size: int = 16
    #: 동시 bkill 프로세스의 manager 전체 상한(1~32). 1이면 직렬 실행.
    #: 동시 대상 수는 kill_workers × kill_chunk_size이므로 서버 부하를 고려해 조정한다.
    kill_workers: int = 4
    kill_max_retry: int = 2              # kill 확인 실패 시 재시도
    kill_retry_delay_s: float = 3.0      # kill 재시도 간격 — bkill은 비동기라
                                         # 확인('is being terminated')까지 여유
    #: optimistic: bkill 수락 시 EXIT로 표시하고 폴링 대상에서 제외한다.
    #: actual: 수락 후에도 실제 조회 결과로만 상태를 갱신한다.
    #: 두 정책 모두 수락된 job에 killed 표식을 남긴다.
    kill_status_policy: str = "optimistic"

    #: kill 후 실제 종료를 bjobs로 확인할지 (kill(verify=…)가 호출별로 덮는다).
    #: 앱 전역 정책으로, submit()에는 지정할 수 없다.
    verify_kill: bool = False

    poll_interval_s: float = 10.0        # 기본 polling 주기
    #: LOST 확정에 필요한 연속 미발견 폴링 횟수. 1이면 즉시 확정한다.
    lost_after_missing_polls: int = 3

    #: 콜백 캐시의 최소 갱신 간격(초). None이면 폴링 주기의 절반, 0이면 캐시 없음.
    #: kill verify와 새 추적 대상의 최초 조회는 캐시를 건너뛴다.
    internal_refresh_min_s: Optional[float] = None

    #: 콜백 원장의 DONE/EXIT 보존 기간(일). finish_time이 없으면 마지막 수신 시각 기준.
    #: 진행 중인 job은 만료하지 않으며, 0이면 종료 job도 만료하지 않는다.
    internal_retention_days: float = 14.0

    #: 콜백 집계 지연을 위한 제출 후 LOST 유예(초). 기간이 지나면 미발견 횟수를 센다.
    #: 기준은 submit_time, 없으면 updated_at이다. 0이면 별도 시간 유예가 없다.
    internal_lost_grace_s: float = 60.0

    #: MultiCluster의 source_cluster·forward_cluster 수집 여부.
    #: 미지원 시 해당 필드만 제외하고 나머지 확장 필드는 유지한다.
    collect_clusters: bool = False

    #: RUN 중 경과시간만 바뀌어도 jobs_updated를 발행할지 여부.
    #: False이면 상태 전이 시에만 경과시간을 반영해 대량 job의 UI 갱신 비용을 줄인다.
    poll_runtime_updates: bool = False

    #: pre_submit의 False 반환 시 submit_finished(cancelled=N)를 발행할지 여부.
    #: False이면 pre_submit_finished(False)만 발행한다. 게이트 예외는 설정과 무관하게
    #: error_occurred와 submit_finished(failed=N)로 보고한다.
    submit_finished_on_gate_reject: bool = True

    #: progress/jobs_updated 발화 빈도 제한 — 이 간격 경과 OR 이 비율만큼
    #: 진행했을 때만 발화(배치). 값이 클수록 시그널이 성겨져 부하↓·반응성↓.
    #: submit progress·jobs_updated 점진 발행·kill progress에 공통 적용.
    progress_min_interval_s: float = 0.5   # 최소 발화 간격(초), 0이면 시간 제한 없음
    progress_min_step_ratio: float = 0.01  # 최소 진행 비율(0~1), 0이면 매번

    #: job별 상태 표시 최소 간격(초). 0이면 지연 없음. Store는 즉시 갱신하되
    #: jobs_updated는 전이 순서대로 지연되어 완료 신호보다 늦게 도착할 수 있다.
    min_state_dwell_s: float = 0.0

    def __post_init__(self):
        for name, (cast, lo, hi, incl) in NUMERIC_RANGES.items():
            setattr(self, name,
                    _in_range(getattr(self, name), name, cast, lo, hi, incl))
        # config의 retry_backoff는 숫자이며, manager/submit 옵션의 'fixed:N'·'expo:N'과 다르다.
        try:
            self.retry_backoff = float(self.retry_backoff)
            if not math.isfinite(self.retry_backoff):
                raise ValueError
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
        # 형식 검증은 여기 한 곳 — 잘못된 값이 제출 worker 안에서야 터지면
        # 그 job만 SUBMIT_FAILED로 조용히 실패해 원인을 찾기 어렵다.
        if self.test_submit_wrapper_pattern_cmd is not None:
            rule = self.test_submit_wrapper_pattern_cmd
            if not isinstance(rule, (tuple, list)) or len(rule) != 2:
                raise ValueError(
                    "test_submit_wrapper_pattern_cmd는 (패턴, 명령) 2-튜플 — 예: "
                    f'("*_sub", "/path/to/customwrapper_sub") (got {rule!r})')
            pattern, cmd = rule
            if not isinstance(pattern, str) or not pattern:
                raise ValueError(
                    f"test_submit_wrapper_pattern_cmd의 패턴은 빈 문자열이 아닌 "
                    f"glob (got {pattern!r})")
            validate_cmd_path(cmd, "test_submit_wrapper_pattern_cmd의 명령")
            self.test_submit_wrapper_pattern_cmd = (pattern, cmd)
        if (self.job_status_fetcher is not None
                and not callable(self.job_status_fetcher)):
            raise ValueError("job_status_fetcher는 호출 가능해야 합니다 "
                             f"(got {self.job_status_fetcher!r})")
        if self.job_status_fetcher_failover is not None:
            if not callable(self.job_status_fetcher_failover):
                raise ValueError(
                    "job_status_fetcher_failover는 호출 가능해야 합니다 "
                    f"(got {self.job_status_fetcher_failover!r})")
            if self.job_status_fetcher is None:
                # 조용히 무시하면 앱은 예비가 있다고 믿는데 조회는 bjobs로
                # 간다 — 장애 때 예비가 안 도는 것을 그때서야 알게 된다.
                raise ValueError(
                    "job_status_fetcher_failover는 job_status_fetcher(주 콜백)와 "
                    "함께 줘야 합니다 — 주 콜백이 없으면 조회가 bjobs로 가서 "
                    "예비 콜백은 아무 데도 안 쓰입니다")
        # internal 갱신 간격은 0(캐시 끔) 허용 — 음수만 막는다.
        if self.internal_refresh_min_s is not None:
            self.internal_refresh_min_s = _in_range(
                self.internal_refresh_min_s, "internal_refresh_min_s",
                float, 0.0, None, True)


def cmd_tokens(path: CmdPath) -> List[str]:
    """CmdPath를 argv 앞부분 토큰 목록으로 정규화. str이면 프로그램 1개."""
    return [path] if isinstance(path, str) else list(path)


def validate_cmd_path(value, what: str) -> None:
    """CmdPath 형태 검증 공용 지점 — 비어있지 않은 str 또는 str 토큰 목록.
    (규약 정의(cmd_tokens) 옆에 검증도 한 곳 — options/_config가 공유)"""
    ok = (isinstance(value, str) and value) or (
        isinstance(value, (tuple, list)) and len(value) > 0
        and all(isinstance(t, str) and t for t in value))
    if not ok:
        raise ValueError(
            f"{what}는 비어있지 않은 str 또는 str 토큰 목록 (got {value!r})")
