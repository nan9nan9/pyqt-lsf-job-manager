"""설정 (LsfConfig) — Qt 비의존."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple, Union

#: LSF 명령 경로. 단일 프로그램은 str, bsub를 호출하는 wrapper처럼 고정 인자가
#: 붙는 명령은 토큰 목록으로 지정한다 (예: ["customwrapper_sub", "--proj", "X"]).
#: wrapper는 표준 bsub 옵션(-q/-J/-g/...)을 받아 bsub로 넘기고, bsub의 출력
#: ("Job <id> ...")을 그대로 뱉으면 된다 — 파싱/추적은 bsub와 동일하다.
CmdPath = Union[str, Sequence[str]]


@dataclass
class LsfConfig:
    """LSF 명령 경로/타임아웃/chunk 등 환경 설정."""
    # (v10: bsub_path/bgdel_path 삭제 — bsub 조립 제출·bgdel group 정리가
    #  제거됨. 제출은 wrapper 커맨드를 그대로 실행한다.)
    bjobs_path: CmdPath = "bjobs"
    bkill_path: CmdPath = "bkill"

    #: MultiCluster kill — {클러스터명: cshrc 경로}. 지정하면 kill이 대상의
    #: cluster를 확인해(미상이면 bkill 직전에 최소 포맷으로 1회 조회) 그
    #: 클러스터 env를 source한 bkill로 나눠 죽인다. forward된 job은 로컬
    #: bkill로 죽지 않는 환경의 해법이다. 와일드카드 키 `"*"`는 미상/미매핑
    #: 대상에 쓸 기본 env(없으면 env source 없는 plain bkill).
    #: 앱 환경 속성이라 호출마다 바뀌지 않는다 — 생성자에서 한 번 준다.
    cluster_envpaths: Dict[str, str] = field(default_factory=dict)

    #: wrapper 제출의 **실행 프로그램 치환** — (glob 패턴, 대체 CmdPath).
    #: 제출 커맨드는 문자열에 프로그램명이 박혀 있어(lsfmgr가 조립하지 않는다)
    #: 별도 경로 노브가 없다. 이 옵션은 argv[0]의 basename이 패턴에 맞을 때
    #: **그 프로그램만** 대체하고 나머지 인자는 그대로 둔다 — 커맨드를 하나도
    #: 안 고치고 전 제출을 다른 실행 파일로 돌린다(mock/테스트 환경 전환).
    #:
    #:     test_submit_wrapper_pattern_cmd=("*_sub", "/path/to/customwrapper_sub")
    #:     ["mytool_sub", "-q", "normal", "a.sp"]
    #:       → ["/path/to/customwrapper_sub", "-q", "normal", "a.sp"]
    #:
    #: 켤지 말지는 **호출자(앱)가 정한다** — 라이브러리는 환경을 읽지 않는다.
    #: 테스트 환경에서만 돌리려면 앱이 자기 기준(예: 환경 변수)으로 판단해 이
    #: 옵션을 줄지 말지 고르면 된다:
    #:
    #:     kw = ({"test_submit_wrapper_pattern_cmd": ("*_sub", MOCK)}
    #:           if os.environ.get("MY_TEST_MODE") else {})
    #:     mgr = LsfJobManager(**kw)
    #:
    #: 대체값은 CmdPath 규약 — 토큰 목록이면 고정 인자가 앞에 붙는다.
    #: **실행만** 바꾼다: JobRecord.command는 원본이 그대로 남아 표시·
    #: 재제출 기준이 흔들리지 않는다(재제출 때 이 규칙이 다시 적용된다).
    #: None(기본)이면 치환 없음.
    test_submit_wrapper_pattern_cmd: Optional[Tuple[str, CmdPath]] = None

    submit_timeout_s: float = 30.0
    query_timeout_s: float = 120.0
    kill_timeout_s: float = 120.0

    #: chunking 시 chunk당 job 수 (100~500). v10.1: 200→500 — 조회가 id chunk
    #: 단일 경로가 되어 사이클당 bjobs 횟수가 job수/chunk_size로 직결된다
    #: (10k job 기준 50→20회, 직렬 왕복이라 사이클 시간에 비례).
    chunk_size: int = 500
    arg_max: int = 131072                # 명령줄 인자 총 길이 상한 (보수적)

    workers: int = 32                    # 병렬 submit worker 수 (1~64)
                                         # 상한↑ 시 submit 호스트 CPU/RAM·master
                                         # 부하 주의 — rate_limit_per_s와 병행
    max_retry: int = 3                   # submit 재시도 횟수
    retry_delay_s: float = 2.0           # 첫 재시도 대기 (v7 기본 "fixed:2")
    retry_backoff: float = 1.0           # >1.0이면 지수 backoff("expo")
    rate_limit_per_s: Optional[float] = None   # bsub 초당 호출 제한

    kill_max_retry: int = 2              # kill 확인 실패 시 재시도
    kill_retry_delay_s: float = 3.0      # kill 재시도 간격 — bkill은 비동기라
                                         # 확인('is being terminated')까지 여유
    #: kill 상태 정책
    #: "optimistic" — bkill 'is being terminated' 확인 시 즉시 EXIT로 간주(기본).
    #                 bkill이 비동기라 실제 종료 전이지만, kill 의도가 수락됐으니
    #                 EXIT로 낙관 표시하고 폴링은 이 job을 더 조회하지 않는다.
    #: "actual"     — terminated 확인만으론 상태를 안 바꾸고, 실제 LSF 상태
    #                 (bjobs verify/폴링)로만 EXIT를 반영한다.
    #: 어느 정책이든 kill이 수락된 job에는 JobRecord.killed 표식이 즉시 남는다
    #: (상태 전이 시점과 무관 — "이 EXIT은 내가 죽인 것"의 근거).
    kill_status_policy: str = "optimistic"

    poll_interval_s: float = 10.0        # 기본 polling 주기
    #: bjobs에서 안 보이는 job을 LOST로 확정하기까지 필요한 **연속** 미발견
    #: 폴링 횟수. 1이면 즉시 확정. 제출 직후 등록 지연이나, 앱 환경이 가리키는
    #: 클러스터와 wrapper가 실제 제출한 클러스터가 달라 한두 사이클 안 보이는
    #: 경우에 멀쩡한 job을 LOST로 만들지 않기 위한 유예다.
    lost_after_missing_polls: int = 3

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
    #: 종료 통지는 pre_submit_finished(False)만으로 한다. (게이트 예외는 이 옵션과
    #: 무관하게 항상 error_occurred + submit_finished(failed=N)로 보고한다)
    submit_finished_on_gate_reject: bool = True

    #: progress/jobs_updated 발화 빈도 제한 — 이 간격 경과 OR 이 비율만큼
    #: 진행했을 때만 발화(배치). 값이 클수록 시그널이 성겨져 부하↓·반응성↓.
    #: submit progress·jobs_updated 점진 발행·kill progress에 공통 적용.
    progress_min_interval_s: float = 0.5   # 최소 발화 간격(초), 0이면 시간 제한 없음
    progress_min_step_ratio: float = 0.01  # 최소 진행 비율(0~1), 0이면 매번

    #: 상태 전이 **표시** 최소 간격(초) — 0(기본)이면 끔. 켜면 job별로 한 상태가
    #: 이 시간만큼 화면에 머문 뒤에야 다음 전이가 jobs_updated로 나간다
    #: (SUBMITTING→PEND, EXIT→SUBMITTING처럼 순식간에 지나가는 전이를 눈에
    #: 보이게 한다). 전이는 버리지 않고 순서대로 밀린다 — 표시가 최대
    #: (밀린 전이 수 × 이 값)만큼 store보다 늦는다. store는 늘 즉시 갱신되므로
    #: 켜는 순간 jobs_updated에 한해 store-first/finished-last가 느슨해진다
    #: (lsfmgr/pacer.py 참고).
    min_state_dwell_s: float = 0.0

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
        if self.min_state_dwell_s < 0:
            raise ValueError("min_state_dwell_s는 0 이상")
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
        # poll_interval_s/submit_timeout_s도 여기서 검증한다 — 안 하면
        # LsfConfig(poll_interval_s=0) 같은 값이 통과해, auto_poll 시
        # start_polling(0.0)이 큐드 Qt slot 안에서 ValueError를 던져 앱이 죽는다.
        # 단, LsfConfig는 저수준 dataclass이므로 **구조적 불변식(양수)만** 강제한다
        # — runtime 가드(start_polling의 `if eff <= 0`)와 정합. 5~60 같은 UX 정책
        # 범위는 상위 options/manager-kwarg 계층(options._validate_option)의 몫이다.
        # 여기서 5~60을 강제하면 poll_interval_s=2(빠른 로컬 폴링) 같은 정당한
        # 저수준 사용을 막고, 이전에 통과하던 config를 생성 시점에 죽인다(회귀).
        if self.poll_interval_s is None or float(self.poll_interval_s) <= 0:
            raise ValueError(
                f"poll_interval_s는 양수 (got {self.poll_interval_s!r})")
        self.poll_interval_s = float(self.poll_interval_s)
        if self.submit_timeout_s is None or float(self.submit_timeout_s) <= 0:
            raise ValueError(
                f"submit_timeout_s는 양수 (got {self.submit_timeout_s!r})")
        self.submit_timeout_s = float(self.submit_timeout_s)


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

# (v10: JobSpec/spec_to_json/spec_from_json 삭제 — bsub 인자 조립 제출이
#  제거되어 옵션 템플릿·재제출 옵션 보존이 필요 없다. 제출은 wrapper
#  커맨드 문자열/argv가 전부다.)


