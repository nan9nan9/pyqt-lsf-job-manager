"""설정 (LsfConfig) — Qt 비의존."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, Union

from .internal_status import JobStatusFetcher

#: LSF 명령 경로. 단일 프로그램은 str, bsub를 호출하는 wrapper처럼 고정 인자가
#: 붙는 명령은 토큰 목록으로 지정한다 (예: ["customwrapper_sub", "--proj", "X"]).
#: wrapper는 표준 bsub 옵션(-q/-J/-g/...)을 받아 bsub로 넘기고, bsub의 출력
#: ("Job <id> ...")을 그대로 뱉으면 된다 — 파싱/추적은 bsub와 동일하다.
CmdPath = Union[str, Sequence[str]]

#: bjobs_path 기본값 — "앱이 명시로 지정했나"를 이 값과의 비교로 판정한다
#: (job_status_fetcher와 함께 주면 무시된다는 경고를 낼지 결정).
DEFAULT_BJOBS_PATH = "bjobs"

#: min_state_dwell_s 상한(초) = 1시간. 표시 지연이 그보다 길 이유가 없다.
MAX_STATE_DWELL_S = 3600.0


@dataclass
class LsfConfig:
    """LSF 명령 경로/타임아웃/chunk 등 환경 설정."""
    # (v10: bsub_path/bgdel_path 삭제 — bsub 조립 제출·bgdel group 정리가
    #  제거됨. 제출은 wrapper 커맨드를 그대로 실행한다.)
    #: 조회 명령. job_status_fetcher를 주면 조회가 그 콜백으로 넘어가고
    #: 이 값은 쓰이지 않는다 (lsfmgr/internal_status.py).
    bjobs_path: CmdPath = DEFAULT_BJOBS_PATH
    bkill_path: CmdPath = "bkill"

    #: 상태 조회 콜백. **주면** 상태 조회가 bjobs subprocess 대신 이 콜백으로
    #: 간다(안 주면 종전대로 bjobs). 인자 없이 호출되고 REST 응답 JSON
    #: ({"jobs": [...]})을 그대로 반환하면 된다 — 파싱·캐시·동시 호출 합치기·
    #: 원장 만료는 라이브러리가 한다 (README §5.8).
    #: URL·인증·**타임아웃**·재시도는 콜백의 몫이다. 안 돌아오는 콜백은
    #: 라이브러리가 견디기만 할 뿐 되살리지 못한다.
    #: 아래 internal_* 노브들이 이 조회원의 동작을 정한다.
    job_status_fetcher: Optional[JobStatusFetcher] = None

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

    #: 동시에 도는 wrapper 프로세스 수 상한 (1~64). 기본 8.
    #: **전역**이다 — submitter가 공용 QThreadPool 하나를 쓰므로 jobset을 몇 개
    #: 동시에 제출하든 총합이 이 값을 넘지 않는다. 호출별 workers는 이 값
    #: **아래로 낮추는** 용도다(올려도 공용 풀 크기를 못 넘는다).
    #: 크게 잡으면 submit 호스트 CPU/RAM과 LSF master(mbatchd/eauth)를 함께
    #: 두들겨 bsub가 간헐적으로 "User permission denied"(exit 255)로 떨어진다.
    #: **제출 부하를 정하는 유일한 노브**다 (v11: rate_limit_per_s 삭제 —
    #: 초당 상한은 이 값과 bsub 1회 소요로 이미 결정된다: workers/bsub_소요).
    workers: int = 8
    max_retry: int = 3                   # submit 재시도 횟수
    retry_delay_s: float = 2.0           # 첫 재시도 대기 (v7 기본 "fixed:2")
    retry_backoff: float = 1.0           # >1.0이면 지수 backoff("expo")

    #: bkill 1회에 실을 target 수. 조회(chunk_size)와 **따로 두는 이유**:
    #: bjobs는 읽기라 500건도 금방 끝나지만 bkill은 job마다 mbatchd가 실제
    #: 처리(+MC면 원격 클러스터로 전달)를 하는 쓰기라 훨씬 느리다. 한 chunk가
    #: kill_timeout_s를 넘기면 subprocess timeout이 bkill **클라이언트**를
    #: 중간에 죽여, 앞쪽 id만 죽고 뒤쪽은 요청조차 안 나간 채 잘린다 —
    #: 그 상태로 재시도하면 "bkill timeout" 경고가 반복된다.
    #: 기본 16 — MC(job forwarding) 사이트 기준으로 잡은 값이다. forward된 job의
    #: bkill은 원격 클러스터 왕복까지 기다려 job당 수백 ms까지 가므로, chunk가
    #: 크면 짧게 잡은 kill_timeout_s를 쉽게 넘긴다. 16이면 기본 120s에서 job당
    #: 7.5초, 8초로 줄여 잡은 사이트에서도 job당 0.5초 여유가 남는다.
    #: 대량 kill에서 bkill 호출 횟수가 늘지만(1000건 → 63회) 각 호출이 확실히
    #: 완주하는 편이 잘려서 재시도하는 것보다 총 소요가 짧다.
    kill_chunk_size: int = 16
    #: **동시에 띄울 bkill 프로세스 수** (1~32). manager 전체 상한이다 —
    #: 실행 풀을 하나 공유하므로 kill 명령이 몇 건 동시에 돌든 총합이 이 값을
    #: 넘지 않는다(kill마다 풀을 만들면 실측 동시 16개까지 갔다).
    #: kill_chunk_size가 "한 호출에 몇 건"이라면 이건 "그 호출을 몇 개 동시에".
    #: MC(job forwarding) 사이트에서 bkill은 원격 클러스터 왕복을 기다리는
    #: **지연 지배적** 작업이라, 병렬로 돌리면 대량 kill이 그만큼 빨라진다
    #: (직렬이면 ceil(N/chunk)회를 한 줄로 세워 기다린다 — 5000건에 bkill
    #: 1회가 3초면 313회 x 3s = 16분이 한 줄로 늘어선다).
    #: 기본 4 — 기본 chunk(16)와 곱해 동시 64건이면 submit의 workers=8이
    #: 거는 부하와 같은 급이다. 1로 두면 직렬(옛 동작).
    #: ⚠ 동시에 mbatchd에 붙는 요청이 kill_workers x kill_chunk_size건이 된다 —
    #: 더 키울 때는 submit의 workers와 같은 이유로 실측하고 올릴 것
    #: (bkill 1회 소요는 DEBUG 로그의 `exec bkill … → rc=0 (N.NNNs)`).
    kill_workers: int = 4
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

    #: 콜백 조회원(job_status_fetcher)이 켜졌을 때 상태 스냅샷의
    #: **최소 갱신 간격**(초).
    #: 이 간격 안에 다시 들어온 조회는 직전 스냅샷을 재사용한다 — REST는 유저의
    #: 전 job을 한 번에 주므로 폴링 1사이클에 콜백을 여러 번 돌릴 이유가 없다.
    #: None(기본)이면 poll_interval_s의 절반 — 폴링 사이클마다 정확히 1회
    #: 갱신되면서, 사이클 중간에 낀 killer verify·detect_lost는 같은 스냅샷을
    #: 공유한다. 0이면 캐시 없음(조회마다 콜백).
    #: ※ kill verify는 이 값과 무관하게 항상 새로 받는다(fresh 조회) — 방금
    #:   죽인 job의 생사는 캐시로 답할 수 없다.
    internal_refresh_min_s: Optional[float] = None

    #: internal 원장의 **종료 job 보존 기간**(일). 콜백이 증분(`updatefrom`)으로
    #: 돌면 내부 원장은 계속 누적되므로, 끝난 지(finish_time) 이만큼 지난
    #: DONE/EXIT은 버려 메모리가 무한정 늘지 않게 한다. finish_time을 안 주는
    #: payload면 그 항목을 마지막으로 받은 시각을 대신 쓴다.
    #: 진행 중(PEND/RUN/...) job은 아무리 오래돼도 버리지 않는다 — 아직 조회
    #: 대상이기 때문. 0이면 만료 없음(무한 누적 — 단기 실행 프로세스 전용).
    internal_retention_days: float = 14.0

    #: internal 모드의 **제출 직후 LOST 유예**(초). 원장(REST 집계)이 아직
    #: 이 job을 모르는 구간에서 미발견을 LOST 스트릭으로 세지 않는다.
    #:
    #: bjobs 경로의 유예(lost_after_missing_polls)와 목적이 다르다. 거기서
    #: 미발견은 대부분 진짜 부재(purge)이고 등록 지연은 초 단위라 '연속 N회'로
    #: 충분했다. 반면 누적 원장은 non-terminal job을 지우지 않으므로 internal
    #: 모드의 미발견은 사실상 **항상** "아직 집계 안 됨"이다 — 이걸 회수로 세면
    #: 폴링 주기에 따라 유예가 조용히 늘었다 줄고(주기 10초·3회=30초), 집계가
    #: 그보다 조금만 늦어도 멀쩡한 job이 LOST(되돌릴 수 없음)로 죽는다.
    #: 그래서 기준을 **제출 후 경과 시간**으로 잡는다 — 폴링 주기와 무관하다.
    #:
    #: 이 시간이 지나도 안 보이면 그때부터 정상 스트릭이 시작된다(진짜 소실은
    #: 여전히 확정된다). 0이면 유예 없음 = bjobs 경로와 같은 판정.
    #: 기준 시각은 JobRecord.submit_time, 없으면 updated_at.
    internal_lost_grace_s: float = 60.0

    #: LSF MultiCluster(job forwarding) 정보 수집 — bjobs -o 에 source_cluster·
    #: forward_cluster 필드를 추가해 JobRecord.source_cluster/forward_cluster 로
    #: 채운다. MC 환경에서만 켠다(기본 꺼짐) — 미지원 LSF면 그 필드만 자동
    #: 강등(FULL+cluster → FULL)돼 run_time 등 다른 확장 필드는 유지된다.
    collect_clusters: bool = False

    #: RUN 중 run_time_s(경과 실행시간) 변화도 폴링 갱신·jobs_updated 발행
    #: 대상에 포함할지.
    #:
    #: 기본 False — 켜면 **RUN job 전원이 매 폴링 재전이**된다. 5000건 기준
    #: 사이클당 5000 transition + 5000레코드짜리 jobs_updated 1회이고(실측
    #: 217ms), 그 배치를 받는 앱의 표 갱신이 진짜 부담이다. 상태가 그대로인
    #: job을 매 주기 다시 그릴 이유가 없다.
    #: 끄면 run_time_s는 **상태 전이 시점**(RUN→DONE 등)에만 반영된다 — 표의
    #: 경과시간 열이 실시간으로 흐르지 않는다. 그 열이 꼭 필요하고 RUN이 수백
    #: 규모라면 True로 켠다.
    poll_runtime_updates: bool = False

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
            self.chunk_size = 500            # 필드 기본값과 동일한 폴백
        if self.kill_chunk_size < 1:
            self.kill_chunk_size = 16        # 필드 기본값과 동일한 폴백
        self.kill_workers = max(1, min(32, int(self.kill_workers)))
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
        # 상한도 본다 — 표시 지연이 1시간을 넘을 이유가 없고, 방치하면
        # QTimer의 int32 ms를 넘겨 pacer의 재예약 slot에서 OverflowError가
        # 난다(그 순간 전이 표시가 통째로 멎는다).
        if not (0 <= self.min_state_dwell_s <= MAX_STATE_DWELL_S):
            raise ValueError(
                f"min_state_dwell_s는 0~{MAX_STATE_DWELL_S:g} "
                f"(got {self.min_state_dwell_s!r})")
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
        # 주기/타임아웃도 여기서 검증한다 — 안 하면 LsfConfig(poll_interval_s=0)
        # 같은 값이 통과해, auto_poll 시 start_polling(0.0)이 큐드 Qt slot 안에서
        # ValueError를 던져 앱이 죽는다.
        # 단, LsfConfig는 저수준 dataclass이므로 **구조적 불변식(양수)만** 강제한다
        # — runtime 가드(start_polling의 `if eff <= 0`)와 정합. 5~60 같은 UX 정책
        # 범위는 상위 options/manager-kwarg 계층(options._validate_option)의 몫이다.
        # 여기서 5~60을 강제하면 poll_interval_s=2(빠른 로컬 폴링) 같은 정당한
        # 저수준 사용을 막고, 이전에 통과하던 config를 생성 시점에 죽인다(회귀).
        # subprocess timeout 3형제도 같이 본다 — query/kill은 manager kwarg가
        # 없어 LsfConfig로만 주는데(검증 계층이 여기뿐), 0/음수면 subprocess.run이
        # 매번 TimeoutExpired를 던진다. 특히 query_timeout_s는 증상이 조용하다:
        # 전 chunk가 '조회 실패'로 귀속되고 monitor는 설계대로 판단을 보류해
        # (LOST 확정 안 함) 폴링이 영영 상태를 못 올린 채 PEND에 고착된다.
        for name in ("poll_interval_s", "submit_timeout_s",
                     "query_timeout_s", "kill_timeout_s"):
            value = getattr(self, name)
            if value is None or float(value) <= 0:
                raise ValueError(f"{name}는 양수 (got {value!r})")
            setattr(self, name, float(value))
        if (self.job_status_fetcher is not None
                and not callable(self.job_status_fetcher)):
            raise ValueError("job_status_fetcher는 호출 가능해야 합니다 "
                             f"(got {self.job_status_fetcher!r})")
        # internal 갱신 간격은 0(캐시 끔) 허용 — 음수만 막는다.
        if self.internal_refresh_min_s is not None:
            if float(self.internal_refresh_min_s) < 0:
                raise ValueError("internal_refresh_min_s는 0 이상 "
                                 f"(got {self.internal_refresh_min_s!r})")
            self.internal_refresh_min_s = float(self.internal_refresh_min_s)
        if float(self.internal_retention_days) < 0:
            raise ValueError("internal_retention_days는 0 이상 "
                             f"(got {self.internal_retention_days!r})")
        self.internal_retention_days = float(self.internal_retention_days)
        if float(self.internal_lost_grace_s) < 0:
            raise ValueError("internal_lost_grace_s는 0 이상 "
                             f"(got {self.internal_lost_grace_s!r})")
        self.internal_lost_grace_s = float(self.internal_lost_grace_s)

    @property
    def effective_internal_refresh_min_s(self) -> float:
        """internal 스냅샷 갱신 간격의 실효값 — 미지정이면 폴링 주기의 절반.

        절반인 이유: 폴링 사이클(poll_interval_s)마다 반드시 한 번은 새로
        받으면서, 그 사이클 안에서 겹쳐 들어오는 조회는 캐시로 흡수한다.
        """
        if self.internal_refresh_min_s is not None:
            return float(self.internal_refresh_min_s)
        return float(self.poll_interval_s) / 2.0


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


