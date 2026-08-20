"""LsfCommand — 제출(wrapper)/bjobs/bkill subprocess 래퍼.

Qt 비의존 순수 Python (§8 원칙). shell 미경유, runner 주입으로 mock 테스트
가능. chunking + ARG_MAX 검사 내장.
"""
from __future__ import annotations

import fnmatch
import inspect
from functools import lru_cache
import logging
import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    Callable, Iterator, List, Optional, Sequence, Set, Tuple,
)

from .config import DEFAULT_BJOBS_PATH, LsfConfig, cmd_tokens
from .errors import ArgMaxExceededError, LsfCommandError, SubmitError
from .internal_status import InternalStatusSource
from .states import LSF_STAT_MAP, JobState

log = logging.getLogger("lsfmgr.command")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


# runner 시그니처: (argv, timeout_s, cwd) -> CommandResult
# cwd: 제출(bsub/wrapper) 시 subprocess를 실행할 작업 디렉토리(None=부모 cwd).
# 커스텀 runner를 주입하는 경우 이 3번째 인자를 받아야 한다(하위호환 계약 변경).
# **thread-safe여야 한다** — 제출은 workers개, kill은 kill_workers개 스레드에서
# 동시에 부른다(조회도 폴링/verify가 겹칠 수 있다). 공유 가변 상태를 두려면
# runner 쪽에서 잠글 것.
Runner = Callable[[Sequence[str], float, Optional[str]], CommandResult]


def default_runner(argv: Sequence[str], timeout: float,
                   cwd: Optional[str] = None) -> CommandResult:
    """기본 runner — subprocess.run (shell 미경유).
    cwd 지정 시 그 디렉토리에서 실행한다 — 자식 프로세스에만 적용돼
    동시 제출 worker 간 경합이 없다(os.chdir 같은 프로세스 전역 변경 금지)."""
    proc = subprocess.run(
        list(argv), capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def _adapt_runner(runner: Runner) -> Runner:
    """cwd 인자가 추가되기 전(구 2-arg (argv, timeout)) runner를 하위호환으로
    감싼다 — cwd를 못 받는 runner면 cwd를 무시하는 어댑터로 래핑해, 계약 확장이
    기존 주입 runner를 깨지 않게 한다(cwd 미지원 runner는 work_dir을 못 지키지만
    TypeError로 죽는 대신 조용히 부모 cwd에서 실행된다)."""
    try:
        params = inspect.signature(runner).parameters
    except (ValueError, TypeError):
        return runner            # 시그니처 조사 불가(빌트인 등) — 그대로 3-arg 시도
    kinds = [p.kind for p in params.values()]
    positional = sum(1 for k in kinds
                     if k in (inspect.Parameter.POSITIONAL_ONLY,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD))
    flexible = (inspect.Parameter.VAR_POSITIONAL in kinds
                or inspect.Parameter.VAR_KEYWORD in kinds
                or "cwd" in params)
    if positional >= 3 or flexible:
        return runner            # cwd 수용 가능
    return lambda argv, timeout, cwd=None: runner(argv, timeout)


@dataclass(frozen=True)
class JobStatus:
    """bjobs 1행 파싱 결과."""
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


_JOB_ID_RE = re.compile(r"Job <(\d+)>")
_ARRAY_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?$")

# ----------------------------------------------------------------------
# kill/verify target 문자열 문법 — "id" / "id[idx]" / "id[m-n]".
# 해석은 여기 한 곳이 소유한다 (killer가 공유) — 문자열 슬라이싱이 여러
# 모듈에 흩어져 표기 변화에 제각기 깨지는 것을 막는다.
# ----------------------------------------------------------------------
_TARGET_RE = re.compile(r"^(\d+)(?:\[(\d+)(?:-(\d+))?\])?$")


def target_parent_id(target) -> Optional[int]:
    """target의 parent job_id — "1000[3]" → 1000. 비수치 형식이면 None."""
    head = str(target).split("[", 1)[0]
    return int(head) if head.isdigit() else None


def classify_targets(targets) -> Tuple[Set[int], set, List[Tuple[int, int, int]]]:
    """target 목록을 (whole, exact, ranges)로 분류한다.

    whole: bare id — 그 job_id 전체(비array job, 또는 array element 전부).
    exact: (job_id, array_index) — element 1개 지정.
    ranges: (job_id, lo, hi) — element 범위 지정.
    파싱 불가 target은 bare id로 관대 처리한다(강건성 — 예외로
    kill_finished가 오보되지 않게), 그래도 안 되면 경고 후 무시."""
    whole: Set[int] = set()
    exact: set = set()
    ranges: List[Tuple[int, int, int]] = []
    for t in targets:
        t = str(t)
        m = _TARGET_RE.match(t)
        if m is None:
            pid = target_parent_id(t)
            if pid is None:
                log.warning("파싱 불가 target 무시: %r", t)
            else:
                whole.add(pid)
            continue
        pid = int(m.group(1))
        if m.group(2) is None:                 # bare id
            whole.add(pid)
        elif m.group(3) is None:               # 단일 element [idx]
            exact.add((pid, int(m.group(2))))
        else:                                  # 범위 [m-n]
            ranges.append((pid, int(m.group(2)), int(m.group(3))))
    return whole, exact, ranges
# bjobs가 매칭 결과 없음을 알릴 때의 메시지들
_NO_JOB_PATTERNS = ("no unfinished job", "no matching job", "is not found",
                    "no job found")

# LSF -o 시간 필드 파싱 — run_time은 "NNN second(s)", start/finish는 시각 문자열.
# LSF 버전/로케일마다 포맷이 달라 방어적으로 여러 형식을 시도한다.
_RUN_TIME_RE = re.compile(r"(\d+)")
_LSF_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%b %d %H:%M:%S %Y",
                     "%b %d %H:%M %Y", "%b %d %H:%M:%S", "%b %d %H:%M")


# bjobs -o가 확장 필드/옵션을 못 알아볼 때의 stderr 신호. 가장 확실한 건 LSF가
# 되돌려주는 '필드명 자체'이고(대부분 에러에 echo됨), 그 외 format/field 류
# 특정 문구를 보조로 쓴다. "unknown host"/"invalid ..." 같은 일시장애 문구가
# 오판되지 않도록 광범위 단독 단어("unknown"/"invalid"/"no such")는 제외한다.
_BJOBS_FIELD_ERR = ("run_time", "start_time", "finish_time",
                    "source_cluster", "forward_cluster",
                    "unknown field", "bad field", "field name", "illegal",
                    "not a valid", "unrecognized", "output format",
                    "format specification", "invalid format", "illegal option")


def _clean_field(s: str) -> Optional[str]:
    """bjobs -o 문자열 필드 정규화 — 빈값/'-'는 None (미해당)."""
    s = (s or "").strip()
    return s if s and s != "-" else None


def _looks_like_field_error(err_text: str) -> bool:
    e = (err_text or "").lower()
    return any(p in e for p in _BJOBS_FIELD_ERR)


# bkill 출력 1행: "Job <123>..." 또는 "Job <123[4]>: ..." — id와 나머지 메시지.
_BKILL_LINE_RE = re.compile(r"Job <(\d+(?:\[\d+\])?)>[:\s]?\s*(.*)")
# 해소 신호 — 더 kill할 필요 없음: 신호 수락 or 이미 없음/끝남.
_BKILL_RESOLVED_MSGS = (
    "is being terminated", "is being signaled", "is being requeued",
    "is being killed", "in progress of being terminated",
    "already finished", "has already", "no matching job", "is not found",
    "no unfinished job", "not found",
)


def _parse_bkill_resolved(text: str, requested: "set[str]") -> "set[str]":
    """bkill stdout/stderr에서 '해소된'(재시도 불필요) job id/target을 뽑는다.
    미해소(일시 장애 등)는 여기 안 들어가 호출자가 재시도한다.

    requested는 이번 chunk에 **실제로 넘긴** target 집합이다 — element 응답
    행에서 bare 부모 id를 유도할지 말지가 여기에 달렸다(아래)."""
    resolved = set()
    for line in text.splitlines():
        m = _BKILL_LINE_RE.search(line)
        if not m:
            continue
        jid, msg = m.group(1), m.group(2).lower()
        if any(p in msg for p in _BKILL_RESOLVED_MSGS):
            resolved.add(jid)
            # bare 부모 id로 array를 kill하면 LSF는 element별("1000[0]")로
            # 확인 행을 낸다 — 부모 pending("1000")과 매칭되게 부모도 해소 처리
            # (kill 요청이 그 job에 수락됐다는 의미). 안 하면 불필요 재시도.
            #
            # **부모를 실제로 요청했을 때만** 유도한다. 안 그러면 element
            # 하나만 겨냥한 kill("1000[3]")의 응답이 bare "1000"까지 해소로
            # 만들고, JobRecord는 array_index가 늘 None이라(집계 레코드)
            # 그 레코드가 target "1000"으로 매칭돼 job 전체가 EXIT로 찍힌다
            # — LSF에선 나머지 element가 멀쩡히 도는데 앱에는 죽은 것으로
            # 보인다(폴링 대상에서도 빠져 영영 안 고쳐진다).
            if "[" in jid:
                parent = jid.split("[", 1)[0]
                if parent in requested:
                    resolved.add(parent)
    return resolved


def _parse_run_time(s: str) -> Optional[int]:
    """'120 second(s)' → 120. 미실행('-'/빈값)은 None."""
    s = s.strip()
    if not s or s == "-":
        return None
    m = _RUN_TIME_RE.search(s)
    return int(m.group(1)) if m else None


def _parse_lsf_time(s: str) -> Optional[datetime]:
    """LSF 시각 문자열 → datetime. 파싱 불가/미해당('-')은 None (graceful).
    'E' 접미(estimated — RUN 중 예상 종료시각)는 실측이 아니므로 버린다.

    LRU 캐시(리뷰 P2): start/finish 문자열은 job마다 고정이라 매 폴링 같은
    문자열을 재파싱한다 — strptime 다중 포맷 시도가 10k job 기준 사이클당
    ~0.5s(실측)를 차지했다. 캐시 크기 65536 ≈ job 3만 개(start+finish).
    연도 없는 포맷의 연도 판정이 캐시로 고정되지만, 그 판정 자체가 '과거
    시각' 가정이라 해가 바뀌어도 동일 문자열의 정답은 같다(무해)."""
    return _parse_lsf_time_cached(s.strip())


@lru_cache(maxsize=65536)
def _parse_lsf_time_cached(s: str) -> Optional[datetime]:
    if s.endswith(" E"):
        return None                        # 예상값 — 실제 시각으로 저장 금지
    s = re.sub(r"\s+[A-Z]$", "", s).strip()   # 상태 접미(L/X 등) 제거
    if not s or s == "-":
        return None
    now = datetime.now()
    for fmt in _LSF_TIME_FORMATS:
        # 연도 없는 포맷은 기본연도 1900(비윤년)이라 "Feb 29" 파싱이 실패해
        # 시각이 통째로 소실된다 — 연도를 명시해 파싱한다(올해 → 불가 시 작년).
        attempts = ([(s, fmt)] if "%Y" in fmt
                    else [(f"{s} {now.year}", fmt + " %Y"),
                          (f"{s} {now.year - 1}", fmt + " %Y")])
        for text, f in attempts:
            try:
                dt = datetime.strptime(text, f)
            except ValueError:
                continue
            # 연말 경계: 12월에 시작한 job을 1월에 조회하면 '올해 12월'은
            # 미래가 된다 — 하루 여유를 두고 미래면 작년으로 되돌린다
            if "%Y" not in fmt and dt > now + timedelta(days=1):
                try:
                    dt = dt.replace(year=dt.year - 1)
                except ValueError:
                    continue                   # 2/29 → 비윤년 보정 불가
            return dt
    log.debug("LSF 시간 파싱 불가: %r", s)
    return None


def chunk_args(items: Sequence[str], chunk_size: int, arg_max: int,
               base_len: int = 0) -> Iterator[List[str]]:
    """인자 목록을 chunk_size 및 ARG_MAX(총 길이) 기준으로 분할."""
    chunk: List[str] = []
    length = base_len
    for item in items:
        add = len(item) + 1
        if base_len + add > arg_max:
            raise ArgMaxExceededError(
                f"단일 인자가 ARG_MAX({arg_max})를 초과: {item[:80]}...")
        if chunk and (len(chunk) >= chunk_size or length + add > arg_max):
            yield chunk
            chunk = []
            length = base_len
        chunk.append(item)
        length += add
    if chunk:
        yield chunk


class LsfCommand:
    """LSF 명령 래퍼. runner를 주입하면 subprocess 없이 단위 테스트 가능."""

    def __init__(self, config: Optional[LsfConfig] = None,
                 runner: Optional[Runner] = None):
        self.config = config or LsfConfig()
        # 구 2-arg runner도 받아들인다(계약 확장 하위호환) — 아래 _run은 항상
        # cwd를 3번째 인자로 넘기므로, cwd 미지원 runner는 어댑터로 감싼다.
        self.runner = _adapt_runner(runner or default_runner)
        # 치환은 '무엇이 실제로 실행되는지'를 바꾼다 — 실수로 켠 채 운영에
        # 제출하는 일이 없도록 INFO로 1회 남긴다 (per-job 원문은 _run의 DEBUG).
        if self.config.test_submit_wrapper_pattern_cmd is not None:
            pattern, cmd = self.config.test_submit_wrapper_pattern_cmd
            log.info("wrapper 제출 치환 활성 — 패턴 %r에 맞는 프로그램은 %s 로 "
                     "실행됩니다", pattern, " ".join(cmd_tokens(cmd)))
        # 확장 필드로 시작 — 필드 오류 감지 시 한 단계씩 강등 (인스턴스 수명 유지).
        # collect_clusters면 FULL+MC를 맨 앞에 둬, 미지원 시 FULL로만 내려가
        # run_time 등은 유지된다(MC 필드만 포기).
        self._bjobs_formats = (
            [self._BJOBS_FULL_MC_FMT, self._BJOBS_FULL_FMT, self._BJOBS_CORE_FMT]
            if self.config.collect_clusters
            else [self._BJOBS_FULL_FMT, self._BJOBS_CORE_FMT])
        self._bjobs_fmt_idx = 0
        # 강등은 폴링 스레드·killer verify 워커·detect_lost 호출 스레드가
        # 동시에 시도할 수 있다 — 무락 증가면 같은 필드 오류에 이중 강등돼
        # FULL을 건너뛰고 CORE로 떨어진다. 사용한 인덱스 기준 CAS로 1단만.
        self._bjobs_fmt_lock = threading.Lock()
        # bkill 실행 풀 — **manager당 하나**를 재사용한다. kill 호출마다 새로
        # 만들면 kill_workers가 kill 1건의 상한일 뿐이라, 동시에 kill이 여러 건
        # 돌면 그 배수만큼 bkill이 뜬다(실측: Killer 풀 4인데 동시 6개 —
        # quiesce 중 releaseThread로 슬롯을 반납해 4보다 많은 kill이 chunk
        # 단계에 겹친다). workers를 전역으로 만든 것과 같은 이유다.
        # ThreadPoolExecutor는 생성 시 스레드를 만들지 않는다(첫 submit에 생성)
        # — kill을 안 쓰는 앱은 비용이 0이다.
        self._bkill_pool: Optional[ThreadPoolExecutor] = None
        if self.config.kill_workers > 1:
            self._bkill_pool = ThreadPoolExecutor(
                max_workers=self.config.kill_workers,
                thread_name_prefix="lsfmgr-bkill")
        self._warn_if_kill_budget_is_tight()
        # --- 조회원 선택: 앱 콜백 / bjobs subprocess ---
        # 갈림은 **job_status_fetcher 하나뿐**이다 — 주면 콜백, 안 주면 bjobs.
        # 위쪽(monitor/killer)은 bjobs_by_ids 계약만 보므로 어느 쪽이든 flow가
        # 같다. 이 판정은 생성 시점 1회로 끝난다(조회마다 다시 보지 않는다).
        self._internal: Optional[InternalStatusSource] = None
        if self.config.job_status_fetcher is not None:
            self._internal = InternalStatusSource(
                self.config.job_status_fetcher,
                refresh_min_s=self.config.effective_internal_refresh_min_s,
                wait_timeout_s=self.config.query_timeout_s,
                retention_days=self.config.internal_retention_days,
                # 앱이 값을 명시하지 않았으면(=폴링 주기에서 유도) 실제
                # 폴링 주기를 알게 될 때 자동으로 낮출 수 있게 한다.
                auto_refresh=self.config.internal_refresh_min_s is None,
                # run_time 갱신을 monitor가 버리는 설정이면 원장에서도
                # 만들지 않는다 (전수 스캔 비용을 통째로 없앤다).
                track_runtime=self.config.poll_runtime_updates)
            log.info("상태 조회원: job_status_fetcher 콜백 (bjobs 미사용, 최소 "
                     "갱신 간격 %.1fs, 종료 job 보존 %.0f일)",
                     self.config.effective_internal_refresh_min_s,
                     self.config.internal_retention_days)
            if self.config.bjobs_path != DEFAULT_BJOBS_PATH:
                # 조회는 콜백으로 가므로 이 경로는 아무 데도 안 쓰인다.
                # 앱이 mock bjobs를 가리켜 놓고 "왜 안 불리지" 하는 것을 막는다.
                log.warning(
                    "bjobs_path=%r는 무시됩니다 — job_status_fetcher가 "
                    "지정되어 상태 조회는 콜백으로 합니다",
                    self.config.bjobs_path)

    #: bkill target 1건당 이 정도 예산도 없으면 경고한다. 근거: bkill은 job마다
    #: mbatchd 왕복이고 MC면 원격 클러스터 왕복까지 더해진다 — 로컬 단일
    #: 클러스터도 job당 수~수십 ms, MC면 수백 ms까지 간다. 기본 설정
    #: (120s / 100건 = 1.2s)은 넉넉히 통과한다.
    _KILL_BUDGET_WARN_S = 0.1

    def _warn_if_kill_budget_is_tight(self) -> None:
        """kill_timeout_s가 **호출 1회**(= chunk 전체)의 상한이라는 점을
        생성 시 1회 알린다.

        이 값을 'job 하나를 죽이는 데 걸리는 시간'으로 읽으면 8초 같은 값을
        주게 되는데, 실제로는 그 8초 안에 chunk(기본 100건)를 **전부** 끝내야
        한다. 못 끝내면 subprocess timeout이 bkill 클라이언트를 중간에 죽여
        앞쪽 id만 죽고 뒤쪽은 요청조차 안 나간 채 잘리고, 그 상태가 매 kill마다
        반복된다. 조용히 두면 "bkill timeout" 경고만 계속 보이고 원인이
        설정이라는 것을 알 길이 없다."""
        budget = self.config.kill_timeout_s / max(1, self.config.kill_chunk_size)
        if budget >= self._KILL_BUDGET_WARN_S:
            return
        log.warning(
            "kill_timeout_s=%.0fs는 bkill **호출 1회**(target %d건 전체)의 "
            "상한입니다 — target당 %.0fms 예산이라 시간 내 못 끝내고 중간에 "
            "잘릴 가능성이 큽니다. kill_chunk_size를 줄이거나(예: %d) "
            "kill_timeout_s를 늘리세요.",
            self.config.kill_timeout_s, self.config.kill_chunk_size,
            budget * 1000.0,
            max(1, int(self.config.kill_timeout_s / self._KILL_BUDGET_WARN_S)))

    @property
    def internal_status(self) -> Optional[InternalStatusSource]:
        """콜백 조회원 (job_status_fetcher 미지정이면 None) — 테스트/진단용."""
        return self._internal

    def note_poll_interval(self, interval_s: float) -> None:
        """실제 폴링 주기 통지 — 콜백 조회원의 갱신 간격을 그에 맞춘다.
        bjobs 경로면 아무 일도 하지 않는다."""
        if self._internal is not None:
            self._internal.note_poll_interval(interval_s)

    def forget_status(self, job_ids: Sequence[int]) -> None:
        """추적 종료(레코드 삭제) 통지 — 콜백 조회원의 원장에서 버린다.
        bjobs 경로면 no-op(누적 원장이 없다)."""
        if self._internal is not None:
            self._internal.forget(job_ids)

    def shutdown_bkill_pool(self) -> None:
        """bkill 실행 풀 종료 — **killer를 join한 뒤에** 부른다(멱등).
        먼저 닫으면 진행 중 kill이 RuntimeError로 무산된다."""
        pool, self._bkill_pool = self._bkill_pool, None
        if pool is not None:
            pool.shutdown(wait=True)

    def shutdown_status_source(self) -> None:
        """콜백 조회원 종료 — 대기 중인 폴링/verify 스레드를 즉시 풀어 준다.
        (bjobs 경로면 no-op)"""
        if self._internal is not None:
            self._internal.shutdown()

    @property
    def _bjobs_fmt(self) -> str:
        return self._bjobs_formats[self._bjobs_fmt_idx]

    @staticmethod
    def _prog_len(path) -> int:
        """chunk_args의 base_len 예약치 — wrapper(다중 토큰)의 총 길이."""
        return sum(len(t) + 1 for t in cmd_tokens(path))

    def _run(self, argv: Sequence[str], timeout: float,
             cwd: Optional[str] = None) -> CommandResult:
        """**모든** LSF subprocess(wrapper 제출/bjobs/bkill)가
        지나는 단일 실행 funnel. 여기서 DEBUG 로깅을 한다 — 실제로 어떤
        명령이 어느 스레드에서 어떤 cwd로 실행되고 얼마나 걸려 무슨 결과가
        나왔는지 추적할 수 있다.

        **thread safety**: 표준 logging 모듈은 핸들러 내부 락으로 스레드 안전
        하므로 여러 submit/kill worker가 동시에 debug를 찍어도 출력이 섞여
        깨지지 않는다. 여기서는 지역 변수(스레드명·monotonic 시각)만 쓰고 공유
        가변 상태를 두지 않아 추가 경합원이 없다. 스레드명을 메시지에 직접 넣어
        (포매터 %(threadName)s 설정과 무관하게) 동시 실행을 구분한다.

        활성화: `logging.getLogger("lsfmgr.command").setLevel(logging.DEBUG)`
        (또는 상위 "lsfmgr")로 레벨을 낮추고 핸들러를 붙이면 이 로그가 나온다.
        cwd는 제출 경로만 넘긴다(bjobs/bkill은 None → 부모 cwd)."""
        tname = threading.current_thread().name
        prog = argv[0].rsplit("/", 1)[-1] if argv else "?"   # 실행 프로그램 그대로
        log.debug("[%s] exec %s: %s (cwd=%s, timeout=%.1fs)",
                  tname, prog, " ".join(map(str, argv)), cwd, timeout)
        t0 = time.monotonic()
        try:
            res = self.runner(argv, timeout, cwd)
        except Exception as e:               # noqa: BLE001 — 로깅 후 그대로 전파
            log.debug("[%s] exec %s 실패 (%.3fs): %r",
                      tname, prog, time.monotonic() - t0, e)
            raise
        log.debug("[%s] exec %s → rc=%d (%.3fs) stdout=%r stderr=%r",
                  tname, prog, res.returncode, time.monotonic() - t0,
                  res.stdout[:500], res.stderr[:500])
        return res

    # ------------------------------------------------------------------
    # 제출 — wrapper 커맨드를 '그대로' 실행하고 'Job <id>' 만 파싱
    # (v10: bsub 인자 조립 경로(bsub()/-q/-J/-g)는 삭제 — 제출은 전부
    #  wrapper 경유. lsfmgr는 어떤 제출 인자도 조립하지 않는다.)
    # ------------------------------------------------------------------
    def _apply_wrapper_pattern(self, argv: Sequence[str]) -> List[str]:
        """test_submit_wrapper_pattern_cmd 적용 — argv[0]의 basename이 패턴에 맞으면
        **프로그램 토큰만** 대체하고 나머지 인자는 그대로 둔다. 규칙이 없으면
        (기본) 첫 줄에서 argv 그대로 빠져나간다.

        basename으로 맞춘다 — 커맨드가 경로째로 와도(/prod/bin/mytool_sub)
        같은 규칙("*_sub")이 걸리게. 대소문자는 항상 구분한다(fnmatchcase) —
        fnmatch는 OS별로 대소문자 정책이 달라 같은 설정이 환경 따라 다르게
        동작한다."""
        rule = self.config.test_submit_wrapper_pattern_cmd
        if rule is None or not argv:
            return list(argv)
        pattern, cmd = rule
        prog = argv[0].rsplit("/", 1)[-1]
        if not fnmatch.fnmatchcase(prog, pattern):
            return list(argv)
        new_argv = cmd_tokens(cmd) + list(argv[1:])
        log.debug("wrapper 치환(패턴 %s): %s → %s",
                  pattern, argv[0], new_argv[0])
        return new_argv

    def run_submit(self, argv: Sequence[str],
                   timeout_s: Optional[float] = None,
                   cwd: Optional[str] = None) -> int:
        """wrapper 커맨드(argv)를 조립 없이 그대로 실행하고 'Job <id>' 파싱.
        cwd 지정 시 그 디렉토리에서 실행한다(wrapper→bsub가 그 cwd를 상속).

        test_submit_wrapper_pattern_cmd가 설정돼 있으면 실행 직전에 프로그램(argv[0])만
        치환한다 — 레코드의 command는 원본 그대로다(표시·재제출 기준 유지).

        lsfmgr 가 -q/-J/-g 등을 붙이지 않는다 — argv 전체가 사용자가 준 wrapper
        커맨드(예: ["customwrapper_sub", "-i", "a.sp"])다. 실패 분류:
          - rc != 0            → BSUB_EXIT_<rc>   (재시도 O — 일시적 오류 가정)
          - timeout            → BSUB_TIMEOUT     (재시도 X — 중복 제출 위험)
          - 'Job <id>' 없음    → NO_JOBID_PARSED  (재시도 X — 이미 제출됐을 수 있음)
        """
        to = timeout_s if timeout_s is not None else self.config.submit_timeout_s
        try:
            res = self._run(self._apply_wrapper_pattern(argv), to, cwd=cwd)
        except subprocess.TimeoutExpired:
            raise SubmitError("wrapper timeout", fail_reason="BSUB_TIMEOUT",
                              retryable=False)
        except OSError as e:
            # cwd 부재/비디렉토리 등 exec 이전 실패 — 분류된 실패로 마무리
            raise SubmitError(f"wrapper 실행 실패(작업 디렉토리 확인): {e}",
                              fail_reason="BSUB_OSERROR", retryable=False)
        if res.returncode != 0:
            raise SubmitError(
                f"wrapper exit {res.returncode}: {res.stderr.strip()[:200]}",
                fail_reason=f"BSUB_EXIT_{res.returncode}",
                returncode=res.returncode, stderr=res.stderr,
                stdout=res.stdout, retryable=True)
        m = _JOB_ID_RE.search(res.stdout)
        if not m:
            raise SubmitError(
                f"job id 파싱 실패: {res.stdout.strip()[:200]}",
                fail_reason="NO_JOBID_PARSED", stderr=res.stderr,
                stdout=res.stdout, retryable=False)
        return int(m.group(1))

    # ------------------------------------------------------------------
    # bjobs — 조회 (전략별 변형)
    # ------------------------------------------------------------------
    # CORE: 상태 추적의 최소 단위 필수 3필드.
    # FULL: CORE + 실행시간/위치 확장 필드. 사이트가 확장 필드를 거부하면
    #       bjobs 전체가 rc≠0로 죽는다 — 그러면 폴링이 아무 상태도 못 걷어
    #       job이 PEND(제출 직후 상태)에 고착된다. 그래서 필드 오류로 실패하면
    #       한 단계씩 자동 강등한다(그 필드만 포기).
    # FULL_MC: FULL + MultiCluster forwarding 필드. collect_clusters=True일 때만
    #       맨 앞 단계로 쓰고, 미지원 사이트면 FULL로 강등돼 run_time 등은 유지된다.
    # v10.2: -json → -noheader + delimiter=';' 복귀 (사용자 결정). 폭 지정은
    # 두지 않는다 — LSF는 폭 미지정 시 필드별 **기본 폭으로 truncation**할 수
    # 있으므로, 잘림이 관측되면 그 필드에만 폭을 준다.
    _DELIM = "delimiter=';'"
    # job_name은 요청하지 않는다 — 파서가 쓰지 않고, 이 필드를 넣으면 조회
    # 결과가 통째로 비는 사이트가 있다(실환경 관측). 되살리지 말 것.
    # exec_cwd도 요청하지 않는다 (v10.4) — 작업 디렉토리는 제출 요청값
    # JobRecord.submit_cwd로 본다. 되살리지 말 것
    # (회귀 가드: tests/test_bjobs_no_exec_cwd.py).
    _CORE_FIELDS = "jobid stat exit_code"
    _FULL_FIELDS = _CORE_FIELDS + " run_time start_time finish_time"
    _BJOBS_CORE_FMT = f"{_CORE_FIELDS} {_DELIM}"
    _BJOBS_FULL_FMT = f"{_FULL_FIELDS} {_DELIM}"
    _BJOBS_FULL_MC_FMT = (f"{_FULL_FIELDS} source_cluster forward_cluster "
                          f"{_DELIM}")

    def _bjobs(self, selector: List[str]) -> List[JobStatus]:
        # -a를 붙이지 않는다 — explicit job id를 주면 LSF는 -a 없이도
        # CLEAN_PERIOD 내 종료 job을 보여준다. CLEAN_PERIOD 밖(purge)만
        # LOST 판정으로 넘어간다. (v10: 조회는 id 기반뿐 — group/name
        # 조회는 제거됐다. 되살릴 때는 -a 오염 문제를 다시 고려할 것.)
        def run(fmt: str) -> CommandResult:
            argv = cmd_tokens(self.config.bjobs_path) + [
                "-noheader", "-o", fmt] + selector
            return self._run_query(argv)

        # 확장 필드/옵션 오류로 보이면 다음 포맷 단계로 영구 강등 후 재시도한다.
        # 강등 후 재시도가 또 필드 오류면 계속 내려간다(FULL+MC → FULL → CORE) —
        # 한 호출에서 지원 가능한 단계까지 도달해, MC·run_time을 둘 다 거부하는
        # 사이트도 즉시 살아난다. 일시 장애(필드 오류 아님)는 강등 없이 전파.
        while True:
            used_idx = self._bjobs_fmt_idx
            try:
                res = run(self._bjobs_formats[used_idx])
                break
            except LsfCommandError as e:
                if (used_idx < len(self._bjobs_formats) - 1
                        and _looks_like_field_error(e.stderr or "")):
                    # e.stderr만 본다 — 필드 오류는 항상 LSF stderr로 온다
                    # (_run_or_nomatch가 실어줌). str(e) 폴백은 stderr 없는
                    # 예외 메시지의 우연한 단어로 오판 강등할 수 있어 금지.
                    with self._bjobs_fmt_lock:      # CAS — 동시 강등 1단만
                        if self._bjobs_fmt_idx == used_idx:
                            self._bjobs_fmt_idx = used_idx + 1
                            log.warning(
                                "bjobs -o 확장 필드 미지원 — 포맷 강등 (→ %s). "
                                "원인: %s", self._bjobs_fmt,
                                (e.stderr or str(e)).strip()[:200])
                    continue
                raise
        return self._parse_bjobs(res.stdout)

    def bjobs_by_ids(self, job_ids: Sequence[int], *, fresh: bool = False
                     ) -> Tuple[List[JobStatus], Set[int]]:
        """job_id 목록 chunked 조회 — 유일한 bjobs 조회 수단 (v10에서
        group/name 조회 제거 — wrapper 제출 job은 부착물로 커버되지 않아
        id chunk가 전 경로를 균일하게 덮는다).

        반환: (조회 성공분, 조회 실패한 chunk의 job_id 집합) — monitor와
        동일한 chunk 단위 실패 격리. caller는 실패 집합의 job만 판단을
        보류하고, 성공 chunk에서 미발견된 job은 부재로 확정할 수 있다.

        fresh=True는 콜백 조회원에서만 의미가 있다 — 스냅샷 캐시를 건너뛰고
        이 호출 이후에 받은 결과만 쓴다(kill verify용). bjobs 경로는 매번
        subprocess를 돌리므로 항상 fresh라 무시된다."""
        if self._internal is not None:
            return self._internal.statuses_by_ids(job_ids, fresh=fresh)
        out: List[JobStatus] = []
        ids = [str(i) for i in job_ids]
        failed = self._query_chunks_isolated(
            ids, self._bjobs_base_len(),
            lambda chunk: out.extend(self._bjobs(chunk)), "bjobs")
        return out, failed

    def _bjobs_base_len(self) -> int:
        """bjobs chunk의 base_len 예약치 — 프로그램 토큰 + 옵션
        (-noheader -o <fmt>) 여유분."""
        return self._prog_len(self.config.bjobs_path) + 40

    def _query_chunks_isolated(self, ids: List[str], base: int,
                               run_chunk: Callable[[List[str]], None],
                               what: str) -> Set[int]:
        """chunked 조회 공통 골격 — chunk 단위 실패 격리 + 연속 실패 회로 차단.

        run_chunk(chunk)가 LsfCommandError를 던지면 그 chunk의 job_id만
        실패로 귀속하고 다음 chunk를 계속한다. 연속 2회 실패는 특정 chunk가
        아니라 조회 수단 자체의 전면 장애로 본다(데몬 hang이면 chunk마다
        timeout까지 기다린다) — 남은 chunk를 호출 없이 실패 처리하고
        중단한다. 격리(1개 chunk 실패는 계속)와 fail-fast(전면 장애에
        chunk 수 × timeout 직렬 블록 방지)를 양립시키는 회로 차단.
        반환: 조회 실패로 귀속된 job_id 집합."""
        failed: Set[int] = set()
        chunks = list(chunk_args(ids, self.config.chunk_size,
                                 self.config.arg_max, base))
        consecutive = 0
        for i, chunk in enumerate(chunks):
            try:
                run_chunk(chunk)
            except LsfCommandError as e:
                log.warning("조회 실패(%s): %s", what, e)
                failed.update(int(x) for x in chunk)
                consecutive += 1
                if consecutive >= 2 and i + 1 < len(chunks):
                    log.warning("%s 연속 %d회 실패 — 남은 %d개 chunk 조회 "
                                "중단(전면 장애로 간주)", what, consecutive,
                                len(chunks) - i - 1)
                    for rest in chunks[i + 1:]:
                        failed.update(int(x) for x in rest)
                    break
                continue
            consecutive = 0
        return failed

    def _run_or_nomatch(self, argv: List[str],
                        timeout: float) -> CommandResult:
        """실행 후 결과 반환. '매칭 job 없음'은 **장애가 아니라 정상 결과**로
        보고 그대로 돌려준다 — timeout/그 외 비정상 종료만 LsfCommandError.

        핵심: 한 chunk에 purge된 id가 하나라도 섞이면 LSF는 rc≠0 +
        `Job <id>: No matching job found`를 내면서도 **stdout에는 찾은 job의
        행을 그대로 출력**한다. 이 결과를 버리면 살아있는 job이 '부재'로
        오인돼 통째로 LOST 확정된다(실환경 관측 버그) — 반드시 stdout을
        파싱해야 한다. 없는 id는 행이 없으니 자연히 미발견으로 남는다."""
        try:
            res = self._run(argv, timeout)
        except subprocess.TimeoutExpired:
            raise LsfCommandError(f"{argv[0]} timeout")
        if res.returncode != 0:
            msg = (res.stderr + res.stdout).lower()
            if not any(p in msg for p in _NO_JOB_PATTERNS):
                raise LsfCommandError(
                    f"{argv[0]} exit {res.returncode}: "
                    f"{res.stderr.strip()[:200]}",
                    returncode=res.returncode, stderr=res.stderr)
        return res

    def _run_query(self, argv: List[str]) -> CommandResult:
        return self._run_or_nomatch(argv, self.config.query_timeout_s)

    @staticmethod
    def _parse_bjobs(stdout: str) -> List[JobStatus]:
        """bjobs delimiter(';') 출력 파싱 (v10.2: -json에서 복귀).

        필드 순서는 _BJOBS_*_FMT 정의 순서 그대로다. 확장 필드는 **필드 수가
        정확히 포맷과 맞을 때만** 신뢰한다 — 3=CORE, 6=FULL, 8=FULL+MC. 그 외
        (구형 열 누락, 값에 ';' 혼입으로 필드 밀림)는 오염을 피해 확장
        필드를 버린다. 파싱 불가 행은 그 행만 버린다 — 부재 확정은
        호출자(monitor의 LOST 판정)의 몫이다."""
        out: List[JobStatus] = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 3:
                log.debug("bjobs 파싱 불가 행 무시: %r", line)
                continue
            m = _ARRAY_ID_RE.match(parts[0])
            if not m:
                log.debug("bjobs job id 파싱 불가: %r", parts[0])
                continue
            state = LSF_STAT_MAP.get(parts[1])
            if state is None:
                log.debug("알 수 없는 LSF 상태 %r → UNKWN", parts[1])
                state = JobState.UNKWN
            exit_code = None
            if parts[2] not in ("", "-"):
                try:
                    exit_code = int(parts[2])
                except ValueError:
                    pass
            run_time_s = start_time = finish_time = None
            source_cluster = forward_cluster = None
            if len(parts) in (6, 8):
                run_time_s = _parse_run_time(parts[3])
                start_time = _parse_lsf_time(parts[4])
                # RUN 중 finish_time은 예상치(estimated)일 수 있다 —
                # 실측만 저장하도록 종료 상태에서만 채운다
                if state in (JobState.DONE, JobState.EXIT):
                    finish_time = _parse_lsf_time(parts[5])
                if len(parts) == 8:       # MultiCluster forwarding
                    source_cluster = _clean_field(parts[6])
                    forward_cluster = _clean_field(parts[7])
            elif len(parts) != 3:
                log.debug("bjobs 필드 수 이상(%d) — 확장 필드 무시: %r",
                          len(parts), line)
            out.append(JobStatus(
                job_id=int(m.group(1)),
                array_index=int(m.group(2)) if m.group(2) else None,
                state=state, exit_code=exit_code,
                run_time_s=run_time_s, start_time=start_time,
                finish_time=finish_time,
                source_cluster=source_cluster,
                forward_cluster=forward_cluster))
        return out

    # ------------------------------------------------------------------
    # bkill — id chunk 단독 (v10: group/name/array tier 삭제 — 부착물이
    # 더 이상 생성되지 않으므로 전략 자체가 성립하지 않는다)
    # ------------------------------------------------------------------
    def _bkill_argv(self, chunk: Sequence[str]) -> List[str]:
        """bkill 실행 argv — shell 미경유라 array target("1000[2]")의 대괄호가
        globbing으로 뭉개질 일이 없다.
        (v10.6: MC 분류 kill 삭제 — cluster env를 source한 tcsh 경로가 사라져
        shell 경유가 없어졌다. 되살릴 때는 set noglob을 다시 고려할 것.)"""
        return cmd_tokens(self.config.bkill_path) + list(chunk)

    def _bkill_base_len(self) -> int:
        return self._prog_len(self.config.bkill_path) + 10

    def bkill_targets_confirm(self, targets: Sequence[str],
                              on_progress: Optional[Callable[[int], None]] = None
                              ) -> Tuple[Set[str], int, Set[str]]:
        """chunked bkill + 출력 확인 파싱.

        반환: (해소된 target 집합, LSF 호출 횟수, **시간 내 반환하지 않은**
        chunk의 target 집합).
        '해소'는 더 이상 kill이 필요 없다고 확인된 것 — 'Job <id> is being
        terminated'(신호 수락) 또는 already-finished/no-matching(이미 없음).
        해소 안 된 target(일시 장애 등)은 호출자가 재시도한다.

        timeout은 **따로 돌려준다**. 그 chunk는 '안 죽었다'가 아니라 '모른다'
        이기 때문이다 — subprocess timeout은 bkill **클라이언트**를 죽일 뿐,
        그때까지 mbatchd에 접수된 요청은 그대로 처리된다(대량 chunk면 앞쪽
        id는 이미 죽고 뒤쪽만 안 나간 상태로 잘린다). 호출자는 이 집합을
        무턱대고 재-bkill하지 말고 조회로 생사를 확인해야 한다.
        on_progress(누적_처리_target수)는 chunk 완료마다 호출된다(진행 통지).

        chunk는 kill_workers개까지 **동시에** 실행한다(기본 1=직렬). bkill은
        MC 사이트에서 원격 왕복을 기다리는 지연 지배적 작업이라 병렬이 크게
        먹히지만, 동시에 mbatchd에 붙는 요청이 kill_workers x kill_chunk_size
        건이 되므로 사이트가 실측하고 켜는 값이다."""
        chunks = list(chunk_args(list(targets), self.config.kill_chunk_size,
                                 self.config.arg_max, self._bkill_base_len()))
        if not chunks:
            return set(), 0, set()

        def run_chunk(chunk: List[str]):
            """[worker] chunk 1건 실행 → (해소된 target, timeout난 target).

            **공유 상태를 건드리지 않는다** — 자기 결과만 돌려주고 집계는
            호출 스레드가 한다. 병렬 chunk가 공유 집합에 쓰면 lock이 필요하고,
            그 lock 안에서 진행 통지까지 하면 Qt 신호가 여기서 나가게 된다
            (아래 참고)."""
            argv = self._bkill_argv(chunk)
            try:
                res = self._run(argv, self.config.kill_timeout_s)
            except subprocess.TimeoutExpired:
                # 죽었는지 **모르는** 상태다 (위 docstring). 재조회로 판정한다.
                log.warning(
                    "bkill이 %.0fs 안에 반환하지 않아 중단했습니다 (%d건) — "
                    "접수된 요청은 살아 있을 수 있어 조회로 확인합니다. "
                    "자주 나오면 kill_chunk_size를 줄이거나 "
                    "kill_timeout_s를 늘리세요: %s",
                    self.config.kill_timeout_s, len(chunk), chunk[:20])
                return set(), set(chunk)
            return _parse_bkill_resolved(
                res.stdout + "\n" + res.stderr, set(chunk)), set()

        resolved: Set[str] = set()
        timed_out: Set[str] = set()
        processed = 0
        pool = self._bkill_pool
        if pool is None or len(chunks) == 1:
            done_iter = ((run_chunk(c), len(c)) for c in chunks)
            for (got, late), n in done_iter:
                resolved |= got
                timed_out |= late
                processed += n
                if on_progress:
                    on_progress(processed)
        else:
            # 집계·진행 통지는 **호출 스레드**에서 한다(as_completed).
            # ① Qt 신호(kill_progress)를 Qt가 모르는 순수 파이썬 스레드에서
            #    쏘지 않는다 — 이 라이브러리의 다른 발화 지점은 전부 main이나
            #    QThread(Pool) 위다. 여기만 예외를 만들 이유가 없다.
            # ② 집계가 단일 스레드라 공유 집합용 lock이 아예 필요 없고,
            #    진행 누적이 자연히 단조증가한다(진행바 되감김 불가).
            # 풀은 **공용**이라 동시에 도는 kill이 몇 건이든 bkill 총수가
            # kill_workers를 넘지 않는다(자기 future만 기다리므로 남의 kill을
            # 기다리지는 않는다).
            pending = {pool.submit(run_chunk, c): len(c) for c in chunks}
            for fut in as_completed(pending):
                got, late = fut.result()
                resolved |= got
                timed_out |= late
                processed += pending[fut]
                if on_progress:
                    on_progress(processed)
        return resolved, len(chunks), timed_out
