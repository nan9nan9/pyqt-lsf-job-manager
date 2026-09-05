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
from .internal_status import FetcherState, InternalStatusSource
from .states import LSF_STAT_MAP, JobState, JobStatus  # noqa: F401
# 기존 import 경로 호환을 위해 JobStatus를 재수출한다.

log = logging.getLogger("lsfmgr.command")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


# runner: (argv, timeout_s, cwd) → CommandResult. cwd=None은 부모 작업 디렉토리.
# cwd는 위치/키워드 인자로 받으며, 구 2인자 runner는 cwd를 무시한다.
# 제출·kill·조회에서 동시에 호출되므로 runner는 thread-safe해야 한다.
Runner = Callable[[Sequence[str], float, Optional[str]], CommandResult]


def default_runner(argv: Sequence[str], timeout: float,
                   cwd: Optional[str] = None) -> CommandResult:
    """기본 runner — subprocess.run (shell 미경유).
    cwd 지정 시 그 디렉토리에서 실행한다 — 자식 프로세스에만 적용돼
    동시 제출 worker 간 경합이 없다(os.chdir 같은 프로세스 전역 변경 금지)."""
    proc = subprocess.run(
        list(argv), capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


class _QueryRunner:
    """조회 subprocess 수명 관리. 종료와 프로세스 생성을 같은 락으로 묶는다.

    shutdown은 실행 중 클라이언트를 kill하고, 호출 스레드가 communicate/
    wait로 회수한다. 제출·bkill 프로세스는 이 취소 범위에 포함하지 않는다.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._closed = False
        self._processes: set = set()

    def __call__(self, argv, timeout, cwd=None) -> CommandResult:
        with self._lock:
            if self._closed:
                raise LsfCommandError("상태 조회원이 종료되었습니다")
            proc = subprocess.Popen(list(argv), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, cwd=cwd)
            self._processes.add(proc)
        try:
            with proc:
                try:
                    stdout, stderr = proc.communicate(timeout=timeout)
                except BaseException:
                    proc.kill()
                    proc.wait()
                    raise
                return CommandResult(proc.returncode, stdout, stderr)
        finally:
            with self._lock:
                self._processes.discard(proc)

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
            for proc in self._processes:
                proc.kill()


def _adapt_runner(runner: Runner) -> Runner:
    """cwd 인자가 추가되기 전(구 2-arg (argv, timeout)) runner를 하위호환으로
    감싼다 — cwd를 못 받는 runner면 cwd를 무시하는 어댑터로 래핑해, 계약 확장이
    기존 주입 runner를 깨지 않게 한다(cwd 미지원 runner는 work_dir을 못 지키지만
    TypeError로 죽는 대신 조용히 부모 cwd에서 실행된다)."""
    try:
        signature = inspect.signature(runner)
    except (ValueError, TypeError):
        return runner            # 시그니처 조사 불가(빌트인 등) — 그대로 3-arg 시도
    try:
        signature.bind([], 1.0, None)
    except TypeError:
        try:
            signature.bind([], 1.0, cwd=None)
        except TypeError:
            signature.bind([], 1.0)
            return lambda argv, timeout, cwd=None: runner(argv, timeout)
        return lambda argv, timeout, cwd=None: runner(argv, timeout, cwd=cwd)
    return runner


_JOB_ID_RE = re.compile(r"Job <(\d+)>")
_ARRAY_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?$")

# kill/verify 공통 target 문법: "id" / "id[idx]" / "id[m-n]".
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
_LSF_TIME_FUTURE_TOLERANCE = timedelta(days=1)


# 확장 필드/옵션 오류만 포맷 강등 대상으로 삼는다.
# 일시 장애를 오인하지 않도록 "unknown" 같은 포괄적인 단독 단어는 제외한다.
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
# kill 신호 수락 — EXIT/killed 마킹 근거.
_BKILL_ACCEPTED_MSGS = (
    "is being terminated", "is being signaled", "is being requeued",
    "is being killed", "in progress of being terminated",
)
# 이미 없음/끝남 — 재시도는 불필요하지만 이 kill이 끝낸 것은 아니다.
_BKILL_GONE_MSGS = (
    "already finished", "has already", "no matching job", "is not found",
    "no unfinished job", "not found",
)


def _parse_bkill_resolved(text: str, requested: "set[str]"
                          ) -> "Tuple[set[str], set[str]]":
    """bkill stdout/stderr 파싱 → (해소된 target, 그중 kill 신호가 수락된 target).

    해소 = 재시도 불필요: 신호 수락 또는 이미 없음/끝남. 미해소(일시 장애 등)는
    호출자가 재시도한다. 이미 끝났다는 응답은 **수락이 아니다** — 그 job의
    종료 상태는 조회로 반영해야지 EXIT/killed로 덮으면 정상 완료가 실패로
    기록된다(kill 시점에 Store가 아직 PEND/RUN인 경우).

    requested는 이번 chunk에 **실제로 넘긴** target 집합이다 — element 응답
    행에서 bare 부모 id를 유도할지 말지가 여기에 달렸다. 부모 전체를 요청한
    경우에만 element 응답을 부모 판정에 집계하며, element 하나라도 미해소면
    부모는 재시도 대상으로 남고(stdout/stderr 순서 무관), element 하나라도
    수락됐으면 부모도 수락이다(접힌 부모 레코드는 element 전체의 합). 수락은
    해소와 별개로 돌려준다 — 수락됐지만 미해소인 부모는 재시도 뒤 해소될 때
    수락 이력이 살아 있어야 한다."""
    resolved, accepted, unresolved = set(), set(), set()
    for line in text.splitlines():
        m = _BKILL_LINE_RE.search(line)
        if not m:
            continue
        jid, msg = m.group(1), m.group(2).lower()
        if any(p in msg for p in _BKILL_ACCEPTED_MSGS):
            buckets = (resolved, accepted)
        elif any(p in msg for p in _BKILL_GONE_MSGS):
            buckets = (resolved,)
        else:
            buckets = (unresolved,)
        ids = {jid}
        if "[" in jid:
            parent = jid.split("[", 1)[0]
            if parent in requested:
                ids.add(parent)
        for bucket in buckets:
            bucket |= ids
    resolved -= unresolved
    # accepted는 resolved와 교집합하지 않는다 — 이번 라운드에 부모가 형제의
    # 실패로 미해소여도 element 수락 이력은 남아야, 재시도에서 '이미 끝남'으로
    # 해소될 때 부모의 killed 근거가 되기 때문이다(호출자가 해소와 함께 판정).
    return resolved, accepted


def _parse_run_time(s: str) -> Optional[int]:
    """'120 second(s)' → 120. 미실행('-'/빈값)은 None."""
    s = s.strip()
    if not s or s == "-":
        return None
    m = _RUN_TIME_RE.search(s)
    return int(m.group(1)) if m else None


def _parse_lsf_time(s: str, now: Optional[datetime] = None) -> Optional[datetime]:
    """LSF 시각을 파싱한다. 예상 종료 시각은 관측값으로 사용하지 않는다.

    포맷 해석은 문자열·기준 연도로 캐시하지만, 연도 없는 시각을 현재 시각과
    비교하는 판단은 매번 수행한다. 같은 문자열도 기준 시각이 바뀌면 다른
    연도를 가리킬 수 있다.
    """
    if now is None:
        now = datetime.now()
    parsed = _parse_lsf_time_cached(s.strip(), now.year)
    if parsed is None:
        return None
    dt, yearless = parsed
    # 연말 경계: 올해로 해석한 시각이 하루보다 더 미래면 작년으로 되돌린다.
    if yearless and dt > now + _LSF_TIME_FUTURE_TOLERANCE:
        try:
            return dt.replace(year=dt.year - 1)
        except ValueError:
            return None                    # 2/29 → 비윤년 보정 불가
    return dt


@lru_cache(maxsize=65536)
def _parse_lsf_time_cached(s: str, year: int) -> Optional[Tuple[datetime, bool]]:
    """포맷 해석 결과와 연도 생략 여부. 현재 시각에 따른 연도 보정은 캐시 밖이다."""
    if s.endswith(" E"):
        return None                        # 예상값 — 실제 시각으로 저장 금지
    s = re.sub(r"\s+[A-Z]$", "", s).strip()   # 상태 접미(L/X 등) 제거
    if not s or s == "-":
        return None
    for fmt in _LSF_TIME_FORMATS:
        # 연도 없는 포맷은 기본연도 1900(비윤년)이라 "Feb 29" 파싱이 실패해
        # 시각이 통째로 소실된다 — 연도를 명시해 파싱한다(올해 → 불가 시 작년).
        attempts = ([(s, fmt)] if "%Y" in fmt
                    else [(f"{s} {year}", fmt + " %Y"),
                          (f"{s} {year - 1}", fmt + " %Y")])
        for text, f in attempts:
            try:
                dt = datetime.strptime(text, f)
            except ValueError:
                continue
            return dt, "%Y" not in fmt
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
        self._query_runner = (_QueryRunner()
                              if runner is None or runner is default_runner
                              else self.runner)
        self._status_shutdown = threading.Event()
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
        # manager 전체가 공용 풀을 사용해 동시 kill 호출도 같은 상한을 지킨다.
        # executor는 첫 작업 제출 전에는 스레드를 생성하지 않는다.
        self._bkill_pool: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(
            max_workers=self.config.kill_workers,
            thread_name_prefix="lsfmgr-bkill")
        self._warn_if_kill_budget_is_tight()
        # 조회원은 생성 시 선택하며 양쪽 모두 bjobs_by_ids 계약을 따른다.
        self._internal: Optional[InternalStatusSource] = None
        if self.config.job_status_fetcher is not None:
            self._internal = InternalStatusSource(
                self.config.job_status_fetcher,
                # 예비 콜백 — 주 콜백이 실패/미회수일 때 같은 조회를 재시도.
                failover=self.config.job_status_fetcher_failover,
                # None이면 소스가 자동 모드로 — 실제 폴링 주기를 알게 될 때
                # 그 절반으로 따라간다(note_poll_interval).
                refresh_min_s=self.config.internal_refresh_min_s,
                poll_interval_s=self.config.poll_interval_s,
                wait_timeout_s=self.config.query_timeout_s,
                retention_days=self.config.internal_retention_days,
                # run_time 갱신을 monitor가 버리는 설정이면 원장에서도
                # 만들지 않는다 (전수 스캔 비용을 통째로 없앤다).
                track_runtime=self.config.poll_runtime_updates)
            log.info("상태 조회원: job_status_fetcher 콜백 (bjobs 미사용, 최소 "
                     "갱신 간격 %.1fs%s, 종료 job 보존 %.0f일%s)",
                     self._internal.stats()["refresh_min_s"],
                     "" if self.config.internal_refresh_min_s is not None
                     else " — 실제 폴링 주기에 자동 추종",
                     self.config.internal_retention_days,
                     ", 예비 콜백 있음"
                     if self.config.job_status_fetcher_failover is not None
                     else "")
            if self.config.bjobs_path != DEFAULT_BJOBS_PATH:
                # 조회는 콜백으로 가므로 이 경로는 아무 데도 안 쓰인다.
                # 앱이 mock bjobs를 가리켜 놓고 "왜 안 불리지" 하는 것을 막는다.
                log.warning(
                    "bjobs_path=%r는 무시됩니다 — job_status_fetcher가 "
                    "지정되어 상태 조회는 콜백으로 합니다",
                    self.config.bjobs_path)

    #: bkill timeout을 target 수로 나눈 예산이 이 값보다 작으면 경고한다.
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

    def status_fetcher_state(self) -> Optional[FetcherState]:
        """[sync] 상태 조회 콜백의 건강 — bjobs 조회면 None."""
        return (None if self._internal is None
                else self._internal.fetcher_state())

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
        """새 조회를 막고 콜백 대기/기본 bjobs 클라이언트를 취소한다.

        주입 Runner는 강제로 중단하지 않는다. 그 호출의 반환·타임아웃까지
        polling/killer 종료 단계가 기다린다.
        """
        self._status_shutdown.set()
        if isinstance(self._query_runner, _QueryRunner):
            self._query_runner.shutdown()
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
             cwd: Optional[str] = None, *, query: bool = False) -> CommandResult:
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
            runner = self._query_runner if query else self.runner
            res = runner(argv, timeout, cwd)
        except Exception as e:               # noqa: BLE001 — 로깅 후 그대로 전파
            log.debug("[%s] exec %s 실패 (%.3fs): %r",
                      tname, prog, time.monotonic() - t0, e)
            raise
        log.debug("[%s] exec %s → rc=%d (%.3fs) stdout=%r stderr=%r",
                  tname, prog, res.returncode, time.monotonic() - t0,
                  res.stdout[:500], res.stderr[:500])
        return res

    # 제출: wrapper 인자를 조립하지 않고 실행한 뒤 Job <id>를 파싱한다.
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

    # bjobs 포맷: CORE(필수 필드), FULL(실행시간), FULL_MC(cluster 정보).
    # 필드 미지원 오류에만 FULL_MC → FULL → CORE 순서로 강등한다.
    # 출력은 -noheader와 세미콜론 구분자를 사용한다.
    _DELIM = "delimiter=';'"
    # job_name은 일부 사이트에서 조회 실패를 유발하므로 요청하지 않는다.
    # 작업 디렉토리는 exec_cwd 조회 대신 JobRecord.submit_cwd를 사용한다.
    _CORE_FIELDS = "jobid stat exit_code"
    _FULL_FIELDS = _CORE_FIELDS + " run_time start_time finish_time"
    _BJOBS_CORE_FMT = f"{_CORE_FIELDS} {_DELIM}"
    _BJOBS_FULL_FMT = f"{_FULL_FIELDS} {_DELIM}"
    _BJOBS_FULL_MC_FMT = (f"{_FULL_FIELDS} source_cluster forward_cluster "
                          f"{_DELIM}")

    def _bjobs(self, selector: List[str]) -> List[JobStatus]:
        # 명시적 job ID 조회는 -a 없이 CLEAN_PERIOD 내 종료 job을 포함한다.
        def run(fmt: str) -> CommandResult:
            argv = cmd_tokens(self.config.bjobs_path) + [
                "-noheader", "-o", fmt] + selector
            return self._run_query(argv)

        # 필드 오류에만 지원 가능한 포맷까지 강등하며, 일시 장애는 그대로 전파한다.
        while True:
            used_idx = self._bjobs_fmt_idx
            try:
                res = run(self._bjobs_formats[used_idx])
                break
            except LsfCommandError as e:
                if (used_idx < len(self._bjobs_formats) - 1
                        and _looks_like_field_error(e.stderr or "")):
                    # e.stderr만 본다 — 필드 오류는 항상 LSF stderr로 온다
                    # (_run_query가 실어줌). str(e) 폴백은 stderr 없는
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
        failed: Set[int] = set()
        chunks = list(chunk_args(ids, self.config.chunk_size,
                                 self.config.arg_max, self._bjobs_base_len()))
        consecutive = 0
        for i, chunk in enumerate(chunks):
            try:
                out.extend(self._bjobs(chunk))
            except LsfCommandError as e:
                log.warning("조회 실패(bjobs): %s", e)
                failed.update(int(x) for x in chunk)
                consecutive += 1
                if consecutive >= 2 and i + 1 < len(chunks):
                    log.warning("bjobs 연속 %d회 실패 — 남은 %d개 chunk 조회 "
                                "중단(전면 장애로 간주)", consecutive,
                                len(chunks) - i - 1)
                    for rest in chunks[i + 1:]:
                        failed.update(int(x) for x in rest)
                    break
                continue
            consecutive = 0
        return out, failed

    def _bjobs_base_len(self) -> int:
        """bjobs chunk의 base_len 예약치 — 프로그램 토큰 + 옵션
        (-noheader -o <fmt>) 여유분."""
        return self._prog_len(self.config.bjobs_path) + 40

    def _run_query(self, argv: List[str]) -> CommandResult:
        """실행 후 결과 반환. '매칭 job 없음'은 **장애가 아니라 정상 결과**로
        보고 그대로 돌려준다 — timeout/그 외 비정상 종료만 LsfCommandError.

        핵심: 한 chunk에 purge된 id가 하나라도 섞이면 LSF는 rc≠0 +
        `Job <id>: No matching job found`를 내면서도 **stdout에는 찾은 job의
        행을 그대로 출력**한다. 이 결과를 버리면 살아있는 job이 '부재'로
        오인돼 통째로 LOST 확정된다(실환경 관측 버그) — 반드시 stdout을
        파싱해야 한다. 없는 id는 행이 없으니 자연히 미발견으로 남는다."""
        if self._status_shutdown.is_set():
            raise LsfCommandError("상태 조회원이 종료되었습니다")
        try:
            res = self._run(argv, self.config.query_timeout_s, query=True)
        except subprocess.TimeoutExpired:
            raise LsfCommandError(f"{argv[0]} timeout")
        if self._status_shutdown.is_set():
            raise LsfCommandError("상태 조회원이 종료되었습니다")
        if res.returncode != 0:
            msg = (res.stderr + res.stdout).lower()
            if not any(p in msg for p in _NO_JOB_PATTERNS):
                raise LsfCommandError(
                    f"{argv[0]} exit {res.returncode}: "
                    f"{res.stderr.strip()[:200]}",
                    returncode=res.returncode, stderr=res.stderr)
        return res

    @staticmethod
    def _parse_bjobs(stdout: str) -> List[JobStatus]:
        """bjobs delimiter(';') 출력 파싱 (v10.2: -json에서 복귀).

        필드 순서는 _BJOBS_*_FMT 정의 순서 그대로다. 확장 필드는 **필드 수가
        정확히 포맷과 맞을 때만** 신뢰한다 — 3=CORE, 6=FULL, 8=FULL+MC. 그 외
        (구형 열 누락, 값에 ';' 혼입으로 필드 밀림)는 오염을 피해 확장
        필드를 버린다. 파싱 불가 행은 그 행만 버린다 — 부재 확정은
        호출자(monitor의 LOST 판정)의 몫이다."""
        out: List[JobStatus] = []
        now = datetime.now()                 # 한 응답의 모든 시각에 같은 기준을 적용한다.
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
                start_time = _parse_lsf_time(parts[4], now)
                # RUN 중 finish_time은 예상치(estimated)일 수 있다 —
                # 실측만 저장하도록 종료 상태에서만 채운다
                if state in (JobState.DONE, JobState.EXIT):
                    finish_time = _parse_lsf_time(parts[5], now)
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

    # bkill: 명시적 target을 chunk로 나누어 실행한다.
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
                              ) -> Tuple[Set[str], Set[str], int, Set[str]]:
        """chunked bkill + 출력 확인 파싱.

        반환: (해소된 target 집합, 그중 신호가 수락된 target 집합, LSF 호출
        횟수, **시간 내 반환하지 않은** chunk의 target 집합).
        '해소'는 더 이상 kill이 필요 없다고 확인된 것 — 'Job <id> is being
        terminated'(신호 수락) 또는 already-finished/no-matching(이미 없음).
        해소 안 된 target(일시 장애 등)은 호출자가 재시도한다. 수락된 것만
        "이 kill이 끝냈다"의 근거다(_parse_bkill_resolved).

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
            return set(), set(), 0, set()

        def run_chunk(chunk: List[str]):
            """[worker] chunk 1건 실행 → (해소, 수락, timeout난 target).

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
                return set(), set(), set(chunk)
            got, acc = _parse_bkill_resolved(
                res.stdout + "\n" + res.stderr, set(chunk))
            return got, acc, set()

        resolved: Set[str] = set()
        accepted: Set[str] = set()
        timed_out: Set[str] = set()
        processed = 0
        pool = self._bkill_pool
        if pool is None:
            raise LsfCommandError("bkill 실행 풀이 종료되었습니다")
        # 모든 chunk가 공용 풀을 거치므로 동시 호출도 같은 상한을 지킨다.
        # 집계와 진행 통지는 호출 스레드에서만 수행한다.
        pending = {pool.submit(run_chunk, c): len(c) for c in chunks}
        for fut in as_completed(pending):
            got, acc, late = fut.result()
            resolved |= got
            accepted |= acc
            timed_out |= late
            processed += pending[fut]
            if on_progress:
                on_progress(processed)
        return resolved, accepted, len(chunks), timed_out
