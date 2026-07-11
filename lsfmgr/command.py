"""LsfCommand — bsub/bjobs/bkill/bhist/bgdel subprocess 래퍼.

Qt 비의존 순수 Python (§8 원칙). shell 미경유, runner 주입으로 mock 테스트 가능
(NFR-8). chunking + ARG_MAX 검사 내장 (NFR-5).
"""
from __future__ import annotations

import logging
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple,
)

from .config import LsfConfig, cmd_tokens
from .errors import ArgMaxExceededError, LsfCommandError, SubmitError
from .states import LSF_STAT_MAP, JobState

log = logging.getLogger("lsfmgr.command")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


# runner 시그니처: (argv, timeout_s) -> CommandResult
Runner = Callable[[Sequence[str], float], CommandResult]


def default_runner(argv: Sequence[str], timeout: float) -> CommandResult:
    """기본 runner — subprocess.run (shell 미경유)."""
    proc = subprocess.run(
        list(argv), capture_output=True, text=True, timeout=timeout)
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


@dataclass(frozen=True)
class JobStatus:
    """bjobs 1행 파싱 결과."""
    job_id: int
    array_index: Optional[int]
    state: JobState
    exit_code: Optional[int]
    job_name: str
    run_time_s: Optional[int] = None       # LSF run_time(초)
    start_time: Optional[datetime] = None  # LSF start_time
    finish_time: Optional[datetime] = None # LSF finish_time
    working_dir: Optional[str] = None      # LSF exec_cwd (실행 디렉토리)
    source_cluster: Optional[str] = None   # MC: 제출(로컬) 클러스터
    forward_cluster: Optional[str] = None  # MC: 포워딩된 실행(원격) 클러스터


_JOB_ID_RE = re.compile(r"Job <(\d+)>")
_ARRAY_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?$")
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
_BJOBS_FIELD_ERR = ("run_time", "start_time", "finish_time", "exec_cwd",
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


def _parse_bkill_resolved(text: str) -> "set[str]":
    """bkill stdout/stderr에서 '해소된'(재시도 불필요) job id/target을 뽑는다.
    미해소(일시 장애 등)는 여기 안 들어가 호출자가 재시도한다."""
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
            if "[" in jid:
                resolved.add(jid.split("[", 1)[0])
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
    'E' 접미(estimated — RUN 중 예상 종료시각)는 실측이 아니므로 버린다."""
    s = s.strip()
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
    """인자 목록을 chunk_size 및 ARG_MAX(총 길이) 기준으로 분할 (NFR-5)."""
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
        self.runner = runner or default_runner
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

    @property
    def _bjobs_fmt(self) -> str:
        return self._bjobs_formats[self._bjobs_fmt_idx]

    @staticmethod
    def _prog_len(path) -> int:
        """chunk_args의 base_len 예약치 — wrapper(다중 토큰)의 총 길이."""
        return sum(len(t) + 1 for t in cmd_tokens(path))

    def _run(self, argv: Sequence[str], timeout: float) -> CommandResult:
        """runner 호출 + NFR-6 DEBUG 로깅 (명령 원문/stdout/stderr)."""
        log.debug("실행: %s", " ".join(argv))
        res = self.runner(argv, timeout)
        log.debug("rc=%d stdout=%r stderr=%r", res.returncode,
                  res.stdout[:500], res.stderr[:500])
        return res

    # ------------------------------------------------------------------
    # bsub
    # ------------------------------------------------------------------
    def bsub(self, command: str, *,
             queue: Optional[str] = None,
             job_name: Optional[str] = None,
             group_path: Optional[str] = None,
             resources: Optional[str] = None,
             outfile: Optional[str] = None,
             errfile: Optional[str] = None,
             extra_args: Sequence[str] = (),
             env: Optional[Sequence[Tuple[str, str]]] = None,
             timeout_s: Optional[float] = None) -> int:
        """bsub 실행 후 'Job <id>' 파싱하여 job_id 반환.

        실패 시 SubmitError(fail_reason=...) — FR-2.1 실패 분류:
        BSUB_TIMEOUT / BSUB_EXIT_<rc> / NO_JOBID_PARSED
        부착물(-g/-J) 관련 거부 시 부착물 없이 1회 재시도 (FR-1.4).
        timeout_s 미지정 시 config.submit_timeout_s.
        """
        argv = self._bsub_argv(command, queue=queue, job_name=job_name,
                               group_path=group_path, resources=resources,
                               outfile=outfile, errfile=errfile,
                               extra_args=extra_args, env=env)
        try:
            res = self._run(argv, timeout_s if timeout_s is not None
                            else self.config.submit_timeout_s)
        except subprocess.TimeoutExpired:
            raise SubmitError("bsub timeout", fail_reason="BSUB_TIMEOUT")

        if res.returncode != 0:
            # group 지정이 원인으로 보이면 group만 빼고 재시도 (FR-1.4).
            # job_name은 유지 — name 패턴 조회/손실 복구의 fallback 식별자.
            # group_path 없는 재시도에서는 이 분기에 다시 들어오지 않는다.
            if group_path and "group" in res.stderr.lower():
                log.warning("bsub group 거부 — LSF group 없이 재시도: %s",
                            res.stderr.strip())
                return self.bsub(command, queue=queue, job_name=job_name,
                                 resources=resources,
                                 outfile=outfile, errfile=errfile,
                                 extra_args=extra_args, env=env,
                                 timeout_s=timeout_s)
            raise SubmitError(
                f"bsub exit {res.returncode}: {res.stderr.strip()[:200]}",
                fail_reason=f"BSUB_EXIT_{res.returncode}",
                returncode=res.returncode, stderr=res.stderr,
                stdout=res.stdout)

        m = _JOB_ID_RE.search(res.stdout)
        if not m:
            raise SubmitError(
                f"job id 파싱 실패: {res.stdout.strip()[:200]}",
                fail_reason="NO_JOBID_PARSED", stderr=res.stderr,
                stdout=res.stdout)
        return int(m.group(1))

    def _bsub_argv(self, command: str, *, queue, job_name, group_path,
                   resources, outfile, errfile, extra_args,
                   env=None) -> List[str]:
        argv = cmd_tokens(self.config.bsub_path)   # wrapper면 여러 토큰
        q = queue if queue is not None else self.config.default_queue
        if q:
            argv += ["-q", q]
        if job_name:
            argv += ["-J", job_name]
        if group_path:
            argv += ["-g", group_path]
        if resources:
            argv += ["-R", resources]
        if outfile:
            argv += ["-o", outfile]
        if errfile:
            argv += ["-e", errfile]
        if env:
            # bsub -env "all, K=V, ..." — 기존 환경 유지 + 추가 변수
            pairs = ", ".join(f"{k}={v}" for k, v in env)
            argv += ["-env", f"all, {pairs}"]
        argv += list(extra_args)
        argv.append(command)
        total = sum(len(a) + 1 for a in argv)
        if total > self.config.arg_max:
            raise ArgMaxExceededError(
                f"bsub 인자 총 길이 {total} > ARG_MAX {self.config.arg_max}")
        return argv

    # ------------------------------------------------------------------
    # wrapper 제출 — wrapper 커맨드를 '그대로' 실행하고 job_id 만 파싱
    # ------------------------------------------------------------------
    def run_submit(self, argv: Sequence[str],
                   timeout_s: Optional[float] = None) -> int:
        """wrapper 커맨드(argv)를 조립 없이 그대로 실행하고 'Job <id>' 파싱.

        lsfmgr 가 -q/-J/-g 등을 붙이지 않는다 — argv 전체가 사용자가 준 wrapper
        커맨드(예: ["customwrapper_sub", "-i", "a.sp"])다. 실패 분류(FR-2.1):
          - rc != 0            → BSUB_EXIT_<rc>   (재시도 O — 일시적 오류 가정)
          - timeout            → BSUB_TIMEOUT     (재시도 X — 중복 제출 위험)
          - 'Job <id>' 없음    → NO_JOBID_PARSED  (재시도 X — 이미 제출됐을 수 있음)
        """
        to = timeout_s if timeout_s is not None else self.config.submit_timeout_s
        try:
            res = self._run(list(argv), to)
        except subprocess.TimeoutExpired:
            raise SubmitError("wrapper timeout", fail_reason="BSUB_TIMEOUT",
                              retryable=False)
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
    # bjobs — 조회 (FR-4.1 전략별 변형)
    # ------------------------------------------------------------------
    # 필드 폭 명시 필수 — LSF -o는 폭 미지정 시 기본 폭(JOBID 7자 등)으로
    # 잘라내므로("js_2026*") 긴 job name/array id의 파싱·매칭이 전멸한다
    # 필드명은 LSF -o 공식 명칭 사용 — job_name (jobname 은 실제 LSF 미지원).
    # 폭은 truncation 한도다 — delimiter 모드에선 패딩이 없어 출력량 증가는
    # 없으므로 긴 이름/경로가 잘리지 않게 넉넉히 잡는다.
    #
    # CORE: 어느 LSF 버전에서나 지원되는 필수 4필드 (상태 추적의 최소 단위).
    # FULL: CORE + 실행시간/위치 확장 필드. 구형 LSF(9.x 등)나 특정 사이트에서는
    #       run_time/exec_cwd 같은 필드를 -o가 거부해 bjobs 전체가 rc≠0로 죽는다 —
    #       그러면 폴링이 아무 상태도 못 걷어 job이 PEND(제출 직후 상태)에 고착된다.
    #       그래서 필드 오류로 실패하면 한 단계씩 자동 강등한다(그 필드만 포기).
    # FULL_MC: FULL + MultiCluster forwarding 필드. collect_clusters=True일 때만
    #       맨 앞 단계로 쓰고, 미지원 사이트면 FULL로 강등돼 run_time 등은 유지된다.
    _CORE_FIELDS = "jobid:20 stat:12 exit_code:12 job_name:512"
    _FULL_EXTRA = "run_time:25 start_time:30 finish_time:30 exec_cwd:2048"
    _CLUSTER_EXTRA = "source_cluster:60 forward_cluster:60"
    _DELIM = "delimiter=';'"
    _BJOBS_CORE_FMT = f"{_CORE_FIELDS} {_DELIM}"
    _BJOBS_FULL_FMT = f"{_CORE_FIELDS} {_FULL_EXTRA} {_DELIM}"
    _BJOBS_FULL_MC_FMT = f"{_CORE_FIELDS} {_FULL_EXTRA} {_CLUSTER_EXTRA} {_DELIM}"

    def _bjobs(self, selector: List[str]) -> List[JobStatus]:
        # -a를 붙이지 않는다. -a는 -g/-J(group/name) 조회에 과거 종료 job까지
        # 끌어와 by_name 풀을 오염시킨다 — job_id 없는 레코드/이름 재사용 시
        # 옛날 다른 job의 DONE/EXIT가 현재 레코드로 로드된다(ID 가드가
        # rec.job_id 있을 때만 동작). group/name 조회는 active(RUN/PEND 등)만
        # 반환하게 두고, 종료 상태는 leftover_ids의 explicit-ID 재조회로 잡는다
        # — explicit job id를 주면 LSF는 -a 없이도 CLEAN_PERIOD 내 종료 job을
        # 보여준다. CLEAN_PERIOD 밖(purge)만 bhist fallback으로 넘어간다.
        def run(fmt: str) -> Optional[CommandResult]:
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
                        and _looks_like_field_error(e.stderr or str(e))):
                    with self._bjobs_fmt_lock:      # CAS — 동시 강등 1단만
                        if self._bjobs_fmt_idx == used_idx:
                            self._bjobs_fmt_idx = used_idx + 1
                            log.warning(
                                "bjobs -o 확장 필드 미지원 — 포맷 강등 (→ %s). "
                                "원인: %s", self._bjobs_fmt,
                                (e.stderr or str(e)).strip()[:200])
                    continue
                raise
        if res is None:
            return []
        return self._parse_bjobs(res.stdout)

    def bjobs_by_group(self, group_path: str) -> List[JobStatus]:
        return self._bjobs(["-g", group_path])

    def bjobs_by_name(self, pattern: str) -> List[JobStatus]:
        return self._bjobs(["-J", pattern])

    def bjobs_by_ids(self, job_ids: Sequence[int]
                     ) -> Tuple[List[JobStatus], Set[int]]:
        """job_id 목록 chunked 조회 — 최후 수단 (graceful degradation).

        반환: (조회 성공분, 조회 실패한 chunk의 job_id 집합) — bhist_states와
        동일한 chunk 단위 실패 격리. caller는 실패 집합의 job만 판단을
        보류하고, 성공 chunk에서 미발견된 job은 부재로 확정할 수 있다."""
        out: List[JobStatus] = []
        ids = [str(i) for i in job_ids]
        base = self._prog_len(self.config.bjobs_path) + 40
        failed = self._query_chunks_isolated(
            ids, base, lambda chunk: out.extend(self._bjobs(chunk)), "bjobs")
        return out, failed

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
                        timeout: float) -> Optional[CommandResult]:
        """실행 후 결과 반환. '매칭 job 없음'은 None (정상 빈 결과)이고,
        timeout/비정상 종료는 LsfCommandError — 장애와 없음을 구분한다."""
        try:
            res = self._run(argv, timeout)
        except subprocess.TimeoutExpired:
            raise LsfCommandError(f"{argv[0]} timeout")
        if res.returncode != 0:
            msg = (res.stderr + res.stdout).lower()
            if any(p in msg for p in _NO_JOB_PATTERNS):
                return None
            raise LsfCommandError(
                f"{argv[0]} exit {res.returncode}: {res.stderr.strip()[:200]}",
                returncode=res.returncode, stderr=res.stderr)
        return res

    def _run_query(self, argv: List[str]) -> Optional[CommandResult]:
        return self._run_or_nomatch(argv, self.config.query_timeout_s)

    @staticmethod
    def _parse_bjobs(stdout: str) -> List[JobStatus]:
        out: List[JobStatus] = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 4:
                log.debug("bjobs 파싱 불가 행 무시: %r", line)
                continue
            jid_s, stat_s, exit_s, name = parts[0], parts[1], parts[2], parts[3]
            m = _ARRAY_ID_RE.match(jid_s)
            if not m:
                log.debug("bjobs job id 파싱 불가: %r", jid_s)
                continue
            state = LSF_STAT_MAP.get(stat_s)
            if state is None:
                log.debug("알 수 없는 LSF 상태 %r → UNKWN", stat_s)
                state = JobState.UNKWN
            exit_code = None
            if exit_s not in ("", "-"):
                try:
                    exit_code = int(exit_s)
                except ValueError:
                    pass
            # 확장 필드 — 필드 수가 정확히 포맷과 맞을 때만 신뢰한다. 8=FULL,
            # 10=FULL+MC(source/forward_cluster 2필드 추가). 그 외(구형 LSF 열
            # 누락, 또는 job name에 ';'가 들어가 필드가 밀림)는 오염을 피해 버린다.
            run_time_s = start_time = finish_time = working_dir = None
            source_cluster = forward_cluster = None
            if len(parts) in (8, 10):
                run_time_s = _parse_run_time(parts[4])
                start_time = _parse_lsf_time(parts[5])
                # RUN 중 finish_time은 예상치(estimated)일 수 있다 —
                # 실측만 저장하도록 종료 상태에서만 채운다
                if state in (JobState.DONE, JobState.EXIT):
                    finish_time = _parse_lsf_time(parts[6])
                cwd = parts[7].strip()
                working_dir = cwd if cwd and cwd != "-" else None
                if len(parts) == 10:      # MultiCluster forwarding
                    source_cluster = _clean_field(parts[8])
                    forward_cluster = _clean_field(parts[9])
            elif len(parts) != 4:
                log.debug("bjobs 필드 수 이상(%d) — 확장 필드 무시: %r",
                          len(parts), line)
            out.append(JobStatus(
                job_id=int(m.group(1)),
                array_index=int(m.group(2)) if m.group(2) else None,
                state=state, exit_code=exit_code, job_name=name,
                run_time_s=run_time_s, start_time=start_time,
                finish_time=finish_time, working_dir=working_dir,
                source_cluster=source_cluster, forward_cluster=forward_cluster))
        return out

    # ------------------------------------------------------------------
    # bhist — fallback (FR-4.3)
    # ------------------------------------------------------------------
    #: bhist 결과 키 — (job_id, array_index). 비array job은 index None.
    BhistKey = Tuple[int, Optional[int]]

    def bhist_states(self, job_ids: Sequence[int]
                     ) -> Tuple[Dict[BhistKey, Tuple[JobState, Optional[int]]],
                                Set[int]]:
        """bhist -l 파싱 — (job_id, array_index) → (최종 상태, exit_code).

        array job은 element별 블록("Job <id[idx]>")이 나오므로 반드시
        element 단위로 구분한다 — id 단일 키로 합치면 마지막 블록이
        전 element를 덮어써 DONE/EXIT가 뒤섞인다. 미발견 job은 미포함.

        반환: (조회 성공분 map, 조회 실패한 chunk의 job_id 집합). 실패는
        chunk 단위로 격리하고 연속 실패엔 회로를 차단한다
        (_query_chunks_isolated 참조). caller는 실패 집합의 job만 판단
        보류하고, 성공 chunk에서 미발견된 job은 진짜 소실로 확정할 수 있다.
        """
        result: Dict[LsfCommand.BhistKey, Tuple[JobState, Optional[int]]] = {}
        ids = [str(i) for i in job_ids]
        base = self._prog_len(self.config.bhist_path) + 20

        def run_chunk(chunk: List[str]) -> None:
            argv = (cmd_tokens(self.config.bhist_path)
                    + ["-l", "-n", "0"] + chunk)
            res = self._run_query(argv)
            if res is not None:
                result.update(self._parse_bhist(res.stdout))

        failed = self._query_chunks_isolated(ids, base, run_chunk, "bhist")
        return result, failed

    def bhist_detail(self, job_id: int,
                     array_index: Optional[int] = None) -> str:
        """job 1건의 bhist -l 원문 조회 — EXIT 원인 확인용 (blocking).

        UI에서 상태 클릭 시 온디맨드로 호출한다(폴링과 무관 — 자동 수집
        오버헤드 없음). array element는 array_index로 "id[idx]" 지정.
        미발견이면 빈 문자열, 장애(timeout 등)는 LsfCommandError."""
        target = (f"{job_id}[{array_index}]" if array_index is not None
                  else str(job_id))
        argv = cmd_tokens(self.config.bhist_path) + ["-l", "-n", "0", target]
        res = self._run_query(argv)
        return res.stdout if res is not None else ""

    @staticmethod
    def _parse_bhist(stdout: str
                     ) -> Dict["LsfCommand.BhistKey",
                               Tuple[JobState, Optional[int]]]:
        result: Dict[LsfCommand.BhistKey, Tuple[JobState, Optional[int]]] = {}
        cur: Optional[LsfCommand.BhistKey] = None
        for line in stdout.splitlines():
            m = re.search(r"Job <(\d+)(?:\[(\d+)\])?", line)
            if m:
                cur = (int(m.group(1)),
                       int(m.group(2)) if m.group(2) else None)
                continue
            if cur is None:
                continue
            if "Done successfully" in line:
                result[cur] = (JobState.DONE, 0)
            elif "Exited with exit code" in line:
                m2 = re.search(r"exit code (\d+)", line)
                result[cur] = (JobState.EXIT,
                               int(m2.group(1)) if m2 else None)
            elif "Exited" in line and cur not in result:
                result[cur] = (JobState.EXIT, None)
        return result

    # ------------------------------------------------------------------
    # bkill — FR-3.1 전략별 변형. 반환값: 실제 LSF 호출 횟수
    # ------------------------------------------------------------------
    def bkill_by_group(self, group_path: str,
                       state_filter: Optional[str] = None) -> bool:
        """반환: 실제 kill 대상이 매칭되었는지. False(no-match)면 호출자는
        이 부착물이 job을 커버하지 못한 것으로 보고 fallback해야 한다."""
        argv = cmd_tokens(self.config.bkill_path) + ["-g", group_path]
        if state_filter:
            argv += ["-stat", state_filter.lower()]
        argv.append("0")                          # 0 == group 내 전체
        return self._run_kill(argv)

    def bkill_by_name(self, pattern: str) -> bool:
        return self._run_kill(
            cmd_tokens(self.config.bkill_path) + ["-J", pattern, "0"])

    def bkill_array(self, array_job_id: int,
                    index_range: Optional[Tuple[int, int]] = None) -> bool:
        target = (f"{array_job_id}[{index_range[0]}-{index_range[1]}]"
                  if index_range else str(array_job_id))
        return self._run_kill(cmd_tokens(self.config.bkill_path) + [target])

    def _bkill_argv(self, chunk: Sequence[str], envpath: str) -> List[str]:
        """bkill 실행 argv. envpath가 있으면 그 LSF env를 source한 뒤 bkill —
        MC forward job은 로컬 bkill로 안 죽고 그 클러스터 env를 source해야
        죽는 환경을 지원한다. `set noglob`을 bkill 직전에 걸어 array target
        ("1000[2]"/"1000[1-3]")의 대괄호가 tcsh 파일 globbing으로 뭉개지는 것을
        막는다(profile source는 globbing 정상 — set noglob이 그 뒤라 안전)."""
        if not envpath:
            return cmd_tokens(self.config.bkill_path) + list(chunk)
        inner = "source {} && set noglob && exec bkill {}".format(
            envpath, " ".join(chunk))
        return ["tcsh", "-c", inner]

    def _bkill_base_len(self, envpath: str) -> int:
        if not envpath:
            return self._prog_len(self.config.bkill_path) + 10
        return len("tcsh -c source  && set noglob && exec bkill ") \
            + len(envpath) + 10

    def bkill_targets(self, targets: Sequence[str],
                      on_progress: Optional[Callable[[int], None]] = None,
                      envpath: str = "") -> int:
        """chunked bkill — "id" 또는 "id[idx]"(array element) 형태 허용.
        ARG_MAX 안전 (④ 최후 수단). 반환: LSF 호출 횟수.
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job).
        on_progress(누적_처리_수)는 chunk 완료마다 호출된다(진행 통지)."""
        calls = 0
        processed = 0
        base = self._bkill_base_len(envpath)
        for chunk in chunk_args(list(targets), self.config.chunk_size,
                                self.config.arg_max, base):
            self._run_kill(self._bkill_argv(chunk, envpath))
            calls += 1
            processed += len(chunk)
            if on_progress:
                on_progress(processed)
        return calls

    def bkill_targets_confirm(self, targets: Sequence[str],
                              on_progress: Optional[Callable[[int], None]] = None,
                              envpath: str = ""
                              ) -> Tuple[Set[str], int]:
        """chunked bkill + 출력 확인 파싱 (FR-3.4).

        반환: (해소된 target 집합, LSF 호출 횟수).
        '해소'는 더 이상 kill이 필요 없다고 확인된 것 — 'Job <id> is being
        terminated'(신호 수락) 또는 already-finished/no-matching(이미 없음).
        해소 안 된 target(일시 장애 등)은 호출자가 재시도한다.
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job).
        on_progress(누적_처리_target수)는 chunk 완료마다 호출된다(진행 통지)."""
        resolved: Set[str] = set()
        calls = 0
        processed = 0
        base = self._bkill_base_len(envpath)
        for chunk in chunk_args(list(targets), self.config.chunk_size,
                                self.config.arg_max, base):
            argv = self._bkill_argv(chunk, envpath)
            try:
                res = self._run(argv, self.config.kill_timeout_s)
            except subprocess.TimeoutExpired:
                # 이 chunk 전부 미확인 — 재시도 대상으로 남긴다
                log.warning("bkill timeout — 재시도 대상: %s", chunk)
                calls += 1
                processed += len(chunk)
                if on_progress:
                    on_progress(processed)
                continue
            calls += 1
            processed += len(chunk)
            resolved |= _parse_bkill_resolved(res.stdout + "\n" + res.stderr)
            if on_progress:
                on_progress(processed)
        return resolved, calls

    def _run_kill(self, argv: List[str]) -> bool:
        """반환: 매칭된 job이 있었는지 (no-match는 예외가 아니라 False —
        커버 실패 신호로, 호출자가 fallback을 결정한다)."""
        return self._run_or_nomatch(argv, self.config.kill_timeout_s) is not None

    # ------------------------------------------------------------------
    # bgdel (close 시 group 정리)
    # ------------------------------------------------------------------
    def _run_lenient(self, argv: List[str], what: str) -> None:
        """부가 작업용 실행 — timeout 포함 어떤 실패도 경고 로그만 남기고
        호출자에게 전파하지 않는다 (bgdel은 실패해도 본 작업과 무관)."""
        try:
            res = self._run(argv, self.config.kill_timeout_s)
        except subprocess.TimeoutExpired:
            log.warning("%s timeout (무시)", what)
            return
        if res.returncode != 0:
            log.warning("%s 실패 (무시): %s", what, res.stderr.strip())

    def bgdel(self, group_path: str) -> None:
        """LSF group 삭제 (FR-5.7 close)."""
        self._run_lenient(
            cmd_tokens(self.config.bgdel_path) + [group_path], "bgdel")
