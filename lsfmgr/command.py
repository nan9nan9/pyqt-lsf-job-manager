"""LsfCommand — bsub/bjobs/bkill/bhist/bmod/bgdel subprocess 래퍼.

Qt 비의존 순수 Python (§8 원칙). shell 미경유, runner 주입으로 mock 테스트 가능
(NFR-8). chunking + ARG_MAX 검사 내장 (NFR-5).
"""
from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .config import LsfConfig
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


_JOB_ID_RE = re.compile(r"Job <(\d+)>")
_ARRAY_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?$")
# bjobs가 매칭 결과 없음을 알릴 때의 메시지들
_NO_JOB_PATTERNS = ("no unfinished job", "no matching job", "is not found",
                    "no job found")


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
                               extra_args=extra_args)
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
                                 extra_args=extra_args, timeout_s=timeout_s)
            raise SubmitError(
                f"bsub exit {res.returncode}: {res.stderr.strip()[:200]}",
                fail_reason=f"BSUB_EXIT_{res.returncode}",
                returncode=res.returncode, stderr=res.stderr)

        m = _JOB_ID_RE.search(res.stdout)
        if not m:
            raise SubmitError(
                f"job id 파싱 실패: {res.stdout.strip()[:200]}",
                fail_reason="NO_JOBID_PARSED", stderr=res.stderr)
        return int(m.group(1))

    def _bsub_argv(self, command: str, *, queue, job_name, group_path,
                   resources, outfile, errfile, extra_args) -> List[str]:
        argv = [self.config.bsub_path]
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
        argv += list(extra_args)
        argv.append(command)
        total = sum(len(a) + 1 for a in argv)
        if total > self.config.arg_max:
            raise ArgMaxExceededError(
                f"bsub 인자 총 길이 {total} > ARG_MAX {self.config.arg_max}")
        return argv

    # ------------------------------------------------------------------
    # bjobs — 조회 (FR-4.1 전략별 변형)
    # ------------------------------------------------------------------
    _BJOBS_FMT = "jobid stat exit_code jobname delimiter=';'"

    def _bjobs(self, selector: List[str]) -> List[JobStatus]:
        argv = [self.config.bjobs_path, "-a", "-noheader",
                "-o", self._BJOBS_FMT] + selector
        res = self._run_query(argv)
        if res is None:
            return []
        return self._parse_bjobs(res.stdout)

    def bjobs_by_group(self, group_path: str) -> List[JobStatus]:
        return self._bjobs(["-g", group_path])

    def bjobs_by_name(self, pattern: str) -> List[JobStatus]:
        return self._bjobs(["-J", pattern])

    def bjobs_by_ids(self, job_ids: Sequence[int]) -> List[JobStatus]:
        """job_id 목록 chunked 조회 — 최후 수단 (graceful degradation)."""
        out: List[JobStatus] = []
        ids = [str(i) for i in job_ids]
        base = len(self.config.bjobs_path) + 40
        for chunk in chunk_args(ids, self.config.chunk_size,
                                self.config.arg_max, base):
            out.extend(self._bjobs(chunk))
        return out

    def _run_query(self, argv: List[str]) -> Optional[CommandResult]:
        try:
            res = self._run(argv, self.config.query_timeout_s)
        except subprocess.TimeoutExpired:
            raise LsfCommandError(f"{argv[0]} timeout")
        if res.returncode != 0:
            msg = (res.stderr + res.stdout).lower()
            if any(p in msg for p in _NO_JOB_PATTERNS):
                return None                      # 매칭 없음은 정상 (빈 결과)
            raise LsfCommandError(
                f"{argv[0]} exit {res.returncode}: {res.stderr.strip()[:200]}",
                returncode=res.returncode, stderr=res.stderr)
        return res

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
            out.append(JobStatus(
                job_id=int(m.group(1)),
                array_index=int(m.group(2)) if m.group(2) else None,
                state=state, exit_code=exit_code, job_name=name))
        return out

    # ------------------------------------------------------------------
    # bhist — fallback (FR-4.3)
    # ------------------------------------------------------------------
    def bhist_states(self, job_ids: Sequence[int]) -> Dict[int, Tuple[JobState, Optional[int]]]:
        """bhist -l 파싱 — job_id → (최종 상태, exit_code). 미발견 job은 미포함."""
        result: Dict[int, Tuple[JobState, Optional[int]]] = {}
        ids = [str(i) for i in job_ids]
        base = len(self.config.bhist_path) + 20
        for chunk in chunk_args(ids, self.config.chunk_size,
                                self.config.arg_max, base):
            argv = [self.config.bhist_path, "-l", "-n", "0"] + chunk
            res = self._run_query(argv)
            if res is None:
                continue
            result.update(self._parse_bhist(res.stdout))
        return result

    @staticmethod
    def _parse_bhist(stdout: str) -> Dict[int, Tuple[JobState, Optional[int]]]:
        result: Dict[int, Tuple[JobState, Optional[int]]] = {}
        cur: Optional[int] = None
        for line in stdout.splitlines():
            m = re.search(r"Job <(\d+)", line)
            if m:
                cur = int(m.group(1))
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
        argv = [self.config.bkill_path, "-g", group_path]
        if state_filter:
            argv += ["-stat", state_filter.lower()]
        argv.append("0")                          # 0 == group 내 전체
        return self._run_kill(argv)

    def bkill_by_name(self, pattern: str) -> bool:
        return self._run_kill([self.config.bkill_path, "-J", pattern, "0"])

    def bkill_array(self, array_job_id: int,
                    index_range: Optional[Tuple[int, int]] = None) -> bool:
        target = (f"{array_job_id}[{index_range[0]}-{index_range[1]}]"
                  if index_range else str(array_job_id))
        return self._run_kill([self.config.bkill_path, target])

    def bkill_targets(self, targets: Sequence[str]) -> int:
        """chunked bkill — "id" 또는 "id[idx]"(array element) 형태 허용.
        ARG_MAX 안전 (④ 최후 수단). 반환: LSF 호출 횟수."""
        calls = 0
        base = len(self.config.bkill_path) + 10
        for chunk in chunk_args(list(targets), self.config.chunk_size,
                                self.config.arg_max, base):
            self._run_kill([self.config.bkill_path] + chunk)
            calls += 1
        return calls

    def bkill_by_ids(self, job_ids: Sequence[int]) -> int:
        return self.bkill_targets([str(i) for i in job_ids])

    def _run_kill(self, argv: List[str]) -> bool:
        """반환: 매칭된 job이 있었는지 (no-match는 예외가 아니라 False)."""
        try:
            res = self._run(argv, self.config.kill_timeout_s)
        except subprocess.TimeoutExpired:
            raise LsfCommandError(f"{argv[0]} timeout")
        if res.returncode != 0:
            msg = (res.stderr + res.stdout).lower()
            if any(p in msg for p in _NO_JOB_PATTERNS):
                return False                      # 대상 없음 — 커버 실패 신호
            raise LsfCommandError(
                f"bkill exit {res.returncode}: {res.stderr.strip()[:200]}",
                returncode=res.returncode, stderr=res.stderr)
        return True

    # ------------------------------------------------------------------
    # bmod / bgdel
    # ------------------------------------------------------------------
    def bmod_group(self, job_ids: Sequence[int], group_path: str) -> None:
        """기존 job을 LSF group에 편입 (FR-5.4 sync_lsf)."""
        ids = [str(i) for i in job_ids]
        base = len(self.config.bmod_path) + len(group_path) + 10
        for chunk in chunk_args(ids, self.config.chunk_size,
                                self.config.arg_max, base):
            argv = [self.config.bmod_path, "-g", group_path] + chunk
            res = self._run(argv, self.config.kill_timeout_s)
            if res.returncode != 0:
                log.warning("bmod -g 실패 (무시): %s", res.stderr.strip())

    def bgdel(self, group_path: str) -> None:
        """LSF group 삭제 (FR-5.7 close). 실패는 경고만."""
        res = self._run([self.config.bgdel_path, group_path],
                          self.config.kill_timeout_s)
        if res.returncode != 0:
            log.warning("bgdel 실패 (무시): %s", res.stderr.strip())
