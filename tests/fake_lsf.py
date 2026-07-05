"""FakeLsf — LSF cluster를 흉내내는 mock runner (NFR-8).

LsfCommand의 runner 시그니처 (argv, timeout) -> CommandResult 를 구현하여
subprocess 없이 bsub/bjobs/bkill/bhist/bmod/bgdel 동작을 시뮬레이션한다.
"""
from __future__ import annotations

import fnmatch
import re
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from lsfmgr.command import CommandResult


@dataclass
class FakeJob:
    job_id: int
    array_index: Optional[int]
    name: str
    group: Optional[str]
    queue: str
    command: str
    stat: str = "PEND"           # PEND/RUN/DONE/EXIT
    exit_code: Optional[int] = None
    vanished: bool = False       # bjobs에서 사라짐 (LOST 시나리오)
    in_bhist: bool = True        # vanished여도 bhist에는 남는지
    env: Optional[str] = None    # bsub -env 원문
    run_time_s: Optional[int] = None     # LSF run_time(초)
    start_time: Optional[str] = None     # bjobs -o 시각 원문
    finish_time: Optional[str] = None
    working_dir: Optional[str] = None    # LSF exec_cwd


_ARRAY_NAME_RE = re.compile(r"^(.*)\[(\d+)-(\d+)\]$")


class FakeLsf:
    """호출 기록 + 실패 주입 기능 포함."""

    def __init__(self):
        self.lock = threading.RLock()
        self.jobs: Dict[str, FakeJob] = {}   # "id" 또는 "id[idx]" → FakeJob
        self.next_id = 1000
        self.calls: List[List[str]] = []     # 모든 호출 argv 기록
        # 실패 주입
        self.fail_next_bsub = 0              # 앞으로 N회 bsub rc=1
        self.no_jobid_next_bsub = 0          # 앞으로 N회 id 파싱 불가 출력
        self.reject_group = False            # -g 지정 시 거부
        self.fail_all_queries = False        # bjobs/bhist 장애 (LSF down)
        self.fail_next_bkill = 0             # 앞으로 N회 bkill rc=255 에러

    # ------------------------------------------------------------------
    def __call__(self, argv, timeout) -> CommandResult:
        argv = list(argv)
        with self.lock:
            self.calls.append(argv)
            cmd = argv[0].rsplit("/", 1)[-1]
            handler = getattr(self, f"_do_{cmd}", None)
            if handler is None:
                return CommandResult(127, "", f"{cmd}: command not found")
            return handler(argv[1:])

    def calls_of(self, name: str) -> List[List[str]]:
        with self.lock:
            return [c for c in self.calls
                    if c[0].rsplit("/", 1)[-1] == name]

    # ------------------------------------------------------------------
    # 상태 조작 헬퍼 (테스트에서 사용)
    # ------------------------------------------------------------------
    def set_all(self, stat: str, exit_code: Optional[int] = None) -> None:
        with self.lock:
            for j in self.jobs.values():
                j.stat = stat
                j.exit_code = exit_code

    def set_job(self, job_id: int, stat: str,
                exit_code: Optional[int] = None,
                array_index: Optional[int] = None) -> None:
        key = f"{job_id}[{array_index}]" if array_index else str(job_id)
        with self.lock:
            self.jobs[key].stat = stat
            self.jobs[key].exit_code = exit_code

    def vanish_job(self, job_id: int, in_bhist: bool = True) -> None:
        """bjobs에서 사라지게 함 (LOST/bhist fallback 시나리오)."""
        with self.lock:
            for j in self.jobs.values():
                if j.job_id == job_id:
                    j.vanished = True
                    j.in_bhist = in_bhist

    def alive_jobs(self) -> List[FakeJob]:
        with self.lock:
            return [j for j in self.jobs.values()
                    if j.stat in ("PEND", "RUN") and not j.vanished]

    # ------------------------------------------------------------------
    # bsub
    # ------------------------------------------------------------------
    def _do_bsub(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args,
                                 {"-q", "-J", "-g", "-R", "-o", "-e", "-env"})
        if self.reject_group and "-g" in opts:
            return CommandResult(255, "", "Bad job group name. Job not submitted.")
        if self.fail_next_bsub > 0:
            self.fail_next_bsub -= 1
            return CommandResult(1, "", "LSF error: queue unavailable")
        if self.no_jobid_next_bsub > 0:
            self.no_jobid_next_bsub -= 1
            return CommandResult(0, "garbled output without id\n", "")

        jid = self.next_id
        self.next_id += 1
        name = opts.get("-J", f"job{jid}")
        queue = opts.get("-q", "default")
        group = opts.get("-g")
        command = rest[-1] if rest else ""

        env = opts.get("-env")
        m = _ARRAY_NAME_RE.match(name)
        if m:
            base, lo, hi = m.group(1), int(m.group(2)), int(m.group(3))
            for i in range(lo, hi + 1):
                self.jobs[f"{jid}[{i}]"] = FakeJob(
                    job_id=jid, array_index=i, name=f"{base}[{i}]",
                    group=group, queue=queue, command=command, env=env)
        else:
            self.jobs[str(jid)] = FakeJob(
                job_id=jid, array_index=None, name=name, group=group,
                queue=queue, command=command, env=env)
        return CommandResult(
            0, f"Job <{jid}> is submitted to queue <{queue}>.\n", "")

    # ------------------------------------------------------------------
    # bjobs
    # ------------------------------------------------------------------
    def _do_bjobs(self, args: List[str]) -> CommandResult:
        if self.fail_all_queries:
            return CommandResult(255, "", "LSF is down. Please wait ...\n")
        opts, rest = _parse_opts(args, {"-o", "-g", "-J"},
                                 flags={"-a", "-noheader"})
        matched = self._select(opts, rest, include_done="-a" in opts)
        if not matched:
            return CommandResult(255, "", "No matching job found\n")
        lines = []
        for j in matched:
            jid = (f"{j.job_id}[{j.array_index}]" if j.array_index
                   else str(j.job_id))
            ec = "-" if j.exit_code is None else str(j.exit_code)
            rt = "-" if j.run_time_s is None else f"{j.run_time_s} second(s)"
            st_ = j.start_time or "-"
            ft = j.finish_time or "-"
            cwd = j.working_dir or "-"
            lines.append(
                f"{jid};{j.stat};{ec};{j.name};{rt};{st_};{ft};{cwd}")
        return CommandResult(0, "\n".join(lines) + "\n", "")

    def _select(self, opts, id_args: List[str],
                include_done: bool = True) -> List[FakeJob]:
        out = []
        ids = set()
        for a in id_args:
            m = re.match(r"^(\d+)(?:\[(\d+)\])?$", a)
            if m:
                ids.add((int(m.group(1)),
                         int(m.group(2)) if m.group(2) else None))
        for j in self.jobs.values():
            if j.vanished:
                continue
            if not include_done and j.stat in ("DONE", "EXIT"):
                continue
            if "-g" in opts and j.group != opts["-g"]:
                continue
            if "-J" in opts and not fnmatch.fnmatch(j.name, opts["-J"]):
                continue
            if ids:
                if (j.job_id, j.array_index) in ids:
                    pass
                elif (j.job_id, None) in ids and j.array_index is not None:
                    pass                      # array 부모 id 지정 → 전 element
                else:
                    continue
            out.append(j)
        return out

    # ------------------------------------------------------------------
    # bkill
    # ------------------------------------------------------------------
    def _do_bkill(self, args: List[str]) -> CommandResult:
        if self.fail_next_bkill > 0:
            self.fail_next_bkill -= 1
            return CommandResult(255, "", "LSF error: cannot reach mbatchd\n")
        opts, rest = _parse_opts(args, {"-g", "-J", "-stat"})
        rest = [r for r in rest if r != "0"]   # "0" == 그룹/패턴 전체
        targets = self._select(opts, rest, include_done=False)
        if "-stat" in opts:
            targets = [j for j in targets
                       if j.stat.lower() == opts["-stat"].lower()]
        # array "id[m-n]" 표현 처리
        for a in rest:
            m = re.match(r"^(\d+)\[(\d+)-(\d+)\]$", a)
            if m:
                jid, lo, hi = int(m.group(1)), int(m.group(2)), int(m.group(3))
                targets += [j for j in self.jobs.values()
                            if j.job_id == jid and j.array_index is not None
                            and lo <= j.array_index <= hi
                            and j.stat in ("PEND", "RUN")]
        if not targets:
            return CommandResult(255, "", "No matching job found\n")
        for j in targets:
            j.stat = "EXIT"
            j.exit_code = 130
        return CommandResult(0, f"{len(targets)} jobs killed\n", "")

    # ------------------------------------------------------------------
    # bhist
    # ------------------------------------------------------------------
    def _do_bhist(self, args: List[str]) -> CommandResult:
        if self.fail_all_queries:
            return CommandResult(255, "", "LSF is down. Please wait ...\n")
        _, rest = _parse_opts(args, {"-n"}, flags={"-l"})
        blocks = []
        for a in rest:
            if not a.isdigit():
                continue
            jid = int(a)
            # 실제 bhist처럼 array는 element별 블록("Job <id[idx]>") 출력
            for j in self.jobs.values():
                if j.job_id != jid or not j.in_bhist:
                    continue
                if j.stat == "DONE":
                    body = "Done successfully."
                elif j.stat == "EXIT":
                    body = f"Exited with exit code {j.exit_code or 1}."
                else:
                    body = "Job is still running."
                jid_s = (f"{jid}[{j.array_index}]" if j.array_index
                         else str(jid))
                blocks.append(
                    f"Job <{jid_s}>, Job Name <{j.name}>\n  {body}\n")
        if not blocks:
            return CommandResult(255, "", "No matching job found\n")
        return CommandResult(0, "\n".join(blocks), "")

    # ------------------------------------------------------------------
    # bmod / bgdel
    # ------------------------------------------------------------------
    def _do_bmod(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args, {"-g"})
        for a in rest:
            if a.isdigit():
                for j in self.jobs.values():
                    if j.job_id == int(a):
                        j.group = opts.get("-g", j.group)
        return CommandResult(0, "Parameters of job are changed\n", "")

    def _do_bgdel(self, args: List[str]) -> CommandResult:
        return CommandResult(0, "Job group was deleted\n", "")

    # ------------------------------------------------------------------
    # primesim_sub — bsub를 호출하는 wrapper 흉내 (전처리 후 bsub로 위임).
    # 자기 인자(--proj X)를 소비하고 나머지 bsub 옵션은 그대로 넘긴 뒤,
    # bsub의 출력("Job <id> ...")을 그대로 반환한다.
    # ------------------------------------------------------------------
    def _do_primesim_sub(self, args: List[str]) -> CommandResult:
        i = 0
        while i < len(args) and args[i] == "--proj":
            i += 2                         # 전처리용 자기 인자 소비
        return self._do_bsub(args[i:])

    # verilog_sub — 또 다른 툴 wrapper (job 마다 다른 wrapper 시연/‏테스트용).
    def _do_verilog_sub(self, args: List[str]) -> CommandResult:
        return self._do_bsub(args)


def _parse_opts(args: List[str], with_value: set, flags: set = frozenset()):
    """단순 옵션 파서 — {opt: value}, 나머지 인자 리스트 반환."""
    opts: Dict[str, str] = {}
    rest: List[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in with_value and i + 1 < len(args):
            opts[a] = args[i + 1]
            i += 2
        elif a in flags:
            opts[a] = ""
            i += 1
        else:
            rest.append(a)
            i += 1
    return opts, rest
