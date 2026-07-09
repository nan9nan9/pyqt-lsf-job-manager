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
    source_cluster: Optional[str] = None   # MC 제출 클러스터
    forward_cluster: Optional[str] = None  # MC 포워딩된 실행 클러스터


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
        self.fail_bhist = False              # bhist만 exit 1 (working dir full 등 —
                                             # bjobs는 정상, bhist 로그 기록 실패)
        self.bhist_fail_ids: set = set()     # 이 job_id가 포함된 bhist 호출만 exit 1
                                             # (chunk 단위 부분 실패 재현용)
        self.bjobs_fail_ids: set = set()     # 이 job_id가 포함된 bjobs id 조회만
                                             # rc=255 (bjobs chunk 부분 실패 재현용)
        self.fail_next_bkill = 0             # 앞으로 N회 bkill rc=255 에러
        self.reject_clusters = False         # MC 필드(-o source_cluster) 미지원 흉내
        self.forward_needs_env = False       # forward job은 env source한 bkill만 죽음

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
        if self.bjobs_fail_ids:
            # 이 호출(chunk)에 실패 지정된 id가 하나라도 있으면 chunk 전체
            # rc=255. 문구에 _NO_JOB_PATTERNS가 없어야 '장애'로 취급된다.
            req = {int(m.group(1)) for a in rest
                   if (m := re.match(r"^(\d+)", a))}
            if req & self.bjobs_fail_ids:
                return CommandResult(255, "", "LSF: mbatchd rejected query\n")
        # 실제 LSF: -a면 종료 job 포함. -a가 없어도 explicit job id를 지정하면
        # CLEAN_PERIOD 내 종료 job(DONE/EXIT)을 보여준다. 반면 -g/-J만으로
        # (id 미지정) 조회하면 active(unfinished)만 나온다.
        has_explicit_id = any(re.match(r"^\d+(?:\[\d+\])?$", a) for a in rest)
        matched = self._select(opts, rest,
                               include_done=("-a" in opts) or has_explicit_id)
        if not matched:
            return CommandResult(255, "", "No matching job found\n")
        fmt = opts.get("-o", "")
        # MC 필드 미지원 사이트 흉내 — reject_clusters면 그 필드 요청 시 rc=255
        if self.reject_clusters and "source_cluster" in fmt:
            return CommandResult(
                255, "", "bad field name: source_cluster\n")
        want_cluster = "source_cluster" in fmt          # 포맷이 요청할 때만 추가
        lines = []
        for j in matched:
            jid = (f"{j.job_id}[{j.array_index}]" if j.array_index
                   else str(j.job_id))
            ec = "-" if j.exit_code is None else str(j.exit_code)
            rt = "-" if j.run_time_s is None else f"{j.run_time_s} second(s)"
            st_ = j.start_time or "-"
            ft = j.finish_time or "-"
            cwd = j.working_dir or "-"
            row = f"{jid};{j.stat};{ec};{j.name};{rt};{st_};{ft};{cwd}"
            if want_cluster:
                row += f";{j.source_cluster or '-'};{j.forward_cluster or '-'}"
            lines.append(row)
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
    def _do_tcsh(self, args: List[str]) -> CommandResult:
        """`tcsh -c "source <envpath> && exec bkill <ids>"` 흉내 — env를 source한
        상태의 bkill로 취급(forward job도 죽음), _do_bkill(sourced=True)로 위임."""
        if len(args) >= 2 and args[0] == "-c":
            m = re.search(r"\bbkill\s+(.+)$", args[1])
            if m:
                return self._do_bkill(m.group(1).split(), sourced=True)
        return CommandResult(0, "", "")

    def _do_bkill(self, args: List[str], sourced: bool = False) -> CommandResult:
        if self.fail_next_bkill > 0:
            self.fail_next_bkill -= 1
            return CommandResult(255, "", "LSF error: cannot reach mbatchd\n")
        opts, rest = _parse_opts(args, {"-g", "-J", "-stat"})
        rest = [r for r in rest if r != "0"]   # "0" == 그룹/패턴 전체
        targets = self._select(opts, rest, include_done=False)
        if "-stat" in opts:
            targets = [j for j in targets
                       if j.stat.lower() == opts["-stat"].lower()]
        # MC forward job은 로컬 bkill(비-sourced)로는 안 죽는 환경 흉내 —
        # env를 source한 bkill(sourced=True)만 죽인다.
        if self.forward_needs_env and not sourced:
            targets = [j for j in targets if not j.forward_cluster]
        # array "id[m-n]" 표현 처리
        for a in rest:
            m = re.match(r"^(\d+)\[(\d+)-(\d+)\]$", a)
            if m:
                jid, lo, hi = int(m.group(1)), int(m.group(2)), int(m.group(3))
                targets += [j for j in self.jobs.values()
                            if j.job_id == jid and j.array_index is not None
                            and lo <= j.array_index <= hi
                            and j.stat in ("PEND", "RUN")]
        # 실제 LSF처럼 job별 확인 메시지를 낸다 — "Job <id> is being terminated".
        # 이 문구가 있어야 lsfmgr가 kill 확인으로 인정한다 (FR-3.4).
        def _disp(j):
            return (f"{j.job_id}[{j.array_index}]" if j.array_index is not None
                    else str(j.job_id))

        lines = []
        for j in targets:
            j.stat = "EXIT"
            j.exit_code = 130
            lines.append(f"Job <{_disp(j)}> is being terminated")
        # 명시적으로 지정됐지만 매칭 안 된 id(이미 없음)는 no-match 행으로 보고.
        matched_disp = {_disp(j) for j in targets}
        matched_ids = {str(j.job_id) for j in targets}
        stderr_lines = []
        for a in rest:
            if a not in matched_disp and a not in matched_ids:
                stderr_lines.append(f"Job <{a}>: No matching job found")
        if not targets and stderr_lines:
            return CommandResult(255, "", "\n".join(stderr_lines) + "\n")
        if not targets:
            return CommandResult(255, "", "No matching job found\n")
        return CommandResult(0, "\n".join(lines) + "\n",
                             ("\n".join(stderr_lines) + "\n")
                             if stderr_lines else "")

    # ------------------------------------------------------------------
    # bhist
    # ------------------------------------------------------------------
    def _do_bhist(self, args: List[str]) -> CommandResult:
        if self.fail_all_queries:
            return CommandResult(255, "", "LSF is down. Please wait ...\n")
        if self.fail_bhist:
            # working dir/파티션 full → bhist가 자기 로그를 못 써 exit 1.
            # 문구에 _NO_JOB_PATTERNS가 없어야 '장애'로 취급된다("없음" 아님).
            return CommandResult(
                1, "", "bhist: cannot write to log directory: No space left\n")
        _, rest = _parse_opts(args, {"-n"}, flags={"-l"})
        if self.bhist_fail_ids:
            # 이 호출(chunk)에 실패 지정된 id가 하나라도 있으면 chunk 전체 exit 1
            req = {int(m.group(1)) for a in rest
                   if (m := re.match(r"^(\d+)", a))}
            if req & self.bhist_fail_ids:
                return CommandResult(
                    1, "", "bhist: cannot write to log directory: No space left\n")
        blocks = []
        for a in rest:
            m = re.match(r"^(\d+)(?:\[(\d+)\])?$", a)   # "id" 또는 "id[idx]"
            if not m:
                continue
            jid = int(m.group(1))
            idx = int(m.group(2)) if m.group(2) else None
            # 실제 bhist처럼 array는 element별 블록("Job <id[idx]>") 출력
            for j in self.jobs.values():
                if j.job_id != jid or not j.in_bhist:
                    continue
                if idx is not None and j.array_index != idx:
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
    # customwrapper_sub — bsub를 호출하는 wrapper 흉내 (전처리 후 bsub로 위임).
    # 자기 인자(--proj X)를 소비하고 나머지 bsub 옵션은 그대로 넘긴 뒤,
    # bsub의 출력("Job <id> ...")을 그대로 반환한다.
    # ------------------------------------------------------------------
    def _do_customwrapper_sub(self, args: List[str]) -> CommandResult:
        i = 0
        while i < len(args) and args[i] == "--proj":
            i += 2                         # 전처리용 자기 인자 소비
        return self._do_bsub(args[i:])


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
