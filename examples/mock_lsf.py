"""시간 진행형 LSF 시뮬레이터 — 예제 앱용 runner.

실제 LSF 없이 예제를 실행하기 위해 bsub/bjobs/bkill/bhist/bmod/bgdel을
흉내낸다. 테스트용 mock(tests/fake_lsf.py)과 달리 **시간이 흐르면 job이
저절로 진행**된다: PEND → RUN → DONE|EXIT.

실패 주입 옵션으로 retry / SUBMIT_FAILED / LOST 시나리오도 재현할 수 있다.

    runner = SimulatedLsf(pend_s=(0.3, 2), run_s=(1, 6), exit_rate=0.1)
    mgr = LsfJobManager(runner=runner)
"""
from __future__ import annotations

import fnmatch
import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from lsfmgr.command import CommandResult

_ARRAY_NAME_RE = re.compile(r"^(.*)\[(\d+)-(\d+)\]$")
_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?$")


@dataclass
class SimJob:
    job_id: int
    array_index: Optional[int]
    name: str
    group: Optional[str]
    queue: str
    command: str
    submitted_at: float
    pend_dur: float                  # PEND 유지 시간
    run_dur: float                   # RUN 유지 시간
    final_state: str                 # "DONE" | "EXIT"
    final_exit_code: int
    lost: bool = False               # RUN 진입 후 bjobs/bhist에서 소실
    killed_at: Optional[float] = None

    def state(self, now: float) -> Tuple[str, Optional[int]]:
        """(현재 상태, exit_code)."""
        if self.killed_at is not None:
            return "EXIT", 130
        t = now - self.submitted_at
        if t < self.pend_dur:
            return "PEND", None
        if t < self.pend_dur + self.run_dur:
            return "RUN", None
        return self.final_state, self.final_exit_code

    def visible_in_bjobs(self, now: float) -> bool:
        if not self.lost:
            return True
        # lost job은 RUN 진입 직후부터 흔적 없이 사라진다
        return now - self.submitted_at < self.pend_dur

    def visible_in_bhist(self, now: float) -> bool:
        return not self.lost


class SimulatedLsf:
    """LsfCommand runner 시그니처 (argv, timeout) -> CommandResult 구현."""

    def __init__(self, *,
                 pend_s: Tuple[float, float] = (0.3, 2.0),
                 run_s: Tuple[float, float] = (1.0, 6.0),
                 exit_rate: float = 0.08,          # EXIT(비정상 종료) 비율
                 submit_fail_rate: float = 0.0,    # bsub 실패 비율 (retry 데모)
                 lost_rate: float = 0.0,           # 흔적 없이 소실 (LOST 데모)
                 latency_s: float = 0.001,         # 명령 1회 소요 시간
                 seed: Optional[int] = None):
        self.pend_s = pend_s
        self.run_s = run_s
        self.exit_rate = exit_rate
        self.submit_fail_rate = submit_fail_rate
        self.lost_rate = lost_rate
        self.latency_s = latency_s
        self._rng = random.Random(seed)
        self._lock = threading.RLock()
        self._jobs: Dict[str, SimJob] = {}       # "id" | "id[idx]" → SimJob
        self._next_id = 1000
        self.bsub_calls = 0
        self.total_calls = 0

    # ------------------------------------------------------------------
    def __call__(self, argv, timeout) -> CommandResult:
        if self.latency_s:
            time.sleep(self.latency_s)
        argv = list(argv)
        cmd = argv[0].rsplit("/", 1)[-1]
        with self._lock:
            self.total_calls += 1
            handler = getattr(self, f"_do_{cmd}", None)
            if handler is None:
                return CommandResult(127, "", f"{cmd}: command not found")
            return handler(argv[1:])

    # ------------------------------------------------------------------
    # 데모 편의
    # ------------------------------------------------------------------
    def summary(self) -> Dict[str, int]:
        now = time.monotonic()
        out: Dict[str, int] = {}
        with self._lock:
            for j in self._jobs.values():
                st, _ = j.state(now)
                out[st] = out.get(st, 0) + 1
        return out

    # ------------------------------------------------------------------
    # bsub
    # ------------------------------------------------------------------
    def _do_bsub(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args, {"-q", "-J", "-g", "-R", "-o", "-e"})
        self.bsub_calls += 1
        if self._rng.random() < self.submit_fail_rate:
            return CommandResult(1, "", "LSF error: queue busy (simulated)")

        jid = self._next_id
        self._next_id += 1
        name = opts.get("-J", f"job{jid}")
        queue = opts.get("-q", "normal")
        group = opts.get("-g")
        command = rest[-1] if rest else ""
        now = time.monotonic()

        def make(idx: Optional[int], nm: str) -> SimJob:
            return SimJob(
                job_id=jid, array_index=idx, name=nm, group=group,
                queue=queue, command=command, submitted_at=now,
                pend_dur=self._rng.uniform(*self.pend_s),
                run_dur=self._rng.uniform(*self.run_s),
                final_state=("EXIT" if self._rng.random() < self.exit_rate
                             else "DONE"),
                final_exit_code=(self._rng.choice([1, 2, 137])
                                 if self._rng.random() < self.exit_rate else 0),
                lost=self._rng.random() < self.lost_rate)

        m = _ARRAY_NAME_RE.match(name)
        if m:
            base, lo, hi = m.group(1), int(m.group(2)), int(m.group(3))
            for i in range(lo, hi + 1):
                self._jobs[f"{jid}[{i}]"] = make(i, f"{base}[{i}]")
        else:
            self._jobs[str(jid)] = make(None, name)
        return CommandResult(
            0, f"Job <{jid}> is submitted to queue <{queue}>.\n", "")

    # ------------------------------------------------------------------
    # bjobs
    # ------------------------------------------------------------------
    def _do_bjobs(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args, {"-o", "-g", "-J"},
                                 flags={"-a", "-noheader"})
        now = time.monotonic()
        matched = [j for j in self._select(opts, rest)
                   if j.visible_in_bjobs(now)]
        if "-a" not in opts:
            matched = [j for j in matched
                       if j.state(now)[0] not in ("DONE", "EXIT")]
        if not matched:
            return CommandResult(255, "", "No matching job found\n")
        lines = []
        for j in matched:
            st, ec = j.state(now)
            jid = (f"{j.job_id}[{j.array_index}]" if j.array_index
                   else str(j.job_id))
            lines.append(f"{jid};{st};{'-' if ec is None else ec};{j.name}")
        return CommandResult(0, "\n".join(lines) + "\n", "")

    def _select(self, opts, id_args: List[str]) -> List[SimJob]:
        ids = set()
        for a in id_args:
            m = _ID_RE.match(a)
            if m:
                ids.add((int(m.group(1)),
                         int(m.group(2)) if m.group(2) else None))
        out = []
        for j in self._jobs.values():
            if "-g" in opts and j.group != opts["-g"]:
                continue
            if "-J" in opts and not fnmatch.fnmatch(j.name, opts["-J"]):
                continue
            if ids and (j.job_id, j.array_index) not in ids \
                    and not ((j.job_id, None) in ids
                             and j.array_index is not None):
                continue
            out.append(j)
        return out

    # ------------------------------------------------------------------
    # bkill
    # ------------------------------------------------------------------
    def _do_bkill(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args, {"-g", "-J", "-stat"})
        rest2 = [r for r in rest if r != "0"]
        now = time.monotonic()
        targets = [j for j in self._select(opts, rest2)
                   if j.state(now)[0] in ("PEND", "RUN")]
        for a in rest2:                          # "id[m-n]" 범위 표현
            m = re.match(r"^(\d+)\[(\d+)-(\d+)\]$", a)
            if m:
                jid, lo, hi = (int(m.group(1)), int(m.group(2)),
                               int(m.group(3)))
                targets += [j for j in self._jobs.values()
                            if j.job_id == jid and j.array_index is not None
                            and lo <= j.array_index <= hi
                            and j.state(now)[0] in ("PEND", "RUN")]
        if "-stat" in opts:
            targets = [j for j in targets
                       if j.state(now)[0].lower() == opts["-stat"].lower()]
        if not targets:
            return CommandResult(255, "", "No matching job found\n")
        for j in targets:
            j.killed_at = now
        return CommandResult(0, f"{len(targets)} jobs killed\n", "")

    # ------------------------------------------------------------------
    # bhist / bmod / bgdel
    # ------------------------------------------------------------------
    def _do_bhist(self, args: List[str]) -> CommandResult:
        _, rest = _parse_opts(args, {"-n"}, flags={"-l"})
        now = time.monotonic()
        blocks = []
        for a in rest:
            if not a.isdigit():
                continue
            jid = int(a)
            for j in self._jobs.values():
                if j.job_id != jid or not j.visible_in_bhist(now):
                    continue
                st, ec = j.state(now)
                if st == "DONE":
                    body = "Done successfully."
                elif st == "EXIT":
                    body = f"Exited with exit code {ec or 1}."
                else:
                    body = "Job is still running."
                blocks.append(f"Job <{jid}>, Job Name <{j.name}>\n  {body}\n")
                break
        if not blocks:
            return CommandResult(255, "", "No matching job found\n")
        return CommandResult(0, "\n".join(blocks), "")

    def _do_bmod(self, args: List[str]) -> CommandResult:
        opts, rest = _parse_opts(args, {"-g"})
        for a in rest:
            if a.isdigit():
                for j in self._jobs.values():
                    if j.job_id == int(a):
                        j.group = opts.get("-g", j.group)
        return CommandResult(0, "Parameters of job are changed\n", "")

    def _do_bgdel(self, args: List[str]) -> CommandResult:
        return CommandResult(0, "Job group was deleted\n", "")


def _parse_opts(args: List[str], with_value: set, flags: set = frozenset()):
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
