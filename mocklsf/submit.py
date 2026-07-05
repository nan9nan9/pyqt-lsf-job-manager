"""bsub 인자 파싱과 job 생성/계획.

실제 LSF 처럼:
  - -q/-m/-J/-n/-P/-o/-e 등 옵션 처리, 그 외 옵션은 무시
  - -Is/-I (인터렉티브) 는 지원 안 하고 무시
  - array job (-J "name[1-10]%4") 지원
  - 제출 시 계획값(pend/run 시간, 종료 결과)을 무작위로 정해 둔다
"""

import getpass
import random
import re
import time
from typing import List, Optional, Tuple

from . import config
from .models import DONE, EXIT, Job

# 값을 하나 받는(=다음 토큰을 소비하는) 알려진 옵션들.
_ARG_OPTS = {
    "-q", "-m", "-J", "-n", "-P", "-o", "-e", "-oo", "-eo", "-R", "-W",
    "-w", "-app", "-g", "-sp", "-cwd", "-M", "-u", "-jsdl", "-Jd", "-L",
    "-E", "-Ep", "-pre", "-post", "-C", "-c", "-D", "-S", "-F", "-T",
    "-G", "-Lp", "-U", "-rnc", "-XF",
}
# 값을 받지 않는(=플래그) 알려진 옵션들. 인터렉티브 계열은 무시된다.
_FLAG_OPTS = {
    "-Is", "-I", "-Ip", "-IS", "-ISp", "-ISs", "-K", "-r", "-rn", "-B",
    "-N", "-x", "-H", "-h", "-V", "-b",
}

# 제출 실패 시 흉내낼 에러 메시지 후보.
_SUBMIT_FAIL_MSGS = [
    "Failed in an LSF library call: Slave LIM configuration is not ready yet",
    "batch daemon (mbatchd) not responding: cannot open connection",
    "LSF daemon (LIM) not responding: still trying",
]

# EXIT 시 흉내낼 exit code 후보.
_EXIT_CODES = [1, 2, 127, 130, 137, 143, 255]


class SubmitError(Exception):
    """제출 파싱 단계의 사용자 오류 (잘못된 큐 등)."""


def parse_args(argv: List[str]) -> Tuple[dict, List[str]]:
    """bsub 인자를 파싱. (opts dict, command 토큰 리스트) 반환.

    알 수 없는 옵션은 무시한다. 값을 받는지 알 수 없는 미지 옵션은
    플래그로 간주(다음 토큰 소비 안 함)한다.
    """
    opts = {
        "queue": None,
        "hosts": [],
        "job_name": None,
        "num_cpus": 1,
        "proj": "default",
        "group": "",
    }
    i = 0
    cmd_start = None
    while i < len(argv):
        tok = argv[i]
        if not tok.startswith("-"):
            # 첫 비옵션 토큰부터 끝까지가 command.
            cmd_start = i
            break
        if tok in _ARG_OPTS:
            val = argv[i + 1] if i + 1 < len(argv) else ""
            if tok == "-q":
                opts["queue"] = val
            elif tok == "-m":
                opts["hosts"] = val.split()
            elif tok == "-J":
                opts["job_name"] = val
            elif tok == "-n":
                # '-n 4' 또는 '-n 4,8' (min,max) → min 사용.
                m = re.match(r"(\d+)", val)
                opts["num_cpus"] = int(m.group(1)) if m else 1
            elif tok == "-P":
                opts["proj"] = val
            elif tok == "-g":
                opts["group"] = val
            # 그 외 값 받는 옵션은 소비만 하고 무시.
            i += 2
            continue
        # 플래그(알려진 것이든 미지의 것이든) → 소비만.
        i += 1

    command = argv[cmd_start:] if cmd_start is not None else []
    return opts, command


def _parse_array_spec(name: str) -> Tuple[str, List[int], int]:
    """'name[1-10,15]%4' -> ('name', [1..10,15], 4).

    array 가 아니면 ([], 0) 형태로 반환.
    대괄호는 있으나 내용이 잘못되면(정수 아님, 빈 범위 등) SubmitError 를 던진다.
    """
    if name is None:
        return name, [], 0
    m = re.match(r"^(.*)\[([^\]]+)\](?:%(\d+))?\s*$", name)
    if not m:
        return name, [], 0
    base = m.group(1)
    body = m.group(2)
    limit = int(m.group(3)) if m.group(3) else 0
    indices = []
    try:
        for part in body.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                rng, _, step = part.partition(":")
                lo, _, hi = rng.partition("-")
                step = int(step) if step else 1
                lo_i, hi_i = int(lo), int(hi)
                if step <= 0:
                    raise ValueError("step must be positive")
                indices.extend(range(lo_i, hi_i + 1, step))
            else:
                indices.append(int(part))
    except ValueError:
        raise SubmitError(
            f"Bad job name. Job array specification <[{body}]> is invalid."
        )
    if not indices:
        # 대괄호는 있으나 유효 인덱스가 없음(예: [10-1] 역방향).
        raise SubmitError(
            f"Bad job name. Job array specification <[{body}]> is empty."
        )
    return base, indices, limit


def precheck(opts: dict):
    """job id 를 발급하기 전에 큐/array 스펙 유효성을 검사한다.

    실패 시 SubmitError 를 던져, 실패한 제출이 job 번호를 소모하지 않게 한다.
    """
    queue = opts["queue"] or config.DEFAULT_QUEUE
    if queue not in config.QUEUES:
        raise SubmitError(
            f"Queue <{queue}> is not defined in the LSF configuration."
        )
    _parse_array_spec(opts["job_name"])


def _plan_timing() -> dict:
    """job 하나의 계획값(pend/run 시간, 종료 결과, suspend)을 무작위 생성."""
    pend = random.uniform(config.PEND_MIN, config.PEND_MAX)
    run = random.uniform(config.RUN_MIN, config.RUN_MAX)
    outcome = EXIT if random.random() < config.EXIT_RATE else DONE
    exit_code = random.choice(_EXIT_CODES) if outcome == EXIT else 0
    susp_at = 0.0
    susp_secs = 0.0
    if random.random() < config.SUSPEND_RATE and run > 2:
        susp_at = random.uniform(1.0, run - 1.0)
        susp_secs = random.uniform(config.SUSPEND_MIN, config.SUSPEND_MAX)
    return {
        "pend_secs": pend,
        "run_secs": run,
        "planned_outcome": outcome,
        "exit_code": exit_code,
        "suspend_at": susp_at,
        "suspend_secs": susp_secs,
    }


def build_jobs(job_id: int, opts: dict, command: List[str],
               submit_time: float = None) -> Tuple[List[Job], int, int]:
    """opts/command 로부터 Job 목록 생성.

    반환: (jobs, array_size, array_limit). 일반 job 은 array_size=0.
    """
    if submit_time is None:
        submit_time = time.time()

    queue = opts["queue"] or config.DEFAULT_QUEUE
    if queue not in config.QUEUES:
        raise SubmitError(
            f"Queue <{queue}> is not defined in the LSF configuration."
        )

    cmd_str = " ".join(command) if command else "/bin/sleep 30"
    base_name, indices, limit = _parse_array_spec(opts["job_name"])
    # 표시용 job 이름: -J 있으면 그 base, 없으면 command.
    job_name = base_name if base_name else cmd_str

    user = getpass.getuser()
    from_host = config.MASTER_HOST
    hosts = " ".join(opts["hosts"])

    def _mk(array_index):
        j = Job(
            job_id=job_id,
            user=user,
            command=cmd_str,
            queue=queue,
            from_host=from_host,
            job_name=job_name,
            submit_time=submit_time,
            array_index=array_index,
            array_size=len(indices),
            array_limit=limit,
            num_cpus=opts["num_cpus"],
            requested_hosts=hosts,
            proj=opts["proj"],
            job_group=opts.get("group", ""),
        )
        for k, v in _plan_timing().items():
            setattr(j, k, v)
        return j

    if indices:
        jobs = [_mk(idx) for idx in indices]
        return jobs, len(indices), limit
    return [_mk(None)], 0, limit


def should_fail() -> Optional[str]:
    """이번 제출이 실패해야 하면 에러 메시지를, 아니면 None 반환."""
    if random.random() < config.SUBMIT_FAIL_RATE:
        return random.choice(_SUBMIT_FAIL_MSGS)
    return None


def submit_delay() -> float:
    return random.uniform(config.SUBMIT_DELAY_MIN, config.SUBMIT_DELAY_MAX)
