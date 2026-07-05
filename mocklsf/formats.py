"""bjobs 등의 출력 포맷.

앱이 파싱하는 대상이므로 실제 LSF 와 바이트 단위로 최대한 맞춘다.
  - 기본 테이블: JOBID USER STAT QUEUE FROM_HOST EXEC_HOST JOB_NAME SUBMIT_TIME
  - -w (wide), -l (long), -o (custom), -json 지원.
"""

import json as _json
import re
import time
from typing import List

from .models import PEND, Job

# 기본 bjobs 컬럼 폭 (실제 LSF 와 동일). 필드 간 공백 1칸 구분.
_DEFAULT_FMT = "%-7s %-7s %-5s %-10s %-11s %-11s %-10s %s"
_DEFAULT_HEADER = _DEFAULT_FMT % (
    "JOBID", "USER", "STAT", "QUEUE", "FROM_HOST", "EXEC_HOST",
    "JOB_NAME", "SUBMIT_TIME",
)
# 각 컬럼 폭 (truncation 판단용).
_WIDTHS = {
    "jobid": 7, "user": 7, "stat": 5, "queue": 10,
    "from_host": 11, "exec_host": 11, "job_name": 10,
}


def fmt_submit_time(epoch) -> str:
    """'Jul  4 10:23' 형태 (LSF 기본). 일(day)은 공백 패딩 2칸."""
    if epoch is None:
        return "-"
    return time.strftime("%b %e %H:%M", time.localtime(epoch))


def _trunc_name(name: str, width: int) -> str:
    """JOB_NAME/command 가 폭을 넘으면 앞을 자르고 '*' 를 붙인다 (LSF 방식)."""
    if len(name) <= width:
        return name
    return "*" + name[-(width - 1):]


def exec_host_str(job: Job) -> str:
    """EXEC_HOST 컬럼 값. 병렬 job 은 'n*host', PEND 는 빈 문자열."""
    if not job.exec_host or job.stat == PEND:
        return ""
    if job.num_cpus > 1:
        return f"{job.num_cpus}*{job.exec_host}"
    return job.exec_host


# ---------------------------------------------------------------------------
# 기본 / wide 테이블
# ---------------------------------------------------------------------------

def default_row(job: Job, wide: bool = False) -> str:
    jobid = job.display_id
    user = job.user
    stat = job.stat
    queue = job.queue
    from_host = job.from_host
    exec_host = exec_host_str(job)
    job_name = job.job_name

    if not wide:
        # 폭 초과분 truncation.
        # JOBID 는 자르지 않는다: array id('1001[10]')나 큰 job id(8자리+)를
        # 자르면 id 자체가 깨져 앱 파싱이 실패한다. 실제 LSF 도 JOBID 는
        # 폭을 넘겨 표시(overflow)하므로 %-7s 의 자연스러운 동작에 맡긴다.
        user = user[: _WIDTHS["user"]]
        queue = queue[: _WIDTHS["queue"]]
        from_host = from_host[: _WIDTHS["from_host"]]
        exec_host = exec_host[: _WIDTHS["exec_host"]]
        job_name = _trunc_name(job_name, _WIDTHS["job_name"])

    return _DEFAULT_FMT % (
        jobid, user, stat, queue, from_host, exec_host, job_name,
        fmt_submit_time(job.submit_time),
    )


def default_table(jobs: List[Job], wide: bool = False) -> str:
    lines = [_DEFAULT_HEADER]
    for j in jobs:
        lines.append(default_row(j, wide=wide))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# -l (long) 포맷
# ---------------------------------------------------------------------------

def long_format(jobs: List[Job]) -> str:
    blocks = []
    for j in jobs:
        blocks.append(_long_one(j))
    return ("\n" + "-" * 78 + "\n").join(blocks)


def _long_one(j: Job) -> str:
    sub = time.strftime("%a %b %e %H:%M:%S", time.localtime(j.submit_time))
    lines = []
    head = (
        f"Job <{j.display_id}>, Job Name <{j.job_name}>, User <{j.user}>, "
        f"Project <{j.proj}>, Status <{j.stat}>, Queue <{j.queue}>, "
        f"Command <{j.command}>"
    )
    lines.append(head)
    lines.append(f"{sub}: Submitted from host <{j.from_host}>, "
                 f"CWD <$HOME>, {j.num_cpus} Processors Requested;")
    if j.start_time:
        st = time.strftime("%a %b %e %H:%M:%S", time.localtime(j.start_time))
        lines.append(f"{st}: Started on <{j.exec_host}>;")
    if j.finish_time and j.stat in ("DONE", "EXIT"):
        ft = time.strftime("%a %b %e %H:%M:%S", time.localtime(j.finish_time))
        verb = "Done successfully" if j.stat == "DONE" else \
            f"Exited with exit code {j.exit_code}"
        lines.append(f"{ft}: {verb};")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# -o (custom) 포맷 / -json
# ---------------------------------------------------------------------------

# -o 필드명 -> (헤더라벨, 값추출 함수)
def _field_value(job: Job, name: str) -> str:
    name = name.lower()
    if name in ("jobid", "id"):
        return job.display_id
    if name == "jobindex":
        return str(job.array_index) if job.array_index is not None else "0"
    if name == "stat":
        return job.stat
    if name == "queue":
        return job.queue
    if name == "user":
        return job.user
    if name in ("job_name", "name"):
        return job.job_name
    if name == "job_description":
        return ""
    if name == "from_host":
        return job.from_host
    if name == "exec_host":
        return exec_host_str(job) or "-"
    if name == "command":
        return job.command
    if name == "submit_time":
        return fmt_submit_time(job.submit_time)
    if name == "start_time":
        return fmt_submit_time(job.start_time) if job.start_time else "-"
    if name == "finish_time":
        return fmt_submit_time(job.finish_time) if job.finish_time else "-"
    if name in ("exit_code",):
        return str(job.exit_code)
    if name in ("nreq_slot", "slots", "nalloc_slot", "min_req_proc"):
        return str(job.num_cpus)
    if name == "proj_name":
        return job.proj
    # 미지원 필드는 '-' (실제 LSF 도 알 수 없는 필드는 빈값 처리).
    return "-"


# -o 스펙 안에 섞여 들어오는 delimiter 키워드.
#   실제 LSF:  bjobs -o "jobid stat queue delimiter='^'"
# 작은/큰따옴표, 따옴표 없는 형태, 그리고 앞에 대시가 붙은 형태(-delimiter=)
# 까지 허용한다. 토큰 경계(앞이 공백/시작)에서만 매칭한다.
_DELIM_RE = re.compile(
    r"""(?<!\S)-?delimiter=(?:'([^']*)'|"([^"]*)"|(\S+))""",
    re.IGNORECASE,
)


def _extract_delimiter(spec: str):
    """-o 스펙 문자열에서 delimiter='X' 키워드를 분리한다.

    반환: (delimiter 제거된 spec, delimiter 문자열 또는 None).
    delimiter 가 없으면 원본 spec 을 그대로 돌려준다.
    """
    if not spec:
        return spec, None
    m = _DELIM_RE.search(spec)
    if not m:
        return spec, None
    # 매칭된 세 캡처 그룹(작은따옴표/큰따옴표/무따옴표) 중 하나가 값이다.
    val = next((g for g in m.groups() if g is not None), "")
    # 키워드 토큰을 제거하고 남은 필드들을 공백 1칸으로 정규화.
    clean = (spec[:m.start()] + " " + spec[m.end():])
    clean = " ".join(clean.split())
    return clean, val


def _parse_o_spec(spec: str):
    """'jobid:8 stat queue' -> [('jobid',8),('stat',None),('queue',None)]."""
    out = []
    for tok in spec.split():
        if ":" in tok:
            name, _, w = tok.partition(":")
            try:
                out.append((name, int(w)))
            except ValueError:
                out.append((name, None))
        else:
            out.append((tok, None))
    return out


def custom_format(jobs: List[Job], spec: str, noheader: bool = False,
                  delimiter: str = None) -> str:
    """bjobs -o 커스텀 포맷.

    delimiter 가 없으면 컬럼을 내용에 맞춰 좌측 정렬 정렬(공백 패딩).
    delimiter 가 있으면 그 구분자로 이어붙인다(패딩 없음, LSF -o delimiter 동작).

    delimiter 는 두 경로로 지정할 수 있다.
      1) -o 스펙 안의 delimiter='X' 키워드 (실제 LSF 방식)
      2) 별도의 delimiter 인자 (하위호환용)
    스펙 안에 지정된 delimiter 가 우선한다.
    """
    spec, embedded_delim = _extract_delimiter(spec)
    if embedded_delim is not None:
        delimiter = embedded_delim
    cols = _parse_o_spec(spec)
    header = [name.upper() for name, _ in cols]

    # 모든 값 계산.
    rows = []
    for j in jobs:
        rows.append([_field_value(j, name) for name, _ in cols])

    if delimiter is not None:
        lines = []
        if not noheader:
            lines.append(delimiter.join(header))
        for r in rows:
            lines.append(delimiter.join(r))
        return "\n".join(lines)

    # 패딩 모드: 컬럼별 폭 = max(내용, 헤더) 또는 지정폭.
    widths = []
    for idx, (name, w) in enumerate(cols):
        if w is not None:
            widths.append(w)
        else:
            content_max = max([len(header[idx])] +
                              [len(r[idx]) for r in rows], default=0)
            widths.append(content_max)

    def fmt_line(vals):
        parts = []
        for idx, v in enumerate(vals):
            w = widths[idx]
            if w is not None:
                v = v[:w] if len(v) > w else v
                # 마지막 컬럼은 패딩 생략.
                if idx == len(vals) - 1:
                    parts.append(v)
                else:
                    parts.append(v.ljust(w))
            else:
                parts.append(v)
        return " ".join(parts).rstrip()

    lines = []
    if not noheader:
        lines.append(fmt_line(header))
    for r in rows:
        lines.append(fmt_line(r))
    return "\n".join(lines)


def json_format(jobs: List[Job], spec: str = None) -> str:
    """bjobs -json 출력. spec 이 있으면 그 필드들만, 없으면 기본 필드 세트."""
    if spec:
        # -o 스펙에 delimiter 키워드가 섞여 있으면 필드로 오인하지 않게 제거.
        spec, _ = _extract_delimiter(spec)
        cols = [name for name, _ in _parse_o_spec(spec)]
    else:
        cols = ["jobid", "stat", "user", "queue", "from_host", "exec_host",
                "job_name", "submit_time"]
    records = []
    for j in jobs:
        rec = {}
        for name in cols:
            rec[name.upper()] = _field_value(j, name)
        records.append(rec)
    out = {
        "COMMAND": "bjobs",
        "JOBS": len(records),
        "RECORDS": records,
    }
    return _json.dumps(out, indent=2)
