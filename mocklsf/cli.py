"""MockLSF CLI 명령 구현 (bsub/bjobs/bkill/bqueues/bhist/bpeek/bstop/bresume).

bin/ 의 각 래퍼 스크립트가 여기 함수를 호출한다.
반환값은 프로세스 exit code.
"""

import getpass
import os
import sys
import time
from typing import List, Optional, Tuple

from . import config, daemon, formats, submit
from .db import Database
from .models import (
    EXIT, FINISHED_STATES, PEND, PSUSP, RUN, SSUSP, USUSP, Job,
)


# ---------------------------------------------------------------------------
# 공용 헬퍼
# ---------------------------------------------------------------------------

def _err(msg: str):
    sys.stderr.write(msg + "\n")


def _out(msg: str):
    sys.stdout.write(msg + "\n")


# 값을 하나 받는(다음 토큰이 값인) 옵션들. positional 토큰만 뽑을 때
# 이 옵션들의 값 토큰을 job spec 으로 오인하지 않도록 함께 건너뛴다.
_VALUE_OPTS = {
    "-J", "-g", "-q", "-m", "-u", "-P", "-app", "-n", "-C", "-S", "-D",
    "-T", "-L",
}


def _positional_args(argv: List[str], value_opts=_VALUE_OPTS) -> List[str]:
    """dash 옵션(및 그 값)을 건너뛰고 positional 토큰만 모은다.

    naive 한 `[a for a in argv if not a.startswith('-')]` 는 '-J name' 의
    'name' 같은 옵션 값을 job spec 으로 오인한다. 이 함수는 값을 받는 옵션의
    다음 토큰까지 소비해 그 문제를 막는다.
    """
    specs = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in value_opts:
            i += 2  # 옵션 + 값 소비
            continue
        if tok.startswith("-"):
            i += 1  # 플래그 소비
            continue
        specs.append(tok)
        i += 1
    return specs


def _collect_names(argv: List[str], opt: str = "-J") -> List[str]:
    """argv 에서 '-J name' 형태의 값들을 모은다 (이름 기반 대상 지정용)."""
    names = []
    i = 0
    while i < len(argv):
        if argv[i] == opt and i + 1 < len(argv):
            names.append(argv[i + 1])
            i += 2
            continue
        i += 1
    return names


def parse_job_spec(token: str) -> Optional[Tuple[int, object]]:
    """'123' -> (123, 'ALL'), '123[5]' -> (123, 5). 파싱 실패 시 None."""
    token = token.strip()
    if token.endswith("]") and "[" in token:
        base, _, rest = token.partition("[")
        idx = rest[:-1]
        try:
            return int(base), int(idx)
        except ValueError:
            return None
    try:
        return int(token), "ALL"
    except ValueError:
        return None


def _collect_by_specs(db: Database, specs: List[str]) -> Tuple[List[Job], List[str]]:
    """job spec 목록으로 Job 들을 모은다. (jobs, 없는스펙목록) 반환."""
    jobs: List[Job] = []
    missing: List[str] = []
    seen = set()
    for tok in specs:
        parsed = parse_job_spec(tok)
        if parsed is None:
            missing.append(tok)
            continue
        job_id, idx = parsed
        found = db.jobs_by_id(job_id)
        if idx != "ALL":
            found = [j for j in found if j.array_index == idx]
        if not found:
            missing.append(tok)
            continue
        for j in found:
            if j.row_id not in seen:
                seen.add(j.row_id)
                jobs.append(j)
    return jobs, missing


def _purged(job: Job, now: float) -> bool:
    """clean period(MOCKLSF_CLEAN_PERIOD) 초과로 bjobs 에서 사라진 완료 job 인지.

    실제 LSF 처럼 완료(DONE/EXIT) 후 일정 시간이 지나면 mbatchd(bjobs)에서
    purge 된다. bhist 는 events 기록으로 계속 조회 가능하다."""
    return bool(job.stat in FINISHED_STATES and job.finish_time
                and (now - job.finish_time) > config.CLEAN_PERIOD)


# ===========================================================================
# bsub
# ===========================================================================

def cmd_bsub(argv: List[str]) -> int:
    daemon.ensure_running()
    opts, command = submit.parse_args(argv)

    # command 가 없고 stdin 이 파이프면 스크립트를 읽어 command 로 사용.
    if not command and not sys.stdin.isatty():
        data = sys.stdin.read().strip()
        if data:
            command = [data.splitlines()[0] if data else data]

    # 실제 환경처럼 약간의 제출 지연.
    time.sleep(submit.submit_delay())

    # 아주 가끔 제출 실패 재현.
    fail = submit.should_fail()
    if fail:
        _err(fail)
        _err("Job not submitted.")
        return 255

    # 큐/array 스펙 검증은 job 번호 발급 전에 수행 (실패 시 번호 낭비 방지).
    try:
        submit.precheck(opts)
    except submit.SubmitError as e:
        _err(str(e))
        _err("Job not submitted.")
        return 255

    db = Database()
    try:
        jobs, array_size, array_limit = submit.build_jobs(
            db.next_job_id(), opts, command
        )
    except submit.SubmitError as e:
        _err(str(e))
        _err("Job not submitted.")
        db.close()
        return 255

    db.insert_jobs(jobs)
    job_id = jobs[0].job_id
    for j in jobs:
        db.log_event(j.job_id, j.array_index, "submit", j.queue,
                     ts=j.submit_time)

    queue = jobs[0].queue
    # 제출 성공 메시지 (실제 LSF 와 동일 문구).
    # -q 없이 기본 큐로 갔을 때만 'default queue' 로 표기.
    if opts["queue"] is None:
        _out(f"Job <{job_id}> is submitted to default queue <{queue}>.")
    else:
        _out(f"Job <{job_id}> is submitted to queue <{queue}>.")
    db.close()
    return 0


# ===========================================================================
# bjobs
# ===========================================================================

def cmd_bjobs(argv: List[str]) -> int:
    db = Database()
    now = time.time()

    show_all = False
    wide = False
    long_fmt = False
    noheader = False
    as_json = False
    o_spec = None
    delimiter = None
    user_filter = getpass.getuser()
    name_filter = None
    queue_filter = None
    host_filter = None
    group_filter = None
    state_filters = []
    job_specs = []

    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "-a":
            show_all = True
        elif tok == "-w":
            wide = True
        elif tok == "-l":
            long_fmt = True
        elif tok == "-json" or tok == "-json2":
            as_json = True
        elif tok == "-noheader":
            noheader = True
        elif tok == "-o":
            o_spec = argv[i + 1] if i + 1 < len(argv) else ""
            i += 2
            continue
        elif tok == "-delimiter":
            delimiter = argv[i + 1] if i + 1 < len(argv) else " "
            i += 2
            continue
        elif tok == "-u":
            user_filter = argv[i + 1] if i + 1 < len(argv) else user_filter
            i += 2
            continue
        elif tok == "-J":
            name_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-q":
            queue_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-m":
            host_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-g":
            group_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-r":
            state_filters.append("run")
        elif tok == "-p":
            state_filters.append("pend")
        elif tok == "-s":
            state_filters.append("susp")
        elif tok == "-d":
            state_filters.append("done")
        elif tok.startswith("-"):
            # 미지원 옵션은 무시 (값 소비 없이).
            pass
        else:
            job_specs.append(tok)
        i += 1

    # 대상 job 수집.
    had_missing = False
    if job_specs:
        jobs, missing = _collect_by_specs(db, job_specs)
        # clean period 초과 완료 job 은 bjobs 에서 purge → 조회 시 not found.
        missing += [j.display_id for j in jobs if _purged(j, now)]
        jobs = [j for j in jobs if not _purged(j, now)]
        if missing and not jobs:
            for m in missing:
                _err(f"Job <{m}>: No matching job found")
            db.close()
            return 255
        for m in missing:
            _err(f"Job <{m}>: No matching job found")
        # 일부라도 매칭 실패가 있으면 실제 LSF 는 255 를 반환한다.
        had_missing = bool(missing)
    else:
        # purge 된 완료 job 은 bjobs -a 에서도 사라진다.
        jobs = [j for j in db.all_jobs() if not _purged(j, now)]
        # 사용자 필터 (-u all 이면 전체).
        if user_filter != "all":
            jobs = [j for j in jobs if j.user == user_filter]

    # 상태 필터.
    if state_filters:
        keep = []
        for j in jobs:
            if "run" in state_filters and j.stat == RUN:
                keep.append(j)
            elif "pend" in state_filters and j.stat == PEND:
                keep.append(j)
            elif "susp" in state_filters and j.stat in (PSUSP, USUSP, SSUSP):
                keep.append(j)
            elif "done" in state_filters and j.stat in FINISHED_STATES:
                keep.append(j)
        jobs = keep
    elif not show_all and not job_specs:
        # 기본: 미완료 job 만 (DONE/EXIT 제외).
        jobs = [j for j in jobs if j.stat not in FINISHED_STATES]

    # 부가 필터.
    if name_filter:
        jobs = [j for j in jobs if j.job_name == name_filter]
    if queue_filter:
        jobs = [j for j in jobs if j.queue == queue_filter]
    if host_filter:
        jobs = [j for j in jobs if j.exec_host == host_filter]
    if group_filter:
        jobs = [j for j in jobs if j.job_group == group_filter]

    if not jobs:
        if not as_json:
            # 실제 LSF 는 매칭 job 이 없으면 stderr 메시지 + exit 255 를 낸다.
            _err("No unfinished job found" if not show_all
                 else "No job found")
            db.close()
            return 255
        _out(formats.json_format([], o_spec))
        db.close()
        return 0

    # 출력.
    if as_json:
        _out(formats.json_format(jobs, o_spec))
    elif o_spec is not None:
        _out(formats.custom_format(jobs, o_spec, noheader=noheader,
                                   delimiter=delimiter))
    elif long_fmt:
        _out(formats.long_format(jobs))
    else:
        _out(formats.default_table(jobs, wide=wide))

    db.close()
    # 매칭된 job 은 출력했지만 일부 spec 이 없었다면 255 (실제 LSF 동작).
    return 255 if had_missing else 0


# ===========================================================================
# bkill
# ===========================================================================

def _kill_reachable(job: Job) -> bool:
    """이 bkill 프로세스의 클러스터 컨텍스트(MOCKLSF_CLUSTER=config.CLUSTER_NAME)
    에서 이 job 을 죽일 수 있는지 (MultiCluster 흉내).

    - 클러스터 정보 없는 job(MC 미사용) → 항상 로컬에서 kill 가능(기존 동작).
    - forward 된 job → 그 클러스터 env(cshrc)를 source 해서 컨텍스트가
      forward_cluster 와 같아야 죽는다. 안 그러면 닿지 못한다(실제 문제 재현).
    - 로컬 job → 로컬(source) 클러스터 컨텍스트에서만.
    """
    if not job.source_cluster and not job.forward_cluster:
        return True
    ctx = config.CLUSTER_NAME
    if job.forward_cluster:
        return ctx == job.forward_cluster
    return ctx == job.source_cluster


def cmd_bkill(argv: List[str]) -> int:
    db = Database()
    specs = []
    kill_all = False
    user_filter = None
    queue_filter = None
    group_filter = None

    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "-u":
            user_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-q":
            queue_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-g":
            group_filter = argv[i + 1] if i + 1 < len(argv) else None
            i += 2
            continue
        elif tok == "-stat":
            # 상태 필터 값 소비 (mocklsf 는 무시).
            i += 2
            continue
        elif tok == "-J":
            # 이름으로 kill (뒤에서 처리).
            name = argv[i + 1] if i + 1 < len(argv) else None
            specs.append(("name", name))
            i += 2
            continue
        elif tok in ("-s", "-r", "-b"):
            # 시그널 지정 등은 무시.
            if tok == "-s":
                i += 2
                continue
        elif tok.startswith("-"):
            pass
        else:
            if tok == "0":
                kill_all = True
            else:
                specs.append(("id", tok))
        i += 1

    now = time.time()
    targets: List[Job] = []
    missing: List[str] = []
    seen = set()

    id_specs = [s for kind, s in specs if kind == "id"]
    name_specs = [s for kind, s in specs if kind == "name"]

    def _add(j: Job):
        if j.row_id not in seen:
            seen.add(j.row_id)
            targets.append(j)

    if id_specs:
        # 개별 job id / element 지정 kill.
        found, miss = _collect_by_specs(db, id_specs)
        for j in found:
            _add(j)
        missing.extend(miss)
        # id 와 함께 지정된 이름도 병합 (드문 경우).
        for nm in name_specs:
            for j in db.all_jobs():
                if j.job_name == nm and j.stat not in FINISHED_STATES:
                    _add(j)
    elif name_specs or kill_all or user_filter or queue_filter or group_filter:
        # '0' / -J / -u / -q / -g 조합 — 지정된 필터를 모두 만족하는 미완료 job.
        # 특히 'bkill -g <group> 0' 이나 'bkill -J name 0' 의 '0' 은 전체 kill 이
        # 아니라 그 group/name 범위로 한정된다(실제 LSF 동작). 이 덕분에 lsfmgr
        # killer 의 group-tier 가 해당 jobset 만 정확히 종료한다.
        target_user = user_filter if user_filter else getpass.getuser()
        for j in db.all_jobs():
            if j.stat in FINISHED_STATES:
                continue
            if target_user != "all" and j.user != target_user:
                continue
            if queue_filter and j.queue != queue_filter:
                continue
            if group_filter and j.job_group != group_filter:
                continue
            if name_specs and j.job_name not in name_specs:
                continue
            _add(j)
    else:
        # 인자 없음.
        _err("No job found. Job ID or -J or 0 must be specified.")
        db.close()
        return 255

    if not targets and missing:
        for m in missing:
            _err(f"Job <{m}>: No matching job found")
        db.close()
        return 255

    # 매칭 대상이 없으면 실제 LSF 처럼 알림+255 (killer 는 이를 no-match 로 보고
    # 다음 전략으로 fallback 한다).
    if not targets:
        _err("No matching job found")
        db.close()
        return 255

    rc = 0
    for j in targets:
        if j.stat in FINISHED_STATES:
            # 이미 끝난 job 은 kill 목표(종료)가 이미 달성된 상태다. 실제 LSF 는
            # 알림을 내지만, 이를 '실패'로 취급하면 array/집합 kill 에서 일부
            # element 만 먼저 끝나도 전체가 오류가 된다. 알림만 내고 성공 유지.
            _err(f"Job <{j.display_id}>: Job has already finished")
            continue
        if not _kill_reachable(j):
            # forward 된 job 을 그 클러스터 env 없이 kill 시도 — 닿지 못한다.
            # lsfmgr 가 '해소됨(resolved)'으로 오인하지 않게 no-match/finished
            # 계열 문구를 피한 에러를 낸다(그래야 재시도·미확인으로 남는다).
            # 해법: 그 클러스터 cshrc 를 source 한 뒤(bkill) 다시 시도.
            _err(f"Job <{j.display_id}>: forwarded to cluster "
                 f"<{j.forward_cluster}> — source its cluster env to kill")
            rc = 255
            continue
        j.stat = EXIT
        j.exit_code = 130
        j.finish_time = now
        # 스냅샷 이후 스케줄러가 방금 종료시킨 job 은 되살리지 않는다.
        if db.update_if_stat_in(j, (PEND, RUN, SSUSP, USUSP, PSUSP)):
            db.log_event(j.job_id, j.array_index, "kill", "user", ts=now)
            _out(f"Job <{j.display_id}> is being terminated")
        else:
            _err(f"Job <{j.display_id}>: Job has already finished")

    # 없는 job spec 만 진짜 오류(255)로 보고한다. 이미 끝난 job 은 성공 취급.
    for m in missing:
        _err(f"Job <{m}>: No matching job found")
        rc = 255

    db.close()
    return rc


# ===========================================================================
# bstop / bresume  (suspend / resume)
# ===========================================================================

def _stop_targets(db: Database, argv: List[str]):
    """bstop/bresume 대상 수집: id/element spec + '-J 이름' 지원."""
    jobs, missing = _collect_by_specs(db, _positional_args(argv))
    names = _collect_names(argv)
    if names:
        seen = {j.row_id for j in jobs}
        for j in db.all_jobs():
            if (j.job_name in names and j.stat not in FINISHED_STATES
                    and j.row_id not in seen):
                jobs.append(j)
                seen.add(j.row_id)
    return jobs, missing


def cmd_bstop(argv: List[str]) -> int:
    db = Database()
    jobs, missing = _stop_targets(db, argv)
    now = time.time()
    rc = 0
    for j in jobs:
        if j.stat == RUN or j.stat == SSUSP:
            j.stat = USUSP
            j.susp_since = now
            # 스냅샷 이후 종료됐다면(스케줄러) USUSP 로 되살리지 않는다.
            if db.update_if_stat_in(j, (RUN, SSUSP)):
                db.log_event(j.job_id, j.array_index, "suspend", "user",
                             ts=now)
            else:
                _err(f"Job <{j.display_id}>: Job cannot be stopped in its "
                     "current state")
                rc = 255
        elif j.stat == PEND:
            j.stat = PSUSP
            j.susp_since = now
            if db.update_if_stat_in(j, (PEND,)):
                db.log_event(j.job_id, j.array_index, "suspend", "user",
                             ts=now)
            else:
                _err(f"Job <{j.display_id}>: Job cannot be stopped in its "
                     "current state")
                rc = 255
        else:
            _err(f"Job <{j.display_id}>: Job cannot be stopped in its "
                 "current state")
            rc = 255
    for m in missing:
        _err(f"Job <{m}>: No matching job found")
        rc = 255
    db.close()
    return rc


def cmd_bresume(argv: List[str]) -> int:
    db = Database()
    jobs, missing = _stop_targets(db, argv)
    now = time.time()
    rc = 0
    for j in jobs:
        if j.stat == USUSP:
            # suspend 되어 있던 만큼 종료 예정 시각을 뒤로 민다.
            if j.susp_since and j.finish_time:
                j.finish_time += (now - j.susp_since)
            j.stat = RUN
            j.susp_since = 0.0
            # 스냅샷 이후 동시 변경(예: bkill→EXIT)을 되살리지 않는다.
            if db.update_if_stat_in(j, (USUSP,)):
                db.log_event(j.job_id, j.array_index, "resume", "user", ts=now)
            else:
                _err(f"Job <{j.display_id}>: Job is not in a suspended state")
                rc = 255
        elif j.stat == PSUSP:
            if j.susp_since:
                j.pend_secs += (now - j.susp_since)
            j.stat = PEND
            j.susp_since = 0.0
            if db.update_if_stat_in(j, (PSUSP,)):
                db.log_event(j.job_id, j.array_index, "resume", "user", ts=now)
            else:
                _err(f"Job <{j.display_id}>: Job is not in a suspended state")
                rc = 255
        else:
            _err(f"Job <{j.display_id}>: Job is not in a suspended state")
            rc = 255
    for m in missing:
        _err(f"Job <{m}>: No matching job found")
        rc = 255
    db.close()
    return rc


# ===========================================================================
# bqueues
# ===========================================================================

_BQ_FMT = "%-15s %4s %-15s %3s %4s %4s %4s %5s %5s %5s %5s"


def cmd_bqueues(argv: List[str]) -> int:
    db = Database()
    only = _positional_args(argv)  # positional = 조회할 큐 이름

    jobs = db.all_jobs()
    # 큐별 상태 카운트.
    counts = {q: {"NJOBS": 0, "PEND": 0, "RUN": 0, "SUSP": 0}
              for q in config.QUEUES}
    for j in jobs:
        if j.queue not in counts:
            continue
        if j.stat in FINISHED_STATES:
            continue
        c = counts[j.queue]
        c["NJOBS"] += 1
        if j.stat == PEND:
            c["PEND"] += 1
        elif j.stat == RUN:
            c["RUN"] += 1
        elif j.stat in (PSUSP, USUSP, SSUSP):
            c["SUSP"] += 1

    _out(_BQ_FMT % ("QUEUE_NAME", "PRIO", "STATUS", "MAX", "JL/U", "JL/P",
                    "JL/H", "NJOBS", "PEND", "RUN", "SUSP"))
    for name, q in config.QUEUES.items():
        if only and name not in only:
            continue
        c = counts[name]
        _out(_BQ_FMT % (
            name, q.get("priority", 0), "Open:Active", "-", "-", "-", "-",
            c["NJOBS"], c["PEND"], c["RUN"], c["SUSP"],
        ))
    db.close()
    return 0


# ===========================================================================
# bhist
# ===========================================================================

def cmd_bhist(argv: List[str]) -> int:
    db = Database()
    specs = _positional_args(argv)
    if not specs:
        _err("bhist: 조회할 job id 를 지정하세요")
        db.close()
        return 255

    rc = 0
    blocks = []
    for tok in specs:
        parsed = parse_job_spec(tok)
        if parsed is None:
            _err(f"Job <{tok}>: No matching job found")
            rc = 255
            continue
        job_id, idx = parsed
        jobs = db.jobs_by_id(job_id)
        if idx != "ALL":
            jobs = [j for j in jobs if j.array_index == idx]
        if not jobs:
            _err(f"Job <{tok}>: No matching job found")
            rc = 255
            continue
        for j in jobs:
            blocks.append(_bhist_one(db, j))

    if blocks:
        _out("Summary of time in seconds spent in various states:")
        _out("\n".join(blocks))
    db.close()
    return rc


def _ts(epoch) -> str:
    return time.strftime("%a %b %e %H:%M:%S", time.localtime(epoch))


def _bhist_one(db: Database, j: Job) -> str:
    lines = [
        f"Job <{j.display_id}>, User <{j.user}>, Project <{j.proj}>, "
        f"Command <{j.command}>",
    ]
    events = db.events_for(j.job_id)
    events = [e for e in events
              if (j.array_index is None) or (e["array_index"] == j.array_index)]
    for e in events:
        kind = e["kind"]
        ts = _ts(e["ts"])
        if kind == "submit":
            lines.append(f"{ts}: Submitted from host <{j.from_host}> to "
                         f"Queue <{e['detail']}>;")
        elif kind == "dispatch":
            lines.append(f"{ts}: Dispatched to <{e['detail']}>;")
        elif kind == "run":
            lines.append(f"{ts}: Running;")
        elif kind == "done":
            lines.append(f"{ts}: Done successfully;")
        elif kind == "exit":
            lines.append(f"{ts}: Exited;")
        elif kind == "suspend":
            lines.append(f"{ts}: Suspended ({e['detail']});")
        elif kind == "resume":
            lines.append(f"{ts}: Resumed ({e['detail']});")
        elif kind == "kill":
            lines.append(f"{ts}: Signal <KILL> requested by user;")
    return "\n".join(lines)


# ===========================================================================
# bpeek
# ===========================================================================

def cmd_bpeek(argv: List[str]) -> int:
    db = Database()
    specs = _positional_args(argv)
    if not specs:
        _err("bpeek: 조회할 job id 를 지정하세요")
        db.close()
        return 255

    parsed = parse_job_spec(specs[0])
    if parsed is None:
        _err(f"Job <{specs[0]}>: No matching job found")
        db.close()
        return 255
    job_id, idx = parsed
    jobs = db.jobs_by_id(job_id)
    if idx != "ALL":
        jobs = [j for j in jobs if j.array_index == idx]
    if not jobs:
        _err(f"Job <{specs[0]}>: No matching job found")
        db.close()
        return 255

    j = jobs[0]
    if j.stat == PEND:
        _err(f"Job <{j.display_id}> : Cannot peek into a pending job")
        db.close()
        return 255

    path = _job_out_path(j)
    if not os.path.exists(path):
        _err(f"Job <{j.display_id}> : No output yet")
        db.close()
        return 0
    with open(path) as f:
        sys.stdout.write(f.read())
    db.close()
    return 0


def _job_out_path(j: Job) -> str:
    name = f"{j.job_id}"
    if j.array_index is not None:
        name += f".{j.array_index}"
    return os.path.join(config.JOB_OUT_DIR, name + ".out")


# ===========================================================================
# bmod / bgdel  (job group 편입 / 삭제)
# ===========================================================================

def _opt_value(argv: List[str], opt: str) -> Optional[str]:
    """argv 에서 '<opt> <value>' 형태의 첫 값을 반환 (없으면 None)."""
    for i, tok in enumerate(argv):
        if tok == opt and i + 1 < len(argv):
            return argv[i + 1]
    return None


def cmd_bmod(argv: List[str]) -> int:
    """bmod -g <group> <ids...> — 지정한 job 들을 job group 으로 편입(이동)한다.

    lsfmgr 는 job 을 jobset 의 LSF group 에 동기화할 때 이 명령을 쓴다.
    -g 와 job id 가 함께 오면 해당 job 들의 group 을 갱신한다. 그 외 형태의
    bmod(자원 변경 등)는 mocklsf 가 다루지 않으므로 no-op 성공으로 처리한다.
    """
    group = _opt_value(argv, "-g")
    specs = _positional_args(argv)   # _VALUE_OPTS 에 -g 포함 → 값 토큰은 제외됨
    if group is None or not specs:
        return 0

    db = Database()
    jobs, missing = _collect_by_specs(db, specs)
    for j in jobs:
        j.job_group = group
        db.update_job(j)
    rc = 0
    for m in missing:
        _err(f"Job <{m}>: No matching job found")
        rc = 255
    db.close()
    return rc


def cmd_bgdel(argv: List[str]) -> int:
    """bgdel <group> — 비어 있는 job group 을 삭제한다.

    실제 LSF 에서 group 컨테이너는 소속 job 이 모두 끝나야 지워진다. mocklsf 는
    group 을 job 속성으로만 추적하므로(별도 컨테이너 없음), 이 명령은 성공(no-op)
    으로 처리한다 — job 이 끝나면 group 조회 결과에서도 자연히 사라진다."""
    return 0


# ===========================================================================
# mocklsfd (데몬 제어)
# ===========================================================================

def cmd_mocklsfd(argv: List[str]) -> int:
    action = argv[0] if argv else "status"
    if action == "start":
        fg = "-f" in argv or "--foreground" in argv
        if daemon.start(foreground=fg):
            if not fg:
                _out("mocklsfd started")
            return 0
        _out("mocklsfd is already running")
        return 0
    if action == "stop":
        if daemon.stop():
            _out("mocklsfd stopped")
            return 0
        _out("mocklsfd is not running")
        return 0
    if action == "restart":
        daemon.stop()
        daemon.start()
        _out("mocklsfd restarted")
        return 0
    if action == "status":
        _out(daemon.status())
        return 0
    if action == "reset":
        # 상태 초기화 (개발/테스트 편의).
        daemon.stop()
        for p in (config.DB_PATH, config.DB_PATH + "-wal",
                  config.DB_PATH + "-shm"):
            try:
                os.remove(p)
            except OSError:
                pass
        _out("mocklsf state reset")
        return 0
    _err(f"unknown action: {action}")
    return 2
