"""가상 스케줄러.

일정 주기로 tick() 을 돌며 job 상태를 전이시킨다.
  - PEND -> RUN : 슬롯 여유 + 최소 pend 시간 경과 + array %limit 준수
  - RUN  -> SSUSP : 계획된 시스템 suspend 구간
  - RUN  -> DONE/EXIT : 계획된 실행 시간 경과
동시 실행량은 호스트 슬롯 총합으로 제한되므로, 수천 개를 던지면
자연스럽게 PEND 가 쌓였다가 순차적으로 실행된다.
"""

import os
import time
from collections import defaultdict
from typing import Dict, List

from . import config
from .db import Database
from .models import (
    ACTIVE_STATES, DONE, EXIT, PEND, RUN, SSUSP, USUSP, Job,
)


class Scheduler:
    def __init__(self, db: Database = None):
        self.db = db or Database()
        # 큐 우선순위 캐시.
        self._qprio = {
            name: q.get("priority", 0) for name, q in config.QUEUES.items()
        }

    # -- 슬롯 계산 ----------------------------------------------------------

    def _active_by_host(self, active: List[Job]) -> Dict[str, int]:
        counts = defaultdict(int)
        for j in active:
            if j.exec_host:
                counts[j.exec_host] += j.num_cpus
        return counts

    def _pick_host(self, job: Job, used: Dict[str, int]) -> str:
        """job 을 배치할 호스트 선택. -m 지정이 있으면 그 안에서만."""
        candidates = list(config.HOSTS.keys())
        if job.requested_hosts:
            wanted = job.requested_hosts.split()
            filtered = [h for h in candidates if h in wanted]
            # -m 로 지정된 호스트가 존재하면 그 안에서만, 아니면 전체에서.
            if filtered:
                candidates = filtered
        # 여유 슬롯이 가장 많은 호스트에 배치 (부하 분산).
        best = None
        best_free = -1
        for h in candidates:
            free = config.HOSTS[h] - used.get(h, 0)
            if free >= job.num_cpus and free > best_free:
                best = h
                best_free = free
        return best

    # -- tick ---------------------------------------------------------------

    def tick(self, now: float = None):
        """한 번의 스케줄 주기를 처리한다."""
        if now is None:
            now = time.time()

        active = self.db.jobs_in_states(list(ACTIVE_STATES))
        # (job, prev_stat) 쌍. prev_stat 은 tick 시작 시점에 DB 에서 읽은 상태로,
        # 조건부 갱신(guarded update)에 사용해 동시 변경(bkill 등)을 덮지 않는다.
        changed = []

        # 1) 실행 중 job 의 종료/suspend 처리.
        for j in active:
            if j.stat == USUSP:
                # 사용자 suspend 는 스케줄러가 건드리지 않는다 (bresume 로만 해제).
                continue
            prev = j.stat
            if j.finish_time is not None and now >= j.finish_time:
                self._finish(j, now)
                changed.append((j, prev))
                continue
            # 계획된 시스템 suspend 구간인지 판정.
            in_susp = (
                j.suspend_secs > 0
                and j.start_time is not None
                and j.start_time + j.suspend_at <= now
                < j.start_time + j.suspend_at + j.suspend_secs
            )
            new_stat = SSUSP if in_susp else RUN
            if new_stat != j.stat:
                j.stat = new_stat
                self.db.log_event(
                    j.job_id, j.array_index,
                    "suspend" if new_stat == SSUSP else "resume",
                    "system",
                )
                changed.append((j, prev))

        # 종료된 job 을 반영하고, 남은 active 로 슬롯 재계산.
        still_active = [j for j in active if j.stat in ACTIVE_STATES]
        used = self._active_by_host(still_active)

        # 2) PEND -> RUN dispatch.
        pend = self.db.jobs_in_states([PEND])
        # 큐 우선순위 내림차순, 그다음 제출 순.
        pend.sort(key=lambda j: (-self._qprio.get(j.queue, 0), j.submit_time,
                                 j.row_id))
        # array %limit 을 위해 job_id 별 현재 active element 수 집계.
        array_active = defaultdict(int)
        for j in still_active:
            if j.array_index is not None:
                array_active[j.job_id] += 1

        dispatched = 0
        for j in pend:
            if dispatched >= config.MAX_DISPATCH_PER_TICK:
                break
            # 최소 pend 시간 미경과면 대기.
            if now - j.submit_time < j.pend_secs:
                continue
            # array 동시 실행 제한.
            if j.array_limit > 0 and array_active[j.job_id] >= j.array_limit:
                continue
            host = self._pick_host(j, used)
            if host is None:
                continue
            self._dispatch(j, host, now)
            used[host] = used.get(host, 0) + j.num_cpus
            if j.array_index is not None:
                array_active[j.job_id] += 1
            changed.append((j, PEND))  # PEND 였던 job 만 dispatch 대상
            dispatched += 1

        if changed:
            self.db.update_guarded_many(changed)

    # -- 전이 헬퍼 ----------------------------------------------------------

    def _dispatch(self, j: Job, host: str, now: float):
        j.stat = RUN
        j.exec_host = host
        j.start_time = now
        # 실제 종료 예정 시각 = 실행시간 + (있다면) suspend 로 지연되는 시간.
        j.finish_time = now + j.run_secs + j.suspend_secs
        self.db.log_event(j.job_id, j.array_index, "dispatch", host, ts=now)
        self._write_job_banner(j, host)

    def _finish(self, j: Job, now: float):
        j.stat = j.planned_outcome  # DONE or EXIT
        j.finish_time = now
        if j.stat == EXIT and j.exit_code == 0:
            j.exit_code = 1
        if j.stat == DONE:
            j.exit_code = 0
        self.db.log_event(
            j.job_id, j.array_index,
            "done" if j.stat == DONE else "exit",
            f"exit_code={j.exit_code}", ts=now,
        )
        self._write_job_result(j)

    # -- job 가상 출력 (bpeek 용) -------------------------------------------

    def _job_out_path(self, j: Job) -> str:
        name = f"{j.job_id}"
        if j.array_index is not None:
            name += f".{j.array_index}"
        return os.path.join(config.JOB_OUT_DIR, name + ".out")

    def _write_job_banner(self, j: Job, host: str):
        try:
            with open(self._job_out_path(j), "w") as f:
                f.write(
                    f"<< output from job {j.display_id} >>\n"
                    f"Job <{j.display_id}> is running on host <{host}>.\n"
                    f"Command: {j.command}\n"
                    f"Starting execution...\n"
                )
        except OSError:
            pass

    def _write_job_result(self, j: Job):
        try:
            with open(self._job_out_path(j), "a") as f:
                if j.stat == DONE:
                    f.write("\nSuccessfully completed.\n")
                else:
                    f.write(
                        f"\nExited with exit code {j.exit_code}.\n"
                    )
        except OSError:
            pass

    # -- 루프 ---------------------------------------------------------------

    def run_forever(self, stop_flag=None):
        """데몬 메인 루프. stop_flag() 가 True 면 종료."""
        while True:
            if stop_flag is not None and stop_flag():
                break
            try:
                self.tick()
            except Exception as e:  # 스케줄러는 죽지 않아야 한다.
                self._log_error(e)
            time.sleep(config.SCHED_INTERVAL)

    def _log_error(self, e):
        try:
            with open(config.LOG_PATH, "a") as f:
                f.write(f"[scheduler error] {e!r}\n")
        except OSError:
            pass
