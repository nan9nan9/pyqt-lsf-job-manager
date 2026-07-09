"""Killer — kill 전략 자동 선택 + verify (FR-3).

전략 우선순위 (ARG_MAX 방지, LSF master 부하 최소화):
① bkill -g <path> 0   ② bkill <array_id>   ③ bkill -J "<pattern>" 0
④ chunked bkill (최후 수단). 부착물 복수면 전부 순회, 없으면 ④ 직행.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .command import LsfCommand
from .errors import LsfmgrError
from .monitor import JobsetQuerier
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import KillProgress, KillReport
from .states import JobState
from .store.base import JobSetStore
from .util import EmitThrottler

log = logging.getLogger("lsfmgr.kill")


class Killer(QObject):
    """kill 진입점 — 실제 실행은 QThreadPool 단발 task (§3.2)."""

    finished = Signal(str, object)           # jobset_id, KillReport
    progress = Signal(str, int, int)         # jobset_id, done, total (chunk 진행)
    error = Signal(str, str)

    def __init__(self, store: JobSetStore, command: LsfCommand,
                 querier: JobsetQuerier, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.store = store
        self.command = command
        self.querier = querier
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(4)
        # 진행 중 kill의 pull 스냅샷 — jobset_id -> [done, total] (throttle 무관
        # 최신값). worker(update)와 조회 스레드(snapshot)가 공유해 lock으로 보호.
        self._active: Dict[str, List[int]] = {}
        self._active_lock = threading.Lock()

    def is_active(self, jobset_id: str) -> bool:
        """이 jobset에 진행 중인 kill이 있는지 (pull)."""
        with self._active_lock:
            return jobset_id in self._active

    def progress_snapshot(self, jobset_id: str) -> Optional[KillProgress]:
        """진행 중 kill의 실시간 스냅샷 — 없으면 None."""
        with self._active_lock:
            dt = self._active.get(jobset_id)
            if dt is None:
                return None
            return KillProgress(jobset_id=jobset_id, done=dt[0], total=dt[1])

    def _reg(self, jobset_id: str) -> None:
        if jobset_id:                    # 전역 kill(jsid 없음)은 스냅샷 대상 아님
            with self._active_lock:
                self._active[jobset_id] = [0, 0]

    def _set_progress(self, jobset_id: str, done: int, total: int) -> None:
        if jobset_id:
            with self._active_lock:
                dt = self._active.get(jobset_id)
                if dt is not None:
                    dt[0], dt[1] = done, total

    def _unreg(self, jobset_id: str) -> None:
        if jobset_id:
            with self._active_lock:
                self._active.pop(jobset_id, None)

    # ------------------------------------------------------------------
    def kill_jobset(self, jobset_id: str, *,
                    only_state: Optional[JobState] = None,
                    verify: bool = False, envpath: str = "",
                    quiesce: Optional[Callable[[], bool]] = None) -> None:
        """quiesce: 지정 시 kill 대상 스냅샷 전에 worker에서 호출되는 blocking
        훅 — 진행 중 submit이 멎기를 기다린다 (kill 우선권, manager가 배선)."""
        self._pool.start(_KillTask(
            self, jobset_id=jobset_id, only_state=only_state, verify=verify,
            envpath=envpath, quiesce=quiesce))

    def kill_jobs(self, job_ids: Sequence, *, verify: bool = False,
                  jobset_id: str = "", envpath: str = "") -> None:
        """job_ids: int(job 전체) 또는 "id[idx]" 문자열(array element 1개).
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job — 클러스터별로
        나눠 각 envpath로 호출)."""
        self._pool.start(_KillTask(
            self, jobset_id=jobset_id, job_ids=list(job_ids), verify=verify,
            envpath=envpath))

    def shutdown(self) -> None:
        self._pool.waitForDone(-1)


class _KillTask(QRunnable):

    def __init__(self, killer: Killer, *, jobset_id: str,
                 only_state: Optional[JobState] = None,
                 job_ids: Optional[List] = None, verify: bool = False,
                 envpath: str = "",
                 quiesce: Optional[Callable[[], bool]] = None):
        super().__init__()
        self.setAutoDelete(True)
        self.killer = killer
        self.jobset_id = jobset_id
        self.only_state = only_state
        self.job_ids = job_ids
        self.verify = verify
        self.envpath = envpath
        self.quiesce = quiesce
        cfg = killer.command.config          # chunk progress throttle (submit 대칭)
        self._prog = EmitThrottler(cfg.progress_min_interval_s,
                                   cfg.progress_min_step_ratio)

    def run(self):
        self.killer._reg(self.jobset_id)     # pull 스냅샷 등록
        try:
            try:
                report = self._run()
            except Exception as e:           # noqa: BLE001 — CS-5
                log.exception("kill 실패: %s", self.jobset_id)
                self.killer.error.emit(self.jobset_id, repr(e))
                return
            # 완료 시 항상 100% 보장 (미확인이 남아도 작업은 끝) — submit과 대칭
            self.killer._set_progress(self.jobset_id, report.requested,
                                      report.requested)
            self.killer.progress.emit(self.jobset_id, report.requested,
                                      report.requested)
            self.killer.finished.emit(self.jobset_id, report)
        finally:
            self.killer._unreg(self.jobset_id)

    def _emit_progress(self, done: int, total: int) -> None:
        """chunk 진행 통지 (throttled) + pull 스냅샷 갱신(throttle 무관 최신)."""
        self.killer._set_progress(self.jobset_id, done, total)
        if total > 0 and self._prog.should_emit(done, total):
            self.killer.progress.emit(self.jobset_id, done, total)

    # ------------------------------------------------------------------
    def _run(self) -> KillReport:
        k = self.killer
        strategies: List[str] = []
        errors: List[str] = []
        if self.quiesce is not None:
            # kill 우선권 (FR-3) — 진행 중 submit이 멎은 뒤에 대상 스냅샷을
            # 뜬다. cancel된 미제출 job은 CREATED로 복귀해 대상에서 빠지고,
            # 그새 제출이 완료된 job은 PEND(job_id 확보)로 확정되어 아래
            # 스냅샷에 포함된다 — SUBMITTING을 건너뛰다 놓치는 유출이 없다.
            if not self.quiesce():
                # 대기 초과는 report.errors에 남긴다 — 스냅샷 이후 제출이
                # 완료된 job이 kill 대상에서 빠졌을 수 있다는 뜻이라, 로그로만
                # 삼키면 kill_finished가 '전부 정리됨'으로 오보된다. errors가
                # 남으면 optimistic EXIT 표시도 함께 억제된다(오표시 방지).
                msg = ("quiesce: submit 정지 대기 초과 — 그 사이 제출된 "
                       "job이 kill에서 빠졌을 수 있음")
                log.warning("%s: %s", msg, self.jobset_id)
                errors.append(msg)
        calls = 0
        unconfirmed = 0
        retries = 0
        optimistic = (k.command.config.kill_status_policy == "optimistic")
        killed_recs: List = []       # optimistic EXIT 대상 (kill 확인된 레코드)

        # verify=True일 때 "잔존(still_alive)"을 셀 대상 job_id — kill 대상만
        # 센다(부분/개별 kill에서 대상 아닌 RUN job까지 잔존으로 세지 않도록).
        verify_ids: set = set()

        if self.job_ids is not None:
            # 개별 ID kill — 확인 후 미확인분 재시도 (FR-3.4)
            requested = len(self.job_ids)
            targets = [str(i) for i in self.job_ids]
            calls, unconfirmed, retries, resolved = self._kill_confirm(
                targets, errors)
            strategies.append("chunk")
            verify_ids = {int(str(i).split("[", 1)[0]) for i in self.job_ids}
            if optimistic:
                killed_recs = self._records_for(resolved)
        elif self.only_state is not None:
            # 부분 kill (FR-3.2) — Store에서 해당 상태 job을 골라 chunking.
            # (bkill -stat은 LSF 버전 의존이라 결정적 방식을 기본으로 한다)
            # array element는 반드시 "id[idx]"로 지정 — parent id로 죽이면
            # 다른 상태의 element까지 전부 kill된다.
            recs = k.store.get_jobs(self.jobset_id, states={self.only_state})
            targets = [self._id_str(r) for r in recs if r.job_id is not None]
            requested = len(targets)
            verify_ids = {r.job_id for r in recs if r.job_id is not None}
            if targets:
                calls, unconfirmed, retries, resolved = self._kill_confirm(
                    targets, errors)
                strategies.append(f"chunk(state={self.only_state.value})")
                if optimistic:
                    killed_recs = [r for r in recs
                                   if self._id_str(r) in resolved]
        else:
            requested, calls, alive = self._kill_whole_jobset(
                strategies, errors)
            # 전체 kill은 group/name 전략(1명령)이라 per-id 확인이 없다 —
            # 전략이 오류 없이 수행됐으면 살아있던 대상 전부를 확인된 것으로
            # 간주한다(kill 명령이 수락됨).
            verify_ids = {r.job_id for r in alive if r.job_id is not None}
            if optimistic and not errors:
                killed_recs = alive

        # optimistic 정책: 확인된 job을 즉시 EXIT로 전이 (bjobs 대기 없이).
        # EXIT는 terminal이라 폴링 대상(is_on_lsf)에서 빠져 다시 조회되지 않는다.
        changed: List = []
        if optimistic and killed_recs:
            changed = self._mark_exited(killed_recs)

        still_alive: Optional[int] = None
        if self.verify and self.jobset_id:
            still_alive = self._verify(verify_ids)

        return KillReport(
            jobset_id=self.jobset_id, requested=requested,
            strategies=strategies, command_calls=calls,
            still_alive=still_alive, unconfirmed=unconfirmed,
            kill_retries=retries, changed=changed, errors=errors)

    @staticmethod
    def _id_str(rec) -> str:
        return (f"{rec.job_id}[{rec.array_index}]"
                if rec.array_index is not None else str(rec.job_id))

    def _records_for(self, resolved: set) -> List:
        """resolved id 집합에 해당하는 레코드 — optimistic EXIT 대상.
        jobset_id를 알면 그 jobset에서, 모르면(kill_jobs 원시 id) 전역 검색."""
        if self.jobset_id:
            pool = self.killer.store.get_jobs(self.jobset_id)
        else:
            # "id[idx]" 문자열 target도 parent id로 정규화해 검색
            pool = self.killer.store.find_jobs(
                {int(str(i).split("[", 1)[0]) for i in (self.job_ids or [])})
        return [r for r in pool
                if r.job_id is not None and self._id_str(r) in resolved]

    def _mark_exited(self, recs: List) -> List:
        """확인된 kill 대상을 EXIT로 전이 (아직 on-lsf인 것만, guard로 CAS).
        반환: 실제 전이된 레코드."""
        changed = []
        for r in recs:
            # r.jobset_id 사용 — kill_jobs 전역 검색은 대상이 여러 JobSet에
            # 걸칠 수 있어 self.jobset_id(빈 값)로는 안 된다
            new = self.killer.store.transition(
                r.jobset_id, r.job_key, JobState.EXIT,
                fail_reason="KILLED",
                guard=lambda cur: cur.state.is_on_lsf)
            if new is not None:
                changed.append(new)
        return changed

    def _kill_confirm(self, targets: List[str],
                      errors: List[str]) -> Tuple[int, int, int, set]:
        """concrete-id kill — bkill 출력의 확인('is being terminated' 등)을
        보고 미확인분을 재시도한다 (submit retry와 대칭, FR-3.4).
        반환: (LSF 호출 횟수, 최종 미확인 수, 재시도 라운드 수, 해소된 id 집합)."""
        k = self.killer
        cfg = k.command.config
        total = len(targets)
        pending = set(targets)
        resolved_all: set = set()
        calls = 0
        attempt = 0
        while True:
            base = len(resolved_all)         # 이번 라운드 시작 시 확인분
            resolved, c = k.command.bkill_targets_confirm(
                sorted(pending), envpath=self.envpath,
                on_progress=lambda done: self._emit_progress(base + done, total))
            calls += c
            resolved_all |= resolved
            pending -= resolved
            if not pending or attempt >= cfg.kill_max_retry:
                break
            attempt += 1
            log.warning("kill 미확인 %d건 — 재시도 %d/%d: %s",
                        len(pending), attempt, cfg.kill_max_retry,
                        sorted(pending)[:20])
            time.sleep(cfg.kill_retry_delay_s)
        if pending:
            msg = (f"kill 확인 실패 {len(pending)}건 "
                   f"(재시도 {attempt}회 후): {sorted(pending)[:20]}")
            log.error(msg)
            errors.append(msg)
        return calls, len(pending), attempt, resolved_all

    def _kill_whole_jobset(self, strategies: List[str],
                           errors: List[str]) -> tuple:
        """FR-3.1 전략 우선순위. 부착물 성공 시 chunking 생략.
        envpath 지정 시(MC forward) group/‏name은 forward job에 안 닿으므로
        그 env를 source한 id 기반 chunk로만 죽인다."""
        k = self.killer
        js = k.store.get_jobset(self.jobset_id)
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.state.is_on_lsf]
        if not alive:
            return 0, 0, []

        if self.envpath:
            ids = sorted({str(r.job_id) for r in alive
                          if r.job_id is not None})
            calls = 0
            if ids:
                n = len(ids)
                try:
                    calls = k.command.bkill_targets(
                        ids, envpath=self.envpath,
                        on_progress=lambda done: self._emit_progress(done, n))
                    strategies.append("chunk(sourced)")
                except LsfmgrError as e:
                    errors.append(f"chunk: {e}")
                    log.warning("kill 전략 실패 chunk(sourced): %s", e)
            return len(alive), calls, alive

        calls = 0
        covered = False
        # 부착물 하나라도 "실행 실패"(예외)면 커버 여부를 신뢰할 수 없다 —
        # merge된 jobset에서 group A 성공 + group B 장애 시 covered만 믿으면
        # B 소속 job이 영원히 살아남는다. 장애 시 fallback을 강제한다.
        had_error = False
        # 부착물(-g/-J/array)은 bsub 경로 job에만 존재한다 — wrapper job은
        # 커버 자체가 불가능하므로 부착물 성공(covered) 여부와 무관하게
        # ④ chunk로 직접 죽인다. merge된 혼합 jobset에서 group 성공만 믿으면
        # wrapper job이 영원히 살아남고, optimistic 정책은 그 생존 job까지
        # EXIT로 오표시한다.
        attachable = [r for r in alive if not r.via_wrapper]

        def run_tier(attempts) -> None:
            nonlocal calls, covered, had_error
            for name, fn in attempts:
                matched = self._attempt(fn, name, strategies, errors)
                if matched is None:
                    had_error = True
                else:
                    calls += 1
                    covered = covered or matched

        if attachable:
            run_tier([(f"group:{p}", lambda p=p: k.command.bkill_by_group(p))
                      for p in js.lsf_group_paths]                   # ①
                     + [(f"array:{a}", lambda a=a: k.command.bkill_array(a))
                        for a in js.array_job_ids])                  # ②
            if not covered or had_error:
                run_tier([(f"name:{pt}",
                           lambda pt=pt: k.command.bkill_by_name(pt))
                          for pt in js.name_patterns])               # ③
        # ④ 최후 수단 — 부착물이 못 덮은 bsub job + 애초에 커버 불가인 wrapper job
        chunk_recs = (attachable if (not covered or had_error) else [])
        chunk_recs = chunk_recs + [r for r in alive if r.via_wrapper]
        if chunk_recs:
            # array element는 parent id 1개로 전체가 죽으므로 dedupe.
            # 이미 죽은 job에 대한 중복 bkill은 no-match로 무해.
            targets = sorted({str(r.job_id) for r in chunk_recs
                              if r.job_id is not None})
            if targets:
                n = len(targets)
                # 장애(LSF 순단 등)는 errors에 담고 계속 — 예외가 여기서
                # 전파되면 kill_finished가 영영 발행되지 않고, errors가
                # 남아야 optimistic 오표시(killed_recs=alive)도 막힌다
                try:
                    calls += k.command.bkill_targets(
                        targets,
                        on_progress=lambda done: self._emit_progress(done, n))
                    strategies.append("chunk")
                except LsfmgrError as e:
                    errors.append(f"chunk: {e}")
                    log.warning("kill 전략 실패 chunk: %s", e)
        return len(alive), calls, alive

    @staticmethod
    def _attempt(fn, name: str, strategies: List[str],
                 errors: List[str]) -> Optional[bool]:
        """전략 1회 시도. 반환: True=대상 kill / False=no-match(커버 실패,
        부착물이 유실됐거나 job이 부착물에 안 붙은 경우 — fallback 필요) /
        None=실행 자체 실패(예외)."""
        try:
            matched = fn()
        except LsfmgrError as e:
            errors.append(f"{name}: {e}")
            log.warning("kill 전략 실패 %s: %s", name, e)
            return None
        strategies.append(name if matched else f"{name}(no-match)")
        return matched

    def _verify(self, target_ids: set) -> int:
        """재조회로 실제 종료 확인 (FR-3.3). kill 대상(target_ids) 중 아직
        LSF에 잔존하는 job 수 반환 — 부분/개별 kill에서 대상 아닌 job은 세지
        않는다. target_ids가 비면(대상 없음) 0."""
        k = self.killer
        if not target_ids:
            return 0
        try:
            k.querier.query(self.jobset_id)      # Store 갱신 목적 (반환값 미사용)
        except LsfmgrError as e:
            log.warning("kill verify 조회 실패: %s", e)
            return -1
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.job_id in target_ids and r.state.is_on_lsf
                 and r.state not in (JobState.UNKWN, JobState.ZOMBI)]
        return len(alive)
