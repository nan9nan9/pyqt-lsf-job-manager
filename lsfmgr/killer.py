"""Killer — kill 전략 자동 선택 + verify (FR-3).

전략 우선순위 (ARG_MAX 방지, LSF master 부하 최소화):
① bkill -g <path> 0   ② bkill <array_id>   ③ bkill -J "<pattern>" 0
④ chunked bkill (최후 수단). 부착물 복수면 전부 순회, 없으면 ④ 직행.
"""
from __future__ import annotations

import logging
import time
from typing import List, Optional, Sequence, Tuple

from .command import LsfCommand
from .errors import LsfmgrError
from .monitor import JobsetQuerier
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import KillReport
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

    # ------------------------------------------------------------------
    def kill_jobset(self, jobset_id: str, *,
                    only_state: Optional[JobState] = None,
                    verify: bool = False) -> None:
        self._pool.start(_KillTask(
            self, jobset_id=jobset_id, only_state=only_state, verify=verify))

    def kill_jobs(self, job_ids: Sequence[int], *, verify: bool = False,
                  jobset_id: str = "") -> None:
        self._pool.start(_KillTask(
            self, jobset_id=jobset_id, job_ids=list(job_ids), verify=verify))

    def shutdown(self) -> None:
        self._pool.waitForDone(-1)


class _KillTask(QRunnable):

    def __init__(self, killer: Killer, *, jobset_id: str,
                 only_state: Optional[JobState] = None,
                 job_ids: Optional[List[int]] = None, verify: bool = False):
        super().__init__()
        self.setAutoDelete(True)
        self.killer = killer
        self.jobset_id = jobset_id
        self.only_state = only_state
        self.job_ids = job_ids
        self.verify = verify
        self._prog = EmitThrottler()         # chunk progress throttle (submit 대칭)

    def run(self):
        try:
            report = self._run()
        except Exception as e:               # noqa: BLE001 — CS-5
            log.exception("kill 실패: %s", self.jobset_id)
            self.killer.error.emit(self.jobset_id, repr(e))
            return
        # 완료 시 항상 100% 보장 (미확인이 남아도 작업은 끝) — submit과 대칭
        self.killer.progress.emit(self.jobset_id, report.requested,
                                  report.requested)
        self.killer.finished.emit(self.jobset_id, report)

    def _emit_progress(self, done: int, total: int) -> None:
        """chunk 진행 통지 (throttled)."""
        if total > 0 and self._prog.should_emit(done, total):
            self.killer.progress.emit(self.jobset_id, done, total)

    # ------------------------------------------------------------------
    def _run(self) -> KillReport:
        k = self.killer
        strategies: List[str] = []
        errors: List[str] = []
        calls = 0
        unconfirmed = 0
        retries = 0
        optimistic = (k.command.config.kill_status_policy == "optimistic")
        killed_recs: List = []       # optimistic EXIT 대상 (kill 확인된 레코드)

        if self.job_ids is not None:
            # 개별 ID kill — 확인 후 미확인분 재시도 (FR-3.4)
            requested = len(self.job_ids)
            targets = [str(i) for i in self.job_ids]
            calls, unconfirmed, retries, resolved = self._kill_confirm(
                targets, errors)
            strategies.append("chunk")
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
            if optimistic and not errors:
                killed_recs = alive

        # optimistic 정책: 확인된 job을 즉시 EXIT로 전이 (bjobs 대기 없이).
        # EXIT는 terminal이라 폴링 대상(is_on_lsf)에서 빠져 다시 조회되지 않는다.
        changed: List = []
        if optimistic and killed_recs:
            changed = self._mark_exited(killed_recs)

        still_alive: Optional[int] = None
        if self.verify and self.jobset_id:
            still_alive = self._verify()

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
            pool = self.killer.store.find_jobs(
                {int(i) for i in (self.job_ids or [])})
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
                sorted(pending),
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
        """FR-3.1 전략 우선순위. 부착물 성공 시 chunking 생략."""
        k = self.killer
        js = k.store.get_jobset(self.jobset_id)
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.state.is_on_lsf]
        if not alive:
            return 0, 0, []

        calls = 0
        covered = False
        # 부착물 하나라도 "실행 실패"(예외)면 커버 여부를 신뢰할 수 없다 —
        # merge된 jobset에서 group A 성공 + group B 장애 시 covered만 믿으면
        # B 소속 job이 영원히 살아남는다. 장애 시 fallback을 강제한다.
        had_error = False

        def run_tier(attempts) -> None:
            nonlocal calls, covered, had_error
            for name, fn in attempts:
                matched = self._attempt(fn, name, strategies, errors)
                if matched is None:
                    had_error = True
                else:
                    calls += 1
                    covered = covered or matched

        run_tier([(f"group:{p}", lambda p=p: k.command.bkill_by_group(p))
                  for p in js.lsf_group_paths]                       # ①
                 + [(f"array:{a}", lambda a=a: k.command.bkill_array(a))
                    for a in js.array_job_ids])                      # ②
        if not covered or had_error:
            run_tier([(f"name:{pt}", lambda pt=pt: k.command.bkill_by_name(pt))
                      for pt in js.name_patterns])                   # ③
        if not covered or had_error:                                 # ④ 최후 수단
            # array element는 parent id 1개로 전체가 죽으므로 dedupe.
            # 이미 죽은 job에 대한 중복 bkill은 no-match로 무해.
            targets = sorted({str(r.job_id) for r in alive
                              if r.job_id is not None})
            if targets:
                n = len(targets)
                calls += k.command.bkill_targets(
                    targets,
                    on_progress=lambda done: self._emit_progress(done, n))
                strategies.append("chunk")
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

    def _verify(self) -> int:
        """재조회로 실제 종료 확인 (FR-3.3). 잔존(alive) job 수 반환."""
        k = self.killer
        try:
            k.querier.query(self.jobset_id)      # Store 갱신 목적 (반환값 미사용)
        except LsfmgrError as e:
            log.warning("kill verify 조회 실패: %s", e)
            return -1
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.state.is_on_lsf
                 and r.state not in (JobState.UNKWN, JobState.ZOMBI)]
        return len(alive)
