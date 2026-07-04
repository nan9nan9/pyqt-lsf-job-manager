"""Killer — kill 전략 자동 선택 + verify (FR-3).

전략 우선순위 (ARG_MAX 방지, LSF master 부하 최소화):
① bkill -g <path> 0   ② bkill <array_id>   ③ bkill -J "<pattern>" 0
④ chunked bkill (최후 수단). 부착물 복수면 전부 순회, 없으면 ④ 직행.
"""
from __future__ import annotations

import logging
import threading
from typing import List, Optional, Sequence

from .command import LsfCommand
from .errors import LsfmgrError
from .monitor import JobsetQuerier
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import KillReport
from .states import JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.kill")


class Killer(QObject):
    """kill 진입점 — 실제 실행은 QThreadPool 단발 task (§3.2)."""

    finished = Signal(str, object)           # jobset_id, KillReport
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

    def run(self):
        try:
            report = self._run()
        except Exception as e:               # noqa: BLE001 — CS-5
            log.exception("kill 실패: %s", self.jobset_id)
            self.killer.error.emit(self.jobset_id, repr(e))
            return
        self.killer.finished.emit(self.jobset_id, report)

    # ------------------------------------------------------------------
    def _run(self) -> KillReport:
        k = self.killer
        strategies: List[str] = []
        errors: List[str] = []
        calls = 0

        if self.job_ids is not None:
            # 개별 ID kill — 항상 chunking (④)
            requested = len(self.job_ids)
            calls += k.command.bkill_by_ids(self.job_ids)
            strategies.append("chunk")
        elif self.only_state is not None:
            # 부분 kill (FR-3.2) — Store에서 해당 상태 ID를 골라 chunking.
            # (bkill -stat은 LSF 버전 의존이라 결정적 방식을 기본으로 한다)
            recs = k.store.get_jobs(self.jobset_id, states={self.only_state})
            ids = [r.job_id for r in recs if r.job_id is not None]
            requested = len(ids)
            if ids:
                calls += k.command.bkill_by_ids(ids)
                strategies.append(f"chunk(state={self.only_state.value})")
        else:
            requested, calls = self._kill_whole_jobset(strategies, errors)

        still_alive: Optional[int] = None
        if self.verify and self.jobset_id:
            still_alive = self._verify()

        return KillReport(
            jobset_id=self.jobset_id, requested=requested,
            strategies=strategies, command_calls=calls,
            still_alive=still_alive, errors=errors)

    def _kill_whole_jobset(self, strategies: List[str],
                           errors: List[str]) -> tuple:
        """FR-3.1 전략 우선순위. 부착물 성공 시 chunking 생략."""
        k = self.killer
        js = k.store.get_jobset(self.jobset_id)
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.state.is_on_lsf]
        requested = len(alive)
        if not alive:
            return 0, 0

        calls = 0
        covered = False
        for path in js.lsf_group_paths:                      # ①
            if self._attempt(lambda p=path: k.command.bkill_by_group(p),
                             f"group:{path}", strategies, errors):
                calls += 1
                covered = True
        for aid in js.array_job_ids:                         # ②
            if self._attempt(lambda a=aid: k.command.bkill_array(a),
                             f"array:{aid}", strategies, errors):
                calls += 1
                covered = True
        if not covered:
            for pattern in js.name_patterns:                 # ③
                if self._attempt(
                        lambda pt=pattern: k.command.bkill_by_name(pt),
                        f"name:{pattern}", strategies, errors):
                    calls += 1
                    covered = True
        if not covered:                                      # ④ 최후 수단
            ids = [r.job_id for r in alive if r.job_id is not None]
            if ids:
                calls += k.command.bkill_by_ids(ids)
                strategies.append("chunk")
        return requested, calls

    @staticmethod
    def _attempt(fn, name: str, strategies: List[str],
                 errors: List[str]) -> bool:
        try:
            fn()
        except LsfmgrError as e:
            errors.append(f"{name}: {e}")
            log.warning("kill 전략 실패 %s: %s", name, e)
            return False
        strategies.append(name)
        return True

    def _verify(self) -> int:
        """재조회로 실제 종료 확인 (FR-3.3). 잔존(alive) job 수 반환."""
        k = self.killer
        try:
            result = k.querier.query(self.jobset_id)
        except LsfmgrError as e:
            log.warning("kill verify 조회 실패: %s", e)
            return -1
        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if r.state.is_on_lsf
                 and r.state not in (JobState.UNKWN, JobState.ZOMBI)]
        return len(alive)
