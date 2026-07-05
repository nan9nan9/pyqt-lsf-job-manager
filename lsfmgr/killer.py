"""Killer — kill 전략 자동 선택 + verify (FR-3).

전략 우선순위 (ARG_MAX 방지, LSF master 부하 최소화):
① bkill -g <path> 0   ② bkill <array_id>   ③ bkill -J "<pattern>" 0
④ chunked bkill (최후 수단). 부착물 복수면 전부 순회, 없으면 ④ 직행.
"""
from __future__ import annotations

import logging
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
            # 부분 kill (FR-3.2) — Store에서 해당 상태 job을 골라 chunking.
            # (bkill -stat은 LSF 버전 의존이라 결정적 방식을 기본으로 한다)
            # array element는 반드시 "id[idx]"로 지정 — parent id로 죽이면
            # 다른 상태의 element까지 전부 kill된다.
            recs = k.store.get_jobs(self.jobset_id, states={self.only_state})
            targets = [f"{r.job_id}[{r.array_index}]"
                       if r.array_index is not None else str(r.job_id)
                       for r in recs if r.job_id is not None]
            requested = len(targets)
            if targets:
                calls += k.command.bkill_targets(targets)
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
        if not alive:
            return 0, 0

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
                calls += k.command.bkill_targets(targets)
                strategies.append("chunk")
        return len(alive), calls

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
