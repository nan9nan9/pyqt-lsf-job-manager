"""상태 조회/모니터링 (FR-4).

- JobsetQuerier: 조회 전략(group → array → name → chunking) + bhist fallback
  → LOST 전이. blocking이므로 반드시 worker 스레드에서 호출.
- PollingService: 전용 QThread + 그 스레드 소속 QTimer로 주기 polling (QT-1).
  결과는 batch Signal로만 통지 (QT-4).
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .command import JobStatus, LsfCommand
from .errors import LsfmgrError
from .qt import QObject, QThread, QTimer, Signal, Slot
from .states import JobRecord, JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.monitor")


@dataclass(frozen=True)
class QueryResult:
    jobset_id: str
    summary: dict
    changed: Tuple[JobRecord, ...] = ()      # 이번 조회로 상태가 바뀐 레코드
    lost: Tuple[JobRecord, ...] = ()         # 이번 조회로 LOST 전이된 레코드
    checked: int = 0                         # 조회 대상(is_on_lsf)이었던 job 수


class JobsetQuerier:
    """jobset 1건의 LSF 실상태 조회 + Store 반영 (Qt Signal 없음)."""

    def __init__(self, store: JobSetStore, command: LsfCommand):
        self.store = store
        self.command = command

    def query(self, jobset_id: str) -> QueryResult:
        js = self.store.get_jobset(jobset_id)
        targets = [r for r in self.store.get_jobs(jobset_id)
                   if r.state.is_on_lsf]                    # FR-4.2
        if not targets:
            return QueryResult(jobset_id, self.store.summary(jobset_id))

        # --- 1) 부착물 기반 조회 (FR-4.1 우선순위) ---
        statuses: Dict[Tuple[int, Optional[int]], JobStatus] = {}
        by_name: Dict[str, JobStatus] = {}

        def collect(items: List[JobStatus]):
            for st in items:
                statuses[(st.job_id, st.array_index)] = st
                if st.array_index is None:
                    statuses.setdefault((st.job_id, None), st)
                by_name[st.job_name] = st

        for path in js.lsf_group_paths:
            self._try(lambda p=path: collect(
                self.command.bjobs_by_group(p)), f"group {path}")
        for aid in js.array_job_ids:
            self._try(lambda a=aid: collect(
                self.command.bjobs_by_ids([a])), f"array {aid}")
        for pattern in js.name_patterns:
            self._try(lambda pt=pattern: collect(
                self.command.bjobs_by_name(pt)), f"name {pattern}")

        # --- 2) 부착물로 커버 안 된 job은 chunked bjobs (graceful degradation) ---
        def lookup(rec: JobRecord) -> Optional[JobStatus]:
            if rec.job_id is not None:
                st = statuses.get((rec.job_id, rec.array_index))
                if st is not None:
                    return st
            return by_name.get(rec.lsf_job_name)     # name fallback 매칭

        leftover_ids = sorted({r.job_id for r in targets
                               if r.job_id is not None and lookup(r) is None})
        if leftover_ids:
            self._try(lambda: collect(
                self.command.bjobs_by_ids(leftover_ids)), "chunk")

        # --- 3) 상태 반영 + bjobs 미발견분은 bhist fallback (FR-4.3) ---
        changed: List[JobRecord] = []
        lost: List[JobRecord] = []
        missing: List[JobRecord] = []
        for rec in targets:
            st = lookup(rec)
            if st is None:
                missing.append(rec)
                continue
            if st.state is not rec.state or st.exit_code != rec.exit_code:
                changed.append(self.store.transition(
                    jobset_id, rec.job_key, st.state,
                    exit_code=st.exit_code,
                    job_id=rec.job_id if rec.job_id is not None else st.job_id))

        if missing:
            hist = {}
            ids = sorted({r.job_id for r in missing if r.job_id is not None})
            if ids:
                self._try(lambda: hist.update(
                    self.command.bhist_states(ids)), "bhist")
            for rec in missing:
                found = hist.get(rec.job_id) if rec.job_id is not None else None
                if found is not None:
                    state, exit_code = found
                    changed.append(self.store.transition(
                        jobset_id, rec.job_key, state, exit_code=exit_code))
                else:
                    new = self.store.transition(
                        jobset_id, rec.job_key, JobState.LOST,
                        fail_reason="NOT_FOUND_IN_LSF")
                    changed.append(new)
                    lost.append(new)
                    log.error("job LOST 확정: %s (job_id=%s)",
                                rec.job_key, rec.job_id)

        return QueryResult(jobset_id, self.store.summary(jobset_id),
                           tuple(changed), tuple(lost), len(targets))

    @staticmethod
    def _try(fn, what: str) -> None:
        """조회 수단 하나의 실패가 전체 polling을 죽이지 않게 격리 (CS-5)."""
        try:
            fn()
        except LsfmgrError as e:
            log.warning("조회 실패(%s): %s", what, e)


class _PollWorker(QObject):
    """polling 스레드 안에서만 사는 worker. QTimer도 이 스레드 소속 (§3.2)."""

    updated = Signal(str, dict, list)        # jobset_id, summary, changed
    lost = Signal(str, object)               # jobset_id, JobRecord
    error = Signal(str, str)

    def __init__(self, querier: JobsetQuerier):
        super().__init__()
        self.querier = querier
        self._timers: Dict[str, QTimer] = {}
        self._in_progress: set = set()       # CS-4 중복 polling 방지
        self._auto_stop = True

    @Slot(str, float)
    def start_polling(self, jobset_id: str, interval_s: float) -> None:
        self.stop_polling(jobset_id)
        timer = QTimer(self)                 # 소속: polling 스레드
        timer.setInterval(int(interval_s * 1000))
        timer.timeout.connect(lambda: self._poll(jobset_id))
        timer.start()
        self._timers[jobset_id] = timer
        self._poll(jobset_id)                # 시작 즉시 1회

    @Slot(str)
    def stop_polling(self, jobset_id: str) -> None:
        timer = self._timers.pop(jobset_id, None)
        if timer is not None:
            timer.stop()
            timer.deleteLater()

    @Slot()
    def stop_all(self) -> None:
        for jsid in list(self._timers):
            self.stop_polling(jsid)

    @Slot(str)
    def _poll(self, jobset_id: str) -> None:
        if jobset_id in self._in_progress:   # CS-4
            return
        self._in_progress.add(jobset_id)
        try:
            result = self.querier.query(jobset_id)
        except Exception as e:               # noqa: BLE001 — 스레드 보호 (CS-5)
            log.exception("polling 실패: %s", jobset_id)
            self.error.emit(jobset_id, repr(e))
            return
        finally:
            self._in_progress.discard(jobset_id)

        # batch Signal — 변경분만 (QT-4)
        self.updated.emit(jobset_id, result.summary, list(result.changed))
        for rec in result.lost:
            self.lost.emit(jobset_id, rec)

        # 전원 terminal이면 자동 중지 (불필요한 LSF 부하 방지, NFR-4)
        if self._auto_stop and jobset_id in self._timers:
            try:
                remaining = self.querier.store.get_jobs(jobset_id)
                if remaining and all(r.state.is_terminal for r in remaining):
                    log.info("jobset %s 전원 terminal — polling 자동 중지",
                             jobset_id)
                    self.stop_polling(jobset_id)
            except LsfmgrError:
                pass


class PollingService(QObject):
    """공개 진입점 — 전용 QThread를 소유하고 요청을 Signal로 전달한다.

    public 메서드는 어느 스레드에서 불러도 안전 (queued connection 경유).
    """

    updated = Signal(str, dict, list)
    lost = Signal(str, object)
    error = Signal(str, str)
    # 내부 요청 relay (호출 스레드 → polling 스레드)
    _req_start = Signal(str, float)
    _req_stop = Signal(str)
    _req_poll = Signal(str)
    _req_stop_all = Signal()

    def __init__(self, querier: JobsetQuerier,
                 parent: Optional[QObject] = None):
        super().__init__(parent)
        self._thread = QThread()
        self._thread.setObjectName("lsfmgr-polling")
        self._worker = _PollWorker(querier)
        self._worker.moveToThread(self._thread)
        self._worker.updated.connect(self.updated)
        self._worker.lost.connect(self.lost)
        self._worker.error.connect(self.error)
        self._req_start.connect(self._worker.start_polling)
        self._req_stop.connect(self._worker.stop_polling)
        self._req_poll.connect(self._worker._poll)
        self._req_stop_all.connect(self._worker.stop_all)
        self._thread.start()

    def start_polling(self, jobset_id: str, interval_s: float = 10.0) -> None:
        self._req_start.emit(jobset_id, float(interval_s))

    def stop_polling(self, jobset_id: str) -> None:
        self._req_stop.emit(jobset_id)

    def poll_now(self, jobset_id: str) -> None:
        """1회 갱신 요청 (비동기, FR-4.4 query_once)."""
        self._req_poll.emit(jobset_id)

    def shutdown(self) -> None:
        """timer 정지 + 스레드 graceful 종료 (좀비 스레드 금지, §3.2)."""
        if not self._thread.isRunning():
            return
        self._req_stop_all.emit()
        self._thread.requestInterruption()
        self._thread.quit()
        if not self._thread.wait(10000):
            log.error("polling 스레드 종료 대기 초과")
