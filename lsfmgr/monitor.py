"""상태 조회/모니터링 (FR-4).

- JobsetQuerier: 조회 전략(group → array → name → chunking) + bhist fallback
  → LOST 전이. blocking이므로 반드시 worker 스레드에서 호출.
- PollingService: 전용 QThread + 그 스레드 소속 QTimer로 주기 polling (QT-1).
  결과는 batch Signal로만 통지 (QT-4).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .command import JobStatus, LsfCommand
from .errors import JobSetNotFoundError, LsfmgrError
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
                by_name[st.job_name] = st

        # 조회 수단 실패("장애")와 "job이 LSF에 없음"은 반드시 구분한다 —
        # 장애를 없음으로 오판하면 LSF 순단 1회에 전원 LOST(terminal) 확정됨.
        # 하나가 실패해도 나머지 probe는 계속 수행해 최대한 수집한다.
        probes = (
            [(f"group {p}", lambda p=p: self.command.bjobs_by_group(p))
             for p in js.lsf_group_paths]
            + [(f"array {a}", lambda a=a: self.command.bjobs_by_ids([a]))
               for a in js.array_job_ids]
            + [(f"name {pt}", lambda pt=pt: self.command.bjobs_by_name(pt))
               for pt in js.name_patterns])
        probe_failed = False
        for what, fn in probes:
            probe_failed |= not self._try(lambda fn=fn: collect(fn()), what)

        # --- 2) 부착물로 커버 안 된 job은 chunked bjobs (graceful degradation) ---
        def lookup(rec: JobRecord) -> Optional[JobStatus]:
            if rec.job_id is not None:
                st = statuses.get((rec.job_id, rec.array_index))
                if st is not None:
                    return st
            st = by_name.get(rec.lsf_job_name)       # name fallback 매칭
            # 동명이지만 다른 인스턴스(id 불일치)면 버린다 — 다른 job의
            # 상태/exit_code가 이 레코드에 혼입되는 것을 막는다
            if (st is not None and rec.job_id is not None
                    and st.job_id != rec.job_id):
                return None
            return st

        leftover_ids = sorted({r.job_id for r in targets
                               if r.job_id is not None and lookup(r) is None})
        if leftover_ids:
            probe_failed |= not self._try(lambda: collect(
                self.command.bjobs_by_ids(leftover_ids)), "chunk")

        # --- 3) 상태 반영 + bjobs 미발견분은 bhist fallback (FR-4.3) ---
        # 이 사이클은 시작 시점 스냅샷(targets) 기반이고 bjobs 왕복 동안 수 초가
        # 흐른다 — 그 사이 레코드가 바뀌었으면(resubmit_jobs의 리셋→재제출 등)
        # 스냅샷 기준 갱신이 새 job_id/상태를 옛 값으로 되돌린다. guard(CAS)로
        # "스냅샷과 동일할 때만" 전이시키고, 밀린 갱신은 다음 사이클에 맡긴다.
        def unchanged(rec: JobRecord):
            return lambda cur: (cur.job_id == rec.job_id
                                and cur.state is rec.state)

        def safe_transition(rec: JobRecord, *args, **kw):
            """job 1건 전이 — 사이클 도중 remove_job으로 키가 사라졌으면
            None (guard 거부와 동일 취급). 사이클 전체 중단을 막는다."""
            try:
                return self.store.transition(jobset_id, rec.job_key,
                                             *args, **kw)
            except LsfmgrError:
                log.warning("전이 대상 소실 — 건너뜀: %s", rec.job_key)
                return None

        changed: List[JobRecord] = []
        lost: List[JobRecord] = []
        missing: List[JobRecord] = []
        for rec in targets:
            st = lookup(rec)
            if st is None:
                missing.append(rec)
                continue
            # 상태·exit_code 외에 실행시간 창(start/finish)·실행 디렉토리가 새로
            # 채워질 때도 반영 — set-once라 매 폴링 반복 갱신(이벤트 스팸) 없음.
            if (st.state is not rec.state or st.exit_code != rec.exit_code
                    or st.start_time != rec.start_time
                    or st.finish_time != rec.finish_time
                    or st.working_dir != rec.working_dir):
                new = safe_transition(
                    rec, st.state,
                    guard=unchanged(rec),
                    exit_code=st.exit_code,
                    run_time_s=st.run_time_s, start_time=st.start_time,
                    finish_time=st.finish_time, working_dir=st.working_dir,
                    job_id=rec.job_id if rec.job_id is not None else st.job_id)
                if new is not None:
                    changed.append(new)

        if missing:
            hist: Dict = {}
            bhist_ok = True
            ids = sorted({r.job_id for r in missing if r.job_id is not None})
            if ids:
                bhist_ok = self._try(lambda: hist.update(
                    self.command.bhist_states(ids)), "bhist")
            for rec in missing:
                # bhist 키는 (job_id, array_index) — array element별 구분
                found = None
                if rec.job_id is not None:
                    found = (hist.get((rec.job_id, rec.array_index))
                             or hist.get((rec.job_id, None)))
                if found is not None:
                    state, exit_code = found
                    new = safe_transition(rec, state, exit_code=exit_code,
                                          guard=unchanged(rec))
                    if new is not None:
                        changed.append(new)
                elif probe_failed or not bhist_ok:
                    # 조회 수단 실패가 섞인 사이클 — LOST 확정 보류.
                    # LSF 순단이면 다음 사이클에서 정상 복구되고, 진짜
                    # 소실이면 장애 해소 후 사이클에서 확정된다 (FR-4.3)
                    log.warning("조회 실패로 %s 판단 보류 (LOST 확정 안 함)",
                                rec.job_key)
                else:
                    new = safe_transition(
                        rec, JobState.LOST,
                        fail_reason="NOT_FOUND_IN_LSF",
                        guard=unchanged(rec))
                    if new is None:
                        continue        # 그 사이 재제출/제거됨 — LOST 아님
                    changed.append(new)
                    lost.append(new)
                    log.error("job LOST 확정: %s (job_id=%s)",
                                rec.job_key, rec.job_id)

        return QueryResult(jobset_id, self.store.summary(jobset_id),
                           tuple(changed), tuple(lost), len(targets))

    @staticmethod
    def _try(fn, what: str) -> bool:
        """조회 수단 하나의 실패가 전체 polling을 죽이지 않게 격리 (CS-5).
        반환: 성공 여부 — False면 이번 사이클의 LOST 확정을 보류해야 한다."""
        try:
            fn()
        except LsfmgrError as e:
            log.warning("조회 실패(%s): %s", what, e)
            return False
        return True


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
        self._idle_counts: Dict[str, int] = {}   # 활동 없음 연속 사이클 수

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
        self._idle_counts.pop(jobset_id, None)
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
        except JobSetNotFoundError:
            # merge/삭제된 jobset — 계속 polling하면 매 주기 error 폭주
            log.info("jobset %s 삭제됨 — polling 자동 중지", jobset_id)
            self.stop_polling(jobset_id)
            return
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

        self._maybe_auto_stop(jobset_id)

    def _maybe_auto_stop(self, jobset_id: str) -> None:
        """AUTO-2 — 더 볼 것이 없으면 polling 자동 중지 (LSF 부하, NFR-4).

        ① 전원 terminal → 즉시 중지.
        ② 활동 없음(빈 jobset / cancel로 CREATED만 잔존): LSF에 있지도,
           submit 진행 중(SUBMITTING/RETRY_WAIT)도 아닌 상태가 2사이클
           연속이면 중지 — 1사이클 유예는 submit 직후 전원 CREATED인
           정상 순간을 조기 중지하지 않기 위함.
        """
        if not self._auto_stop or jobset_id not in self._timers:
            return
        try:
            remaining = self.querier.store.get_jobs(jobset_id)
        except LsfmgrError:
            return
        if remaining and all(r.state.is_terminal for r in remaining):
            log.info("jobset %s 전원 terminal — polling 자동 중지", jobset_id)
            self.stop_polling(jobset_id)
            return
        active = any(r.state.is_on_lsf
                     or r.state in (JobState.SUBMITTING, JobState.RETRY_WAIT)
                     for r in remaining)
        if active:
            self._idle_counts.pop(jobset_id, None)
            return
        n = self._idle_counts.get(jobset_id, 0) + 1
        self._idle_counts[jobset_id] = n
        if n >= 2:
            log.info("jobset %s 관찰 대상 없음(%d사이클) — polling 자동 중지",
                     jobset_id, n)
            self.stop_polling(jobset_id)


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
