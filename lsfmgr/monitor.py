"""상태 조회/모니터링 (FR-4).

- JobsetQuerier: 조회 전략(group → array → name → chunking) + bhist fallback
  → LOST 전이. blocking이므로 반드시 worker 스레드에서 호출.
- PollingService: 전용 QThread + 그 스레드 소속 QTimer로 주기 polling (QT-1).
  결과는 batch Signal로만 통지 (QT-4).
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .command import JobStatus, LsfCommand
from .errors import JobSetNotFoundError, LsfmgrError
from .qt import (
    DEFERRED_DELETE, QCoreApplication, QObject, QThread, QTimer, Signal, Slot,
)
from .states import _ON_LSF, JobRecord, JobState
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
        # 조회 대상(is_on_lsf)만 SQL 단에서 걸러 가져온다 — 전체 스캔 대신
        # 인덱스(jobset_id,state)로. 대다수가 terminal인 대형 jobset에서 매
        # 사이클 terminal 레코드까지 재구성하던 비용을 없앤다 (NFR-3).
        targets = self.store.get_jobs(jobset_id, states=_ON_LSF)   # FR-4.2
        if not targets:
            return QueryResult(jobset_id, self.store.summary(jobset_id))

        # --- 1) 부착물 기반 조회 (FR-4.1 우선순위) ---
        statuses: Dict[Tuple[int, Optional[int]], JobStatus] = {}
        by_name: Dict[str, JobStatus] = {}
        by_id: Dict[int, List[JobStatus]] = {}

        def collect(items: List[JobStatus]):
            for st in items:
                statuses[(st.job_id, st.array_index)] = st
                by_name[st.job_name] = st
                by_id.setdefault(st.job_id, []).append(st)

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
                # wrapper가 array job을 제출한 경우: 레코드는 (id, None)인데
                # bjobs는 element별 (id, idx) 행만 낸다 — 같은 id의 element들을
                # 집계해 대표 상태를 만든다 (안 하면 RUN 중인데 LOST 오판)
                if rec.array_index is None:
                    elems = by_id.get(rec.job_id)
                    if elems:
                        return _aggregate_elements(rec, elems)
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

        # 전이는 개별 실행하지 않고 spec으로 모아 store.transition_many로
        # 한 트랜잭션에 적용한다 — 수만 건이 한 사이클에 몰릴 때 건당 commit
        # (sqlite에서 건당 수 ms)이 폴링 스레드를 수십 초 블로킹하던 것을 없앤다.
        update_specs = []       # bjobs/bhist 기반 일반 전이 [(key,state,guard,fields)]
        lost_specs = []         # LOST 전이 (반환분을 lost로도 분류)
        missing: List[JobRecord] = []
        runtime_updates = self.command.config.poll_runtime_updates
        collect_clusters = self.command.config.collect_clusters
        for rec in targets:
            st = lookup(rec)
            if st is None:
                missing.append(rec)
                continue
            # 상태·exit_code 외에 실행시간 창(start/finish)·실행 디렉토리 변화도
            # 반영(start/finish/cwd는 set-once). run_time_s(경과 실행시간)은
            # RUN 중 매 폴링 증가하므로, poll_runtime_updates=True일 때만 갱신
            # 대상에 넣어 jobs_updated로 live runtime을 발행한다(끄면 상태 전이
            # 시점에만 반영 — 대량 job 폴링 부하 절감). 클러스터 필드는
            # collect_clusters일 때만 비교·기록한다 — 안 그러면 이전 세션이
            # 채운 forward_cluster를 (수집 안 하는) 이번 세션 폴링이 st의 None으로
            # 덮어 데이터가 소실된다(persistent+recover 시).
            if (st.state is not rec.state or st.exit_code != rec.exit_code
                    or st.start_time != rec.start_time
                    or st.finish_time != rec.finish_time
                    or st.working_dir != rec.working_dir
                    or (collect_clusters
                        and (st.source_cluster != rec.source_cluster
                             or st.forward_cluster != rec.forward_cluster))
                    or (runtime_updates
                        and st.run_time_s != rec.run_time_s)):
                fields = {
                    "exit_code": st.exit_code, "run_time_s": st.run_time_s,
                    "start_time": st.start_time, "finish_time": st.finish_time,
                    "working_dir": st.working_dir,
                    "job_id": rec.job_id if rec.job_id is not None
                    else st.job_id}
                if collect_clusters:     # 끄면 저장값 보존(덮지 않음)
                    fields["source_cluster"] = st.source_cluster
                    fields["forward_cluster"] = st.forward_cluster
                update_specs.append(
                    (rec.job_key, st.state, unchanged(rec), fields))

        if missing:
            hist: Dict = {}
            bhist_failed: set = set()        # bhist 조회 실패한 job_id (chunk 격리)
            ids = sorted({r.job_id for r in missing if r.job_id is not None})
            if ids:
                try:
                    hist, bhist_failed = self.command.bhist_states(ids)
                except LsfmgrError as e:
                    # 예상 밖 전면 실패는 전원 보류로 폴백 (기존 안전망 유지)
                    log.warning("조회 실패(bhist): %s", e)
                    bhist_failed = set(ids)
            for rec in missing:
                # bhist 키는 (job_id, array_index) — array element별 구분
                found = None
                if rec.job_id is not None:
                    found = (hist.get((rec.job_id, rec.array_index))
                             or hist.get((rec.job_id, None)))
                    if found is None and rec.array_index is None:
                        # wrapper가 제출한 array — element 블록들을 집계
                        entries = [v for (jid, _i), v in hist.items()
                                   if jid == rec.job_id]
                        if entries:
                            found = _aggregate_hist(entries)
                if found is not None:
                    state, exit_code = found
                    update_specs.append((rec.job_key, state, unchanged(rec),
                                         {"exit_code": exit_code}))
                elif probe_failed or rec.job_id in bhist_failed:
                    # 조회 수단 실패가 섞인 사이클 — LOST 확정 보류.
                    # probe(bjobs) 실패, 또는 이 job이 속한 bhist chunk가
                    # 실패한 경우. LSF 순단이면 다음 사이클에서 정상 복구되고,
                    # 진짜 소실이면 장애 해소 후 사이클에서 확정된다 (FR-4.3).
                    # bhist chunk 격리 덕에 다른 chunk에서 확인된 job은 여기서
                    # 안 걸리고 정상 전이/LOST 확정된다.
                    log.warning("조회 실패로 %s 판단 보류 (LOST 확정 안 함)",
                                rec.job_key)
                else:
                    lost_specs.append((rec.job_key, JobState.LOST,
                                       unchanged(rec),
                                       {"fail_reason": "NOT_FOUND_IN_LSF"}))

        # 전이 대상 소실(사이클 도중 remove_job)·guard 거부는 transition_many가
        # 조용히 건너뛰고 반환 목록에서 제외한다 (safe_transition과 동일 계약).
        changed: List[JobRecord] = list(
            self.store.transition_many(jobset_id, update_specs))
        lost = list(self.store.transition_many(jobset_id, lost_specs))
        for rec in lost:
            log.error("job LOST 확정: %s (job_id=%s)", rec.job_key, rec.job_id)
        changed.extend(lost)

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


def _aggregate_elements(rec: JobRecord,
                        elems: List[JobStatus]) -> JobStatus:
    """wrapper가 제출한 array job의 element 상태들을 레코드 1건(array_index
    None)의 대표 상태로 집계한다 — 하나라도 LSF에 살아있으면 진행 중,
    전원 종료면 EXIT(하나라도 실패) / DONE."""
    on = [e for e in elems if e.state.is_on_lsf]
    if on:
        pick = next((e for e in on if e.state is JobState.RUN), on[0])
        state = pick.state
        exit_code = None
    else:
        bad = [e for e in elems if e.state is JobState.EXIT]
        if bad:
            state = JobState.EXIT
            exit_code = next((e.exit_code for e in bad
                              if e.exit_code is not None), None)
        else:
            state, exit_code = JobState.DONE, None
    starts = [e.start_time for e in elems if e.start_time is not None]
    finishes = [e.finish_time for e in elems if e.finish_time is not None]
    rts = [e.run_time_s for e in elems if e.run_time_s is not None]
    cwds = [e.working_dir for e in elems if e.working_dir]
    srcs = [e.source_cluster for e in elems if e.source_cluster]
    fwds = [e.forward_cluster for e in elems if e.forward_cluster]
    return JobStatus(
        job_id=rec.job_id, array_index=None, state=state,
        exit_code=exit_code, job_name=rec.lsf_job_name,
        run_time_s=max(rts) if rts else None,
        start_time=min(starts) if starts else None,
        # 실행 중 element가 남았으면 종료 시각은 아직 실측이 아니다
        finish_time=(max(finishes) if finishes and not on else None),
        working_dir=cwds[0] if cwds else None,
        # MC — 한 array의 element들은 같은 클러스터로 forward된다(대표값)
        source_cluster=srcs[0] if srcs else None,
        forward_cluster=fwds[0] if fwds else None)


def _aggregate_hist(entries: List[Tuple[JobState, Optional[int]]]
                    ) -> Tuple[JobState, Optional[int]]:
    """bhist element 블록들의 (state, exit_code) 집계 — bhist는 종료 상태만
    기록하므로 하나라도 EXIT면 EXIT, 아니면 DONE."""
    bad = [(s, c) for s, c in entries if s is JobState.EXIT]
    if bad:
        return (JobState.EXIT,
                next((c for _s, c in bad if c is not None), None))
    return (JobState.DONE, 0)


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
        #: stop_all 완료(타이머 정리)를 shutdown이 quit 전에 확인하는 신호
        self.stopped_event = threading.Event()

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
        # 타이머 deleteLater를 폴링 스레드에서 즉시 처리한다 — 이 슬롯이
        # 이 스레드에서 실행되므로 여기서 flush하지 않으면, 이벤트 루프가
        # quit된 뒤 타이머가 다른 스레드에서 파괴돼 Qt 위반(killTimer from
        # another thread)이 난다. DeferredDelete만 골라 보내 재진입은 없다.
        QCoreApplication.sendPostedEvents(None, DEFERRED_DELETE)
        self.stopped_event.set()             # shutdown이 quit 전에 대기

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
        # stop_all이 폴링 스레드에서 타이머를 정지·삭제한 뒤에 quit해야
        # 타이머가 그 스레드에서 파괴된다. quit을 먼저 하면 stop_all이
        # 루프 종료로 실행되지 못해 타이머가 main 스레드에서 파괴된다.
        # (mid-query 등으로 stop_all이 안 돌면 타임아웃 후 그대로 진행 —
        # requestInterruption+quit+wait의 기존 안전망으로 폴백, 행 없음)
        self._worker.stopped_event.clear()
        self._req_stop_all.emit()
        self._worker.stopped_event.wait(5.0)
        self._thread.requestInterruption()
        self._thread.quit()
        if not self._thread.wait(10000):
            log.error("polling 스레드 종료 대기 초과")
