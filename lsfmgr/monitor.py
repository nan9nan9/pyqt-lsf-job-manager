"""상태 조회/모니터링 (FR-4).

- JobsetQuerier: job_id chunked bjobs 조회 → 미발견은 LOST 전이.
  (v10: group/name 부착물 조회 제거 — id 기반 단일 경로.)
  blocking이므로 반드시 worker 스레드에서 호출.
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

#: 관측값이 None이면 저장값을 보존하는 병합 대상 확장 필드 (리뷰 M6)
_PRESERVE_ON_NONE = ("run_time_s", "start_time", "finish_time", "working_dir")


def _keep(new, old):
    """관측값 병합 — None(판정 불가)은 저장값 보존."""
    return new if new is not None else old


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
        self.store.get_jobset(jobset_id)     # 존재 검증 (없으면 예외)
        # 조회 대상(is_on_lsf)만 store 단에서 걸러 가져온다 — 대다수가
        # terminal인 대형 jobset에서 매 사이클 terminal 레코드까지 재구성해
        # 나르던 비용을 줄인다 (NFR-3. InMemory는 상태 필터만, SQL 백엔드를
        # 다시 붙인다면 (jobset_id,state) 인덱스 전제).
        targets = self.store.get_jobs(jobset_id, states=_ON_LSF)   # FR-4.2
        if not targets:
            return QueryResult(jobset_id, self.store.summary(jobset_id))

        # --- 1) job_id chunked 조회 — 유일한 조회 수단 (v10) ---
        # group/name 부착물 조회는 제거됐다: wrapper 제출 job은 부착물로
        # 커버되지 않고, explicit id 조회는 -a 없이도 CLEAN_PERIOD 내 종료
        # job을 보여주므로 id chunk가 모든 제출 경로를 균일하게 덮는다.
        # 조회 수단 실패("장애")와 "job이 LSF에 없음"은 반드시 구분한다 —
        # 장애를 없음으로 오판하면 LSF 순단 1회에 전원 LOST(terminal) 확정됨.
        # bjobs_by_ids는 chunk 단위 실패 격리형(예외 대신 실패 id 집합 반환) —
        # 실패 chunk의 job만 판단을 보류하고 성공 chunk는 정상 반영한다.
        statuses: Dict[Tuple[int, Optional[int]], JobStatus] = {}
        # by_id는 array_index별 최신값만 유지 — _aggregate_elements가 stale
        # 행을 섞어 folded 레코드를 잘못된 terminal로 확정하지 않게 한다.
        by_id: Dict[int, Dict[Optional[int], JobStatus]] = {}

        ids = sorted({r.job_id for r in targets if r.job_id is not None})
        bjobs_failed: set = set()
        if ids:
            sts, bjobs_failed = self.command.bjobs_by_ids(ids)
            for st in sts:
                statuses[(st.job_id, st.array_index)] = st
                by_id.setdefault(st.job_id, {})[st.array_index] = st

        # --- 2) 레코드 ↔ 조회 결과 매칭 ---
        def lookup(rec: JobRecord) -> Optional[JobStatus]:
            if rec.job_id is None:
                # id 미확보 레코드는 id 조회로 확인 자체가 불가 —
                # 복구/LOST 판단은 detect_lost(FR-5.3)의 몫이다
                return None
            st = statuses.get((rec.job_id, rec.array_index))
            if st is not None:
                return st
            # wrapper가 array job을 제출한 경우: 레코드는 (id, None)인데
            # bjobs는 element별 (id, idx) 행만 낸다 — 같은 id의 element들을
            # 집계해 대표 상태를 만든다 (안 하면 RUN 중인데 LOST 오판)
            if rec.array_index is None:
                elems = by_id.get(rec.job_id)
                if elems:
                    return _aggregate_elements(rec, list(elems.values()))
            return None

        # --- 3) 상태 반영 + bjobs 미발견분은 LOST 판정 (FR-4.3) ---
        # 이 사이클은 시작 시점 스냅샷(targets) 기반이고 bjobs 왕복 동안 수 초가
        # 흐른다 — 그 사이 레코드가 바뀌었으면(재제출(submit)의 리셋→재제출 등)
        # 스냅샷 기준 갱신이 새 job_id/상태를 옛 값으로 되돌린다. guard(CAS)로
        # "스냅샷과 동일할 때만" 전이시키고, 밀린 갱신은 다음 사이클에 맡긴다.
        def unchanged(rec: JobRecord):
            return lambda cur: (cur.job_id == rec.job_id
                                and cur.state is rec.state)

        # 전이는 개별 실행하지 않고 spec으로 모아 store.transition_many로
        # 한 번에 적용한다 — 수만 건이 한 사이클에 몰릴 때 건당 처리가
        # 폴링 스레드를 블로킹하지 않게 한다.
        update_specs = []       # bjobs 기반 일반 전이 [(key,state,guard,fields)]
        lost_specs = []         # LOST 전이 (반환분을 lost로도 분류)
        missing: List[JobRecord] = []
        runtime_updates = self.command.config.poll_runtime_updates
        collect_clusters = self.command.config.collect_clusters
        for rec in targets:
            st = lookup(rec)
            if st is None:
                missing.append(rec)
                continue
            # 확장 필드는 **유효값(eff) 기준**으로 비교·기록한다 — st가
            # None(포맷 강등/판정 불가)이면 저장값 보존(_keep, 리뷰 M6).
            # 그대로 쓰면 저장값이 None으로 소실되고 None!=저장값 비교가 매
            # 사이클 참이 되어 전 job이 재전이(jobs_updated 스팸)된다.
            # set-once 계약과 정합 — 재제출 리셋이 이 필드들을 지운다.
            # 클러스터 2필드는 collect_clusters일 때만(꺼짐: 무접촉).
            # run_time_s는 RUN 중 매 폴링 증가하므로 poll_runtime_updates
            # 일 때만 변경 감지에 포함(끄면 상태 전이 시점에만 반영).
            eff = {f: _keep(getattr(st, f), getattr(rec, f))
                   for f in _PRESERVE_ON_NONE}
            if collect_clusters:
                for f in ("source_cluster", "forward_cluster"):
                    eff[f] = _keep(getattr(st, f), getattr(rec, f))
            if (st.state is not rec.state or st.exit_code != rec.exit_code
                    or any(eff[f] != getattr(rec, f) for f in eff
                           if f != "run_time_s")
                    or (runtime_updates
                        and eff["run_time_s"] != rec.run_time_s)):
                fields = {"exit_code": st.exit_code, **eff,
                          "job_id": rec.job_id if rec.job_id is not None
                          else st.job_id}
                update_specs.append(
                    (rec.job_key, st.state, unchanged(rec), fields))

        deferred: List[str] = []             # 이번 사이클 판단 보류 job_key
        if missing:
            # v10.3: bhist fallback 삭제 — 최종 상태 복원 수단은 bjobs뿐이다.
            # explicit id 조회는 CLEAN_PERIOD 내 종료 job을 보여주므로 폴링
            # 주기(수십 초) 안에 종료를 관측한다. 그래도 안 보이면 purge된
            # 것이고, 그 job의 최종 상태는 어디서도 알 수 없다 = LOST.
            for rec in missing:
                if (rec.job_id in bjobs_failed
                        or (rec.job_id is None and bjobs_failed)):
                    # job_id 없는 레코드는 id 기반 조회로 확인 자체가 불가 —
                    # bjobs chunk 장애가 섞인 사이클엔 보수적으로 보류한다
                    # (FR-4.3). LSF 순단이면 다음 사이클에 정상 복구되고,
                    # 진짜 소실이면 장애 해소 후 사이클에서 확정된다. chunk
                    # 격리 덕에 다른 chunk에서 확인된 job은 정상 전이된다.
                    deferred.append(rec.job_key)
                else:
                    lost_specs.append((rec.job_key, JobState.LOST,
                                       unchanged(rec),
                                       {"fail_reason": "NOT_FOUND_IN_LSF"}))
            if deferred:
                # 사이클당 1줄로 집계 — job당 경고면 대형 jobset의 장애
                # 사이클마다 수백 줄이 반복돼 로그가 잠긴다 (NFR-6)
                log.warning("조회 실패로 %d건 판단 보류 (LOST 확정 안 함): "
                            "%s%s", len(deferred), ", ".join(deferred[:5]),
                            f" 외 {len(deferred) - 5}건"
                            if len(deferred) > 5 else "")

        # 전이 대상 소실(사이클 도중 remove_job)·guard 거부는 transition_many가
        # 조용히 건너뛰고 반환 목록에서 제외한다 (safe_transition과 동일 계약).
        changed: List[JobRecord] = list(
            self.store.transition_many(jobset_id, update_specs))
        lost = list(self.store.transition_many(jobset_id, lost_specs))
        for rec in lost:
            log.error("job LOST 확정: %s (job_id=%s)", rec.job_key, rec.job_id)
        changed.extend(lost)

        # 사이클 요약은 DEBUG — 정규 폴링은 매 주기라 INFO면 스팸(신호로 통지).
        # 변화가 있을 때만 남겨 조용한 사이클은 로그를 안 늘린다.
        if changed:
            log.debug("poll %s: 대상 %d, 전이 %d (LOST %d, 보류 %d)",
                      jobset_id, len(targets), len(changed), len(lost),
                      len(deferred))

        return QueryResult(jobset_id, self.store.summary(jobset_id),
                           tuple(changed), tuple(lost), len(targets))


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
        exit_code=exit_code,
        run_time_s=max(rts) if rts else None,
        start_time=min(starts) if starts else None,
        # 실행 중 element가 남았으면 종료 시각은 아직 실측이 아니다
        finish_time=(max(finishes) if finishes and not on else None),
        working_dir=cwds[0] if cwds else None,
        # MC — 한 array의 element들은 같은 클러스터로 forward된다(대표값)
        source_cluster=srcs[0] if srcs else None,
        forward_cluster=fwds[0] if fwds else None)


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
