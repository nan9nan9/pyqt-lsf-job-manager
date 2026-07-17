"""StatePacer — 상태 전이 **표시** 최소 간격 보장 (min_state_dwell_s).

문제: 상태가 실제로 그렇게 빨리 바뀐다. SUBMITTING→PEND는 bsub 왕복
(수백 ms)만큼만, 재제출의 EXIT→SUBMITTING(리셋)은 거의 0초 만에 지나가
GUI에서는 중간 상태가 보이지 않고 곧장 최종 상태만 찍힌다.

해결: jobs_updated **발화**를 job_key별로 큐잉해, 한 상태가 화면에 최소
dwell(기본 1초)만큼 머문 뒤에야 다음 전이를 내보낸다. 전이는 **버리지
않고 순서대로** 흘려보내므로 EXIT→SUBMITTING→PEND가 1초 간격으로 차례로
보인다. dwell이 0이면 pacer 자체를 만들지 않는다(발화 경로 무변경).

    t=0.0  store: SUBMITTING  → jobs_updated([SUBMITTING])   표: SUBMITTING
    t=0.3  store: PEND        → (큐에 보류)                   표: SUBMITTING
    t=1.0                     → jobs_updated([PEND])          표: PEND

이 모듈은 **표시만** 늦춘다 — store는 늘 즉시 진실을 쓴다. 그래서 켜면
jobs_updated에 한해 두 계약이 느슨해진다 (README §5.1):

  - store-first: 지연된 jobs_updated의 slot에서 js.jobs()를 pull하면
    신호보다 **앞선** 상태가 보인다 (store는 이미 PEND).
  - finished-last: submit_finished/kill_finished가 마지막 전이 배치보다
    먼저 도착할 수 있다 (위 예에서 t=0.4의 finished).
  라이브러리 내부 판정(post_process·can_submit·detect_lost·handler)은 전부
  store를 직접 보므로 pacer의 영향을 받지 않는다.

스레드: manager와 같은 스레드(main)에서만 쓰인다 — push()는 jobs_updated를
발화하던 manager slot들이(전부 queued connection으로 main에 도착) 호출하고,
drain은 QTimer가 같은 스레드에서 호출한다. 그래서 lock이 없다.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Dict, List, Optional, Tuple

from .qt import QObject, QTimer
from .states import JobRecord, JobState

log = logging.getLogger("lsfmgr.pacer")

__all__ = ["StatePacer"]

#: 타이머가 due 직전에 깨어나도 그 tick에서 배출 — 안 그러면 남은 1~2ms짜리
#: 타이머를 다시 걸어 한 전이가 두 tick으로 쪼개진다 (dwell 오차는 그 tolerance만큼).
_DUE_TOLERANCE_S = 0.005

#: _shown 청소 임계 — 이 수를 넘길 때만 dwell 지난(더는 쓸모없는) 항목을 훑어 지운다.
#: 항목은 (state, 시각) 튜플 2개뿐이라 임계 자체가 곧 상한이 아니라 청소 주기다.
_PRUNE_AT = 4096


class StatePacer(QObject):
    """job_key별 전이 dwell 큐. manager가 1개를 소유하고 jobs_updated 발화를
    전부 이 클래스를 통과시킨다.

    emit_fn(jobset_id, [JobRecord]) — 실제 발화 콜백 (보통 jobs_updated.emit).
    """

    def __init__(self, dwell_s: float,
                 emit_fn: Callable[[str, List[JobRecord]], None],
                 parent: Optional[QObject] = None):
        super().__init__(parent)
        self._dwell = float(dwell_s)
        self._emit_fn = emit_fn
        #: (jobset_id, job_key) → (표시 중인 상태, 그 상태로 바뀐 시각)
        self._shown: Dict[Tuple[str, str], Tuple[JobState, float]] = {}
        #: (jobset_id, job_key) → 아직 못 내보낸 전이 FIFO
        self._queue: Dict[Tuple[str, str], List[JobRecord]] = {}
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._drain)

    # ------------------------------------------------------------------
    # 입력
    # ------------------------------------------------------------------
    def push(self, jobset_id: str, records: List[JobRecord]) -> None:
        """변경분 배치를 접수 — 바로 낼 수 있는 것만 그 자리에서 발화하고,
        dwell이 안 찬 전이는 큐에 넣어 타이머로 미룬다."""
        now = time.monotonic()
        due: List[JobRecord] = []
        for rec in records:
            key = (jobset_id, rec.job_key)
            q = self._queue.get(key)
            if q is not None:
                _enqueue(q, rec)          # 이미 밀려 있음 — 순서 보존
                continue
            shown = self._shown.get(key)
            if shown is not None and rec.state is shown[0]:
                # 전이가 아님(run_time_s 갱신 등) — 지연 없이 통과시키고
                # dwell 시계도 건드리지 않는다. 안 그러면 poll_runtime_updates가
                # 매 폴링마다 시계를 되감아 진짜 전이가 영영 안 나간다.
                due.append(rec)
                continue
            if shown is None or now - shown[1] >= self._dwell - _DUE_TOLERANCE_S:
                due.append(rec)           # 첫 발행이거나 이미 충분히 머물렀음
                self._shown[key] = (rec.state, now)
            else:
                self._queue[key] = [rec]
        if due:
            self._safe_emit(jobset_id, due)
        self._prune(now)
        self._reschedule(now)

    def forget(self, jobset_id: str,
               job_keys: Optional[List[str]] = None) -> None:
        """사라진 job(remove/clear)·jobset(merge source)의 보류분을 버린다.
        안 버리면 dwell 창(최대 dwell초) 안에 지워진 job의 전이가 뒤늦게
        발화돼, job_key로 행을 채우는 표에 삭제된 행이 되살아난다.
        job_keys=None이면 그 jobset 전체."""
        drop = ([(jobset_id, k) for k in job_keys] if job_keys is not None
                else [k for k in self._queue if k[0] == jobset_id]
                + [k for k in self._shown if k[0] == jobset_id])
        for key in drop:
            self._queue.pop(key, None)
            self._shown.pop(key, None)
        self._reschedule(time.monotonic())

    def stop(self) -> None:
        """보류분을 **즉시 전부** 발화하고 이후 push는 지연 없이 통과시킨다
        (shutdown용, 멱등). 타이머는 이벤트루프가 돌아야 뛰므로, 종료 중에
        큐를 안고 있으면 그 전이는 GUI에 영영 안 나타난다."""
        self._timer.stop()
        self._dwell = 0.0                 # 이후 push는 전부 즉시 통과
        queued, self._queue = self._queue, {}
        batches: Dict[str, List[JobRecord]] = {}
        for (jsid, _job_key), recs in queued.items():
            # 같은 job의 밀린 전이는 마지막 것만 — 종료 시점엔 중간 과정을
            # 보여줄 화면 시간이 없다. 최종 상태가 맞게 남는 것이 유일한 목표.
            batches.setdefault(jsid, []).append(recs[-1])
        for jsid, recs in batches.items():
            self._safe_emit(jsid, recs)
        self._shown.clear()

    # ------------------------------------------------------------------
    # 배출
    # ------------------------------------------------------------------
    def _drain(self) -> None:
        """dwell이 찬 job마다 큐에서 **1건씩** 꺼내 발화 — 같은 tick의 배출은
        jobset별 한 배치로 합친다(대량이어도 신호 폭주 없음)."""
        now = time.monotonic()
        batches: Dict[str, List[JobRecord]] = {}
        for key in list(self._queue):
            shown = self._shown.get(key)
            if shown is not None and now - shown[1] < self._dwell - _DUE_TOLERANCE_S:
                continue                  # 아직 머무는 중
            q = self._queue[key]
            rec = q.pop(0)
            if not q:
                del self._queue[key]
            self._shown[key] = (rec.state, now)
            batches.setdefault(key[0], []).append(rec)
        for jsid, recs in batches.items():
            self._safe_emit(jsid, recs)
        self._reschedule(now)

    def _reschedule(self, now: float) -> None:
        """남은 큐 중 가장 이른 due에 타이머를 맞춘다 (없으면 정지)."""
        due_at = None
        for key in self._queue:
            shown = self._shown.get(key)
            t = shown[1] + self._dwell if shown is not None else now
            if due_at is None or t < due_at:
                due_at = t
        if due_at is None:
            self._timer.stop()
            return
        self._timer.start(max(0, int((due_at - now) * 1000.0 + 0.5)))

    def _prune(self, now: float) -> None:
        """dwell이 지나 더는 판단에 쓰이지 않는 _shown 항목 청소. 진행 중
        jobset(전원이 dwell 안)에선 아무것도 안 지워지지만, push는 이미
        스로틀돼 있어(0.5초 배치) 훑는 비용이 문제되지 않는다."""
        if len(self._shown) <= _PRUNE_AT:
            return
        for key in [k for k, (_s, t) in self._shown.items()
                    if now - t > self._dwell and k not in self._queue]:
            del self._shown[key]

    def _safe_emit(self, jobset_id: str, records: List[JobRecord]) -> None:
        """user slot 예외 격리 (CS-5) — _drain은 Qt slot이라 예외가 나가면
        PyQt에서 abort로 이어지고, 남은 배출/_reschedule을 건너뛰어 큐가
        영영 안 빠진다."""
        try:
            self._emit_fn(jobset_id, records)
        except Exception:                 # noqa: BLE001
            log.exception("jobs_updated slot 예외(무시)")


def _enqueue(queue: List[JobRecord], rec: JobRecord) -> None:
    """FIFO 추가 — 직전 대기분과 같은 상태면 교체한다. 전이가 아니라 같은
    상태의 필드 갱신(run_time_s 등)이므로 dwell을 한 칸 더 쓰면 안 된다."""
    if queue and queue[-1].state is rec.state:
        queue[-1] = rec
    else:
        queue.append(rec)
