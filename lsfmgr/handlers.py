"""JobSetHandlerService — JobSet별 사용자 handler 주기 실행 (FR-7).

JobSet 하나에 이름 있는 handler를 붙여, 지정한 state 구간 동안 몇 초마다
worker 스레드에서 실행한다. 각 job이 시작 state에 들어가면 주기 실행을 켜고,
종료 state에 도달하면 **마지막으로 한 번 더** 실행한 뒤 끈다.

- tick(QTimer)은 main 스레드에서 돌며 Store 스냅샷으로 실행 여부만 판단하고,
  실제 handler 호출은 QThreadPool worker에서 수행한다 (GUI freeze 방지).
- handler 반환값(처리한 데이터)은 `finished(jobset_id, name, HandlerResult)`
  Signal로 전달된다 — 이름으로 필터링해서 구독한다.
- handler는 상태 갱신을 Store에서 읽으므로 **polling이 돌고 있어야** state 전이를
  본다(폴링이 멈춰 있으면 handler도 진행하지 않는다).
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional, Tuple, Union

from .errors import LsfmgrError
from .qt import QObject, QRunnable, QThread, QThreadPool, QTimer, Signal
from .states import JobRecord, JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.handler")

#: 기본 시작 state — job이 실제로 돌기 시작한 시점
DEFAULT_START_STATES: FrozenSet[JobState] = frozenset({JobState.RUN})
#: 기본 종료 state — terminal 전부 (여기 도달하면 최종 실행 후 종료)
DEFAULT_END_STATES: FrozenSet[JobState] = frozenset({
    JobState.DONE, JobState.EXIT, JobState.SUBMIT_FAILED, JobState.LOST})

StateSpec = Union[JobState, Iterable[JobState], None]


def _as_states(x: StateSpec, default: FrozenSet[JobState]) -> FrozenSet[JobState]:
    if x is None:
        return default
    if isinstance(x, JobState):
        return frozenset({x})
    return frozenset(x)


@dataclass(frozen=True)
class HandlerContext:
    """handler 호출 시 넘어오는 인자 — job 참조 포인트."""
    jobset_id: str
    record: JobRecord          # job_id / lsf_job_name / command / state 등
    final: bool                # 종료 state에서의 마지막 실행이면 True

    @property
    def job_id(self) -> Optional[int]:
        return self.record.job_id

    @property
    def job_key(self) -> str:
        return self.record.job_key

    @property
    def working_dir(self) -> Optional[str]:
        """LSF 실행 디렉토리(exec_cwd) — 실행 시작 후 채워진다."""
        return self.record.working_dir


@dataclass(frozen=True)
class HandlerResult:
    """handler 1회 실행 결과 — finished Signal로 전달."""
    handler_name: str
    jobset_id: str
    job_key: str
    job_id: Optional[int]
    final: bool
    data: Any = None                 # handler 반환값(처리한 데이터)
    error: Optional[str] = None      # 예외 발생 시 repr, 정상이면 None


# job별 handler 진행 상태
_PENDING, _RUNNING, _FINISHED = "PENDING", "RUNNING", "FINISHED"


@dataclass
class _Handler:
    jobset_id: str
    name: str
    fn: Callable[[HandlerContext], Any]
    interval_s: float
    start_states: FrozenSet[JobState]
    end_states: FrozenSet[JobState]
    timer: QTimer
    status: Dict[str, str] = field(default_factory=dict)   # job_key → 진행 상태
    inflight: set = field(default_factory=set)             # 실행 중인 job_key
    lock: threading.Lock = field(default_factory=threading.Lock)


class JobSetHandlerService(QObject):
    """JobSet별 handler 등록/주기 실행 관리. manager(Facade)가 소유."""

    finished = Signal(str, str, object)      # jobset_id, handler_name, HandlerResult
    # worker 스레드에서의 remove_handler 요청을 main으로 위임 (queued)
    _remove_requested = Signal(str, str)     # jobset_id, handler_name

    def __init__(self, store: JobSetStore, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.store = store
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(4)
        self._handlers: Dict[Tuple[str, str], _Handler] = {}
        self._remove_requested.connect(self.remove_handler)

    # ------------------------------------------------------------------
    # 등록/해제
    # ------------------------------------------------------------------
    def add_handler(self, jobset_id: str, name: str,
                    fn: Callable[[HandlerContext], Any], *,
                    interval_s: float = 10.0,
                    start_states: StateSpec = None,
                    end_states: StateSpec = None) -> None:
        """[main] jobset_id에 이름 있는 handler 등록 — 즉시 주기 실행 시작.

        interval_s초마다 각 job을 검사해서, start_states에 들어간 job에 대해
        handler(fn)를 worker에서 실행하고, end_states 도달 시 마지막으로 한 번
        더 실행한다. fn(ctx)의 반환값은 finished Signal로 전달된다.
        모든 job이 최종 실행까지 끝나면 **휴면**(타이머 정지, 등록 유지) —
        resubmit_jobs 재실행 시 자동 재가동되고, 완전 해제는 remove_handler.
        """
        if interval_s <= 0:
            raise ValueError("interval_s는 0보다 커야 합니다")
        if QThread.currentThread() is not self.thread():
            # worker 스레드(예: handler fn 안)에서 부르면 QTimer가 cross-thread
            # 로 생성돼 조용히 발화하지 않는다 — 명시적으로 거부한다
            raise LsfmgrError(
                "add_handler는 main 스레드에서만 호출할 수 있습니다")
        key = (jobset_id, name)
        if key in self._handlers:
            raise ValueError(f"handler 이름 중복: {jobset_id}/{name}")
        timer = QTimer(self)
        h = _Handler(
            jobset_id=jobset_id, name=name, fn=fn, interval_s=interval_s,
            start_states=_as_states(start_states, DEFAULT_START_STATES),
            end_states=_as_states(end_states, DEFAULT_END_STATES),
            timer=timer)
        self._handlers[key] = h
        timer.timeout.connect(lambda h=h: self._tick(h))
        timer.start(int(interval_s * 1000))

    def remove_handler(self, jobset_id: str, name: str) -> None:
        """handler 완전 해제 — 타이머 중지. 실행 중 task는 자연 종료된다.
        worker 스레드(handler fn 안 포함)에서 불러도 안전하다 — main으로
        위임된다 (QTimer는 소속 스레드 밖에서 멈출 수 없다)."""
        if QThread.currentThread() is not self.thread():
            self._remove_requested.emit(jobset_id, name)   # → main 스레드
            return
        h = self._handlers.pop((jobset_id, name), None)
        if h is not None:
            h.timer.stop()

    def rearm(self, jobset_id: str, job_keys: Iterable[str]) -> None:
        """[main] 지정 job들의 handler 진행 상태를 리셋 (resubmit_jobs 용).

        재실행되는 job은 _FINISHED로 남아 있으면 새 실행에서 handler가 영영
        침묵하고, _RUNNING으로 남아 있으면 아직 안 뜬 레코드에 발화한다 —
        _PENDING으로 되돌려 새 실행의 start/end 주기를 다시 돌게 한다.
        전원 완료로 휴면(타이머 정지)된 handler는 다시 가동한다."""
        keys = set(job_keys)
        for (jsid, _name), h in self._handlers.items():
            if jsid != jobset_id:
                continue
            with h.lock:
                for key in keys:
                    h.status.pop(key, None)     # → _PENDING (기본값)
            if not h.timer.isActive():          # 휴면 해제 (자동 재가동)
                h.timer.start(int(h.interval_s * 1000))

    def remove_all(self, jobset_id: str) -> None:
        """[main] jobset의 모든 handler 해제 (close/merge 시)."""
        for name in [n for (j, n) in self._handlers if j == jobset_id]:
            self.remove_handler(jobset_id, name)

    def shutdown(self) -> None:
        for h in list(self._handlers.values()):
            h.timer.stop()
        self._handlers.clear()
        self._pool.waitForDone(-1)

    # ------------------------------------------------------------------
    # tick (main 스레드) — 실행 여부 판단만, 실제 호출은 worker
    # ------------------------------------------------------------------
    def _tick(self, h: _Handler) -> None:
        try:
            recs = self.store.get_jobs(h.jobset_id)
        except LsfmgrError:
            # jobset이 사라짐(삭제/merge) — handler 정리
            self.remove_handler(h.jobset_id, h.name)
            return
        except Exception:                        # noqa: BLE001 — CS-5
            # store 일시 장애(sqlite lock 등) — 이 tick만 건너뛰고 다음
            # tick에 재시도. QTimer slot 밖으로 전파되면 PyQt는 abort한다
            log.exception("handler tick 조회 실패: %s/%s",
                          h.jobset_id, h.name)
            return

        with h.lock:
            for rec in recs:
                st = h.status.get(rec.job_key, _PENDING)
                if st == _FINISHED or rec.job_key in h.inflight:
                    continue
                in_end = rec.state in h.end_states
                in_start = rec.state in h.start_states
                # end_states에 없는 terminal(예: end={DONE}인데 EXIT) —
                # 더 진행할 수 없으니 최종 실행 없이 종결한다. 안 하면 죽은
                # job에 매 tick 발화하고 타이머가 영원히 안 멈춘다
                if rec.state.is_terminal and not in_end:
                    h.status[rec.job_key] = _FINISHED
                    continue
                # 아직 시작 state에 안 왔고 종료도 아니면 대기.
                # (_RUNNING은 start를 벗어나도 계속 돈다 — start_states는
                # "켜는 조건"이지 "도는 구간"이 아니다. resubmit 리셋 레코드는
                # rearm()이 _PENDING으로 되돌려 이 규칙으로 다시 대기한다)
                if st == _PENDING and not in_start and not in_end:
                    continue
                final = in_end
                h.status[rec.job_key] = _FINISHED if final else _RUNNING
                h.inflight.add(rec.job_key)
                self._pool.start(_HandlerTask(self, h, rec, final))

            # 모든 job이 최종 실행까지 끝났으면 **휴면**(타이머만 정지, 등록
            # 유지). 완전 해제하면 "전부 끝난 job을 재실행"(resubmit의 대표
            # 사용례)에서 rearm이 no-op이 되어 handler가 조용히 사라진다 —
            # 휴면이면 rearm이 타이머를 재가동한다. 빈 jobset도 휴면 처리
            # (빈 tick이 영원히 도는 것 방지).
            done = (all(h.status.get(r.job_key) == _FINISHED for r in recs)
                    and not h.inflight)
        if done:
            h.timer.stop()
            log.debug("handler 휴면: %s/%s (전원 최종 실행 완료)",
                      h.jobset_id, h.name)

    # worker 스레드에서 호출
    def _run(self, h: _Handler, rec: JobRecord, final: bool) -> None:
        try:
            data = h.fn(HandlerContext(h.jobset_id, rec, final))
            result = HandlerResult(h.name, h.jobset_id, rec.job_key,
                                   rec.job_id, final, data=data)
        except Exception as e:                       # noqa: BLE001 — CS-5
            log.exception("handler 실행 실패: %s/%s", h.jobset_id, h.name)
            result = HandlerResult(h.name, h.jobset_id, rec.job_key,
                                   rec.job_id, final, error=repr(e))
        finally:
            with h.lock:
                h.inflight.discard(rec.job_key)
        self.finished.emit(h.jobset_id, h.name, result)


class _HandlerTask(QRunnable):
    """handler 1회 실행 worker."""

    def __init__(self, service: JobSetHandlerService, h: _Handler,
                 rec: JobRecord, final: bool):
        super().__init__()
        self.setAutoDelete(True)
        self.service = service
        self.h = h
        self.rec = rec
        self.final = final

    def run(self):
        self.service._run(self.h, self.rec, self.final)
