"""JobSetHandlerService — JobSet별 사용자 handler (FR-7).

JobSet 하나에 이름 있는 handler를 붙여, 지정한 state 구간 동안 **폴링 사이클마다**
worker 스레드에서 실행한다. 각 job이 시작 state(기본 RUN)에 들어가면 실행을 켜고,
종료 state(기본 DONE/EXIT)에 도달하면 **마지막으로 한 번 더** 실행한 뒤 끝낸다.

- handler는 별도 타이머를 갖지 않는다 — LsfJobManager의 **폴링이 bjobs로 Store를
  갱신한 직후** 평가된다(`tick`). 그래서 handler가 보는 상태는 항상 방금 폴링된
  최신값이고, 주기도 `poll_interval_s` 하나로 통일된다.
- tick은 main 스레드에서 실행 여부만 판단하고, 실제 handler 호출은 QThreadPool
  worker에서 수행한다 (GUI freeze 방지).
- 반환값(처리한 데이터)은 `finished(jobset_id, name, HandlerResult)` Signal로
  전달된다 — 이름으로 필터링해 구독한다.
- **폴링이 돌고 있어야** 동작한다 (handler는 폴링 사이클에 tie돼 있음).
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional, Tuple, Union

from .errors import LsfmgrError
from .qt import QObject, QRunnable, QThread, QThreadPool, Signal
from .states import JobRecord, JobState

log = logging.getLogger("lsfmgr.handler")

#: 기본 시작 state — job이 실제로 돌기 시작한 시점
DEFAULT_START_STATES: FrozenSet[JobState] = frozenset({JobState.RUN})
#: 기본 종료 state — DONE/EXIT (여기 도달하면 최종 실행 후 종료)
DEFAULT_END_STATES: FrozenSet[JobState] = frozenset({
    JobState.DONE, JobState.EXIT})

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
    start_states: FrozenSet[JobState]
    end_states: FrozenSet[JobState]
    status: Dict[str, str] = field(default_factory=dict)   # job_key → 진행 상태
    inflight: set = field(default_factory=set)             # 실행 중인 job_key
    lock: threading.Lock = field(default_factory=threading.Lock)


class JobSetHandlerService(QObject):
    """JobSet별 handler 등록/실행 관리 — 폴링 사이클 구동. manager가 소유."""

    finished = Signal(str, str, object)      # jobset_id, handler_name, HandlerResult
    # worker 스레드에서의 remove_handler 요청을 main으로 위임 (queued)
    _remove_requested = Signal(str, str)     # jobset_id, handler_name

    def __init__(self, store, parent: Optional[QObject] = None):
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
                    start_states: StateSpec = None,
                    end_states: StateSpec = None) -> None:
        """[main] jobset_id에 이름 있는 handler 등록.

        폴링 사이클마다(= bjobs 갱신 직후) 각 job을 검사해서, start_states(기본
        {RUN})에 들어간 job에 대해 handler(fn)를 worker에서 실행하고, end_states
        (기본 {DONE, EXIT}) 도달 시 마지막으로 한 번 더 실행한다(final=True).
        fn(ctx)의 반환값은 finished Signal로 전달된다.
        **폴링이 돌고 있어야 동작**하며, 첫 실행은 다음 폴링 사이클이다.
        """
        if QThread.currentThread() is not self.thread():
            # _handlers/status는 main(tick)과 공유돼 worker에서 등록하면
            # 순회 중 변경 경합이 난다 — main 전용으로 강제한다
            raise LsfmgrError(
                "add_handler는 main 스레드에서만 호출할 수 있습니다")
        key = (jobset_id, name)
        if key in self._handlers:
            raise ValueError(f"handler 이름 중복: {jobset_id}/{name}")
        self._handlers[key] = _Handler(
            jobset_id=jobset_id, name=name, fn=fn,
            start_states=_as_states(start_states, DEFAULT_START_STATES),
            end_states=_as_states(end_states, DEFAULT_END_STATES))

    def remove_handler(self, jobset_id: str, name: str) -> None:
        """handler 해제. worker 스레드(handler fn 안 포함)에서 불러도 안전하다
        — main으로 위임된다."""
        if QThread.currentThread() is not self.thread():
            self._remove_requested.emit(jobset_id, name)   # → main 스레드
            return
        self._handlers.pop((jobset_id, name), None)

    def rearm(self, jobset_id: str, job_keys: Iterable[str]) -> None:
        """[main] 지정 job들의 handler 진행 상태를 리셋 (resubmit_jobs 용).
        _FINISHED로 남으면 재실행에서 handler가 영영 침묵하므로 _PENDING으로
        되돌려 새 실행의 start/end 주기를 다시 돌게 한다."""
        keys = set(job_keys)
        for (jsid, _name), h in self._handlers.items():
            if jsid != jobset_id:
                continue
            with h.lock:
                for key in keys:
                    h.status.pop(key, None)     # → _PENDING (기본값)

    def remove_all(self, jobset_id: str) -> None:
        """[main] jobset의 모든 handler 해제 (close/merge 시)."""
        for name in [n for (j, n) in self._handlers if j == jobset_id]:
            self.remove_handler(jobset_id, name)

    def shutdown(self) -> None:
        self._handlers.clear()
        self._pool.waitForDone(-1)

    # ------------------------------------------------------------------
    # tick (main 스레드) — 폴링 갱신 직후 호출됨. 실행 여부만 판단, 호출은 worker
    # ------------------------------------------------------------------
    def tick(self, jobset_id: str) -> None:
        """[main] 이 jobset의 모든 handler를 1회 평가 — 폴링 사이클마다 호출한다.
        Store는 방금 폴링으로 갱신됐으므로 handler가 보는 상태는 최신값이다."""
        hs = [h for (jsid, _n), h in self._handlers.items()
              if jsid == jobset_id]
        if not hs:
            return
        try:
            recs = self.store.get_jobs(jobset_id)
        except LsfmgrError:
            self.remove_all(jobset_id)          # jobset 사라짐(삭제/merge)
            return
        except Exception:                        # noqa: BLE001 — CS-5
            # store 일시 장애 — 이번 사이클만 건너뛴다 (다음 폴링에 재시도).
            # slot 밖으로 전파되면 PyQt는 abort한다
            log.exception("handler tick 조회 실패: %s", jobset_id)
            return
        for h in hs:
            self._run_cycle(h, recs)

    def _run_cycle(self, h: _Handler, recs) -> None:
        with h.lock:
            for rec in recs:
                st = h.status.get(rec.job_key, _PENDING)
                if st == _FINISHED or rec.job_key in h.inflight:
                    continue
                in_end = rec.state in h.end_states
                in_start = rec.state in h.start_states
                # end_states에 없는 terminal(예: end={DONE}인데 EXIT/LOST/
                # SUBMIT_FAILED) — 더 진행할 수 없으니 최종 실행 없이 종결.
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
