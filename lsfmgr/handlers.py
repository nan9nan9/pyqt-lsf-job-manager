"""JobSetHandlerService — JobSet별 사용자 handler.

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
from collections import deque
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

#: 동시에 도는 handler 수 상한 (= drain worker 수).
MAX_HANDLER_WORKERS = 4


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
    record: JobRecord          # job_id / job_key / command / state 등
    final: bool                # 종료 state에서의 마지막 실행이면 True

    @property
    def job_id(self) -> Optional[int]:
        return self.record.job_id

    @property
    def job_key(self) -> str:
        return self.record.job_key

    @property
    def submit_cwd(self) -> Optional[str]:
        """이 job의 작업 디렉토리 — create_jobset의 work_dir(s) 요청값이다.
        None이면 부모 프로세스 cwd에서 실행됐다는 뜻(`os.getcwd()`로 보완).
        (v10.4: bjobs exec_cwd 관측값이던 working_dir을 대체 — 같은 경로를
        가리키면서 RUN 전에는 비어 있어 헷갈리기만 했다.)"""
        return self.record.submit_cwd


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
    lock: threading.Lock = field(default_factory=threading.Lock)
    # 실행 중(inflight) 표식은 이 객체가 아니라 **서비스**가 들고 있다
    # (JobSetHandlerService._inflight) — remove_handler로 이 객체가 버려져도
    # "같은 job에 handler가 겹쳐 돌지 않는다"는 불변식이 유지돼야 하기 때문.


class JobSetHandlerService(QObject):
    """JobSet별 handler 등록/실행 관리 — 폴링 사이클 구동. manager가 소유."""

    finished = Signal(str, str, object)      # jobset_id, handler_name, HandlerResult
    # worker 스레드에서의 remove_handler 요청을 main으로 위임 (queued)
    _remove_requested = Signal(str, str)     # jobset_id, handler_name
    # handler 실행 종료 직후 그 job 1건 재평가 (worker → main, queued)
    _recheck = Signal(str, str, str)         # jobset_id, handler_name, job_key

    def __init__(self, store, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.store = store
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(MAX_HANDLER_WORKERS)
        self._handlers: Dict[Tuple[str, str], _Handler] = {}
        # 실행 표식은 handler 객체 밖에 보관해 삭제·재등록 중 중복 실행을 막는다.
        # 키는 (jobset_id, handler_name, job_key)이며 서로 다른 handler는 독립 실행한다.
        self._inflight: set = set()
        self._inflight_lock = threading.Lock()
        # 작업은 큐에 모으고 제한된 수의 worker가 꺼내 실행한다.
        # job마다 QThreadPool.start를 호출하는 GUI 스레드 비용을 피한다.
        self._queue: deque = deque()
        self._queue_lock = threading.Lock()
        self._draining = 0                   # 도는 drain worker 수
        self._remove_requested.connect(self.remove_handler)
        self._recheck.connect(self._on_recheck)

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
        """[main] 지정 job들의 handler 진행 상태를 리셋 (mgr.submit 재제출 용).
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
        """[main] jobset의 모든 handler 해제 (remove_jobset 시)."""
        for name in [n for (j, n) in self._handlers if j == jobset_id]:
            self.remove_handler(jobset_id, name)

    def shutdown(self) -> None:
        """[main] 종료 — 대기 중인 **중간 실행분은 버리고** 최종 실행분만 돌린다.

        중간 실행(final=False)은 "이번 폴링 사이클의 수집"이라 종료 시점에
        굳이 따라잡을 이유가 없다. 그대로 두면 큐에 쌓인 만큼 종료가 밀린다
        — job 60건 x 핸들러 0.5초면 shutdown이 7.3초였다(실측).
        최종 실행(final=True)은 "이 job은 이렇게 끝났다"는 마지막 수집이라
        버리지 않는다. 이미 도는 것은 앱 코드라 멈출 수 없으니 기다린다."""
        self._handlers.clear()
        with self._queue_lock:
            dropped = [q for q in self._queue if not q[2]]
            if dropped:
                self._queue = deque(q for q in self._queue if q[2])
        if dropped:
            log.info("shutdown: 대기 중이던 handler 중간 실행 %d건 생략 "
                     "(최종 실행분은 그대로 수행)", len(dropped))
        self._pool.waitForDone(-1)       # 도는 task가 각자 표식을 해제한다
        with self._inflight_lock:
            self._inflight.clear()       # 혹시 남은 잔재까지 정리

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
            self.remove_all(jobset_id)          # jobset 사라짐(remove_jobset)
            return
        except Exception:                        # noqa: BLE001
            # store 일시 장애 — 이번 사이클만 건너뛴다 (다음 폴링에 재시도).
            # slot 밖으로 전파되면 PyQt는 abort한다
            log.exception("handler tick 조회 실패: %s", jobset_id)
            return
        for h in hs:
            self._run_cycle(h, recs)
        self._pump()                        # 쌓인 대기분에 worker 배정

    def _run_cycle(self, h: _Handler, recs) -> None:
        with h.lock:
            for rec in recs:
                self._eval_record(h, rec)

    def _eval_record(self, h: _Handler, rec) -> None:
        """레코드 1건 평가 — h.lock 보유 상태에서 호출한다."""
        st = h.status.get(rec.job_key, _PENDING)
        if st == _FINISHED or self._is_inflight(h, rec.job_key):
            return
        in_end = rec.state in h.end_states
        in_start = rec.state in h.start_states
        # end_states에 없는 terminal(예: end={DONE}인데 EXIT/LOST/
        # SUBMIT_FAILED) — 더 진행할 수 없으니 최종 실행 없이 종결.
        if rec.state.is_terminal and not in_end:
            h.status[rec.job_key] = _FINISHED
            return
        # start_states는 실행 시작 조건이다. 시작 후에는 그 상태를 벗어나도 계속 실행한다.
        # 재제출은 rearm에서 _PENDING으로 되돌린다.
        if st == _PENDING and not in_start and not in_end:
            return
        final = in_end
        h.status[rec.job_key] = _FINISHED if final else _RUNNING
        self._mark_inflight(h, rec.job_key)
        with self._queue_lock:
            self._queue.append((h, rec, final))

    # inflight는 handler 수명과 독립적이다. 잠금 순서는 h.lock → _inflight_lock.
    def _inflight_key(self, h: "_Handler", job_key: str):
        return (h.jobset_id, h.name, job_key)

    def _is_inflight(self, h: "_Handler", job_key: str) -> bool:
        with self._inflight_lock:
            return self._inflight_key(h, job_key) in self._inflight

    def _mark_inflight(self, h: "_Handler", job_key: str) -> None:
        with self._inflight_lock:
            self._inflight.add(self._inflight_key(h, job_key))

    def _clear_inflight(self, h: "_Handler", job_key: str) -> None:
        with self._inflight_lock:
            self._inflight.discard(self._inflight_key(h, job_key))

    def _on_recheck(self, jobset_id: str, name: str, job_key: str) -> None:
        """[main] handler 실행 종료 직후 그 job 1건 재평가 — 실행 중(inflight)에
        job이 종료 state로 넘어가면 그 사이클 tick은 건너뛰는데, 전원 terminal로
        폴링이 auto-stop하면 다음 tick이 없어 final 실행이 유실된다. 종료
        state로 넘어간 경우에만 여기서 final을 보충한다 (아직 진행 중이면
        아무것도 안 함 — 다음 폴링 tick의 정상 경로 유지)."""
        h = self._handlers.get((jobset_id, name))
        if h is None:
            return
        try:
            rec = self.store.get_job(jobset_id, job_key)
        except Exception:                        # noqa: BLE001
            return                               # jobset/job 소멸 — 무시
        if not (rec.state in h.end_states or rec.state.is_terminal):
            return
        with h.lock:
            self._eval_record(h, rec)
        self._pump()

    # ------------------------------------------------------------------
    # 실행 큐 — main은 넣기만, worker가 꺼내 돈다
    # ------------------------------------------------------------------
    def _pump(self) -> None:
        """대기분이 있으면 drain worker를 상한까지 채운다 (호출 스레드 무관)."""
        with self._queue_lock:
            want = min(MAX_HANDLER_WORKERS - self._draining, len(self._queue))
            self._draining += want
        for _ in range(want):
            self._pool.start(_DrainTask(self))

    def _drain(self) -> None:
        """[worker] 큐가 빌 때까지 handler를 실행한다.

        '큐 비었나' 확인과 worker 수 감소를 **같은 lock 획득 안에서** 한다 —
        갈라 놓으면 그 틈에 들어온 항목이 아무 worker에도 안 잡힌 채로
        남는다(_pump는 _draining이 아직 안 줄어 새 worker를 안 띄운다)."""
        while True:
            with self._queue_lock:
                if not self._queue:
                    self._draining -= 1
                    return
                h, rec, final = self._queue.popleft()
            try:
                self._run(h, rec, final)
            except Exception:                # noqa: BLE001 — worker 보호
                log.exception("handler 실행 루프 예외(무시): %s/%s",
                              h.jobset_id, h.name)

    # worker 스레드에서 호출
    def _run(self, h: _Handler, rec: JobRecord, final: bool) -> None:
        try:
            data = h.fn(HandlerContext(h.jobset_id, rec, final))
            result = HandlerResult(h.name, h.jobset_id, rec.job_key,
                                   rec.job_id, final, data=data)
        except Exception as e:                       # noqa: BLE001
            log.exception("handler 실행 실패: %s/%s", h.jobset_id, h.name)
            result = HandlerResult(h.name, h.jobset_id, rec.job_key,
                                   rec.job_id, final, error=repr(e))
        finally:
            self._clear_inflight(h, rec.job_key)
        self.finished.emit(h.jobset_id, h.name, result)
        # 이전 final 실행 중 재제출한 job도 종료됐을 수 있다.
        # 현재 실행의 _FINISHED 판정이 중복 final을 막는다.
        self._recheck.emit(h.jobset_id, h.name, rec.job_key)


class _DrainTask(QRunnable):
    """실행 큐를 비우는 worker — 큐가 빌 때까지 handler를 연달아 돌린다.
    (job 1건당 QRunnable 1개를 만들던 방식은 tick이 main 스레드에서
     pool.start 비용을 job 수만큼 물어 GUI를 초 단위로 멈췄다.)"""

    def __init__(self, service: JobSetHandlerService):
        super().__init__()
        self.setAutoDelete(True)
        self.service = service

    def run(self):
        self.service._drain()
