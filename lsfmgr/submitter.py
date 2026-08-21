"""BulkSubmitter — QThreadPool 기반 대량 submit.

- 병렬/순차 submit, rate limit(token bucket), progress throttle
- 실패 시 QTimer 스케줄 retry (RETRY_WAIT → SUBMITTING, sleep 점유 없음)
- cancel: job 단위 경계에서 안전 중단, 이미 submit된 job은 정상 기록
- worker 예외 격리 → error Signal

retry 원장(ctx.pending_retries): QTimer 대기 중인 재시도는 pool 밖에 있어
waitForDone이 기다려주지 않는다. 대기 중 재시도를 전부 원장에 등록해 두고,
발화(fire)·포기(cancel/shutdown)가 원장에서의 원자적 pop으로만 결정되게
하면 어느 쪽이 먼저 오든 정확히 한 번만 처리된다.
이 원장은 ctx.lock이 아니라 **ctx.retry_lock**으로 잠근다 — 근거는
_SubmitContext.retry_lock 주석(main 스레드가 혼잡한 ctx.lock을 기다리지
않게 한다).
"""
from __future__ import annotations

import logging
import shlex
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence

from .command import LsfCommand
from .config import LsfConfig
from .errors import SubmitError
#: 제출 도중 대상 레코드가 사라졌다는 신호 — main 스레드의 동시 삭제
#: (remove_jobs / clear_jobs / remove_jobset, 전부 force 경로)는 **정상 동작**
#: 이지 INTERNAL_ERROR가 아니다 (killer/monitor/handlers와 같은 소실 방어).
from .errors import RECORD_GONE as _RECORD_GONE
from .lifecycle import SubmitGate
from .options import Options
from .qt import (
    CallTask, QObject, QRunnable, QThreadPool, QTimer, Signal, Slot, timer_ms,
)
from .reports import SubmitProgress, SubmitReport
from .states import JobState
from .store.base import JobSetStore
from .util import EmitThrottler, WorkerSlots

log = logging.getLogger("lsfmgr.submit")

#: 게이트(pre_submit)·착수 worker 수. bsub를 돌리지 않으므로 workers 상한과
#: 별개다 — 동시에 제출을 거는 jobset이 이보다 많으면 그만큼 착수가 줄 선다.
COORD_THREADS = 8


@dataclass
class _PendingRetry:
    """QTimer 대기 중인 재시도 1건 (job 1개 — v10.1: array 다발 잔재 제거)."""
    key: str                            # 포기 시 SUBMIT_FAILED 확정할 job_key
    delay_s: float
    make_task: Callable[[], QRunnable]  # 발화 시 pool에 넣을 task 생성


@dataclass
class _SubmitContext:
    """jobset 1건의 submit 진행 상태 (thread-safe 카운터).
    튜닝 값(max_retry 등)은 사본 없이 options 한 곳에서 읽는다."""
    jobset_id: str
    total: int
    #: 이 사이클의 동시 제출 상한 — 호출별 workers가 전역 상한보다 **낮을 때**만
    #: 실제로 조인다(같으면 공용 풀 크기에 이미 걸려 무해한 통과다).
    slots: WorkerSlots
    options: Options = field(default_factory=Options)
    throttler: EmitThrottler = field(default_factory=EmitThrottler)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    # RLock인 이유(리뷰 M2): finished/progress는 순서 보장을 위해 이 lock을
    # 쥔 채 발화된다. worker 발화는 queued라 무해하지만, main 스레드 발화
    # 경로(born-cancelled 정산·QTimer 재시도 확정·shutdown drain)는 슬롯이
    # direct로 실행돼, 슬롯이 progress_snapshot()/abort_retries() 같은
    # lock-보유 API를 pull하면 비재진입 Lock에선 같은 스레드 재획득으로
    # 영구 데드락(GUI freeze)이 났다. RLock은 같은 스레드 재진입만 허용해
    # 이 계열을 없애고 cross-thread 배타는 그대로 유지한다.
    lock: threading.RLock = field(default_factory=threading.RLock)
    #: 재시도 원장 전용 lock — **ctx.lock과 겹치지 않는다**.
    #: ctx.lock은 순서 보장을 위해 worker가 신호를 발화하는 동안에도 쥐고
    #: 있어(‑> _emit_progress) 대량 제출에서 상시 혼잡하다. 재시도 원장까지
    #: 거기 얹으면 main 스레드의 _on_retry_requested가 매번 그 혼잡을 기다린다
    #: — 2000건 재시도 폭풍 실측에서 main이 lock 대기로만 628ms를 썼다.
    #: 원장은 카운터와 원자적일 이유가 없으므로 따로 잠근다.
    #: 중첩 취득 없음(원장 lock을 쥔 채 ctx.lock을 잡는 경로가 없다).
    retry_lock: threading.Lock = field(default_factory=threading.Lock)
    started_at: float = field(default_factory=time.monotonic)
    done: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    retried_keys: set = field(default_factory=set)
    fail_reasons: Dict[str, int] = field(default_factory=dict)
    #: QTimer 대기 중인 재시도 원장 — **retry_lock**으로만 잠근다(ctx.lock 아님)
    pending_retries: Dict[str, _PendingRetry] = field(default_factory=dict)
    finished: bool = False
    # 진행 중 상태 전이분(PEND/SUBMIT_FAILED) 버퍼 — progress와 같은 cadence로
    # jobs_changed 배치 발행. UI가 완료를 안 기다리고 점진 갱신하게 한다.
    changed_buffer: list = field(default_factory=list)
    # SubmitGate 등록 토큰 — None이면 kill barrier에 거부돼 born-cancelled
    gate_token: object = None
    # manager가 발급한 제출 사이클 token — records_reset/gate_rejected에 실어
    # 돌려준다 (낡은 신호가 새 사이클의 보류분을 건드리지 못하게)
    arm_token: object = None
    #: **job 단위** 취소 지시 (선택 kill 전용) — cancel_event가 사이클 전체를
    #: 멈추는 것과 달리, 여기 담긴 key만 제출을 포기한다. 나머지 job의 제출은
    #: 그대로 진행된다. 한 번 담기면 사이클이 끝날 때까지 지우지 않는다 —
    #: 지우면 pool/QTimer에 남은 잔여 task가 kill 뒤에 그 job을 되살린다.
    cancelled_keys: set = field(default_factory=set)
    #: 지금 **제출 시도 구간에 들어가 있는** key — SUBMITTING 전이 직전에
    #: 등록하고 task 종료(성공/실패/취소/예외)에서 해제한다. 선택 kill의
    #: quiesce가 "이 job의 bsub가 아직 도는 중인가"를 판정하는 근거.
    inflight: set = field(default_factory=set)

    def __post_init__(self):
        # inflight 변화 통지 — lock을 공유해 '취소 지시 → inflight 판정'이
        # 하나의 임계구역 안에서 원자적으로 보이게 한다.
        self.inflight_cv = threading.Condition(self.lock)


class _SubmitTask(QRunnable):
    """단건 submit worker — wrapper 커맨드(argv)를 그대로 실행하고 stdout의
    'Job <id>'만 파싱한다. 관리는 그렇게 얻은 job_id로만 이뤄진다.
    (v10.1: bsub 경로 삭제로 서브클래스가 하나뿐이던 _BaseSubmitTask/
    _WrapperSubmitTask 상속 계층을 단일 클래스로 병합 — 동작 동일.)
    예외는 submitter로 격리 전달한다."""

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, argv: Sequence[str], attempt: int,
                 submit_cwd: Optional[str] = None):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.job_key = job_key
        self.argv = list(argv)
        self.attempt = attempt          # 0 == 최초 시도
        self.submit_cwd = submit_cwd    # 제출 subprocess의 작업 디렉토리(요청값)

    def run(self):
        try:
            self._run()
        except Exception as e:          # noqa: BLE001 — worker 스레드 보호
            if not isinstance(e, _RECORD_GONE):
                # 소실(동시 삭제)은 정상 경로 — traceback을 찍지 않는다
                log.exception("submit worker 예외: %s", self.job_key)
            self.submitter._task_crashed(self.ctx, self.job_key, e)
        finally:
            # 어느 경로로 끝나든 제출 구간에서 빠진다 — 여기서 빠뜨리면
            # 선택 kill의 quiesce가 영영 이 key를 기다린다.
            self.submitter._leave_inflight(self.ctx, self.job_key)

    def _run(self):
        sub, ctx = self.submitter, self.ctx
        # 취소 판정과 제출 구간 진입(inflight)을 **한 임계구역에서** 한다 —
        # 갈라 놓으면 cancel_jobs가 '판정을 막 통과한' task를 놓치고
        # quiesce_jobs는 inflight가 비어 즉시 통과해, 그 job이 kill이 끝난
        # 뒤에 제출을 마쳐 LSF에 살아남는다(선택 kill 유출).
        if not sub._enter_inflight(ctx, self.job_key):
            sub._task_cancelled(ctx, self.job_key)
            return
        try:
            starting = sub.store.transition(ctx.jobset_id, self.job_key,
                                            JobState.SUBMITTING)
        except _RECORD_GONE:
            # 착수 직전에 레코드가 지워졌다 — 아직 wrapper를 안 돌렸으므로
            # LSF에 흔적이 없다. 조용히 1단위 계상하고 끝낸다.
            sub._task_vanished(ctx, self.job_key, job_id=None)
            return
        if self.attempt:
            # 재시도의 SUBMITTING만 알린다 — 최초 시도분은 착수 시점의
            # 리셋 배치가 이미 발행했고, 거기에 또 얹으면 대량 제출에서
            # job 수만큼 중복 레코드가 흐른다.
            sub._note_transition(ctx, starting)
        # 이 사이클의 동시 제출 상한 — 호출별 workers가 전역보다 낮을 때만
        # 실제로 막는다. 취소를 보며 기다려 kill이 슬롯 대기에 갇히지 않는다.
        # (v11: 초당 상한 rate_limit_per_s는 삭제됐다 — 공용 풀에서 그 대기가
        #  worker 슬롯을 물고 있어 다른 jobset을 굶겼고, 전역 workers가
        #  생기면서 실효도 사라졌다. 초당 ≈ workers / bsub 1회 소요.)
        if not ctx.slots.acquire(self._cancelled):
            sub._task_cancelled(ctx, self.job_key)
            return
        try:
            return self._submit_once(sub, ctx)
        finally:
            ctx.slots.release()

    def _cancelled(self) -> bool:
        """이 job의 제출을 그만둬야 하는가 (사이클 전체 취소 또는 job 단위)."""
        ctx = self.ctx
        if ctx.cancel_event.is_set():
            return True
        with ctx.lock:
            return self.job_key in ctx.cancelled_keys

    def _submit_once(self, sub, ctx):
        """[worker] 슬롯을 쥔 채 wrapper 1회 실행 — 여기서만 bsub가 돈다."""
        # 슬롯 대기 사이에 kill 대상이 됐을 수 있다 — 대기가 끝난 지금 다시
        # 확인해, 그 job은 wrapper를 돌리지 않고 CANCELLED로 확정한다.
        if self._cancelled():
            sub._task_cancelled(ctx, self.job_key)
            return
        try:
            job_id = sub.command.run_submit(
                self.argv, timeout_s=ctx.options.submit_timeout_s,
                cwd=self.submit_cwd)     # wrapper는 -cwd 대신 subprocess cwd
        except SubmitError as e:
            sub._task_failed(ctx, self.job_key, self.attempt, e,
                             self._retry_factory())
            return
        sub._task_succeeded(ctx, self.job_key, job_id)

    def _retry_factory(self):
        """attempt→새 task를 만드는 콜백 — 지연 실행되므로 값(로컬)만 캡처하고
        self(autoDelete QRunnable)는 참조하지 않는다."""
        sub, ctx, job_key, argv, cwd = (self.submitter, self.ctx,
                                        self.job_key, self.argv,
                                        self.submit_cwd)
        return lambda att: _SubmitTask(sub, ctx, job_key, argv, att, cwd)


class BulkSubmitter(QObject):
    """대량 submit 진입점. manager(Facade)가 소유."""

    progress = Signal(str, int, int)          # jobset_id, done, total
    finished = Signal(str, object)            # jobset_id, SubmitReport
    error = Signal(str, str)                  # jobset_id, message
    jobs_changed = Signal(str, list)          # jobset_id, [JobRecord] 전이 배치
    started = Signal(str)                     # jobset_id — 게이트 통과 후 제출 착수
    # 착수 확정/무산 — manager의 보류 무장분(_pending_arm) 처리 전용 내부 신호.
    # records_reset: 레코드 리셋 완료(=이 사이클의 제출 착수 확정).
    # gate_rejected: 게이트가 착수 없이 종료(거부/예외/통과 직후 취소/shutdown).
    # 두 번째 인자는 manager가 발급한 사이클 token — 낡은 신호 식별용.
    records_reset = Signal(str, object)       # jobset_id, arm_token
    gate_rejected = Signal(str, object)       # jobset_id, arm_token
    pre_submit_started = Signal(str)               # jobset_id — pre_submit 게이트 시작
    pre_submit_finished = Signal(str, bool)        # jobset_id, ok — 게이트 종료(True=통과)
    # 내부용 — worker 스레드에서 emit → submitter 소속 스레드에서 QTimer 스케줄
    _retry_requested = Signal(str, str)       # jobset_id, 원장 키

    def __init__(self, store: JobSetStore, command: LsfCommand,
                 config: Optional[LsfConfig] = None,
                 parent: Optional[QObject] = None,
                 gate: Optional[SubmitGate] = None,
                 worker_limit: Optional[int] = None):
        super().__init__(parent)
        self.store = store
        self.command = command
        self.config = config or command.config
        # 동시 제출 상한은 **전역**이다 — 제출 사이클(=진행 중인 jobset)마다
        # 풀을 새로 만들면 jobset 3개 동시 제출에 wrapper가 8이 아니라 24개
        # 뜬다. workers를 낮게 잡아 eauth/mbatchd 과부하를 막으려는 사이트에서
        # 그 보호가 동시 제출 수만큼 무력화된다(8코어 실측: 동시 64개면 GUI
        # main 최대 지연 193ms, 전역 8이면 40ms).
        # worker_limit는 manager가 해석한 실효 workers(①config + ②manager kwarg).
        self._workers = max(1, int(self.config.workers if worker_limit is None
                                   else worker_limit))
        self._pool = QThreadPool(self)          # 제출(bsub) 전용
        self._pool.setMaxThreadCount(self._workers)
        # 게이트(pre_submit)와 착수(레코드 리셋+task 생성)는 bsub가 아니므로
        # workers 상한과 **별개** 풀에서 돈다. 같은 풀에 두면 느린 pre_submit
        # 하나가 다른 jobset의 제출 슬롯까지 물고 있게 된다.
        self._coord = QThreadPool(self)
        self._coord.setMaxThreadCount(COORD_THREADS)
        # kill 우선권 게이트 — manager가 killer와 공유하는 것을 주입한다.
        # (단독 사용 시 자체 생성 — barrier 없는 항상-허용 게이트로 동작)
        self._gate = gate or SubmitGate()
        self._contexts: Dict[str, _SubmitContext] = {}
        self._ctx_lock = threading.Lock()
        self._shutdown = False
        self._retry_requested.connect(self._on_retry_requested)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def resubmit_existing(self, jobset_id: str,
                          keyed_items: Sequence, options: Options,
                          pre_submit=None, arm_token=None) -> bool:
        """[async→Signal] 기존 레코드 재제출 — mgr.submit(js)의 실행부 (v9).

        keyed_items: [(job_key, argv토큰리스트), ...] — wrapper 커맨드를
        그대로 실행하는 단일 제출 경로 (v10: bsub 조립 경로 삭제).
        새 CREATED 레코드를 만들지 않고 **이미 존재하는 레코드**를 리셋(이전
        job_id/exit_code/실행시간 초기화 + command 갱신) 후 같은 job_key로
        재submit한다. 결과는 finished(SubmitReport).
        pre_submit(commands)->bool 지정 시 **리셋 이전에** 게이트 워커에서
        검사한다 — False/예외면 레코드를 건드리지 않고 끝난다.

        반환: 제출이 실제 착수됐으면 True. shutdown/born-cancelled(kill
        barrier 중 시작 — 레코드 원상 유지, 전원 취소 정산)면 False —
        caller는 rearm/polling 재개 등 착수 전제 후속 작업을 생략해야 한다."""
        if self._shutdown:
            # shutdown 후 queued 경로로 도달할 수 있다 — 새 pool/프로세스를
            # 만들면 아무도 기다려주지 않는 좀비가 된다
            log.warning("shutdown 후 재제출 요청 무시: %s", jobset_id)
            return False
        keyed = list(keyed_items)
        ctx = self._new_context(jobset_id, [k for k, _item in keyed], options)
        ctx.arm_token = arm_token
        if ctx.cancel_event.is_set():
            # born-cancelled (kill barrier 중 시작) — 아래 리셋은 기존
            # job_id/이력을 소거하는 파괴적 연산이라, 시작 전 취소면 레코드를
            # 아예 건드리지 않고 전원 취소로 정산한다 (원상 유지).
            # _gate_reject가 같은 일(cancelled=total 일괄 + finished 1회 +
            # _drop_ctx)을 O(1)로 한다 — 건당 _count 루프는 대형 재제출에서
            # main 스레드가 O(N)회 lock/신호 발화를 하게 되므로 금지.
            #
            # started/finished 짝: born-cancelled는 게이트를 통째로 건너뛰므로
            # pre_submit_* 신호도, _launch_cycle의 started도 나가지 않는다. 그대로면
            # 아무 착수 신호 없이 finished(cancelled)만 나가 started↔finished를
            # 쌍으로 세는 구독자(스피너/제출버튼 게이팅)의 카운터가 음수로
            # 내려간다. 게이트가 실행되지 않아 순서 충돌이 없으니 finished 직전에
            # started를 내 최소 짝(started→finished)을 맞춘다.
            self._safe_emit(self.started, jobset_id)
            self._gate_reject(ctx, finish=True)
            return False
        if pre_submit is None:
            # v10.1(리뷰 M1): 비게이트 착수도 worker에서 — N건 store 리셋 +
            # task 생성이 main(GUI) 스레드를 O(N) 블록하지 않게(실측
            # 10k에서 ~1s). 예외 래핑은 게이트 경로(_GateTask.run)와 동일
            # 목적 — 착수가 죽어도 ctx를 미완으로 두지 않는다.
            def _launch_guarded():
                try:
                    self._launch_cycle(ctx, keyed)
                except Exception as e:           # noqa: BLE001
                    log.exception("제출 착수 실패: %s", jobset_id)
                    self._gate_fail(ctx, repr(e))
            self._coord.start(CallTask(_launch_guarded))
            return True
        # 게이트 경로 — 리셋 **이전**에 검사하므로 False/예외면 레코드 원상
        # 유지 (예외 시에도 새 레코드를 만들지 않는다 — error +
        # finished(failed=N)로만 마무리)
        commands = [self._item_command(item) for _key, item in keyed]
        self._coord.start(_GateTask(self, ctx, commands, pre_submit,
                                 lambda: self._launch_cycle(ctx, keyed)))
        return True

    def _launch_cycle(self, ctx: _SubmitContext, keyed: List) -> None:
        """제출 착수 본체 — **단계 순서 자체가 계약**이다. worker 스레드에서
        실행되며 게이트 경로(_GateTask)와 비게이트 경로가 공유한다. 예외는
        caller가 _gate_fail로 마무리한다(ctx 미완 방지).

          ① started 발화 — **실제 착수 지점**에서만. shutdown·born-cancelled로
             여기 못 오면 started도 없어 'started만 나가고 finished는 영영
             안 오는' 고아가 원천 차단된다(started/finished 짝 계약).
          ② 레코드 리셋 → ③ task **전부** 생성 → ④ 무장 신호 → ⑤ fan-out.
             ③이 ⑤보다 앞인 이유: 생성이 실패하면 어느 task도 시작하기 전에
             예외가 난다(부분 착수 금지 — 실행 중 제출이 고아가 된다).
             ④가 ⑤보다 앞인 이유: 즉시 완주하는 task의 finished가 무장보다
             먼저 main에 도착하면 post_process 판정이 no-op으로 유실된다.
        """
        log.info("submit 착수 %s: %d건", ctx.jobset_id, len(keyed))
        self._safe_emit(self.started, ctx.jobset_id)                   # ①
        launch, reset_recs = self._reset_records(ctx, keyed)           # ②
        tasks = [self._make_resubmit_task(ctx, key, item, cwd)         # ③
                 for key, item, cwd in launch]
        if reset_recs:
            # 리셋된 SUBMITTING 즉시 발행 — 완료를 안 기다리고 표에 반영.
            self._safe_emit(self.jobs_changed, ctx.jobset_id, reset_recs)

        with ctx.lock:
            already_finished = ctx.finished
        if already_finished:
            # 전 키 리셋 실패(전멸) — ②의 계상이 이미 finished를 확정했다.
            # 여기서 무장 신호를 내면 끝난 사이클에 rearm/post_process 무장이
            # 잔존해 다음 폴링이 옛 terminal 레코드에 오발화한다(리뷰 B2).
            self._safe_emit(self.gate_rejected, ctx.jobset_id, ctx.arm_token)
            return
        self._safe_emit(self.records_reset, ctx.jobset_id, ctx.arm_token)  # ④
        for task in tasks:                                             # ⑤
            self._pool.start(task)
        if not launch:
            # 띄운 task가 0건 — 완료를 여기서 직접 확정한다(게이트 경로의
            # 빈 제출 마무리와 동일). ※ 과거의 QTimer.singleShot(0) 방식은
            # 이 worker 스레드에 이벤트 루프가 없어 영영 발화하지 않았다.
            self._finish_if_done(ctx, force=True)

    def _reset_records(self, ctx: _SubmitContext, keyed: List):
        """② 이전 실행의 흔적을 지우고 SUBMITTING으로 — 반환: (착수 목록,
        리셋된 레코드).

        job_id/exit_code/실행시간을 지운다. 안 지우면 재제출 실패 시 죽은 옛
        job_id와 이전 실행의 start/finish가 잔존한다. submit_cwd는 job 단위
        속성이라 리셋 대상이 아니다(그대로 실어 보낸다). 상태를 곧장
        SUBMITTING으로 두어 EXIT(kill) → SUBMITTING → PEND로 자연스럽게 보인다.

        **barrier에 걸린 key는 리셋조차 하지 않는다** — 리셋은 파괴적 연산이라
        제출하지 않을 job에 하면 '원상 유지' 계약이 깨진다.
        어느 키에서 실패해도 나머지는 진행한다 — 여기서 예외가 전파되면
        ctx가 미완(finished 미발행)으로 고착돼 jobset이 영구 잠긴다.
        """
        with ctx.lock:
            blocked = set(ctx.cancelled_keys)
        launch, reset_recs, skipped = [], [], 0
        for key, item in keyed:
            if key in blocked:
                skipped += 1
                continue
            try:
                rec = self.store.transition(
                    ctx.jobset_id, key, JobState.SUBMITTING,
                    job_id=None, exit_code=None, fail_reason=None,
                    fail_message=None, killed=False,
                    retry_count=0, command=self._item_command(item),
                    submit_time=None, run_time_s=None, start_time=None,
                    finish_time=None,
                    source_cluster=None, forward_cluster=None)
            except _RECORD_GONE:
                # 소실(동시 삭제)은 정상 경로 — traceback을 찍지 않는다.
                # 5000건 제출 중 remove_jobset이 들어오면 job 수만큼
                # traceback이 쏟아져 로그가 잠긴다.
                log.info("삭제된 job의 리셋 건너뜀: %s/%s", ctx.jobset_id, key)
                self._count(ctx, cancelled=True)
                continue
            except Exception as e:           # noqa: BLE001 — 이 키만 포기
                log.exception("재제출 리셋 실패 — 건너뜀: %s/%s",
                              ctx.jobset_id, key)
                # 계상은 cancelled로 갈 수밖에 없지만(작업 1단위는 끝났다),
                # 그대로 두면 앱에는 "취소됨"으로만 보여 store 장애가 kill과
                # 구별되지 않는다. error로 따로 알린다.
                self._safe_emit(self.error, ctx.jobset_id,
                                f"{key}: 제출 리셋 실패 — {e!r}")
                self._count(ctx, cancelled=True)
                continue
            if rec is not None:
                reset_recs.append(rec)
            launch.append((key, item,
                           rec.submit_cwd if rec is not None else None))
        if skipped:
            # barrier 거부분은 **한 번에** 계상한다 — 건당 계상은 대형
            # 재제출에서 O(N)회 lock/신호 발화가 된다.
            log.info("kill barrier로 제출 보류 %s: %d건 (레코드 원상 유지)",
                     ctx.jobset_id, skipped)
            self._count(ctx, n=skipped, cancelled=True)
        return launch, reset_recs

    @staticmethod
    def _item_command(item) -> str:
        """레코드에 저장할 command 문자열 — wrapper argv는 shlex 인용을 보존해
        재제출 시 shlex.split 왕복이 원본 argv를 복원하게 한다."""
        return shlex.join(item)

    def _make_resubmit_task(self, ctx: _SubmitContext, key: str,
                            item, submit_cwd: Optional[str] = None) -> QRunnable:
        return _SubmitTask(self, ctx, key, item, 0, submit_cwd)

    def _ctx(self, jobset_id: str) -> Optional[_SubmitContext]:
        """진행 중 사이클 ctx 조회 — _contexts 접근의 단일 지점."""
        with self._ctx_lock:
            return self._contexts.get(jobset_id)

    def is_active(self, jobset_id: str) -> bool:
        """해당 jobset에 아직 끝나지 않은 submit 사이클이 있는지 (resubmit_jobs 가드)."""
        ctx = self._ctx(jobset_id)
        return ctx is not None and not ctx.finished

    def progress_snapshot(self, jobset_id: str) -> Optional["SubmitProgress"]:
        """진행 중 submit의 실시간 스냅샷 — 없거나 이미 끝났으면 None.
        submit_progress Signal을 못 받은 시점에도 현재 진행을 pull로 조회한다."""
        ctx = self._ctx(jobset_id)
        if ctx is None:
            return None
        with ctx.lock:                       # 카운터 원자적 읽기
            if ctx.finished:
                return None
            return SubmitProgress(
                jobset_id=jobset_id, done=ctx.done, total=ctx.total,
                succeeded=ctx.succeeded, failed=ctx.failed,
                cancelled=ctx.cancelled)

    def _new_context(self, jobset_id: str, keys: Sequence[str],
                     options: Options) -> _SubmitContext:
        """submit 사이클 1건의 pool/ctx 구성 + 등록 (submit/재제출 공통).

        SubmitGate 등록까지 여기서 한다 — kill barrier가 올라가 있으면 그
        범위의 job은 시작 자체가 거부된다: 전 job이 걸리면 사이클 통째로
        born-cancelled(cancel_event가 켜진 채 시작 — 레코드/LSF 미접촉,
        전원 취소 계상), 일부만 걸리면 그 key만 `cancelled_keys`로 들어가
        제출에서 빠진다. barrier 확인과 등록이 게이트 lock 아래 원자적이라
        'kill의 취소를 빠져나가는 늦은 사이클'이 구조적으로 불가능하다."""
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=len(keys),
            # 호출별 workers는 전역 상한 **아래로 낮추는** 용도다 — 올려도
            # 공용 풀 크기를 못 넘는다.
            slots=WorkerSlots(min(options.workers, self._workers)),
            throttler=self._make_throttler(), options=options)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx
        # 진행 중 bsub는 submit_timeout_s로 반드시 끝난다(subprocess timeout).
        # 취소된 나머지 worker는 즉시 빠지므로 여유만 더한 대기 상한.
        reg = self._gate.register(
            jobset_id, keys,
            lambda scope: self._cancel_scope(ctx, scope),
            lambda scope, t: self._wait_scope(ctx, scope, t),
            options.submit_timeout_s + 30.0)
        ctx.gate_token = reg.activity
        if reg.activity is None:
            ctx.cancel_event.set()           # 전 job 거부 — born-cancelled
        elif reg.refused:
            with ctx.lock:                   # 일부만 거부 — 그 key만 제외
                ctx.cancelled_keys |= set(reg.refused)
        return ctx

    # ------------------------------------------------------------------
    # SubmitGate 활동 콜백 — kill(KillScope)이 worker 스레드에서 부른다
    # ------------------------------------------------------------------
    def _cancel_scope(self, ctx: _SubmitContext, scope) -> None:
        """범위 내 제출 취소. scope=None이면 사이클 전체.

        pool/QTimer에 남은 잔여 task는 `_enter_inflight`가 막는다. 취소 표시는
        사이클이 끝날 때까지 지우지 않는다 — 풀면 그 잔여 task가 kill 뒤에
        job을 다시 제출해 되살린다."""
        if scope is None:
            ctx.cancel_event.set()
        else:
            with ctx.lock:
                ctx.cancelled_keys |= set(scope)
        # QTimer 대기 중인 재시도 — 그대로 두면 kill 뒤 발화해 job이 부활한다.
        # 손에 쥔 ctx를 직접 넘긴다 — 이름으로 재조회하면 그 사이 사이클이
        # 교체됐을 때(_drop_ctx 후 새 등록) 남의 사이클 재시도를 지운다.
        self._abort_retries_ctx(ctx, keys=scope)

    def _wait_scope(self, ctx: _SubmitContext, scope,
                    timeout_s: float) -> bool:
        """범위 내 제출이 멎을 때까지 대기 — 반환: 시간 내 정지 여부.

        판정은 **제출 구간(ctx.inflight)이 비었는가** 하나다. caller
        (KillScope.acquire → begin)가 이미 취소를 걸어 뒀으므로 아직 시작 안 한
        task는 _enter_inflight에서 그대로 빠진다 — 즉 큐에 남아 있는 것은 이미
        '제출하지 않을 것'이라 기다릴 이유가 없다. 도는 중인 wrapper만
        기다리면 되고, 그래야 job_id가 확보돼 killer의 key→id 해석이 그 id를
        잡는다.

        pool.waitForDone으로 판정하면 안 되고(공용 풀이라 남의 jobset까지
        기다린다), 작업 단위 소진(done==total)으로 판정해서도 안 된다 —
        공용 큐 뒤에 다른 jobset의 대량 작업이 밀려 있으면 이 사이클의 취소된
        task가 dequeue되기까지 통째로 기다리게 된다(실측: 3000건 뒤에 선
        20건짜리 kill이 7.9초, 백로그가 크면 타임아웃).

        범위 지정(선택 kill)은 그 key만 본다 — 대상 아닌 job의 제출이 끝나기를
        기다릴 이유가 없다(그게 선택 kill의 요점)."""
        wanted = None if scope is None else set(scope)
        if wanted is not None and not wanted:
            return True
        deadline = time.monotonic() + timeout_s
        with ctx.inflight_cv:
            while True:
                busy = (ctx.inflight if wanted is None
                        else wanted & ctx.inflight)
                if not busy:
                    return True
                remain = deadline - time.monotonic()
                if remain <= 0:
                    log.warning("제출 정지 대기 초과 %s: %d건 제출 중",
                                ctx.jobset_id, len(busy))
                    return False
                ctx.inflight_cv.wait(remain)

    def _enter_inflight(self, ctx: _SubmitContext, job_key: str) -> bool:
        """제출 구간 진입 — 취소(사이클/job 단위) 중이면 False(진입 거부)."""
        with ctx.lock:
            if ctx.cancel_event.is_set() or job_key in ctx.cancelled_keys:
                return False
            ctx.inflight.add(job_key)
            return True

    def _leave_inflight(self, ctx: _SubmitContext, job_key: str) -> None:
        """제출 구간 이탈 — 대기 중인 정지 대기를 깨운다 (멱등)."""
        with ctx.inflight_cv:
            if job_key in ctx.inflight:
                ctx.inflight.discard(job_key)
                ctx.inflight_cv.notify_all()

    def _make_throttler(self) -> EmitThrottler:
        """config의 progress throttle 설정으로 EmitThrottler 생성."""
        return EmitThrottler(self.config.progress_min_interval_s,
                             self.config.progress_min_step_ratio)

    def cancel_submit(self, jobset_id: str) -> None:
        """진행 중 submit 중단. 이미 submit된 job은 유지."""
        ctx = self._ctx(jobset_id)
        if ctx is not None:
            ctx.cancel_event.set()

    def shutdown(self) -> None:
        """모든 submit 중단 요청 후 pool join.
        진행 중이던 bsub는 완료까지 기다려 job_id 유실을 막는다."""
        self._shutdown = True
        with self._ctx_lock:
            contexts = list(self._contexts.values())
        for ctx in contexts:
            ctx.cancel_event.set()
        # 공용 풀 — 사이클별로 join할 것이 없다(착수/게이트도 함께 비운다)
        self._coord.waitForDone(-1)
        self._pool.waitForDone(-1)
        # QTimer 대기 중인 재시도는 이벤트 루프가 곧 끝나면 영영 발화하지
        # 않는다 — 원장 잔류분을 여기서 확정해야 RETRY_WAIT가 비terminal로
        # 잔존하지 않고 finished도 발행된다.
        # waitForDone 이후에는 원장에 새 항목이 추가되지 않는다.
        for ctx in contexts:
            with ctx.retry_lock:
                entries = list(ctx.pending_retries.values())
                ctx.pending_retries.clear()
            for entry in entries:
                self._finalize_retry(ctx, entry, "SHUTDOWN")

    def abort_retries(self, jobset_id: str,
                      keys: Optional[Sequence[str]] = None) -> None:
        """이 jobset의 대기 중 재시도(QTimer)를 포기 확정 — RETRY_WAIT →
        SUBMIT_FAILED. 전체 kill과 함께 호출해, kill 뒤 재시도 타이머가
        발화해 job이 부활하는 것을 막는다. 이후 타이머가 발화해도 원장
        pop이 빈손이라 no-op(정확히 한 번).

        keys=None이면 이 jobset의 전 재시도, key 집합이면 그 job의 재시도만
        포기한다 (범위 kill — 대상 아닌 job의 재시도는 살려 둔다).
        SubmitGate의 Scope와 같은 규약(None=전체)이라 `_cancel_scope`가
        받은 scope를 그대로 넘긴다."""
        ctx = self._ctx(jobset_id)
        if ctx is not None:
            self._abort_retries_ctx(ctx, keys=keys)

    def _abort_retries_ctx(self, ctx: _SubmitContext,
                           keys: Optional[Sequence[str]] = None) -> None:
        """abort_retries의 본체 — **이 ctx**의 원장만 건드린다. kill 콜백
        (_cancel_scope)처럼 ctx를 이미 쥔 caller는 이름 재조회 없이 직접
        부른다(사이클 교체 경합 창 제거)."""
        with ctx.retry_lock:
            if keys is None:
                entries = list(ctx.pending_retries.values())
                ctx.pending_retries.clear()
            else:
                entries = [ctx.pending_retries.pop(k)
                           for k in set(keys) if k in ctx.pending_retries]
        for entry in entries:
            self._finalize_retry(ctx, entry, "KILLED")

    # ------------------------------------------------------------------
    # worker 콜백 (worker 스레드에서 호출됨 — Store는 thread-safe)
    # ------------------------------------------------------------------
    def _task_succeeded(self, ctx: _SubmitContext, job_key: str,
                        job_id: int) -> None:
        try:
            rec = self.store.transition(
                ctx.jobset_id, job_key, JobState.PEND,
                job_id=job_id, submit_time=datetime.now(),
                fail_reason=None, fail_message=None)
        except _RECORD_GONE:
            # 제출은 **이미 성공**했는데 기록할 레코드가 없다 — job_id를
            # 여기서 놓치면 LSF의 살아있는 job이 아무 흔적 없이 남는다.
            self._task_vanished(ctx, job_key, job_id=job_id)
            return
        self._count(ctx, succeeded=True, changed=rec)

    def _task_vanished(self, ctx: _SubmitContext, job_key: str, *,
                       job_id: Optional[int]) -> None:
        """[worker] 대상 레코드가 사라진 채로 task가 끝났다 — 동시 삭제는
        정상 경로이므로 error Signal도 SUBMIT_FAILED도 만들지 않는다.

        단 job_id가 이미 잡힌 뒤라면 그 id의 **유일한 흔적**이 이 로그다 —
        레코드가 없어 caller가 조회로 찾을 방법이 없으므로 WARNING으로
        남긴다(force 삭제 계약: LSF job 정리는 caller 책임).
        계상은 cancelled — 실패가 아니고, 살아남은 레코드도 없다."""
        if job_id is None:
            log.info("삭제된 job 건너뜀: %s/%s", ctx.jobset_id, job_key)
        else:
            log.warning(
                "삭제된 job의 제출이 이미 성공 — LSF job %s가 추적 없이 "
                "남습니다 (%s/%s, 정리는 caller 책임)",
                job_id, ctx.jobset_id, job_key)
        self._count(ctx, cancelled=True)

    def _task_failed(self, ctx: _SubmitContext, job_key: str, attempt: int,
                     err: SubmitError,
                     retry_factory: Callable[[int], QRunnable]) -> None:
        """실패 처리. err.retryable=False(예: NO_JOBID_PARSED)면 재시도 없이
        바로 SUBMIT_FAILED 확정한다. 재시도 task는 retry_factory(attempt+1)로
        생성한다(bsub 경로/‏wrapper 경로가 각자 자기 task 를 만든다)."""
        log.warning("submit 실패 [%s] %s: %s", err.fail_reason, job_key, err)
        if (getattr(err, "retryable", True) and attempt < ctx.options.max_retry
                and not ctx.cancel_event.is_set() and not self._shutdown):
            # RETRY_WAIT → QTimer 스케줄 (스레드 sleep 점유 금지, §4)
            # fail_message: 재시도 대기 중에도 마지막 시도의 터미널 메시지를
            # 표에 보여줄 수 있고, 포기 확정(_finalize_retry) 시에도 잔존한다
            try:
                waiting = self.store.transition(
                    ctx.jobset_id, job_key,
                    JobState.RETRY_WAIT,
                    retry_count=attempt + 1,
                    fail_reason=err.fail_reason,
                    fail_message=err.diagnostic()[:4000])
            except _RECORD_GONE:
                # 재시도할 레코드가 사라졌다 — 타이머를 걸면 발화 때 또 같은
                # 소실을 만난다. 여기서 1단위 계상하고 끝낸다.
                self._task_vanished(ctx, job_key, job_id=None)
                return
            with ctx.lock:
                ctx.retried_keys.add(job_key)
            # 타이머를 걸기 **전에** 알린다 — 재시도가 먼저 끝나 PEND가 나가면
            # UI가 RETRY_WAIT를 건너뛴다.
            self._note_transition(ctx, waiting)
            self._schedule_retry(
                ctx, job_key, ctx.options.retry_delay_s(attempt),
                lambda: retry_factory(attempt + 1))
            return
        log.error("SUBMIT_FAILED 확정 [%s] %s (%d회 시도)",
                  err.fail_reason, job_key, attempt + 1)      # ERROR
        try:
            rec = self.store.transition(ctx.jobset_id, job_key,
                                        JobState.SUBMIT_FAILED,
                                        retry_count=attempt,
                                        fail_reason=err.fail_reason,
                                        fail_message=err.diagnostic()[:4000])
        except _RECORD_GONE:
            # 실패를 기록할 레코드가 없다 — 제출도 안 됐으니 LSF 흔적 없음
            self._task_vanished(ctx, job_key, job_id=None)
            return
        self._count(ctx, failed=True, reason=err.fail_reason, changed=rec)

    def _task_cancelled(self, ctx: _SubmitContext, job_key: str) -> None:
        self._mark_cancelled(ctx, [job_key])

    def _mark_cancelled(self, ctx: _SubmitContext,
                        keys: List[str]) -> None:
        """kill/cancel 안전 지점 중단 — 제출을 포기한 job을 CANCELLED로
        확정하고 작업 1단위 완료로 계상한다.

        CREATED 복귀가 아니라 CANCELLED인 이유: 이 job은 '아직 제출 안 한
        것'이 아니라 '제출하려다 취소된 것'이다. CREATED로 되돌리면 표에서
        한 번도 손대지 않은 job과 구별되지 않아, kill을 눌렀는데 아무 흔적도
        안 남는다. CANCELLED는 terminal이지만 is_inactive라 재제출은 그대로
        된다(재제출 시 do_launch의 리셋이 이 이력을 지운다).

        - guard(CAS)로 SUBMITTING/RETRY_WAIT일 때만 전이 — 그새 다른 상태로
          바뀐(또는 remove_jobs으로 소실된) 키는 조용히 건너뛴다.
        - fail_reason은 "CANCELLED"로 남기고 이전 시도의 잔재
          (fail_message/retry_count)는 지운다 — 취소는 실패가 아니므로
          bsub 실패 메시지를 달고 남으면 원인 표시가 오해된다.
        - killed 표식도 남긴다 — "이 종료는 내가 끝낸 것"이라는 같은 뜻이다.
          안 남기면 completion의 '전원 내 kill로 끝났으면 통지 안 함' 판정이
          깨져, 제출 도중 kill한 jobset에 "다 끝났다" 알림이 간다.
        - 반드시 changed로 발행한다 — CANCELLED는 폴링 대상(is_on_lsf)이
          아니라 여기서 안 알리면 UI 표가 SUBMITTING에 영구 고착된다.
        """
        def guard(cur):
            return cur.state in (JobState.SUBMITTING, JobState.RETRY_WAIT)
        changed = list(self.store.transition_many(
            ctx.jobset_id,
            [(k, JobState.CANCELLED, guard,
              {"fail_reason": "CANCELLED", "fail_message": None,
               "retry_count": 0, "killed": True})
             for k in keys]))
        self._count(ctx, cancelled=True, changed=changed)

    def _task_crashed(self, ctx: _SubmitContext, job_key: str,
                      err: Exception) -> None:
        """분류 불가 예외 — SUBMIT_FAILED 처리 + error Signal.
        guard(리뷰 B3): 제출 자체는 성공(job_id 확보)했는데 성공 처리 도중
        예외가 난 경우, 실제 LSF에 살아있는 job을 SUBMIT_FAILED로 덮어
        고아화하지 않는다 — job_id 없는(진짜 미제출) 레코드만 확정한다."""
        if isinstance(err, _RECORD_GONE):
            # 소실은 위 경로들이 이미 다 흡수한다 — 여기까지 왔다면 방어를
            # 빠뜨린 store 접근이 새로 생겼다는 뜻이므로, 그대로 삼키지 말고
            # 소실 계상으로 정직하게 끝낸다(INTERNAL_ERROR로 오분류 금지).
            log.info("삭제된 job에서 예외 흡수: %s/%s", ctx.jobset_id, job_key)
            self._task_vanished(ctx, job_key, job_id=None)
            return
        try:
            self.store.transition(ctx.jobset_id, job_key,
                                  JobState.SUBMIT_FAILED,
                                  fail_reason="INTERNAL_ERROR",
                                  fail_message=repr(err)[:4000],
                                  guard=lambda cur: cur.job_id is None)
        except _RECORD_GONE:
            pass                                # 소실 — 남길 레코드가 없다
        except Exception:                       # noqa: BLE001
            log.exception("crash 후 전이 실패: %s", job_key)
        self._safe_emit(self.error, ctx.jobset_id, f"{job_key}: {err!r}")
        self._count(ctx, failed=True, reason="INTERNAL_ERROR")

    # ------------------------------------------------------------------
    # retry 스케줄 — 원장 등록(worker 스레드) → QTimer 발화(submitter 스레드)
    # ------------------------------------------------------------------
    def _schedule_retry(self, ctx: _SubmitContext, key: str,
                        delay_s: float,
                        make_task: Callable[[], QRunnable]) -> None:
        """RETRY_WAIT 전이 직후 worker 스레드에서 호출 — 원장에 등록한다."""
        with ctx.retry_lock:
            ctx.pending_retries[key] = _PendingRetry(key, delay_s, make_task)
        self._retry_requested.emit(ctx.jobset_id, key)

    @Slot(str, str)
    def _on_retry_requested(self, jobset_id: str, entry_key: str) -> None:
        ctx = self._ctx(jobset_id)
        if ctx is None:
            return

        def fire():
            with ctx.retry_lock:
                entry = ctx.pending_retries.pop(entry_key, None)
            if entry is None:
                return                  # shutdown이 이미 확정 — 정확히 한 번
            if self._shutdown or ctx.cancel_event.is_set():
                self._finalize_retry(ctx, entry, "CANCELLED")
                return
            self._pool.start(entry.make_task())

        if self._shutdown or ctx.cancel_event.is_set():
            fire()                      # 대기 없이 즉시 포기 확정
            return
        with ctx.retry_lock:
            entry = ctx.pending_retries.get(entry_key)
        if entry is not None:
            QTimer.singleShot(timer_ms(entry.delay_s), fire)

    def _finalize_retry(self, ctx: _SubmitContext, entry: _PendingRetry,
                        default_reason: str) -> None:
        """재시도 포기 — RETRY_WAIT 잔류분을 CANCELLED로 최종 확정.

        호출 사유는 셋(SHUTDOWN/KILLED/CANCELLED) 다 **제출을 그만둔** 것이지
        제출이 실패한 게 아니다. SUBMIT_FAILED로 확정하면 "재시도까지 다 하고
        실패했다"로 읽혀 실패 집계가 부풀고, kill을 눌렀다는 사실이 사라진다.
        fail_reason에는 직전 시도의 원인이 있으면 그걸(왜 재시도 중이었는지),
        없으면 사유(default_reason)를 남긴다.
        jobset이 이미 삭제됐어도(remove_jobset 등) 카운터 확정은 계속한다."""
        changed = []
        key = entry.key
        try:
            rec = self.store.get_job(ctx.jobset_id, key)
            if rec.state is JobState.RETRY_WAIT:
                new = self.store.transition(ctx.jobset_id, key,
                                            JobState.CANCELLED,
                                            killed=True,
                                            fail_reason=rec.fail_reason
                                            or default_reason)
                if new is not None:
                    changed.append(new)
        except _RECORD_GONE:
            # 소실(동시 삭제)은 정상 — 카운터 확정만 하고 넘어간다
            log.info("삭제된 job의 retry 포기: %s/%s", ctx.jobset_id, key)
        except Exception:                        # noqa: BLE001
            # store 장애여도 _count까지는 반드시 도달해야 한다 — 여기서
            # 전파되면 done<total 고착 → finished 미발행
            log.exception("retry 포기 확정 실패(무시): %s/%s",
                          ctx.jobset_id, key)
        # 실패가 아니라 취소로 계상 — SubmitReport.failed에 섞이면 "몇 건이
        # 진짜 실패했나"를 못 읽는다 (상태가 CANCELLED인 것과 정합).
        self._count(ctx, cancelled=True, changed=changed)

    # ------------------------------------------------------------------
    # 진행/완료 통지 — 모든 종료 경로의 단일 출구
    # ------------------------------------------------------------------
    def _count(self, ctx: _SubmitContext, *, n: int = 1,
               succeeded: bool = False,
               failed: bool = False, cancelled: bool = False,
               reason: Optional[str] = None,
               changed=None) -> None:
        """작업 완료 계상(+진행/완료 통지)의 단일 지점 — job 1개당 1단위.

        n>1은 일괄 계상 — barrier 거부분처럼 대량이 한 번에 확정되는 경로는
        건당 호출이 O(N)회 lock/신호 발화가 되므로 반드시 n으로 묶는다.
        changed(전이된 JobRecord 또는 리스트)는 버퍼에 쌓아 progress와 같은
        cadence로 jobs_changed 배치 발행한다."""
        if n <= 0:
            return
        with ctx.lock:
            ctx.done += n
            if succeeded:
                ctx.succeeded += n
            if failed:
                ctx.failed += n
            if cancelled:
                ctx.cancelled += n
            if reason is not None:
                ctx.fail_reasons[reason] = ctx.fail_reasons.get(reason, 0) + n
            if changed is not None:
                if isinstance(changed, list):
                    ctx.changed_buffer.extend(changed)
                else:
                    ctx.changed_buffer.append(changed)
        self._emit_progress(ctx)
        self._finish_if_done(ctx)

    def _note_transition(self, ctx: _SubmitContext, changed) -> None:
        """**완료 계상 없이** 전이만 발행한다 (RETRY_WAIT 등 과도 상태).

        _count는 '작업 1단위 완료' 계상기다 — 재시도 중에 부르면 done이 부풀어
        submit_finished가 조기 발화한다. 그렇다고 안 부르면 전이가 아예 발행되지
        않았다: 발행 버퍼(changed_buffer)를 흘려보내는 유일한 통로가 계상에만
        물려 있었기 때문이다. 그래서 계상 없이 같은 cadence로 흘려보내는 경로를
        따로 둔다.

        이게 없으면 재시도 중인 job이 UI에서 SUBMITTING에 머문다 — 몇 분 걸리는
        재시도가 '멈춘 것'으로 보이고, store에 있는 retry_count/fail_message
        (터미널 원문)가 표에 영영 안 뜬다.
        """
        if changed is None:
            return
        with ctx.lock:
            ctx.changed_buffer.append(changed)
        self._emit_progress(ctx, progress=False)

    def _emit_progress(self, ctx: _SubmitContext,
                       progress: bool = True) -> None:
        # 발화(progress·jobs_changed)를 ctx.lock 안에서 수행한다 — drain과 emit이
        # 원자적이어야, 다른 worker 스레드의 _finish_if_done가 그 사이에 끼어들어
        # finished를 마지막 per-job jobs_changed보다 먼저 post하는 경합을 막는다
        # (worker→main은 queued connection이라 lock 중 emit은 post만 한다).
        with ctx.lock:
            if ctx.finished:
                return                   # 최종 flush는 _finish_if_done 담당
            done, total = ctx.done, ctx.total
            if not ctx.throttler.should_emit(done, total):   # throttle
                return
            batch = ctx.changed_buffer
            ctx.changed_buffer = []
            if progress:
                # 계상이 안 움직인 호출(_note_transition)은 진행률을 다시
                # 쏘지 않는다 — 같은 수치를 반복 발행할 이유가 없다.
                self.progress.emit(ctx.jobset_id, done, total)
            if batch:
                self.jobs_changed.emit(ctx.jobset_id, batch)

    def _finish_if_done(self, ctx: _SubmitContext, force: bool = False) -> None:
        with ctx.lock:
            if ctx.finished or (ctx.done < ctx.total and not force):
                return
            ctx.finished = True
            batch, ctx.changed_buffer = ctx.changed_buffer, []
            report = self._make_report(ctx)     # _gate_reject/_fail과 동일 구성
            # 마지막 progress는 반드시 (total, total) — 계약(README §6.1).
            # _count는 _emit_progress → _finish_if_done 순서지만, 마지막 1건을
            # 계상한 worker가 _emit_progress에 들어가기 **전에** 다른 worker의
            # _finish_if_done이 done==total을 보고 finished를 세우면, 그
            # _emit_progress가 `if ctx.finished: return`으로 빠져 최종 통지가
            # 통째로 유실된다(진행바가 49/50에서 멈춘 채 완료). 여기서 낸다.
            # force 경로(게이트 거부 등)는 done<total일 수 있어 제외한다.
            if ctx.done >= ctx.total:
                self.progress.emit(ctx.jobset_id, ctx.total, ctx.total)
            # 마지막 전이분 flush → finished 를 같은 lock 안에서 순서대로 발화.
            # 모든 per-job jobs_changed도 ctx.lock 안에서 발화되므로, 락이
            # 직렬화해 finished가 반드시 마지막 per-job jobs_changed 뒤에
            # post된다 — UI가 완료 통지 시점에 전 job 갱신을 이미 받도록 보장.
            if batch:                    # throttle 잔여 마지막 전이분
                self.jobs_changed.emit(ctx.jobset_id, batch)
            # 완료 로그는 finished **발화 전에** — 발화는 worker→main queued라,
            # 뒤에 두면 신호를 받은 main이 로그가 찍히기 전에 먼저 깨어난다
            # (완료 통지를 보고 로그를 읽는 쪽에서 완료 라인이 비어 보인다).
            # kill 경로(killer.py 'kill 완료')와 같은 순서.
            log.info("submit 완료 %s: 성공 %d / 실패 %d / 취소 %d (총 %d)",
                     ctx.jobset_id, report.succeeded, report.failed,
                     report.cancelled, report.total)
            self.finished.emit(ctx.jobset_id, report)
        # 완료된 ctx 정리 — 장수 세션에서 jobset 수만큼 누적되는 것 방지.
        # (다른 사이클이 이미 새 ctx로 교체했으면 그대로 둔다)
        self._drop_ctx(ctx)

    # ------------------------------------------------------------------
    # pre_submit 게이트 — 제출 전 단일 워커 검사
    # ------------------------------------------------------------------
    def _make_report(self, ctx: _SubmitContext) -> SubmitReport:
        return SubmitReport(
            jobset_id=ctx.jobset_id, total=ctx.total,
            succeeded=ctx.succeeded, failed=ctx.failed,
            cancelled=ctx.cancelled, retried=len(ctx.retried_keys),
            duration_s=time.monotonic() - ctx.started_at,
            fail_reasons=dict(ctx.fail_reasons))

    @staticmethod
    def _safe_emit(signal, *args) -> None:
        """Signal 발화 중 user slot 예외를 격리한다 — _launch_cycle 등
        착수 경로에서 전파되면 ctx가 미완으로 고착돼 jobset이 잠긴다."""
        try:
            signal.emit(*args)
        except Exception:                    # noqa: BLE001
            log.exception("signal slot 예외(무시)")

    def _drop_ctx(self, ctx: _SubmitContext) -> None:
        if ctx.gate_token is not None:       # 게이트 활동 종료 (멱등)
            self._gate.unregister(ctx.jobset_id, ctx.gate_token)
        with self._ctx_lock:
            if self._contexts.get(ctx.jobset_id) is ctx:
                del self._contexts[ctx.jobset_id]

    def _finish_forced(self, ctx: _SubmitContext, *, failed: bool,
                       emit_finished: bool) -> None:
        """게이트 거부/착수 실패의 **공통 종료 꼬리** — 카운터를 일괄
        확정(failed=True면 전원 실패, 아니면 전원 취소)하고 finished를
        최대 1회 발화한다. gate_rejected는 caller가 이보다 먼저 발화한다."""
        with ctx.lock:
            if ctx.finished:
                return
            ctx.finished = True
            if failed:
                ctx.failed = ctx.total
                ctx.fail_reasons["PRE_SUBMIT_FAILED"] = ctx.total
            else:
                ctx.cancelled = ctx.total
            if emit_finished:
                self._safe_emit(self.finished, ctx.jobset_id,
                                self._make_report(ctx))

    def _gate_reject(self, ctx: _SubmitContext, finish: bool) -> None:
        """게이트 False/취소 — 제출하지 않음(레코드 미생성 → 요약은 N CREATED
        유지). finish=True일 때만 submit_finished(cancelled=N)를 발화한다
        (False 반환은 config.submit_finished_on_gate_reject, 취소는 항상 True).
        착수가 없었으므로 gate_rejected로 manager의 보류 무장분을 정리한다."""
        # gate_rejected(무장 정리)를 finished보다 **먼저** 발화한다 — 같은
        # 스레드에서 queued될 때 manager가 finished(submit 완료) 처리 전에
        # _pending_arm을 비우도록 (착수가 없었으니 무장분은 폐기돼야 한다).
        self._safe_emit(self.gate_rejected, ctx.jobset_id, ctx.arm_token)
        self._finish_forced(ctx, failed=False, emit_finished=finish)
        log.info("pre_submit 게이트 거부 %s (finished 발화=%s)",
                 ctx.jobset_id, finish)
        self._drop_ctx(ctx)

    def _gate_fail(self, ctx: _SubmitContext, msg: str) -> None:
        """게이트/착수 예외 — 실패 확정. 리셋됐지만 착수 못 한 SUBMITTING
        레코드를 SUBMIT_FAILED로 되돌려 고착을 막고(비게이트 착수 예외
        대비), error + finished + gate_rejected(무장 정리)로 마무리한다."""
        # 리셋만 되고 착수 못 한 SUBMITTING 레코드 un-stick — 안 하면 재제출
        # 가드(활성 job)에 걸려 jobset이 잠긴다. **CAS guard**: 그새 실행 중
        # task가 SUBMITTING→PEND로 옮겼으면 되돌리지 않는다(실행 중 bsub를
        # SUBMIT_FAILED로 덮으면 그 LSF job이 고아가 된다).
        stuck = []
        try:
            recs = self.store.get_jobs(ctx.jobset_id)
        except Exception:                    # noqa: BLE001
            log.exception("착수 실패 레코드 조회 실패: %s", ctx.jobset_id)
            recs = []
        for r in recs:
            if r.state is not JobState.SUBMITTING:
                continue
            # 개별 transition을 각자 try로 — 한 key가 remove_jobs 경합으로
            # JobNotFoundError를 던져도 루프가 통째로 죽어 나머지가 SUBMITTING에
            # 갇히면(재제출 busy 가드에 걸려 영구 잠김) 안 된다. 그 key만 건너뛴다.
            try:
                nr = self.store.transition(
                    ctx.jobset_id, r.job_key, JobState.SUBMIT_FAILED,
                    guard=lambda cur: cur.state is JobState.SUBMITTING,
                    fail_reason="LAUNCH_FAILED", fail_message=msg[:4000])
                if nr is not None:
                    stuck.append(nr)
            except Exception:                # noqa: BLE001
                log.exception("착수 실패 레코드 정리 실패 — 건너뜀: %s/%s",
                              ctx.jobset_id, r.job_key)
        if stuck:
            self._safe_emit(self.jobs_changed, ctx.jobset_id, stuck)
        # user error 슬롯 예외 격리 — main 스레드 direct 연결에서 전파되면
        # 아래 ctx.finished 설정 전에 죽어 jobset이 영구 잠긴다
        self._safe_emit(self.error, ctx.jobset_id, f"pre_submit: {msg}")
        # gate_rejected를 finished보다 먼저 (무장 정리 우선 — _gate_reject 주석)
        self._safe_emit(self.gate_rejected, ctx.jobset_id, ctx.arm_token)
        self._finish_forced(ctx, failed=True, emit_finished=True)
        self._drop_ctx(ctx)


class _GateTask(QRunnable):
    """pre_submit 게이트 워커 — 단일 스레드에서 커맨드 리스트 전체를
    1회 검사. True면 do_launch()로 실제 제출 착수, False/예외면 거부/실패."""

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 commands: list, pre_submit, do_launch):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.commands = commands
        self.pre_submit = pre_submit
        self.do_launch = do_launch

    def run(self):
        sub, ctx = self.submitter, self.ctx
        # 착수 없는 모든 종료 경로의 gate_rejected(무장 정리)는 _gate_reject/
        # _gate_fail이 발행한다 — 여기서 중복 emit하지 않는다.
        sub.pre_submit_started.emit(ctx.jobset_id)
        if sub._shutdown or ctx.cancel_event.is_set():
            sub.pre_submit_finished.emit(ctx.jobset_id, False)
            sub._gate_reject(ctx, finish=True)      # 취소 — 항상 finished
            return
        try:
            ok = bool(self.pre_submit(list(self.commands)))
        except Exception as e:                       # noqa: BLE001
            log.exception("pre_submit 게이트 예외: %s", ctx.jobset_id)
            sub.pre_submit_finished.emit(ctx.jobset_id, False)
            sub._gate_fail(ctx, repr(e))
            return
        if ok and (ctx.cancel_event.is_set() or sub._shutdown):
            # 통과했지만 그새 취소/종료 — ok=True를 먼저 알리면 manager가
            # rearm 폴링 재개를 수행해, 재실행이 없는데 terminal
            # 레코드의 최종 핸들러가 중복 발화한다. False로 강등해 알린다.
            sub.pre_submit_finished.emit(ctx.jobset_id, False)
            sub._gate_reject(ctx, finish=True)
            return
        sub.pre_submit_finished.emit(ctx.jobset_id, ok)
        if not ok:
            sub._gate_reject(
                ctx, finish=sub.config.submit_finished_on_gate_reject)
            return
        # started(submit_started)는 do_launch가 착수 지점에서 발화한다 — 여기서
        # 중복 발화하지 않는다 (started/finished 짝 계약, 단일 발화점).
        try:
            self.do_launch()
        except Exception as e:                       # noqa: BLE001
            # do_launch(레코드 add_jobs / 워커 spawn)에서 store 장애 등으로
            # 예외가 나면 게이트 워커가 여기서 죽어 finished가 영영 미발화 →
            # jobset이 잠긴다. error + finished(failed=N)로 반드시 마무리한다.
            log.exception("게이트 통과 후 제출 착수 실패: %s", ctx.jobset_id)
            sub._gate_fail(ctx, repr(e))
            return
        if ctx.total == 0:                           # 빈 제출 — 직접 마무리
            sub._finish_if_done(ctx, force=True)


