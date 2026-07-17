"""BulkSubmitter — QThreadPool 기반 대량 submit (FR-1, FR-2).

- 병렬/순차 submit, rate limit(token bucket), progress throttle (QT-5)
- 실패 시 QTimer 스케줄 retry (RETRY_WAIT → SUBMITTING, sleep 점유 없음)
- cancel: job 단위 경계에서 안전 중단, 이미 submit된 job은 정상 기록 (QT-6)
- worker 예외 격리 → error Signal (CS-5)

retry 원장(ctx.pending_retries): QTimer 대기 중인 재시도는 pool 밖에 있어
waitForDone이 기다려주지 않는다. 대기 중 재시도를 전부 원장에 등록해 두고,
발화(fire)·포기(cancel/shutdown)가 원장에서의 원자적 pop으로만 결정되게
하면 어느 쪽이 먼저 오든 정확히 한 번만 처리된다.
"""
from __future__ import annotations

import logging
import os
import shlex
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence

from .command import LsfCommand
from .config import JobSpec, LsfConfig, spec_to_json
from .errors import SubmitError
from .jobset_core import JobSetManager
from .lifecycle import SubmitGate
from .options import Options
from .qt import QObject, QRunnable, QThreadPool, QTimer, Signal, Slot
from .reports import SubmitProgress, SubmitReport
from .states import JobRecord, JobState
from .store.base import JobSetStore
from .util import EmitThrottler, TokenBucketLimiter

log = logging.getLogger("lsfmgr.submit")


@dataclass
class _PendingRetry:
    """QTimer 대기 중인 재시도 1건 (job 1개)."""
    keys: List[str]                     # 포기 시 SUBMIT_FAILED 확정할 job_key들
    delay_s: float
    make_task: Callable[[], QRunnable]  # 발화 시 pool에 넣을 task 생성


@dataclass
class _SubmitContext:
    """jobset 1건의 submit 진행 상태 (thread-safe 카운터)."""
    jobset_id: str
    total: int
    max_retry: int
    pool: QThreadPool
    limiter: TokenBucketLimiter
    options: Options = field(default_factory=Options)
    throttler: EmitThrottler = field(default_factory=EmitThrottler)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    started_at: float = field(default_factory=time.monotonic)
    done: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    retried_keys: set = field(default_factory=set)
    fail_reasons: Dict[str, int] = field(default_factory=dict)
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


class _BaseSubmitTask(QRunnable):
    """단건 submit worker 공통 골격 (CS-5: 예외는 submitter로 격리 전달).

    공통 흐름(취소 확인 → SUBMITTING 전이 → rate limit → submit → 성공/‏실패
    처리)만 여기 두고, 실제 submit 호출(`_do_submit`)과 재시도 task 생성
    (`_retry_factory`)만 서브클래스가 구현한다.
    """

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, attempt: int,
                 submit_cwd: Optional[str] = None):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.job_key = job_key
        self.attempt = attempt          # 0 == 최초 시도
        self.submit_cwd = submit_cwd    # 제출 subprocess의 작업 디렉토리(요청값)

    def run(self):
        try:
            self._run()
        except Exception as e:          # noqa: BLE001 — worker 스레드 보호
            log.exception("submit worker 예외: %s", self.job_key)
            self.submitter._task_crashed(self.ctx, self.job_key, e)

    def _run(self):
        sub, ctx = self.submitter, self.ctx
        if ctx.cancel_event.is_set():
            sub._task_cancelled(ctx, self.job_key)
            return
        sub.store.transition(ctx.jobset_id, self.job_key, JobState.SUBMITTING)
        if not ctx.limiter.acquire(ctx.cancel_event):   # rate limit (NFR-4)
            sub._task_cancelled(ctx, self.job_key)
            return
        try:
            job_id = self._do_submit()
        except SubmitError as e:
            sub._task_failed(ctx, self.job_key, self.attempt, e,
                             self._retry_factory())
            return
        sub._task_succeeded(ctx, self.job_key, job_id)

    # --- 서브클래스 구현 지점 ---
    def _do_submit(self) -> int:
        """실제 제출 수행 후 job_id 반환. 실패 시 SubmitError."""
        raise NotImplementedError

    def _retry_factory(self):
        """attempt→새 task 를 만드는 콜백을 반환한다. 지연 실행되므로 값(로컬)만
        캡처하고 self(autoDelete QRunnable)는 참조하지 않는다."""
        raise NotImplementedError


class _SubmitTask(_BaseSubmitTask):
    """bsub 1회 수행 worker (lsfmgr 가 -q/-J/-g 등 인자를 조립)."""

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, spec: JobSpec, attempt: int,
                 submit_cwd: Optional[str] = None):
        super().__init__(submitter, ctx, job_key, attempt, submit_cwd)
        self.spec = spec

    def _do_submit(self) -> int:
        sub, ctx = self.submitter, self.ctx
        js = sub.store.get_jobset(ctx.jobset_id)
        group = js.lsf_group_paths[0] if js.lsf_group_paths else None
        opts = ctx.options
        # 우선순위: JobSpec 필드 > call 옵션 (§1.2)
        outfile, errfile = self.spec.outfile, self.spec.errfile
        if opts.output_dir and not outfile:
            outfile = os.path.join(opts.output_dir, f"{self.job_key}.out")
        if opts.output_dir and not errfile:
            errfile = os.path.join(opts.output_dir, f"{self.job_key}.err")
        return sub.command.bsub(
            self.spec.command,
            queue=(self.spec.queue if self.spec.queue is not None
                   else (opts.queue or None)),
            job_name=self.job_key,           # -J <jobset_id>_<idx> (FR-1.4)
            group_path=group,                # -g /lsfmgr/<user>/<jsid>
            resources=self.spec.resources or opts.resource_req,
            outfile=outfile, errfile=errfile,
            extra_args=self.spec.extra_args, env=self.spec.env,
            timeout_s=opts.submit_timeout_s,
            cwd=self.submit_cwd)             # 요청 작업 디렉토리(없으면 부모 cwd)

    def _retry_factory(self):
        sub, ctx, job_key, spec, cwd = (self.submitter, self.ctx,
                                        self.job_key, self.spec,
                                        self.submit_cwd)
        return lambda att: _SubmitTask(sub, ctx, job_key, spec, att, cwd)


class _WrapperSubmitTask(_BaseSubmitTask):
    """wrapper 커맨드 1개를 '그대로' 실행하는 worker (wrapper 제출용).

    lsfmgr 는 인자를 조립하지 않는다 — argv(예: ["customwrapper_sub","-i","a.sp"])를
    subprocess 로 실행하고 stdout 의 'Job <id>' 만 파싱한다. 관리는 그렇게 얻은
    job_id 로만 이뤄진다(그룹/이름 부착물 없음).
    """

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, argv: Sequence[str], attempt: int,
                 submit_cwd: Optional[str] = None):
        super().__init__(submitter, ctx, job_key, attempt, submit_cwd)
        self.argv = list(argv)

    def _do_submit(self) -> int:
        return self.submitter.command.run_submit(
            self.argv, timeout_s=self.ctx.options.submit_timeout_s,
            cwd=self.submit_cwd)             # wrapper는 -cwd 대신 subprocess cwd

    def _retry_factory(self):
        sub, ctx, job_key, argv, cwd = (self.submitter, self.ctx,
                                        self.job_key, self.argv,
                                        self.submit_cwd)
        return lambda att: _WrapperSubmitTask(sub, ctx, job_key, argv, att, cwd)


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
                 jobset_manager: JobSetManager,
                 config: Optional[LsfConfig] = None,
                 parent: Optional[QObject] = None,
                 gate: Optional[SubmitGate] = None):
        super().__init__(parent)
        self.store = store
        self.command = command
        self.jobsets = jobset_manager
        self.config = config or command.config
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

        keyed_items: [(job_key, JobSpec | argv토큰리스트), ...] — item 타입으로
        제출 경로를 job 단위로 고른다(JobSpec=bsub 조립, list=wrapper 그대로).
        merge로 wrapper/bsub job이 한 jobset에 섞여 있어도 정확히 동작한다.
        새 CREATED 레코드를 만들지 않고 **이미 존재하는 레코드**를 리셋(이전
        job_id/exit_code/실행시간 초기화 + command 갱신) 후 같은 job_key로
        재submit한다. 결과는 finished(SubmitReport).
        pre_submit(commands)->bool 지정 시 **리셋 이전에** 게이트 워커에서
        검사한다 — False/예외면 레코드를 건드리지 않고 끝난다 (FR-9).

        반환: 제출이 실제 착수됐으면 True. shutdown/born-cancelled(kill
        barrier 중 시작 — 레코드 원상 유지, 전원 취소 정산)면 False —
        caller는 rearm/polling 재개 등 착수 전제 후속 작업을 생략해야 한다."""
        if self._shutdown:
            # shutdown 후 queued 경로로 도달할 수 있다 — 새 pool/프로세스를
            # 만들면 아무도 기다려주지 않는 좀비가 된다 (CS-8)
            log.warning("shutdown 후 재제출 요청 무시: %s", jobset_id)
            return False
        keyed = list(keyed_items)
        ctx = self._new_context(jobset_id, len(keyed), options)
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
            # pre_submit_* 신호도, do_launch의 started도 나가지 않는다. 그대로면
            # 아무 착수 신호 없이 finished(cancelled)만 나가 started↔finished를
            # 쌍으로 세는 구독자(스피너/제출버튼 게이팅)의 카운터가 음수로
            # 내려간다. 게이트가 실행되지 않아 순서 충돌이 없으니 finished 직전에
            # started를 내 최소 짝(started→finished)을 맞춘다.
            self._safe_emit(self.started, jobset_id)
            self._gate_reject(ctx, finish=True)
            return False
        def do_launch():
            log.info("submit 착수 %s: %d건", jobset_id, len(keyed))
            # started(=submit_started)를 **실제 착수 지점**에서 발화한다 — 게이트
            # 통과 후/비게이트 리셋 시작 직전. shutdown·born-cancelled로 do_launch에
            # 도달 못 하면 started도 없어 'started만 나가고 finished가 영영 안 오는'
            # 고아를 원천 차단한다 (started/finished 짝 계약).
            self._safe_emit(self.started, jobset_id)
            # 기존 레코드 리셋 — 이전 실행의 흔적(job_id/exit_code/실행시간/
            # 위치)을 지우고 새 command 반영. 지우지 않으면 재제출 실패 시
            # 죽은 옛 job_id·이전 실행의 start/finish/working_dir가 잔존한다.
            # 상태는 곧장 SUBMITTING으로 — 재제출은 즉시 제출 착수라,
            # EXIT(kill) → SUBMITTING → PEND로 자연스럽게 보인다.
            launch = []
            reset_recs = []
            for key, item in keyed:
                try:
                    rec = self.store.transition(
                        jobset_id, key, JobState.SUBMITTING,
                        job_id=None, exit_code=None, fail_reason=None,
                        fail_message=None,
                        retry_count=0, command=self._item_command(item),
                        submit_time=None, run_time_s=None, start_time=None,
                        finish_time=None, working_dir=None,
                        source_cluster=None, forward_cluster=None,
                        spec_json=(spec_to_json(item)
                                   if isinstance(item, JobSpec) else None))
                except Exception:                # noqa: BLE001 — CS-5
                    # 키 소실(remove_job 경합)·store 장애 어느 쪽이든 이
                    # 키만 건너뛰고 나머지는 진행 — 여기서 전파되면 ctx가
                    # 미완(finished 미발행)으로 고착되어 jobset이 잠긴다
                    log.exception("재제출 리셋 실패 — 건너뜀: %s/%s",
                                  jobset_id, key)
                    self._count(ctx, cancelled=True)
                    continue
                if rec is not None:
                    reset_recs.append(rec)
                # submit_cwd는 리셋이 건드리지 않아 rec에 보존된다 — task로
                # 실어 subprocess cwd로 쓴다(rec None인 드문 경우만 부모 cwd).
                launch.append((key, item,
                               rec.submit_cwd if rec is not None else None))
            # task 객체를 **먼저 전부** 만든다 — 생성이 실패하면 어느 task도
            # 시작하기 전에 예외가 난다. 부분 착수(일부 task는 이미 실행 중인데
            # do_launch가 죽어 _gate_fail이 그 레코드를 SUBMIT_FAILED로 되돌리면
            # 실행 중 bsub가 고아가 된다)를 원천 차단한다.
            tasks = [self._make_resubmit_task(ctx, key, item, cwd)
                     for key, item, cwd in launch]
            # 리셋된 SUBMITTING 즉시 발행 — 완료를 안 기다리고 표에 반영.
            # user slot 예외를 격리한다(CS-5): 전파되면 do_launch가 죽어 ctx가
            # 미완으로 고착되고 jobset이 영구 잠긴다.
            if reset_recs:
                self._safe_emit(self.jobs_changed, jobset_id, reset_recs)
            # **착수 확정** = 레코드 리셋 완료 + task 생성 성공. 이 시점(pool.start
            # **직전**)에 manager가 rearm/post_process 무장/AUTO-1을 하도록 arming
            # 신호를 보낸다. pool.start보다 먼저 발행해야 즉시 완주하는 task의
            # finished가 records_reset보다 먼저 main에 도착해 무장 전에 post_process
            # 판정(no-op)이 나는 유실을 막는다. (리셋은 위에서 끝나 레코드는
            # SUBMITTING — 옛 terminal 오발화 창도 없다. pool.start는 예외 없음)
            self._safe_emit(self.records_reset, jobset_id, ctx.arm_token)
            for task in tasks:
                ctx.pool.start(task)
            if not launch:
                QTimer.singleShot(0,
                                  lambda: self._finish_if_done(ctx, force=True))

        if pre_submit is None:
            # 게이트 경로와 대칭으로 래핑 — do_launch가(주로 store 장애/드문
            # pool.start 예외로) 죽어도 ctx를 미완으로 두지 않고 실패 확정한다.
            # (게이트 경로는 _GateTask.run이 같은 목적으로 래핑한다)
            try:
                do_launch()
            except Exception as e:               # noqa: BLE001 — CS-5
                log.exception("제출 착수 실패: %s", jobset_id)
                self._gate_fail(ctx, [], repr(e))
            return True
        # 게이트 경로 — 리셋 **이전**에 검사하므로 False/예외면 레코드 원상
        # 유지 (make_failed=[]: 예외 시에도 새 레코드를 만들지 않는다 —
        # error + finished(failed=N)로만 마무리, FR-9)
        commands = [self._item_command(item) for _key, item in keyed]
        ctx.pool.start(_GateTask(self, ctx, commands, pre_submit,
                                 do_launch, lambda msg: []))
        return True

    @staticmethod
    def _item_command(item) -> str:
        """레코드에 저장할 command 문자열 — wrapper argv는 shlex 인용을 보존해
        재제출 시 shlex.split 왕복이 원본 argv를 복원하게 한다."""
        return item.command if isinstance(item, JobSpec) else shlex.join(item)

    def _make_resubmit_task(self, ctx: _SubmitContext, key: str,
                            item, submit_cwd: Optional[str] = None) -> QRunnable:
        if isinstance(item, JobSpec):
            return _SubmitTask(self, ctx, key, item, 0, submit_cwd)
        return _WrapperSubmitTask(self, ctx, key, item, 0, submit_cwd)

    def is_active(self, jobset_id: str) -> bool:
        """해당 jobset에 아직 끝나지 않은 submit 사이클이 있는지 (resubmit_jobs 가드)."""
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        return ctx is not None and not ctx.finished

    def progress_snapshot(self, jobset_id: str) -> Optional["SubmitProgress"]:
        """진행 중 submit의 실시간 스냅샷 — 없거나 이미 끝났으면 None.
        submit_progress Signal을 못 받은 시점에도 현재 진행을 pull로 조회한다."""
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        if ctx is None:
            return None
        with ctx.lock:                       # 카운터 원자적 읽기
            if ctx.finished:
                return None
            return SubmitProgress(
                jobset_id=jobset_id, done=ctx.done, total=ctx.total,
                succeeded=ctx.succeeded, failed=ctx.failed,
                cancelled=ctx.cancelled)

    def _new_context(self, jobset_id: str, total: int,
                     options: Options) -> _SubmitContext:
        """submit 사이클 1건의 pool/ctx 구성 + 등록 (submit/resubmit/array 공통).

        SubmitGate 등록까지 여기서 한다 — kill barrier가 올라가 있으면
        born-cancelled(cancel_event가 켜진 채 시작): 어떤 job도 LSF에
        제출되지 않고 기존 취소 경로(CREATED 복귀, finished(cancelled))로
        마무리된다. barrier 확인과 등록이 게이트 lock 아래 원자적이라
        'kill의 취소를 빠져나가는 늦은 사이클'이 구조적으로 불가능하다."""
        pool = QThreadPool()
        pool.setMaxThreadCount(options.workers)
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=total,
            max_retry=options.max_retry, pool=pool,
            limiter=TokenBucketLimiter(options.rate_limit_per_s),
            throttler=self._make_throttler(), options=options)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx
        # 진행 중 bsub는 submit_timeout_s로 반드시 끝난다(subprocess timeout).
        # 취소된 나머지 worker는 즉시 빠지므로 여유만 더한 대기 상한.
        ctx.gate_token = self._gate.register(
            jobset_id, ctx.cancel_event,
            lambda t: ctx.pool.waitForDone(int(t * 1000)),
            options.submit_timeout_s + 30.0)
        if ctx.gate_token is None:
            ctx.cancel_event.set()           # born-cancelled
        return ctx

    def _make_throttler(self) -> EmitThrottler:
        """config의 progress throttle 설정으로 EmitThrottler 생성 (QT-5)."""
        return EmitThrottler(self.config.progress_min_interval_s,
                             self.config.progress_min_step_ratio)

    def cancel_submit(self, jobset_id: str) -> None:
        """진행 중 submit 중단 (QT-6). 이미 submit된 job은 유지."""
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        if ctx is not None:
            ctx.cancel_event.set()

    def shutdown(self) -> None:
        """모든 submit 중단 요청 후 pool join (CS-8).
        진행 중이던 bsub는 완료까지 기다려 job_id 유실을 막는다."""
        self._shutdown = True
        with self._ctx_lock:
            contexts = list(self._contexts.values())
        for ctx in contexts:
            ctx.cancel_event.set()
        for ctx in contexts:
            ctx.pool.waitForDone(-1)
        # QTimer 대기 중인 재시도는 이벤트 루프가 곧 끝나면 영영 발화하지
        # 않는다 — 원장 잔류분을 여기서 확정해야 RETRY_WAIT가 비terminal로
        # 잔존하지 않고 finished도 발행된다.
        # waitForDone 이후에는 원장에 새 항목이 추가되지 않는다.
        for ctx in contexts:
            with ctx.lock:
                entries = list(ctx.pending_retries.values())
                ctx.pending_retries.clear()
            for entry in entries:
                self._finalize_retry(ctx, entry, "SHUTDOWN")

    def abort_retries(self, jobset_id: str) -> None:
        """이 jobset의 대기 중 재시도(QTimer)를 포기 확정 — RETRY_WAIT →
        SUBMIT_FAILED. 전체 kill과 함께 호출해, kill 뒤 재시도 타이머가
        발화해 job이 부활하는 것을 막는다. 이후 타이머가 발화해도 원장
        pop이 빈손이라 no-op(정확히 한 번)."""
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        if ctx is None:
            return
        with ctx.lock:
            entries = list(ctx.pending_retries.values())
            ctx.pending_retries.clear()
        for entry in entries:
            self._finalize_retry(ctx, entry, "KILLED")

    # ------------------------------------------------------------------
    # worker 콜백 (worker 스레드에서 호출됨 — Store는 thread-safe)
    # ------------------------------------------------------------------
    def _task_succeeded(self, ctx: _SubmitContext, job_key: str,
                        job_id: int) -> None:
        rec = self.store.transition(ctx.jobset_id, job_key, JobState.PEND,
                                    job_id=job_id, submit_time=datetime.now(),
                                    fail_reason=None, fail_message=None)
        self._count(ctx, succeeded=True, changed=rec)

    def _task_failed(self, ctx: _SubmitContext, job_key: str, attempt: int,
                     err: SubmitError,
                     retry_factory: Callable[[int], QRunnable]) -> None:
        """실패 처리. err.retryable=False(예: NO_JOBID_PARSED)면 재시도 없이
        바로 SUBMIT_FAILED 확정한다. 재시도 task는 retry_factory(attempt+1)로
        생성한다(bsub 경로/‏wrapper 경로가 각자 자기 task 를 만든다)."""
        log.warning("submit 실패 [%s] %s: %s", err.fail_reason, job_key, err)
        if (getattr(err, "retryable", True) and attempt < ctx.max_retry
                and not ctx.cancel_event.is_set() and not self._shutdown):
            # RETRY_WAIT → QTimer 스케줄 (스레드 sleep 점유 금지, §3.2)
            # fail_message: 재시도 대기 중에도 마지막 시도의 터미널 메시지를
            # 표에 보여줄 수 있고, 포기 확정(_finalize_retry) 시에도 잔존한다
            self.store.transition(ctx.jobset_id, job_key, JobState.RETRY_WAIT,
                                  retry_count=attempt + 1,
                                  fail_reason=err.fail_reason,
                                  fail_message=err.diagnostic()[:4000])
            with ctx.lock:
                ctx.retried_keys.add(job_key)
            self._schedule_retry(
                ctx, [job_key], ctx.options.retry_delay_s(attempt),
                lambda: retry_factory(attempt + 1))
            return
        log.error("SUBMIT_FAILED 확정 [%s] %s (%d회 시도)",
                  err.fail_reason, job_key, attempt + 1)      # NFR-6 ERROR
        rec = self.store.transition(ctx.jobset_id, job_key,
                                    JobState.SUBMIT_FAILED,
                                    retry_count=attempt,
                                    fail_reason=err.fail_reason,
                                    fail_message=err.diagnostic()[:4000])
        self._count(ctx, failed=True, reason=err.fail_reason, changed=rec)

    def _task_cancelled(self, ctx: _SubmitContext, job_key: str) -> None:
        self._revert_to_created(ctx, [job_key])

    def _revert_to_created(self, ctx: _SubmitContext,
                           keys: List[str]) -> None:
        """kill/cancel 안전 지점 중단 — 아직 submit 전인 job을 CREATED로
        복귀시키고 작업 1단위 완료로 계상한다 (QT-6, bulk/array 공용 정책).

        - guard(CAS)로 SUBMITTING/RETRY_WAIT일 때만 전이 — 그새 다른 상태로
          바뀐(또는 remove_job으로 소실된) 키는 조용히 건너뛴다.
        - 이전 시도의 실패 잔재(fail_reason/fail_message/retry_count)를 함께
          리셋한다 — 안 지우면 '제출된 적 없는' CREATED job이 실패 이력을
          달고 UI/store에 남는다 (_task_succeeded가 성공 시
          fail_reason=None을 명시 전달하는 것과 대칭).
        - CREATED 복귀도 changed로 발행한다 — CREATED는 폴링 대상(is_on_lsf)
          이 아니라 여기서 안 알리면 UI 표가 SUBMITTING에 영구 고착된다.
        """
        def guard(cur):
            return cur.state in (JobState.SUBMITTING, JobState.RETRY_WAIT)
        changed = list(self.store.transition_many(
            ctx.jobset_id,
            [(k, JobState.CREATED, guard,
              {"fail_reason": None, "fail_message": None, "retry_count": 0})
             for k in keys]))
        self._count(ctx, cancelled=True, changed=changed)

    def _task_crashed(self, ctx: _SubmitContext, job_key: str,
                      err: Exception) -> None:
        """분류 불가 예외 — SUBMIT_FAILED 처리 + error Signal (CS-5)."""
        try:
            self.store.transition(ctx.jobset_id, job_key,
                                  JobState.SUBMIT_FAILED,
                                  fail_reason="INTERNAL_ERROR",
                                  fail_message=repr(err)[:4000])
        except Exception:                       # noqa: BLE001
            log.exception("crash 후 전이 실패: %s", job_key)
        self.error.emit(ctx.jobset_id, f"{job_key}: {err!r}")
        self._count(ctx, failed=True, reason="INTERNAL_ERROR")

    # ------------------------------------------------------------------
    # retry 스케줄 — 원장 등록(worker 스레드) → QTimer 발화(submitter 스레드)
    # ------------------------------------------------------------------
    def _schedule_retry(self, ctx: _SubmitContext, keys: List[str],
                        delay_s: float,
                        make_task: Callable[[], QRunnable]) -> None:
        """RETRY_WAIT 전이 직후 worker 스레드에서 호출 — 원장에 등록한다."""
        with ctx.lock:
            ctx.pending_retries[keys[0]] = _PendingRetry(keys, delay_s,
                                                         make_task)
        self._retry_requested.emit(ctx.jobset_id, keys[0])

    @Slot(str, str)
    def _on_retry_requested(self, jobset_id: str, entry_key: str) -> None:
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        if ctx is None:
            return

        def fire():
            with ctx.lock:
                entry = ctx.pending_retries.pop(entry_key, None)
            if entry is None:
                return                  # shutdown이 이미 확정 — 정확히 한 번
            if self._shutdown or ctx.cancel_event.is_set():
                self._finalize_retry(ctx, entry, "CANCELLED")
                return
            ctx.pool.start(entry.make_task())

        if self._shutdown or ctx.cancel_event.is_set():
            fire()                      # 대기 없이 즉시 포기 확정
            return
        with ctx.lock:
            entry = ctx.pending_retries.get(entry_key)
        if entry is not None:
            QTimer.singleShot(int(entry.delay_s * 1000), fire)

    def _finalize_retry(self, ctx: _SubmitContext, entry: _PendingRetry,
                        default_reason: str) -> None:
        """재시도 포기 — RETRY_WAIT 잔류분을 SUBMIT_FAILED로 최종 확정.
        jobset이 이미 삭제됐어도(merge 등) 카운터 확정은 계속한다."""
        changed = []
        for key in entry.keys:
            try:
                rec = self.store.get_job(ctx.jobset_id, key)
                if rec.state is JobState.RETRY_WAIT:
                    new = self.store.transition(ctx.jobset_id, key,
                                                JobState.SUBMIT_FAILED,
                                                fail_reason=rec.fail_reason
                                                or default_reason)
                    if new is not None:
                        changed.append(new)
            except Exception:                    # noqa: BLE001 — CS-5
                # store 장애(sqlite lock 등)여도 _count까지는 반드시 도달해야
                # 한다 — 여기서 전파되면 done<total 고착 → finished 미발행
                log.exception("retry 포기 확정 실패(무시): %s/%s",
                              ctx.jobset_id, key)
        self._count(ctx, failed=True, changed=changed)

    # ------------------------------------------------------------------
    # 진행/완료 통지 — 모든 종료 경로의 단일 출구
    # ------------------------------------------------------------------
    def _count(self, ctx: _SubmitContext, *, succeeded: bool = False,
               failed: bool = False, cancelled: bool = False,
               reason: Optional[str] = None,
               changed=None) -> None:
        """작업 1단위(bulk job 1개 / array 1회) 완료 계상 + 진행/완료 통지.
        changed(전이된 JobRecord 또는 리스트)는 버퍼에 쌓아 progress와 같은
        cadence로 jobs_changed 배치 발행한다."""
        with ctx.lock:
            ctx.done += 1
            if succeeded:
                ctx.succeeded += 1
            if failed:
                ctx.failed += 1
            if cancelled:
                ctx.cancelled += 1
            if reason is not None:
                ctx.fail_reasons[reason] = ctx.fail_reasons.get(reason, 0) + 1
            if changed is not None:
                if isinstance(changed, list):
                    ctx.changed_buffer.extend(changed)
                else:
                    ctx.changed_buffer.append(changed)
        self._emit_progress(ctx)
        self._finish_if_done(ctx)

    def _emit_progress(self, ctx: _SubmitContext) -> None:
        # 발화(progress·jobs_changed)를 ctx.lock 안에서 수행한다 — drain과 emit이
        # 원자적이어야, 다른 worker 스레드의 _finish_if_done가 그 사이에 끼어들어
        # finished를 마지막 per-job jobs_changed보다 먼저 post하는 경합을 막는다
        # (worker→main은 queued connection이라 lock 중 emit은 post만 한다).
        with ctx.lock:
            if ctx.finished:
                return                   # 최종 flush는 _finish_if_done 담당
            done, total = ctx.done, ctx.total
            if not ctx.throttler.should_emit(done, total):   # QT-5 throttle
                return
            batch = ctx.changed_buffer
            ctx.changed_buffer = []
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
    # pre_submit 게이트 (FR-9) — 제출 전 단일 워커 검사
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
        """Signal 발화 중 user slot 예외를 격리한다 (CS-5) — do_launch 등
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

    def _gate_reject(self, ctx: _SubmitContext, finish: bool) -> None:
        """게이트 False/취소 — 제출하지 않음(레코드 미생성 → 요약은 N CREATED
        유지). finish=True일 때만 submit_finished(cancelled=N)를 발화한다
        (False 반환은 config.submit_finished_on_gate_reject, 취소는 항상 True).
        착수가 없었으므로 gate_rejected로 manager의 보류 무장분을 정리한다."""
        # gate_rejected(무장 정리)를 finished보다 **먼저** 발화한다 — 같은
        # 스레드에서 queued될 때 manager가 finished(submit 완료) 처리 전에
        # _pending_arm을 비우도록 (착수가 없었으니 무장분은 폐기돼야 한다).
        self._safe_emit(self.gate_rejected, ctx.jobset_id, ctx.arm_token)
        with ctx.lock:
            if ctx.finished:
                return
            ctx.finished = True
            ctx.cancelled = ctx.total
            report = self._make_report(ctx)
            if finish:
                self._safe_emit(self.finished, ctx.jobset_id, report)
        log.info("pre_submit 게이트 거부 %s (finished 발화=%s)",
                 ctx.jobset_id, finish)
        self._drop_ctx(ctx)

    def _gate_fail(self, ctx: _SubmitContext, failed_records: list,
                   msg: str) -> None:
        """게이트/착수 예외 — 실패 확정. 리셋됐지만 착수 못 한 SUBMITTING
        레코드를 SUBMIT_FAILED로 되돌려 고착을 막고(비게이트 do_launch 예외
        대비), error + finished + gate_rejected(무장 정리)로 마무리한다."""
        try:
            created = self.store.store_add_jobs(failed_records)
            if created:
                self._safe_emit(self.jobs_changed, ctx.jobset_id, list(created))
        except Exception:                    # noqa: BLE001 — CS-5
            log.exception("게이트 실패 레코드 생성 실패: %s", ctx.jobset_id)
        # 리셋만 되고 착수 못 한 SUBMITTING 레코드 un-stick — 안 하면 재제출
        # 가드(활성 job)에 걸려 jobset이 잠긴다. **CAS guard**: 그새 실행 중
        # task가 SUBMITTING→PEND로 옮겼으면 되돌리지 않는다(실행 중 bsub를
        # SUBMIT_FAILED로 덮으면 그 LSF job이 고아가 된다).
        stuck = []
        try:
            recs = self.store.get_jobs(ctx.jobset_id)
        except Exception:                    # noqa: BLE001 — CS-5
            log.exception("착수 실패 레코드 조회 실패: %s", ctx.jobset_id)
            recs = []
        for r in recs:
            if r.state is not JobState.SUBMITTING:
                continue
            # 개별 transition을 각자 try로 — 한 key가 remove_job 경합으로
            # JobNotFoundError를 던져도 루프가 통째로 죽어 나머지가 SUBMITTING에
            # 갇히면(재제출 busy 가드에 걸려 영구 잠김) 안 된다. 그 key만 건너뛴다.
            try:
                nr = self.store.transition(
                    ctx.jobset_id, r.job_key, JobState.SUBMIT_FAILED,
                    guard=lambda cur: cur.state is JobState.SUBMITTING,
                    fail_reason="LAUNCH_FAILED", fail_message=msg[:4000])
                if nr is not None:
                    stuck.append(nr)
            except Exception:                # noqa: BLE001 — CS-5
                log.exception("착수 실패 레코드 정리 실패 — 건너뜀: %s/%s",
                              ctx.jobset_id, r.job_key)
        if stuck:
            self._safe_emit(self.jobs_changed, ctx.jobset_id, stuck)
        # user error 슬롯 예외 격리 — main 스레드 direct 연결에서 전파되면
        # 아래 ctx.finished 설정 전에 죽어 jobset이 영구 잠긴다 (CS-5)
        self._safe_emit(self.error, ctx.jobset_id, f"pre_submit: {msg}")
        # gate_rejected를 finished보다 먼저 (무장 정리 우선 — _gate_reject 주석)
        self._safe_emit(self.gate_rejected, ctx.jobset_id, ctx.arm_token)
        with ctx.lock:
            if not ctx.finished:
                ctx.finished = True
                ctx.failed = ctx.total
                ctx.fail_reasons["PRE_SUBMIT_FAILED"] = ctx.total
                report = self._make_report(ctx)
                self._safe_emit(self.finished, ctx.jobset_id, report)
        self._drop_ctx(ctx)


class _GateTask(QRunnable):
    """pre_submit 게이트 워커 (FR-9) — 단일 스레드에서 커맨드 리스트 전체를
    1회 검사. True면 do_launch()로 실제 제출 착수, False/예외면 거부/실패."""

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 commands: list, pre_submit,
                 do_launch, make_failed):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.commands = commands
        self.pre_submit = pre_submit
        self.do_launch = do_launch
        self.make_failed = make_failed

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
        except Exception as e:                       # noqa: BLE001 — CS-5
            log.exception("pre_submit 게이트 예외: %s", ctx.jobset_id)
            sub.pre_submit_finished.emit(ctx.jobset_id, False)
            sub._gate_fail(ctx, self.make_failed(repr(e)[:4000]), repr(e))
            return
        if ok and (ctx.cancel_event.is_set() or sub._shutdown):
            # 통과했지만 그새 취소/종료 — ok=True를 먼저 알리면 manager가
            # rearm/AUTO-1 폴링 재개를 수행해, 재실행이 없는데 terminal
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
        except Exception as e:                       # noqa: BLE001 — CS-5
            # do_launch(레코드 add_jobs / 워커 spawn)에서 store 장애 등으로
            # 예외가 나면 게이트 워커가 여기서 죽어 finished가 영영 미발화 →
            # jobset이 잠긴다. error + finished(failed=N)로 반드시 마무리한다.
            log.exception("게이트 통과 후 제출 착수 실패: %s", ctx.jobset_id)
            sub._gate_fail(ctx, self.make_failed(repr(e)[:4000]), repr(e))
            return
        if ctx.total == 0:                           # 빈 제출 — 직접 마무리
            sub._finish_if_done(ctx, force=True)


