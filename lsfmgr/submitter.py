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
import stat as stat_mod
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence

from .command import LsfCommand
from .config import ArrayJobSpec, JobSpec, LsfConfig, spec_to_json
from .errors import SubmitError
from .jobset_core import JobSetManager
from .options import Options
from .qt import QObject, QRunnable, QThreadPool, QTimer, Signal, Slot
from .reports import SubmitReport
from .states import JobRecord, JobState
from .store.base import JobSetStore
from .util import EmitThrottler, TokenBucketLimiter

log = logging.getLogger("lsfmgr.submit")


@dataclass
class _PendingRetry:
    """QTimer 대기 중인 재시도 1건 (bulk job 1개 또는 array 1회분)."""
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


class _BaseSubmitTask(QRunnable):
    """단건 submit worker 공통 골격 (CS-5: 예외는 submitter로 격리 전달).

    공통 흐름(취소 확인 → SUBMITTING 전이 → rate limit → submit → 성공/‏실패
    처리)만 여기 두고, 실제 submit 호출(`_do_submit`)과 재시도 task 생성
    (`_retry_factory`)만 서브클래스가 구현한다.
    """

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, attempt: int):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.job_key = job_key
        self.attempt = attempt          # 0 == 최초 시도

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
                 job_key: str, spec: JobSpec, attempt: int):
        super().__init__(submitter, ctx, job_key, attempt)
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
            timeout_s=opts.submit_timeout_s)

    def _retry_factory(self):
        sub, ctx, job_key, spec = (self.submitter, self.ctx,
                                   self.job_key, self.spec)
        return lambda att: _SubmitTask(sub, ctx, job_key, spec, att)


class _WrapperSubmitTask(_BaseSubmitTask):
    """wrapper 커맨드 1개를 '그대로' 실행하는 worker (submit_wrapper 용).

    lsfmgr 는 인자를 조립하지 않는다 — argv(예: ["primesim_sub","-i","a.sp"])를
    subprocess 로 실행하고 stdout 의 'Job <id>' 만 파싱한다. 관리는 그렇게 얻은
    job_id 로만 이뤄진다(그룹/이름 부착물 없음).
    """

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, argv: Sequence[str], attempt: int):
        super().__init__(submitter, ctx, job_key, attempt)
        self.argv = list(argv)

    def _do_submit(self) -> int:
        return self.submitter.command.run_submit(
            self.argv, timeout_s=self.ctx.options.submit_timeout_s)

    def _retry_factory(self):
        sub, ctx, job_key, argv = (self.submitter, self.ctx,
                                   self.job_key, self.argv)
        return lambda att: _WrapperSubmitTask(sub, ctx, job_key, argv, att)


class BulkSubmitter(QObject):
    """대량 submit 진입점. manager(Facade)가 소유."""

    progress = Signal(str, int, int)          # jobset_id, done, total
    finished = Signal(str, object)            # jobset_id, SubmitReport
    error = Signal(str, str)                  # jobset_id, message
    jobs_changed = Signal(str, list)          # jobset_id, [JobRecord] 전이 배치
    started = Signal(str)                     # jobset_id — 게이트 통과 후 제출 착수
    ready_started = Signal(str)               # jobset_id — pre_submit 게이트 시작
    ready_finished = Signal(str, bool)        # jobset_id, ok — 게이트 종료(True=통과)
    # 내부용 — worker 스레드에서 emit → submitter 소속 스레드에서 QTimer 스케줄
    _retry_requested = Signal(str, str)       # jobset_id, 원장 키

    def __init__(self, store: JobSetStore, command: LsfCommand,
                 jobset_manager: JobSetManager,
                 config: Optional[LsfConfig] = None,
                 parent: Optional[QObject] = None):
        super().__init__(parent)
        self.store = store
        self.command = command
        self.jobsets = jobset_manager
        self.config = config or command.config
        self._contexts: Dict[str, _SubmitContext] = {}
        self._ctx_lock = threading.Lock()
        self._shutdown = False
        self._retry_requested.connect(self._on_retry_requested)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def submit_bulk(self, jobset_id: str, specs: Sequence[JobSpec],
                    options: Options, pre_submit=None) -> None:
        """[async→Signal] 대량 submit. CREATED 레코드 생성 후 즉시 반환,
        실제 bsub는 QThreadPool worker에서 수행 (QT-1). 결과는 finished.

        lsfmgr 가 bsub 인자(-q/-J/-g …)를 조립하는 경로.
        pre_submit(commands)->bool: 지정 시 제출 전 게이트 (FR-9).
        """
        self._launch(
            jobset_id, specs, options,
            record_command=lambda spec: spec.command,
            make_task=lambda ctx, key, spec: _SubmitTask(self, ctx, key,
                                                         spec, 0),
            pre_submit=pre_submit)

    def submit_wrappers(self, jobset_id: str, argvs: Sequence[Sequence[str]],
                        options: Options, pre_submit=None) -> None:
        """[async→Signal] wrapper 커맨드 대량 실행 (submit_wrapper 용).

        각 argv 를 그대로 subprocess 실행하고 'Job <id>' 를 파싱한다. lsfmgr 가
        인자를 조립하지 않고 argv 를 다루는 점만 submit_bulk 와 다르다.
        record_command은 shlex 인용 보존 — resubmit 시 split 왕복으로 원본
        argv가 복원돼야 한다 (공백 포함 인자 손상 방지).
        pre_submit(commands)->bool: 지정 시 제출 전 게이트 (FR-9).
        """
        self._launch(
            jobset_id, argvs, options,
            record_command=lambda argv: shlex.join(argv),
            make_task=lambda ctx, key, argv: _WrapperSubmitTask(self, ctx, key,
                                                               argv, 0),
            via_wrapper=True, pre_submit=pre_submit)

    def resubmit_existing(self, jobset_id: str,
                          keyed_items: Sequence, options: Options) -> None:
        """[async→Signal] 기존 레코드 재제출 (resubmit_jobs 용).

        keyed_items: [(job_key, JobSpec | argv토큰리스트), ...] — item 타입으로
        제출 경로를 job 단위로 고른다(JobSpec=bsub 조립, list=wrapper 그대로).
        merge로 wrapper/bsub job이 한 jobset에 섞여 있어도 정확히 동작한다.
        새 CREATED 레코드를 만들지 않고 **이미 존재하는 레코드**를 리셋(이전
        job_id/exit_code/실행시간 초기화 + command 갱신) 후 같은 job_key로
        재submit한다. 결과는 finished(SubmitReport)."""
        if self._shutdown:
            # shutdown 후 queued 경로로 도달할 수 있다 — 새 pool/프로세스를
            # 만들면 아무도 기다려주지 않는 좀비가 된다 (CS-8)
            log.warning("shutdown 후 재제출 요청 무시: %s", jobset_id)
            return
        keyed = list(keyed_items)
        ctx = self._new_context(jobset_id, len(keyed), options)
        # 기존 레코드 리셋 — 이전 실행의 흔적(job_id/exit_code/실행시간/위치)을
        # 지우고 새 command 반영. 지우지 않으면 재제출 실패 시 죽은 옛 job_id·
        # 이전 실행의 start/finish/working_dir가 새 커맨드의 것처럼 잔존한다.
        # 상태는 곧장 SUBMITTING으로 — 재제출은 즉시 제출 착수라, 파이프라인이
        # EXIT(kill) → SUBMITTING(제출 중) → PEND로 자연스럽게 보인다(CREATED
        # 중간 표시 생략). worker가 다시 SUBMITTING으로 두는 건 무해한 재설정.
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
            except Exception:                    # noqa: BLE001 — CS-5
                # 키 소실(remove_job 경합)·store 장애(sqlite lock 등) 어느
                # 쪽이든 이 키만 건너뛰고 나머지는 진행 — 여기서 전파되면
                # ctx가 미완(finished 미발행)으로 고착되어 jobset이 잠긴다
                log.exception("재제출 리셋 실패 — 건너뜀: %s/%s",
                              jobset_id, key)
                self._count(ctx, cancelled=True)
                continue
            if rec is not None:
                reset_recs.append(rec)
            launch.append((key, item))
        # 리셋된 SUBMITTING을 즉시 발행 — 재제출도 완료를 안 기다리고 표에 반영
        if reset_recs:
            self.jobs_changed.emit(jobset_id, reset_recs)
        for key, item in launch:
            ctx.pool.start(self._make_resubmit_task(ctx, key, item))
        if not launch:
            QTimer.singleShot(0, lambda: self._finish_if_done(ctx, force=True))

    @staticmethod
    def _item_command(item) -> str:
        """레코드에 저장할 command 문자열 — wrapper argv는 shlex 인용을 보존해
        재제출 시 shlex.split 왕복이 원본 argv를 복원하게 한다."""
        return item.command if isinstance(item, JobSpec) else shlex.join(item)

    def _make_resubmit_task(self, ctx: _SubmitContext, key: str,
                            item) -> QRunnable:
        if isinstance(item, JobSpec):
            return _SubmitTask(self, ctx, key, item, 0)
        return _WrapperSubmitTask(self, ctx, key, item, 0)

    def is_active(self, jobset_id: str) -> bool:
        """해당 jobset에 아직 끝나지 않은 submit 사이클이 있는지 (resubmit_jobs 가드)."""
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        return ctx is not None and not ctx.finished

    def _new_context(self, jobset_id: str, total: int,
                     options: Options) -> _SubmitContext:
        """submit 사이클 1건의 pool/ctx 구성 + 등록 (submit/resubmit 공통)."""
        pool = QThreadPool()
        pool.setMaxThreadCount(options.workers)
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=total,
            max_retry=options.max_retry, pool=pool,
            limiter=TokenBucketLimiter(options.rate_limit_per_s),
            throttler=self._make_throttler(), options=options)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx
        return ctx

    def _make_throttler(self) -> EmitThrottler:
        """config의 progress throttle 설정으로 EmitThrottler 생성 (QT-5)."""
        return EmitThrottler(self.config.progress_min_interval_s,
                             self.config.progress_min_step_ratio)

    def _launch(self, jobset_id: str, items: Sequence, options: Options,
                record_command: Callable[[object], str],
                make_task: Callable[..., QRunnable],
                via_wrapper: bool = False, pre_submit=None) -> None:
        """단건 submit 공통 골격 — pool/‏ctx 구성 + CREATED 레코드 선생성 + 발화.

        item(JobSpec 또는 argv)마다 make_task(ctx, job_key, item) 로 worker 를
        만들어 병렬 실행한다. 재시도·rate limit·취소는 ctx 를 통해 공유된다.
        via_wrapper는 레코드에 제출 경로를 남긴다 — resubmit_jobs가 job 단위로
        재제출 경로를 복원하는 근거 (merge된 혼합 jobset에서도 정확).
        pre_submit(commands)->bool 지정 시, 실제 제출 전에 게이트 워커 1개로
        검사한다 (통과해야 do_launch 진행, FR-9).
        """
        items = list(items)
        ctx = self._new_context(jobset_id, len(items), options)

        def do_launch():
            # 레코드 선생성 → 요약 불변식(합계==intended). 상태는 곧장
            # SUBMITTING("제출 중"). 배치 API 필수(Sqlite caller 블로킹 방지).
            created = self.store.add_jobs([
                JobRecord(job_id=None, array_index=None, jobset_id=jobset_id,
                          lsf_job_name=f"{jobset_id}_{idx}",
                          state=JobState.SUBMITTING,
                          command=record_command(item), via_wrapper=via_wrapper,
                          spec_json=(spec_to_json(item)
                                     if isinstance(item, JobSpec) else None))
                for idx, item in enumerate(items)])
            if created:                  # 초기 SUBMITTING 즉시 발행 (표 채움)
                self.jobs_changed.emit(jobset_id, list(created))
            for idx, item in enumerate(items):
                ctx.pool.start(make_task(ctx, f"{jobset_id}_{idx}", item))

        def make_failed(msg):            # 게이트 예외 시 전원 SUBMIT_FAILED 레코드
            return [
                JobRecord(job_id=None, array_index=None, jobset_id=jobset_id,
                          lsf_job_name=f"{jobset_id}_{idx}",
                          state=JobState.SUBMIT_FAILED,
                          fail_reason="PRE_SUBMIT_FAILED", fail_message=msg,
                          command=record_command(item), via_wrapper=via_wrapper,
                          spec_json=(spec_to_json(item)
                                     if isinstance(item, JobSpec) else None))
                for idx, item in enumerate(items)]

        if pre_submit is None:
            do_launch()
            if not items:
                # 동기 emit 금지 — caller(manager.submit)가 아직 핸들을 만들기
                # 전이라 finished가 유실된다. 이벤트 루프 한 바퀴 뒤로 지연.
                QTimer.singleShot(0,
                                  lambda: self._finish_if_done(ctx, force=True))
            return

        # 게이트 경로 — 워커 1개에서 pre_submit 검사 후 통과 시 do_launch
        commands = [record_command(item) for item in items]
        ctx.pool.start(_GateTask(self, ctx, commands, pre_submit,
                                 do_launch, make_failed))

    def submit_array(self, jobset_id: str, spec: ArrayJobSpec,
                     options: Options, pre_submit=None) -> None:
        """[async→Signal] array job submit (FR-1.3) — bsub 1회.
        pre_submit(commands)->bool: 지정 시 제출 전 게이트 (FR-9)."""
        n = spec.size
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=1,
            max_retry=options.max_retry,
            pool=QThreadPool(), limiter=TokenBucketLimiter(None),
            throttler=self._make_throttler(), options=options)
        ctx.pool.setMaxThreadCount(1)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx

        def do_launch():
            created = self.store.add_jobs([
                JobRecord(job_id=None, array_index=i, jobset_id=jobset_id,
                          lsf_job_name=f"{jobset_id}[{i}]",
                          state=JobState.SUBMITTING,
                          command=(spec.commands[i - 1] if spec.commands
                                   else (spec.command or "")))
                for i in range(1, n + 1)])
            if created:
                self.jobs_changed.emit(jobset_id, list(created))
            ctx.pool.start(_ArraySubmitTask(self, ctx, spec, 0))

        def make_failed(msg):
            return [
                JobRecord(job_id=None, array_index=i, jobset_id=jobset_id,
                          lsf_job_name=f"{jobset_id}[{i}]",
                          state=JobState.SUBMIT_FAILED,
                          fail_reason="PRE_SUBMIT_FAILED", fail_message=msg,
                          command=(spec.commands[i - 1] if spec.commands
                                   else (spec.command or "")))
                for i in range(1, n + 1)]

        if pre_submit is None:
            do_launch()
            return
        commands = (list(spec.commands) if spec.commands
                    else [spec.command or ""])
        ctx.pool.start(_GateTask(self, ctx, commands, pre_submit,
                                 do_launch, make_failed))

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
        # 잔존(persistent 모드에서는 DB 오염)하지 않고 finished도 발행된다.
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
        # 아직 submit 전이므로 CREATED로 되돌림 (안전 지점 중단, QT-6)
        rec = self.store.get_job(ctx.jobset_id, job_key)
        if rec.state in (JobState.SUBMITTING, JobState.RETRY_WAIT):
            self.store.transition(ctx.jobset_id, job_key, JobState.CREATED)
        self._count(ctx, cancelled=True)

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
            report = SubmitReport(
                jobset_id=ctx.jobset_id, total=ctx.total,
                succeeded=ctx.succeeded, failed=ctx.failed,
                cancelled=ctx.cancelled, retried=len(ctx.retried_keys),
                duration_s=time.monotonic() - ctx.started_at,
                fail_reasons=dict(ctx.fail_reasons))
            # 마지막 전이분 flush → finished 를 같은 lock 안에서 순서대로 발화.
            # 모든 per-job jobs_changed도 ctx.lock 안에서 발화되므로, 락이
            # 직렬화해 finished가 반드시 마지막 per-job jobs_changed 뒤에
            # post된다 — UI가 완료 통지 시점에 전 job 갱신을 이미 받도록 보장.
            if batch:                    # throttle 잔여 마지막 전이분
                self.jobs_changed.emit(ctx.jobset_id, batch)
            self.finished.emit(ctx.jobset_id, report)
        log.info("submit 완료 %s: 성공 %d / 실패 %d / 취소 %d (총 %d)",
                 ctx.jobset_id, report.succeeded, report.failed,
                 report.cancelled, report.total)
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

    def _drop_ctx(self, ctx: _SubmitContext) -> None:
        with self._ctx_lock:
            if self._contexts.get(ctx.jobset_id) is ctx:
                del self._contexts[ctx.jobset_id]

    def _gate_reject(self, ctx: _SubmitContext, finish: bool) -> None:
        """게이트 False/취소 — 제출하지 않음(레코드 미생성 → 요약은 N CREATED
        유지). finish=True일 때만 submit_finished(cancelled=N)를 발화한다
        (False 반환은 config.submit_finished_on_gate_reject, 취소는 항상 True)."""
        with ctx.lock:
            if ctx.finished:
                return
            ctx.finished = True
            ctx.cancelled = ctx.total
            report = self._make_report(ctx)
            if finish:
                self.finished.emit(ctx.jobset_id, report)
        log.info("pre_submit 게이트 거부 %s (finished 발화=%s)",
                 ctx.jobset_id, finish)
        self._drop_ctx(ctx)

    def _gate_fail(self, ctx: _SubmitContext, failed_records: list,
                   msg: str) -> None:
        """게이트 예외 — 전원 SUBMIT_FAILED 레코드 + error + finished(항상)."""
        try:
            created = self.store.add_jobs(failed_records)
            if created:
                self.jobs_changed.emit(ctx.jobset_id, list(created))
        except Exception:                    # noqa: BLE001 — CS-5
            log.exception("게이트 실패 레코드 생성 실패: %s", ctx.jobset_id)
        self.error.emit(ctx.jobset_id, f"pre_submit: {msg}")
        with ctx.lock:
            if ctx.finished:
                return
            ctx.finished = True
            ctx.failed = ctx.total
            ctx.fail_reasons["PRE_SUBMIT_FAILED"] = ctx.total
            report = self._make_report(ctx)
            self.finished.emit(ctx.jobset_id, report)
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
        sub.ready_started.emit(ctx.jobset_id)
        if sub._shutdown or ctx.cancel_event.is_set():
            sub.ready_finished.emit(ctx.jobset_id, False)
            sub._gate_reject(ctx, finish=True)      # 취소 — 항상 finished
            return
        try:
            ok = bool(self.pre_submit(list(self.commands)))
        except Exception as e:                       # noqa: BLE001 — CS-5
            log.exception("pre_submit 게이트 예외: %s", ctx.jobset_id)
            sub.ready_finished.emit(ctx.jobset_id, False)
            sub._gate_fail(ctx, self.make_failed(repr(e)[:4000]), repr(e))
            return
        sub.ready_finished.emit(ctx.jobset_id, ok)
        if not ok:
            sub._gate_reject(
                ctx, finish=sub.config.submit_finished_on_gate_reject)
            return
        if ctx.cancel_event.is_set() or sub._shutdown:
            sub._gate_reject(ctx, finish=True)       # 통과했지만 그새 취소됨
            return
        sub.started.emit(ctx.jobset_id)              # 게이트 통과 → 제출 착수
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


class _ArraySubmitTask(QRunnable):
    """array job bsub 1회 worker (FR-1.3)."""

    def __init__(self, submitter: BulkSubmitter, ctx: _SubmitContext,
                 spec: ArrayJobSpec, attempt: int):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.spec = spec
        self.attempt = attempt

    def run(self):
        try:
            self._run()
        except Exception as e:                  # noqa: BLE001
            log.exception("array submit 예외: %s", self.ctx.jobset_id)
            changed = self._fail_all("INTERNAL_ERROR", repr(e)[:4000])
            self.submitter.error.emit(self.ctx.jobset_id, repr(e))
            self.submitter._count(self.ctx, failed=True,
                                  reason="ARRAY_SUBMIT_FAILED",
                                  changed=changed)

    def _run(self):
        sub, ctx, spec = self.submitter, self.ctx, self.spec
        jsid = ctx.jobset_id
        n = spec.size
        keys = [f"{jsid}[{i}]" for i in range(1, n + 1)]
        for key in keys:
            sub.store.transition(jsid, key, JobState.SUBMITTING)

        command = spec.command
        if spec.commands is not None:
            command = self._write_dispatch_script(jsid, spec.commands)

        js = sub.store.get_jobset(jsid)
        group = js.lsf_group_paths[0] if js.lsf_group_paths else None
        try:
            opts = ctx.options
            array_id = sub.command.bsub(
                command,
                queue=(spec.queue if spec.queue is not None
                       else (opts.queue or None)),
                job_name=f"{jsid}[1-{n}]",            # array 지정
                group_path=group,
                resources=spec.resources or opts.resource_req,
                outfile=spec.outfile, errfile=spec.errfile,
                extra_args=spec.extra_args,
                env=spec.env,
                timeout_s=opts.submit_timeout_s)
        except SubmitError as e:
            if (self.attempt < ctx.max_retry and not ctx.cancel_event.is_set()
                    and not sub._shutdown):
                msg = e.diagnostic()[:4000]
                for key in keys:
                    sub.store.transition(jsid, key, JobState.RETRY_WAIT,
                                         retry_count=self.attempt + 1,
                                         fail_reason=e.fail_reason,
                                         fail_message=msg)
                nxt = self.attempt + 1      # 값 즉시 캡처 (task는 autoDelete)
                sub._schedule_retry(
                    ctx, keys, ctx.options.retry_delay_s(self.attempt),
                    lambda: _ArraySubmitTask(sub, ctx, spec, nxt))
                return
            changed = self._fail_all(e.fail_reason, e.diagnostic()[:4000])
            sub._count(ctx, failed=True, reason="ARRAY_SUBMIT_FAILED",
                       changed=changed)
            return

        sub.jobsets.add_array_attachment(jsid, array_id)
        now = datetime.now()
        recs = []
        for key in keys:
            r = sub.store.transition(jsid, key, JobState.PEND,
                                     job_id=array_id, submit_time=now,
                                     fail_reason=None, fail_message=None)
            if r is not None:
                recs.append(r)
        sub._count(ctx, succeeded=True, changed=recs)

    def _write_dispatch_script(self, jsid: str, commands) -> str:
        """element별 command가 다른 경우 $LSB_JOBINDEX dispatch 스크립트 생성."""
        script_dir = self.submitter.config.resolve_script_dir()
        cmds_path = os.path.join(script_dir, f"{jsid}.cmds")
        sh_path = os.path.join(script_dir, f"{jsid}.sh")
        with open(cmds_path, "w", encoding="utf-8") as f:
            f.write("\n".join(commands) + "\n")
        with open(sh_path, "w", encoding="utf-8") as f:
            f.write("#!/bin/sh\n"
                    "# lsfmgr 자동 생성 — $LSB_JOBINDEX 번째 명령을 실행\n"
                    f'CMD=$(sed -n "${{LSB_JOBINDEX}}p" "{cmds_path}")\n'
                    'exec /bin/sh -c "$CMD"\n')
        os.chmod(sh_path, os.stat(sh_path).st_mode
                 | stat_mod.S_IXUSR | stat_mod.S_IXGRP)
        return sh_path

    def _fail_all(self, reason: str, message: Optional[str] = None) -> list:
        """전 element를 SUBMIT_FAILED로 확정. 반환: 전이된 레코드 목록 —
        _count(changed=...)로 넘겨 jobs_updated/jobs_failed 발행을 보장한다
        (누락 시 UI 표가 SUBMITTING에 고착)."""
        jsid = self.ctx.jobset_id
        changed = []
        for i in range(1, self.spec.size + 1):
            try:
                rec = self.submitter.store.transition(
                    jsid, f"{jsid}[{i}]", JobState.SUBMIT_FAILED,
                    retry_count=self.attempt, fail_reason=reason,
                    fail_message=message)
                if rec is not None:
                    changed.append(rec)
            except Exception:                   # noqa: BLE001
                pass
        return changed
