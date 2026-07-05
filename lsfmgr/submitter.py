"""BulkSubmitter — QThreadPool 기반 대량 submit (FR-1, FR-2).

- 병렬/순차 submit, rate limit(token bucket), progress throttle (QT-5)
- 실패 시 QTimer 스케줄 retry (RETRY_WAIT → SUBMITTING, sleep 점유 없음)
- cancel: job 단위 경계에서 안전 중단, 이미 submit된 job은 정상 기록 (QT-6)
- worker 예외 격리 → error Signal (CS-5)
"""
from __future__ import annotations

import logging
import os
import stat as stat_mod
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Sequence

from .command import LsfCommand
from .config import ArrayJobSpec, JobSpec, LsfConfig
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
    finished: bool = False


class _SubmitTask(QRunnable):
    """bsub 1회 수행 worker. 예외는 submitter로 격리 전달 (CS-5)."""

    def __init__(self, submitter: "BulkSubmitter", ctx: _SubmitContext,
                 job_key: str, spec: JobSpec, attempt: int):
        super().__init__()
        self.setAutoDelete(True)
        self.submitter = submitter
        self.ctx = ctx
        self.job_key = job_key
        self.spec = spec
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
        sub.store.transition(ctx.jobset_id, self.job_key,
                             JobState.SUBMITTING)
        if not ctx.limiter.acquire(ctx.cancel_event):   # rate limit (NFR-4)
            sub._task_cancelled(ctx, self.job_key)
            return

        js = sub.store.get_jobset(ctx.jobset_id)
        group = js.lsf_group_paths[0] if js.lsf_group_paths else None
        opts = ctx.options
        # 우선순위: JobSpec 필드 > call 옵션 (§1.2)
        outfile, errfile = self.spec.outfile, self.spec.errfile
        if opts.output_dir and not outfile:
            outfile = os.path.join(opts.output_dir, f"{self.job_key}.out")
        if opts.output_dir and not errfile:
            errfile = os.path.join(opts.output_dir, f"{self.job_key}.err")
        try:
            job_id = sub.command.bsub(
                self.spec.command,
                queue=(self.spec.queue if self.spec.queue is not None
                       else (opts.queue or None)),
                job_name=self.job_key,           # -J <jobset_id>_<idx> (FR-1.4)
                group_path=group,                # -g /lsfmgr/<user>/<jsid>
                resources=self.spec.resources or opts.resource_req,
                outfile=outfile,
                errfile=errfile,
                extra_args=self.spec.extra_args,
                env=self.spec.env,
                timeout_s=opts.submit_timeout_s)
        except SubmitError as e:
            sub._task_failed(ctx, self.job_key, self.spec, self.attempt, e)
            return
        sub._task_succeeded(ctx, self.job_key, job_id)


class BulkSubmitter(QObject):
    """대량 submit 진입점. manager(Facade)가 소유."""

    progress = Signal(str, int, int)          # jobset_id, done, total
    finished = Signal(str, object)            # jobset_id, SubmitReport
    error = Signal(str, str)                  # jobset_id, message
    # 내부용 — worker 스레드에서 emit → submitter 소속 스레드에서 QTimer 스케줄
    _retry_requested = Signal(str, str, object, int, float)

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
                    options: Options) -> None:
        """[async→Signal] 대량 submit. CREATED 레코드 생성 후 즉시 반환,
        실제 bsub는 QThreadPool worker에서 수행 (QT-1). 결과는 finished."""
        pool = QThreadPool()
        pool.setMaxThreadCount(options.workers)
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=len(specs),
            max_retry=options.max_retry,
            pool=pool,
            limiter=TokenBucketLimiter(options.rate_limit_per_s),
            options=options)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx

        # CREATED 레코드 선생성 → 요약 불변식(합계==intended) 즉시 성립.
        # 배치 API 필수 — 건당 insert는 Sqlite에서 caller 스레드 블로킹.
        self.store.add_jobs([
            JobRecord(job_id=None, array_index=None, jobset_id=jobset_id,
                      lsf_job_name=f"{jobset_id}_{idx}",
                      state=JobState.CREATED, command=spec.command)
            for idx, spec in enumerate(specs)])
        for idx, spec in enumerate(specs):
            pool.start(_SubmitTask(self, ctx, f"{jobset_id}_{idx}", spec, 0))
        if not specs:
            # 동기 emit 금지 — caller(manager.submit)가 아직 핸들을 만들기
            # 전이라 finished가 유실된다. 이벤트 루프 한 바퀴 뒤로 지연.
            QTimer.singleShot(0, lambda: self._finish_if_done(ctx, force=True))

    def submit_array(self, jobset_id: str, spec: ArrayJobSpec,
                     options: Options) -> None:
        """[async→Signal] array job submit (FR-1.3) — bsub 1회."""
        n = spec.size
        self.store.add_jobs([
            JobRecord(job_id=None, array_index=i, jobset_id=jobset_id,
                      lsf_job_name=f"{jobset_id}[{i}]",
                      state=JobState.CREATED,
                      command=(spec.commands[i - 1] if spec.commands
                               else (spec.command or "")))
            for i in range(1, n + 1)])
        ctx = _SubmitContext(
            jobset_id=jobset_id, total=1,
            max_retry=options.max_retry,
            pool=QThreadPool(), limiter=TokenBucketLimiter(None),
            options=options)
        ctx.pool.setMaxThreadCount(1)
        with self._ctx_lock:
            self._contexts[jobset_id] = ctx
        ctx.pool.start(_ArraySubmitTask(self, ctx, spec, 0))

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
        # pending retry(QTimer/큐잉된 Signal)는 pool 밖에 있어 waitForDone이
        # 기다려주지 않고, 이벤트 루프가 곧 끝나면 영영 실행되지 않는다 —
        # RETRY_WAIT 잔류분을 여기서 SUBMIT_FAILED로 확정하지 않으면
        # 비terminal로 영구 잔존(persistent 모드에서는 DB 오염)하고
        # finished Signal도 발행되지 않는다. waitForDone 이후에는 새
        # RETRY_WAIT 전이가 없으므로 안전.
        for ctx in contexts:
            with ctx.lock:
                if ctx.finished:
                    continue
            try:
                pending = self.store.get_jobs(ctx.jobset_id,
                                              states={JobState.RETRY_WAIT})
            except Exception:                   # noqa: BLE001 — 종료 경로 보호
                continue
            for rec in pending:
                self.store.transition(ctx.jobset_id, rec.job_key,
                                      JobState.SUBMIT_FAILED,
                                      fail_reason=rec.fail_reason
                                      or "SHUTDOWN")
                with ctx.lock:
                    ctx.done += 1
                    ctx.failed += 1
            self._finish_if_done(ctx)

    # ------------------------------------------------------------------
    # worker 콜백 (worker 스레드에서 호출됨 — Store는 thread-safe)
    # ------------------------------------------------------------------
    def _task_succeeded(self, ctx: _SubmitContext, job_key: str,
                        job_id: int) -> None:
        self.store.transition(ctx.jobset_id, job_key, JobState.PEND,
                              job_id=job_id, submit_time=datetime.now(),
                              fail_reason=None)
        with ctx.lock:
            ctx.done += 1
            ctx.succeeded += 1
        self._emit_progress(ctx)
        self._finish_if_done(ctx)

    def _task_failed(self, ctx: _SubmitContext, job_key: str, spec: JobSpec,
                     attempt: int, err: SubmitError) -> None:
        log.warning("submit 실패 [%s] %s: %s", err.fail_reason, job_key, err)
        if (attempt < ctx.max_retry and not ctx.cancel_event.is_set()
                and not self._shutdown):
            # RETRY_WAIT → QTimer 스케줄 (스레드 sleep 점유 금지, §3.2)
            self.store.transition(ctx.jobset_id, job_key, JobState.RETRY_WAIT,
                                  retry_count=attempt + 1,
                                  fail_reason=err.fail_reason)
            with ctx.lock:
                ctx.retried_keys.add(job_key)
            delay = ctx.options.retry_delay_s(attempt)
            self._retry_requested.emit(ctx.jobset_id, job_key, spec,
                                       attempt + 1, delay)
            return
        log.error("SUBMIT_FAILED 확정 [%s] %s (%d회 시도)",
                  err.fail_reason, job_key, attempt + 1)      # NFR-6 ERROR
        self.store.transition(ctx.jobset_id, job_key, JobState.SUBMIT_FAILED,
                              retry_count=attempt,
                              fail_reason=err.fail_reason)
        with ctx.lock:
            ctx.done += 1
            ctx.failed += 1
            ctx.fail_reasons[err.fail_reason] = (
                ctx.fail_reasons.get(err.fail_reason, 0) + 1)
        self._emit_progress(ctx)
        self._finish_if_done(ctx)

    def _task_cancelled(self, ctx: _SubmitContext, job_key: str) -> None:
        # 아직 submit 전이므로 CREATED로 되돌림 (안전 지점 중단, QT-6)
        rec = self.store.get_job(ctx.jobset_id, job_key)
        if rec.state in (JobState.SUBMITTING, JobState.RETRY_WAIT):
            self.store.transition(ctx.jobset_id, job_key, JobState.CREATED)
        with ctx.lock:
            ctx.done += 1
            ctx.cancelled += 1
        self._finish_if_done(ctx)

    def _task_crashed(self, ctx: _SubmitContext, job_key: str,
                      err: Exception) -> None:
        """분류 불가 예외 — SUBMIT_FAILED 처리 + error Signal (CS-5)."""
        try:
            self.store.transition(ctx.jobset_id, job_key,
                                  JobState.SUBMIT_FAILED,
                                  fail_reason="INTERNAL_ERROR")
        except Exception:                       # noqa: BLE001
            log.exception("crash 후 전이 실패: %s", job_key)
        with ctx.lock:
            ctx.done += 1
            ctx.failed += 1
            ctx.fail_reasons["INTERNAL_ERROR"] = (
                ctx.fail_reasons.get("INTERNAL_ERROR", 0) + 1)
        self.error.emit(ctx.jobset_id, f"{job_key}: {err!r}")
        self._finish_if_done(ctx)

    # ------------------------------------------------------------------
    # retry 스케줄 (submitter 소속 스레드에서 실행 — queued connection)
    # ------------------------------------------------------------------
    @Slot(str, str, object, int, float)
    def _on_retry_requested(self, jobset_id: str, job_key: str, spec,
                            attempt: int, delay_s: float) -> None:
        with self._ctx_lock:
            ctx = self._contexts.get(jobset_id)
        if ctx is None:
            return
        if self._shutdown:
            return          # shutdown()이 RETRY_WAIT 잔류를 일괄 확정함
        if ctx.cancel_event.is_set():
            self._abandon_retry(ctx, job_key)
            return

        def fire():
            if self._shutdown:
                return      # 닫힌 store 접근 금지 — shutdown()이 확정함
            if ctx.cancel_event.is_set():
                self._abandon_retry(ctx, job_key)
                return
            if isinstance(spec, _ArrayRetrySpec):
                ctx.pool.start(_ArraySubmitTask(self, ctx, spec.spec, attempt))
            else:
                ctx.pool.start(_SubmitTask(self, ctx, job_key, spec, attempt))

        QTimer.singleShot(int(delay_s * 1000), fire)

    def _abandon_retry(self, ctx: _SubmitContext, job_key: str) -> None:
        """cancel/shutdown으로 재시도 포기 — 최종 SUBMIT_FAILED 확정."""
        if job_key == "__array__":
            keys = [r.job_key for r in self.store.get_jobs(ctx.jobset_id)
                    if r.state is JobState.RETRY_WAIT]
        else:
            keys = [job_key]
        for key in keys:
            rec = self.store.get_job(ctx.jobset_id, key)
            if rec.state is JobState.RETRY_WAIT:
                self.store.transition(ctx.jobset_id, key,
                                      JobState.SUBMIT_FAILED,
                                      fail_reason=rec.fail_reason or "CANCELLED")
        with ctx.lock:
            ctx.done += 1
            ctx.failed += 1
        self._finish_if_done(ctx)

    # ------------------------------------------------------------------
    # 진행/완료 통지
    # ------------------------------------------------------------------
    def _emit_progress(self, ctx: _SubmitContext) -> None:
        with ctx.lock:
            done, total = ctx.done, ctx.total
        if ctx.throttler.should_emit(done, total):      # QT-5 throttle
            self.progress.emit(ctx.jobset_id, done, total)

    def _finish_if_done(self, ctx: _SubmitContext, force: bool = False) -> None:
        with ctx.lock:
            if ctx.finished or (ctx.done < ctx.total and not force):
                return
            ctx.finished = True
            report = SubmitReport(
                jobset_id=ctx.jobset_id, total=ctx.total,
                succeeded=ctx.succeeded, failed=ctx.failed,
                cancelled=ctx.cancelled, retried=len(ctx.retried_keys),
                duration_s=time.monotonic() - ctx.started_at,
                fail_reasons=dict(ctx.fail_reasons))
        log.info("submit 완료 %s: 성공 %d / 실패 %d / 취소 %d (총 %d)",
                 ctx.jobset_id, report.succeeded, report.failed,
                 report.cancelled, report.total)
        self.finished.emit(ctx.jobset_id, report)


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
            self._fail_all("INTERNAL_ERROR")
            self.submitter.error.emit(self.ctx.jobset_id, repr(e))
            self._count_done(failed=True)

    def _run(self):
        sub, ctx, spec = self.submitter, self.ctx, self.spec
        jsid = ctx.jobset_id
        n = spec.size
        for i in range(1, n + 1):
            sub.store.transition(jsid, f"{jsid}[{i}]", JobState.SUBMITTING)

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
            if self.attempt < ctx.max_retry and not ctx.cancel_event.is_set():
                for i in range(1, n + 1):
                    sub.store.transition(jsid, f"{jsid}[{i}]",
                                         JobState.RETRY_WAIT,
                                         retry_count=self.attempt + 1,
                                         fail_reason=e.fail_reason)
                delay = ctx.options.retry_delay_s(self.attempt)
                sub._retry_requested.emit(
                    jsid, "__array__", _ArrayRetrySpec(spec),
                    self.attempt + 1, delay)
                return
            self._fail_all(e.fail_reason)
            self._count_done(failed=True)
            return

        sub.jobsets.add_array_attachment(jsid, array_id)
        now = datetime.now()
        for i in range(1, n + 1):
            sub.store.transition(jsid, f"{jsid}[{i}]", JobState.PEND,
                                 job_id=array_id, submit_time=now,
                                 fail_reason=None)
        self._count_done(failed=False)

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

    def _fail_all(self, reason: str) -> None:
        jsid = self.ctx.jobset_id
        for i in range(1, self.spec.size + 1):
            try:
                self.submitter.store.transition(
                    jsid, f"{jsid}[{i}]", JobState.SUBMIT_FAILED,
                    retry_count=self.attempt, fail_reason=reason)
            except Exception:                   # noqa: BLE001
                pass

    def _count_done(self, failed: bool) -> None:
        with self.ctx.lock:
            self.ctx.done += 1
            if failed:
                self.ctx.failed += 1
                self.ctx.fail_reasons["ARRAY_SUBMIT_FAILED"] = (
                    self.ctx.fail_reasons.get("ARRAY_SUBMIT_FAILED", 0) + 1)
            else:
                self.ctx.succeeded += 1
        self.submitter._emit_progress(self.ctx)
        self.submitter._finish_if_done(self.ctx)


class _ArrayRetrySpec:
    """retry Signal 페이로드로 ArrayJobSpec을 감싸는 마커."""

    def __init__(self, spec: ArrayJobSpec):
        self.spec = spec
