"""LsfJobManager — 앱이 사용하는 단일 진입점 (QObject Facade + 핸들 발급).

- High-level: submit() → JobSet 핸들 (v7 §1.1~1.3), AUTO-1~4 자동화
- Low-level: 전역 Facade Signal (v6 §1.4 유지) — 핸들 Signal은 그 위의
  편의 계층으로 동일 이벤트를 이중 발행한다.
- 옵션은 defaults → manager kwargs → call kwargs 3단 계층 (§1.2, options.py)

QT-0 표기 규약: [async→Signal] = 즉시 반환·결과는 Signal /
[sync, snapshot] = 동기지만 Store 스냅샷만 조회 (LSF 호출 없음).
"""
from __future__ import annotations

import logging
import os
from dataclasses import replace as dc_replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from .command import LsfCommand, Runner
from .config import ArrayJobSpec, JobSpec, LsfConfig
from .errors import JobSetClosedError
from .handle import JobSet
from .jobset_core import JobSetManager, detect_array_template
from .killer import Killer
from .monitor import JobsetQuerier, PollingService
from .options import (
    MANAGER_ONLY_KEYS,
    Options,
    SHARED_KEYS,
    resolve_options,
    validate_options,
)
from .qt import QCoreApplication, QObject, QRunnable, QThreadPool, Signal
from .reports import ReconcileReport
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore
from .store.memory import InMemoryStore

log = logging.getLogger("lsfmgr.manager")

#: LsfConfig 필드로 직접 전달되는 manager 전용 키
_CONFIG_KEYS = ("bsub_path", "bjobs_path", "bkill_path", "bhist_path",
                "bmod_path", "bgdel_path", "script_dir", "lsf_group_root",
                "arg_max", "default_queue", "chunk_size")


class LsfJobManager(QObject):
    """Facade — 컴포넌트 조립 + Facade Signal + JobSet 핸들 발급."""

    # --- Low-level Facade Signal (v6 유지, 모두 jobset_id 포함) ---
    submit_started = Signal(str)               # jobset_id
    submit_progress = Signal(str, int, int)    # jobset_id, done, total
    submit_finished = Signal(str, object)      # jobset_id, SubmitReport
    jobset_updated = Signal(str, dict)         # jobset_id, summary
    jobs_updated = Signal(str, list)           # jobset_id, [JobRecord] 변경분
    job_lost = Signal(str, object)             # jobset_id, JobRecord
    kill_finished = Signal(str, object)        # jobset_id, KillReport
    error_occurred = Signal(str, str)          # jobset_id, message

    def __init__(self, store: Optional[JobSetStore] = None,
                 config: Optional[LsfConfig] = None,
                 runner: Optional[Runner] = None,
                 parent: Optional[QObject] = None,
                 **kwargs: Any):
        """kwargs = §1.2 옵션 카탈로그의 ②(manager) 계층.
        config와 동시 지정 시 kwargs 우선 (OPT-4)."""
        super().__init__(parent)

        # --- 옵션 분리: manager 전용 / 공통(②) — 오타는 TypeError (OPT-2) ---
        mgr_only = {k: kwargs.pop(k) for k in list(kwargs)
                    if k in MANAGER_ONLY_KEYS}
        mgr_only = validate_options(mgr_only, allowed=MANAGER_ONLY_KEYS,
                                    where="LsfJobManager()")   # 범위 검증 (OPT-3)
        shared = validate_options(kwargs, allowed=SHARED_KEYS,
                                  where="LsfJobManager()")

        # --- LsfConfig 구성 (기존 config 주입도 계속 지원, OPT-4) ---
        base_cfg = config or LsfConfig()
        cfg_updates = {k: mgr_only[k] for k in _CONFIG_KEYS if k in mgr_only}
        if "submit_timeout_s" in shared:
            cfg_updates["submit_timeout_s"] = shared["submit_timeout_s"]
        self.config = (dc_replace(base_cfg, **cfg_updates)
                       if cfg_updates else base_cfg)

        # --- Store 선택: store 객체 > persistent=True > InMemory(기본) ---
        if store is not None:
            self.store = store
        elif mgr_only.get("persistent"):
            from .store.sqlite import SqliteStore
            db_path = mgr_only.get(
                "db_path", os.path.join(os.path.expanduser("~"),
                                        ".lsfmgr", "jobsets.db"))
            self.store = SqliteStore(db_path)
        else:
            self.store = InMemoryStore()

        # --- ①내장+config 기본값 위에 ②manager kwargs를 merge ---
        cfg = self.config
        self._defaults: Dict[str, Any] = {
            "workers": cfg.workers,
            "max_retry": cfg.max_retry,
            "retry_backoff": (f"fixed:{cfg.retry_delay_s:g}"
                              if cfg.retry_backoff <= 1.0
                              else f"expo:{cfg.retry_delay_s:g}"),
            "rate_limit_per_s": cfg.rate_limit_per_s,
            "poll_interval_s": cfg.poll_interval_s,
            "queue": cfg.default_queue,
            "submit_timeout_s": cfg.submit_timeout_s,
            "chunk_size": cfg.chunk_size,
        }
        self._defaults.update(shared)
        if cfg.retry_backoff > 1.0 and cfg.retry_backoff != 2.0:
            log.warning("LsfConfig.retry_backoff=%s — v7 옵션 체계의 expo "
                        "지수 밑은 2로 고정되어 배수가 그대로 반영되지 "
                        "않습니다", cfg.retry_backoff)

        # --- 컴포넌트 조립 ---
        self.command = LsfCommand(self.config, runner)
        self.jobsets = JobSetManager(self.store, self.command, self.config)
        self.querier = JobsetQuerier(self.store, self.command)

        from .submitter import BulkSubmitter
        self.submitter = BulkSubmitter(self.store, self.command,
                                       self.jobsets, self.config, parent=self)
        self.submitter.progress.connect(self.submit_progress)
        self.submitter.finished.connect(self.submit_finished)
        self.submitter.error.connect(self.error_occurred)

        self.polling = PollingService(self.querier, parent=self)
        self.polling.updated.connect(self._on_poll_updated)
        self.polling.lost.connect(self.job_lost)
        self.polling.error.connect(self.error_occurred)

        self.killer = Killer(self.store, self.command, self.querier,
                             parent=self)
        self.killer.finished.connect(self.kill_finished)
        self.killer.error.connect(self.error_occurred)

        self._misc_pool = QThreadPool(self)     # reconcile 등 단발 작업
        self._misc_pool.setMaxThreadCount(2)
        self._shutdown_done = False

        # --- JobSet 핸들 계층 (v7) — Facade Signal 위에 이중 발행 ---
        self._handles: Dict[str, JobSet] = {}
        self.submit_progress.connect(self._h_progress)
        self.submit_finished.connect(self._h_finished)
        self.jobset_updated.connect(self._h_updated)
        self.jobs_updated.connect(self._h_jobs_updated)
        self.kill_finished.connect(self._h_kill_finished)
        self.error_occurred.connect(self._h_error)

        # AUTO-3: 앱 종료 시 shutdown 자동 연결 (명시 호출과 중복 안전)
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self.shutdown)

    # ------------------------------------------------------------------
    # 옵션 해석 (OPT-1)
    # ------------------------------------------------------------------
    def resolve_options(self, call_kwargs: Dict[str, Any],
                        context: str = "submit") -> Options:
        """③call kwargs를 defaults(①+②) 위에 merge — 단일 해석 지점."""
        return resolve_options(self._defaults, call_kwargs, context=context)

    # ------------------------------------------------------------------
    # High-level submit (v7 §1.1) — JobSet 핸들 반환
    # ------------------------------------------------------------------
    def submit(self, jobs: Union[str, Sequence[Union[str, JobSpec]]],
               count: Optional[int] = None, **kwargs: Any) -> JobSet:
        """[async→Signal] 통합 submit 진입점 — JobSet 핸들 반환.

        - list[str] 또는 list[JobSpec] 허용 (str은 JobSpec 자동 변환)
        - 단일 str + count=N: 동일 command N개 ($LSB_JOBINDEX 활용 array)
        - AUTO-4: mode="auto"(기본)면 동일 command 패턴/$LSB_JOBINDEX 치환
          가능 시 array, 아니면 bulk parallel. mode="array"|"bulk"로 강제 가능
        - AUTO-1: 반환 직전 polling 자동 시작 (auto_poll=False로 해제)
        - 결과는 핸들의 progress/finished/updated/failed Signal로 도착
        """
        opts = self.resolve_options(kwargs, context="submit")

        # 단일 str + count → array 표현 (README §4.2)
        if isinstance(jobs, str):
            if count is None:
                jobs = [jobs]                     # 단일 job으로 취급
            elif count < 1:
                raise ValueError(f"count는 1 이상 (got {count})")
            elif opts.mode == "bulk":             # 강제 bulk → 동일 command N개
                jobs = [jobs] * count
            else:
                spec = ArrayJobSpec(command=jobs, count=count)
                jsid = self._submit_array_impl(spec, opts)
                return self._post_submit(jsid, opts)
        elif count is not None:
            raise ValueError("count는 단일 command 문자열과만 사용 가능")

        specs, plain = _normalize_jobs(jobs)
        commands = [s.command for s in specs]

        mode = opts.mode
        template: Optional[str] = None
        if mode == "auto":                        # AUTO-4
            template = detect_array_template(commands) if plain else None
            mode = "array" if template else "bulk"

        if mode == "array" and len(specs) >= 2:
            if template is None:
                template = detect_array_template(commands)
            common = _common_spec_options(specs)  # 상이 옵션이면 ValueError
            if template is not None:
                spec = ArrayJobSpec(command=template, count=len(specs),
                                    **common)
            else:                                 # 상이 command → dispatch
                spec = ArrayJobSpec(commands=tuple(commands), **common)
            jsid = self._submit_array_impl(spec, opts)
        else:
            jsid = self._submit_bulk_impl(specs, opts)

        return self._post_submit(jsid, opts)

    def _post_submit(self, jsid: str, opts: Options) -> JobSet:
        """핸들 발급 + AUTO-1 (submit 반환 직전 polling 자동 시작)."""
        handle = self.jobset(jsid)
        if opts.auto_poll:                        # AUTO-1
            self.start_polling(jsid, opts.poll_interval_s)
        return handle

    def jobset(self, jobset_id: str) -> JobSet:
        """[sync, snapshot] JobSet 핸들 재획득 (복원/검색 결과에서)."""
        handle = self._handles.get(jobset_id)
        if handle is not None:
            return handle
        self.store.get_jobset(jobset_id)          # 존재 검증
        handle = JobSet(self, jobset_id)
        self._handles[jobset_id] = handle
        return handle

    # ------------------------------------------------------------------
    # Low-level submit (v6 유지)
    # ------------------------------------------------------------------
    def submit_bulk(self, jobs: Sequence[JobSpec], *, parallel: bool = True,
                    workers: Optional[int] = None,
                    max_retry: Optional[int] = None,
                    rate_limit_per_s: Optional[float] = None,
                    label: str = "", tags: Sequence[str] = (),
                    description: str = "",
                    jobset_id: Optional[str] = None) -> str:
        """[async→Signal] 대량 submit (Low-level) — jobset_id 반환.
        결과는 Facade Signal. polling은 자동 시작하지 않는다 (v6 계약)."""
        # tags는 원형 그대로 — tuple(str)은 문자 단위로 분해되므로
        # 정규화는 options._validate에 일임
        kw: Dict[str, Any] = {"label": label, "tags": tags,
                              "description": description}
        if not parallel:
            kw["workers"] = 1
        elif workers is not None:
            kw["workers"] = workers
        if max_retry is not None:
            kw["max_retry"] = max_retry
        if rate_limit_per_s is not None:
            kw["rate_limit_per_s"] = rate_limit_per_s
        opts = self.resolve_options(kw, context="submit")
        specs, _ = _normalize_jobs(jobs)
        return self._submit_bulk_impl(specs, opts, jobset_id=jobset_id)

    def submit_array(self, spec: ArrayJobSpec, *,
                     max_retry: Optional[int] = None, label: str = "",
                     tags: Sequence[str] = (),
                     jobset_id: Optional[str] = None) -> str:
        """[async→Signal] array job submit (Low-level) — jobset_id 반환."""
        kw: Dict[str, Any] = {"label": label, "tags": tags}
        if max_retry is not None:
            kw["max_retry"] = max_retry
        opts = self.resolve_options(kw, context="submit")
        return self._submit_array_impl(spec, opts, jobset_id=jobset_id)

    def cancel_submit(self, jobset_id: str) -> None:
        """[async→Signal] 진행 중 submit 중단 — submit된 job은 유지 (QT-6)."""
        self.submitter.cancel_submit(jobset_id)

    # --- 내부 submit 구현 (High/Low 공유) ---
    def _submit_bulk_impl(self, specs: List[JobSpec], opts: Options,
                          jobset_id: Optional[str] = None) -> str:
        js = self.jobsets.create_jobset(
            len(specs), label=opts.label, tags=opts.tags,
            description=opts.description, jobset_id=jobset_id)
        self.submit_started.emit(js.jobset_id)
        self.submitter.submit_bulk(js.jobset_id, specs, opts)
        return js.jobset_id

    def _submit_array_impl(self, spec: ArrayJobSpec, opts: Options,
                           jobset_id: Optional[str] = None) -> str:
        js = self.jobsets.create_jobset(
            spec.size, label=opts.label, tags=opts.tags,
            description=opts.description, jobset_id=jobset_id)
        # array의 LSF job name은 "<jsid>[idx]" 형태 → 패턴도 그에 맞춤
        self.store.update_jobset(
            _with_pattern(self.store.get_jobset(js.jobset_id),
                          f"{js.jobset_id}[*]"))
        self.submit_started.emit(js.jobset_id)
        self.submitter.submit_array(js.jobset_id, spec, opts)
        return js.jobset_id

    # ------------------------------------------------------------------
    # 모니터링 (FR-4)
    # ------------------------------------------------------------------
    def start_polling(self, jobset_id: str,
                      interval_s: Optional[float] = None) -> None:
        """[async→Signal] 주기 polling 시작 — 갱신은 jobset_updated."""
        self.polling.start_polling(
            jobset_id, interval_s if interval_s is not None
            else self._defaults["poll_interval_s"])

    def stop_polling(self, jobset_id: str) -> None:
        """[async→Signal] polling 중지."""
        self.polling.stop_polling(jobset_id)

    def query_once(self, jobset_id: str) -> None:
        """[async→Signal] 1회 갱신 — 결과는 jobset_updated/jobs_updated."""
        self.polling.poll_now(jobset_id)

    def summary(self, jobset_id: str) -> Dict[str, Any]:
        """[sync, snapshot] Store의 현재 요약 (LSF 호출 없음)."""
        return self.store.summary(jobset_id)

    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        """[sync, snapshot] job 상세 (Store 조회)."""
        return self.store.get_jobs(jobset_id, states)

    # ------------------------------------------------------------------
    # Kill (FR-3)
    # ------------------------------------------------------------------
    def kill_jobset(self, jobset_id: str, *,
                    only_state: Optional[JobState] = None,
                    verify: Optional[bool] = None) -> None:
        """[async→Signal] JobSet kill — 결과는 kill_finished.
        verify 미지정 시 verify_kill 옵션(②) 적용."""
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        self.killer.kill_jobset(jobset_id, only_state=only_state,
                                verify=verify)

    def kill_jobs(self, job_ids: Sequence[int], *,
                  verify: Optional[bool] = None) -> None:
        """[async→Signal] 개별 ID kill (chunking 자동)."""
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        self.killer.kill_jobs(job_ids, verify=verify)

    # ------------------------------------------------------------------
    # JobSet 관리 (FR-5)
    # ------------------------------------------------------------------
    def create_jobset(self, intended_count: int, *, label: str = "",
                      tags: Sequence[str] = (),
                      parent: Optional[str] = None) -> str:
        """[sync] 수동 JobSet 생성 — jobset_id 반환."""
        return self.jobsets.create_jobset(
            intended_count, label=label, tags=tags, parent=parent).jobset_id

    def add_job(self, jobset_id: str, record: JobRecord, *,
                sync_lsf: bool = True) -> JobRecord:
        """[sync] job 편입 (sync_lsf=True면 bmod -g — LSF 호출 포함)."""
        return self.jobsets.add_job(jobset_id, record, sync_lsf=sync_lsf)

    def merge_jobsets(self, jobset_ids: Sequence[str], *,
                      sync_lsf: bool = False,
                      keep_originals: bool = False) -> str:
        """[sync] 병합 — 새 jobset_id 반환. 원본 미보존 시 해당 핸들 파괴."""
        new_id = self.jobsets.merge_jobsets(
            jobset_ids, sync_lsf=sync_lsf,
            keep_originals=keep_originals).jobset_id
        if not keep_originals:
            for old in jobset_ids:
                # polling 중지 필수 — 삭제된 jobset을 계속 polling하면
                # 매 주기 JobSetNotFoundError → error Signal 폭주
                self.polling.stop_polling(old)
                self._invalidate_handle(old)
        return new_id

    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """[sync, LSF 조회 포함] 손실 감지/복구 (FR-5.3) — blocking 주의."""
        return self.jobsets.detect_lost(jobset_id)

    def search_jobsets(self, *, tag: Optional[str] = None,
                       label: Optional[str] = None,
                       since: Optional[datetime] = None) -> List[JobSetRecord]:
        """[sync, snapshot] 세션 범위 검색."""
        return self.store.search(tag=tag, label=label, since=since)

    def close_jobset(self, jobset_id: str, *, force: bool = False) -> None:
        """[sync] 종결 (전원 terminal일 때) — 핸들도 파괴.
        전원 terminal이 아니면 예외 — polling/핸들은 건드리지 않고 유지.
        LSF group 정리(bgdel)는 worker 스레드에서 비동기 수행 (QT-1)."""
        js = self.jobsets.close_jobset(jobset_id, force=force,
                                       run_bgdel=False)   # 실패 시 여기서 예외
        self.polling.stop_polling(jobset_id)
        self._invalidate_handle(jobset_id)
        if js.lsf_group_paths:
            paths = list(js.lsf_group_paths)
            self._misc_pool.start(_CallTask(
                lambda: [self.command.bgdel(p) for p in paths]))

    def list_jobsets(self) -> List[JobSetRecord]:
        """[sync, snapshot] 현재 세션의 JobSet 목록."""
        return self.store.list_jobsets()

    # ------------------------------------------------------------------
    # Sqlite 전용 (FR-6) — InMemory면 PersistenceNotSupportedError
    # ------------------------------------------------------------------
    @property
    def persistent(self) -> bool:
        """[sync] Store 모드 판별 — GUI 복원 메뉴 분기용."""
        return self.store.persistent

    def list_orphan_jobsets(self) -> List[JobSetRecord]:
        """[sync, snapshot] 이전 세션 미종결 JobSet (FR-6.1)."""
        return self.store.list_orphan_jobsets()

    def recover_jobset(self, jobset_id: str) -> JobSet:
        """[sync] orphan을 현재 세션으로 복원 — JobSet 핸들 반환 (v7 §5)."""
        self.store.recover_jobset(jobset_id)
        return self.jobset(jobset_id)

    def reconcile(self, jobset_id: str) -> None:
        """[async→Signal] 저장 상태 vs LSF 실상태 대조 (worker 스레드).
        완료 시 jobset_updated Signal."""
        if not self.store.persistent:
            raise self.store._not_persistent()
        self._misc_pool.start(_ReconcileTask(self, jobset_id))

    def search_all_sessions(self, **kwargs) -> List[JobSetRecord]:
        return self.store.search_all_sessions(**kwargs)

    def get_history(self, jobset_id: str) -> List[Dict[str, Any]]:
        return self.store.get_history(jobset_id)

    def stats(self, since: Optional[datetime] = None,
              until: Optional[datetime] = None) -> Dict[str, Any]:
        return self.store.stats(since, until)

    def archive(self, older_than_days: int = 30) -> int:
        return self.store.archive(older_than_days)

    def export_jobset(self, jobset_id: str, path: str) -> None:
        self.store.export_jobset(jobset_id, path)

    # ------------------------------------------------------------------
    # 수명 관리
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """모든 스레드 안전 종료 (멱등, CS-8).
        AUTO-3로 aboutToQuit에 자동 연결되며 명시 호출도 안전."""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        log.info("lsfmgr shutdown 시작")
        app = QCoreApplication.instance()
        if app is not None:
            try:
                app.aboutToQuit.disconnect(self.shutdown)
            except (TypeError, RuntimeError):
                pass                             # 미연결/이미 해제 — 무시
        self.submitter.shutdown()       # 진행 중 bsub 완료 대기 (job_id 보존)
        self.polling.shutdown()
        self.killer.shutdown()
        self._misc_pool.waitForDone(-1)
        self.store.close()
        log.info("lsfmgr shutdown 완료 — 잔여 스레드 없음")

    # ------------------------------------------------------------------
    # 내부 slot — polling relay + 핸들 dispatch
    # ------------------------------------------------------------------
    def _on_poll_updated(self, jobset_id: str, summary: dict,
                         changed: list) -> None:
        """polling 결과 relay — 요약 + 변경분 batch (QT-4)."""
        self.jobset_updated.emit(jobset_id, summary)
        if changed:
            self.jobs_updated.emit(jobset_id, changed)

    def _handle_of(self, jobset_id: str) -> Optional[JobSet]:
        h = self._handles.get(jobset_id)
        return h if (h is not None and not h._closed) else None

    def _invalidate_handle(self, jobset_id: str) -> None:
        h = self._handles.pop(jobset_id, None)
        if h is not None:
            h._mark_closed()

    def _h_progress(self, jsid: str, done: int, total: int) -> None:
        h = self._handle_of(jsid)
        if h:
            h.progress.emit(done, total)

    def _h_finished(self, jsid: str, report) -> None:
        h = self._handle_of(jsid)
        if h is None:
            return
        h.finished.emit(report)
        if getattr(report, "failed", 0):
            failed = self.store.get_jobs(jsid,
                                         states={JobState.SUBMIT_FAILED})
            if failed:
                h.failed.emit(failed)

    def _h_updated(self, jsid: str, summary: dict) -> None:
        h = self._handle_of(jsid)
        if h:
            h.updated.emit(summary)

    def _h_jobs_updated(self, jsid: str, changed: list) -> None:
        h = self._handle_of(jsid)
        if h is None:
            return
        failed = [r for r in changed if r.state.is_failed]
        if failed:
            h.failed.emit(failed)

    def _h_kill_finished(self, jsid: str, report) -> None:
        h = self._handle_of(jsid)
        if h:
            h.killed.emit(report)

    def _h_error(self, jsid: str, message: str) -> None:
        h = self._handle_of(jsid)
        if h:
            h.error.emit(message)


class _CallTask(QRunnable):
    """임의 callable을 worker 스레드에서 실행 (bgdel 등 fire-and-forget)."""

    def __init__(self, fn):
        super().__init__()
        self.setAutoDelete(True)
        self._fn = fn

    def run(self):
        try:
            self._fn()
        except Exception:                    # noqa: BLE001 — CS-5
            log.exception("백그라운드 작업 실패")


class _ReconcileTask(QRunnable):
    """recover된 jobset의 상태 대조 — worker 스레드 (FR-6.2)."""

    def __init__(self, mgr: LsfJobManager, jobset_id: str):
        super().__init__()
        self.setAutoDelete(True)
        self.mgr = mgr
        self.jobset_id = jobset_id

    def run(self):
        try:
            result = self.mgr.querier.query(self.jobset_id)
            report = ReconcileReport(
                jobset_id=self.jobset_id,
                checked=result.checked,
                transitioned=len(result.changed), lost=len(result.lost),
                summary=dict(result.summary))
            log.info("reconcile %s: %d건 갱신, %d건 LOST",
                     self.jobset_id, report.transitioned, report.lost)
        except Exception as e:               # noqa: BLE001 — CS-5
            log.exception("reconcile 실패: %s", self.jobset_id)
            self.mgr.error_occurred.emit(self.jobset_id, repr(e))
            return
        self.mgr.jobset_updated.emit(self.jobset_id, result.summary)
        if result.changed:
            self.mgr.jobs_updated.emit(self.jobset_id, list(result.changed))
        for rec in result.lost:
            self.mgr.job_lost.emit(self.jobset_id, rec)
        # reconcile 후 미종결 job이 남으면 polling 자동 시작 (README §6)
        if not self.mgr._shutdown_done and any(
                r.state.is_on_lsf
                for r in self.mgr.store.get_jobs(self.jobset_id)):
            self.mgr.start_polling(self.jobset_id)


def _common_spec_options(specs: List[JobSpec]) -> Dict[str, Any]:
    """array 모드용 — 전 spec의 옵션이 동일해야 하며, 동일하면 그 값을
    ArrayJobSpec 인자로 반환한다. 조용히 버리면 -q/-R/-o/-e/env가 소실되어
    기본 queue·기본 리소스로 오실행되므로 상이하면 즉시 거부."""
    fields = ("queue", "resources", "outfile", "errfile", "env",
              "extra_args")
    for name in fields:
        values = {getattr(s, name) for s in specs}
        if len(values) > 1:
            raise ValueError(
                f"mode='array'는 job별 상이 옵션({name})을 지원하지 않습니다"
                f" — 동일 옵션으로 통일하거나 mode='bulk'를 사용하세요")
    first = specs[0]
    return {name: getattr(first, name) for name in fields}


def _normalize_jobs(jobs: Sequence[Union[str, JobSpec]]
                    ) -> Tuple[List[JobSpec], bool]:
    """list[str] → list[JobSpec] 변환. plain=True면 command 외 옵션이 없어
    AUTO-4 array 치환 후보가 될 수 있다."""
    specs: List[JobSpec] = []
    plain = True
    for j in jobs:
        if isinstance(j, str):
            specs.append(JobSpec(command=j))
        elif isinstance(j, JobSpec):
            specs.append(j)
            if (j.queue is not None or j.resources is not None
                    or j.outfile is not None or j.errfile is not None
                    or j.env is not None or j.extra_args):
                plain = False
        else:
            raise TypeError(
                f"submit()은 str 또는 JobSpec 목록만 허용 (got {type(j)!r})")
    return specs, plain


def _with_pattern(js: JobSetRecord, pattern: str) -> JobSetRecord:
    if pattern in js.name_patterns:
        return js
    return dc_replace(js, name_patterns=js.name_patterns + [pattern])
