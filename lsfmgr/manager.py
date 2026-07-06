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
import shlex
from dataclasses import replace as dc_replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from .command import LsfCommand, Runner
from .config import ArrayJobSpec, JobSpec, LsfConfig, spec_from_json
from .errors import JobNotFoundError, LsfmgrError
from .handle import JobSet
from .handlers import HandlerContext, JobSetHandlerService, StateSpec
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
from .resubmit import ResubmitCoordinator, ResubmitPlan
from .reports import ReconcileReport
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore
from .store.memory import InMemoryStore

log = logging.getLogger("lsfmgr.manager")

#: LsfConfig 필드로 직접 전달되는 manager 전용 키
_CONFIG_KEYS = ("bsub_path", "bjobs_path", "bkill_path", "bhist_path",
                "bmod_path", "bgdel_path", "script_dir", "lsf_group_root",
                "arg_max", "default_queue", "chunk_size",
                "kill_status_policy", "kill_max_retry", "kill_retry_delay_s")


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
    kill_progress = Signal(str, int, int)      # jobset_id, done, total (chunk kill)
    error_occurred = Signal(str, str)          # jobset_id, message
    handler_finished = Signal(str, str, object)  # jobset_id, handler_name, HandlerResult

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
        self.submitter.jobs_changed.connect(self._relay_jobs_changed)

        self.polling = PollingService(self.querier, parent=self)
        self.polling.updated.connect(self._on_poll_updated)
        self.polling.lost.connect(self.job_lost)
        self.polling.error.connect(self.error_occurred)

        self.killer = Killer(self.store, self.command, self.querier,
                             parent=self)
        self.killer.finished.connect(self.kill_finished)
        self.killer.progress.connect(self.kill_progress)
        self.killer.error.connect(self.error_occurred)

        # resubmit_jobs 오케스트레이터 — kill(worker) → resubmit(main) 순차 조율
        self._resubmitter = ResubmitCoordinator(self)
        self._resubmitter.jobs_changed.connect(self._relay_jobs_changed)

        # JobSet별 사용자 handler 주기 실행 (FR-7)
        self.handlers = JobSetHandlerService(self.store, parent=self)
        self.handlers.finished.connect(self.handler_finished)

        # jobset별 마지막 polling interval — resubmit 후 polling 재개에 사용
        self._poll_intervals: Dict[str, float] = {}

        self._misc_pool = QThreadPool(self)     # reconcile 등 단발 작업
        self._misc_pool.setMaxThreadCount(2)
        self._shutdown_done = False

        # --- JobSet 핸들 계층 (v7) — Facade Signal 위에 이중 발행 ---
        self._handles: Dict[str, JobSet] = {}
        # 핸들 Signal 이름은 Facade와 동일 — relay 대상 attr명도 그대로
        self.submit_progress.connect(self._handle_relay("submit_progress"))
        self.jobset_updated.connect(self._handle_relay("jobset_updated"))
        self.kill_finished.connect(self._handle_relay("kill_finished"))
        self.kill_progress.connect(self._handle_relay("kill_progress"))
        self.error_occurred.connect(self._handle_relay("error_occurred"))
        self.handler_finished.connect(self._handle_relay("handler_finished"))
        self.submit_finished.connect(self._h_finished)
        self.submit_finished.connect(self._emit_summary_after_submit)
        self.kill_finished.connect(self._emit_updates_after_kill)
        self.jobs_updated.connect(self._h_jobs_updated)

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

    # ------------------------------------------------------------------
    # submit_wrapper (v8) — wrapper 커맨드로 제출, job_id 기반 관리
    # ------------------------------------------------------------------
    def submit_wrapper(self,
                       commands: Union[str, Sequence[Union[str, Sequence[str]]]],
                       **kwargs: Any) -> JobSet:
        """[async→Signal] wrapper 커맨드로 job 제출 — JobSet 핸들 반환.

        실제 환경에서 job 마다 `primesim_sub`/`verilog_sub` 등 서로 다른 제출
        wrapper 를 쓰는 구조를 그대로 지원한다. lsfmgr 는 각 커맨드를 **그대로**
        subprocess 실행하고 stdout 의 `Job <id>` 만 파싱해, 그 job_id 로 모니터링·
        kill 을 수행한다(‑q/‑J/‑g 등 인자 조립·주입 없음, 그룹/이름 부착물 없음).

        commands:
          - 단일 문자열 `"primesim_sub -i a.sp"` → job 1개 (공백 분해)
          - 리스트의 각 항목이 job 1개. 항목은 문자열(공백 분해) 또는 토큰 리스트
            `["primesim_sub", "-i", "a.sp"]` (셸 파싱 없이 그대로).

        옵션(kwargs): workers / max_retry / rate_limit_per_s / label / tags /
        description / auto_poll / poll_interval_s. 재시도는 **비정상 종료(non-zero)
        만** 대상이며, 파싱 실패(NO_JOBID_PARSED)·timeout 은 재시도하지 않는다.
        """
        opts = self.resolve_options(kwargs, context="submit")
        argvs = _normalize_wrapper_commands(commands)
        if not argvs:
            raise ValueError("submit_wrapper: commands가 비어 있습니다")

        # job_id 만으로 관리 → 그룹/이름 부착물 없는 jobset 생성.
        js = self.jobsets.create_jobset(
            len(argvs), label=opts.label, tags=opts.tags,
            description=opts.description, with_attachments=False)
        self.submit_started.emit(js.jobset_id)
        self.submitter.submit_wrappers(js.jobset_id, argvs, opts)
        return self._post_submit(js.jobset_id, opts)

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
        """[async→Signal] 진행 중 submit 중단 — submit된 job은 유지 (QT-6).
        resubmit_jobs의 kill-phase 대기 중이면 그 plan도 취소한다 (이 구간엔
        submitter ctx가 없어 cancel이 조용히 증발하는 창이 있었다)."""
        self._resubmitter.cancel(jobset_id)
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
        eff = (interval_s if interval_s is not None
               else self._defaults["poll_interval_s"])
        self._poll_intervals[jobset_id] = eff    # resubmit 후 재개용 기억
        self.polling.start_polling(jobset_id, eff)

    def stop_polling(self, jobset_id: str) -> None:
        """[async→Signal] polling 중지."""
        # 재개 기억도 지운다 — 사용자가 일부러 끈 polling을 resubmit_jobs가
        # 마음대로 되살리지 않게 (재개는 AUTO-2 자동중지 복구 용도만)
        self._poll_intervals.pop(jobset_id, None)
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
                  jobset_id: Optional[str] = None,
                  verify: Optional[bool] = None) -> None:
        """[async→Signal] 개별 ID kill (chunking 자동).
        jobset_id를 주면 그 JobSet 컨텍스트로 동작 — optimistic EXIT 전이와
        verify가 켜지고 결과가 js.killed로도 중계된다. 생략하면 optimistic
        전이는 전역 검색으로 처리하되 verify는 스킵된다."""
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        self.killer.kill_jobs(job_ids, verify=verify,
                              jobset_id=jobset_id or "")

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

    def remove_job(self, jobset_id: str, job_key: str) -> JobRecord:
        """[sync] job 편입 취소 — 제거된 레코드 반환 (add_job의 역연산).
        LSF의 실제 job은 유지된다(저장소 추적에서만 제외)."""
        return self.jobsets.remove_job(jobset_id, job_key)

    def resubmit_jobs(self, jobset_id: str, job_keys: Sequence[str], *,
                 commands: Optional[Dict[str, str]] = None,
                 verify: bool = True, workers: Optional[int] = None,
                 max_retry: Optional[int] = None,
                 rate_limit_per_s: Optional[float] = None) -> None:
        """[async→Signal] 지정 job들을 상태에 따라 (재)실행 — 결과는 submit_finished.

        submit/resubmit을 호출자가 고르지 않는다. **각 job의 현재 상태**로 매니저가
        자동 분기한다:
          - LSF에 살아있는 job(is_on_lsf, 예: PEND/RUN) → **kill 후** 재제출
          - 그 외(CREATED/SUBMIT_FAILED/LOST/DONE/EXIT) → 그냥 제출
        레코드는 **재사용**된다 — 같은 job_key(-J 이름)로 다시 제출하고 job_id/
        exit_code만 교체하므로 목록 슬롯·intended_count가 유지된다(삭제/재생성 없음).

        job_keys: (재)실행할 job_key(lsf_job_name) 목록.
        commands: {job_key: 새 커맨드} — 생략 시 기존 rec.command 재사용.
        verify=True면 kill 후 실제 종료를 확인한 뒤 재제출한다.
        """
        recs = {r.job_key: r for r in self.get_jobs(jobset_id)}
        targets: List[JobRecord] = []
        for key in dict.fromkeys(job_keys):    # 중복 제거 (순서 유지) —
            rec = recs.get(key)                # 같은 key 2회면 이중 제출됨
            if rec is None:
                raise JobNotFoundError(f"{jobset_id}/{key}")
            if rec.array_index is not None:
                raise LsfmgrError(
                    f"array element({key})는 resubmit_jobs로 재제출할 수 없습니다")
            targets.append(rec)
        # kill-phase(코디네이터) 진행 중에도 거부해야 한다 — 이 구간엔
        # submitter ctx가 아직 없어 is_active만으로는 plan 덮어쓰기를 못 막는다
        if (self.submitter.is_active(jobset_id)
                or self._resubmitter.is_active(jobset_id)):
            raise LsfmgrError(
                f"{jobset_id}: submit/resubmit 진행 중에는 "
                f"resubmit_jobs를 호출할 수 없습니다")
        if not targets:
            return

        kw: Dict[str, Any] = {}
        if workers is not None:
            kw["workers"] = workers
        if max_retry is not None:
            kw["max_retry"] = max_retry
        if rate_limit_per_s is not None:
            kw["rate_limit_per_s"] = rate_limit_per_s
        opts = self.resolve_options(kw, context="submit")

        cmds = commands or {}
        live = [r for r in targets if r.state.is_on_lsf]
        live_ids = sorted({r.job_id for r in live if r.job_id is not None})
        live_keys = [r.job_key for r in live]

        # 재제출 경로는 job 단위 속성(rec.via_wrapper)으로 결정 — jobset 부착물
        # 유무로 판별하면 merge된 혼합 jobset에서 wrapper job을 bsub로(이중
        # 제출), bsub job을 로컬 실행으로(오실행) 보내는 오판이 생긴다
        def to_item(r: JobRecord):
            new_cmd = cmds.get(r.job_key)
            if r.via_wrapper:
                return shlex.split(new_cmd if new_cmd is not None
                                   else r.command)
            # bsub 경로 — 원 제출 옵션(queue/resources/outfile/env) 복원.
            # command만 다시 만들면 이 옵션들이 기본값으로 조용히 소실된다
            try:
                spec = (spec_from_json(r.spec_json) if r.spec_json
                        else JobSpec(command=r.command))
            except (ValueError, TypeError) as e:
                # 손상/신버전 spec_json (전방 호환) — 옵션은 포기하고
                # command만으로 진행. 여기서 죽으면 재제출 전체가 막힌다
                log.warning("spec_json 복원 실패(%s) — 옵션 없이 재제출: %s",
                            e, r.job_key)
                spec = JobSpec(command=r.command)
            if new_cmd is not None:
                spec = dc_replace(spec, command=new_cmd)
            return spec

        keyed = [(r.job_key, to_item(r)) for r in targets]

        self.submit_started.emit(jobset_id)
        self._resubmitter.start(ResubmitPlan(
            jobset_id=jobset_id, keyed=keyed, opts=opts,
            live_ids=live_ids, live_keys=live_keys, verify=verify))

    # ------------------------------------------------------------------
    # JobSet handler (FR-7)
    # ------------------------------------------------------------------
    def add_handler(self, jobset_id: str, name: str,
                    fn: "Callable[[HandlerContext], Any]", *,
                    start_states: StateSpec = None,
                    end_states: StateSpec = None) -> None:
        """[main→Signal] jobset에 이름 있는 handler 등록.

        **폴링 사이클마다**(bjobs 갱신 직후) 각 job을 검사해서 start_states
        (기본 {RUN})에 든 job은 handler(fn)를 worker 스레드에서 실행하고,
        end_states(기본 {DONE, EXIT}) 도달 시 마지막으로 한 번 더 실행한다.
        결과(fn 반환값)는 `handler_finished(jobset_id, name, HandlerResult)` 로
        온다. 별도 주기 없이 `poll_interval_s`에 tie되며, **폴링이 돌고 있어야
        동작**한다."""
        self.store.get_jobset(jobset_id)          # 존재 검증
        self.handlers.add_handler(
            jobset_id, name, fn,
            start_states=start_states, end_states=end_states)

    def remove_handler(self, jobset_id: str, name: str) -> None:
        """[main] handler 해제."""
        self.handlers.remove_handler(jobset_id, name)

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
                self.handlers.remove_all(old)
                self._poll_intervals.pop(old, None)
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
        self.handlers.remove_all(jobset_id)
        self._poll_intervals.pop(jobset_id, None)
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
        self.handlers.shutdown()        # handler 타이머 중지 + task 완료 대기
        self._resubmitter.shutdown()         # resubmit_jobs kill-phase 완료 대기
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
        """polling 결과 relay — 요약 + 변경분 batch (QT-4). 이어서 등록된
        handler를 평가한다 — Store가 방금 갱신됐으므로 handler는 최신 상태를
        본다 (handler는 폴링 사이클에 tie돼 있음, FR-7)."""
        self.jobset_updated.emit(jobset_id, summary)
        if changed:
            self.jobs_updated.emit(jobset_id, changed)
        self.handlers.tick(jobset_id)

    def _relay_jobs_changed(self, jsid: str, records: list) -> None:
        """상태 전이분(배치)을 즉시 jobs_updated + jobset_updated로 발행 —
        완료를 기다리지 않는다. submitter(초기 CREATED 선발행 → PEND/실패 점진)와
        resubmit kill 단계(EXIT 발행)가 공유한다. 파이프라인처럼 단계마다 표가
        갱신된다. (실패분은 _h_jobs_updated가 js.jobs_failed까지 중계)"""
        self.jobs_updated.emit(jsid, records)
        try:
            self.jobset_updated.emit(jsid, self.store.summary(jsid))
        except LsfmgrError:
            pass

    def _emit_summary_after_submit(self, jsid: str, report) -> None:
        """submit 완료 시 최종 요약(jobset_updated)을 보장 발화 — 진행 중
        점진 발행(_relay_jobs_changed)의 마무리. 개별 job(jobs_updated)은
        이미 점진 발행이 전부 커버했으므로 여기서 다시 쏘지 않는다(실패분
        js.jobs_failed 이중 발행 방지)."""
        try:
            summary = self.store.summary(jsid)
        except LsfmgrError:
            return                       # jobset이 이미 사라짐(merge/close 등)
        self.jobset_updated.emit(jsid, summary)

    def _emit_updates_after_kill(self, jsid: str, report) -> None:
        """kill 완료 시 상태 반영을 update Signal로 발화. optimistic 정책이면
        EXIT로 전이된 job(report.changed)을 jobs_updated로, 그리고 요약을
        jobset_updated로 — 폴링 없이도 UI가 kill 결과를 즉시 본다.
        (actual 정책이면 changed가 비어 요약만 나가고, 실제 EXIT는 폴링/verify로)

        kill_jobs(전역)는 changed가 여러 JobSet에 걸칠 수 있어 jobset별로 묶어
        발화한다."""
        changed = getattr(report, "changed", None) or []
        by_js: Dict[str, list] = {}
        for r in changed:
            by_js.setdefault(r.jobset_id, []).append(r)
        if jsid:                         # jobset 단위 kill은 changed 없어도 요약
            by_js.setdefault(jsid, [])
        for j, recs in by_js.items():
            if recs:
                self.jobs_updated.emit(j, recs)
            try:
                self.jobset_updated.emit(j, self.store.summary(j))
            except LsfmgrError:
                pass

    def _handle_of(self, jobset_id: str) -> Optional[JobSet]:
        h = self._handles.get(jobset_id)
        return h if (h is not None and not h._closed) else None

    def _invalidate_handle(self, jobset_id: str) -> None:
        h = self._handles.pop(jobset_id, None)
        if h is not None:
            h._mark_closed()

    def _handle_relay(self, signal_name: str):
        """Facade Signal(jsid, ...)을 해당 핸들의 Signal(...)로 중계하는 slot."""
        def slot(jsid: str, *args) -> None:
            h = self._handle_of(jsid)
            if h is not None:
                getattr(h, signal_name).emit(*args)
        return slot

    def _h_finished(self, jsid: str, report) -> None:
        h = self._handle_of(jsid)
        if h is None:
            return
        h.submit_finished.emit(report)
        # js.jobs_failed는 submit 완료 시 발화되는 jobs_updated →
        # _h_jobs_updated가 담당한다 (SUBMIT_FAILED 포함) — 여기서 또 쏘면 이중.

    def _h_jobs_updated(self, jsid: str, changed: list) -> None:
        h = self._handle_of(jsid)
        if h is None:
            return
        failed = [r for r in changed if r.state.is_failed]
        if failed:
            h.jobs_failed.emit(failed)


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


def _normalize_wrapper_commands(
        commands: Union[str, Sequence[Union[str, Sequence[str]]]]
        ) -> List[List[str]]:
    """submit_wrapper 입력을 argv(토큰 리스트) 목록으로 정규화.

    - 최상위 문자열 → job 1개 (shlex 분해)
    - 리스트의 각 항목이 job 1개: 문자열이면 shlex 분해, 토큰 리스트면 그대로
    """
    if isinstance(commands, str):
        commands = [commands]
    argvs: List[List[str]] = []
    for c in commands:
        if isinstance(c, str):
            argv = shlex.split(c)
        elif isinstance(c, (list, tuple)):
            argv = [str(t) for t in c]
        else:
            raise TypeError(
                "submit_wrapper: 각 커맨드는 str 또는 토큰 리스트여야 함 "
                f"(got {type(c)!r})")
        if not argv:
            raise ValueError("submit_wrapper: 빈 커맨드는 허용되지 않습니다")
        argvs.append(argv)
    return argvs


def _with_pattern(js: JobSetRecord, pattern: str) -> JobSetRecord:
    if pattern in js.name_patterns:
        return js
    return dc_replace(js, name_patterns=js.name_patterns + [pattern])
