"""LsfJobManager — 앱이 사용하는 단일 진입점 (QObject Facade + 핸들 발급).

- High-level: submit() → JobSet 핸들 (v7 §1.1~1.3), AUTO-1~4 자동화
- Low-level: 전역 Facade Signal (v6 §1.4 유지) — 핸들 Signal은 그 위의
  편의 계층으로 동일 이벤트를 이중 발행한다.
- 옵션은 defaults → manager kwargs → call kwargs 3단 계층 (§1.2, options.py)

QT-0 표기 규약: [async→Signal] = 즉시 반환·결과는 Signal /
[sync, snapshot] = 동기지만 Store 스냅샷만 조회 (LSF 호출 없음).
"""
from __future__ import annotations

import atexit
import logging
import os
import re
import shlex
from dataclasses import replace as dc_replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from .command import LsfCommand, Runner
from .config import JobSpec, LsfConfig, spec_from_json, spec_to_json
from .errors import (
    JobNotFoundError,
    LsfmgrError,
    MergeNotAllowedError,
    SubmitNotAllowedError,
)
from .handle import JobSet
from .handlers import HandlerContext, JobSetHandlerService, StateSpec
from .jobset_core import JobSetManager
from .killer import Killer
from .monitor import JobsetQuerier, PollingService
from .options import (
    MANAGER_ONLY_KEYS,
    Options,
    SHARED_KEYS,
    resolve_options,
    validate_options,
)
from .qt import QCoreApplication, QObject, QRunnable, QThreadPool, QTimer, Signal
from .reports import KillProgress, SubmitProgress
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore
from .store.memory import InMemoryStore

log = logging.getLogger("lsfmgr.manager")

#: LsfConfig 필드로 직접 전달되는 manager 전용 키
_CONFIG_KEYS = ("bsub_path", "bjobs_path", "bkill_path", "bhist_path",
                "bgdel_path", "script_dir", "lsf_group_root",
                "arg_max", "default_queue", "chunk_size",
                "kill_status_policy", "kill_max_retry", "kill_retry_delay_s",
                "progress_min_interval_s", "progress_min_step_ratio",
                "poll_runtime_updates", "submit_finished_on_gate_reject",
                "collect_clusters")


class LsfJobManager(QObject):
    """Facade — 컴포넌트 조립 + Facade Signal + JobSet 핸들 발급."""

    # --- Low-level Facade Signal (v6 유지, 모두 jobset_id 포함) ---
    submit_started = Signal(str)               # jobset_id (게이트 통과 후)
    pre_submit_started = Signal(str)                # jobset_id — pre_submit 게이트 시작
    pre_submit_finished = Signal(str, bool)         # jobset_id, ok — 게이트 종료(True=통과)
    post_processing_started = Signal(str)               # jobset_id — 전원 terminal 후처리 시작
    post_processing_finished = Signal(str, object)      # jobset_id, result (예외 시 None)
    submit_progress = Signal(str, int, int)    # jobset_id, done, total
    submit_finished = Signal(str, object)      # jobset_id, SubmitReport
    jobset_updated = Signal(str, dict)         # jobset_id, summary
    jobs_updated = Signal(str, list)           # jobset_id, [JobRecord] 변경분
    job_lost = Signal(str, object)             # jobset_id, JobRecord
    kill_started = Signal(str)                 # jobset_id — kill 접수 즉시(동기)
    kill_finished = Signal(str, object)        # jobset_id, KillReport
    kill_progress = Signal(str, int, int)      # jobset_id, done, total (chunk kill)
    error_occurred = Signal(str, str)          # jobset_id, message
    handler_finished = Signal(str, str, object)  # jobset_id, handler_name, HandlerResult
    job_detail_ready = Signal(str, str, str)   # jobset_id, job_key, 상세 텍스트

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

        # --- Store: 주입 객체 > InMemory(기본) ---
        self.store = store if store is not None else InMemoryStore()

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

        # kill 우선권 게이트 (FR-3) — submit 사이클 등록과 kill barrier를
        # 한 lock으로 묶어, kill의 취소를 빠져나가는 늦은 submit 시작이
        # 구조적으로 불가능하게 한다 (lifecycle.py). submitter가 등록하고
        # killer가 kill_jobset의 scope로 barrier를 잡는다.
        from .lifecycle import SubmitGate
        self._gate = SubmitGate()

        from .submitter import BulkSubmitter
        self.submitter = BulkSubmitter(self.store, self.command,
                                       self.jobsets, self.config, parent=self,
                                       gate=self._gate)
        self.submitter.progress.connect(self.submit_progress)
        self.submitter.finished.connect(self.submit_finished)
        self.submitter.error.connect(self.error_occurred)
        self.submitter.jobs_changed.connect(self._relay_jobs_changed)
        self.submitter.started.connect(self.submit_started)     # 게이트 통과 후
        self.submitter.pre_submit_started.connect(self.pre_submit_started)
        self.submitter.pre_submit_finished.connect(self.pre_submit_finished)

        self.polling = PollingService(self.querier, parent=self)
        self.polling.updated.connect(self._on_poll_updated)
        self.polling.lost.connect(self.job_lost)
        self.polling.error.connect(self.error_occurred)

        self.killer = Killer(self.store, self.command, self.querier,
                             parent=self)
        self.killer.finished.connect(self.kill_finished)
        self.killer.progress.connect(self.kill_progress)
        self.killer.error.connect(self.error_occurred)

        # JobSet별 사용자 handler 주기 실행 (FR-7)
        self.handlers = JobSetHandlerService(self.store, parent=self)
        self.handlers.finished.connect(self.handler_finished)

        # jobset별 마지막 polling interval — resubmit 후 polling 재개에 사용
        self._poll_intervals: Dict[str, float] = {}

        self._misc_pool = QThreadPool(self)     # 단발 작업 (detail 조회 등)
        self._misc_pool.setMaxThreadCount(2)
        self._shutdown_done = False

        # post_process — jobset이 전원 terminal에 도달하면 1회 실행되는 후처리
        # 콜백. submit(post_process=fn)으로 무장, 완료 감지(_on_poll_updated)
        # 시점에 worker에서 실행하고 post_processing_started/finished로 통지.
        self._post_process: Dict[str, Callable] = {}
        self._post_pool = QThreadPool(self)
        self._post_pool.setMaxThreadCount(2)

        # pre_submit 게이트 경로의 AUTO-1 지연 — 게이트 통과(pre_submit_finished True)
        # 후에야 polling을 켠다. 게이트가 오래 걸리면 레코드가 없어 AUTO-2가
        # polling을 조기 중지해 실제 job 전이를 놓치기 때문.
        self._pending_autopoll: Dict[str, float] = {}
        self._pending_rearm: Dict[str, list] = {}

        # --- JobSet 핸들 계층 (v7) — Facade Signal 위에 이중 발행 ---
        self._handles: Dict[str, JobSet] = {}
        # 핸들 Signal 이름은 Facade와 동일 — relay 대상 attr명도 그대로
        self.submit_progress.connect(self._handle_relay("submit_progress"))
        self.jobset_updated.connect(self._handle_relay("jobset_updated"))
        self.kill_started.connect(self._handle_relay("kill_started"))
        self.kill_finished.connect(self._handle_relay("kill_finished"))
        self.kill_progress.connect(self._handle_relay("kill_progress"))
        self.error_occurred.connect(self._handle_relay("error_occurred"))
        self.handler_finished.connect(self._handle_relay("handler_finished"))
        self.job_detail_ready.connect(self._handle_relay("job_detail_ready"))
        self.pre_submit_started.connect(self._handle_relay("pre_submit_started"))
        self.pre_submit_finished.connect(self._handle_relay("pre_submit_finished"))
        self.pre_submit_finished.connect(self._on_pre_submit_finished)
        self.post_processing_started.connect(self._handle_relay("post_processing_started"))
        self.post_processing_finished.connect(self._handle_relay("post_processing_finished"))
        self.submit_finished.connect(self._h_finished)
        self.submit_finished.connect(self._emit_summary_after_submit)
        self.kill_finished.connect(self._emit_updates_after_kill)
        self.jobs_updated.connect(self._h_jobs_updated)

        # AUTO-3: 스레드 좀비/‏core dump 원천 차단 — shutdown을 아래 3중으로 보장.
        # (1) 앱 이벤트루프 정상 종료: aboutToQuit (앱이 이미 있으면 즉시 연결).
        # (2) 앱을 나중에 만든 경우: 매 이벤트 사이클 초 aboutToQuit 재시도(1회성).
        # (3) 최후 안전망: 인터프리터 종료 시 atexit. — 모두 멱등이라 중복 안전.
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self.shutdown)
        else:
            # 매니저를 QApplication보다 먼저 만든 경우 — 앱이 생기면 그때 연결
            self._hook_timer = QTimer(self)
            self._hook_timer.setInterval(200)
            self._hook_timer.timeout.connect(self._try_hook_about_to_quit)
            self._hook_timer.start()
        atexit.register(self.shutdown)       # (3) 이벤트루프 없이 끝나도 정리

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
    def submit(self, js, *,
               pre_submit: Optional[Callable[[List[str]], bool]] = None,
               post_process: Optional[Callable[[list], Any]] = None,
               **kwargs: Any) -> JobSet:
        """[async→Signal] jobset 제출 — **유일한 제출 경로** (v9).

        jobset의 **전 job**을 (재)제출한다: 전원 비활성(CREATED/DONE/EXIT/
        SUBMIT_FAILED/LOST)이어야 하며 활성이 있으면 LsfmgrError —
        can_submit(js)로 선확인. 리셋 후 재실행되므로 같은 jobset/job_key가
        전이된다(핸들·테이블 연속). 흐름:

            js = mgr.create_jobset(
                ["customwrapper_sub -q normal run_0.sp",
                 "customwrapper_sub -q long tb_1.v"],
                merge_ids=["run_0", "tb_1"], label="sweep")
            mgr.submit(js, workers=8)

        pre_submit(commands)->bool: 지정 시 제출 전에 커맨드 리스트 전체를
        게이트 워커에서 검사(FR-9) — **리셋 이전**이라 False/예외면 레코드
        원상 유지. 신호: (pre_submit_started → pre_submit_finished(ok)) →
        submit_started → jobs_updated/progress → submit_finished.

        post_process(records)->Any: 지정 시 이 제출의 **전 job이 terminal**에
        도달하면(폴링/query_once로 완료 감지) worker에서 1회 실행. 인자는 최종
        JobRecord 목록(성공/실패 혼재 가능 — DONE/EXIT/SUBMIT_FAILED/LOST 무관
        전원 terminal이면 실행). 반환값은 post_processing_finished로 전달.
        신호: post_processing_started → post_processing_finished(result).
        ※ pre_submit·post_process 콜백 모두 worker 스레드 실행 — GUI 접근 금지.
        옵션(kwargs): workers/max_retry/rate_limit_per_s/auto_poll/
        poll_interval_s/queue/submit_timeout_s 등 (§1.2)."""
        return self._submit_jobset(js, pre_submit=pre_submit,
                                   post_process=post_process, **kwargs)

    def _on_pre_submit_finished(self, jsid: str, ok: bool) -> None:
        """pre_submit 게이트 종료 — 통과 시 미뤄둔 rearm/AUTO-1 polling.
        rearm을 폴링 시작보다 먼저(같은 main slot) 해야 재실행 첫 tick에서
        핸들러가 새 주기로 돈다."""
        keys = self._pending_rearm.pop(jsid, None)
        iv = self._pending_autopoll.pop(jsid, None)
        if not ok:
            return
        if keys:
            self.handlers.rearm(jsid, keys)
        if iv is not None:
            self.start_polling(jsid, iv)

    @staticmethod
    def _jsid(js) -> str:
        """명령 인자 정규화 — JobSet 핸들 또는 jobset_id 문자열을 받는다.
        모든 명령은 manager 한 곳에만 있고(v9 통일), 핸들은 조회+신호 전용."""
        if isinstance(js, JobSet):
            js._check_open()
            return js._jobset_id
        return js

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
    def cancel_submit(self, jobset_id: str) -> None:
        """[async→Signal] 진행 중 submit 중단 — submit된 job은 유지 (QT-6)."""
        jobset_id = self._jsid(jobset_id)
        self.submitter.cancel_submit(jobset_id)

    def is_submitting(self, jobset_id: str) -> bool:
        """[sync] 이 jobset에 진행 중인 submit/resubmit이 있는지.
        대량 제출을 백그라운드로 돌려놓고 진행 dialog를 닫은 뒤에도, 아직
        도는 중인지 아무 때나 확인한다."""
        jobset_id = self._jsid(jobset_id)
        return self.submitter.is_active(jobset_id)

    def submit_snapshot(self, jobset_id: str) -> "Optional[SubmitProgress]":
        """[sync] 진행 중 submit의 실시간 스냅샷 (done/total/성공/실패/취소) —
        없거나 이미 끝났으면 None. submit_progress Signal을 놓친 시점에도 현재
        진행을 pull로 조회한다(백그라운드 제출 상태 패널 재구성용).
        resubmit의 kill 단계처럼 아직 submit ctx가 없는 구간에선 None이지만
        is_submitting은 True일 수 있다(준비 중)."""
        jobset_id = self._jsid(jobset_id)
        return self.submitter.progress_snapshot(jobset_id)

    def is_killing(self, jobset_id: str) -> bool:
        """[sync] 이 jobset에 진행 중인 kill이 있는지. 대량 chunked kill을
        백그라운드로 돌려놓고 진행 dialog를 닫은 뒤에도 확인한다."""
        jobset_id = self._jsid(jobset_id)
        return self.killer.is_active(jobset_id)

    def kill_snapshot(self, jobset_id: str) -> "Optional[KillProgress]":
        """[sync] 진행 중 kill의 실시간 스냅샷(done/total) — 없으면 None.
        kill_progress Signal을 놓친 시점에도 현재 진행을 pull로 조회한다."""
        jobset_id = self._jsid(jobset_id)
        return self.killer.progress_snapshot(jobset_id)

    # --- 내부 submit 구현 (High/Low 공유) ---
    def start_polling(self, jobset_id: str,
                      interval_s: Optional[float] = None) -> None:
        """[async→Signal] 주기 polling 시작 — 갱신은 jobset_updated."""
        jobset_id = self._jsid(jobset_id)
        eff = float(interval_s if interval_s is not None
                    else self._defaults["poll_interval_s"])
        if eff <= 0:
            # 0이면 QTimer가 매 이벤트 루프마다 발화 — bjobs 핫루프로
            # LSF master를 두들긴다 (옵션 경로의 5~60초 검증과 달리
            # 직접 호출은 무검증이었음)
            raise ValueError(f"interval_s는 양수여야 합니다 (got {eff})")
        self._poll_intervals[jobset_id] = eff    # merge 이관/재개용 기억
        self.polling.start_polling(jobset_id, eff)

    def stop_polling(self, jobset_id: str) -> None:
        """[async→Signal] polling 중지."""
        jobset_id = self._jsid(jobset_id)
        # 재개 기억도 지운다 — 사용자가 일부러 끈 polling이 merge 이관 등으로
        # 마음대로 되살아나지 않게
        self._poll_intervals.pop(jobset_id, None)
        self.polling.stop_polling(jobset_id)

    def query_once(self, jobset_id: str) -> None:
        """[async→Signal] 1회 갱신 — 결과는 jobset_updated/jobs_updated."""
        jobset_id = self._jsid(jobset_id)
        self.polling.poll_now(jobset_id)

    def summary(self, jobset_id: str) -> Dict[str, Any]:
        """[sync, snapshot] Store의 현재 요약 (LSF 호출 없음)."""
        jobset_id = self._jsid(jobset_id)
        return self.store.summary(jobset_id)

    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        """[sync, snapshot] job 상세 (Store 조회)."""
        jobset_id = self._jsid(jobset_id)
        return self.store.get_jobs(jobset_id, states)

    def fetch_job_detail(self, jobset_id: str, job_key: str) -> None:
        """[async→Signal] job 1건의 실패/종료 상세 텍스트 조회 — 결과는
        job_detail_ready(jobset_id, job_key, text).

        UI에서 상태 셀 클릭 시 온디맨드로 호출한다 (폴링과 무관 — 자동 수집
        오버헤드 없음). LSF에 제출됐던 job(job_id 확보)은 `bhist -l` 원문,
        제출 실패 job(job_id 없음)은 저장된 fail_message(터미널 stderr/stdout).
        blocking(bhist)은 worker 스레드에서 수행되므로 GUI가 멎지 않는다."""
        jobset_id = self._jsid(jobset_id)
        rec = self.store.get_job(jobset_id, job_key)   # 존재 검증 (동기)

        def work():
            try:
                text = self._job_detail_text(rec)
            except Exception as e:               # noqa: BLE001 — CS-5
                # LsfmgrError 외의 예외(bhist_path 오설정 FileNotFoundError 등)
                # 도 반드시 signal로 응답한다 — 여기서 전파되면 _CallTask가
                # 삼켜 job_detail_ready가 영영 안 오고 UI가 무응답이 된다
                text = f"(조회 실패) {e}"
            self.job_detail_ready.emit(jobset_id, job_key, text)

        self._misc_pool.start(_CallTask(work))

    def job_detail(self, jobset_id: str, job_key: str) -> str:
        """[sync, LSF 조회 포함] fetch_job_detail의 동기 버전 — blocking 주의
        (GUI main 스레드에서는 fetch_job_detail 권장)."""
        jobset_id = self._jsid(jobset_id)
        return self._job_detail_text(self.store.get_job(jobset_id, job_key))

    def _job_detail_text(self, rec: JobRecord) -> str:
        """상세 텍스트 결정 — bhist -l 원문 우선, 없으면 fail_message."""
        if rec.job_id is None:                # 제출 실패 — LSF에 이력 없음
            return rec.fail_message or ""
        text = self.command.bhist_detail(rec.job_id, rec.array_index)
        if not text.strip():                  # bhist 이력 만료 등
            return rec.fail_message or ""
        return text

    # ------------------------------------------------------------------
    # Kill (FR-3)
    # ------------------------------------------------------------------
    def kill(self, jobset_id, *,
                    only_state: Optional[JobState] = None,
                    verify: Optional[bool] = None, envpath: str = "") -> None:
        """[async→Signal] JobSet kill — 결과는 kill_finished.
        verify 미지정 시 verify_kill 옵션(②) 적용.
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job)."""
        jobset_id = self._jsid(jobset_id)
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        scope = None
        if only_state is None:
            # 전체 kill은 진행 중 submit에 대해 우선권을 갖는다 (FR-3):
            # ① 진행 중 submit 즉시 취소(응답성 — 빨리 멈출수록 kill 대상↓),
            #    kill-phase 대기 중 재제출 plan 취소(kill 후 발화 부활 방지).
            # ② 대기 중 submit 재시도 포기 확정 — 안 하면 RETRY_WAIT의
            #    QTimer가 kill 뒤에 발화해 job이 부활한다.
            # ③ killer가 SubmitGate barrier(scope)를 잡는다 — 정확성은 이게
            #    보장한다: barrier와 등록이 한 lock 아래 원자적이라, ①이 못
            #    잡은 늦은 사이클은 barrier가 넘겨받아 취소하거나(먼저 등록)
            #    등록 자체가 거부된다(나중). 재취소 루프가 필요 없다.
            # (부분 kill(only_state)은 살아있는 특정 상태만 겨냥하므로 유지)
            self.submitter.cancel_submit(jobset_id)
            self.submitter.abort_retries(jobset_id)
            scope = self._gate.kill_scope(jobset_id)
        self.killer.kill_jobset(jobset_id, only_state=only_state,
                                verify=verify, envpath=envpath,
                                scope=scope)
        # 접수 즉시(동기) 착수 통지 — quiesce(진행 중 bsub 완료 대기)로
        # kill_finished가 수십 초 늦어지는 케이스에서도 UI가 '접수됨'을
        # 바로 표시할 수 있다. killer.kill_jobset(동기 — 등록+task 큐잉만)
        # **이후**에 발행해야 kill_started slot에서 is_killing()/
        # kill_snapshot()을 pull해도 True/값이 나온다 (신호-pull 일치).
        self.kill_started.emit(jobset_id)

    def kill_jobs(self, job_ids_or_jobset, job_keys: Optional[Sequence[str]] = None, *,
                  jobset_id: Optional[str] = None,
                  verify: Optional[bool] = None, envpath: str = "") -> None:
        """[async→Signal] 개별 job kill (chunking 자동).

        두 형태를 받는다:
          - kill_jobs(js, [job_key, ...]) — jobset의 선택 job만 kill (GUI
            테이블 선택 행). array element는 "id[idx]"로 변환돼 그 element만
            죽는다(parent id로 죽이면 나머지 element까지 전부 kill됨).
          - kill_jobs([id 또는 "id[idx]", ...], jobset_id=...) — 원시 id 기반.
        jobset 컨텍스트가 있으면 optimistic EXIT 전이·verify가 켜지고 결과가
        핸들 kill_finished로도 중계된다. envpath 지정 시 그 LSF env를
        source한 bkill (MC forward job)."""
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        if isinstance(job_ids_or_jobset, JobSet) or job_keys is not None:
            jsid = self._jsid(job_ids_or_jobset)
            recs = {r.job_key: r for r in self.get_jobs(jsid)}
            ids: List[object] = []
            for k in (job_keys or ()):
                r = recs.get(k)
                if r is None or r.job_id is None:
                    continue
                ids.append(f"{r.job_id}[{r.array_index}]"
                           if r.array_index is not None else r.job_id)
        else:
            ids = list(job_ids_or_jobset)
            jsid = (self._jsid(jobset_id)
                    if jobset_id is not None else "")
        self.killer.kill_jobs(ids, verify=verify,
                              jobset_id=jsid or "", envpath=envpath)
        if jsid:                         # jobset 컨텍스트가 있을 때만 착수 통지
            self.kill_started.emit(jsid)   # killer 등록 후 (pull 일치)

    def create_jobset(self, commands: Sequence = (), *,
                      merge_ids: Optional[Sequence[Optional[str]]] = None,
                      user_datas: Optional[Sequence[Optional[dict]]] = None,
                      wrapper: bool = True,
                      label: str = "", tags: Sequence[str] = (),
                      parent: Optional[str] = None,
                      intended_count: int = 0) -> JobSet:
        """[sync] JobSet 생성 — job까지 함께 만들고 핸들 즉시 반환 (CREATED).

        **job 생성은 이 함수 한 곳뿐이다** (v9). 생성 후 job을 더 넣는
        유일한 방법은 **merge** — 별도 jobset을 만들어 `mgr.merge(js, src)`로
        흡수한다. 흐름:

            js = mgr.create_jobset(
                ["customwrapper_sub -i a.sp", "customwrapper_sub -i b.sp"],
                merge_ids=["a", "b"], user_datas=[{"run": "..."}, None],
                label="sweep")
            if mgr.can_submit(js):
                mgr.submit(js, workers=8)     # 전 job (재)제출

        commands 각 항목의 타입으로 제출 경로가 정해진다:
          - JobSpec          → bsub 경로 (queue/resources 등 옵션 보존)
          - 토큰 리스트(argv) → wrapper 경로 (그대로 실행)
          - 문자열           → wrapper=True(기본)면 wrapper(공백 분해),
                               False면 bsub(JobSpec(command=...))
        merge_ids: 각 job의 논리 키 — merge 시 같은 merge_id의 기존 job이
        이 내용으로 replace된다. jobset 내 유일해야 한다(None 제외).
        user_datas: job별 사용자 정의 dict (JSON 직렬화 가능) — 보존만.
        merge_ids/user_datas는 commands와 같은 길이(생략 시 전부 None).
        commands가 비면 **빈 jobset** — 이후 merge로만 채운다.
        생성 즉시 jobs_updated/jobset_updated가 발행돼 표가 갱신된다."""
        if isinstance(tags, str):             # 편의: 단일 태그 문자열 허용
            tags = [tags]
        rec = self.jobsets.create_jobset(
            intended_count, label=label, tags=tags, parent=parent)
        jsid = rec.jobset_id
        items = list(commands)
        if items:
            records = self._build_job_records(
                jsid, items, merge_ids, user_datas, wrapper)
            out = self.jobsets.create_jobs(jsid, records)
            self._relay_jobs_changed(jsid, list(out))     # 표 즉시 갱신
        return self.jobset(jsid)

    def _build_job_records(self, jsid: str, items: list,
                           merge_ids: Optional[Sequence[Optional[str]]],
                           user_datas: Optional[Sequence[Optional[dict]]],
                           wrapper: bool) -> List[JobRecord]:
        """commands → CREATED JobRecord 목록 (create_jobset 내부용)."""
        mids = list(merge_ids) if merge_ids is not None else [None] * len(items)
        uds = list(user_datas) if user_datas is not None else [None] * len(items)
        if len(mids) != len(items) or len(uds) != len(items):
            raise ValueError("merge_ids/user_datas 길이가 commands와 다릅니다")

        # job_key 연번 — 기존 키의 최대 suffix 다음부터
        used = set()
        for r in self.get_jobs(jsid):
            m = re.match(rf"^{re.escape(jsid)}_(\d+)$", r.job_key)
            if m:
                used.add(int(m.group(1)))
        nxt = (max(used) + 1) if used else 0

        records = []
        for item, mid, ud in zip(items, mids, uds):
            key = f"{jsid}_{nxt}"
            nxt += 1
            if isinstance(item, JobSpec):
                records.append(JobRecord(
                    job_id=None, array_index=None, jobset_id=jsid,
                    lsf_job_name=key, state=JobState.CREATED,
                    command=item.command, via_wrapper=False,
                    spec_json=spec_to_json(item), merge_id=mid, user_data=ud))
                continue
            if isinstance(item, str):
                if not wrapper:
                    records.append(JobRecord(
                        job_id=None, array_index=None, jobset_id=jsid,
                        lsf_job_name=key, state=JobState.CREATED,
                        command=item, via_wrapper=False,
                        spec_json=spec_to_json(JobSpec(command=item)),
                        merge_id=mid, user_data=ud))
                    continue
                argv = shlex.split(item)
            else:
                argv = [str(t) for t in item]
            if not argv:
                raise ValueError("create_jobset: 빈 커맨드")
            records.append(JobRecord(
                job_id=None, array_index=None, jobset_id=jsid,
                lsf_job_name=key, state=JobState.CREATED,
                command=shlex.join(argv), via_wrapper=True,
                merge_id=mid, user_data=ud))
        return records

    def set_user_data(self, jobset_id: str, ref, user_data: Optional[dict]
                    ) -> JobRecord:
        """[sync] job의 user_data 교체 — ref는 job_key(str) 또는 merge_id(str,
        job_key 미매칭 시) 또는 job_id(int). 갱신 레코드를 jobs_updated로
        발행한다."""
        jobset_id = self._jsid(jobset_id)
        rec = self._find_job(jobset_id, ref)
        new = self.store.update_job(dc_replace(rec, user_data=user_data))
        self._relay_jobs_changed(jobset_id, [new])
        return new

    def _find_job(self, jobset_id: str, ref) -> JobRecord:
        """job_id(int) / job_key / merge_id 로 단일 job 찾기."""
        jobs = self.get_jobs(jobset_id)
        if isinstance(ref, int):
            hits = [r for r in jobs if r.job_id == ref]
        else:
            hits = [r for r in jobs if r.job_key == ref]
            if not hits:
                hits = [r for r in jobs if r.merge_id == ref]
        if not hits:
            raise JobNotFoundError(f"{jobset_id}/{ref}")
        return hits[0]

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
        jobset_id = self._jsid(jobset_id)
        self.store.get_jobset(jobset_id)          # 존재 검증
        self.handlers.add_handler(
            jobset_id, name, fn,
            start_states=start_states, end_states=end_states)

    def remove_handler(self, jobset_id: str, name: str) -> None:
        """[main] handler 해제."""
        jobset_id = self._jsid(jobset_id)
        self.handlers.remove_handler(jobset_id, name)

    def merge(self, target_id, source_id, *,
                   force: bool = False) -> List[JobRecord]:
        """[sync] source jobset을 target에 **in-place 흡수** — merge_id 기준
        (FR-5.5 v9). source는 삭제되고 target 핸들/테이블은 그대로 유지된다.

        규칙: source job의 merge_id가 target에 있으면 그 job을 **replace**
        (물리 키 job_key 유지 — 테이블 행 연속), 없거나 None이면 신규 추가.
        가드: 양쪽 전 job이 비활성(CREATED/DONE/EXIT 등)이어야 하며 활성이
        있으면 LsfmgrError — force=True면 레코드만 강제 교체(살아있는 LSF
        job의 정리는 caller 책임, 먼저 kill 권장). can_merge()로 선확인.
        반환: target에서 replace/추가된 레코드 (jobs_updated로도 발행)."""
        tid = self._jsid(target_id)
        sid = self._jsid(source_id)
        for jsid in (tid, sid):
            if self.submitter.is_active(jsid) or self.killer.is_active(jsid):
                raise MergeNotAllowedError(
                    f"{jsid}: submit/kill 진행 중에는 merge할 수 없습니다",
                    jobset_id=jsid)
        changed = self.jobsets.merge_from(tid, sid, force=force)
        # source 정리 — 삭제된 jobset을 계속 polling하면 error 폭주.
        # 폴링 연속성: target이 폴링을 안 쓰는데 source가 쓰고 있었다면
        # 가장 짧은 interval로 target에 이어받는다.
        src_iv = self._poll_intervals.pop(sid, None)
        self._post_process.pop(sid, None)        # source 소멸 — 후처리 무장 해제
        self.polling.stop_polling(sid)
        self.handlers.remove_all(sid)
        self._invalidate_handle(sid)
        tgt_iv = self._poll_intervals.get(tid)
        if src_iv is not None:
            self.start_polling(tid,
                               min(src_iv, tgt_iv) if tgt_iv else src_iv)
        if changed:
            self._relay_jobs_changed(tid, list(changed))
        return changed

    def can_merge(self, target_id, source_id) -> bool:
        """[sync, snapshot] merge_from 가능 여부 — 양쪽 전 job이 비활성이고
        진행 중 작업(submit/kill)이 없으면 True. GUI 버튼 활성화 판단용."""
        target_id = self._jsid(target_id)
        source_id = self._jsid(source_id)
        try:
            if target_id == source_id:
                return False
            if (self.submitter.is_active(target_id)
                    or self.submitter.is_active(source_id)
                    or self.killer.is_active(target_id)
                    or self.killer.is_active(source_id)):
                return False
            return all(r.state.is_inactive
                       for r in (self.get_jobs(target_id)
                                 + self.get_jobs(source_id)))
        except LsfmgrError:
            return False

    def remove_job(self, jobset_id, *,
                    job_id: Optional[int] = None,
                    merge_id: Optional[str] = None,
                    job_key: Optional[str] = None,
                    force: bool = False) -> List[JobRecord]:
        """[sync] job 삭제 — job_id/merge_id/job_key 중 하나로 지정.
        비활성만 삭제 가능(활성이면 LsfmgrError, force로 레코드만 강제
        삭제 — LSF job 정리는 caller 책임). 삭제분은 jobset_updated로 반영."""
        jobset_id = self._jsid(jobset_id)
        removed = self.jobsets.remove_jobs(
            jobset_id, job_id=job_id, merge_id=merge_id, job_key=job_key,
            force=force)
        self._emit_summary(jobset_id)
        return removed

    def clear(self, jobset_id, *, force: bool = False
                   ) -> List[JobRecord]:
        """[sync] 전 job 삭제 — remove_jobs와 동일 가드."""
        jobset_id = self._jsid(jobset_id)
        removed = self.jobsets.clear_jobs(jobset_id, force=force)
        self._emit_summary(jobset_id)
        return removed

    def _emit_summary(self, jobset_id: str) -> None:
        try:
            self.jobset_updated.emit(jobset_id, self.store.summary(jobset_id))
        except LsfmgrError:
            pass

    # ------------------------------------------------------------------
    # jobset 단위 submit (v9) — 전 job (재)제출
    # ------------------------------------------------------------------
    def can_submit(self, jobset_id: str) -> bool:
        """[sync, snapshot] submit_jobset 가능 여부 — job이 1건 이상 있고
        전원 비활성(CREATED/DONE/EXIT/SUBMIT_FAILED/LOST)이며 진행 중
        submit/kill이 없으면 True. GUI 버튼 활성화 판단용."""
        jobset_id = self._jsid(jobset_id)
        try:
            if (self.submitter.is_active(jobset_id)
                    or self.killer.is_active(jobset_id)):
                return False
            jobs = [r for r in self.get_jobs(jobset_id)
                    if r.array_index is None]
            return bool(jobs) and all(r.state.is_inactive for r in jobs)
        except LsfmgrError:
            return False

    def _submit_jobset(self, js: "JobSet",
                       pre_submit: Optional[Callable[[List[str]], bool]] = None,
                       post_process: Optional[Callable[[list], Any]] = None,
                       **kwargs: Any) -> JobSet:
        """jobset 전 job (재)제출 — mgr.submit(js, ...)의 구현."""
        jobset_id = self._jsid(js)
        if (self.submitter.is_active(jobset_id)
                or self.killer.is_active(jobset_id)):
            raise SubmitNotAllowedError(
                f"{jobset_id}: submit/kill 진행 중에는 submit할 수 없습니다",
                jobset_id=jobset_id)
        jobs = [r for r in self.get_jobs(jobset_id) if r.array_index is None]
        if not jobs:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 제출할 job이 없습니다", jobset_id=jobset_id)
        busy = [r.job_key for r in jobs if not r.state.is_inactive]
        if busy:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 활성(진행 중) job이 있어 submit 불가 — "
                f"{busy[:5]} (먼저 kill 하거나 완료를 기다리세요)",
                jobset_id=jobset_id, job_keys=busy)
        # post_process 무장 — 이번 실행이 전원 terminal에 도달하면 1회 발화.
        # (게이트 거부/취소 시 job은 CREATED로 남아 terminal이 아니므로 안 뜬다)
        if post_process is not None:
            self._post_process[jobset_id] = post_process
        else:
            self._post_process.pop(jobset_id, None)   # 재제출 시 이전 무장 해제
        opts = self.resolve_options(kwargs, context="submit")
        keyed = [(r.job_key, self._record_to_item(r)) for r in jobs]
        keys = [k for k, _ in keyed]
        if pre_submit is None:
            # handler 재무장 — 재실행 job의 진행 상태 리셋 (새 실행에서
            # start/end 주기가 다시 돌게). 게이트 경로는 통과 확정 후에.
            self.handlers.rearm(jobset_id, keys)
            self.submit_started.emit(jobset_id)
            self.submitter.resubmit_existing(jobset_id, keyed, opts)
            if opts.auto_poll:                    # AUTO-1
                self.start_polling(jobset_id, opts.poll_interval_s)
        else:
            # 게이트 경로 — rearm/폴링은 게이트 통과(pre_submit_finished True)
            # 후에 한다: 거부되면 재실행이 없는데 rearm하면 terminal 레코드에
            # _PENDING 핸들러가 남아 end 핸들러가 중복 발화한다
            self._pending_rearm[jobset_id] = keys
            if opts.auto_poll:
                self._pending_autopoll[jobset_id] = opts.poll_interval_s
            self.submitter.resubmit_existing(jobset_id, keyed, opts,
                                             pre_submit=pre_submit)
        return self.jobset(jobset_id)

    @staticmethod
    def _record_to_item(r: JobRecord):
        """레코드 → 제출 item 재구성. 경로는 job 단위 속성(rec.via_wrapper)
        으로 결정 — jobset 부착물로 판별하면 merge된 혼합 jobset에서
        오판한다."""
        if r.via_wrapper:
            return shlex.split(r.command)
        # bsub 경로 — 원 제출 옵션(queue/resources/outfile/env) 복원.
        # command만 다시 만들면 이 옵션들이 기본값으로 조용히 소실된다
        try:
            return (spec_from_json(r.spec_json) if r.spec_json
                    else JobSpec(command=r.command))
        except (ValueError, TypeError) as e:
            log.warning("spec_json 복원 실패(%s) — 옵션 없이 제출: %s",
                        e, r.job_key)
            return JobSpec(command=r.command)

    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """[sync, LSF 조회 포함] 손실 감지/복구 (FR-5.3) — blocking 주의."""
        jobset_id = self._jsid(jobset_id)
        return self.jobsets.detect_lost(jobset_id)

    def search_jobsets(self, *, tag: Optional[str] = None,
                       label: Optional[str] = None,
                       since: Optional[datetime] = None) -> List[JobSetRecord]:
        """[sync, snapshot] 세션 범위 검색."""
        return self.store.search(tag=tag, label=label, since=since)

    def close(self, jobset_id, *, force: bool = False) -> None:
        """[sync] 종결 (전원 terminal일 때) — 핸들도 파괴.
        전원 terminal이 아니면 예외 — polling/핸들은 건드리지 않고 유지.
        LSF group 정리(bgdel)는 worker 스레드에서 비동기 수행 (QT-1)."""
        jobset_id = self._jsid(jobset_id)
        js = self.jobsets.close_jobset(jobset_id, force=force,
                                       run_bgdel=False)   # 실패 시 여기서 예외
        self.polling.stop_polling(jobset_id)
        self.handlers.remove_all(jobset_id)
        self._poll_intervals.pop(jobset_id, None)
        self._post_process.pop(jobset_id, None)
        self._invalidate_handle(jobset_id)
        if js.lsf_group_paths:
            paths = list(js.lsf_group_paths)
            self._misc_pool.start(_CallTask(
                lambda: [self.command.bgdel(p) for p in paths]))

    def list_jobsets(self) -> List[JobSetRecord]:
        """[sync, snapshot] 현재 세션의 JobSet 목록."""
        return self.store.list_jobsets()

    # ------------------------------------------------------------------
    # 수명 관리
    # ------------------------------------------------------------------
    def _try_hook_about_to_quit(self) -> None:
        """QApplication보다 먼저 생성된 매니저용 — 앱이 생기면 aboutToQuit 연결."""
        if self._shutdown_done:
            self._hook_timer.stop()
            return
        app = QCoreApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self.shutdown)
            self._hook_timer.stop()

    def shutdown(self) -> None:
        """모든 스레드 안전 종료 (멱등, CS-8). 앱 종료 시 core dump/좀비 스레드를
        원천 차단하기 위해 aboutToQuit·atexit에 자동 연결되며, 명시 호출도 안전.
        한 컴포넌트가 실패해도 나머지 스레드는 반드시 join한다(best-effort)."""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        log.info("lsfmgr shutdown 시작")
        timer = getattr(self, "_hook_timer", None)
        if timer is not None:
            try:
                timer.stop()
            except RuntimeError:
                pass
        app = QCoreApplication.instance()
        if app is not None:
            try:
                app.aboutToQuit.disconnect(self.shutdown)
            except (TypeError, RuntimeError):
                pass                             # 미연결/이미 해제 — 무시
        try:
            atexit.unregister(self.shutdown)
        except Exception:                        # noqa: BLE001
            pass
        # 각 컴포넌트를 best-effort로 종료 — 하나가 예외를 던져도 나머지 스레드는
        # 반드시 join해야 좀비/core dump가 안 남는다.
        # misc_pool은 polling보다 먼저 drain — 단발 태스크가 폴링에 새
        # 작업을 거는 것을 막고 종료한다.
        for name, fn in (("handlers", self.handlers.shutdown),
                         ("submitter", self.submitter.shutdown),
                         ("misc_pool", lambda: self._misc_pool.waitForDone(-1)),
                         ("post_pool", lambda: self._post_pool.waitForDone(-1)),
                         ("polling", self.polling.shutdown),
                         ("killer", self.killer.shutdown),
                         ("store", self.store.close)):
            try:
                fn()
            except Exception:                    # noqa: BLE001 — CS-5/‏CS-8
                log.exception("shutdown 중 %s 종료 실패(계속)", name)
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
        self._maybe_post_process(jobset_id)

    def _maybe_post_process(self, jobset_id: str) -> None:
        """전원 terminal 도달 시 등록된 post_process를 worker에서 1회 실행.
        폴링/query_once 완료 감지의 공통 지점에서 호출된다 — 감지 즉시 무장을
        해제(pop)하므로 이어지는 폴링 사이클에서 중복 발화하지 않는다."""
        fn = self._post_process.get(jobset_id)
        if fn is None:
            return
        try:
            recs = self.store.get_jobs(jobset_id)
        except LsfmgrError:
            self._post_process.pop(jobset_id, None)   # jobset 소멸 — 무장 해제
            return
        if not recs or not all(r.state.is_terminal for r in recs):
            return
        self._post_process.pop(jobset_id, None)       # 한 번만
        self.post_processing_started.emit(jobset_id)
        self._post_pool.start(_PostProcessTask(self, jobset_id, fn, recs))

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
        # 단일 JobSet 위젯의 표 갱신용 — jsid 필터 없이 변경분 배치를 그대로.
        h.jobs_updated.emit(changed)
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


class _PostProcessTask(QRunnable):
    """전원 terminal 도달 후처리 콜백을 worker 스레드에서 1회 실행.
    반환값은 post_processing_finished로, 예외는 error_occurred +
    post_processing_finished(None)으로 통지 (CS-5 격리)."""

    def __init__(self, mgr: "LsfJobManager", jsid: str, fn, records: list):
        super().__init__()
        self.setAutoDelete(True)
        self.mgr = mgr
        self.jsid = jsid
        self.fn = fn
        self.records = records

    def run(self):
        try:
            result = self.fn(self.records)
        except Exception as e:               # noqa: BLE001 — CS-5
            log.exception("post_process 예외: %s", self.jsid)
            self.mgr.error_occurred.emit(self.jsid, f"post_process: {e!r}")
            self.mgr.post_processing_finished.emit(self.jsid, None)
            return
        log.info("post_process 완료 %s", self.jsid)
        self.mgr.post_processing_finished.emit(self.jsid, result)


def _with_pattern(js: JobSetRecord, pattern: str) -> JobSetRecord:
    if pattern in js.name_patterns:
        return js
    return dc_replace(js, name_patterns=js.name_patterns + [pattern])
