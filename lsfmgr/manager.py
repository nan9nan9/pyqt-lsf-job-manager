"""LsfJobManager — 앱이 사용하는 단일 진입점 (QObject Facade + 핸들 발급).

- 명령 일원화(v9): 모든 명령은 mgr.* 한 곳, JobSet 핸들은 조회+Signal 뷰.
  라이프사이클 자동화. 전역 Facade Signal(jsid 포함) 위에 핸들
  Signal이 같은 이벤트를 이중 발행한다.
- 옵션은 defaults → manager kwargs → call kwargs 3단 계층 (§1.2, options.py)

표기 규약: [async→Signal] = 즉시 반환·결과는 Signal /
[sync, snapshot] = 동기지만 Store 스냅샷만 조회 (LSF 호출 없음).
"""
from __future__ import annotations

import atexit
import logging
import shlex
from collections import Counter
from dataclasses import dataclass
from dataclasses import replace as dc_replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

from .command import LsfCommand, Runner
from .config import LsfConfig
from .errors import (
    JobEditNotAllowedError,
    JobNotFoundError,
    JobSetNotFoundError,
    LsfmgrError,
    RemoveJobSetNotAllowedError,
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
from .pacer import StatePacer
from .qt import (QCoreApplication, QObject, QRunnable, QThreadPool,
                 QTimer, Signal)
from .reports import KillProgress, SubmitProgress
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore
from .store.memory import InMemoryStore

log = logging.getLogger("lsfmgr.manager")


@dataclass(frozen=True)
class _PendingArm:
    """제출 사이클 1건의 보류 무장분 — submitter의 records_reset(착수 확정)
    에서 무장하고 gate_rejected(착수 없음 확정)에서 폐기한다.
    token은 사이클 정체성 — 큐에 남은 이전 사이클의 낡은 신호가 새 사이클의
    보류분을 건드리지 못하게 한다."""
    token: object
    keys: List[str]                          # rearm 대상 job_key
    post_process: Optional[Callable]
    poll_interval_s: Optional[float]         # None이면 자동 폴링 없음


class LsfJobManager(QObject):
    """Facade — 컴포넌트 조립 + Facade Signal + JobSet 핸들 발급."""

    # --- Low-level Facade Signal (v6 유지, 모두 jobset_id 포함) ---
    #
    # [submit 신호 계약] submit 시도 1건은 반드시 submit_finished로 끝난다
    # (shutdown/삭제된 jobset 등 접수 자체가 거부된 경우는 제외 — 신호 없음).
    # 하지만 submit_started는 **실제 착수(게이트 통과 후 do_launch)에서만** 발화
    # 하므로, finished 앞에 submit_started가 항상 오지는 않는다:
    #   - 정상 착수:        submit_started → … → submit_finished
    #   - 게이트 거부/예외:  pre_submit_started → pre_submit_finished(False) →
    #                        submit_finished   (submit_started 없음 — 거부는
    #                        pre_submit_finished(False)가 알린다)
    #   - born-cancelled:   submit_started → submit_finished(cancelled)
    #                        (게이트를 건너뛰어 pre_submit_* 가 없으므로,
    #                         finished가 신호 없이 홀로 나가지 않도록 최소 짝을 낸다)
    # ⇒ '진행 중 submit'을 추적하는 구독자는 submit_started/submit_finished를
    #    단순 카운터로 세면 안 된다(경로에 따라 카운터가 음수로 샌다). jobset_id를
    #    **집합**에 넣고(submit_started 또는 pre_submit_started에서 add) finished
    #    에서 discard하라 — 집합은 없는 키 discard/중복 add에 안전해 모든 경로에서
    #    정확하다.
    submit_started = Signal(str)               # jobset_id (착수 or born-cancelled)
    pre_submit_started = Signal(str)                # jobset_id — pre_submit 게이트 시작
    pre_submit_finished = Signal(str, bool)         # jobset_id, ok — 게이트 종료(True=통과)
    # jobset의 전 job이 terminal(DONE/EXIT/SUBMIT_FAILED/LOST)에 도달한 순간
    # 1회. post_process 등록 여부와 **무관**하게 LSF 상태만 보고 발화한다
    # (post_process를 걸었다면 이 신호 → post_processing_started 순서).
    # 재제출로 다시 활성이 되면 재무장돼 다음 완료에 또 발화한다.
    jobset_finished = Signal(str, dict)                 # jobset_id, 최종 요약
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

    def __init__(self, store: Optional[JobSetStore] = None,
                 config: Optional[LsfConfig] = None,
                 runner: Optional[Runner] = None,
                 parent: Optional[QObject] = None,
                 **kwargs: Any):
        """kwargs = §1.2 옵션 카탈로그의 ②(manager) 계층.
        config와 동시 지정 시 kwargs 우선."""
        super().__init__(parent)

        # --- 옵션 분리: manager 전용 / 공통(②) — 오타는 TypeError ---
        mgr_only = {k: kwargs.pop(k) for k in list(kwargs)
                    if k in MANAGER_ONLY_KEYS}
        mgr_only = validate_options(mgr_only, allowed=MANAGER_ONLY_KEYS,
                                    where="LsfJobManager()")   # 범위 검증
        shared = validate_options(kwargs, allowed=SHARED_KEYS,
                                  where="LsfJobManager()")

        # --- LsfConfig 구성 (기존 config 주입도 계속 지원) ---
        base_cfg = config or LsfConfig()
        # MANAGER_ONLY 키는 전부 LsfConfig 필드다 — 별도 사본 목록 없이 그대로
        # config에 싣는다 (목록 이중 유지가 드리프트를 만들던 잔재 제거, v10.1)
        cfg_updates = dict(mgr_only)
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
            "submit_timeout_s": cfg.submit_timeout_s,
        }
        self._defaults.update(shared)
        if cfg.retry_backoff > 1.0 and cfg.retry_backoff != 2.0:
            log.warning("LsfConfig.retry_backoff=%s — v7 옵션 체계의 expo "
                        "지수 밑은 2로 고정되어 배수가 그대로 반영되지 "
                        "않습니다", cfg.retry_backoff)

        # --- 컴포넌트 조립 ---
        self.command = LsfCommand(self.config, runner)
        self.jobsets = JobSetManager(self.store)
        self.querier = JobsetQuerier(self.store, self.command)

        # kill 우선권 게이트 — submit 사이클 등록과 kill barrier를
        # 한 lock으로 묶어, kill의 취소를 빠져나가는 늦은 submit 시작이
        # 구조적으로 불가능하게 한다 (lifecycle.py). submitter가 등록하고
        # killer가 kill_jobset의 scope로 barrier를 잡는다.
        from .lifecycle import SubmitGate
        self._gate = SubmitGate()

        from .submitter import BulkSubmitter
        self.submitter = BulkSubmitter(self.store, self.command,
                                       self.config, parent=self,
                                       gate=self._gate)
        self.submitter.progress.connect(self.submit_progress)
        self.submitter.finished.connect(self.submit_finished)
        self.submitter.error.connect(self.error_occurred)
        self.submitter.jobs_changed.connect(self._relay_jobs_changed)
        self.submitter.started.connect(self.submit_started)     # 게이트 통과 후
        self.submitter.pre_submit_started.connect(self.pre_submit_started)
        self.submitter.pre_submit_finished.connect(self.pre_submit_finished)
        # 착수 확정/무산 내부 신호 — 보류 무장분(_pending_arm) 처리 전용
        self.submitter.records_reset.connect(self._on_records_reset)
        self.submitter.gate_rejected.connect(self._on_gate_rejected)

        self.polling = PollingService(self.querier, parent=self)
        self.polling.updated.connect(self._on_poll_updated)
        self.polling.lost.connect(self.job_lost)
        self.polling.error.connect(self.error_occurred)

        self.killer = Killer(self.store, self.command, self.querier,
                             parent=self)
        self.killer.finished.connect(self.kill_finished)
        self.killer.progress.connect(self.kill_progress)
        self.killer.error.connect(self.error_occurred)

        # JobSet별 사용자 handler 주기 실행
        self.handlers = JobSetHandlerService(self.store, parent=self)
        self.handlers.finished.connect(self.handler_finished)

        # jobset별 마지막 polling interval — 편집/재제출 후 폴링 재개에 사용
        self._poll_intervals: Dict[str, float] = {}

        self._shutdown_done = False

        # post_process — jobset이 전원 terminal에 도달하면 1회 실행되는 후처리
        # 콜백. submit(post_process=fn)으로 등록되며 실제 무장은 착수 확정
        # (records_reset) 시점에 _pending_arm에서 승격된다. 완료 감지
        # (_on_poll_updated)에서 worker로 실행, post_processing_*로 통지.
        self._post_process: Dict[str, Callable] = {}
        self._post_pool = QThreadPool(self)
        self._post_pool.setMaxThreadCount(2)

        # jobset_finished 1회 latch — 전원 terminal을 이미 통지한 jobset.
        # 완료 감지가 다시 non-terminal을 보면(재제출/add_jobs로 job 추가) 자동
        # 해제되고, 착수 확정(records_reset)에서도 명시적으로 푼다 — 폴링
        # 없이 곧장 전원 SUBMIT_FAILED로 끝나는 사이클도 새로 통지되도록.
        self._finished_latch: Set[str] = set()

        # 제출 사이클별 보류 무장분 (무장/폐기 규칙은 _PendingArm docstring)
        self._pending_arm: Dict[str, _PendingArm] = {}

        # 상태 전이 표시 pacer — min_state_dwell_s>0일 때만. jobs_updated를
        # job별로 dwell만큼 띄워 발화해 순식간에 지나가는 전이(SUBMITTING→PEND,
        # EXIT→SUBMITTING)를 눈에 보이게 한다. 0(기본)이면 아예 만들지 않아
        # 발화 경로가 종전과 동일하다 (pacer.py의 계약 완화 주석 참고).
        self._pacer = (StatePacer(self.config.min_state_dwell_s,
                                  self.jobs_updated.emit, parent=self)
                       if self.config.min_state_dwell_s > 0 else None)

        # --- JobSet 핸들 계층 (v7) — Facade Signal 위에 이중 발행 ---
        self._handles: Dict[str, JobSet] = {}
        # 핸들 Signal 이름은 Facade와 동일 — relay 대상 attr명도 그대로
        # 내부 정산 slot을 핸들 relay보다 먼저 연결한다 — 같은 신호의 slot은
        # 연결 순서대로 실행되므로, kill의 EXIT 전이(jobs_updated)가 핸들
        # kill_finished보다 먼저 나가야 finished-last 계약이 핸들 계층에서도
        # 유지된다 (submit 경로는 submitter가 emit 순서로 보장).
        self.kill_finished.connect(self._emit_updates_after_kill)
        self.submit_progress.connect(self._handle_relay("submit_progress"))
        self.jobset_updated.connect(self._handle_relay("jobset_updated"))
        self.kill_started.connect(self._handle_relay("kill_started"))
        self.kill_finished.connect(self._handle_relay("kill_finished"))
        self.kill_progress.connect(self._handle_relay("kill_progress"))
        self.error_occurred.connect(self._handle_relay("error_occurred"))
        self.handler_finished.connect(self._handle_relay("handler_finished"))
        self.pre_submit_started.connect(self._handle_relay("pre_submit_started"))
        self.pre_submit_finished.connect(self._handle_relay("pre_submit_finished"))
        self.jobset_finished.connect(self._handle_relay("jobset_finished"))
        self.post_processing_started.connect(self._handle_relay("post_processing_started"))
        self.post_processing_finished.connect(self._handle_relay("post_processing_finished"))
        self.submit_finished.connect(self._h_finished)
        self.submit_finished.connect(self._emit_summary_after_submit)
        self.jobs_updated.connect(self._h_jobs_updated)

        # 스레드 좀비/‏core dump 원천 차단 — shutdown을 아래 3중으로 보장.
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
    # 옵션 해석
    # ------------------------------------------------------------------
    def resolve_options(self, call_kwargs: Dict[str, Any],
                        context: str = "submit") -> Options:
        """③call kwargs를 defaults(①+②) 위에 merge — 단일 해석 지점."""
        return resolve_options(self._defaults, call_kwargs, context=context)

    # ------------------------------------------------------------------
    # High-level submit (v7 §1.1) — JobSet 핸들 반환
    # ------------------------------------------------------------------
    def submit(self, js, *,
               only: Optional[Sequence] = None,
               pre_submit: Optional[Callable[[List[str]], bool]] = None,
               post_process: Optional[Callable[[list], Any]] = None,
               **kwargs: Any) -> JobSet:
        """[async→Signal] jobset 제출 — **유일한 제출 경로**.

        기본은 jobset의 **전 job** (재)제출이다: 대상이 전원 비활성(CREATED/
        DONE/EXIT/SUBMIT_FAILED/LOST)이어야 하며 활성이 있으면 LsfmgrError —
        can_submit(js)로 선확인. 리셋 후 재실행되므로 같은 jobset/job_key가
        전이된다(핸들·테이블 연속).

        only=[ref, ...]: 지정하면 **그 job들만** 제출한다. ref는 job_key /
        job_id(int) 중 아무거나 — remove_jobs·set_user_data의 ref와
        같은 규칙이다. 나머지 job은 상태도 레코드도 건드리지 않으므로,
        **다른 job이 RUN 중이어도** 선택분만 돌릴 수 있다(전체 제출과 달리
        '전원 비활성' 가드가 선택분에만 걸린다). 실패분만 재실행하거나 GUI
        테이블에서 고른 행만 돌릴 때 쓴다:

            failed = [r.job_key for r in js.failed_jobs]
            mgr.submit(js, only=failed)

        빈 리스트는 SubmitNotAllowedError — '아무것도 안 함'을 조용히
        성공으로 처리하면 호출자 실수가 묻힌다.

        흐름:

            js = mgr.create_jobset(
                ["customwrapper_sub -q normal run_0.sp",
                 "customwrapper_sub -q long tb_1.v"],
                job_keys=["run_0", "tb_1"], label="sweep")
            mgr.submit(js, workers=8)

        pre_submit(commands)->bool: 지정 시 제출 전에 커맨드 리스트 전체를
        게이트 워커에서 검사 — **리셋 이전**이라 False/예외면 레코드
        원상 유지. 신호: (pre_submit_started → pre_submit_finished(ok)) →
        submit_started → jobs_updated/progress → submit_finished.

        post_process(records)->Any: 지정 시 **jobset의 전 job이 terminal**에
        도달하면(폴링/query_once로 완료 감지) worker에서 1회 실행. only로
        일부만 제출했어도 판정 대상은 jobset 전체다 — 안 돌린 job이 아직
        RUN이면 그것까지 끝나야 발화한다. 인자는 최종
        JobRecord 목록(성공/실패 혼재 가능 — DONE/EXIT/SUBMIT_FAILED/LOST 무관
        전원 terminal이면 실행). 반환값은 post_processing_finished로 전달.
        신호: post_processing_started → post_processing_finished(result).
        ※ pre_submit·post_process 콜백 모두 worker 스레드 실행 — GUI 접근 금지.
        옵션(kwargs): workers/max_retry/rate_limit_per_s/auto_poll/
        poll_interval_s/submit_timeout_s 등 (§1.2)."""
        return self._submit_jobset(js, only=only, pre_submit=pre_submit,
                                   post_process=post_process, **kwargs)

    @staticmethod
    def _jsid(js) -> str:
        """명령 인자 정규화 — JobSet 핸들 또는 jobset_id 문자열을 받는다.
        모든 명령은 manager 한 곳에만 있고(v9 통일), 핸들은 조회+신호 전용."""
        if isinstance(js, JobSet):
            js._check_open()
            return js._jobset_id
        return js

    def jobset(self, jobset_id: str) -> JobSet:
        """[sync, snapshot] JobSet 핸들 재획득 (복원/검색 결과에서).
        삭제된(remove_jobset) jobset은 레코드가 없으므로
        JobSetNotFoundError — 새 열린 핸들이 발급될 여지가 없다."""
        handle = self._handles.get(jobset_id)
        if handle is not None:
            return handle
        self.store.get_jobset(jobset_id)         # 존재 검증
        handle = JobSet(self, jobset_id)
        self._handles[jobset_id] = handle
        return handle

    # ------------------------------------------------------------------
    # Low-level submit (v6 유지)
    # ------------------------------------------------------------------
    def cancel_submit(self, jobset_id: str) -> None:
        """[async→Signal] 진행 중 submit 중단 — submit된 job은 유지."""
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
        self._poll_intervals[jobset_id] = eff    # 편집/재제출 재개용 기억
        self.polling.start_polling(jobset_id, eff)

    def stop_polling(self, jobset_id: str) -> None:
        """[async→Signal] polling 중지."""
        jobset_id = self._jsid(jobset_id)
        # 재개 기억도 지운다 — 재제출(_on_records_reset)이 옛 interval로
        # 마음대로 되살리지 않게.
        # ※ job 편집(add/replace/upsert)은 예외다: 편집 후 관찰할 job이 남으면
        #   기본 interval
        #   로라도 폴링을 다시 켠다(관찰 대상이 있는데 멈춰 있으면 handler가
        #   조용히 죽는다). 껐다는 사실보다 볼 것이 있다는 사실이 우선한다.
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

    def total_summary(self) -> Dict[str, Any]:
        """[sync, snapshot] 전체 jobset을 가로지른 상태별 카운트 합산.

        각 jobset의 summary()를 합산한다. "total"은 intended_count 합계,
        나머지 키는 JobState.value별 개수 합계. LSF 호출 없이 Store 스냅샷만
        읽으므로, 최신 LSF 상태는 각 jobset이 폴링되고 있어야 반영된다.

        list_jobsets() 스냅샷과 이후 summary() 사이에 다른 스레드가 그 jobset을
        remove_jobset으로 지울 수 있다(find_jobs와 동일한 경합) — 사라진 것은
        건너뛴다."""
        agg: Counter = Counter()
        for js in self.store.list_jobsets():
            try:
                agg.update(self.store.summary(js.jobset_id))
            except JobSetNotFoundError:
                continue
        return dict(agg)

    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        """[sync, snapshot] job 상세 (Store 조회)."""
        jobset_id = self._jsid(jobset_id)
        return self.store.get_jobs(jobset_id, states)

    # ------------------------------------------------------------------
    # Kill
    # ------------------------------------------------------------------
    def kill(self, jobset_id, *,
                    only_state: Optional[JobState] = None,
                    verify: Optional[bool] = None) -> None:
        """[async→Signal] JobSet kill — 결과는 kill_finished.
        verify 미지정 시 verify_kill 옵션(②) 적용.

        MC 분류 kill(cluster별 env source)은 생성자 옵션
        `LsfJobManager(cluster_envpaths={클러스터명: cshrc경로})`로 켠다 —
        앱 환경 속성이라 호출마다 주지 않는다.

        **kill은 겨냥한 job의 제출에 항상 우선권을 갖는다** — 그 job이 제출
        중이면 멈추고(미착수분 CREATED 복귀), 이미 wrapper가 돌면 job_id
        확보를 기다렸다가 죽인다. 대상 아닌 job의 제출은 계속된다.
        jobset의 제출 자체를 통째로 멈추려면 `mgr.cancel_submit(js)`를
        먼저 부른다(취소는 되돌려지지 않으므로 순서만 지키면 된다)."""
        jobset_id = self._jsid(jobset_id)
        if self._shutdown_done:
            # shutdown 후 kill은 join되지 않는 worker를 만들고 kill_started만
            # 발화된 채 finished가 영영 안 와 UI가 고착된다 — no-op으로 무시
            log.warning("shutdown 후 kill 요청 무시: %s", jobset_id)
            return
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        # kill 우선권(SubmitGate barrier) — 범위는 **겨냥한 job**이다:
        #   전체 kill        → None (= 이 jobset 전 job)
        #   부분 kill        → 지금 그 상태인 job의 key
        # barrier와 등록이 게이트의 한 lock 아래 원자적이라, "kill의 취소를
        # 빠져나가는 늦은 제출"이 타이밍이 아닌 규칙으로 막힌다: 먼저 등록된
        # 사이클은 barrier가 넘겨받아 취소·대기하고, 나중 것은 등록이 거부된다.
        # 재취소 루프가 필요 없는 이유다.
        keys: Optional[List[str]] = None
        if only_state is not None:
            try:
                keys = [r.job_key for r in
                        self.store.get_jobs(jobset_id, states={only_state})]
            except LsfmgrError:
                # jobset이 없다 — 어차피 killer가 같은 예외로 보고한다.
                # 여기서 터뜨리면 kill_started/finished 짝이 깨진다.
                keys = []
        scope = self._gate.kill_scope(jobset_id, keys)
        queued = self._launch_kill(scope, lambda: self.killer.kill_jobset(
            jobset_id, only_state=only_state, verify=verify, scope=scope))
        if queued:
            self.kill_started.emit(jobset_id)

    @staticmethod
    def _launch_kill(scope, queue_fn: Callable[[], bool]) -> bool:
        """kill 우선권 barrier의 수명 관리 **단일 지점** — kill()/kill_jobs()
        공용.

        barrier↑ + 취소(begin)는 **접수 스레드에서 즉시** 한다(논블로킹).
        killer worker 차례로 미루면 그동안 제출된 job이 전부 '제출됐다가 곧
        죽는' 낭비가 된다. 정지 대기(blocking)만 worker의 scope.acquire()가
        이어받는다.

        barrier를 올린 뒤 task를 못 띄우면(shutdown 경합/예외) 아무도
        release하지 않아 그 jobset의 제출이 **영구 차단**된다 — 미큐잉/예외
        경로에서 반드시 회수한다.

        반환: 실제 큐잉 여부. True일 때만 caller가 kill_started를 발화한다 —
        killer 등록(동기) **이후** 발행이라 kill_started slot에서
        is_killing()/kill_snapshot()을 pull해도 True/값이 나오고(신호-pull
        일치), shutdown 경합 no-op이면 kill_finished도 안 오므로 started를
        내지 않아 UI의 영구 'killing' 고착을 막는다(started/finished 짝)."""
        if scope is not None:
            scope.begin()
        try:
            queued = queue_fn()
        except BaseException:
            if scope is not None:
                scope.release()
            raise
        if not queued and scope is not None:
            scope.release()
        return queued

    def kill_jobs(self, job_ids_or_jobset=None,
                  job_keys: Optional[Sequence[str]] = None, *,
                  jobset_id: Optional[str] = None,
                  verify: Optional[bool] = None) -> None:
        """[async→Signal] 개별 job kill (chunking 자동).

        세 형태를 받는다:
          - kill_jobs(js, [job_key, ...]) — jobset의 선택 job만 kill (GUI
            테이블 선택 행). array element는 "id[idx]"로 변환돼 그 element만
            죽는다(parent id로 죽이면 나머지 element까지 전부 kill됨).
          - kill_jobs(job_keys=[...]) — **jobset 핸들 없이** key만으로 kill.
            key로 레코드를 전역 검색하고, 전부 한 jobset 소속이면 그 jobset을
            컨텍스트로 자동 채택한다(verify·신호·우선권이 그대로 동작).
          - kill_jobs([id 또는 "id[idx]", ...], jobset_id=...) — 원시 id 기반.
        jobset 컨텍스트가 있으면 optimistic EXIT 전이·verify가 켜지고 결과가
        핸들 kill_finished로도 중계된다.

        **선택 대상이 제출 중이면 그 job의 제출은 항상 멈춘다** — 아직
        wrapper를 안 돌린 대상은 CREATED로 되돌리고, 이미 도는 대상은 끝나길
        기다렸다가 확보된 job_id로 죽인다. 안 그러면 job_id가 없는 대상이
        key→id 해석에서 빠져 bkill이 안 나가고, 그 job은 제출을 마쳐 PEND→RUN
        으로 살아난다. 대상 **아닌** job의 제출은 계속된다 — jobset의 제출을
        통째로 멈추려면 `mgr.cancel_submit(js)`를 먼저 부른다.

        원시 id 경로는 대상이 이미 job_id를 가진(=제출이 끝난) job이므로
        제출 우선권을 걸지 않는다."""
        if self._shutdown_done:
            log.warning("shutdown 후 kill_jobs 요청 무시")
            return
        if verify is None:
            verify = bool(self._defaults.get("verify_kill", False))
        # 문자열은 시퀀스라 list()가 **글자 단위로 분해**된다 — jobset_id나
        # key 하나를 실수로 넘기면 "js_2026..." 이 한 글자짜리 target이 되어
        # 무관한 job id에 bkill이 나간다. 조용히 통과시키지 않는다.
        if isinstance(job_keys, str):
            raise TypeError("kill_jobs: job_keys는 문자열이 아니라 "
                            f"목록이어야 한다 (got {job_keys!r})")
        if isinstance(job_ids_or_jobset, str) and job_keys is None:
            raise TypeError(
                "kill_jobs: 첫 인자 문자열은 job id 목록이 아니다 — jobset "
                f"이면 job_keys와 함께 넘기고, 원시 id면 목록으로 넘겨라 "
                f"(got {job_ids_or_jobset!r})")
        if job_keys is not None or isinstance(job_ids_or_jobset, JobSet):
            if job_keys is None:
                # 선택 kill인데 선택이 없다 — 조용한 no-op은 위험하다
                # (호출자는 뭔가 죽었다고 믿는다). 빈 선택은 job_keys=[]로
                # 명시해야 한다.
                raise TypeError(
                    "kill_jobs: jobset을 넘겼으면 job_keys도 필요하다 "
                    "(전체 kill은 mgr.kill(js), 빈 선택은 job_keys=[])")
            owner = (job_ids_or_jobset if job_ids_or_jobset is not None
                     else jobset_id)
            self._kill_selected(owner, list(job_keys), verify)
            return
        # 원시 id 경로 — caller가 문자열을 그대로 넘긴다. array element를
        # "id[idx]"로 지정하는데 그 array를 monitor가 (id, None) 단일
        # 레코드로 접은 상태면, verify는 element 단위 생사를 판정할 수 없어
        # (집계 레코드=여러 element의 합) 그 target을 잔존 집계에서 제외한다.
        # 즉 접힌 array의 특정 element만 겨냥한 raw kill은 verify가 과소집계
        # 할 수 있다 — 접힌 array는 bare id(전체)로 kill·verify하거나,
        # jobset+job_key 경로(위)를 쓰면 안전하다(None 레코드에 bare id 생성).
        # 대상이 이미 job_id를 가진(=제출이 끝난) job이므로 제출 우선권
        # (scope)은 걸지 않는다.
        if job_ids_or_jobset is None:
            # 셋 중 어느 형태도 아니다(kill_jobs() / kill_jobs(jobset_id=..)).
            # 그대로 두면 list(None)의 "'NoneType' object is not iterable"만
            # 나와 어느 인자를 빠뜨렸는지 알 수 없다 — 위 오용 검사들과
            # 같은 격으로 쓸 형태를 짚어 준다.
            raise TypeError(
                "kill_jobs: kill 대상이 없다 — 원시 id 목록을 첫 인자로 "
                "넘기거나(kill_jobs([123, ...], jobset_id=...)), 선택 kill은 "
                "job_keys=로 넘겨라. jobset 전체 kill은 mgr.kill(js)")
        jsid = self._jsid(jobset_id) if jobset_id is not None else ""
        self._queue_kill_jobs(jsid, ids=list(job_ids_or_jobset), keys=None,
                              verify=verify, scope=None)

    def _kill_selected(self, owner, keys: List[str], verify: bool) -> None:
        """선택(key) kill — 소유 jobset 해석 + 제출 우선권 scope 부여.

        owner는 JobSet 핸들/jobset_id 문자열/None(핸들 없는 key 호출).
        key→id 해석은 killer worker에서 한다 (barrier 이후 — 제출 중이던
        job의 id까지 잡는다). 여기서 미리 풀면 그 job이 kill을 빠져나간다."""
        if owner is not None:
            jsid = self._jsid(owner)
        else:
            # 핸들 없는 key 호출 — 소유 jobset을 역추적한다. 여러 jobset에
            # 걸치면 jobset별로 나눠 각각 정식 kill을 건다(컨텍스트를 ""로
            # 뭉개면 verify/신호/우선권/진행표시가 전부 무력해진다).
            owners: Dict[str, List[str]] = {}
            for r in self.store.find_jobs_by_keys(set(keys)):
                owners.setdefault(r.jobset_id, []).append(r.job_key)
            if not owners:
                log.warning("kill_jobs: 알 수 없는 job_key %d건 — 무시",
                            len(keys))
                return
            if len(owners) > 1:
                for oid, okeys in owners.items():
                    self._kill_selected(oid, okeys, verify)
                return
            jsid, keys = next(iter(owners.items()))
        # 선택 kill의 우선권 — 범위는 **선택한 key**다. 제출 중(job_id 미확보)인
        # 대상이 key→id 해석에서 빠지면 bkill이 안 나가고 그 job은 제출을 마쳐
        # PEND→RUN으로 살아난다("kill했는데 안 죽는다"). barrier도 이 범위에만
        # 걸려, 대상 아닌 job의 제출과 그들의 새 제출 사이클은 그대로다.
        scope = self._gate.kill_scope(jsid, keys) if jsid else None
        self._queue_kill_jobs(jsid, ids=None, keys=keys,
                              verify=verify, scope=scope)

    def _queue_kill_jobs(self, jsid: str, *, ids, keys, verify, scope) -> None:
        """kill_jobs 큐잉 + 착수 통지 — barrier 수명은 _launch_kill이 관리."""
        queued = self._launch_kill(scope, lambda: self.killer.kill_jobs(
            ids, job_keys=keys, verify=verify, jobset_id=jsid or "",
            scope=scope))
        if queued:                       # 실제 큐잉일 때만 (shutdown 경합 제외)
            # 전역 kill(jsid="")도 발화한다 — started/finished 짝 계약은
            # jobset 유무와 무관하다(안 그러면 전역 kill을 건 UI가 완료만
            # 받고 착수를 못 받아 진행 표시를 못 켠다). 핸들 중계는 jsid가
            # 빈 값이면 자연히 no-op.
            self.kill_started.emit(jsid)   # killer 등록 후 (pull 일치)

    def create_jobset(self, commands: Sequence = (), *,
                      job_keys: Optional[Sequence[str]] = None,
                      user_datas: Optional[Sequence[Optional[dict]]] = None,
                      work_dir: Optional[str] = None,
                      work_dirs: Optional[Sequence[Optional[str]]] = None,
                      label: str = "", tags: Sequence[str] = (),
                      intended_count: int = 0) -> JobSet:
        """[sync] JobSet 생성 — job까지 함께 만들고 핸들 즉시 반환 (CREATED).

        생성 후 job을 더 넣거나 바꾸려면 편집 3형제를 쓴다 —
        `add_jobs`(추가) / `replace_jobs`(교체) / `upsert_jobs`(있으면 교체).
        흐름:

            js = mgr.create_jobset(
                ["customwrapper_sub -i a.sp", "customwrapper_sub -i b.sp"],
                job_keys=["a", "b"], user_datas=[{"run": "..."}, None],
                label="sweep")
            if mgr.can_submit(js):
                mgr.submit(js, workers=8)     # 전 job (재)제출

        commands 각 항목 (v10: wrapper 단일 경로 — bsub 조립 경로 삭제):
          - 토큰 리스트(argv) → 그대로 실행
          - 문자열           → shlex 분해 후 실행
        job_keys: 각 job의 키 — **필수**다. 앱이 정하는 이름이고 jobset 안에서
        유일해야 한다. replace_jobs/upsert_jobs가 교체 대상을 찾고,
        remove_jobs·set_user_data·submit(only=)의 ref가 되며, 재제출해도
        유지돼 GUI 표의 행 정체성이 된다.
        user_datas: job별 사용자 정의 dict (JSON 직렬화 가능) — 보존만.
        work_dir: 이 jobset 전 job의 제출 작업 디렉토리(단일 값) — 전 job이
        이 cwd에서 실행된다. work_dirs와 **동시 지정 불가**(둘 중 하나).
        work_dirs: job별 제출 작업 디렉토리 — 제출 subprocess를 그 cwd에서
        실행한다(wrapper 경로도 적용; bsub -cwd를 못 주는 wrapper의 실행
        디렉토리 지정 수단). None인 항목은 부모 cwd. 재제출에도 보존.
        work_dir/work_dirs 미지정 시 부모(GUI) 프로세스의 cwd.
        job_keys/user_datas/work_dirs는 commands와 같은 길이(생략 시 전부 None).
        commands가 비면 **빈 jobset** — 이후 add_jobs로 채운다.
        생성 즉시 jobs_updated/jobset_updated가 발행돼 표가 갱신된다."""
        if isinstance(tags, str):             # 편의: 단일 태그 문자열 허용
            tags = [tags]
        rec = self.jobsets.local_create_jobset(
            intended_count, label=label, tags=tags)
        jsid = rec.jobset_id
        items = list(commands)
        if items:
            try:
                records = self._build_job_records(
                    jsid, items, job_keys, user_datas,
                    work_dir, work_dirs)
                out = self.jobsets.local_create_jobs(jsid, records)
            except BaseException:
                # 검증 실패(길이 불일치·빈 커맨드·job_key 중복 등) — 방금
                # 넣은 jobset 레코드를 되돌린다. 안 하면 핸들도 없는 유령
                # 빈 jobset이 list/search에 영구 잔류한다.
                self.store.store_delete_jobset(jsid)
                raise
            self._relay_jobs_changed(jsid, list(out))     # 표 즉시 갱신
        return self.jobset(jsid)

    def _build_job_records(self, jsid: str, items: list,
                           job_keys: Optional[Sequence[str]],
                           user_datas: Optional[Sequence[Optional[dict]]],
                           work_dir: Optional[str],
                           work_dirs: Optional[Sequence[Optional[str]]]
                           ) -> List[JobRecord]:
        """commands → CREATED JobRecord 목록 (create_jobset / _edit_jobs 공용).
        submit_cwd: work_dir(전체 단일) 또는 work_dirs(job별) — 동시 지정 불가.
        job_keys: job별 키 — **필수**이며 jobset 안에서 유일해야 한다."""
        if work_dir is not None and work_dirs is not None:
            raise ValueError(
                "work_dir와 work_dirs는 동시에 지정할 수 없습니다(둘 중 하나)")
        if job_keys is None:
            raise ValueError(
                "job_keys는 필수입니다 — 각 job의 키를 앱이 정해야 합니다"
                " (commands와 같은 길이)")
        keys_in = list(job_keys)
        uds = list(user_datas) if user_datas is not None else [None] * len(items)
        # work_dir 단일 지정이면 전 job에 적용, 아니면 work_dirs(job별) 사용
        wds = (list(work_dirs) if work_dirs is not None
               else [work_dir] * len(items))
        if (len(keys_in) != len(items) or len(uds) != len(items)
                or len(wds) != len(items)):
            raise ValueError(
                "job_keys/user_datas/work_dirs 길이가 commands와 다릅니다")

        # job_key는 **앱이 정한다** — 자동 생성하지 않는다. 라이브러리가 임의로
        # 이름을 붙이면 그 job을 나중에 가리킬 방법이 앱에 없고(replace/remove/
        # only의 ref가 전부 이 키다), 재제출 뒤에도 같은 job인지 앱이 스스로 알
        # 수 없다. 빠뜨리면 조용히 넘어가지 않고 여기서 막는다.
        # 기존 키와의 충돌은 여기서 보지 않는다 — 겹치는 것이 정상인 경로
        # (replace/upsert)가 있어, 그 판단은 정책을 아는 계층(local_edit_jobs)의
        # 몫이다. 여기서는 한 호출 안의 중복만 잡는다(어느 정책에서도 오류).
        in_batch = set()
        for k in keys_in:
            if not isinstance(k, str) or not k.strip():
                raise ValueError(
                    f"job_key는 비어있지 않은 문자열이어야 합니다: {k!r}"
                    f" (job_keys는 commands와 같은 길이로 반드시 지정)")
            if k in in_batch:
                raise ValueError(f"job_key 중복(한 호출 안에서): {k!r}")
            in_batch.add(k)

        records = []
        for item, key, ud, cwd in zip(items, keys_in, uds, wds):
            argv = (shlex.split(item) if isinstance(item, str)
                    else [str(t) for t in item])
            if not argv:
                raise ValueError("빈 커맨드는 job으로 만들 수 없습니다")
            records.append(JobRecord(
                job_id=None, array_index=None, jobset_id=jsid,
                job_key=key, state=JobState.CREATED,
                command=shlex.join(argv),
                user_data=ud, submit_cwd=cwd))
        return records

    def set_user_data(self, jobset_id: str, ref, user_data: Optional[dict]
                    ) -> JobRecord:
        """[sync] job의 user_data 교체 — ref는 job_key(str) 또는 job_id(int).
        갱신 레코드를 jobs_updated로 발행한다."""
        jobset_id = self._jsid(jobset_id)
        rec = self._find_job(jobset_id, ref)
        new = self.store.update_job(dc_replace(rec, user_data=user_data))
        self._relay_jobs_changed(jobset_id, [new])
        return new

    def _find_job(self, jobset_id: str, ref) -> JobRecord:
        """job_key(str) / job_id(int) 로 단일 job 찾기.

        해석은 _resolve_refs 하나로 통일한다 — 직접 훑으면 job_id가 array
        parent와 element에 겹칠 때 '먼저 나온 것'을 골라, 같은 ref가 API마다
        다른 job을 가리킨다(레코드 삽입 순서에 좌우된다)."""
        return self._resolve_refs(jobset_id, [ref])[0]

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

    # ------------------------------------------------------------------
    # job 편집 3형제 — 하려는 일이 이름에 드러난다
    #   add_jobs     : 추가 전용 (job_key가 이미 있으면 ValueError)
    #   replace_jobs : 교체 전용 (job_key가 없으면 JobNotFoundError)
    #   upsert_jobs  : 있으면 교체, 없으면 추가
    # 셋 다 도메인에선 local_edit_jobs 한 몸통을 공유한다 — 가드와
    # intended_count 규칙이 갈릴 수 없게.
    # ------------------------------------------------------------------
    def add_jobs(self, jobset_id, commands: Sequence, *,
                 job_keys: Optional[Sequence[str]] = None,
                 user_datas: Optional[Sequence[Optional[dict]]] = None,
                 work_dir: Optional[str] = None,
                 work_dirs: Optional[Sequence[Optional[str]]] = None
                 ) -> List[JobRecord]:
        """[sync] 기존 jobset에 job을 **추가**한다 (CREATED).

        인자는 create_jobset과 같은 모양이다. job_key가 이미 있으면
        ValueError — 교체할 생각이면 replace_jobs/upsert_jobs를 쓴다.
        추가분은 jobs_updated/jobset_updated로 발행돼 표가 갱신된다.
        제출은 별도다: 이어서 mgr.submit(js)를 부르면 **전 job**이 재제출된다."""
        return self._edit_jobs(jobset_id, commands, policy="add",
                               job_keys=job_keys, user_datas=user_datas,
                               work_dir=work_dir, work_dirs=work_dirs)

    def replace_jobs(self, jobset_id, commands: Sequence, *,
                     job_keys: Sequence[str],
                     user_datas: Optional[Sequence[Optional[dict]]] = None,
                     work_dir: Optional[str] = None,
                     work_dirs: Optional[Sequence[Optional[str]]] = None,
                     force: bool = False) -> List[JobRecord]:
        """[sync] 기존 job의 내용을 **교체**한다 — job_key로 지정(필수).

        키가 곧 행의 정체성이므로 GUI 표의 행이 그대로 이어진다. 수정한
        커맨드로 재실행할 때 쓴다: replace_jobs(...) → submit(js).
        대상이 없으면 JobNotFoundError, 대상이 활성이면
        JobEditNotAllowedError(force=True면 강제 — LSF 정리는 caller 책임)."""
        return self._edit_jobs(jobset_id, commands, policy="replace",
                               job_keys=job_keys, user_datas=user_datas,
                               work_dir=work_dir, work_dirs=work_dirs,
                               force=force)

    def upsert_jobs(self, jobset_id, commands: Sequence, *,
                    job_keys: Sequence[str],
                    user_datas: Optional[Sequence[Optional[dict]]] = None,
                    work_dir: Optional[str] = None,
                    work_dirs: Optional[Sequence[Optional[str]]] = None,
                    force: bool = False) -> List[JobRecord]:
        """[sync] job_key가 있으면 교체, 없으면 추가 (일괄 반영).

        "이 목록을 jobset에 반영해라"를 한 번에 표현한다 — 어느 것이 이미
        있는지 caller가 분류하지 않아도 된다. 의도가 '추가만'/'교체만'으로
        분명하면 add_jobs/replace_jobs를 쓰는 편이 실수를 잡아준다."""
        return self._edit_jobs(jobset_id, commands, policy="upsert",
                               job_keys=job_keys, user_datas=user_datas,
                               work_dir=work_dir, work_dirs=work_dirs,
                               force=force)

    def _edit_jobs(self, jobset_id, commands: Sequence, *, policy: str,
                   job_keys=None, user_datas=None, work_dir=None,
                   work_dirs=None, force: bool = False) -> List[JobRecord]:
        """add/replace/upsert_jobs의 구현 — 정책만 다르다."""
        jobset_id = self._jsid(jobset_id)
        if (self.submitter.is_active(jobset_id)
                or self.killer.is_active(jobset_id)):
            # 진행 중 사이클은 착수 시점의 job 스냅샷으로 돈다 — 그 사이
            # 목록을 바꾸면 '추가했는데 이번 제출엔 없다'는 혼란만 남는다.
            raise JobEditNotAllowedError(
                f"{jobset_id}: submit/kill 진행 중에는 job 목록을 바꿀 수"
                f" 없습니다 (완료를 기다리거나 먼저 kill 하세요)",
                jobset_id=jobset_id)
        items = list(commands)
        if not items:
            return []
        records = self._build_job_records(jobset_id, items, job_keys,
                                          user_datas, work_dir, work_dirs)
        changed = self.jobsets.local_edit_jobs(jobset_id, records,
                                               policy=policy, force=force)
        if changed:
            # 교체분은 job_key가 유지되므로 추적 장부를 리셋해야 새 내용에
            # handler/LOST 판정이 정상 동작한다. 추가분은 장부가 없어 no-op.
            self._rearm_tracking(jobset_id, [r.job_key for r in changed])
            self._relay_jobs_changed(jobset_id, list(changed))
        self._resume_polling_if_watchable(jobset_id)
        return changed

    def _rearm_tracking(self, jobset_id: str, keys: List[str]) -> None:
        """재실행/교체될 job의 추적 장부 무효화 — 항상 **쌍으로** 리셋한다.

        - handler 진행 상태(rearm): 옛 _FINISHED가 남으면 같은 key의 새
          실행에 handler가 영영 침묵한다.
        - LOST 미발견 스트릭(forget): querier는 사이클마다 스트릭을
          재구축하지만, 조회 대상(on-LSF)이 하나도 없는 사이클은 query()가
          조기 반환해 옛 값이 그대로 남는다 — 재제출 사이가 정확히 그
          상태다(전원 terminal/CREATED). 그대로 두면 새 실행이 옛 실행의
          미발견 횟수를 물려받아, 제출 직후 등록 지연으로 한 번만 안 보여도
          LOST(되돌릴 수 없는 terminal)가 확정된다."""
        self.handlers.rearm(jobset_id, keys)
        self.querier.forget(jobset_id, keys)

    def _resume_polling_if_watchable(self, jobset_id: str) -> None:
        """관찰할 job(비terminal)이 생겼으면 폴링을 (재)시작한다.

        job 목록이 늘거나 바뀌는 경로의 공통 뒷정리. 전원 terminal이 돼
        폴링이 **자동 중지**된 뒤(monitor._maybe_auto_stop은 서비스 타이머만
        끄고 아래 기억은 남긴다) job이 추가되면, 관찰할 일이 다시 생겼는데도
        폴링이 죽은 채로 남는다 — 폴링 tick에 tie된 handler가 새 job에 영영
        침묵한다(리뷰 사이클 15).
        interval은 이 jobset의 기억, 없으면 기본값.
        ※ 이 재개는 "사용자가 stop_polling으로 끈 폴링은 되살리지 않는다"는
          계약보다 우선한다 — 관찰 대상이 있는데 조용히 멈춰 있는 쪽이 더
          나쁜 실패이기 때문. 볼 것이 없어지면 _maybe_auto_stop이 다시 끈다."""
        try:
            watchable = any(not r.state.is_terminal
                            for r in self.store.get_jobs(jobset_id))
        except LsfmgrError:                      # jobset 소멸
            return
        if watchable:
            self.start_polling(jobset_id, self._poll_intervals.get(jobset_id))

    def remove_jobs(self, jobset_id, refs: Sequence, *,
                    force: bool = False) -> List[JobRecord]:
        """[sync] 지정한 job들을 삭제 — refs는 job_key(str)/job_id(int) 목록.

        편집 3형제·submit(only=)와 같은 ref 규칙이고, 같은 job을 두 형태로
        지정하면 1회로 접힌다. 없는 ref는 JobNotFoundError.
        비활성만 삭제 가능(활성이면 RemoveNotAllowedError, force로 레코드만
        강제 삭제 — LSF job 정리는 caller 책임). 전부 지우려면 clear_jobs().
        삭제분은 반환값과 jobset_updated로 반영된다(레코드가 사라져
        jobs_updated로는 표현할 수 없다)."""
        jobset_id = self._jsid(jobset_id)
        keys = [r.job_key for r in self._resolve_refs(jobset_id, refs)]
        removed = self.jobsets.local_remove_jobs(
            jobset_id, keys, force=force)
        self._forget_paced(jobset_id, [r.job_key for r in removed])
        self._emit_summary(jobset_id)
        return removed

    def clear_jobs(self, jobset_id, *,
                   force: bool = False) -> List[JobRecord]:
        """[sync] jobset의 **job 전부** 삭제 — remove_jobs과 동일 가드.

        jobset 자체는 남는다 — id/label/tags/handler/폴링/GUI 행이 그대로
        유지되므로 add_jobs/upsert_jobs로 새 batch를 채우면 **같은 jobset을
        그대로 재사용**할 수 있다.
        jobset 자체를 없애려면 remove_jobset()."""
        jobset_id = self._jsid(jobset_id)
        removed = self.jobsets.local_clear_jobs(jobset_id, force=force)
        self._forget_paced(jobset_id)
        self._emit_summary(jobset_id)
        return removed

    def _forget_paced(self, jobset_id: str,
                      job_keys: Optional[List[str]] = None) -> None:
        """사라진 job/jobset에 매달린 **보류 상태**를 일괄 정리한다.

        - pacer: 보류 중인 표시 전이 — dwell 창 안에 삭제되면 늦게 도착한
          전이가 지워진 행을 되살린다.
        - querier: LOST 미발견 스트릭 — jobset이 통째로 사라지면(remove_jobset)
          그 jobset으로는 다시 query()가 안 불려 항목이 영영 남는다."""
        if self._pacer is not None:
            self._pacer.forget(jobset_id, job_keys)
        self.querier.forget(jobset_id, job_keys)

    def _emit_summary(self, jobset_id: str) -> None:
        try:
            self.jobset_updated.emit(jobset_id, self.store.summary(jobset_id))
        except LsfmgrError:
            pass

    # ------------------------------------------------------------------
    # jobset 단위 submit (v9) — 전 job (재)제출
    # ------------------------------------------------------------------
    def can_submit(self, jobset_id: str, *,
                   only: Optional[Sequence] = None) -> bool:
        """[sync, snapshot] submit 가능 여부 — 대상 job이 1건 이상 있고 전원
        비활성(CREATED/DONE/EXIT/SUBMIT_FAILED/LOST)이며 진행 중 submit/kill이
        없으면 True. GUI 버튼 활성화 판단용.

        only를 주면 **그 job들만** 본다 — 나머지가 RUN이어도 True일 수 있다
        (submit(only=...)의 가드와 같은 술어)."""
        jobset_id = self._jsid(jobset_id)
        try:
            # 삭제된 jobset은 아래 get_jobs가 JobSetNotFoundError —
            # except LsfmgrError로 떨어져 False가 된다(submit()의 예외와 일치)
            if (self.submitter.is_active(jobset_id)
                    or self.killer.is_active(jobset_id)):
                return False
            jobs = self._submit_targets(jobset_id, only)
            return bool(jobs) and all(r.state.is_inactive for r in jobs)
        except (LsfmgrError, ValueError):
            # ValueError까지 잡는다 — 술어는 "이 인자로 submit이 되겠는가"에
            # 답하는 것이고, 잘못된 only(없는 ref·array element)면 답은 False다.
            # 여기서 예외가 새면 버튼 갱신 루프에서 부르는 GUI가 죽는다
            # (앱 버그 자체는 실제 submit 호출이 예외로 드러낸다).
            return False

    def _submit_targets(self, jobset_id: str,
                        only: Optional[Sequence]) -> List[JobRecord]:
        """제출 대상 레코드 — only=None이면 전 job, 아니면 지정분만.

        array element(array_index 지정분)는 개별 제출 대상이 아니다 — parent
        job 하나가 element 전체를 만들므로 element만 따로 제출할 수 없다.
        전체 제출에서는 조용히 걸러지고, only로 콕 집으면 오류로 알린다
        (조용히 무시하면 '선택했는데 안 돌았다'가 된다)."""
        if only is None:
            return [r for r in self.get_jobs(jobset_id)
                    if r.array_index is None]
        targets = self._resolve_refs(jobset_id, only)
        for rec in targets:
            if rec.array_index is not None:
                raise ValueError(
                    f"array element는 개별 제출할 수 없습니다:"
                    f" {rec.job_key!r} (parent job을 제출하세요)")
        return targets

    def _resolve_refs(self, jobset_id: str,
                      refs: Sequence) -> List[JobRecord]:
        """ref 목록 → 레코드 목록 — remove_jobs·submit(only=) 공용.

        ref는 job_key(str) 또는 job_id(int). 같은 job을 두 형태로 지정하면
        1회로 접히고, 순서는 지정 순서를 따른다. 없는 ref는 JobNotFoundError.

        job_id 색인은 **parent만** 담는다 — array element는 parent와 job_id를
        공유하므로 전부 넣으면 마지막 element가 parent를 덮어, job_id로 지정한
        parent가 엉뚱하게 element로 해석된다. element를 콕 집으려면 그
        element의 job_key를 쓴다."""
        jobs = self.get_jobs(jobset_id)
        by_key = {r.job_key: r for r in jobs}
        by_jid = {r.job_id: r for r in jobs
                  if r.job_id is not None and r.array_index is None}
        out: List[JobRecord] = []
        seen = set()
        for ref in refs:
            rec = (by_jid.get(ref) if isinstance(ref, int)
                   else by_key.get(ref))
            if rec is None:
                if isinstance(ref, int) and any(r.job_id == ref for r in jobs):
                    # 그 id는 있는데 element뿐 — 정확한 이유를 알린다.
                    # "job_key로 지정하라"고 단정하지 않는다: 삭제는 element를
                    # job_key로 지정할 수 있지만 제출은 그래도 거부되므로,
                    # 안내대로 했다가 또 막히는 모순이 된다. 사실만 말하고
                    # 무엇이 가능한지는 각 명령의 오류가 이어서 알린다.
                    raise ValueError(
                        f"job_id={ref}에 해당하는 job이 array element뿐입니다"
                        f" (그 id의 parent 레코드가 없음)")
                raise JobNotFoundError(f"{jobset_id}/{ref}")
            if rec.job_key in seen:              # 같은 job을 두 형태로 지정
                continue
            seen.add(rec.job_key)
            out.append(rec)
        return out

    def _submit_jobset(self, js: "JobSet",
                       only: Optional[Sequence] = None,
                       pre_submit: Optional[Callable[[List[str]], bool]] = None,
                       post_process: Optional[Callable[[list], Any]] = None,
                       **kwargs: Any) -> JobSet:
        """jobset (재)제출 — mgr.submit(js, ...)의 구현.
        only=None이면 전 job, 아니면 지정분만."""
        jobset_id = self._jsid(js)
        if self._shutdown_done:
            # shutdown 후 제출은 아무도 join하지 않는 스레드/영영 안 오는
            # 완료 신호를 만든다 — 조용히 고착시키지 말고 명확히 거부한다
            raise SubmitNotAllowedError(
                f"{jobset_id}: shutdown 이후에는 submit할 수 없습니다",
                jobset_id=jobset_id)
        self.store.get_jobset(jobset_id)     # 존재 검증 (삭제분이면 예외)
        if (self.submitter.is_active(jobset_id)
                or self.killer.is_active(jobset_id)):
            raise SubmitNotAllowedError(
                f"{jobset_id}: submit/kill 진행 중에는 submit할 수 없습니다",
                jobset_id=jobset_id)
        jobs = self._submit_targets(jobset_id, only)
        if not jobs:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 제출할 job이 없습니다"
                + (" (only=[] — 빈 선택)" if only is not None else ""),
                jobset_id=jobset_id)
        # 가드는 **제출 대상**에만 건다 — only로 일부만 돌릴 때 나머지가 RUN
        # 이어도 막지 않는다. 대상 job은 리셋(이전 job_id/이력 소거) 후 다시
        # 제출되므로, 그것이 활성이면 LSF에 살아있는 job을 추적 불가로 만든다.
        busy = [r.job_key for r in jobs if not r.state.is_inactive]
        if busy:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 활성(진행 중) job이 있어 submit 불가 — "
                f"{busy[:5]} (먼저 kill 하거나 완료를 기다리세요)",
                jobset_id=jobset_id, job_keys=busy)
        # 이전 제출의 잔여 무장 해제 — 이번 호출 기준으로만 무장
        self._post_process.pop(jobset_id, None)
        self._pending_arm.pop(jobset_id, None)
        opts = self.resolve_options(kwargs, context="submit")
        keyed = [(r.job_key, self._record_to_item(r)) for r in jobs]
        keys = [k for k, _ in keyed]
        # rearm/post_process 무장/자동 폴링은 **레코드 리셋 완료**
        # (submitter.records_reset — 착수 확정 그 자체) 시점에 한다.
        # 먼저 하면 ① 게이트/취소 창에서 이전 실행의 terminal 레코드에
        # end 핸들러가 중복 발화하고 post_process가 낡은 레코드로 오발화하며,
        # ② 폴링 tick이 리셋 전 옛 레코드를 평가하는 경합 창이 생긴다.
        # token은 이 제출 사이클의 정체성 — 큐에 남은 이전 사이클의 낡은
        # 거부 신호가 새 사이클의 보류분을 폐기하지 못하게 한다.
        token = object()
        self._pending_arm[jobset_id] = _PendingArm(
            token, keys, post_process,
            opts.poll_interval_s if opts.auto_poll else None)
        # submit_started는 submitter가 실제 착수 지점(do_launch)에서 발화한다
        # (submitter.started → submit_started 릴레이). shutdown은 started/finished
        # 둘 다 안 나가 짝이 유지되고, born-cancelled는 finished를 내므로 짝을
        # 맞추려 started도 함께 낸다(submitter가 담당) — 어느 경우든 started↔
        # finished 쌍이 깨지지 않는다.
        ok = self.submitter.resubmit_existing(jobset_id, keyed, opts,
                                              pre_submit=pre_submit,
                                              arm_token=token)
        if not ok:
            # 착수 안 됨(shutdown/born-cancelled — kill barrier 경합).
            # records_reset/gate_rejected 어느 신호도 오지 않으므로 보류분을
            # 여기서 정리한다. (born-cancelled의 완료 정산은 _gate_reject의
            # finished(cancelled=N)가 이미 발행했다)
            ent = self._pending_arm.get(jobset_id)
            if ent is not None and ent.token is token:
                self._pending_arm.pop(jobset_id, None)
        return self.jobset(jobset_id)

    def _on_records_reset(self, jsid: str, token: object) -> None:
        """[main] submitter가 레코드 리셋을 마친 직후(착수 확정) — 이 사이클의
        보류분(rearm/post_process/폴링)을 무장한다. 리셋 완료 후라 폴링
        tick이 봐도 레코드는 이미 SUBMITTING — 옛 terminal 오발화 창이 없다.
        token 불일치(다른 사이클의 낡은 신호)면 무시한다."""
        ent = self._pending_arm.get(jsid)
        if ent is None or ent.token is not token:
            return
        self._pending_arm.pop(jsid, None)
        # 새 실행 착수 — 이전 완료 통지 latch를 푼다. 폴링 없이 곧장 전원
        # terminal로 끝나는 사이클(예: bsub 전량 거부)은 그 사이 완료 감지가
        # 한 번도 안 돌아 latch가 자동 해제되지 못한다.
        self._finished_latch.discard(jsid)
        if ent.keys:
            self._rearm_tracking(jsid, ent.keys)
        if ent.post_process is not None:
            self._post_process[jsid] = ent.post_process
        if ent.poll_interval_s is not None:
            self.start_polling(jsid, ent.poll_interval_s)   # 자동 폴링 시작

    def _on_gate_rejected(self, jsid: str, token: object) -> None:
        """[main] 게이트가 착수 없이 끝남(거부/예외/통과 직후 취소/shutdown) —
        이 사이클의 보류분을 폐기한다. token 불일치면 다른 사이클의 낡은
        신호이므로 무시(새 사이클의 보류분을 파괴하지 않는다)."""
        ent = self._pending_arm.get(jsid)
        if ent is None or ent.token is not token:
            return
        self._pending_arm.pop(jsid, None)

    @staticmethod
    def _record_to_item(r: JobRecord):
        """레코드 → 제출 item(wrapper argv) 재구성 (v10: wrapper 단일 경로 —
        command 문자열의 shlex 왕복이 원본 argv를 복원한다)."""
        return shlex.split(r.command)

    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """[sync] 손실 감지 — ID 미확보 SUBMITTING을 LOST 확정
        (v10: LSF 조회/복구 없음 — 순수 Store 판정).

        LOST 전이는 job_lost/jobs_updated/jobset_updated로 발행한다 — LOST는
        terminal이라 폴링이 다시 보고하지 않으므로 여기서 안 내면 push 기반
        UI가 영구 고착된다(리뷰 M3).

        ⚠️ 제출 진행 중(SUBMITTING이 정상 과도 상태) 호출하면 순간 LOST
        오확정될 수 있다 — 제출 성공이 PEND로 자가 복구하지만 job_lost가
        오발화된다. submit_finished 이후에 호출할 것."""
        jobset_id = self._jsid(jobset_id)
        lost = self.jobsets.detect_lost(jobset_id)
        if lost:
            self._relay_jobs_changed(jobset_id, lost)
            for r in lost:
                self.job_lost.emit(jobset_id, r)
        return lost

    def search_jobsets(self, *, tag: Optional[str] = None,
                       label: Optional[str] = None,
                       since: Optional[datetime] = None) -> List[JobSetRecord]:
        """[sync, snapshot] 세션 범위 검색."""
        return self.store.search(tag=tag, label=label, since=since)

    def remove_jobset(self, jobset_id, *,
                      force: bool = False) -> JobSetRecord:
        """[sync] jobset **자체를 삭제** (전원 terminal일 때) — 핸들도 파괴.

        레코드가 실제로 지워지므로 이후 list_jobsets/search_jobsets/get_jobs
        어디에도 남지 않는다. **결과를 나중에 다시 볼
        수 없으니** 필요하면 삭제 전에 스냅샷을 떠 둘 것 — 반환값이 삭제
        직전의 JobSetRecord다.
        job만 비우고 jobset은 재사용하려면 clear_jobs()."""
        jobset_id = self._jsid(jobset_id)
        if (self.submitter.is_active(jobset_id)
                or self.killer.is_active(jobset_id)):
            # pre_submit 게이트 대기 중엔 레코드가 아직 전원 terminal이라
            # 아래 local_remove_jobset 검사를 통과해 버린다 — 게이트가 통과하면
            # 이미 사라진 jobset에 실제 LSF job이 제출되므로(job 편집과 동일 가드)
            # 진행 중 submit/kill이 있으면 삭제를 거부한다.
            # force=True(강제 삭제 계약)면 거부 대신 진행 중 제출을 취소시키고
            # 진행한다 — RETRY_WAIT 장기 backoff 중에도 강제 정리가 가능해야
            # 한다 (LSF에 이미 제출된 job의 정리는 caller 책임).
            if not force:
                raise RemoveJobSetNotAllowedError(
                    f"{jobset_id}: submit/kill 진행 중에는 remove_jobset할 수"
                    f" 없습니다 (force=True로 강제 가능 — 진행 중 제출은 취소됨)",
                    jobset_id=jobset_id)
            self.submitter.cancel_submit(jobset_id)
            self.submitter.abort_retries(jobset_id)
        # 실패 시 예외 — 아래 정리는 하지 않는다(polling/핸들 그대로 유지)
        removed = self.jobsets.local_remove_jobset(jobset_id, force=force)
        # 사라진 jobset에 매달린 것들을 정리한다 —
        # 남기면 매 주기 JobSetNotFoundError가 터진다.
        self.polling.stop_polling(jobset_id)
        self.handlers.remove_all(jobset_id)
        self._forget_paced(jobset_id)    # dwell 보류분 — 삭제된 jobset의
                                         # 늦은 발행 방지
        self._poll_intervals.pop(jobset_id, None)
        self._post_process.pop(jobset_id, None)
        self._pending_arm.pop(jobset_id, None)
        self._finished_latch.discard(jobset_id)
        self._invalidate_handle(jobset_id)
        return removed

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
        """모든 스레드 안전 종료 (멱등). 앱 종료 시 core dump/좀비 스레드를
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
        # pacer는 맨 먼저 — 이후엔 이벤트루프가 안 돌아 타이머가 안 뛰므로 밀린
        # 전이가 GUI에 영영 안 나간다. stop()은 보류분을 즉시 내보내고, 이후
        # 발화(submitter.shutdown의 취소 배치 등)를 지연 없이 통과시킨다.
        steps = [("handlers", self.handlers.shutdown),
                 ("submitter", self.submitter.shutdown),
                 ("post_pool", lambda: self._post_pool.waitForDone(-1)),
                 ("polling", self.polling.shutdown),
                 ("killer", self.killer.shutdown),
                 ("store", self.store.store_dispose)]
        if self._pacer is not None:
            steps.insert(0, ("pacer", self._pacer.stop))
        for name, fn in steps:
            try:
                fn()
            except Exception:                    # noqa: BLE001
                log.exception("shutdown 중 %s 종료 실패(계속)", name)
        log.info("lsfmgr shutdown 완료 — 잔여 스레드 없음")

    # ------------------------------------------------------------------
    # 내부 slot — polling relay + 핸들 dispatch
    # ------------------------------------------------------------------
    def _on_poll_updated(self, jobset_id: str, summary: dict,
                         changed: list) -> None:
        """polling 결과 relay — 요약 + 변경분 batch. 이어서 등록된
        handler를 평가한다 — Store가 방금 갱신됐으므로 handler는 최신 상태를
        본다 (handler는 폴링 사이클에 tie돼 있음)."""
        if self._shutdown_done:
            # 명시적 shutdown() 후 main 큐에 남아 있던 polling.updated —
            # 여기서 신호를 중계하거나 post_pool에 새 task를 시작하면
            # 이미 drain된 pool에 join되지 않는 스레드가 생긴다
            return
        self.jobset_updated.emit(jobset_id, summary)
        if changed:
            self._emit_jobs(jobset_id, changed)
        self.handlers.tick(jobset_id)
        self._maybe_finish(jobset_id)

    def _maybe_finish(self, jobset_id: str) -> None:
        """전원 terminal 도달 감지의 **공통 지점** — 폴링/query_once/submit
        완료에서 호출된다. 두 가지를 처리한다:

          1. jobset_finished(요약) 발화 — post_process 등록과 **무관**하게
             LSF 상태만 보고 전원 terminal이면 1회.
          2. 등록돼 있으면 post_process를 worker에서 1회 실행.

        1회성은 서로 다른 방식으로 보장한다 — post_process는 무장 해제(pop),
        jobset_finished는 latch. latch는 다시 non-terminal이 보이면(재제출 등)
        스스로 풀려 다음 완료에 또 발화한다."""
        if self._shutdown_done:
            return
        if self.submitter.is_active(jobset_id):
            # 제출(게이트 포함) 진행 중 — 레코드가 아직 이전 실행의 terminal
            # 상태로 남아 있는 창이다. 이번 실행의 완료가 아니므로 미룬다.
            return
        try:
            recs = self.store.get_jobs(jobset_id)
        except LsfmgrError:
            self._post_process.pop(jobset_id, None)   # jobset 소멸 — 무장 해제
            self._finished_latch.discard(jobset_id)
            return
        if not recs or not all(r.state.is_terminal for r in recs):
            self._finished_latch.discard(jobset_id)   # 다시 활성 — 재무장
            return
        fn = self._post_process.pop(jobset_id, None)  # 한 번만
        if all(r.killed for r in recs):
            # 전원이 내 kill로 끝났다 — 사용자가 스스로 끝낸 완료라 통지하지
            # 않는다(latch만). kill 완료 시점에 걸러지는 경우(_mute_finish_
            # after_kill)와 달리, 이 판정은 EXIT가 나중에 확인돼도(actual
            # 정책·verify·폴링 수렴) 레코드 표식만으로 성립한다.
            self._finished_latch.add(jobset_id)
        if jobset_id not in self._finished_latch:
            try:
                summary = self.store.summary(jobset_id)
            except LsfmgrError:
                return       # jobset이 방금 사라짐 — 빈 요약을 쏘면 구독자가
                             # s["total"]에서 깨진다. latch도 세우지 않는다.
            self._finished_latch.add(jobset_id)
            self.jobset_finished.emit(jobset_id, summary)
        if fn is None:
            return
        if self._shutdown_done:
            # jobset_finished slot이 그 자리에서 shutdown한 경우 — 이미 drain된
            # pool에 join되지 않는 스레드를 만들지 않는다.
            # ※ 여기서 is_active(재제출)까지 막지는 않는다. 이 실행은 전원
            #   terminal에 **도달했으므로** post_process는 실행돼야 한다
            #   (콜백은 위에서 뜬 recs 스냅샷만 쓴다). 새 사이클의 무장은
            #   _pending_arm이 따로 들고 있어 서로 간섭하지 않는다.
            return
        self.post_processing_started.emit(jobset_id)
        self._post_pool.start(_PostProcessTask(self, jobset_id, fn, recs))

    def _emit_jobs(self, jsid: str, records: list) -> None:
        """jobs_updated 발화 **단일 지점** — min_state_dwell_s가 켜져 있으면
        pacer가 job별 dwell을 채운 뒤 대신 발화한다. 켜져 있어도 요약
        (jobset_updated)과 pull(get_jobs)은 늦추지 않는다 — 요약은 store에서
        바로 계산되므로, dwell 동안 배지 카운트가 표보다 앞선다(의도된 시차)."""
        if self._pacer is None:
            self.jobs_updated.emit(jsid, records)
        else:
            self._pacer.push(jsid, records)

    def _relay_jobs_changed(self, jsid: str, records: list) -> None:
        """상태 전이분(배치)을 즉시 jobs_updated + jobset_updated로 발행 —
        완료를 기다리지 않는다. submitter(초기 CREATED 선발행 → PEND/실패 점진)와
        resubmit kill 단계(EXIT 발행)가 공유한다. 파이프라인처럼 단계마다 표가
        갱신된다. (실패분은 _h_jobs_updated가 js.jobs_failed까지 중계)"""
        self._emit_jobs(jsid, records)
        self._emit_summary(jsid)

    def _emit_summary_after_submit(self, jsid: str, report) -> None:
        """submit 완료 시 최종 요약(jobset_updated)을 보장 발화 — 진행 중
        점진 발행(_relay_jobs_changed)의 마무리. 개별 job(jobs_updated)은
        이미 점진 발행이 전부 커버했으므로 여기서 다시 쏘지 않는다(실패분
        js.jobs_failed 이중 발행 방지)."""
        try:
            summary = self.store.summary(jsid)
        except LsfmgrError:
            return                    # jobset이 이미 사라짐(remove_jobset 등)
        self.jobset_updated.emit(jsid, summary)
        # 제출 시점에 이미 전원 terminal인 경우(예: bsub 전량 거부 → 전원
        # SUBMIT_FAILED)는 폴링 tick의 _maybe_finish가 ctx.finished
        # 확정 전 창에 걸려 완료 감지를 놓칠 수 있다(is_active 방어에 막힘).
        # ctx가 확정된 이 시점(submit_finished)에 한 번 더 확인해 유실을 막는다.
        self._maybe_finish(jsid)

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
                self._emit_jobs(j, recs)
            self._emit_summary(j)
            self._mute_finish_after_kill(j)

    def _mute_finish_after_kill(self, jsid: str) -> None:
        """**사용자가 건 kill로** 전원 terminal이 된 완료는 통지하지 않는다 —
        스스로 끝낸 것이라 "다 끝났다"는 알림이 필요 없다. 발화 없이 latch만
        세워, 이어지는 폴링 tick의 완료 감지가 조용히 지나가게 한다.

        의도치 않은 종료(자연 종료, LSF/관리자의 외부 bkill, EXIT)는 이 경로를
        타지 않으므로 그대로 통지된다 — 사용자가 알아야 하는 쪽만 남는다.

        여기서 '전원 terminal'인지를 보므로 **부분 kill은 억제되지 않는다**:
        PEND만/선택 행만 죽이면 남은 job이 아직 non-terminal이라 latch가 안
        서고, 그 job들이 나중에 끝나면 정상적으로 통지된다.

        ※ kill_status_policy="actual"이면 이 시점에 레코드가 아직 EXIT가
          아니라 억제되지 않는다 — 폴링이 EXIT를 확인하는 순간 통지가 나간다.
        ※ post_process는 억제하지 않는다 — "전원 terminal이면 실행"이라는
          별개 계약이고, kill로 끝난 결과도 수집 대상이다."""
        if not jsid or jsid in self._finished_latch:
            return
        try:
            recs = self.store.get_jobs(jsid)
        except LsfmgrError:
            return
        if recs and all(r.state.is_terminal for r in recs):
            self._finished_latch.add(jsid)

    def _handle_of(self, jobset_id: str) -> Optional[JobSet]:
        h = self._handles.get(jobset_id)
        return h if (h is not None and not h._removed) else None

    def _invalidate_handle(self, jobset_id: str) -> None:
        h = self._handles.pop(jobset_id, None)
        if h is not None:
            h._mark_removed()

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


class _PostProcessTask(QRunnable):
    """전원 terminal 도달 후처리 콜백을 worker 스레드에서 1회 실행.
    반환값은 post_processing_finished로, 예외는 error_occurred +
    post_processing_finished(None)으로 통지 (예외 격리)."""

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
        except Exception as e:               # noqa: BLE001
            log.exception("post_process 예외: %s", self.jsid)
            self.mgr.error_occurred.emit(self.jsid, f"post_process: {e!r}")
            self.mgr.post_processing_finished.emit(self.jsid, None)
            return
        log.info("post_process 완료 %s", self.jsid)
        self.mgr.post_processing_finished.emit(self.jsid, result)

