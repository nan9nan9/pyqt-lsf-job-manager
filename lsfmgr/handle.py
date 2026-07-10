"""JobSet 핸들 — jobset 1개의 **조회(pull) + Signal 전용 뷰** (v9).

명령(submit/kill/merge/…)은 전부 manager 한 곳에 있다 —
`mgr.submit(js, ...)` / `mgr.kill(js)` / `mgr.merge(a, b)` 처럼 이 핸들을
인자로 넘긴다. 핸들은 GUI 위젯이 바인딩할 상태 스냅샷과 신호만 제공한다.
close/삭제된 핸들 접근 시 JobSetClosedError.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Set

from .errors import JobSetClosedError
from .qt import QObject, Signal
from .states import JobRecord, JobState

if TYPE_CHECKING:
    from .manager import LsfJobManager
    from .reports import KillProgress, SubmitProgress


class JobSet(QObject):
    """jobset 1개의 조회+Signal 뷰 — jobset_id 필터링 불필요.
    명령은 manager로: mgr.submit(js) / mgr.kill(js) / mgr.merge(a, b) …"""

    # 이름은 Manager Signal과 일치시킨다(jsid 인자만 없음) — 두 계층 매핑이
    # 1:1로 명확해지도록. 같은 이벤트를 이 JobSet으로 좁혀 발행한다.
    jobset_updated = Signal(dict)      # 요약 {"total":.., "RUN":.., ...}
    jobs_updated = Signal(list)        # 상태 변경분 [JobRecord] — 테이블 행 갱신용
    submit_progress = Signal(int, int) # submit 진행 (done, total), throttled
    submit_finished = Signal(object)   # SubmitReport (retry 포함 최종)
    jobs_failed = Signal(list)         # SUBMIT_FAILED/EXIT/LOST 변경분 [JobRecord]
    kill_started = Signal()            # kill 접수 즉시(동기) — 착수 피드백
    kill_finished = Signal(object)     # KillReport
    kill_progress = Signal(int, int)   # chunk kill 진행 (done, total)
    error_occurred = Signal(str)       # worker 예외 등
    handler_finished = Signal(str, object)   # handler_name, HandlerResult
    job_detail_ready = Signal(str, str)      # job_key, 상세 텍스트 (fetch_job_detail)
    ready_started = Signal()           # pre_submit 게이트 시작
    ready_finished = Signal(bool)      # 게이트 종료 (True=통과)

    def __init__(self, manager: "LsfJobManager", jobset_id: str):
        super().__init__(manager)
        self._manager = manager
        self._jobset_id = jobset_id
        self._closed = False

    # ------------------------------------------------------------------
    # 내부
    # ------------------------------------------------------------------
    def _check_open(self) -> None:
        if self._closed:
            raise JobSetClosedError(
                f"파괴된 JobSet 핸들 접근: {self._jobset_id}")

    def _mark_closed(self) -> None:
        self._closed = True

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<JobSet {self._jobset_id} ({state})>"

    # ------------------------------------------------------------------
    # 조회 (pull) — 명령은 manager에 있다 (v9 통일)
    # ------------------------------------------------------------------
    @property
    def id(self) -> str:
        """[sync, snapshot] jobset_id."""
        return self._jobset_id

    @property
    def summary(self) -> dict:
        """[sync, snapshot] 상태별 카운트 (합계 == intended_count)."""
        self._check_open()
        return self._manager.summary(self._jobset_id)

    @property
    def is_done(self) -> bool:
        """[sync, snapshot] 전원 terminal 여부."""
        self._check_open()
        s = self._manager.summary(self._jobset_id)
        total = s.get("total", 0)
        terminal = sum(v for k, v in s.items()
                       if k != "total" and JobState(k).is_terminal)
        return total > 0 and terminal >= total

    @property
    def is_active(self) -> bool:
        """[sync, snapshot] 하나라도 아직 안 끝난(non-terminal) job이 있으면 True.
        inactive(전원 terminal)의 반대 — 이 JobSet을 다시 수행할지 판단할 때 쓴다.
        non-terminal 예: CREATED/SUBMITTING/RETRY_WAIT/PEND/RUN/suspend 등."""
        self._check_open()
        s = self._manager.summary(self._jobset_id)
        return any(v > 0 for k, v in s.items()
                   if k != "total" and not JobState(k).is_terminal)

    @property
    def is_inactive(self) -> bool:
        """[sync, snapshot] 모든 job이 terminal(DONE/EXIT/SUBMIT_FAILED/LOST)이면
        True — 더 진행할 것이 없는 상태. is_active의 반대.
        (job이 하나도 없는 빈 JobSet도 '진행 중인 것 없음'이라 inactive=True)"""
        return not self.is_active

    @property
    def is_submitting(self) -> bool:
        """[sync] 이 JobSet에 진행 중인 submit/resubmit이 있는지.
        대량 제출은 백그라운드(worker 스레드)라 submit()은 즉시 반환한다 —
        진행 dialog를 닫고 딴 작업을 하다가도, 아직 제출 중인지 아무 때나
        이걸로 확인한다. (jobs의 PEND/RUN이 아니라 '제출 작업 자체'의 진행 여부)"""
        self._check_open()
        return self._manager.is_submitting(self._jobset_id)

    @property
    def submit_state(self) -> "Optional[SubmitProgress]":
        """[sync] 진행 중 submit의 실시간 스냅샷(done/total/성공/실패/취소) —
        진행 중이 아니면 None. submit_progress Signal을 놓친 뒤(백그라운드로
        돌려놓고 dialog를 닫은 뒤) 상태 패널을 다시 그릴 때 pull로 조회한다.
        완료 후 최종 결과는 summary / submit_finished(SubmitReport)로 본다."""
        self._check_open()
        return self._manager.submit_snapshot(self._jobset_id)

    @property
    def is_killing(self) -> bool:
        """[sync] 이 JobSet에 진행 중인 kill이 있는지. 대량 chunked kill(특히
        MC envpath/verify)을 백그라운드로 돌려놓고 진행 dialog를 닫은 뒤에도
        아직 kill 중인지 아무 때나 확인한다."""
        self._check_open()
        return self._manager.is_killing(self._jobset_id)

    @property
    def kill_state(self) -> "Optional[KillProgress]":
        """[sync] 진행 중 kill의 실시간 스냅샷(done/total) — 진행 중이 아니면
        None. kill_progress Signal을 놓친 뒤 상태 패널을 다시 그릴 때 pull로
        조회한다. 완료 후 최종 결과는 kill_finished(KillReport)로 본다."""
        self._check_open()
        return self._manager.kill_snapshot(self._jobset_id)

    @property
    def failed_jobs(self) -> List[JobRecord]:
        """[sync, snapshot] 실패 상태(EXIT/SUBMIT_FAILED/LOST) job 목록."""
        self._check_open()
        return self._manager.get_jobs(
            self._jobset_id,
            states={JobState.EXIT, JobState.SUBMIT_FAILED, JobState.LOST})

    def jobs(self, states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        """[sync, snapshot] job 상세 목록 (상태 필터 가능)."""
        self._check_open()
        return self._manager.get_jobs(self._jobset_id, states)
