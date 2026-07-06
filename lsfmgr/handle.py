"""JobSet 핸들 (v7 §1.3) — jobset 1개 전용 Signal + 위임 메서드.

manager가 소유/발급하며, Low-level Facade Signal 위에 얹힌 편의 계층이다
(동일 이벤트 이중 발행). close/삭제된 핸들 접근 시 JobSetClosedError.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Set

from .errors import JobSetClosedError
from .qt import QObject, Signal
from .states import JobRecord, JobState

if TYPE_CHECKING:
    from .manager import LsfJobManager


class JobSet(QObject):
    """JobSet 1개에 대한 High-level 핸들. jobset_id 필터링 불필요."""

    updated = Signal(dict)         # 요약 {"total":.., "RUN":.., ...}
    progress = Signal(int, int)    # submit 진행 (done, total), throttled
    finished = Signal(object)      # SubmitReport (retry 포함 최종)
    failed = Signal(list)          # SUBMIT_FAILED/EXIT/LOST 변경분 [JobRecord]
    killed = Signal(object)        # KillReport
    error = Signal(str)            # worker 예외 등
    handler_finished = Signal(str, object)   # handler_name, HandlerResult

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
    # 제어 — 전부 [async→Signal]: 즉시 반환, 결과는 Signal
    # ------------------------------------------------------------------
    def kill(self, only_state: Optional[JobState] = None,
             verify: Optional[bool] = None) -> None:
        """[async→Signal] JobSet kill — 결과는 killed Signal (FR-3)."""
        self._check_open()
        self._manager.kill_jobset(self._jobset_id, only_state=only_state,
                                  verify=verify)

    def kill_jobs(self, job_keys: "Sequence[str]",
                  verify: Optional[bool] = None) -> None:
        """[async→Signal] 이 JobSet의 특정 job만 kill (job_key 지정).
        jobset 컨텍스트가 있어 optimistic EXIT 전이·verify가 켜지고 결과가
        killed Signal로 온다 — 테이블의 선택 행만 죽일 때 쓴다."""
        self._check_open()
        recs = {r.job_key: r
                for r in self._manager.get_jobs(self._jobset_id)}
        ids = [recs[k].job_id for k in job_keys
               if k in recs and recs[k].job_id is not None]
        self._manager.kill_jobs(ids, jobset_id=self._jobset_id, verify=verify)

    def cancel(self) -> None:
        """[async→Signal] 진행 중 submit 중단 (QT-6) — 결과는 finished."""
        self._check_open()
        self._manager.cancel_submit(self._jobset_id)

    def refresh(self) -> None:
        """[async→Signal] 1회 강제 조회 — 결과는 updated/failed Signal."""
        self._check_open()
        self._manager.query_once(self._jobset_id)

    def reconcile(self) -> None:
        """[async→Signal] 저장 상태 vs LSF 실상태 대조 (Sqlite 전용, FR-6.2).
        완료 시 updated Signal, 미종결 job이 남아 있으면 polling 자동 시작.
        InMemory Store면 PersistenceNotSupportedError."""
        self._check_open()
        self._manager.reconcile(self._jobset_id)

    def start_polling(self, interval_s: Optional[float] = None) -> None:
        """[async→Signal] 주기 polling 시작 — 갱신은 updated Signal."""
        self._check_open()
        self._manager.start_polling(self._jobset_id, interval_s)

    def stop_polling(self) -> None:
        """[async→Signal] polling 중지."""
        self._check_open()
        self._manager.stop_polling(self._jobset_id)

    def close(self) -> None:
        """[sync] 종결 — 전원 terminal일 때만 가능 (FR-5.7).
        이후 이 핸들 접근은 JobSetClosedError."""
        self._check_open()
        self._manager.close_jobset(self._jobset_id)

    def merge_with(self, *others: "JobSet", keep_originals: bool = False,
                   sync_lsf: bool = False) -> "JobSet":
        """[sync] 다른 JobSet들과 병합 — 새 JobSet 핸들 반환 (FR-5.5).
        keep_originals=False면 원본(이 핸들 포함)은 파괴된다."""
        self._check_open()
        ids = [self._jobset_id] + [o.id for o in others]
        new_id = self._manager.merge_jobsets(
            ids, keep_originals=keep_originals, sync_lsf=sync_lsf)
        return self._manager.jobset(new_id)

    def add_job(self, record: JobRecord, sync_lsf: bool = True) -> JobRecord:
        """[sync] job 편입 (FR-5.4). sync_lsf=True면 bmod -g 동기화."""
        self._check_open()
        return self._manager.add_job(self._jobset_id, record,
                                     sync_lsf=sync_lsf)

    def remove_job(self, job_key: str) -> JobRecord:
        """[sync] job 제외 — 제거된 레코드 반환 (add_job의 역연산).
        LSF의 실제 job은 유지된다(추적만 해제 — 필요하면 먼저 kill)."""
        self._check_open()
        return self._manager.remove_job(self._jobset_id, job_key)

    def resubmit_jobs(self, job_keys: Sequence[str], *,
                      commands: Optional[Dict[str, str]] = None,
                      verify: bool = True, **opts: object) -> None:
        """[async→Signal] 지정 job들을 상태 기반으로 재실행 — 결과는 finished.
        살아있는 job은 kill 후, 나머지는 그냥 재제출한다(레코드 재사용).
        commands로 job_key별 새 커맨드 지정 가능(생략 시 기존 커맨드 재사용)."""
        self._check_open()
        self._manager.resubmit_jobs(self._jobset_id, job_keys,
                                    commands=commands, verify=verify, **opts)

    def add_handler(self, name: str, fn: "Callable[..., object]", *,
                    interval_s: float = 10.0,
                    start_states: object = None,
                    end_states: object = None) -> None:
        """[main→Signal] 이름 있는 handler를 이 JobSet에 등록 — 주기 실행 시작.
        결과는 handler_finished(name, HandlerResult) Signal. 상세는
        LsfJobManager.add_handler 참고."""
        self._check_open()
        self._manager.add_handler(
            self._jobset_id, name, fn, interval_s=interval_s,
            start_states=start_states, end_states=end_states)

    def remove_handler(self, name: str) -> None:
        """[main] handler 해제 — 타이머 중지."""
        self._check_open()
        self._manager.remove_handler(self._jobset_id, name)

    def detect_lost(self) -> List[JobRecord]:
        """[sync, LSF 조회 포함] 손실 감지/복구 (FR-5.3) — blocking 주의."""
        self._check_open()
        return self._manager.detect_lost(self._jobset_id)

    # ------------------------------------------------------------------
    # 조회 — 전부 [sync, snapshot]: Store만 읽음, LSF 호출 없음
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
