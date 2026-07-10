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
    from .reports import KillProgress, SubmitProgress


class JobSet(QObject):
    """JobSet 1개에 대한 High-level 핸들. jobset_id 필터링 불필요."""

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
    # 제어 — 전부 [async→Signal]: 즉시 반환, 결과는 Signal
    # ------------------------------------------------------------------
    def kill(self, only_state: Optional[JobState] = None,
             verify: Optional[bool] = None, envpath: str = "") -> None:
        """[async→Signal] JobSet kill — 결과는 kill_finished Signal (FR-3).
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job)."""
        self._check_open()
        self._manager.kill_jobset(self._jobset_id, only_state=only_state,
                                  verify=verify, envpath=envpath)

    def kill_jobs(self, job_keys: "Sequence[str]",
                  verify: Optional[bool] = None, envpath: str = "") -> None:
        """[async→Signal] 이 JobSet의 특정 job만 kill (job_key 지정).
        jobset 컨텍스트가 있어 optimistic EXIT 전이·verify가 켜지고 결과가
        kill_finished Signal로 온다 — 테이블의 선택 행만 죽일 때 쓴다.
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job). job마다
        클러스터가 다르면 forward_cluster로 분류해 클러스터별로 각 envpath로
        나눠 호출한다."""
        self._check_open()
        recs = {r.job_key: r
                for r in self._manager.get_jobs(self._jobset_id)}
        # array element는 반드시 "id[idx]"로 지정 — parent id로 죽이면
        # 선택하지 않은 나머지 element까지 전부 kill된다
        ids: List[object] = []
        for k in job_keys:
            r = recs.get(k)
            if r is None or r.job_id is None:
                continue
            ids.append(f"{r.job_id}[{r.array_index}]"
                       if r.array_index is not None else r.job_id)
        self._manager.kill_jobs(ids, jobset_id=self._jobset_id, verify=verify,
                                envpath=envpath)

    def cancel(self) -> None:
        """[async→Signal] 진행 중 submit 중단 (QT-6) — 결과는 submit_finished."""
        self._check_open()
        self._manager.cancel_submit(self._jobset_id)

    def refresh(self) -> None:
        """[async→Signal] 1회 강제 조회 — 결과는 jobset_updated/jobs_failed Signal."""
        self._check_open()
        self._manager.query_once(self._jobset_id)

    def start_polling(self, interval_s: Optional[float] = None) -> None:
        """[async→Signal] 주기 polling 시작 — 갱신은 jobset_updated Signal."""
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

    def merge_from(self, source: "JobSet", *,
                   force: bool = False) -> List[JobRecord]:
        """[sync] source jobset을 이 jobset에 **in-place 흡수** — merge_id
        기준 (같으면 replace·물리 키 유지, 없거나 None이면 신규 추가).
        source는 삭제되고 이 핸들/테이블은 그대로. 활성 job이 있으면
        LsfmgrError — force로 레코드만 강제 교체(LSF 정리는 caller 책임).
        can_merge(source)로 선확인."""
        self._check_open()
        return self._manager.merge_from(self._jobset_id, source.id,
                                        force=force)

    def can_merge(self, source: "JobSet") -> bool:
        """[sync, snapshot] merge_from(source) 가능 여부 — GUI 버튼용."""
        self._check_open()
        return self._manager.can_merge(self._jobset_id, source.id)

    def create_job(self, command, *, merge_id: Optional[str] = None,
                   ud_data: Optional[dict] = None,
                   wrapper: bool = True) -> JobRecord:
        """[sync] job 1건 생성(CREATED) — JobSpec=bsub / argv=wrapper /
        문자열=wrapper 기본(wrapper=False면 bsub). merge_id는 논리 키
        (merge 시 replace 기준, jobset 내 유일), ud_data는 사용자 dict."""
        self._check_open()
        return self._manager.create_job(self._jobset_id, command,
                                        merge_id=merge_id, ud_data=ud_data,
                                        wrapper=wrapper)

    def create_jobs(self, commands, **kw) -> List[JobRecord]:
        """[sync] job 일괄 생성 — create_job의 배치 버전
        (merge_ids=/ud_datas=/wrapper=)."""
        self._check_open()
        return self._manager.create_jobs(self._jobset_id, commands, **kw)

    def set_ud_data(self, ref, ud_data: Optional[dict]) -> JobRecord:
        """[sync] job의 ud_data 교체 — ref는 job_key/merge_id(str) 또는
        job_id(int)."""
        self._check_open()
        return self._manager.set_ud_data(self._jobset_id, ref, ud_data)

    def can_submit(self) -> bool:
        """[sync, snapshot] submit 가능 여부 — job 1건 이상 + 전원 비활성
        (CREATED/DONE/EXIT/SUBMIT_FAILED/LOST) + 진행 중 submit/kill 없음."""
        self._check_open()
        return self._manager.can_submit(self._jobset_id)

    def submit(self, **opts: object) -> None:
        """[async→Signal] 이 jobset의 **전 job**을 (재)제출 — 결과는
        submit_finished. 활성 job이 있으면 LsfmgrError (can_submit로 선확인).
        같은 jobset/job_key가 전이되므로 핸들·테이블이 그대로."""
        self._check_open()
        self._manager.submit_jobset(self._jobset_id, **opts)

    def clear(self, *, force: bool = False) -> List[JobRecord]:
        """[sync] 전 job 삭제 — 비활성만(활성이면 LsfmgrError, force로
        레코드만 강제 삭제)."""
        self._check_open()
        return self._manager.clear_jobs(self._jobset_id, force=force)

    def add_job(self, record: JobRecord, sync_lsf: bool = True) -> JobRecord:
        """[sync] job 편입 (FR-5.4). sync_lsf=True면 bmod -g 동기화."""
        self._check_open()
        return self._manager.add_job(self._jobset_id, record,
                                     sync_lsf=sync_lsf)

    def remove_job(self, *, job_id: Optional[int] = None,
                   merge_id: Optional[str] = None,
                   job_key: Optional[str] = None,
                   force: bool = False) -> List[JobRecord]:
        """[sync] job 삭제 — job_id/merge_id/job_key 중 하나로 지정 (v9).
        비활성만 삭제 가능(활성이면 LsfmgrError, force로 레코드만 강제 삭제
        — LSF job 정리는 caller 책임). LSF의 실제 job은 죽이지 않는다."""
        self._check_open()
        return self._manager.remove_jobs(
            self._jobset_id, job_id=job_id, merge_id=merge_id,
            job_key=job_key, force=force)

    def add_handler(self, name: str, fn: "Callable[..., object]", *,
                    start_states: object = None,
                    end_states: object = None) -> None:
        """[main→Signal] 이름 있는 handler를 이 JobSet에 등록 — 폴링 사이클 구동.
        결과는 handler_finished(name, HandlerResult) Signal. 상세는
        LsfJobManager.add_handler 참고."""
        self._check_open()
        self._manager.add_handler(
            self._jobset_id, name, fn,
            start_states=start_states, end_states=end_states)

    def remove_handler(self, name: str) -> None:
        """[main] handler 해제 — 타이머 중지."""
        self._check_open()
        self._manager.remove_handler(self._jobset_id, name)

    def detect_lost(self) -> List[JobRecord]:
        """[sync, LSF 조회 포함] 손실 감지/복구 (FR-5.3) — blocking 주의."""
        self._check_open()
        return self._manager.detect_lost(self._jobset_id)

    def fetch_job_detail(self, job_key: str) -> None:
        """[async→Signal] job 1건의 실패/종료 상세 텍스트 조회 — 결과는
        job_detail_ready(job_key, text) Signal. 상태 셀 클릭 핸들러에서
        호출하면 된다 (bhist는 worker 스레드 — GUI 안 멎음).
        EXIT/DONE 등 제출됐던 job은 bhist -l 원문, 제출 실패 job은 저장된
        fail_message(터미널 stderr/stdout)."""
        self._check_open()
        self._manager.fetch_job_detail(self._jobset_id, job_key)

    def job_detail(self, job_key: str) -> str:
        """[sync, LSF 조회 포함] fetch_job_detail의 동기 버전 — blocking 주의."""
        self._check_open()
        return self._manager.job_detail(self._jobset_id, job_key)

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
