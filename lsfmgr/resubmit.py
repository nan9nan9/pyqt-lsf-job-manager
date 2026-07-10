"""ResubmitCoordinator — resubmit_jobs의 kill→재제출 오케스트레이션 (FR-8).

kill(+verify)은 worker 스레드에서 blocking으로, 이어지는 재제출은 main
스레드에서 수행한다 (Qt 스레드 규율: 블로킹은 worker, QObject/pool 조작은
main). manager(Facade)가 소유하며 killer.py와 대칭 구조다.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List

from .options import Options
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import SubmitReport
from .states import JobState

if TYPE_CHECKING:
    from .manager import LsfJobManager

log = logging.getLogger("lsfmgr.resubmit")


@dataclass
class ResubmitPlan:
    """resubmit_jobs 1건의 실행 계획 (kill-phase task로 전달)."""
    jobset_id: str
    keyed: list                 # [(job_key, JobSpec|argv)] — 타입이 곧 제출 경로
    opts: Options
    live_ids: list              # kill 대상 job_id (비어 있으면 kill 생략)
    live_keys: list             # kill 대상 job_key (EXIT 전이·발행용)
    verify: bool
    pre_submit: object = None   # 재제출 전 게이트 (FR-9) — kill 이전에 검사
    envpath: str = ""           # kill 시 source할 LSF env (MC forward job)
    # 게이트 실행 중 cancel — 게이트 통과 후 kill 착수 전에 확인해, 취소면
    # 돌던 job을 죽이지 않고 멈춘다 (게이트가 kill 이전이라 완전 취소 가능)
    cancel_event: threading.Event = field(default_factory=threading.Event)


class ResubmitCoordinator(QObject):
    """resubmit_jobs 오케스트레이터. manager가 소유."""

    _killed = Signal(str)       # jobset_id: kill-phase 완료 → main에서 resubmit
    jobs_changed = Signal(str, list)   # jobset_id, [JobRecord] — kill 단계 EXIT 발행
    # 게이트 거부/예외 → main에서 정리(_plans pop + submit_finished/error 발화)
    _gate_aborted = Signal(str, bool, str)   # jobset_id, failed(예외), msg

    def __init__(self, manager: "LsfJobManager"):
        super().__init__(manager)
        self.mgr = manager
        self._plans: Dict[str, ResubmitPlan] = {}
        self._shutdown = False
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(2)
        self._killed.connect(self._resubmit)    # main 스레드 slot (queued)
        self._gate_aborted.connect(self._on_gate_aborted)

    def is_active(self, jobset_id: str) -> bool:
        """kill-phase 진행 중 여부 — 이 구간엔 submitter ctx가 아직 없어
        submitter.is_active만으로는 중복 resubmit을 못 막는다."""
        return jobset_id in self._plans

    def start(self, plan: ResubmitPlan) -> None:
        log.info("resubmit 착수 %s: %d건 (kill 대상 %d) — kill→재제출",
                 plan.jobset_id, len(plan.keyed), len(plan.live_ids))
        self._plans[plan.jobset_id] = plan
        self._pool.start(_KillPhaseTask(self, plan))

    def cancel(self, jobset_id: str) -> bool:
        """[main] kill-phase 대기 중인 plan 취소 — 재제출을 막는다 (QT-6).
        취소해도 이미 나간 bkill은 되돌리지 않는다(kill task는 자연 종료).
        취소된 plan에 대해 submit_finished(전원 cancelled)를 발행해
        submit_started와 짝을 맞춘다."""
        plan = self._plans.pop(jobset_id, None)
        if plan is None:
            return False
        plan.cancel_event.set()      # 게이트 통과 후 kill 착수를 막는다
        n = len(plan.keyed)
        self.mgr.submit_finished.emit(jobset_id, SubmitReport(
            jobset_id=jobset_id, total=n, succeeded=0, failed=0,
            cancelled=n, retried=0, duration_s=0.0, fail_reasons={}))
        return True

    def _on_gate_aborted(self, jobset_id: str, failed: bool, msg: str) -> None:
        """[main] pre_submit 게이트 거부/예외 — kill·재제출 없이 마무리한다.
        재제출 대상 job 레코드는 건드리지 않는다(돌던 job은 그대로 유지)."""
        plan = self._plans.pop(jobset_id, None)
        if plan is None:
            return
        n = len(plan.keyed)
        if failed:
            self.mgr.error_occurred.emit(jobset_id, f"pre_submit: {msg}")
            self.mgr.submit_finished.emit(jobset_id, SubmitReport(
                jobset_id=jobset_id, total=n, succeeded=0, failed=n,
                cancelled=0, retried=0, duration_s=0.0,
                fail_reasons={"PRE_SUBMIT_FAILED": n}))
        elif self.mgr.config.submit_finished_on_gate_reject:
            self.mgr.submit_finished.emit(jobset_id, SubmitReport(
                jobset_id=jobset_id, total=n, succeeded=0, failed=0,
                cancelled=n, retried=0, duration_s=0.0, fail_reasons={}))

    def _resubmit(self, jobset_id: str) -> None:
        """[main 스레드] kill 완료 후 재제출 착수."""
        plan = self._plans.pop(jobset_id, None)
        if plan is None or self._shutdown:
            # shutdown 중 queued 발화 — 여기서 재제출을 시작하면 shutdown이
            # 기다려주지 않는 좀비 pool/프로세스가 생긴다 (CS-8)
            return
        # handler 재무장 — 재실행되는 job의 handler 진행 상태를 리셋해,
        # 새 실행에서도 start/end 주기가 다시 돌게 한다 (레코드 리셋과 같은
        # main 스레드 흐름이라 tick과 인터리브 없음)
        log.info("resubmit kill 단계 완료 %s → 재제출 dispatch (%d건)",
                 jobset_id, len(plan.keyed))
        self.mgr.handlers.rearm(jobset_id, [k for k, _ in plan.keyed])
        self.mgr.submitter.resubmit_existing(jobset_id, plan.keyed, plan.opts)
        # polling 재개 — 전원 terminal이었다면 AUTO-2가 polling을 꺼둔 상태라
        # 재실행된 job의 전이를 아무도 안 본다. polling을 쓰던 jobset에 한해
        # 마지막 interval로 다시 켠다 (한 번도 안 켰다면 v6 무자동 계약 유지)
        iv = self.mgr._poll_intervals.get(jobset_id)
        if iv is not None:
            self.mgr.start_polling(jobset_id, iv)

    def shutdown(self) -> None:
        self._shutdown = True
        self._pool.waitForDone(-1)
        # waitForDone 중 emit된 queued _killed는 이벤트 루프 재개 후 도착
        # 하는데, _shutdown 플래그와 빈 _plans가 이를 무해하게 만든다
        self._plans.clear()


class _KillPhaseTask(QRunnable):
    """resubmit_jobs의 kill-phase — 살아있는 job을 죽이고(+verify) worker
    스레드에서 blocking 수행. 완료되면 _killed Signal로 main에 넘긴다."""

    def __init__(self, coord: ResubmitCoordinator, plan: ResubmitPlan):
        super().__init__()
        self.setAutoDelete(True)
        self._coord = coord
        self.plan = plan

    def run(self):
        plan = self.plan
        mgr = self._coord.mgr
        # pre_submit 게이트 — kill 이전에 검사한다. 통과해야 kill+재제출 진행.
        # 거부/예외면 돌던 job을 죽이지 않고 그대로 둔다(레코드 미변경).
        if plan.pre_submit is not None:
            mgr.ready_started.emit(plan.jobset_id)
            try:
                commands = [mgr.submitter._item_command(it)
                            for _k, it in plan.keyed]
                ok = bool(plan.pre_submit(commands))
            except Exception as e:                   # noqa: BLE001 — CS-5
                log.exception("resubmit pre_submit 게이트 예외: %s",
                              plan.jobset_id)
                mgr.ready_finished.emit(plan.jobset_id, False)
                self._coord._gate_aborted.emit(plan.jobset_id, True, repr(e))
                return
            mgr.ready_finished.emit(plan.jobset_id, ok)
            if not ok or self._coord._shutdown:
                self._coord._gate_aborted.emit(plan.jobset_id, False, "")
                return
            if plan.cancel_event.is_set():
                # 게이트 도는 사이 cancel — kill 없이 멈춘다 (cancel이 이미
                # submit_finished(cancelled)를 발화했으므로 여기선 조용히 종료)
                return
            mgr.submit_started.emit(plan.jobset_id)  # 게이트 통과 → 착수
        try:
            if plan.live_ids:
                # envpath 지정 시 그 클러스터 env를 source한 bkill —
                # MC forward job은 로컬 bkill로 안 죽어, 안 그러면 원 job이
                # 산 채로 새 job이 중복 제출된다
                with self._coord.mgr.command.operation("resubmit"):  # 태깅
                    self._coord.mgr.command.bkill_by_ids(
                        plan.live_ids, envpath=plan.envpath)
                    if plan.verify:
                        self._await_dead(plan.live_ids)
                # 파이프라인 stage 1 가시화 — 죽인 job을 EXIT로 전이·발행한다.
                # (이어지는 재제출이 CREATED로 리셋하기 전에 kill 결과를 표에
                # 드러낸다: 살아있던 것만 EXIT, 이미 terminal/미제출은 안 건드림)
                self._mark_killed()
        except Exception as e:                   # noqa: BLE001 — CS-5
            # kill 실패해도 재제출은 진행 — 좀비가 남을 수 있으나 새 job은 뜬다.
            # (좀비 회피가 더 중요하면 여기서 중단하도록 정책 변경 가능)
            log.warning("resubmit_jobs kill-phase 경고 %s: %r",
                        plan.jobset_id, e)
        self._coord._killed.emit(plan.jobset_id)

    def _mark_killed(self) -> None:
        """kill한 job(살아있던 것)을 EXIT로 전이하고 jobs_changed로 발행 —
        재제출 전 kill 단계를 UI에 드러낸다. 이미 terminal이 된(경합) 것은
        guard가 건너뛴다."""
        store = self._coord.mgr.store
        jsid = self.plan.jobset_id
        changed = []
        for key in self.plan.live_keys:
            try:
                rec = store.transition(
                    jsid, key, JobState.EXIT, fail_reason="KILLED",
                    guard=lambda cur: cur.state.is_on_lsf)
                if rec is not None:
                    changed.append(rec)
            except Exception:                    # noqa: BLE001 — CS-5
                log.exception("resubmit kill EXIT 전이 실패(무시): %s/%s",
                              jsid, key)
        if changed:
            self._coord.jobs_changed.emit(jsid, changed)

    def _await_dead(self, ids: List[int]) -> None:
        """bjobs 재조회로 종료 확인 — 유한 대기(best-effort). 못 죽어도 진행."""
        interval = max(0.2, self._coord.mgr.config.poll_interval_s / 5)
        for _ in range(5):
            statuses, failed = self._coord.mgr.command.bjobs_by_ids(ids)
            # 조회 실패 chunk가 있으면 그 job들의 종료를 확인 못 한 것 —
            # '없음'으로 오판하지 않고 다음 라운드에서 재확인한다
            if not failed and not any(s.state.is_on_lsf for s in statuses):
                return
            time.sleep(interval)
        log.warning("resubmit_jobs verify: 일부 job이 아직 종료 안 됨 (진행): %s",
                    ids)
