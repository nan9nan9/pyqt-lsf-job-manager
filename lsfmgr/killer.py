"""Killer — chunked bkill + verify.

(v10: group/array/name 전략 tier 삭제 — 부착물이 생성되지 않으므로
 kill은 job_id 기반 chunked bkill 단일 경로다. ARG_MAX 안전은 chunk_args.)
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .command import LsfCommand, classify_targets, target_parent_id
from .errors import LsfmgrError, RECORD_GONE
from .monitor import JobsetQuerier
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import KillProgress, KillReport
from .states import JobState
from .store.base import JobSetStore
from .util import EmitThrottler, ledger_add, ledger_remove

log = logging.getLogger("lsfmgr.kill")


@dataclass
class _KillPlan:
    """kill 대상 계획 — 원시 id·선택 key / 전체의 차이를
    이 다섯 값으로 접어, 실행·부기 꼬리(confirm→verify→마킹)를 한 벌로
    공유한다 (v10.1).

    recs=None은 '레코드 풀 미조회'라는 뜻 — 원시 id 경로는 풀을 confirm
    **이후** 지연 조회한다. 먼저 조회하면 jobset 소실 경합/비수치 id의
    예외가 bkill 자체를 무산시킨다 (리팩토링 회귀 F1: 구버전은 kill 완료 후
    마킹 단계에서만 풀을 읽었다)."""
    recs: Optional[List]                 # 대상 레코드 풀 (None=지연 조회)
    targets: List[str]                   # bkill target 문자열 목록
    requested: int                       # KillReport.requested
    rec_target: Callable                 # 레코드 → target 문자열 매핑
    label: str                           # 전략 라벨 ("chunk" 등)


class Killer(QObject):
    """kill 진입점 — 실제 실행은 QThreadPool 단발 task (§4)."""

    finished = Signal(str, object)           # jobset_id, KillReport
    progress = Signal(str, int, int)         # jobset_id, done, total (chunk 진행)
    error = Signal(str, str)

    def __init__(self, store: JobSetStore, command: LsfCommand,
                 querier: JobsetQuerier, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.store = store
        self.command = command
        self.querier = querier
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(4)
        # jobset별 진행 slot 목록. 겹친 kill을 각각 등록·해제하며 worker와 조회 스레드가 공유한다.
        self._active: Dict[str, List[List[int]]] = {}
        self._active_lock = threading.Lock()
        self._shutdown = False
        # shutdown 판정과 pool.start를 원자화해 종료 이후 task 추가를 막는다.
        # 잠금 순서는 _shutdown_lock → _active_lock이며 역순 취득하지 않는다.
        self._shutdown_lock = threading.Lock()

    def is_active(self, jobset_id: str) -> bool:
        """이 jobset에 진행 중인 kill이 있는지 (pull)."""
        with self._active_lock:
            return bool(self._active.get(jobset_id))

    def progress_snapshot(self, jobset_id: str) -> Optional[KillProgress]:
        """진행 중 kill의 실시간 스냅샷 — 없으면 None.
        겹친 kill은 합산해 하나의 진행으로 보인다."""
        with self._active_lock:
            slots = self._active.get(jobset_id)
            if not slots:
                return None
            return KillProgress(jobset_id=jobset_id,
                                done=sum(s[0] for s in slots),
                                total=sum(s[1] for s in slots))

    def _reg(self, jobset_id: str) -> List[int]:
        """kill 1건 등록 — 반환 slot으로만 갱신/해제한다.
        전역 kill(jsid="")도 빈 키로 등록한다 — kill_started 직후
        is_killing()/kill_state()이 True/값을 준다는 pull 계약이 전역
        경로에서만 깨지지 않게 한다(jobset 가드는 실제 jsid로만 조회하므로
        빈 키 항목의 영향이 없다)."""
        slot = [0, 0]
        with self._active_lock:
            ledger_add(self._active, jobset_id, slot)
        return slot

    def _set_progress(self, slot: Optional[List[int]],
                      done: int, total: int) -> None:
        if slot is not None:
            with self._active_lock:
                slot[0], slot[1] = done, total

    def _unreg(self, jobset_id: str, slot: Optional[List[int]]) -> None:
        """slot 해제 — 멱등(이미 제거됐으면 no-op).
        반드시 identity로 제거한다 — list.remove는 equality 매칭이라 겹친
        kill의 slot 값이 같으면([0,0] 등) 남의 slot을 지운다."""
        if slot is None:
            return
        with self._active_lock:
            ledger_remove(self._active, jobset_id, slot)

    # ------------------------------------------------------------------
    def kill_jobset(self, jobset_id: str, *,
                    verify: bool = False,
                    scope: Optional[object] = None) -> bool:
        """scope: KillScope (kill 우선권, manager가 SubmitGate로 배선).
        지정 시 worker에서 scope.acquire()로 barrier를 올려 진행 중 submit을
        취소·대기하고, kill 완료까지 새 submit 시작을 막는다.
        반환: task를 실제 띄웠으면 True(shutdown 무시=False)."""
        return self._queue_kill(jobset_id, "kill",
                                verify=verify, scope=scope)

    def kill_jobs(self, job_ids: Optional[Sequence] = None, *,
                  job_keys: Optional[Sequence[str]] = None,
                  verify: bool = False, jobset_id: str = "",
                  scope: Optional[object] = None) -> bool:
        """job_ids: int(job 전체) 또는 "id[idx]" 문자열(array element 1개).
        job_keys: jobset 내 job_key — target id 해석을 **worker에서**(scope
        barrier 이후) 한다. 그래야 호출 순간 제출 중이라 job_id가 없던 job도
        quiesce로 id를 확보한 뒤 대상에 포함된다(유출 방지).
        scope: KillScope (kill 우선권 — 범위는 선택한 job_key).
        반환: task를 실제 띄웠으면 True(shutdown 무시=False)."""
        return self._queue_kill(
            jobset_id, "kill_jobs",
            job_ids=None if job_ids is None else list(job_ids),
            job_keys=None if job_keys is None else list(job_keys),
            verify=verify, scope=scope)

    def _queue_kill(self, jobset_id: str, what: str, **task_kwargs) -> bool:
        """kill task 큐잉의 **단일 지점** — 체크+등록+start를 shutdown lock
        아래 원자화한다(락 규율이 진입점마다 복사되지 않게).

        진행 스냅샷 등록(_reg)은 여기(호출 스레드)에서 한다 — 반환 시점에
        is_killing()/kill_state()이 즉시 True/값을 주도록 (caller가 직후
        발행하는 kill_started와 pull API가 일치해야 한다)."""
        with self._shutdown_lock:            # 체크+start 원자화 (shutdown 경합)
            if self._shutdown:
                # shutdown 후 새 kill worker는 아무도 join하지 않는다.
                # False 반환 — caller가 kill_started를 발화하지 않게(started/
                # finished 짝 계약: task를 안 띄웠으니 kill_finished도 안 온다).
                log.warning("shutdown 후 %s 요청 무시: %s",
                            what, jobset_id or "(전역)")
                return False
            slot = self._reg(jobset_id)
            self._pool.start(_KillTask(
                self, jobset_id=jobset_id, slot=slot, **task_kwargs))
        return True

    def shutdown(self) -> None:
        # 플래그를 락 아래에서 세운다 — kill_jobset/kill_jobs의 체크+start와
        # 직렬화돼, waitForDone 이후 task가 start되는 창을 닫는다.
        with self._shutdown_lock:
            self._shutdown = True
        self._pool.waitForDone(-1)


class _KillTask(QRunnable):

    def __init__(self, killer: Killer, *, jobset_id: str,
                 job_ids: Optional[List] = None,
                 job_keys: Optional[List[str]] = None,
                 verify: bool = False,
                 scope: Optional[object] = None,
                 slot: Optional[List[int]] = None):
        super().__init__()
        self.setAutoDelete(True)
        self.killer = killer
        self.jobset_id = jobset_id
        self.job_ids = job_ids
        self.job_keys = job_keys       # 지연 해석 대상 (worker에서 job_ids로)
        self.verify = verify
        self.scope = scope
        self.slot = slot                     # 진행 스냅샷 slot (killer._reg 발급)
        cfg = killer.command.config          # chunk progress throttle (submit 대칭)
        self._prog = EmitThrottler(cfg.progress_min_interval_s,
                                   cfg.progress_min_step_ratio)

    def run(self):
        target = (self.jobset_id or f"ids={len(self.job_ids or [])}")
        mode = ("keys" if self.job_keys is not None
                else ("ids" if self.job_ids is not None else "전체"))
        log.info("kill 착수 %s (%s)", target, mode)

        try:
            try:
                report = self._run()
            except Exception as e:           # noqa: BLE001
                # 착수/완료 짝 계약 — 예외에도 kill_finished는 발행한다.
                # 안 하면 kill_started로 스피너를 켠 UI가 영구 고착된다.
                log.exception("kill 실패: %s", self.jobset_id)
                self.killer.error.emit(self.jobset_id, repr(e))
                report = KillReport(
                    jobset_id=self.jobset_id, requested=0,
                    errors=[f"internal: {e!r}"])
            else:
                log.info("kill 완료 %s: 요청 %d / 미확인 %d / 잔존 %s "
                         "(전략 %s, LSF호출 %d회%s)",
                         target, report.requested, report.unconfirmed,
                         "미검증" if report.still_alive is None
                         else report.still_alive,
                         "+".join(report.strategies) or "-",
                         report.command_calls,
                         f", 오류 {len(report.errors)}건"
                         if report.errors else "")
                # 완료 시 항상 100% 보장 (미확인이 남아도 작업은 끝) —
                # submit과 대칭. 예외 경로는 진행 보장 대상이 아니다.
                self.killer._set_progress(self.slot, report.requested,
                                          report.requested)
                self.killer.progress.emit(self.jobset_id, report.requested,
                                          report.requested)
            # 종료 시퀀스 **단일 출구** — finished보다 먼저 등록 해제:
            # queued 신호를 받은 slot이 is_killing을 pull하면 반드시 False여야
            # 한다 (결정적 계약). finally의 중복 해제는 멱등이라 무해.
            self.killer._unreg(self.jobset_id, self.slot)
            self.killer.finished.emit(self.jobset_id, report)
        finally:
            self.killer._unreg(self.jobset_id, self.slot)

    def _emit_progress(self, done: int, total: int) -> None:
        """chunk 진행 통지 (throttled) + pull 스냅샷 갱신(throttle 무관 최신)."""
        self.killer._set_progress(self.slot, done, total)
        if total > 0 and self._prog.should_emit(done, total):
            self.killer.progress.emit(self.jobset_id, done, total)

    # ------------------------------------------------------------------
    def _run(self) -> KillReport:
        # kill 우선권 barrier는 kill이 끝날 때까지 유지한다 — 그동안 새
        # submit 등록은 SubmitGate가 거부(born-cancelled)하므로 'kill 진행 중
        # 도착한 제출/재제출은 취소된다'가 타이밍이 아닌 규칙이 된다.
        scope = self.scope
        try:
            return self._run_kill(scope)
        finally:
            if scope is not None:
                scope.release()

    def _run_kill(self, scope) -> KillReport:
        """kill 1건의 전 과정 — **단계 순서 자체가 계약**이다.

        각 단계를 이름 붙은 메서드로 나눈 이유: 순서를 바꾸면 조용히 깨지는데
        예전에는 그 제약이 주석으로만 있었다.
          ① quiesce 먼저 — 그래야 제출 중이던 job의 job_id가 잡힌다.
          ② key→target 해석은 ① **뒤** — 먼저 풀면 그 job이 kill을 빠져나간다.
          ③ verify는 마킹 **앞** — 먼저 EXIT로 찍으면 그 레코드가 재조회
             대상에서 빠져 verify가 생존을 영영 못 본다(verify 무력화).
        """
        errors: List[str] = []
        self._quiesce(scope, errors)                                   # ①
        if self.job_keys is not None:                                  # ②
            self.job_ids = self._resolve_keys()

        plan = self._make_plan()
        calls = unconfirmed = retries = 0
        resolved: set = set()
        strategies: List[str] = []
        if plan.targets:
            calls, unconfirmed, retries, resolved = self._kill_confirm(
                plan.targets, errors)
            strategies.append(plan.label)

        # verify는 잔존을 셀 때 target 문자열로 정확 매칭한다 — job_id만으로
        # 세면 element 1개 kill에 형제 element가 잔존으로 오집계된다.
        still_alive: Optional[int] = None
        alive_keys: set = set()
        verify_changed: List = []
        if self.verify:            # jobset이 없으면 직접 조회로 검증한다.
            still_alive, alive_keys, verify_changed = \
                self._verify(set(plan.targets))                        # ③

        changed = self._mark_killed(plan, resolved, alive_keys)
        # verify로 terminal이 된 레코드도 갱신 배치에 포함한다.
        # 동일 job은 마킹까지 반영한 마지막 레코드만 발행한다.
        changed = list({(r.jobset_id, r.job_key): r
                        for r in verify_changed + changed}.values())
        return KillReport(
            jobset_id=self.jobset_id, requested=plan.requested,
            strategies=strategies, command_calls=calls,
            still_alive=still_alive, unconfirmed=unconfirmed,
            kill_retries=retries, changed=changed,
            errors=errors)

    def _quiesce(self, scope, errors: List[str]) -> None:
        """① kill 우선권 — barrier를 올리고 그 범위의 제출이 멎기를 기다린다.

        barrier를 올리는 순간(SubmitGate lock 아래 원자적) 그 시점의 submit
        활동을 넘겨받아 취소·대기한다. 미제출 job은 CANCELLED로 확정돼 대상에서
        빠지고, 그새 제출이 완료된 job은 PEND(job_id 확보)로 확정돼 아래
        스냅샷에 포함된다. barrier 이후의 새 시작은 등록이 거부되므로 유출이 없다.

        대기 동안 pool 슬롯을 반납한다(releaseThread — Qt의 blocking task 표준
        패턴). 안 그러면 대기 몇 건이 pool(4개)을 전부 점유해 후속 kill(긴급
        kill 포함)이 큐에 갇힌다.

        대기 초과는 errors에 남긴다 — 스냅샷 이후 제출이 완료된 job이 kill에서
        빠졌을 수 있다는 뜻이라, 로그로만 삼키면 kill_finished가 '전부 정리됨'
        으로 오보된다.
        """
        if scope is None:
            return
        self.killer._pool.releaseThread()
        try:
            quiesced = scope.acquire()
        finally:
            self.killer._pool.reserveThread()
        if not quiesced:
            msg = ("quiesce: submit 정지 대기 초과 — 그 사이 제출된 "
                   "job이 kill에서 빠졌을 수 있음")
            log.warning("%s: %s", msg, self.jobset_id)
            errors.append(msg)

    def _mark_killed(self, plan: _KillPlan, resolved: set,
                     alive_keys: set) -> List:
        """⑤ "내가 죽였다"를 레코드에 남긴다. 반환: 종료 상태 갱신분.

        **확인된 target만** 마킹한다(errors 유무와 무관) — 미확인분은 on-LSF로
        남아 폴링/재kill이 처리한다. verify가 실측한 생존분은 제외해 EXIT로
        덮어 숨기지 않는다.

        정책과 무관하게 killed 표식은 남긴다: optimistic은 EXIT 전이까지,
        actual은 표식만(전이는 폴링이 실측으로). 어느 쪽이든 "내가 죽였다"는
        지금만 알 수 있는 사실이다.
        """
        if not resolved:
            return []
        recs = plan.recs
        if recs is None:
            # 개별 id 경로는 풀을 여기서야 조회한다 — kill은 이미 끝났으므로
            # 조회 실패(jobset 소실/비수치 id)는 마킹만 포기하고 삼킨다.
            try:
                recs = self._record_pool()
            except Exception as e:           # noqa: BLE001 — 마킹만 포기
                log.warning("kill 마킹용 레코드 조회 실패(무시): %r", e)
                return []
        killed = [r for r in recs
                  if r.job_id is not None and plan.rec_target(r) in resolved
                  and r.job_key not in alive_keys]
        optimistic = self.killer.command.config.kill_status_policy == "optimistic"
        changed = []
        for r in killed:
            def same_job(cur, target=r):
                return (cur._generation == target._generation
                        and cur.job_id == target.job_id
                        and cur.array_index == target.array_index)

            try:
                new = None
                if optimistic:
                    new = self.killer.store.transition(
                        r.jobset_id, r.job_key, JobState.EXIT,
                        fail_reason="KILLED", killed=True,
                        guard=lambda cur: same_job(cur) and cur.state.is_on_lsf)
                if new is None:
                    # verify/폴링이 먼저 종료를 관측했어도 취소 근거는 남긴다.
                    # 관측한 상태·exit_code는 그대로 보존한다.
                    new = self.killer.store.transition(
                        r.jobset_id, r.job_key, None, killed=True,
                        guard=same_job)
            except RECORD_GONE:
                continue
            if new is not None and (optimistic or new.state.is_terminal):
                changed.append(new)
        return changed

    def _make_plan(self) -> _KillPlan:
        """진입 형태별 kill 계획 산출 — _run_kill의 공유 꼬리에 공급한다."""
        k = self.killer
        if self.job_ids is not None:
            # 개별 ID kill — 원시 id/"id[idx]" 문자열 그대로.
            # 레코드 풀은 지연 조회(recs=None) — 근거는 _KillPlan docstring.
            targets = [str(i) for i in self.job_ids]
            return _KillPlan(None, targets, len(targets), self._id_str,
                             "chunk")
        # 전체 kill은 배열의 모든 element를 포함하도록 부모 ID로 중복 제거한다.
        # verify도 부모 ID를 사용해 kill 중 재실행된 element를 확인한다.
        k.store.get_jobset(self.jobset_id)   # 존재 검증 (없으면 예외)
        recs = [r for r in k.store.get_jobs(self.jobset_id)
                if r.state.is_on_lsf]
        targets = sorted({str(r.job_id) for r in recs
                          if r.job_id is not None})
        return _KillPlan(recs, targets, len(recs),
                         lambda r: str(r.job_id), "chunk")

    def _resolve_keys(self) -> List[str]:
        """job_key → target 문자열. 미제출(job_id None)·소실 key는 제외.
        jobset 컨텍스트가 없으면 key로 전역 검색한다(GUI가 핸들 없이 행
        선택만으로 kill하는 경로)."""
        wanted = set(self.job_keys or ())
        pool = (self.killer.store.get_jobs(self.jobset_id) if self.jobset_id
                else self.killer.store.find_jobs_by_keys(wanted))
        # dict으로 접지 않는다 — job_key는 jobset 안에서만 유일해서 전역
        # 검색은 같은 key를 여럿 돌려줄 수 있다(접으면 조용히 하나만 죽는다).
        return [self._id_str(r) for r in pool
                if r.job_key in wanted and r.job_id is not None]

    @staticmethod
    def _id_str(rec) -> str:
        return (f"{rec.job_id}[{rec.array_index}]"
                if rec.array_index is not None else str(rec.job_id))

    def _record_pool(self) -> List:
        """kill_jobs(원시 id) 대상의 레코드 후보 풀 — jobset_id를 알면 그
        jobset에서, 모르면 parent id로 전역 검색 ("id[idx]"도 정규화,
        비수치 id는 건너뜀)."""
        if self.jobset_id:
            return self.killer.store.get_jobs(self.jobset_id)
        return self.killer.store.find_jobs(
            {pid for pid in map(target_parent_id, self.job_ids or [])
             if pid is not None})

    def _verify_direct(self, whole: set, exact: set,
                       ranges: List) -> Optional[List]:
        """jobset 없는 verify — 대상 parent id를 직접 chunked 조회한다.
        반환: JobStatus 목록 / 조회 불완전이면 None(미검증).
        한 chunk라도 실패하면 '미검증'(None)으로 정직하게 보고한다 — 본 것만
        세면 생존을 과소집계해 kill_finished가 '전부 정리됨'으로 오보된다.
        store 갱신·전이는 하지 않는다(잔존 집계 전용 — jobset 경로와 달리
        report.changed에 실을 전이가 없다)."""
        pids = sorted(whole | {p for p, _ in exact} | {p for p, _, _ in ranges})
        if not pids:
            return []
        try:
            # fresh — 방금 bkill한 job의 생사는 캐시로 답할 수 없다
            # (internal 조회원의 스냅샷 TTL을 건너뛴다).
            sts, failed = self.killer.command.bjobs_by_ids(pids, fresh=True)
        except LsfmgrError as e:
            log.warning("kill verify 직접 조회 실패: %s", e)
            return None
        if failed:
            log.warning("kill verify 직접 조회 일부 실패(미검증): %d건",
                        len(failed))
            return None
        return sts

    def _kill_confirm(self, targets: List[str],
                      errors: List[str]) -> Tuple[int, int, int, set]:
        """concrete-id kill — bkill 출력의 확인('is being terminated' 등)을
        보고 미확인분을 재시도한다 (submit retry와 대칭).
        반환: (LSF 호출 횟수, 최종 미확인 수, 재시도 라운드 수, 해소된 id 집합)."""
        k = self.killer
        cfg = k.command.config
        total = len(targets)
        pending = set(targets)
        resolved_all: set = set()
        calls = 0
        attempt = 0
        while True:
            # 자식 확인 행 수가 아니라 요청한 target 중 해소된 수를 센다.
            base = total - len(pending)
            resolved, c, timed_out = k.command.bkill_targets_confirm(
                sorted(pending),
                on_progress=lambda done: self._emit_progress(
                    base + done, total))
            calls += c
            resolved_all |= resolved
            pending -= resolved
            # timeout은 bkill 클라이언트만 중단한다. 접수된 요청이 처리됐는지 조회한 뒤 재시도한다.
            unknown = timed_out & pending
            if unknown:
                gone, qc = self._confirm_by_query(unknown)
                calls += qc
                resolved_all |= gone
                pending -= gone
            if not pending or attempt >= cfg.kill_max_retry:
                break
            attempt += 1
            log.warning("kill 미확인 %d건 — 재시도 %d/%d: %s",
                        len(pending), attempt, cfg.kill_max_retry,
                        sorted(pending)[:20])
            time.sleep(cfg.kill_retry_delay_s)
        if pending:
            msg = (f"kill 확인 실패 {len(pending)}건 "
                   f"(재시도 {attempt}회 후): {sorted(pending)[:20]}")
            log.error(msg)
            errors.append(msg)
        return calls, len(pending), attempt, resolved_all


    def _confirm_by_query(self, targets: set) -> Tuple[set, int]:
        """생사 재조회로 '해소'를 판정한다 — bkill이 시간 내 반환하지 않은
        target 전용. 반환: (죽은 것으로 확인된 target 집합, LSF 호출 수).

        **모호하면 해소로 치지 않는다** — 여기서 잘못 "죽었다"고 접으면 그
        target은 재시도 대상에서 빠져 살아있는 job이 kill을 빠져나간다.
        보류하는 경우는 둘이다:
          ① 조회 자체가 실패한 job ("조회 장애 ≠ job 없음"과 같은 규칙)
          ② element/범위 target인데 조회가 **접힌 행**(array_index=None)만 준
             경우 — 그 행은 여러 element의 합이라 특정 element의 생사를
             판정할 수 없다(_alive_in이 element target에 접힌 행을 안 거는
             것과 같은 이유).
        그 외에는 잔존 술어(_alive_in)를 그대로 쓴다: 살아있지 않으면
        (부재·종료) 더 kill할 필요가 없다. UNKWN/ZOMBI는 확인 보류다."""
        pids = sorted({p for p in map(target_parent_id, targets)
                       if p is not None})
        if not pids:
            return set(), 0
        try:
            # fresh — 방금 kill을 쏜 job의 생사는 캐시로 답할 수 없다
            sts, failed = self.killer.command.bjobs_by_ids(pids, fresh=True)
        except Exception as e:               # noqa: BLE001 — 부기용 조회다
            log.warning("kill timeout 후 생사 조회 실패(재시도로 넘김): %r", e)
            return set(), 1
        rows: Dict[int, List] = {}
        for st in sts:
            rows.setdefault(st.job_id, []).append(st)
        gone = set()
        for t in targets:
            pid = target_parent_id(t)
            if pid is None or pid in failed:
                continue                     # ① 판단 불가 — 재시도로 남긴다
            mine = rows.get(pid)
            if not mine:
                gone.add(t)                  # LSF가 그 id를 모른다 = 끝났다
                continue
            # target 하나씩 분류한다 — 범위("id[m-n]")도 규칙 소유자
            # (classify_targets)가 그대로 해석하게 한다.
            whole, exact, ranges = classify_targets([t])
            if (exact or ranges) and any(st.array_index is None
                                         for st in mine):
                continue                     # ② 접힌 행 — element 판정 불가
            if not self._alive_in(mine, whole, exact, ranges):
                gone.add(t)
        if gone:
            log.info("bkill 중단분 %d건은 조회 결과 이미 죽었습니다 — "
                     "재시도하지 않습니다", len(gone))
        return gone, 1

    def _verify(self, targets: set) -> Tuple[Optional[int], set, List]:
        """재조회로 실제 종료 확인 — 잔존을 센다.
        반환: (잔존 수, 잔존 job_key 집합, 조회가 전이시킨 레코드 목록 —
        caller가 report.changed에 합류시켜 신호 유실을 막는다).
        생존분은 caller가 optimistic EXIT 마킹에서 제외해(EXIT로 덮어 숨기지
        않도록) 폴링/재kill에 남긴다.
        target 문법("id"/"id[idx]"/"id[m-n]")과 관대 처리 규칙은
        command.classify_targets가 소유한다. 부분/개별 kill에서 대상 아닌
        job은 세지 않는다.
        targets가 비면 (0, set(), []), 조회 실패는 (None, set(), [])=미검증."""
        if not targets:
            return 0, set(), []
        whole, exact, ranges = classify_targets(targets)
        if self.jobset_id:
            return self._verify_in_jobset(whole, exact, ranges)
        return self._verify_global(whole, exact, ranges)

    @staticmethod
    def _alive_in(pool, whole, exact, ranges) -> List:
        """잔존 판정 공용 술어 — JobRecord/JobStatus 어느 풀에도 적용.

        element/범위 target은 (job_id, array_index)로 정확 매칭한다.
        array_index=None 레코드(비array job, 또는 monitor가 array를 접은
        집계 레코드)는 그 자체가 "여러 element의 합"이라 특정 element
        target으로 잔존 여부를 판정할 수 없다 — 형제 element를 잔존으로
        과대집계하지 않도록 element/범위 target에는 걸지 않는다. 전체
        kill(bare id, whole)만 집계."""
        def hit(r) -> bool:
            if r.job_id in whole:                # bare id — element 전부 포함
                return True
            if r.array_index is None:            # 집계/비array — 판정 불가
                return False
            if (r.job_id, r.array_index) in exact:
                return True
            return any(r.job_id == pid and lo <= r.array_index <= hi
                       for pid, lo, hi in ranges)

        return [r for r in pool if hit(r) and r.state.is_on_lsf]

    def _verify_in_jobset(self, whole, exact, ranges
                          ) -> Tuple[Optional[int], set, List]:
        """jobset 경로 — polling 파이프라인(query)으로 store까지 갱신하고
        잔존을 센다. 조회가 전이시킨 레코드를 셋째 값으로 반환한다."""
        k = self.killer
        try:
            # fresh — _verify_direct와 같은 이유(캐시된 스냅샷은 kill 이전일 수 있다)
            qr = k.querier.query(self.jobset_id, fresh=True)  # Store 갱신 + 전이 수집
        except LsfmgrError as e:
            log.warning("kill verify 조회 실패: %s", e)
            # 조회 실패는 '미확인'이다 — 수(-1 같은 센티넬)로 뭉개지 않는다.
            # None을 caller가 KillReport.still_alive(None=미검증)로 전달.
            return None, set(), []
        alive = self._alive_in(k.store.get_jobs(self.jobset_id),
                               whole, exact, ranges)
        # 조회가 실패한 job은 생존이 아니라 미확인이다 — 생존으로 세면
        # 확인된 kill의 EXIT/killed 마킹까지 빠진다(_verify_direct와 같은 규칙).
        known = [r for r in alive if r.job_id not in qr.query_failed]
        still_alive = len(known) if len(known) == len(alive) else None
        return still_alive, {r.job_key for r in known}, list(qr.changed)

    def _verify_global(self, whole, exact, ranges
                       ) -> Tuple[Optional[int], set, List]:
        """jobset 컨텍스트 없는 전역 kill — polling 파이프라인(jobset 단위)을
        못 쓰므로 대상 id를 직접 조회해 잔존만 센다(store 갱신·전이 없음 —
        셋째 값은 항상 빈 목록).

        조회 풀이 JobStatus(=job_key 없음)라 생존 레코드를 레코드 풀에서
        역매핑한다. array 레코드는 monitor가 element들을 (job_id, None)
        하나로 접으므로 (job_id, array_index) 동일성만으로는 **절대**
        매칭되지 않는다 — 접힌 레코드는 parent id로 매칭해야 한다(안 그러면
        alive_keys가 늘 비어 살아있는 array가 EXIT로 덮인다)."""
        pool = self._verify_direct(whole, exact, ranges)
        if pool is None:
            return None, set(), []
        alive = self._alive_in(pool, whole, exact, ranges)
        if not alive:
            return 0, set(), []
        live_pairs = {(st.job_id, st.array_index) for st in alive}
        live_pids = {st.job_id for st in alive}
        try:
            recs = [r for r in self._record_pool()
                    if (r.job_id in live_pids if r.array_index is None
                        else (r.job_id, r.array_index) in live_pairs)]
        except Exception as e:           # noqa: BLE001 — 집계 수치는 유효
            log.warning("verify 생존 레코드 역매핑 실패(무시): %r", e)
            recs = []
        if not recs:                     # 레코드를 못 찾음 — 수치만 보고
            return len(alive), set(), []
        # 잔존 수 단위를 jobset 경로(레코드 수)와 맞춘다 — element 수로 세면
        # 같은 array가 요청 1건에 잔존 3건으로 보고된다.
        return len(recs), {r.job_key for r in recs}, []
