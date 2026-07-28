"""Killer — chunked bkill + verify (FR-3).

(v10: group/array/name 전략 tier 삭제 — 부착물이 생성되지 않으므로
 kill은 job_id 기반 chunked bkill 단일 경로다. ARG_MAX 안전은 chunk_args.)
"""
from __future__ import annotations

import logging
import re
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

from .command import LsfCommand
from .errors import LsfmgrError
from .monitor import JobsetQuerier
from .qt import QObject, QRunnable, QThreadPool, Signal
from .reports import KillProgress, KillReport
from .states import JobState
from .store.base import JobSetStore
from .util import EmitThrottler, ledger_add, ledger_remove

log = logging.getLogger("lsfmgr.kill")


class Killer(QObject):
    """kill 진입점 — 실제 실행은 QThreadPool 단발 task (§3.2)."""

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
        # 진행 중 kill의 pull 스냅샷 — jobset_id → kill별 slot([done,total])
        # 리스트. 같은 jobset에 kill이 겹칠 수 있으므로(예: 선택 kill 직후
        # 전체 kill) 단일 엔트리 덮어쓰기가 아니라 kill 1건당 slot 1개를
        # 등록·해제한다 — 먼저 끝난 kill이 진행 중인 다른 kill의 등록을
        # 지우지 못한다. worker(update)와 조회 스레드(snapshot)가 공유.
        self._active: Dict[str, List[List[int]]] = {}
        self._active_lock = threading.Lock()
        self._shutdown = False
        # _shutdown 체크와 _pool.start를 원자화한다 — 없으면 kill 스레드가
        # _shutdown=False를 읽은 직후 shutdown()이 _shutdown=True + waitForDone
        # 으로 pool을 비워버린 뒤 task가 start돼 아무도 join 못 하는 worker가
        # 뜬다(CS-8). shutdown()도 이 락 아래에서 플래그를 세운다.
        # 락 순서: _shutdown_lock → _active_lock(_reg) (역순 금지).
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

    def _reg(self, jobset_id: str) -> Optional[List[int]]:
        """kill 1건 등록 — 반환 slot으로만 갱신/해제한다."""
        if not jobset_id:                # 전역 kill(jsid 없음)은 스냅샷 대상 아님
            return None
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
                    only_state: Optional[JobState] = None,
                    verify: bool = False, envpath: str = "",
                    cluster_envpaths: Optional[Dict[str, str]] = None,
                    scope: Optional[object] = None) -> bool:
        """scope: KillScope (kill 우선권, manager가 SubmitGate로 배선).
        지정 시 worker에서 scope.acquire()로 barrier를 올려 진행 중 submit을
        취소·대기하고, kill 완료까지 새 submit 시작을 막는다.

        진행 스냅샷 등록(_reg)은 여기(호출 스레드)에서 한다 — 반환 시점에
        is_killing()/kill_snapshot()이 즉시 True/값을 주도록 (caller가 직후
        발행하는 kill_started와 pull API가 일치해야 한다)."""
        with self._shutdown_lock:            # 체크+start 원자화 (shutdown 경합)
            if self._shutdown:
                # shutdown 후 새 kill worker는 아무도 join하지 않는다 (CS-8).
                # False 반환 — caller가 kill_started를 발화하지 않게(started/
                # finished 짝 계약: task를 안 띄웠으니 kill_finished도 안 온다).
                log.warning("shutdown 후 kill 요청 무시: %s", jobset_id)
                return False
            slot = self._reg(jobset_id)
            self._pool.start(_KillTask(
                self, jobset_id=jobset_id, only_state=only_state,
                verify=verify, envpath=envpath,
                cluster_envpaths=cluster_envpaths, scope=scope, slot=slot))
        return True

    def kill_jobs(self, job_ids: Sequence, *, verify: bool = False,
                  jobset_id: str = "", envpath: str = "",
                  cluster_envpaths: Optional[Dict[str, str]] = None) -> bool:
        """job_ids: int(job 전체) 또는 "id[idx]" 문자열(array element 1개).
        envpath 지정 시 그 LSF env를 source한 bkill (MC forward job — 클러스터별로
        나눠 각 envpath로 호출). 반환: task를 실제 띄웠으면 True(shutdown 무시=False)."""
        with self._shutdown_lock:            # 체크+start 원자화 (shutdown 경합)
            if self._shutdown:
                log.warning("shutdown 후 kill_jobs 요청 무시")
                return False
            slot = self._reg(jobset_id)
            self._pool.start(_KillTask(
                self, jobset_id=jobset_id, job_ids=list(job_ids),
                verify=verify, envpath=envpath,
                cluster_envpaths=cluster_envpaths, slot=slot))
        return True

    def shutdown(self) -> None:
        # 플래그를 락 아래에서 세운다 — kill_jobset/kill_jobs의 체크+start와
        # 직렬화돼, waitForDone 이후 task가 start되는 창을 닫는다.
        with self._shutdown_lock:
            self._shutdown = True
        self._pool.waitForDone(-1)


class _KillTask(QRunnable):

    def __init__(self, killer: Killer, *, jobset_id: str,
                 only_state: Optional[JobState] = None,
                 job_ids: Optional[List] = None, verify: bool = False,
                 envpath: str = "",
                 cluster_envpaths: Optional[Dict[str, str]] = None,
                 scope: Optional[object] = None,
                 slot: Optional[List[int]] = None):
        super().__init__()
        self.setAutoDelete(True)
        self.killer = killer
        self.jobset_id = jobset_id
        self.only_state = only_state
        self.job_ids = job_ids
        self.verify = verify
        self.envpath = envpath
        self.cluster_envpaths = cluster_envpaths
        self.scope = scope
        self.slot = slot                     # 진행 스냅샷 slot (killer._reg 발급)
        cfg = killer.command.config          # chunk progress throttle (submit 대칭)
        self._prog = EmitThrottler(cfg.progress_min_interval_s,
                                   cfg.progress_min_step_ratio)

    def run(self):
        target = (self.jobset_id or f"ids={len(self.job_ids or [])}")
        mode = (f"only={self.only_state.value}" if self.only_state
                else ("ids" if self.job_ids is not None else "전체"))
        log.info("kill 착수 %s (%s%s)", target, mode,
                 ", envpath" if self.envpath else "")
        try:
            try:
                report = self._run()
            except Exception as e:           # noqa: BLE001 — CS-5
                log.exception("kill 실패: %s", self.jobset_id)
                self.killer.error.emit(self.jobset_id, repr(e))
                # 착수/완료 짝 계약 — 예외에도 kill_finished는 발행한다.
                # 안 하면 kill_started로 스피너를 켠 UI가 영구 고착된다.
                report = KillReport(
                    jobset_id=self.jobset_id, requested=0,
                    errors=[f"internal: {e!r}"])
                self.killer._unreg(self.jobset_id, self.slot)
                self.killer.finished.emit(self.jobset_id, report)
                return
            log.info("kill 완료 %s: 요청 %d / 미확인 %d / 잔존 %s "
                     "(전략 %s, LSF호출 %d회%s)",
                     target, report.requested, report.unconfirmed,
                     "미검증" if report.still_alive is None
                     else report.still_alive,
                     "+".join(report.strategies) or "-", report.command_calls,
                     f", 오류 {len(report.errors)}건" if report.errors else "")
            # 완료 시 항상 100% 보장 (미확인이 남아도 작업은 끝) — submit과 대칭
            self.killer._set_progress(self.slot, report.requested,
                                      report.requested)
            self.killer.progress.emit(self.jobset_id, report.requested,
                                      report.requested)
            # finished보다 먼저 등록 해제 — queued 신호를 받은 slot이
            # is_killing을 pull하면 반드시 False여야 한다 (결정적 계약).
            # finally의 중복 해제는 멱등이라 무해.
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
        k = self.killer
        strategies: List[str] = []
        errors: List[str] = []
        if scope is not None:
            # kill 우선권 (FR-3) — barrier를 올리는 순간(SubmitGate lock 아래
            # 원자적) 그 시점의 submit 활동을 넘겨받아 취소·대기한다. 미제출
            # job은 CREATED로 복귀해 대상에서 빠지고, 그새 제출이 완료된
            # job은 PEND(job_id 확보)로 확정되어 아래 스냅샷에 포함된다.
            # barrier 이후의 새 시작은 등록 거부되므로 놓치는 유출이 없다.
            # 대기 동안 pool 슬롯을 반납한다(releaseThread — Qt의 blocking
            # task 표준 패턴) — 안 그러면 대기 몇 건이 pool(maxThreadCount=4)
            # 을 전부 점유해 후속 kill(긴급 kill 포함)이 큐에 갇힌다.
            k._pool.releaseThread()
            try:
                quiesced = scope.acquire()
            finally:
                k._pool.reserveThread()
            if not quiesced:
                # 대기 초과는 report.errors에 남긴다 — 스냅샷 이후 제출이
                # 완료된 job이 kill 대상에서 빠졌을 수 있다는 뜻이라, 로그로만
                # 삼키면 kill_finished가 '전부 정리됨'으로 오보된다.
                # (v10.1: per-id 확인 도입으로 optimistic 마킹은 resolved
                # 기반 — errors가 있어도 확인된 job의 EXIT 표시는 정확하다)
                msg = ("quiesce: submit 정지 대기 초과 — 그 사이 제출된 "
                       "job이 kill에서 빠졌을 수 있음")
                log.warning("%s: %s", msg, self.jobset_id)
                errors.append(msg)
        optimistic = (k.command.config.kill_status_policy == "optimistic")

        # --- MC(cluster_envpaths) 모드: kill 전 강제 조회 ---
        # 제출 직후에는 source/forward_cluster가 미상이라(첫 bjobs 관측 전)
        # 어느 env로 bkill해야 할지 모른다 — 미상 대상이 있으면 bkill **전에**
        # bjobs 1회를 강제해 cluster를 채운다. 조회 실패는 미상인 채 진행
        # (미상은 기본 envpath로 kill — kill 자체는 반드시 나간다).
        # ※ cluster 관측은 collect_clusters=True 폴링 포맷 전제.
        if self.cluster_envpaths and self.jobset_id:
            try:
                need = any(r.state.is_on_lsf
                           and r.forward_cluster is None
                           and r.source_cluster is None
                           for r in k.store.get_jobs(self.jobset_id))
                if need:
                    log.info("kill 전 강제 조회 — cluster 미상 대상 존재: %s",
                             self.jobset_id)
                    k.querier.query(self.jobset_id)
            except LsfmgrError as e:
                log.warning("kill 전 강제 조회 실패(미상인 채 진행): %s", e)

        # --- kill 계획: 세 경로 모두 confirm chunk(_kill_confirm)로 죽인다 —
        # 경로마다 다른 것은 (대상 레코드 풀, target 문자열, rec→target 매핑,
        # 전략 라벨)뿐이고 실행·부기(장부)는 아래 공유 꼬리 한 벌이다 (v10.1).
        if self.job_ids is not None:
            # 개별 ID kill (FR-3.4) — 원시 id/"id[idx]" 문자열 그대로.
            # 레코드 풀은 여기서 조회하지 않는다(recs=None → 공유 꼬리에서
            # confirm **이후** 지연 조회) — 먼저 조회하면 jobset 소실 경합/
            # 비수치 id의 예외가 bkill 자체를 무산시킨다 (리팩토링 회귀 F1:
            # 구버전은 kill 완료 후 마킹 단계에서만 풀을 읽었다).
            recs = None
            targets = [str(i) for i in self.job_ids]
            requested = len(targets)
            rec_target = self._id_str
            label = "chunk"
        elif self.only_state is not None:
            # 부분 kill (FR-3.2) — 해당 상태 job만. array element는 반드시
            # "id[idx]" 지정(parent id면 다른 상태 element까지 전부 죽는다).
            # (bkill -stat은 LSF 버전 의존이라 결정적 방식을 기본으로 한다)
            recs = k.store.get_jobs(self.jobset_id, states={self.only_state})
            targets = [self._id_str(r) for r in recs if r.job_id is not None]
            requested = len(targets)
            rec_target = self._id_str
            label = f"chunk(state={self.only_state.value})"
        else:
            # 전체 kill — 살아있는 전 job. array element는 parent id 1개로
            # 전체가 죽으므로 bare id로 dedupe하고, rec→target 매핑도 bare id
            # (verify가 kill 창에 rerun(bsub -r)으로 되살아난 element까지
            # 잔존으로 잡는 근거이기도 하다).
            k.store.get_jobset(self.jobset_id)   # 존재 검증 (없으면 예외)
            recs = [r for r in k.store.get_jobs(self.jobset_id)
                    if r.state.is_on_lsf]
            targets = sorted({str(r.job_id) for r in recs
                              if r.job_id is not None})
            requested = len(recs)
            rec_target = (lambda r: str(r.job_id))
            label = "chunk(sourced)" if self.envpath else "chunk"

        # --- 공유 꼬리: confirm 실행 + 전략 기록 + optimistic 대상 산출 ---
        # verify_targets: "잔존(still_alive)"을 셀 target 문자열 집합 — job_id
        # 만으로 세면 element 1개 kill에 형제 element가 잔존으로 오집계된다
        # (_verify가 "id[idx]"를 (id,idx) 정확 매칭하는 이유).
        verify_targets = set(targets)
        calls = unconfirmed = retries = 0
        resolved: set = set()
        if targets:
            for envpath, group in self._split_by_envpath(
                    recs, targets, rec_target).items():
                c, u, rt, res = self._kill_confirm(group, errors,
                                                   envpath=envpath)
                calls += c
                unconfirmed += u
                retries = max(retries, rt)
                resolved |= res
            strategies.append(label)
        # optimistic 대상 — per-id 확인이 있으므로 errors 유무와 무관하게
        # **확인된 target만** 마킹한다(미확인분은 on-LSF로 남아 폴링/재kill).
        # 개별 id 경로(recs=None)는 풀을 여기서야 조회한다 — kill은 이미
        # 끝났으므로 조회 실패(jobset 소실/비수치 id)는 마킹만 포기하고
        # 삼킨다(LSF job은 죽었다 — 레코드 정리는 폴링이 수렴시킨다).
        killed_recs: List = []
        if optimistic and resolved:
            if recs is None:
                try:
                    recs = self._record_pool()
                except Exception as e:       # noqa: BLE001 — 마킹만 포기
                    log.warning("optimistic 마킹용 레코드 조회 실패(무시): %r",
                                e)
                    recs = []
            killed_recs = [r for r in recs
                           if r.job_id is not None
                           and rec_target(r) in resolved]

        # verify를 optimistic 마킹보다 **먼저** 한다. 먼저 EXIT로 찍으면 그
        # 레코드가 재조회 대상(_ON_LSF)과 잔존 집계(is_on_lsf)에서 모두 빠져
        # verify가 생존 job을 영영 못 보고 항상 0을 반환한다(verify 무력화).
        # verify가 실측한 생존분(alive_keys)은 optimistic 마킹에서 제외해
        # EXIT로 덮어 숨기지 않고 폴링/재kill에 남긴다.
        still_alive: Optional[int] = None
        alive_keys: set = set()
        verify_changed: List = []
        if self.verify and self.jobset_id:
            still_alive, alive_keys, verify_changed = \
                self._verify(verify_targets)

        # optimistic 정책: 확인된 job을 즉시 EXIT로 전이 (bjobs 대기 없이).
        # EXIT는 terminal이라 폴링 대상(is_on_lsf)에서 빠져 다시 조회되지 않는다.
        changed: List = []
        if optimistic and killed_recs:
            to_mark = ([r for r in killed_recs if r.job_key not in alive_keys]
                       if alive_keys else killed_recs)
            changed = self._mark_exited(to_mark)

        # verify 조회가 수행한 전이도 report.changed로 합류시킨다 — verify가
        # terminal(EXIT/DONE)로 전이시킨 job은 이후 폴링(_ON_LSF만 조회)에도,
        # optimistic 마킹(is_on_lsf guard)에도 다시 안 잡혀 행 갱신 신호가
        # 영구 유실됐다(리뷰 M5). verify 전이 → optimistic 마킹 순서라
        # 중복 시에도 최신(optimistic) 레코드가 batch 뒤에 온다.
        return KillReport(
            jobset_id=self.jobset_id, requested=requested,
            strategies=strategies, command_calls=calls,
            still_alive=still_alive, unconfirmed=unconfirmed,
            kill_retries=retries, changed=verify_changed + changed,
            errors=errors)

    @staticmethod
    def _id_str(rec) -> str:
        return (f"{rec.job_id}[{rec.array_index}]"
                if rec.array_index is not None else str(rec.job_id))

    def _split_by_envpath(self, recs, targets: List[str],
                          rec_target) -> Dict[str, List[str]]:
        """target을 실행 envpath별로 분류 (MC — cluster_envpaths 모드).

        관측된 cluster(forward 우선, 없으면 source)가 매핑에 있으면 그
        env를, 미상/매핑 없음은 기본 envpath(self.envpath)를 쓴다.
        cluster_envpaths 미지정이면 전부 기본 envpath 한 그룹 — 기존 동작.
        개별 id 경로(recs=None)는 분류에만 레코드 풀이 필요하므로 지연
        조회하되, 실패해도 kill이 무산되지 않게 빈 풀로 진행한다(F1 계약)."""
        if not self.cluster_envpaths:
            return {self.envpath: list(targets)}
        if recs is None:
            try:
                recs = self._record_pool()
            except Exception as e:       # noqa: BLE001 — 분류만 포기
                log.warning("cluster 분류용 레코드 조회 실패(기본 env로 "
                            "진행): %r", e)
                recs = []
        env_of = {}
        for r in recs:
            if r.job_id is None:
                continue
            cluster = r.forward_cluster or r.source_cluster
            if cluster and cluster in self.cluster_envpaths:
                env_of[rec_target(r)] = self.cluster_envpaths[cluster]
        groups: Dict[str, List[str]] = {}
        for t in targets:
            groups.setdefault(env_of.get(t, self.envpath), []).append(t)
        if len(groups) > 1 or any(e for e in groups):
            log.info("kill env 분류: %s",
                     {e or "(local)": len(g) for e, g in groups.items()})
        return groups

    def _record_pool(self) -> List:
        """kill_jobs(원시 id) 대상의 레코드 후보 풀 — jobset_id를 알면 그
        jobset에서, 모르면 parent id로 전역 검색 ("id[idx]"도 정규화)."""
        if self.jobset_id:
            return self.killer.store.get_jobs(self.jobset_id)
        return self.killer.store.find_jobs(
            {int(str(i).split("[", 1)[0]) for i in (self.job_ids or [])})

    def _mark_exited(self, recs: List) -> List:
        """확인된 kill 대상을 EXIT로 전이 (아직 on-lsf인 것만, guard로 CAS).
        반환: 실제 전이된 레코드."""
        changed = []
        for r in recs:
            # r.jobset_id 사용 — kill_jobs 전역 검색은 대상이 여러 JobSet에
            # 걸칠 수 있어 self.jobset_id(빈 값)로는 안 된다
            new = self.killer.store.transition(
                r.jobset_id, r.job_key, JobState.EXIT,
                fail_reason="KILLED",
                guard=lambda cur: cur.state.is_on_lsf)
            if new is not None:
                changed.append(new)
        return changed

    def _kill_confirm(self, targets: List[str], errors: List[str],
                      envpath: Optional[str] = None
                      ) -> Tuple[int, int, int, set]:
        """concrete-id kill — bkill 출력의 확인('is being terminated' 등)을
        보고 미확인분을 재시도한다 (submit retry와 대칭, FR-3.4).
        envpath 미지정 시 task 기본값(self.envpath) — MC 분류 kill은 그룹별
        envpath를 명시 전달한다.
        반환: (LSF 호출 횟수, 최종 미확인 수, 재시도 라운드 수, 해소된 id 집합)."""
        k = self.killer
        cfg = k.command.config
        env = self.envpath if envpath is None else envpath
        total = len(targets)
        pending = set(targets)
        resolved_all: set = set()
        calls = 0
        attempt = 0
        while True:
            base = len(resolved_all)         # 이번 라운드 시작 시 확인분
            resolved, c = k.command.bkill_targets_confirm(
                sorted(pending), envpath=env,
                on_progress=lambda done: self._emit_progress(base + done, total))
            calls += c
            resolved_all |= resolved
            pending -= resolved
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


    def _verify(self, targets: set) -> Tuple[Optional[int], set, List]:
        """재조회로 실제 종료 확인 (FR-3.3).
        반환: (잔존 수, 잔존 job_key 집합, 조회가 전이시킨 레코드 목록 —
        caller가 report.changed에 합류시켜 신호 유실을 막는다).
        kill 대상(targets: "id"/"id[idx]"/"id[m-n]" 문자열) 중 아직 LSF에
        잔존하는 job을 센다 — 부분/개별 kill에서 대상 아닌 job은 세지 않는다.
        element 지정("id[idx]")은 그 element만, 범위("id[m-n]")는 그 범위
        element만, bare id("id")는 그 job_id 전체. 파싱 불가한 target은 bare
        id로 관대 처리(강건성 — 예외로 kill_finished가 오보되지 않게).
        targets가 비면 (0, set(), []), 조회 실패는 (None, set(), [])=미검증."""
        k = self.killer
        if not targets:
            return 0, set(), []
        exact: set = set()          # (job_id, array_index) — element 지정
        ranges: List[Tuple[int, int, int]] = []  # (job_id, lo, hi) — 범위 지정
        whole: set = set()          # job_id — bare id (비array/array 전체)
        for t in targets:
            t = str(t)
            m = re.match(r"^(\d+)(?:\[(\d+)(?:-(\d+))?\])?$", t)
            if m is None:           # 예상 밖 형식 — bare id 시도, 그래도 안 되면 skip
                head = t.split("[", 1)[0]
                if head.isdigit():
                    whole.add(int(head))
                else:
                    log.warning("kill verify: 파싱 불가 target 무시: %r", t)
                continue
            pid = int(m.group(1))
            if m.group(2) is None:                 # bare id
                whole.add(pid)
            elif m.group(3) is None:               # 단일 element [idx]
                exact.add((pid, int(m.group(2))))
            else:                                  # 범위 [m-n]
                ranges.append((pid, int(m.group(2)), int(m.group(3))))
        try:
            qr = k.querier.query(self.jobset_id)   # Store 갱신 + 전이 수집
        except LsfmgrError as e:
            log.warning("kill verify 조회 실패: %s", e)
            # 조회 실패는 '미확인'이다 — 수(-1 같은 센티넬)로 뭉개지 않는다.
            # None을 caller가 KillReport.still_alive(None=미검증)로 그대로 전달.
            return None, set(), []

        # element/범위 target은 (job_id, array_index)로 정확 매칭한다.
        # array_index=None 레코드(비array job, 또는 monitor가 array를 접은
        # 집계 레코드)는 그 자체가 "여러 element의 합"이라 특정 element target으로
        # 잔존 여부를 판정할 수 없다 — 형제 element를 잔존으로 과대집계하지 않도록
        # element/범위 target에는 걸지 않는다. 전체 kill(bare id, whole)만 집계.
        def _hit(r) -> bool:
            if r.job_id in whole:                # bare id — element 전부 포함
                return True
            if r.array_index is None:            # 집계/비array 레코드 — element 판정 불가
                return False
            if (r.job_id, r.array_index) in exact:
                return True
            return any(r.job_id == pid and lo <= r.array_index <= hi
                       for pid, lo, hi in ranges)

        alive = [r for r in k.store.get_jobs(self.jobset_id)
                 if _hit(r) and r.state.is_on_lsf
                 and r.state not in (JobState.UNKWN, JobState.ZOMBI)]
        # 생존 수 + 생존 레코드의 job_key 집합 + 조회 전이분을 함께 반환한다.
        # 생존분은 caller가 optimistic EXIT 마킹에서 제외해(EXIT로 덮어 숨기지
        # 않도록) 폴링/재kill에 남기고, 전이분은 report.changed로 합류시킨다.
        return len(alive), {r.job_key for r in alive}, list(qr.changed)
