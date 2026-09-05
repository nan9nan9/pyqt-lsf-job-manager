"""CompletionTracker — jobset '완료'의 단일 소유자.

manager에 흩어져 있던 세 기구를 한 곳에 모은다(부품 — Signal은 manager의
것을 발화한다):

1. **jobset_finished 1회 통지** — 전원 terminal 도달 시 1회. latch로
   보장하며, 다시 non-terminal이 보이면(재제출/add_jobs) 자동 재무장.
2. **post_process 실행** — 전원 terminal 도달 시 worker에서 1회. 등록은
   submit(post_process=fn), 무장은 착수 확정(confirm) 시점.
3. **제출 사이클 보류 무장분(token 프로토콜)** — stage(제출 접수) →
   confirm(착수 확정, records_reset) / discard(착수 없음, gate_rejected).
   token이 사이클 정체성이라 큐에 남은 낡은 신호가 새 사이클의 보류분을
   건드리지 못한다.

셋은 한 몸이다: 무장·해제·latch가 같은 생명주기를 공유해서, 따로 두면
"한쪽만 정리"되는 실수(옛 post_process 오발화, latch 잔존으로 통지 유실)가
생긴다 — remove_jobset 정리도 forget() 한 번으로 끝나야 한다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set

from .errors import LsfmgrError
from .qt import QRunnable, QThreadPool

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


class CompletionTracker:
    """완료 추적 부품 — LsfJobManager가 1개를 소유한다.

    Signal(jobset_finished/post_processing_*)과 store/submitter는 manager의
    것을 쓴다 — 이 클래스는 상태(latch/무장분)와 판정 규칙만 소유한다."""

    def __init__(self, mgr):
        self._mgr = mgr
        # 착수 확정 시 무장하고 전원 terminal 도달 시 한 번 실행할 후처리 콜백.
        self._post_process: Dict[str, Callable] = {}
        self._pool = QThreadPool(mgr)
        self._pool.setMaxThreadCount(2)
        # 완료 통지 latch. non-terminal 관측 또는 새 사이클 착수 확정 시 해제한다.
        self._finished_latch: Set[str] = set()
        # 제출 사이클별 보류 무장분 (무장/폐기 규칙은 _PendingArm docstring)
        self._pending_arm: Dict[str, _PendingArm] = {}

    # ------------------------------------------------------------------
    # 제출 사이클 무장 프로토콜 — stage → confirm / discard
    # ------------------------------------------------------------------
    def stage(self, jobset_id: str, token: object, keys: List[str],
              post_process: Optional[Callable],
              poll_interval_s: Optional[float]) -> None:
        """[main] 제출 접수 — 이전 제출의 잔여 무장을 해제하고(이번 호출
        기준으로만 무장) 이 사이클의 보류분을 등록한다. 실제 무장은
        confirm(착수 확정)이 한다 — 먼저 하면 게이트/취소 창에서 이전
        실행의 terminal 레코드에 post_process가 오발화한다."""
        self._post_process.pop(jobset_id, None)
        self._pending_arm[jobset_id] = _PendingArm(
            token, keys, post_process, poll_interval_s)

    def confirm(self, jobset_id: str, token: object) -> Optional[_PendingArm]:
        """[main] 착수 확정(records_reset) — 보류분을 소진하고 무장한다.
        token 불일치(다른 사이클의 낡은 신호)면 None(무시).

        post_process는 여기서 무장하고, 완료 통지 latch를 푼다 — 새 실행
        착수이므로. 폴링 없이 곧장 전원 terminal로 끝나는 사이클(예: bsub
        전량 거부)은 그 사이 완료 감지가 한 번도 안 돌아 latch가 자동
        해제되지 못하기 때문에 여기서 명시적으로 푼다.
        rearm/폴링 재개는 반환값을 받은 manager의 몫이다."""
        ent = self._pending_arm.get(jobset_id)
        if ent is None or ent.token is not token:
            return None
        self._pending_arm.pop(jobset_id, None)
        self._finished_latch.discard(jobset_id)
        if ent.post_process is not None:
            self._post_process[jobset_id] = ent.post_process
        return ent

    def discard(self, jobset_id: str, token: object) -> None:
        """[main] 착수 없이 끝남(게이트 거부/예외/shutdown/born-cancelled) —
        이 사이클의 보류분을 폐기한다. token 불일치면 다른 사이클의 낡은
        신호이므로 무시(새 사이클의 보류분을 파괴하지 않는다)."""
        ent = self._pending_arm.get(jobset_id)
        if ent is not None and ent.token is token:
            self._pending_arm.pop(jobset_id, None)

    # ------------------------------------------------------------------
    # 완료 감지
    # ------------------------------------------------------------------
    @staticmethod
    def _all_terminal(recs: list) -> bool:
        """완료 판정 공용 술어 — job이 1개 이상 있고 전원 terminal.
        (핸들의 is_done은 intended_count 기준 summary 판정 — 별개 계약)"""
        return bool(recs) and all(r.state.is_terminal for r in recs)

    def maybe_finish(self, jobset_id: str) -> None:
        """전원 terminal 도달 감지의 **공통 지점** — 폴링/query_once/submit
        완료에서 호출된다. 두 가지를 처리한다:

          1. jobset_finished(요약) 발화 — post_process 등록과 **무관**하게
             LSF 상태만 보고 전원 terminal이면 1회.
          2. 등록돼 있으면 post_process를 worker에서 1회 실행.

        1회성은 서로 다른 방식으로 보장한다 — post_process는 무장 해제(pop),
        jobset_finished는 latch. latch는 다시 non-terminal이 보이면(재제출 등)
        스스로 풀려 다음 완료에 또 발화한다."""
        mgr = self._mgr
        if mgr._shutdown_done:
            return
        if mgr.submitter.is_active(jobset_id):
            # 제출(게이트 포함) 진행 중 — 레코드가 아직 이전 실행의 terminal
            # 상태로 남아 있는 창이다. 이번 실행의 완료가 아니므로 미룬다.
            return
        try:
            recs = mgr.store.get_jobs(jobset_id)
        except LsfmgrError:
            self._post_process.pop(jobset_id, None)   # jobset 소멸 — 무장 해제
            self._finished_latch.discard(jobset_id)
            return
        if not self._all_terminal(recs):
            self._finished_latch.discard(jobset_id)   # 다시 활성 — 재무장
            return
        # 완료 슬롯의 재제출이 현재 후처리를 지우지 않도록 신호 발행 전에 꺼내 둔다.
        fn = self._post_process.pop(jobset_id, None)  # 한 번만
        if all(r.killed for r in recs):
            # 전원이 이 manager의 kill로 끝난 경우 자동 완료 통지를 생략한다.
            # actual 정책으로 나중에 종료가 확인되어도 killed 표식으로 판정한다.
            self._finished_latch.add(jobset_id)
        if jobset_id not in self._finished_latch:
            try:
                summary = mgr.store.summary(jobset_id)
            except LsfmgrError:
                return       # jobset이 방금 사라짐 — 빈 요약을 쏘면 구독자가
                             # s["total"]에서 깨진다. latch도 세우지 않는다.
            self._finished_latch.add(jobset_id)
            mgr.jobset_finished.emit(jobset_id, summary)
        if fn is None:
            return
        if mgr._shutdown_done:
            # 종료된 pool에는 작업을 추가하지 않는다.
            # 재제출은 현재 완료의 후처리를 막지 않는다. 이 콜백은 이전 실행의 스냅샷을 사용한다.
            return
        mgr.post_processing_started.emit(jobset_id)
        if mgr._shutdown_done:
            # started의 직접 연결 슬롯에서도 shutdown할 수 있다. 그 슬롯이
            # 풀을 정리한 뒤에는 새 후처리 작업을 접수하지 않는다.
            return
        self._pool.start(_PostProcessTask(mgr, jobset_id, fn, recs))

    def mute_after_kill(self, jobset_id: str) -> None:
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
        if not jobset_id or jobset_id in self._finished_latch:
            return
        try:
            recs = self._mgr.store.get_jobs(jobset_id)
        except LsfmgrError:
            return
        if self._all_terminal(recs):
            self._finished_latch.add(jobset_id)

    # ------------------------------------------------------------------
    # 정리
    # ------------------------------------------------------------------
    def forget(self, jobset_id: str) -> None:
        """[main] jobset 소멸(remove_jobset) — 이 jobset에 매달린 무장분·
        latch를 한 번에 정리한다."""
        self._post_process.pop(jobset_id, None)
        self._pending_arm.pop(jobset_id, None)
        self._finished_latch.discard(jobset_id)

    def shutdown(self) -> None:
        """post_process worker 전부 join (manager.shutdown 단계)."""
        self._pool.waitForDone(-1)


class _PostProcessTask(QRunnable):
    """전원 terminal 도달 후처리 콜백을 worker 스레드에서 1회 실행.
    반환값은 post_processing_finished로, 예외는 error_occurred +
    post_processing_finished(None)으로 통지 (예외 격리)."""

    def __init__(self, mgr, jsid: str, fn, records: list):
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
