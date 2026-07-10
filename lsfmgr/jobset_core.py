"""JobSetManager — JobSet CRUD / 요약 / 손실 감지 / merge / close (FR-5).

Store와 LsfCommand만 사용 (Qt 비의존). LSF 호출이 필요한 메서드
(detect_lost, close의 bgdel, add_job의 sync_lsf)는 호출 스레드에서 blocking
실행되므로, GUI 앱에서는 manager(Facade)가 worker 스레드에서 호출한다.
"""
from __future__ import annotations

import getpass
import logging
import threading
import uuid
from dataclasses import replace
from datetime import datetime
from typing import Iterable, List, Optional, Sequence

from .command import LsfCommand
from .config import LsfConfig
from .errors import JobNotFoundError, LsfmgrError
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.jobset")


def generate_jobset_id() -> str:
    """timestamp + uuid 조합 (FR-5.1)."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"js_{ts}_{uuid.uuid4().hex[:8]}"


class JobSetManager:

    def __init__(self, store: JobSetStore, command: LsfCommand,
                 config: Optional[LsfConfig] = None):
        self.store = store
        self.command = command
        self.config = config or command.config
        # JobSetRecord read-modify-write 직렬화 — Store는 개별 연산만
        # 원자적이므로, intended_count/부착물 갱신처럼 "읽고-고쳐-쓰는"
        # 경로가 겹치면 한쪽 갱신이 유실된다 (예: worker의
        # add_array_attachment vs 사용자 스레드의 add_job)
        self._meta_lock = threading.RLock()

    # ------------------------------------------------------------------
    # 생성/부착물
    # ------------------------------------------------------------------
    def group_path_for(self, jobset_id: str) -> str:
        """사용자 격리 LSF group 경로 (CS-10)."""
        return f"{self.config.lsf_group_root}/{getpass.getuser()}/{jobset_id}"

    def create_jobset(self, intended_count: int, *, label: str = "",
                      tags: Sequence[str] = (), description: str = "",
                      parent: Optional[str] = None,
                      jobset_id: Optional[str] = None,
                      with_attachments: bool = True) -> JobSetRecord:
        jsid = jobset_id or generate_jobset_id()
        record = JobSetRecord(
            jobset_id=jsid, intended_count=intended_count,
            lsf_group_paths=[self.group_path_for(jsid)] if with_attachments else [],
            name_patterns=[f"{jsid}_*"] if with_attachments else [],
            array_job_ids=[],
            label=label, tags=list(tags), description=description,
            parent_jobset_id=parent, created_by=getpass.getuser(),
            created_at=datetime.now(), merged_from=[], session_id="",
            closed=False)
        return self.store.create_jobset(record)

    def add_array_attachment(self, jobset_id: str, array_job_id: int) -> None:
        with self._meta_lock:
            js = self.store.get_jobset(jobset_id)
            if array_job_id not in js.array_job_ids:
                self.store.update_jobset(replace(
                    js, array_job_ids=js.array_job_ids + [array_job_id]))

    # ------------------------------------------------------------------
    # job 추가 (FR-5.4)
    # ------------------------------------------------------------------
    def add_job(self, jobset_id: str, record: JobRecord, *,
                sync_lsf: bool = True) -> JobRecord:
        """job을 jobset에 편입. sync_lsf=True면 bmod -g로 LSF group도 동기화."""
        if record.jobset_id != jobset_id:
            record = replace(record, jobset_id=jobset_id)
        with self._meta_lock:
            js = self.store.get_jobset(jobset_id)
            # 동일 job_key 중복 거부 — store.add_job은 upsert라 조용히
            # 기존 레코드를 덮어쓴다(merge의 충돌 선검사와 동일 이유)
            try:
                self.store.get_job(jobset_id, record.job_key)
            except JobNotFoundError:
                pass
            else:
                raise ValueError(
                    f"job 이름 중복: {jobset_id}/{record.job_key} — "
                    f"기존 레코드를 덮어쓸 수 없습니다 (먼저 remove_job)")
            rec = self.store.add_job(record)
            # 수동 추가는 intended_count도 증가 (불변식 유지, FR-5.2)
            jobs_n = len(self.store.get_jobs(jobset_id))
            if jobs_n > js.intended_count:
                self.store.update_jobset(
                    replace(self.store.get_jobset(jobset_id),
                            intended_count=jobs_n))
        if sync_lsf and rec.job_id is not None and js.lsf_group_paths:
            self.command.bmod_group([rec.job_id], js.lsf_group_paths[0])
        return rec

    def create_jobs(self, jobset_id: str,
                    records: Sequence[JobRecord]) -> List[JobRecord]:
        """제출 전(CREATED) 레코드 일괄 생성 — 바구니 누적 (FR-5.4 확장).

        job_key 중복 + merge_id 중복(None 제외)을 선검사하고 단일 배치로
        추가한다 — merge_id는 jobset 내 논리 키라 유일해야 replace가
        결정적이다. intended_count는 1회 갱신."""
        records = list(records)
        if not records:
            return []
        with self._meta_lock:
            existing = self.store.get_jobs(jobset_id)
            keys = {r.job_key for r in existing}
            mids = {r.merge_id for r in existing if r.merge_id is not None}
            for rec in records:
                if rec.job_key in keys:
                    raise ValueError(
                        f"job 이름 중복: {jobset_id}/{rec.job_key}")
                if rec.merge_id is not None and rec.merge_id in mids:
                    raise ValueError(
                        f"merge_id 중복: {jobset_id}/{rec.merge_id}")
                keys.add(rec.job_key)
                if rec.merge_id is not None:
                    mids.add(rec.merge_id)
            out = self.store.add_jobs(records)
            js = self.store.get_jobset(jobset_id)
            if len(keys) > js.intended_count:
                self.store.update_jobset(
                    replace(js, intended_count=len(keys)))
        return out

    def merge_from(self, target_id: str, source_id: str, *,
                   force: bool = False) -> List[JobRecord]:
        """source jobset의 job들을 merge_id 규칙으로 target에 **in-place
        흡수**하고 source를 삭제한다 (target 핸들/테이블 연속).

        규칙 (FR-5.5 v9):
          - source job의 merge_id가 target에 존재 → **replace**: target의
            기존 job_key(물리 키)는 유지하고 내용/상태를 source 것으로 교체
            (테이블 행 연속). LSF의 실제 job은 건드리지 않는다 — 살아있는
            job을 force로 replace하면 그 LSF job의 정리는 caller(GUI) 책임.
          - merge_id가 없거나(None) target에 미존재 → 신규 추가.
        가드: 양쪽 모든 job이 비활성(CREATED/terminal)이어야 한다 — 활성
        (SUBMITTING/RETRY_WAIT/on-LSF)이 있으면 LsfmgrError, force면 진행.
        반환: target에서 replace/추가된 레코드 목록 (신호 발행용)."""
        if target_id == source_id:
            raise ValueError("같은 jobset끼리는 merge할 수 없습니다")
        with self._meta_lock:
            tgt = self.store.get_jobset(target_id)
            src = self.store.get_jobset(source_id)
            tgt_jobs = self.store.get_jobs(target_id)
            src_jobs = self.store.get_jobs(source_id)

            if not force:
                busy = [r.job_key for r in tgt_jobs + src_jobs
                        if not r.state.is_inactive]
                if busy:
                    raise LsfmgrError(
                        f"merge 불가 — 활성(진행 중) job {len(busy)}건: "
                        f"{busy[:5]} (force=True로 레코드만 강제 교체 가능)")

            by_mid = {r.merge_id: r for r in tgt_jobs
                      if r.merge_id is not None}
            tgt_keys = {r.job_key for r in tgt_jobs}
            changed: List[JobRecord] = []
            for rec in src_jobs:
                old = by_mid.get(rec.merge_id) if rec.merge_id else None
                if old is not None:
                    # replace — 물리 키(job_key)는 target 것 유지
                    new = replace(rec, jobset_id=target_id,
                                  lsf_job_name=old.job_key)
                    self.store.remove_job(target_id, old.job_key)
                    self.store.add_job(new)
                    changed.append(new)
                else:
                    if rec.job_key in tgt_keys:
                        raise ValueError(
                            f"merge 불가 — job 이름 충돌: {rec.job_key!r}")
                    new = replace(rec, jobset_id=target_id)
                    self.store.add_job(new)
                    tgt_keys.add(new.job_key)
                    changed.append(new)
            # 부착물 누적 (조회/kill 시 전부 순회, §1.1)
            self.store.update_jobset(replace(
                self.store.get_jobset(target_id),
                intended_count=len(self.store.get_jobs(target_id)),
                lsf_group_paths=_dedup(tgt.lsf_group_paths
                                       + src.lsf_group_paths),
                name_patterns=_dedup(tgt.name_patterns + src.name_patterns),
                array_job_ids=_dedup(tgt.array_job_ids + src.array_job_ids),
                merged_from=_dedup(tgt.merged_from + [source_id])))
            self.store.delete_jobset(source_id)
        return changed

    def remove_jobs(self, jobset_id: str, *,
                    job_id: Optional[int] = None,
                    merge_id: Optional[str] = None,
                    job_key: Optional[str] = None,
                    force: bool = False) -> List[JobRecord]:
        """job 삭제 — job_id / merge_id / job_key 중 하나로 지정 (FR-5.4 v9).

        비활성(CREATED/terminal)만 삭제 가능 — 활성이면 LsfmgrError,
        force=True면 레코드만 강제 삭제(LSF job 정리는 caller 책임).
        intended_count도 함께 줄여 유령 CREATED가 남지 않는다."""
        given = [x for x in (job_id, merge_id, job_key) if x is not None]
        if len(given) != 1:
            raise ValueError("job_id/merge_id/job_key 중 정확히 하나를 지정")
        with self._meta_lock:
            jobs = self.store.get_jobs(jobset_id)
            if job_id is not None:
                targets = [r for r in jobs if r.job_id == job_id]
            elif merge_id is not None:
                targets = [r for r in jobs if r.merge_id == merge_id]
            else:
                targets = [r for r in jobs if r.job_key == job_key]
            if not targets:
                raise JobNotFoundError(
                    f"{jobset_id}: 대상 없음 (job_id={job_id}, "
                    f"merge_id={merge_id}, job_key={job_key})")
            busy = [r.job_key for r in targets if not r.state.is_inactive]
            if busy and not force:
                raise LsfmgrError(
                    f"삭제 불가 — 활성(진행 중) job: {busy[:5]} "
                    f"(force=True로 레코드만 강제 삭제 가능)")
            for r in targets:
                self.store.remove_job(jobset_id, r.job_key)
            js = self.store.get_jobset(jobset_id)
            n = len(self.store.get_jobs(jobset_id))
            if js.intended_count != n:
                self.store.update_jobset(replace(js, intended_count=n))
        return targets

    def clear_jobs(self, jobset_id: str, *,
                   force: bool = False) -> List[JobRecord]:
        """전 job 삭제 — remove_jobs와 동일 가드 (활성이 있으면 예외,
        force로 강제). intended_count는 0이 된다."""
        with self._meta_lock:
            jobs = self.store.get_jobs(jobset_id)
            busy = [r.job_key for r in jobs if not r.state.is_inactive]
            if busy and not force:
                raise LsfmgrError(
                    f"clear 불가 — 활성(진행 중) job {len(busy)}건: "
                    f"{busy[:5]} (force=True로 강제 가능)")
            for r in jobs:
                self.store.remove_job(jobset_id, r.job_key)
            js = self.store.get_jobset(jobset_id)
            if js.intended_count != 0:
                self.store.update_jobset(replace(js, intended_count=0))
        return jobs

    def remove_job(self, jobset_id: str, job_key: str) -> JobRecord:
        """job을 jobset에서 제외하고 제거된 레코드를 반환 (add_job의 역연산).
        제거한 몫만큼 intended_count도 줄여 요약 불변식(총합 == intended_count,
        FR-5.2)을 유지한다 — 줄이지 않으면 빈 슬롯이 유령 CREATED로 되살아난다.
        LSF의 실제 job은 죽이지 않는다(저장소 추적에서만 제외)."""
        with self._meta_lock:
            js = self.store.get_jobset(jobset_id)
            rec = self.store.remove_job(jobset_id, job_key)   # 없으면 예외
            jobs_n = len(self.store.get_jobs(jobset_id))
            new_intended = max(jobs_n, js.intended_count - 1)
            if new_intended != js.intended_count:
                self.store.update_jobset(replace(
                    self.store.get_jobset(jobset_id),
                    intended_count=new_intended))
        return rec

    # ------------------------------------------------------------------
    # 손실 감지 (FR-5.3)
    # ------------------------------------------------------------------
    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """intended_count 대비 ID 미확보 job을 감지하고, name 패턴 조회로
        '실제로는 submit된 job'의 ID를 복구한다. 복구 불가면 LOST 전이.
        반환: 이번 호출로 LOST 확정된 레코드 목록."""
        js = self.store.get_jobset(jobset_id)
        records = self.store.get_jobs(jobset_id)
        # ID 미확보이면서 submit이 시도된 (실패 확정도 아닌) 레코드
        candidates = [r for r in records if r.job_id is None
                      and r.state in (JobState.SUBMITTING, JobState.LOST)]
        if not candidates:
            return []

        # name 패턴으로 LSF에서 이름 → job_id 역조회
        name_to_id = {}
        for pattern in js.name_patterns:
            try:
                for st in self.command.bjobs_by_name(pattern):
                    name_to_id[st.job_name] = st.job_id
            except LsfmgrError as e:
                log.warning("detect_lost 패턴 조회 실패 %s: %s", pattern, e)

        # guard(CAS): 스냅샷 이후 submit 재시도가 job_id를 채웠으면(정상 PEND)
        # 복구/LOST 확정 모두 건너뛴다 — 살아있는 레코드를 덮어쓰지 않는다
        lost: List[JobRecord] = []
        for rec in candidates:
            still = lambda cur, rec=rec: (cur.job_id is None       # noqa: E731
                                          and cur.state is rec.state)
            jid = name_to_id.get(rec.lsf_job_name)
            if jid is not None:
                new = self.store.transition(
                    jobset_id, rec.job_key, JobState.PEND,
                    job_id=jid, fail_reason=None, guard=still)
                if new is not None:
                    log.info("손실 job 복구: %s → job_id=%d", rec.job_key, jid)
            elif rec.state is not JobState.LOST:
                new = self.store.transition(
                    jobset_id, rec.job_key, JobState.LOST,
                    fail_reason=rec.fail_reason or "NO_JOBID_PARSED",
                    guard=still)
                if new is not None:
                    lost.append(new)
        return lost

    # ------------------------------------------------------------------
    # 종결 (FR-5.7)
    # ------------------------------------------------------------------
    def close_jobset(self, jobset_id: str, *, force: bool = False,
                     run_bgdel: bool = True) -> JobSetRecord:
        """전원 terminal이면 close. LSF group은 bgdel로 정리.

        run_bgdel=False면 bgdel을 생략 — 호출자(manager)가 worker 스레드에서
        비동기 수행할 때 사용 (main 스레드 LSF 호출 방지, QT-1)."""
        js = self.store.get_jobset(jobset_id)
        records = self.store.get_jobs(jobset_id)
        not_terminal = [r for r in records if not r.state.is_terminal]
        if not_terminal and not force:
            raise LsfmgrError(
                f"terminal이 아닌 job {len(not_terminal)}개 — close 불가 "
                f"(force=True로 강제 가능)")
        if run_bgdel:
            for path in js.lsf_group_paths:
                self.command.bgdel(path)
        return self.store.update_jobset(replace(js, closed=True))


def detect_array_template(commands: Sequence[str]) -> Optional[str]:
    """AUTO-4 — command 목록이 array로 표현 가능하면 $LSB_JOBINDEX 템플릿 반환.

    조건: 모든 command의 숫자 외 골격이 동일하고, 달라지는 숫자 필드가
    전부 1..N 인덱스와 일치. 전부 동일한 command면 command 자체를 반환.
    불가하면 None (→ bulk 방식).
    """
    import re
    if len(commands) < 2:
        return None
    tokens = [re.split(r"(\d+)", c) for c in commands]
    n_tok = len(tokens[0])
    if any(len(t) != n_tok for t in tokens):
        return None
    template = list(tokens[0])
    for pos in range(n_tok):
        column = [t[pos] for t in tokens]
        if pos % 2 == 0:                        # 숫자 아닌 골격 — 전부 동일해야
            if len(set(column)) != 1:
                return None
        elif len(set(column)) != 1:             # 달라지는 숫자 — 인덱스여야
            # 문자열 비교 필수: int 비교면 "01" == 1로 오판해
            # $LSB_JOBINDEX(=1) 치환 시 run_01 → run_1 오실행이 된다
            if all(column[i] == str(i + 1) for i in range(len(column))):
                # 중괄호 필수: "run_$LSB_JOBINDEX_final"이면 셸이 변수명을
                # LSB_JOBINDEX_final로 흡수해 빈 문자열로 확장된다
                template[pos] = "${LSB_JOBINDEX}"
            else:
                return None
    return "".join(template)


def _dedup(items: Iterable) -> list:
    """순서 보존 중복 제거."""
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
