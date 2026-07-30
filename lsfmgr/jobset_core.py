"""JobSetManager — JobSet CRUD / 요약 / 손실 감지 / merge / close.

Store만 사용 (Qt 비의존). v10: LSF 호출(부착물 name 역조회·bgdel)이
전부 제거되어 이 계층은 순수 Store 연산이다.

이름 규약(계층이 이름에 드러나게): 공개 API=mgr.create_jobset/remove_job/close,
도메인=local_* (local_create_jobset/local_create_jobs, local_remove_jobs/
local_clear_jobs, local_close_jobset, merge_from), 저장소=store_*
(store_insert_jobset/store_add_jobs/store_delete_job/store_dispose).
"""
from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import replace
from datetime import datetime
from typing import Iterable, List, Optional, Sequence

from .errors import (
    CloseNotAllowedError,
    JobNotFoundError,
    MergeNotAllowedError,
    RemoveNotAllowedError,
)
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.jobset")


def generate_jobset_id() -> str:
    """timestamp + uuid 조합."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"js_{ts}_{uuid.uuid4().hex[:8]}"


class JobSetManager:

    def __init__(self, store: JobSetStore):
        # (v10.1: LsfCommand/config 의존 제거 — LSF 호출이 전부 사라져
        #  이 계층은 선언대로 순수 Store 연산이다)
        self.store = store
        # JobSetRecord read-modify-write 직렬화 — Store는 개별 연산만
        # 원자적이므로, intended_count 갱신처럼 "읽고-고쳐-쓰는" 경로가 겹치면
        # 한쪽 갱신이 유실된다 (예: new_jobs vs merge_from의 intended_count 갱신).
        self._meta_lock = threading.RLock()

    # ------------------------------------------------------------------
    # 생성
    # ------------------------------------------------------------------
    def local_create_jobset(self, intended_count: int, *, label: str = "",
                            tags: Sequence[str] = (), description: str = "",
                            jobset_id: Optional[str] = None) -> JobSetRecord:
        jsid = jobset_id or generate_jobset_id()
        record = JobSetRecord(
            jobset_id=jsid, intended_count=intended_count,
            label=label, tags=list(tags), description=description,
            created_at=datetime.now(), merged_from=[], closed=False)
        return self.store.store_insert_jobset(record)

    # ------------------------------------------------------------------
    # job 추가 — 생성은 local_create_jobs, 이후 추가는 merge_from만
    # ------------------------------------------------------------------
    def local_create_jobs(self, jobset_id: str,
                          records: Sequence[JobRecord]) -> List[JobRecord]:
        """제출 전(CREATED) 레코드 일괄 생성 — 바구니 누적.

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
                if rec.jobset_id != jobset_id:
                    # 남의 jobset에 끼워 넣으면 그 jobset의 summary 불변식이
                    # 깨지고 이 jobset엔 유령 intended_count가 남는다
                    raise ValueError(
                        f"레코드 jobset_id 불일치: {rec.jobset_id!r} != "
                        f"{jobset_id!r} ({rec.job_key})")
                if rec.job_key in keys:
                    raise ValueError(
                        f"job 이름 중복: {jobset_id}/{rec.job_key}")
                if rec.merge_id is not None and rec.merge_id in mids:
                    raise ValueError(
                        f"merge_id 중복: {jobset_id}/{rec.merge_id}")
                keys.add(rec.job_key)
                if rec.merge_id is not None:
                    mids.add(rec.merge_id)
            out = self.store.store_add_jobs(records)
            js = self.store.get_jobset(jobset_id)
            if len(keys) > js.intended_count:
                self.store.update_jobset(
                    replace(js, intended_count=len(keys)))
        return out

    def merge_from(self, target_id: str, source_id: str, *,
                   force: bool = False) -> List[JobRecord]:
        """source jobset의 job들을 merge_id 규칙으로 target에 **in-place
        흡수**하고 source를 삭제한다 (target 핸들/테이블 연속).

        규칙 (v9):
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
            self.store.get_jobset(source_id)     # 존재 검증 (없으면 예외)
            tgt_jobs = self.store.get_jobs(target_id)
            src_jobs = self.store.get_jobs(source_id)

            if not force:
                busy = [r.job_key for r in tgt_jobs + src_jobs
                        if not r.state.is_inactive]
                if busy:
                    raise MergeNotAllowedError(
                        f"merge 불가 — 활성(진행 중) job {len(busy)}건: "
                        f"{busy[:5]} (force=True로 레코드만 강제 교체 가능)",
                        jobset_id=target_id, job_keys=busy)

            by_mid = {r.merge_id: r for r in tgt_jobs
                      if r.merge_id is not None}
            tgt_keys = {r.job_key for r in tgt_jobs}
            # 키 충돌은 **변경 시작 전에** 전부 검증한다 (원자성) — 루프
            # 도중 예외면 앞선 job이 이미 target에 들어간 부분-반영 상태로
            # 중단되어, 같은 job이 양쪽에 중복 존재하고 intended_count가
            # 실제 레코드 수보다 작아져 summary 불변식이 영구 파손된다.
            dup = [r.job_key for r in src_jobs
                   if not (r.merge_id and r.merge_id in by_mid)
                   and r.job_key in tgt_keys]
            if dup:
                raise ValueError(
                    f"merge 불가 — job 이름 충돌: {dup[:5]!r}")
            changed: List[JobRecord] = []
            for rec in src_jobs:
                old = by_mid.get(rec.merge_id) if rec.merge_id else None
                if old is not None:
                    # replace — 물리 키(job_key)는 target 것 유지
                    new = replace(rec, jobset_id=target_id,
                                  job_key=old.job_key)
                    self.store.store_delete_job(target_id, old.job_key)
                    self.store.store_add_job(new)
                    changed.append(new)
                else:
                    new = replace(rec, jobset_id=target_id)
                    self.store.store_add_job(new)
                    changed.append(new)
            self.store.update_jobset(replace(
                self.store.get_jobset(target_id),
                intended_count=len(self.store.get_jobs(target_id)),
                merged_from=_dedup(tgt.merged_from + [source_id])))
            self.store.store_delete_jobset(source_id)
        return changed

    def local_remove_jobs(self, jobset_id: str, *,
                    job_id: Optional[int] = None,
                    merge_id: Optional[str] = None,
                    job_key: Optional[str] = None,
                    force: bool = False) -> List[JobRecord]:
        """job 삭제 — job_id / merge_id / job_key 중 하나로 지정 (v9).

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
                raise RemoveNotAllowedError(
                    f"삭제 불가 — 활성(진행 중) job: {busy[:5]} "
                    f"(force=True로 레코드만 강제 삭제 가능)",
                    jobset_id=jobset_id, job_keys=busy)
            for r in targets:
                self.store.store_delete_job(jobset_id, r.job_key)
            js = self.store.get_jobset(jobset_id)
            n = len(self.store.get_jobs(jobset_id))
            if js.intended_count != n:
                self.store.update_jobset(replace(js, intended_count=n))
        return targets

    def local_clear_jobs(self, jobset_id: str, *,
                   force: bool = False) -> List[JobRecord]:
        """전 job 삭제 — remove_jobs와 동일 가드 (활성이 있으면 예외,
        force로 강제). intended_count는 0이 된다."""
        with self._meta_lock:
            jobs = self.store.get_jobs(jobset_id)
            busy = [r.job_key for r in jobs if not r.state.is_inactive]
            if busy and not force:
                raise RemoveNotAllowedError(
                    f"clear 불가 — 활성(진행 중) job {len(busy)}건: "
                    f"{busy[:5]} (force=True로 강제 가능)",
                    jobset_id=jobset_id, job_keys=busy)
            for r in jobs:
                self.store.store_delete_job(jobset_id, r.job_key)
            js = self.store.get_jobset(jobset_id)
            if js.intended_count != 0:
                self.store.update_jobset(replace(js, intended_count=0))
        return jobs

    # ------------------------------------------------------------------
    # 손실 감지
    # ------------------------------------------------------------------
    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """intended_count 대비 ID 미확보 job을 감지해 LOST 전이한다.
        (v10: name 패턴 역조회 복구는 bjobs group/name 조회 제거와 함께
        삭제 — ID 미확보 job은 재조회 수단이 없어 바로 LOST 확정한다.)
        반환: 이번 호출로 LOST 확정된 레코드 목록."""
        self.store.get_jobset(jobset_id)     # 존재 검증 (없으면 예외)
        records = self.store.get_jobs(jobset_id)
        # ID 미확보이면서 submit이 시도된 (실패 확정도 아닌) 레코드
        candidates = [r for r in records if r.job_id is None
                      and r.state is JobState.SUBMITTING]

        # guard(CAS): 스냅샷 이후 submit 재시도가 job_id를 채웠으면(정상 PEND)
        # LOST 확정을 건너뛴다 — 살아있는 레코드를 덮어쓰지 않는다
        lost: List[JobRecord] = []
        for rec in candidates:
            still = lambda cur, rec=rec: (cur.job_id is None       # noqa: E731
                                          and cur.state is rec.state)
            new = self.store.transition(
                jobset_id, rec.job_key, JobState.LOST,
                fail_reason=rec.fail_reason or "NO_JOBID_PARSED",
                guard=still)
            if new is not None:
                lost.append(new)
        return lost

    # ------------------------------------------------------------------
    # 종결
    # ------------------------------------------------------------------
    def local_close_jobset(self, jobset_id: str, *,
                           force: bool = False) -> JobSetRecord:
        """전원 terminal이면 close. (v10: bgdel group 정리 제거 — 부착물이
        생성되지 않으므로 정리할 group도 없다.)"""
        # 이 클래스의 다른 JobSetRecord 갱신 경로와 같이 _meta_lock 아래에서
        # 읽고-고쳐-쓴다. 지금은 JobSetRecord 갱신이 전부 main 스레드(manager
        # 공개 API)라 경합이 없지만, 여기만 lock 밖이면 갱신 하나가 off-main으로
        # 옮겨지는 순간 close가 그 사이 바뀐 intended_count를 옛 값으로 되돌려
        # summary 불변식(합계==intended_count)이 영구 파손된다.
        with self._meta_lock:
            js = self.store.get_jobset(jobset_id)
            records = self.store.get_jobs(jobset_id)
            not_terminal = [r for r in records if not r.state.is_terminal]
            if not_terminal and not force:
                raise CloseNotAllowedError(
                    f"terminal이 아닌 job {len(not_terminal)}개 — close 불가 "
                    f"(force=True로 강제 가능)",
                    jobset_id=jobset_id,
                    job_keys=[r.job_key for r in not_terminal])
            return self.store.update_jobset(replace(js, closed=True))


def _dedup(items: Iterable) -> list:
    """순서 보존 중복 제거."""
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
