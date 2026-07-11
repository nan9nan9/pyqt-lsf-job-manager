"""InMemoryStore — 기본 저장소. dict + RLock, 파일 미생성 (§4.4)."""
from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from ..errors import JobNotFoundError, JobSetNotFoundError
from ..states import JobRecord, JobSetRecord, JobState
from .base import JobSetStore, make_summary


class InMemoryStore(JobSetStore):
    """프로세스 메모리에만 저장. 종료 시 JobSet 소멸 (LSF job 자체는 잔존)."""

    def __init__(self):
        self._lock = threading.RLock()          # CS-1
        self._jobsets: Dict[str, JobSetRecord] = {}
        # jobset_id → {job_key → JobRecord}
        self._jobs: Dict[str, Dict[str, JobRecord]] = {}

    # ------------------------------------------------------------------
    # JobSet CRUD
    # ------------------------------------------------------------------
    def insert_jobset(self, record: JobSetRecord) -> JobSetRecord:
        with self._lock:
            if record.jobset_id in self._jobsets:
                raise ValueError(f"jobset 중복: {record.jobset_id}")
            if record.created_at is None:
                record = replace(record, created_at=datetime.now())
            self._jobsets[record.jobset_id] = record
            self._jobs.setdefault(record.jobset_id, {})
            return record

    def get_jobset(self, jobset_id: str) -> JobSetRecord:
        with self._lock:
            try:
                return self._jobsets[jobset_id]
            except KeyError:
                raise JobSetNotFoundError(jobset_id) from None

    def update_jobset(self, record: JobSetRecord) -> JobSetRecord:
        with self._lock:
            if record.jobset_id not in self._jobsets:
                raise JobSetNotFoundError(record.jobset_id)
            self._jobsets[record.jobset_id] = record
            return record

    def delete_jobset(self, jobset_id: str) -> None:
        with self._lock:
            self._jobsets.pop(jobset_id, None)
            self._jobs.pop(jobset_id, None)

    def list_jobsets(self) -> List[JobSetRecord]:
        with self._lock:
            return list(self._jobsets.values())

    # ------------------------------------------------------------------
    # JobRecord
    # ------------------------------------------------------------------
    def add_job(self, record: JobRecord) -> JobRecord:
        with self._lock:
            if record.jobset_id not in self._jobsets:
                raise JobSetNotFoundError(record.jobset_id)
            if record.updated_at is None:
                record = replace(record, updated_at=datetime.now())
            self._jobs[record.jobset_id][record.job_key] = record
            return record

    def add_jobs(self, records) -> List[JobRecord]:
        records = list(records)
        out: List[JobRecord] = []
        now = datetime.now()
        with self._lock:                        # lock 1회로 일괄 처리
            # 선검증 — 중간 실패 시 앞선 레코드만 반영되는 부분 적용을
            # 막는다 (일괄 연산의 원자성 계약)
            for record in records:
                if record.jobset_id not in self._jobsets:
                    raise JobSetNotFoundError(record.jobset_id)
            for record in records:
                if record.updated_at is None:
                    record = replace(record, updated_at=now)
                self._jobs[record.jobset_id][record.job_key] = record
                out.append(record)
        return out

    def delete_job(self, jobset_id: str, job_key: str) -> JobRecord:
        with self._lock:
            jobs = self._jobs.get(jobset_id)
            if jobs is None or job_key not in jobs:
                raise JobNotFoundError(f"{jobset_id}/{job_key}")
            return jobs.pop(job_key)

    def update_job(self, record: JobRecord) -> JobRecord:
        with self._lock:
            jobs = self._jobs.get(record.jobset_id)
            if jobs is None or record.job_key not in jobs:
                raise JobNotFoundError(
                    f"{record.jobset_id}/{record.job_key}")
            record = replace(record, updated_at=datetime.now())
            jobs[record.job_key] = record
            return record

    def get_job(self, jobset_id: str, job_key: str) -> JobRecord:
        with self._lock:
            try:
                return self._jobs[jobset_id][job_key]
            except KeyError:
                raise JobNotFoundError(f"{jobset_id}/{job_key}") from None

    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        with self._lock:
            if jobset_id not in self._jobsets:
                raise JobSetNotFoundError(jobset_id)
            recs = list(self._jobs.get(jobset_id, {}).values())
        if states is not None:
            recs = [r for r in recs if r.state in states]
        return recs

    def transition(self, jobset_id: str, job_key: str, new_state: JobState,
                   guard=None, **fields: Any) -> Optional[JobRecord]:
        self._reject_key_fields(fields)
        with self._lock:                        # read-modify-write 원자성 (CS-1)
            old = self.get_job(jobset_id, job_key)
            if guard is not None and not guard(old):
                return None                     # CAS 불일치 — 전이 건너뜀
            new = replace(old, state=new_state, updated_at=datetime.now(),
                          **fields)
            self._jobs[jobset_id][job_key] = new
            return new

    def transition_many(self, jobset_id, specs):
        """lock 1회로 다건 전이 — 건당 lock acquire/release 제거."""
        out: List[JobRecord] = []
        now = datetime.now()
        with self._lock:
            jobs = self._jobs.get(jobset_id, {})
            for job_key, new_state, guard, fields in specs:
                self._reject_key_fields(fields)
                old = jobs.get(job_key)
                if old is None:
                    continue                     # 사이클 도중 remove_job 등
                if guard is not None and not guard(old):
                    continue
                new = replace(old, state=new_state, updated_at=now, **fields)
                jobs[job_key] = new
                out.append(new)
        return out

    # ------------------------------------------------------------------
    # 조회/검색
    # ------------------------------------------------------------------
    def summary(self, jobset_id: str) -> Dict[str, Any]:
        with self._lock:
            js = self.get_jobset(jobset_id)
            return make_summary(js, self._jobs.get(jobset_id, {}).values())

    def search(self, *, tag: Optional[str] = None, label: Optional[str] = None,
               since: Optional[datetime] = None) -> List[JobSetRecord]:
        with self._lock:
            out = []
            for js in self._jobsets.values():
                if tag is not None and tag not in js.tags:
                    continue
                if label is not None and label != js.label:
                    continue
                if since is not None and (js.created_at is None
                                          or js.created_at < since):
                    continue
                out.append(js)
            return out
