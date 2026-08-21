"""InMemoryStore — 기본 저장소. dict + RLock, 파일 미생성 (§5)."""
from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from ..errors import JobNotFoundError, JobSetNotFoundError
from ..states import JobRecord, JobSetRecord, JobState
from .base import JobSetStore, summary_from_counts


class InMemoryStore(JobSetStore):
    """프로세스 메모리에만 저장. 종료 시 JobSet 소멸 (LSF job 자체는 잔존)."""

    def __init__(self):
        self._lock = threading.RLock()          # store 접근 직렬화
        self._jobsets: Dict[str, JobSetRecord] = {}
        # jobset_id → {job_key → JobRecord}
        self._jobs: Dict[str, Dict[str, JobRecord]] = {}
        # jobset_id → {state.value → 개수}. summary()를 전수 스캔 없이 답하려고
        # **증분으로** 유지한다. 스캔판은 O(job수)라 대량 제출 중 main 스레드가
        # 그 값을 물었다(2만 건 실측: 호출당 15ms × 104회 = 1.6s, 게다가 그동안
        # store lock을 쥐고 있어 worker 전이까지 밀렸다).
        # 개수가 0이 된 상태는 키를 지운다 — "0인 상태는 키 자체가 없다"는
        # 요약 계약(README §5.5)을 카운트 단계에서 지킨다.
        # ※ 레코드 쓰기는 반드시 _put_rec/_drop_rec만 거친다.
        self._counts: Dict[str, Dict[str, int]] = {}

    # ------------------------------------------------------------------
    # JobSet CRUD
    # ------------------------------------------------------------------
    def store_insert_jobset(self, record: JobSetRecord) -> JobSetRecord:
        with self._lock:
            if record.jobset_id in self._jobsets:
                raise ValueError(f"jobset 중복: {record.jobset_id}")
            if record.created_at is None:
                record = replace(record, created_at=datetime.now())
            self._jobsets[record.jobset_id] = record
            self._jobs.setdefault(record.jobset_id, {})
            self._counts.setdefault(record.jobset_id, {})
            return record

    def exists(self, jobset_id: str) -> bool:
        with self._lock:
            return jobset_id in self._jobsets

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

    def store_delete_jobset(self, jobset_id: str) -> None:
        with self._lock:
            self._jobsets.pop(jobset_id, None)
            self._jobs.pop(jobset_id, None)
            self._counts.pop(jobset_id, None)

    def list_jobsets(self) -> List[JobSetRecord]:
        with self._lock:
            return list(self._jobsets.values())

    # ------------------------------------------------------------------
    # 레코드 쓰기 funnel — 여기만 self._jobs를 직접 건드린다
    # ------------------------------------------------------------------
    def _put_rec(self, record: JobRecord) -> None:
        """[lock 보유] 레코드 1건 삽입/교체 + 상태 카운트 갱신.
        이 함수를 우회해 self._jobs에 대입하면 summary가 조용히 어긋난다."""
        jobs = self._jobs[record.jobset_id]
        old = jobs.get(record.job_key)
        if old is not None:
            self._count_del(record.jobset_id, old.state)
        jobs[record.job_key] = record
        counts = self._counts.setdefault(record.jobset_id, {})
        key = record.state.value
        counts[key] = counts.get(key, 0) + 1

    def _drop_rec(self, jobset_id: str, job_key: str) -> Optional[JobRecord]:
        """[lock 보유] 레코드 1건 제거 + 카운트 갱신. 없으면 None."""
        jobs = self._jobs.get(jobset_id)
        if jobs is None:
            return None
        old = jobs.pop(job_key, None)
        if old is not None:
            self._count_del(jobset_id, old.state)
        return old

    def _count_del(self, jobset_id: str, state: JobState) -> None:
        """[lock 보유] 카운트 1 감소 — 0이 되면 키를 지운다."""
        counts = self._counts.get(jobset_id)
        if not counts:
            return
        key = state.value
        n = counts.get(key, 0) - 1
        if n > 0:
            counts[key] = n
        else:
            counts.pop(key, None)

    def _debug_counts_ok(self, jobset_id: str) -> bool:
        """증분 카운트가 실제 레코드와 일치하는가 (테스트/진단 전용).
        denormalize한 값은 쓰기 경로를 하나라도 빠뜨리면 조용히 어긋나므로,
        교차 검증 수단을 코드 옆에 둔다."""
        with self._lock:
            actual: Dict[str, int] = {}
            for rec in self._jobs.get(jobset_id, {}).values():
                actual[rec.state.value] = actual.get(rec.state.value, 0) + 1
            return actual == self._counts.get(jobset_id, {})

    # ------------------------------------------------------------------
    # JobRecord
    # ------------------------------------------------------------------
    def store_add_job(self, record: JobRecord) -> JobRecord:
        with self._lock:
            if record.jobset_id not in self._jobsets:
                raise JobSetNotFoundError(record.jobset_id)
            if record.job_key in self._jobs[record.jobset_id]:
                # 조용한 덮어쓰기 금지 — 교체는 update_job/transition의 몫
                # (add가 덮어쓰면 중복 키 버그가 무증상으로 데이터를 삼킨다)
                raise ValueError(
                    f"job_key 중복: {record.jobset_id}/{record.job_key}")
            if record.updated_at is None:
                record = replace(record, updated_at=datetime.now())
            self._put_rec(record)
            return record

    def store_add_jobs(self, records) -> List[JobRecord]:
        records = list(records)
        out: List[JobRecord] = []
        now = datetime.now()
        with self._lock:                        # lock 1회로 일괄 처리
            # 선검증 — 중간 실패 시 앞선 레코드만 반영되는 부분 적용을
            # 막는다 (일괄 연산의 원자성 계약). 키 중복(기존/배치 내)도
            # 여기서 거른다 — dict 대입의 조용한 덮어쓰기 방지.
            seen: set = set()
            for record in records:
                if record.jobset_id not in self._jobsets:
                    raise JobSetNotFoundError(record.jobset_id)
                k = (record.jobset_id, record.job_key)
                if k in seen or record.job_key in self._jobs[record.jobset_id]:
                    raise ValueError(
                        f"job_key 중복: {record.jobset_id}/{record.job_key}")
                seen.add(k)
            for record in records:
                if record.updated_at is None:
                    record = replace(record, updated_at=now)
                self._put_rec(record)
                out.append(record)
        return out

    def store_delete_job(self, jobset_id: str, job_key: str) -> JobRecord:
        with self._lock:
            jobs = self._jobs.get(jobset_id)
            if jobs is None or job_key not in jobs:
                raise JobNotFoundError(f"{jobset_id}/{job_key}")
            return self._drop_rec(jobset_id, job_key)

    def update_job(self, record: JobRecord) -> JobRecord:
        with self._lock:
            jobs = self._jobs.get(record.jobset_id)
            if jobs is None or record.job_key not in jobs:
                raise JobNotFoundError(
                    f"{record.jobset_id}/{record.job_key}")
            record = replace(record, updated_at=datetime.now())
            self._put_rec(record)
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

    def transition(self, jobset_id: str, job_key: str,
                   new_state: Optional[JobState],
                   guard=None, **fields: Any) -> Optional[JobRecord]:
        self._reject_key_fields(fields)
        with self._lock:                        # read-modify-write 원자성
            old = self.get_job(jobset_id, job_key)
            if guard is not None and not guard(old):
                return None                     # CAS 불일치 — 전이 건너뜀
            # new_state=None = 상태 유지(부분 갱신) — 계약은 base.transition
            new = replace(old, updated_at=datetime.now(),
                          state=old.state if new_state is None else new_state,
                          **fields)
            self._put_rec(new)
            return new

    def _transition_many_impl(self, jobset_id, specs):
        """lock 1회로 다건 전이 — 건당 lock acquire/release 제거.
        (키 필드 선검증은 base.transition_many 템플릿이 소유)"""
        out: List[JobRecord] = []
        now = datetime.now()
        with self._lock:
            jobs = self._jobs.get(jobset_id, {})
            for job_key, new_state, guard, fields in specs:
                old = jobs.get(job_key)
                if old is None:
                    continue                     # 사이클 도중 remove_jobs 등
                if guard is not None and not guard(old):
                    continue
                new = replace(
                    old, updated_at=now, **fields,
                    state=old.state if new_state is None else new_state)
                self._put_rec(new)
                out.append(new)
        return out

    # ------------------------------------------------------------------
    # 조회/검색
    # ------------------------------------------------------------------
    def summary(self, jobset_id: str) -> Dict[str, Any]:
        """증분 카운트에서 바로 만든다 — 전수 스캔 없음(O(상태 수))."""
        with self._lock:
            js = self.get_jobset(jobset_id)
            return summary_from_counts(js, self._counts.get(jobset_id, {}))

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
