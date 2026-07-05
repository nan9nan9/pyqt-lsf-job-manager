"""Store 계약 테스트 — InMemory/Sqlite 동일 스위트 통과 (수용 기준 10)."""
from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest

from lsfmgr.errors import JobNotFoundError, JobSetNotFoundError
from lsfmgr.states import JobRecord, JobSetRecord, JobState


def make_jobset(jsid="js1", n=3, **kw) -> JobSetRecord:
    defaults = dict(jobset_id=jsid, intended_count=n,
                    lsf_group_paths=[f"/lsfmgr/u/{jsid}"],
                    name_patterns=[f"{jsid}_*"], label="lbl",
                    tags=["t1", "t2"], created_by="u")
    defaults.update(kw)
    return JobSetRecord(**defaults)


def make_job(jsid="js1", idx=0, **kw) -> JobRecord:
    defaults = dict(job_id=None, array_index=None, jobset_id=jsid,
                    lsf_job_name=f"{jsid}_{idx}", state=JobState.CREATED,
                    command=f"run {idx}")
    defaults.update(kw)
    return JobRecord(**defaults)


# ----------------------------------------------------------------------
# JobSet CRUD
# ----------------------------------------------------------------------
def test_jobset_crud(store):
    js = store.create_jobset(make_jobset())
    assert js.created_at is not None
    got = store.get_jobset("js1")
    assert got.intended_count == 3
    assert got.lsf_group_paths == ["/lsfmgr/u/js1"]
    assert got.tags == ["t1", "t2"]

    from dataclasses import replace
    store.update_jobset(replace(got, label="new"))
    assert store.get_jobset("js1").label == "new"

    store.delete_jobset("js1")
    with pytest.raises(JobSetNotFoundError):
        store.get_jobset("js1")


def test_duplicate_jobset_rejected(store):
    store.create_jobset(make_jobset())
    with pytest.raises(ValueError):
        store.create_jobset(make_jobset())


def test_list_jobsets(store):
    store.create_jobset(make_jobset("a", 1))
    store.create_jobset(make_jobset("b", 2))
    assert {j.jobset_id for j in store.list_jobsets()} == {"a", "b"}


# ----------------------------------------------------------------------
# JobRecord
# ----------------------------------------------------------------------
def test_job_add_get(store):
    store.create_jobset(make_jobset())
    store.add_job(make_job(idx=0))
    rec = store.get_job("js1", "js1_0")
    assert rec.state is JobState.CREATED
    assert rec.updated_at is not None


def test_add_job_requires_jobset(store):
    with pytest.raises(JobSetNotFoundError):
        store.add_job(make_job(jsid="nope"))


def test_get_jobs_with_state_filter(store):
    store.create_jobset(make_jobset(n=3))
    store.add_job(make_job(idx=0, state=JobState.PEND, job_id=1))
    store.add_job(make_job(idx=1, state=JobState.RUN, job_id=2))
    store.add_job(make_job(idx=2))
    assert len(store.get_jobs("js1")) == 3
    only = store.get_jobs("js1", states={JobState.RUN, JobState.PEND})
    assert {r.lsf_job_name for r in only} == {"js1_0", "js1_1"}


def test_transition(store):
    store.create_jobset(make_jobset())
    store.add_job(make_job(idx=0))
    rec = store.transition("js1", "js1_0", JobState.PEND, job_id=123)
    assert rec.state is JobState.PEND
    assert rec.job_id == 123
    assert store.get_job("js1", "js1_0").state is JobState.PEND


def test_transition_missing_job(store):
    store.create_jobset(make_jobset())
    with pytest.raises(JobNotFoundError):
        store.transition("js1", "nope", JobState.PEND)


def test_remove_job(store):
    store.create_jobset(make_jobset(n=3))
    for i in range(3):
        store.add_job(make_job(idx=i, job_id=100 + i))
    # 제거 → 제거된 레코드 반환 + 나머지 유지
    rec = store.remove_job("js1", "js1_1")
    assert rec.lsf_job_name == "js1_1" and rec.job_id == 101
    assert {r.lsf_job_name for r in store.get_jobs("js1")} == {"js1_0", "js1_2"}
    with pytest.raises(JobNotFoundError):
        store.get_job("js1", "js1_1")


def test_remove_job_missing(store):
    store.create_jobset(make_jobset())
    with pytest.raises(JobNotFoundError):
        store.remove_job("js1", "nope")


def test_transition_guard_cas(store):
    # guard(CAS)가 False면 전이가 무시되고 None — 스냅샷 stale write 방어
    store.create_jobset(make_jobset(n=1))
    store.add_job(make_job(idx=0, job_id=100, state=JobState.RUN))
    # 스냅샷 이후 다른 경로가 레코드를 바꿨다고 가정 (job_id 100→200)
    store.transition("js1", "js1_0", JobState.PEND, job_id=200)
    # 옛 스냅샷(job_id=100, RUN) 기준의 stale 갱신 시도 → guard가 거부
    res = store.transition(
        "js1", "js1_0", JobState.EXIT, exit_code=1, job_id=100,
        guard=lambda cur: cur.job_id == 100 and cur.state is JobState.RUN)
    assert res is None
    cur = store.get_job("js1", "js1_0")
    assert cur.state is JobState.PEND and cur.job_id == 200   # 안 덮임
    # guard 통과 케이스
    ok = store.transition(
        "js1", "js1_0", JobState.RUN,
        guard=lambda cur: cur.job_id == 200)
    assert ok is not None and ok.state is JobState.RUN


def test_via_wrapper_roundtrip(store):
    store.create_jobset(make_jobset(n=2))
    store.add_job(make_job(idx=0, via_wrapper=True))
    store.add_job(make_job(idx=1))
    assert store.get_job("js1", "js1_0").via_wrapper is True
    assert store.get_job("js1", "js1_1").via_wrapper is False


def test_runtime_fields_roundtrip(store):
    # run_time_s/start_time/finish_time 저장·복원 (sqlite 컬럼 포함)
    store.create_jobset(make_jobset(n=1))
    store.add_job(make_job(idx=0, job_id=7))
    st = datetime(2026, 7, 5, 14, 0, 0)
    ft = datetime(2026, 7, 5, 14, 5, 30)
    store.transition("js1", "js1_0", JobState.DONE, exit_code=0,
                     run_time_s=330, start_time=st, finish_time=ft)
    got = store.get_job("js1", "js1_0")
    assert got.run_time_s == 330
    assert got.start_time == st and got.finish_time == ft


# ----------------------------------------------------------------------
# summary 불변식 (FR-5.2, 수용 기준 4)
# ----------------------------------------------------------------------
def test_summary_invariant(store):
    store.create_jobset(make_jobset(n=5))
    for i in range(5):
        store.add_job(make_job(idx=i))
    store.transition("js1", "js1_0", JobState.PEND, job_id=1)
    store.transition("js1", "js1_1", JobState.RUN, job_id=2)
    store.transition("js1", "js1_2", JobState.SUBMIT_FAILED)
    s = store.summary("js1")
    assert s["total"] == 5
    state_sum = sum(v for k, v in s.items() if k != "total")
    assert state_sum == 5                     # 불변식: 합계 == intended_count
    assert s["PEND"] == 1 and s["RUN"] == 1
    assert s["SUBMIT_FAILED"] == 1 and s["CREATED"] == 2


def test_summary_counts_missing_records_as_created(store):
    # 레코드 미생성분도 CREATED로 계상 → 합계 == intended
    store.create_jobset(make_jobset(n=10))
    store.add_job(make_job(idx=0, state=JobState.PEND, job_id=1))
    s = store.summary("js1")
    assert s["total"] == 10
    assert s["CREATED"] == 9
    assert sum(v for k, v in s.items() if k != "total") == 10


# ----------------------------------------------------------------------
# 검색 (FR-5.6)
# ----------------------------------------------------------------------
def test_search_by_tag_label_since(store):
    old = datetime.now() - timedelta(days=2)
    store.create_jobset(make_jobset("a", tags=["sweep"], label="x",
                                    created_at=old))
    store.create_jobset(make_jobset("b", tags=["sweep", "tt"], label="y"))
    store.create_jobset(make_jobset("c", tags=[], label="x"))

    assert {j.jobset_id for j in store.search(tag="sweep")} == {"a", "b"}
    assert {j.jobset_id for j in store.search(label="x")} == {"a", "c"}
    recent = store.search(since=datetime.now() - timedelta(hours=1))
    assert {j.jobset_id for j in recent} == {"b", "c"}


# ----------------------------------------------------------------------
# 동시성 (CS-1, 수용 기준 9)
# ----------------------------------------------------------------------
def test_concurrent_transitions(store):
    n_jobs, n_threads = 50, 8
    store.create_jobset(make_jobset(n=n_jobs))
    for i in range(n_jobs):
        store.add_job(make_job(idx=i))

    errors = []

    def worker(tid):
        try:
            for i in range(n_jobs):
                if i % n_threads == tid:
                    store.transition("js1", f"js1_{i}", JobState.PEND,
                                     job_id=i + 1)
                    store.transition("js1", f"js1_{i}", JobState.RUN)
        except Exception as e:                # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    s = store.summary("js1")
    assert s["RUN"] == n_jobs
    assert sum(v for k, v in s.items() if k != "total") == n_jobs
