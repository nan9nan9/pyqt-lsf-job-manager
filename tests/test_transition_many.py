"""transition_many — 대량 폴링 전이의 배치 API 계약 테스트 (두 백엔드 공통).

수만 건 전이가 한 폴링 사이클에 몰릴 때 건당 트랜잭션(sqlite) / 건당 lock
(memory)을 없애 폴링 스레드 블로킹을 줄이는 최적화. 개별 transition과
동일한 guard(CAS)·키 소실·이벤트 기록 계약을 지켜야 한다.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from lsfmgr.states import JobRecord, JobSetRecord, JobState


def _seed(store, n=5):
    store.insert_jobset(JobSetRecord(jobset_id="js", intended_count=n,
                                     created_at=datetime.now()))
    store.add_jobs([
        JobRecord(job_id=1000 + i, array_index=None, jobset_id="js",
                  lsf_job_name=f"js_{i}", state=JobState.PEND, command="e")
        for i in range(n)])


def test_transition_many_applies_all(store):
    _seed(store, 3)
    specs = [(f"js_{i}", JobState.RUN, None, {"start_time": datetime.now()})
             for i in range(3)]
    changed = store.transition_many("js", specs)
    assert len(changed) == 3
    assert all(r.state is JobState.RUN for r in changed)
    assert all(store.get_job("js", f"js_{i}").state is JobState.RUN
               for i in range(3))


def test_transition_many_guard_rejects(store):
    _seed(store, 3)
    # js_1은 이미 RUN이 아니라는 guard로 걸러지게 한다
    store.transition("js", "js_1", JobState.RUN)
    specs = [
        ("js_0", JobState.DONE, lambda cur: cur.state is JobState.PEND, {}),
        ("js_1", JobState.DONE, lambda cur: cur.state is JobState.PEND, {}),
        ("js_2", JobState.DONE, lambda cur: cur.state is JobState.PEND, {}),
    ]
    changed = store.transition_many("js", specs)
    keys = {r.job_key for r in changed}
    assert keys == {"js_0", "js_2"}                # js_1은 guard 거부
    assert store.get_job("js", "js_1").state is JobState.RUN


def test_transition_many_missing_key_skipped(store):
    _seed(store, 2)
    specs = [
        ("js_0", JobState.DONE, None, {}),
        ("nonexistent", JobState.DONE, None, {}),   # 사이클 도중 remove된 키
        ("js_1", JobState.DONE, None, {}),
    ]
    changed = store.transition_many("js", specs)   # 예외 없이 진행
    assert {r.job_key for r in changed} == {"js_0", "js_1"}


def test_transition_many_rejects_key_fields(store):
    _seed(store, 1)
    with pytest.raises(ValueError):
        store.transition_many(
            "js", [("js_0", JobState.RUN, None, {"jobset_id": "other"})])


def test_transition_many_empty(store):
    _seed(store, 1)
    assert store.transition_many("js", []) == []


def test_transition_many_matches_individual(store):
    """배치와 개별 transition의 결과가 동일해야 한다 (계약 등가성)."""
    _seed(store, 4)
    specs = [(f"js_{i}", JobState.RUN, None, {"exit_code": None})
             for i in range(4)]
    batch = store.transition_many("js", specs)
    # 다시 개별로 되돌렸다가 개별 적용
    for i in range(4):
        store.transition("js", f"js_{i}", JobState.PEND)
    indiv = [store.transition("js", f"js_{i}", JobState.RUN, exit_code=None)
             for i in range(4)]
    assert [(r.job_key, r.state) for r in batch] \
        == [(r.job_key, r.state) for r in indiv]


