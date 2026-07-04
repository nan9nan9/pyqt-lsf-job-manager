"""SqliteStore 전용 API + InMemory 예외 테스트 (FR-6, 수용 기준 11·12)."""
from __future__ import annotations

import os

import pytest

from lsfmgr import InMemoryStore, SqliteStore
from lsfmgr.errors import PersistenceNotSupportedError
from lsfmgr.states import JobState
from tests.test_store_contract import make_job, make_jobset


# ----------------------------------------------------------------------
# InMemory — 전용 API 거부 + 파일 미생성 (수용 기준 11)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("call", [
    lambda s: s.list_orphan_jobsets(),
    lambda s: s.recover_jobset("x"),
    lambda s: s.search_all_sessions(),
    lambda s: s.get_history("x"),
    lambda s: s.stats(),
    lambda s: s.archive(),
    lambda s: s.vacuum(),
    lambda s: s.export_jobset("x", "/tmp/x.json"),
])
def test_inmemory_rejects_persistent_api(call):
    store = InMemoryStore()
    assert store.persistent is False
    with pytest.raises(PersistenceNotSupportedError):
        call(store)


def test_inmemory_creates_no_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = InMemoryStore()
    store.create_jobset(make_jobset())
    store.add_job(make_job())
    assert list(tmp_path.iterdir()) == []


# ----------------------------------------------------------------------
# Sqlite — 세션 복원 (수용 기준 12)
# ----------------------------------------------------------------------
def test_orphan_and_recover(tmp_path):
    db = str(tmp_path / "j.db")

    # 세션 1: jobset 생성 후 "프로세스 kill" (close 없이 소멸)
    s1 = SqliteStore(db)
    s1.create_jobset(make_jobset("js1", n=2))
    s1.add_job(make_job("js1", 0, state=JobState.RUN, job_id=100))
    s1.add_job(make_job("js1", 1, state=JobState.PEND, job_id=101))
    s1.close()

    # 세션 2: orphan 감지 → recover
    s2 = SqliteStore(db)
    assert s2.persistent is True
    orphans = s2.list_orphan_jobsets()
    assert [o.jobset_id for o in orphans] == ["js1"]
    assert s2.list_jobsets() == []             # 현재 세션 범위엔 없음

    recovered = s2.recover_jobset("js1")
    assert recovered.session_id == s2.session_id
    assert [j.jobset_id for j in s2.list_jobsets()] == ["js1"]
    assert s2.list_orphan_jobsets() == []
    # job 데이터 보존 확인
    jobs = s2.get_jobs("js1")
    assert {j.job_id for j in jobs} == {100, 101}
    s2.close()


def test_closed_jobset_not_orphan(tmp_path):
    from dataclasses import replace
    db = str(tmp_path / "j.db")
    s1 = SqliteStore(db)
    js = s1.create_jobset(make_jobset("js1"))
    s1.update_jobset(replace(js, closed=True))
    s1.close()

    s2 = SqliteStore(db)
    assert s2.list_orphan_jobsets() == []
    s2.close()


# ----------------------------------------------------------------------
# 이력 / 통계 (FR-6.3)
# ----------------------------------------------------------------------
def test_history_records_transitions(tmp_path):
    s = SqliteStore(str(tmp_path / "j.db"))
    s.create_jobset(make_jobset("js1", n=1))
    s.add_job(make_job("js1", 0))
    s.transition("js1", "js1_0", JobState.SUBMITTING)
    s.transition("js1", "js1_0", JobState.PEND, job_id=1)
    s.transition("js1", "js1_0", JobState.RUN)
    s.transition("js1", "js1_0", JobState.DONE, exit_code=0)
    hist = s.get_history("js1")
    assert [(h["old_state"], h["new_state"]) for h in hist] == [
        ("CREATED", "SUBMITTING"), ("SUBMITTING", "PEND"),
        ("PEND", "RUN"), ("RUN", "DONE")]
    s.close()


def test_stats(tmp_path):
    s = SqliteStore(str(tmp_path / "j.db"))
    s.create_jobset(make_jobset("js1", n=3))
    for i in range(3):
        s.add_job(make_job("js1", i))
        s.transition("js1", f"js1_{i}", JobState.SUBMITTING)
    s.transition("js1", "js1_0", JobState.PEND, job_id=1)
    s.transition("js1", "js1_0", JobState.RUN)
    s.transition("js1", "js1_1", JobState.PEND, job_id=2)
    s.transition("js1", "js1_2", JobState.SUBMIT_FAILED)
    st = s.stats()
    assert st["submit_success"] == 2
    assert st["submit_failed"] == 1
    assert st["submit_success_rate"] == pytest.approx(2 / 3)
    assert st["pend_wait_count"] == 1
    s.close()


def test_search_all_sessions(tmp_path):
    db = str(tmp_path / "j.db")
    s1 = SqliteStore(db)
    s1.create_jobset(make_jobset("old", tags=["sweep"]))
    s1.close()
    s2 = SqliteStore(db)
    s2.create_jobset(make_jobset("new", tags=["sweep"]))
    assert {j.jobset_id for j in s2.search(tag="sweep")} == {"new"}
    assert {j.jobset_id
            for j in s2.search_all_sessions(tag="sweep")} == {"old", "new"}
    s2.close()


def test_archive_and_export(tmp_path):
    import json
    from dataclasses import replace
    from datetime import datetime, timedelta

    s = SqliteStore(str(tmp_path / "j.db"))
    old_date = datetime.now() - timedelta(days=60)
    js = s.create_jobset(make_jobset("old", created_at=old_date))
    s.update_jobset(replace(js, closed=True))
    s.create_jobset(make_jobset("keep"))

    out = str(tmp_path / "export.json")
    s.export_jobset("old", out)
    data = json.load(open(out))
    assert data["jobset"]["jobset_id"] == "old"

    assert s.archive(older_than_days=30) == 1
    assert {j.jobset_id for j in s.search_all_sessions()} == {"keep"}
    s.vacuum()
    s.close()
