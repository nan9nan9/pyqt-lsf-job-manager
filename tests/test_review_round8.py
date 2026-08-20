"""전체 정독 리뷰 8차 — 요약(summary) 전수 스캔 제거.

- R8-1: InMemoryStore.summary()가 매번 전 레코드를 훑었다. 대량 제출 중
  main 스레드가 jobs_changed 배치마다 이 값을 물으므로(_relay_jobs_changed →
  _emit_summary), 2만 건 기준 호출당 15ms × 104회 = 1.6초를 main이 썼고
  그동안 store lock을 쥐고 있어 worker 8개의 전이까지 함께 밀렸다
  (제출 전체 22.9s). → 상태별 개수를 증분으로 유지한다
  (summary 1585ms → 2ms, 제출 전체 22.9s → 12.9s).

  denormalize한 값은 쓰기 경로를 하나라도 빠뜨리면 **조용히** 어긋나므로,
  쓰기를 _put_rec/_drop_rec 두 곳으로 모으고 교차 검증(_debug_counts_ok)을
  코드 옆에 뒀다. 아래 테스트가 모든 쓰기 경로를 그 검증으로 훑는다.
"""
from __future__ import annotations

import random
import threading
from datetime import datetime

import pytest

from lsfmgr import InMemoryStore, JobState
from lsfmgr.states import JobRecord, JobSetRecord


def _store(n=0, jsid="js", intended=None):
    st = InMemoryStore()
    st.store_insert_jobset(JobSetRecord(
        jobset_id=jsid, intended_count=n if intended is None else intended,
        created_at=datetime.now()))
    if n:
        st.store_add_jobs([
            JobRecord(job_id=None, array_index=None, jobset_id=jsid,
                      job_key=f"k{i}", state=JobState.CREATED, command="x")
            for i in range(n)])
    return st


def _invariant(st, jsid="js"):
    """증분 카운트가 (a) 실제 레코드와 (b) 전수 스캔판 요약과 일치하는가.

    intended_count와의 합계 불변식은 **도메인 계층**(JobSetManager)이
    유지하는 계약이라 여기서 보지 않는다 — 이 테스트는 store 안에서
    카운트를 denormalize한 것이 스캔판과 같은 답을 내는지만 본다."""
    from lsfmgr.store.base import make_summary

    assert st._debug_counts_ok(jsid), (
        f"증분 카운트 불일치: {st._counts.get(jsid)}")
    got = st.summary(jsid)
    want = make_summary(st.get_jobset(jsid), st.get_jobs(jsid))
    assert got == want, f"증분 {got} != 스캔 {want}"
    assert all(v > 0 for k, v in got.items() if k != "total"), (
        f"0인 상태가 키로 남았다: {got}")
    return got


def test_counts_track_every_write_path():
    """쓰기 경로를 하나라도 funnel 밖에 두면 여기서 걸린다."""
    st = _store(5)
    _invariant(st)

    st.store_add_job(JobRecord(job_id=None, array_index=None, jobset_id="js",
                               job_key="extra", state=JobState.CREATED,
                               command="x"))
    _invariant(st)

    st.transition("js", "k0", JobState.SUBMITTING)
    st.transition("js", "k1", JobState.PEND, job_id=1)
    _invariant(st)

    st.transition("js", "k1", None, user_data={"a": 1})   # 부분 갱신(상태 유지)
    assert _invariant(st).get("PEND") == 1

    st.transition_many("js", [("k2", JobState.RUN, None, {}),
                              ("k3", JobState.DONE, None, {}),
                              ("없는키", JobState.RUN, None, {})])
    _invariant(st)

    rec = st.get_job("js", "k4")
    st.update_job(rec.__class__(**{**rec.__dict__, "state": JobState.EXIT}))
    _invariant(st)

    st.store_delete_job("js", "k0")
    _invariant(st)

    with pytest.raises(Exception):
        st.store_delete_job("js", "k0")                   # 이미 없음
    _invariant(st)


def test_counts_survive_guard_rejections():
    """guard가 거부한 전이는 카운트를 건드리면 안 된다."""
    st = _store(3)
    _invariant(st)
    st.transition("js", "k0", JobState.RUN,
                  guard=lambda cur: cur.state is JobState.PEND)   # 거부됨
    assert _invariant(st).get("CREATED") == 3
    st.transition_many("js", [("k1", JobState.RUN,
                               lambda cur: False, {})])
    assert _invariant(st).get("CREATED") == 3


def test_summary_matches_a_full_scan_after_random_churn():
    """무작위 혼합 연산 뒤에도 전수 스캔과 같은 답이어야 한다."""
    from lsfmgr.store.base import make_summary

    rnd = random.Random(20260820)
    st = _store(60)
    states = list(JobState)
    for _ in range(600):
        op = rnd.random()
        key = f"k{rnd.randrange(60)}"
        if op < 0.6:
            try:
                st.transition("js", key, rnd.choice(states))
            except Exception:
                pass                          # 앞서 지워진 키
        elif op < 0.8:
            st.transition_many("js", [(f"k{rnd.randrange(60)}",
                                       rnd.choice(states), None, {})
                                      for _ in range(5)])
        elif op < 0.9:
            try:
                st.store_delete_job("js", key)
            except Exception:
                pass
        else:
            try:
                st.store_add_job(JobRecord(
                    job_id=None, array_index=None, jobset_id="js",
                    job_key=key, state=rnd.choice(states), command="x"))
            except ValueError:
                pass
    _invariant(st)
    js = st.get_jobset("js")
    assert st.summary("js") == make_summary(js, st.get_jobs("js"))
    assert len(st.get_jobs("js")) < 60, "삭제 경로가 한 번도 안 탔다"


def test_counts_hold_under_concurrent_writers():
    """8 스레드가 동시에 전이해도 카운트가 새지 않는다 (store lock 아래)."""
    n = 200
    st = _store(n)
    states = [JobState.PEND, JobState.RUN, JobState.DONE, JobState.EXIT]

    def worker(seed):
        rnd = random.Random(seed)
        for _ in range(500):
            st.transition("js", f"k{rnd.randrange(n)}", rnd.choice(states))

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    _invariant(st)


def test_removing_the_jobset_drops_its_counts():
    st = _store(4)
    st.transition("js", "k0", JobState.RUN)
    st.store_delete_jobset("js")
    assert "js" not in st._counts, "삭제된 jobset의 카운트가 남았다"
