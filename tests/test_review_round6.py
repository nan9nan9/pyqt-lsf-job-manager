"""전체 정독 리뷰 6차 — 아직 정독 안 한 영역(set_user_data / bkill 응답 파싱).

- R6-1: set_user_data가 **읽고-고쳐-쓰기**였다. 스냅샷을 update_job으로 통째
  되쓰기 때문에, 그 사이 submit worker가 기록한 job_id/PEND가 지워진다.
  LSF에서 도는 job이 추적 불가가 되고 레코드는 SUBMITTING에 고착돼 결국
  LOST로 확정된다. → store의 원자적 부분 갱신(transition(new_state=None))
  으로 user_data만 얹는다.

- R6-2: element 하나만 겨냥한 kill("1000[3]")의 응답이 bare 부모 "1000"까지
  '해소'로 만들었다. JobRecord.array_index는 늘 None(집계 레코드)이라 그
  레코드가 target "1000"에 매칭돼 job 전체가 EXIT로 찍힌다 — LSF에선 나머지
  element가 도는데 앱에는 죽은 것으로 보이고, terminal이라 폴링 대상에서도
  빠져 영영 안 고쳐진다. → 부모는 **실제로 요청했을 때만** 유도한다.
"""
from __future__ import annotations

from lsfmgr import JobState
from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# R6-1 — set_user_data는 동시 전이를 덮지 않는다
# ----------------------------------------------------------------------
def test_set_user_data_does_not_clobber_a_concurrent_submit(manager):
    """제출 성공(job_id 확보)과 겹치면 그 job_id가 지워졌다 — LSF에 살아있는
    job이 추적 불가가 되고 레코드는 SUBMITTING에 고착된다."""
    js = manager.create_jobset(["mytool a.sp"], job_keys=["a"])
    store = manager.store
    store.transition(js.id, "a", JobState.SUBMITTING)

    # set_user_data가 대상을 읽은 **직후** worker가 제출 성공을 기록하는 순간
    real_find = manager._find_job

    def find_then_race(jsid, ref):
        rec = real_find(jsid, ref)
        store.transition(jsid, "a", JobState.PEND, job_id=98765)
        return rec

    manager._find_job = find_then_race
    try:
        new = manager.set_user_data(js, "a", {"n": 1})
    finally:
        manager._find_job = real_find

    assert new.user_data == {"n": 1}
    assert new.job_id == 98765, "제출 성공의 job_id가 지워졌다(추적 불가)"
    assert new.state is JobState.PEND, f"상태가 되감겼다: {new.state.value}"


# ----------------------------------------------------------------------
# R6-2 — element kill이 job 레코드 전체를 죽이지 않는다
# ----------------------------------------------------------------------
def test_element_kill_does_not_mark_the_whole_job_exited(qtbot, manager):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND and rec.job_id is not None

    # element 3만 겨냥한 raw kill — 이 job에는 element가 없다
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill_jobs([f"{rec.job_id}[3]"], jobset_id=js.id)

    after = js.jobs()[0]
    assert after.state is JobState.PEND, (
        f"element만 겨냥했는데 job 레코드 전체가 {after.state.value}")
    assert not after.killed


def test_whole_array_kill_still_resolves_from_element_replies(qtbot, manager):
    """반대 방향 — bare 부모로 array를 kill하면 LSF는 element별로 확인 행을
    낸다. 그 응답으로 부모 target이 해소돼야 불필요한 재시도가 없다."""
    from lsfmgr.command import _parse_bkill_resolved
    text = ("Job <1000[0]> is being terminated\n"
            "Job <1000[1]> is being terminated\n")
    assert _parse_bkill_resolved(text, {"1000"})[0] == {
        "1000[0]", "1000[1]", "1000"}


# ----------------------------------------------------------------------
# R6-3 — handler tick이 main 스레드를 job 수만큼 잡지 않는다
# ----------------------------------------------------------------------
def test_handler_tick_does_not_block_the_main_thread(qtbot):
    """job 1건당 QRunnable 1개를 pool에 넣으면 tick이 main에서 pool.start
    비용(이 바인딩 기준 건당 ~180us, pool 크기와 무관하게 선형)을 job 수만큼
    문다 — RUN 5000건이면 **폴링 사이클마다** ~1.1초, 2만 건이 한꺼번에
    종료되면 4.4초 GUI 정지였다. 동시 실행은 어차피 상한이 있으니 큐잉만
    main에서 하고 꺼내 도는 일은 worker가 한다."""
    import time
    from datetime import datetime

    from lsfmgr import InMemoryStore
    from lsfmgr.handlers import JobSetHandlerService
    from lsfmgr.states import JobRecord, JobSetRecord

    n = 5000
    store = InMemoryStore()
    store.store_insert_jobset(JobSetRecord(jobset_id="js", intended_count=n,
                                           created_at=datetime.now()))
    store.store_add_jobs([
        JobRecord(job_id=1000 + i, array_index=None, jobset_id="js",
                  job_key=f"k{i}", state=JobState.RUN, command="x")
        for i in range(n)])
    svc = JobSetHandlerService(store)
    svc.add_handler("js", "h", lambda ctx: None)
    try:
        t0 = time.perf_counter()
        svc.tick("js")                       # 폴링 사이클 1회 = main 스레드
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
    finally:
        svc.shutdown()
    # 수정 전 ~1100ms / 수정 후 ~35ms. 느린 CI를 감안해도 300ms면 충분히 가른다.
    assert elapsed_ms < 300, f"tick이 main을 {elapsed_ms:.0f}ms 잡았다"


def test_handler_runs_exactly_once_per_job(qtbot):
    """큐 방식이 실행을 빠뜨리거나 중복하지 않는다 (실행 상한만 바뀐 것)."""
    import threading
    from datetime import datetime

    from lsfmgr import InMemoryStore
    from lsfmgr.handlers import JobSetHandlerService
    from lsfmgr.states import JobRecord, JobSetRecord

    n = 500
    store = InMemoryStore()
    store.store_insert_jobset(JobSetRecord(jobset_id="js", intended_count=n,
                                           created_at=datetime.now()))
    store.store_add_jobs([
        JobRecord(job_id=1000 + i, array_index=None, jobset_id="js",
                  job_key=f"k{i}", state=JobState.DONE, command="x")
        for i in range(n)])
    seen, lock = [], threading.Lock()

    def fn(ctx):
        with lock:
            seen.append((ctx.job_key, ctx.final))

    svc = JobSetHandlerService(store)
    svc.add_handler("js", "h", fn)
    svc.tick("js")
    svc.shutdown()                           # 큐 잔여분까지 drain 후 반환

    assert len(seen) == n, f"실행 {len(seen)}회 (기대 {n})"
    assert len({k for k, _ in seen}) == n, "같은 job이 두 번 돌았다"
    assert all(final for _, final in seen), "종료 상태인데 final=False"
