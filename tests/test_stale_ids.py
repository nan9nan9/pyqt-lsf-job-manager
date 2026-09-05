"""job_id를 레코드에서 떨어뜨리는 모든 경로가 원장에서도 버린다.

콜백 조회원(job_status_fetcher)의 관심 집합·원장은 "조회할 id"의 목록이다.
레코드에서 id가 사라졌는데 여기 남으면 아무도 조회하지 않는 id가 영영
남는다 — 만료는 **종료(DONE/EXIT)** 항목만 걷어내므로, 마지막으로 진행
중으로 보였던 id는 만료 대상조차 아니다. 게다가 관심에 있는 동안 매 폴링의
병합·경과시간 갱신이 계속 훑는다.

규칙: **id를 지우는 쪽이 버리는 것도 책임진다.**
경로마다 주인이 다르므로(삭제=remove_*, 재제출=submitter, 교체=
_rearm_tracking) 새 경로가 생기면 여기 한 줄을 추가하도록 강제한다.
삭제 뒤 늦게 등록되거나 미등록 ID를 직접 조회하는 경우는 query_ids가 정리한다.
"""
from __future__ import annotations

import subprocess
import threading

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.command import CommandResult
from tests.fake_lsf import FakeLsf


@pytest.fixture
def mgr(qtbot, fake_lsf):
    live: dict = {}
    m = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(job_status_fetcher=lambda: {"jobs": [
            {"dataId": f"{i}.c1", "stat": "RUN"} for i in live]},
            internal_refresh_min_s=0.0),
        runner=fake_lsf)
    m._probe_live = live                      # fetcher가 돌려줄 id 집합
    try:
        yield m
    finally:
        m.shutdown()


def _submitted(qtbot, mgr, keys):
    js = mgr.create_jobset([f"mytool {k}.sp" for k in keys], job_keys=list(keys))
    with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
        mgr.submit(js, auto_poll=False)
    ids = [r.job_id for r in js.jobs()]
    mgr._probe_live.update({i: 1 for i in ids})
    mgr.query_once(js)                        # 관심 등록
    qtbot.wait(200)
    src = mgr.command.internal_status
    assert set(ids) <= set(src._interest), "관심 등록이 안 됐다 — 전제가 깨졌다"
    return js, ids


def _quiesce(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
        mgr.kill(js)


def _resubmit(m, js, qtbot):
    with qtbot.waitSignal(m.submit_finished, timeout=20000):
        m.submit(js, auto_poll=False)


DROPPERS = {
    "resubmit":      lambda m, js, qtbot: _resubmit(m, js, qtbot),
    "replace_jobs":  lambda m, js, qtbot: m.replace_jobs(
        js, ["mytool new.sp"], job_keys=["k0"]),
    "upsert_jobs":   lambda m, js, qtbot: m.upsert_jobs(
        js, ["mytool new.sp"], job_keys=["k0"]),
    "remove_jobs":   lambda m, js, qtbot: m.remove_jobs(js, ["k0"], force=True),
    "clear_jobs":    lambda m, js, qtbot: m.clear_jobs(js, force=True),
    "remove_jobset": lambda m, js, qtbot: m.remove_jobset(js, force=True),
}


@pytest.mark.parametrize("name", sorted(DROPPERS))
def test_dropping_a_job_id_also_forgets_it(name, mgr, qtbot):
    js, ids = _submitted(qtbot, mgr, ["k0", "k1"])
    _quiesce(qtbot, mgr, js)                  # 편집 가드 통과용
    src = mgr.command.internal_status

    DROPPERS[name](mgr, js, qtbot)
    qtbot.wait(200)

    # 레코드에 남아 있는 id만 관심에 있어야 한다
    alive = set()
    for jsr in mgr.store.list_jobsets():
        alive |= {r.job_id for r in mgr.store.get_jobs(jsr.jobset_id) if r.job_id}
    ghosts = set(src._interest) - alive
    assert not ghosts, (
        f"{name}: 레코드에서 사라진 job_id가 원장에 남았다 {sorted(ghosts)}")
    assert not (set(src._ledger) - alive)


def test_partial_resubmit_keeps_untargeted_ids_in_the_ledger(mgr, qtbot):
    """submit(only=)은 **대상 아닌** job의 원장 항목을 버리면 안 된다 —
    증분 payload는 안 바뀐 job을 다시 보내지 않으므로, 버리는 순간 그
    RUN job이 매 폴링 미발견으로 몰려 LOST(되돌릴 수 없음)로 확정될 수
    있다. forget은 이 사이클이 리셋하는 key의 id에만 걸려야 한다."""
    js, _ids = _submitted(qtbot, mgr, ["k0", "k1"])
    src = mgr.command.internal_status
    k1_id = next(r.job_id for r in js.jobs() if r.job_key == "k1")
    assert k1_id in src._ledger                  # 전제 — 원장에 있다
    with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
        mgr.kill_jobs(js, ["k0"])                # k0만 비활성으로
    with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
        mgr.submit(js, only=["k0"], auto_poll=False)   # k1은 RUN인 채
    assert k1_id in src._interest and k1_id in src._ledger, \
        "only= 재제출이 대상 아닌 RUN job의 원장 항목을 버렸다"


def test_the_matrix_covers_every_dropping_api():
    """job_id를 떨어뜨릴 수 있는 공개 API가 위 표에 다 있는지.

    레코드를 새로 쓰거나 지우는 명령은 전부 후보다 — 새 명령이 생기면
    표에 넣고 규칙을 지키는지 확인하라는 뜻이다."""
    CANDIDATES = {"submit", "replace_jobs", "upsert_jobs", "remove_jobs",
                  "clear_jobs", "remove_jobset"}
    covered = set(DROPPERS) | {"submit"}      # resubmit == submit 재호출
    assert CANDIDATES <= covered, f"표에 없는 경로: {sorted(CANDIDATES - covered)}"
    for name in CANDIDATES:
        assert hasattr(LsfJobManager, name), f"없는 API가 표에: {name}"


def test_query_snapshot_older_than_removal_does_not_revive_the_id(mgr, qtbot):
    """조회는 대상 스냅샷(store) → 관심 등록(bjobs_by_ids) 순서다. 그 사이에
    삭제 API가 id를 버리면(forget) 늦은 등록이 그것을 되돌려 유령 id가 남는다
    — 조회가 등록 뒤 스냅샷과 현재 레코드를 대조해 되돌린 id를 다시 버린다."""
    js, ids = _submitted(qtbot, mgr, ["a", "b"])
    real = mgr.querier.store
    fired = []

    class SnapshotThenRemove:
        def __getattr__(self, name):
            return getattr(real, name)

        def get_jobs(self, *args, **kwargs):
            recs = real.get_jobs(*args, **kwargs)
            if not fired:                        # 스냅샷 직후, 등록 전에 삭제
                fired.append(1)
                mgr.remove_jobs(js, ["a"], force=True)
            return recs

    mgr.querier.store = SnapshotThenRemove()
    try:
        mgr.querier.query(js.id)                 # main 스레드에서 직접 1회
    finally:
        mgr.querier.store = real
    src = mgr.command.internal_status
    assert fired and ids[0] not in src._interest, sorted(src._interest)
    assert ids[1] in src._interest               # 남은 job은 그대로 추적


def test_poll_cleanup_reads_only_queried_keys(mgr, qtbot, monkeypatch):
    """정리 비용은 전체 Store나 완료 작업 수가 아닌 조회한 key 수에 비례한다."""
    js, ids = _submitted(qtbot, mgr, ["a", "b", "done"])
    mgr.store.transition(js.id, "done", JobState.DONE)
    mgr.create_jobset(["unrelated task"], job_keys=["other"])
    real = mgr.store
    reads = []

    class BoundedReads:
        def __getattr__(self, name):
            return getattr(real, name)

        def find_jobs(self, job_ids):
            pytest.fail("폴링의 관심 ID 정리가 Store 전역을 검색했다")

        def get_jobs(self, jobset_id, states=None):
            assert jobset_id == js.id and states is not None
            return real.get_jobs(jobset_id, states=states)

        def get_jobs_by_keys(self, jobset_id, job_keys):
            reads.append((jobset_id, set(job_keys)))
            return real.get_jobs_by_keys(jobset_id, job_keys)

    monkeypatch.setattr(mgr.querier, "store", BoundedReads())
    mgr._probe_live.clear()                    # 증분 응답이 비어도 원장은 보존
    mgr.querier.query(js.id, fresh=True)
    assert reads == [(js.id, {"a", "b"})]
    assert mgr.command.internal_status._interest == set(ids)
    assert js.jobs()[0].state is JobState.RUN


@pytest.mark.parametrize("job_keys", [None, ["a"]], ids=["jobset", "keys"])
@pytest.mark.parametrize("query_fails", [False, True])
def test_scoped_query_cleans_ids_after_jobset_removal(
        mgr, qtbot, monkeypatch, job_keys, query_fails):
    """소속을 아는 조회도 jobset 삭제·조회 실패 뒤 관심을 되살리지 않는다."""
    js, ids = _submitted(qtbot, mgr, ["a"])
    original = mgr.command.bjobs_by_ids

    def query_after_removal(job_ids, *, fresh=False):
        mgr.remove_jobset(js, force=True)
        result = original(job_ids, fresh=fresh)  # 삭제의 forget보다 늦은 등록
        if query_fails:
            raise RuntimeError("query interrupted")
        return result

    monkeypatch.setattr(mgr.command, "bjobs_by_ids", query_after_removal)
    if query_fails:
        with pytest.raises(RuntimeError, match="query interrupted"):
            mgr.querier.query_ids(ids, jobset_id=js.id, job_keys=job_keys)
    else:
        statuses, failed = mgr.querier.query_ids(
            ids, jobset_id=js.id, job_keys=job_keys)
        assert not failed and [st.job_id for st in statuses] == ids
    src = mgr.command.internal_status
    assert not src._interest and not src._ledger
    assert not mgr.querier._id_queries


@pytest.mark.parametrize("route", ["already_finished", "verify", "timeout"])
@pytest.mark.parametrize("remove_during_kill", [False, True])
@pytest.mark.parametrize("query_fails", [False, True])
def test_direct_kill_query_cleans_untracked_ids(
        qtbot, route, remove_during_kill, query_fails):
    """kill의 직접 조회도 삭제 뒤 늦게 등록한 ID와 미등록 ID를 정리한다.
    조회 장애로 상태를 확인하지 못했어도 추적할 레코드가 없다는 사실은 같다."""
    fake = FakeLsf()
    entered, release = threading.Event(), threading.Event()

    def runner(argv, timeout, cwd=None):
        if argv[0] != "bkill":
            return fake(argv, timeout, cwd)
        entered.set()
        assert release.wait(3)
        if route == "timeout":
            raise subprocess.TimeoutExpired(argv, timeout)
        message = ("is being terminated" if route == "verify"
                   else "Job has already finished")
        return CommandResult(0, f"Job <{argv[1]}>: {message}\n", "")

    def fetcher():
        if query_fails:
            raise RuntimeError("status source unavailable")
        return {"jobs": []}

    cfg = LsfConfig(job_status_fetcher=fetcher, internal_refresh_min_s=0,
                    kill_max_retry=0)
    mgr = LsfJobManager(runner=runner, config=cfg)
    try:
        job_id = 987654
        if remove_during_kill:
            js = mgr.create_jobset(["wrapper task"], job_keys=["a"])
            with qtbot.waitSignal(mgr.submit_finished, timeout=3000):
                mgr.submit(js, auto_poll=False)
            job_id = js.jobs()[0].job_id
        with qtbot.waitSignal(mgr.kill_finished, timeout=3000) as result:
            mgr.kill_jobs([job_id], verify=(route == "verify"))
            assert entered.wait(3)
            if remove_during_kill:
                mgr.remove_jobs(js, ["a"], force=True)
            release.set()
        report = result.args[1]
        assert not any("internal:" in error for error in report.errors), report
        assert report.unconfirmed == int(route == "timeout" and query_fails)
        assert mgr.store.find_jobs({job_id}) == []
        src = mgr.command.internal_status
        assert job_id not in src._interest
        assert job_id not in src._ledger
        assert not mgr.querier._id_queries
    finally:
        release.set()
        mgr.shutdown()


def test_direct_query_preserves_tracked_incremental_status(mgr, qtbot):
    """조회 대상인 RUN과 대상 밖 RUN의 원장은 모두 보존한다. 콜백은 증분이라
    다음 응답에서 빠진다고 지우면 그 작업은 LOST로 잘못 판정될 수 있다."""
    js, ids = _submitted(qtbot, mgr, ["a", "b"])
    with qtbot.waitSignal(mgr.kill_finished, timeout=3000):
        mgr.kill_jobs([ids[0], 987654], verify=True)
    src = mgr.command.internal_status
    assert src._interest == set(ids)
    assert set(src._ledger) == set(ids)
    mgr._probe_live.clear()                    # 이후 payload는 변경분 없음
    mgr.querier.query(js.id, fresh=True)
    assert all(r.state is JobState.RUN for r in js.jobs())


def test_overlapping_queries_keep_results_until_both_read(qtbot, monkeypatch):
    """같은 미등록 ID를 조회 중인 다른 호출이 결과를 읽기 전에 지우지 않는다."""
    payloads = iter([{"jobs": [{"dataId": "987654.c1", "stat": "RUN"}]},
                     {"jobs": []}])          # 두 번째 응답은 증분
    mgr = LsfJobManager(runner=FakeLsf(), config=LsfConfig(
        job_status_fetcher=lambda: next(payloads), internal_refresh_min_s=0))
    ready, release = threading.Event(), threading.Event()
    results, errors = {}, []
    src = mgr.command.internal_status
    ensure_fetched = src._ensure_fetched

    def pause_before_read(*, fresh):
        ok = ensure_fetched(fresh=fresh)
        if threading.current_thread().name == "held-reader":
            ready.set()
            assert release.wait(3)
        return ok

    monkeypatch.setattr(src, "_ensure_fetched", pause_before_read)

    def held_query():
        try:
            results["held"] = mgr.querier.query_ids([987654], fresh=True)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=held_query, name="held-reader")
    try:
        worker.start()
        assert ready.wait(3)
        results["other"] = mgr.querier.query_ids([987654], fresh=True)
        release.set()
        worker.join(3)
        assert not worker.is_alive() and not errors
        for statuses, failed in results.values():
            assert not failed
            assert [(st.job_id, st.state) for st in statuses] == [
                (987654, JobState.RUN)]
        assert not src._interest and not src._ledger
        assert not mgr.querier._id_queries
    finally:
        release.set()
        worker.join(3)
        mgr.shutdown()
