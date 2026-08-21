"""jobset을 지우면 그에 매달린 상태가 **어디에도** 남지 않는다.

jobset별 상태를 들고 있는 곳이 여덟 컴포넌트에 흩어져 있다(핸들·폴링 주기·
완료 무장·LOST 스트릭·조회 lock·handler 진행·제출 ctx·게이트 원장·kill slot·
표시 pacer·store·콜백 원장). 하나만 정리를 빠뜨려도 create/remove를 반복하는
장수 세션에서 조용히 쌓인다 — 실제로 _query_locks와 콜백 원장이 그랬다.

목록을 손으로 유지하면 새 컴포넌트를 놓치므로, **지운 jobset_id가 살아 있는
객체 어딘가에 남아 있는지**를 직접 훑는다.
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager


def _find_traces(obj, needles, path="mgr", depth=0, seen=None):
    """살아 있는 자료구조에서 지워진 jobset_id의 흔적을 찾는다."""
    seen = seen if seen is not None else set()
    if depth > 3 or id(obj) in seen:
        return []
    seen.add(id(obj))
    hits = []

    def mentions(x):
        if isinstance(x, str):
            return x in needles
        if isinstance(x, tuple):
            return any(isinstance(i, str) and i in needles for i in x)
        return False

    if isinstance(obj, dict):
        for k in list(obj):
            if mentions(k):
                hits.append(f"{path}[{k!r}]")
        for k, v in list(obj.items())[:200]:
            hits += _find_traces(v, needles, f"{path}[{k!r}]", depth + 1, seen)
    elif isinstance(obj, (set, frozenset)):
        hits += [f"{path}<{x!r}>" for x in obj if mentions(x)]
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj[:200]):
            hits += _find_traces(v, needles, f"{path}[{i}]", depth + 1, seen)
    elif hasattr(obj, "__dict__"):
        for name, v in list(vars(obj).items()):
            if name.startswith("__"):
                continue
            hits += _find_traces(v, needles, f"{path}.{name}", depth + 1, seen)
    return hits


def test_removed_jobsets_leave_no_trace(qtbot, fake_lsf):
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(min_state_dwell_s=0.2, poll_interval_s=5.0,
                         job_status_fetcher=lambda: {"jobs": []},
                         internal_refresh_min_s=0.0),
        runner=fake_lsf)
    removed = set()
    try:
        for i in range(20):
            js = mgr.create_jobset([f"mytool {i}_{j}.sp" for j in range(3)],
                                   job_keys=[f"k{j}" for j in range(3)])
            mgr.add_handler(js, "h", lambda c: None)
            with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
                mgr.submit(js, auto_poll=False, post_process=lambda r: None)
            mgr.start_polling(js, 5.0)
            with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
                mgr.kill(js)
            mgr.query_once(js)
            qtbot.wait(40)
            removed.add(js.id)
            mgr.remove_jobset(js)
        qtbot.wait(1200)
        traces = _find_traces(mgr, removed)
        assert not traces, ("지운 jobset의 흔적이 남았다:\n  "
                            + "\n  ".join(sorted(traces)[:15]))
    finally:
        mgr.shutdown()


def test_ledger_forgets_removed_jobs(qtbot, fake_lsf):
    """콜백 원장은 job_id로 키를 잡아 jobset_id 훑기에 안 걸린다 — 따로 본다.
    (만료는 종료 job만 보므로 삭제 시 명시적으로 버려야 한다)"""
    ids = []
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(job_status_fetcher=lambda: {
            "jobs": [{"dataId": f"{i}.c1", "stat": "RUN"} for i in ids]},
            internal_refresh_min_s=0.0),
        runner=fake_lsf)
    src = mgr.command.internal_status
    try:
        for i in range(15):
            js = mgr.create_jobset([f"mytool {i}.sp"], job_keys=["k"])
            with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
                mgr.submit(js, auto_poll=False)
            ids.extend(r.job_id for r in js.jobs())
            mgr.query_once(js)
            qtbot.wait(40)
            with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
                mgr.kill(js)
            mgr.remove_jobset(js)
        qtbot.wait(600)
        st = src.stats()
        assert (st["entries"], st["tracked_ids"]) == (0, 0), st
    finally:
        mgr.shutdown()
