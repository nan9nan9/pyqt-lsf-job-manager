"""문서에 적힌 노브가 실제로 동작을 바꾼다.

verify_kill이 그렇지 않았다 — 받아들여지고 검증까지 되지만 아무것도 바꾸지
않았다. "설정했는데 안 먹는" 건 앱 입장에서 가장 찾기 어려운 부류의 결함이라,
값을 넣는 것만이 아니라 **관측 가능한 결과가 달라지는지**까지 본다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager


@pytest.fixture
def factory(qtbot):
    made = []

    def make(**cfg):
        from tests.fake_lsf import FakeLsf
        runner = FakeLsf()
        mgr = LsfJobManager(store=InMemoryStore(),
                            config=LsfConfig(poll_interval_s=5.0, **cfg),
                            runner=runner)
        made.append(mgr)
        return mgr, runner
    yield make
    for m in made:
        m.shutdown()


def _submit(qtbot, mgr, n):
    js = mgr.create_jobset([f"mytool {i}.sp" for i in range(n)],
                           job_keys=[f"k{i}" for i in range(n)])
    with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
        mgr.submit(js, auto_poll=False)
    return js


def test_chunk_size_changes_bjobs_call_count(qtbot, factory):
    calls = []
    for size in (40, 4):
        mgr, runner = factory(chunk_size=size)
        js = _submit(qtbot, mgr, 40)
        mgr.query_once(js)
        qtbot.wait(300)
        calls.append(len(runner.calls_of("bjobs")))
    assert calls[1] > calls[0], f"chunk_size가 bjobs 호출 수를 안 바꾼다: {calls}"


def test_kill_chunk_size_changes_bkill_call_count(qtbot, factory):
    calls = []
    for size in (40, 2):
        mgr, runner = factory(kill_chunk_size=size)
        js = _submit(qtbot, mgr, 20)
        with qtbot.waitSignal(mgr.kill_finished, timeout=30000):
            mgr.kill(js)
        calls.append(len(runner.calls_of("bkill")))
    assert calls[1] > calls[0], f"kill_chunk_size가 안 먹는다: {calls}"


def test_collect_clusters_changes_queried_fields(qtbot, factory):
    argv = []
    for flag in (False, True):
        mgr, runner = factory(collect_clusters=flag)
        js = _submit(qtbot, mgr, 3)
        mgr.query_once(js)
        qtbot.wait(300)
        argv.append(" ".join(runner.calls_of("bjobs")[0]))
    assert "source_cluster" not in argv[0]
    assert "source_cluster" in argv[1] and "forward_cluster" in argv[1]


def test_poll_runtime_updates_controls_runtime_only_transitions(qtbot, factory):
    """RUN 중 경과시간만 변했을 때 재발행할지 — 끄면 0건이어야 한다."""
    rows = []
    for flag in (False, True):
        mgr, runner = factory(poll_runtime_updates=flag)
        js = _submit(qtbot, mgr, 10)
        with runner.lock:
            for j in runner.jobs.values():
                j.stat, j.run_time_s = "RUN", 10
        with qtbot.waitSignal(mgr.jobs_updated, timeout=10000):
            mgr.query_once(js)                       # PEND→RUN 반영
        seen = []
        mgr.jobs_updated.connect(lambda _j, ch: seen.extend(ch))
        for r in range(3):                           # 이제 run_time만 변한다
            with runner.lock:
                for j in runner.jobs.values():
                    j.run_time_s = 20 + 10 * r
            mgr.query_once(js)
            qtbot.wait(250)
        rows.append(len(seen))
    assert rows[0] == 0, f"꺼도 경과시간 변화로 재전이한다: {rows[0]}건"
    assert rows[1] > 0, "켜도 경과시간 변화가 발행되지 않는다"


def test_progress_throttle_interval_reduces_emissions(qtbot, factory):
    """간격 조건만 남기고(step 조건 차단) 비교 — 둘은 OR라서 같이 봐야 한다."""
    counts = []
    for interval in (0.0, 5.0):
        mgr, _ = factory(progress_min_interval_s=interval,
                         progress_min_step_ratio=1.0)
        n = []
        mgr.submit_progress.connect(lambda *a: n.append(1))
        _submit(qtbot, mgr, 60)
        counts.append(len(n))
    assert counts[0] > counts[1] * 3, f"간격 노브가 발화를 안 줄인다: {counts}"
