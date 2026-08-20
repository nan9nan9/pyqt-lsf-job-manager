"""kill_workers — 한 kill 안에서 bkill chunk를 동시에 몇 개 돌릴지.

kill_chunk_size가 "한 호출에 몇 건"이라면 이건 "그 호출을 몇 개 동시에".
MC 사이트의 bkill은 원격 왕복을 기다리는 **지연 지배적** 작업이라 직렬이면
ceil(N/chunk)회를 한 줄로 세워 기다린다.
"""
from __future__ import annotations

import threading
import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds


class _WatchBkill:
    """bkill 동시 실행 수를 관측하는 runner 래퍼 (호출당 지연 주입)."""

    def __init__(self, inner, delay=0.05):
        self.inner = inner
        self.delay = delay
        self.lock = threading.Lock()
        self.now = 0
        self.peak = 0
        self.calls = 0
        self.sizes = []

    def __call__(self, argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] != "bkill":
            return self.inner(argv, timeout, cwd)
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
            self.calls += 1
            self.sizes.append(len(argv) - 1)
        try:
            time.sleep(self.delay)
            return self.inner(argv, timeout, cwd)
        finally:
            with self.lock:
                self.now -= 1


def _kill_all(qtbot, runner, n, **cfg):
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(rate_limit_per_s=None, **cfg),
        runner=runner)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(n)],
                             auto_poll=False, workers=8)
        t0 = time.perf_counter()
        with qtbot.waitSignal(mgr.kill_finished, timeout=120000) as blk:
            mgr.kill(js)
        return time.perf_counter() - t0, blk.args[1], js.jobs()
    finally:
        mgr.shutdown()


def test_default_is_serial(qtbot, fake_lsf):
    """기본값 1 — 지금까지의 동작 그대로여야 한다(부하 특성 변경 금지)."""
    assert LsfConfig().kill_workers == 1
    w = _WatchBkill(fake_lsf)
    _dt, rpt, recs = _kill_all(qtbot, w, 100, kill_chunk_size=16)
    assert w.calls == 7 and set(w.sizes) == {16, 4}
    assert w.peak == 1, f"기본값인데 동시 실행 {w.peak}"
    assert rpt.unconfirmed == 0
    assert all(r.state is JobState.EXIT for r in recs)


@pytest.mark.parametrize("workers", [2, 4])
def test_chunks_run_in_parallel(qtbot, fake_lsf, workers):
    w = _WatchBkill(fake_lsf)
    _dt, rpt, recs = _kill_all(qtbot, w, 100,
                               kill_chunk_size=16, kill_workers=workers)
    assert w.calls == 7
    assert w.peak > 1, "병렬로 안 돌았다"
    assert w.peak <= workers, f"상한 {workers}을 넘겨 {w.peak}개 동시 실행"
    # 정확성은 직렬과 동일해야 한다
    assert rpt.unconfirmed == 0 and not rpt.errors
    assert all(r.state is JobState.EXIT and r.killed for r in recs)


def test_parallel_is_faster_when_bkill_is_latency_bound(qtbot, fake_lsf):
    """MC처럼 bkill 1회가 오래 걸리는 환경 — 병렬이 실제로 줄여 주나."""
    serial, _r1, _j1 = _kill_all(qtbot, _WatchBkill(fake_lsf, delay=0.08), 96,
                                 kill_chunk_size=16, kill_workers=1)
    par, _r2, _j2 = _kill_all(qtbot, _WatchBkill(fake_lsf, delay=0.08), 96,
                              kill_chunk_size=16, kill_workers=6)
    print(f"\n직렬 {serial:.2f}s → 병렬(6) {par:.2f}s")
    assert par < serial * 0.6, f"직렬 {serial:.2f}s / 병렬 {par:.2f}s"


def test_progress_never_goes_backwards(qtbot, fake_lsf):
    """병렬 chunk가 섞여도 진행 누적치가 되감기면 안 된다."""
    seen = []
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(rate_limit_per_s=None, kill_chunk_size=4,
                         kill_workers=4, progress_min_interval_s=0.0,
                         progress_min_step_ratio=0.0),
        runner=_WatchBkill(fake_lsf, delay=0.01))
    mgr.kill_progress.connect(lambda j, d, t: seen.append(d))
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(40)],
                             auto_poll=False, workers=8)
        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill(js)
        qtbot.wait(200)
    finally:
        mgr.shutdown()
    assert seen == sorted(seen), f"진행이 되감겼다: {seen}"


def test_workers_is_clamped():
    assert LsfConfig(kill_workers=0).kill_workers == 1
    assert LsfConfig(kill_workers=999).kill_workers == 32
    with pytest.raises(ValueError):
        LsfJobManager(kill_workers=0)          # 옵션 계층은 1~32 강제
