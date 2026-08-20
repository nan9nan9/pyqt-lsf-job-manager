"""workers는 **전역** 상한 — 제출 사이클이 몇 개든 동시 wrapper 총수를 넘지 않는다.

사이클(=진행 중인 jobset)마다 QThreadPool을 새로 만들기 때문에, 예전에는
jobset 3개를 동시에 제출하면 wrapper가 8이 아니라 24개 떴다. workers를 낮게
잡아 eauth/mbatchd 과부하를 막으려는 사이트에서 그 보호가 동시 제출 수만큼
무력화된다(8코어에서 64개가 돌면 GUI main 최대 지연 193ms 실측).
"""
from __future__ import annotations

import threading
import time

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager


class _Peak:
    """동시에 도는 wrapper 수를 관측."""

    def __init__(self, inner, cost=0.02):
        self.inner = inner
        self.cost = cost
        self.lock = threading.Lock()
        self.now = 0
        self.peak = 0

    def __call__(self, argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] in ("bjobs", "bkill"):
            return self.inner(argv, timeout, cwd)
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
        try:
            time.sleep(self.cost)
            return self.inner(argv, timeout, cwd)
        finally:
            with self.lock:
                self.now -= 1


def _submit_many(qtbot, fake_lsf, n_sets, per_set=40, **kw):
    w = _Peak(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None),
                        runner=w, **kw)
    try:
        sets = [mgr.create_jobset(
            [f"mytool {k}_{i}.sp" for i in range(per_set)],
            job_keys=[f"k{k}_{i}" for i in range(per_set)])
            for k in range(n_sets)]
        done = [0]
        mgr.submit_finished.connect(
            lambda j, r: done.__setitem__(0, done[0] + 1))
        for js in sets:
            mgr.submit(js, auto_poll=False)
        qtbot.waitUntil(lambda: done[0] >= n_sets, timeout=120000)
        return w.peak
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("n_sets", [1, 3, 8])
def test_concurrent_wrappers_never_exceed_workers(qtbot, fake_lsf, n_sets):
    peak = _submit_many(qtbot, fake_lsf, n_sets, workers=4)
    assert peak <= 4, (
        f"jobset {n_sets}개 동시 제출에 wrapper {peak}개 — 전역 상한 4를 넘었다")


def test_the_limit_is_actually_used(qtbot, fake_lsf):
    """상한까지는 실제로 쓴다 — 과보호로 직렬화되면 안 된다."""
    assert _submit_many(qtbot, fake_lsf, 3, workers=6) >= 5


def test_call_level_workers_can_only_lower(qtbot, fake_lsf):
    """호출별 workers는 전역 상한 **아래로 낮추는** 용도다."""
    w = _Peak(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None),
                        runner=w, workers=4)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(40)],
                               job_keys=[f"k{i}" for i in range(40)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(js, auto_poll=False, workers=32)   # 올려도
        assert w.peak <= 4, f"호출별 workers=32가 전역 상한을 뚫었다({w.peak})"

        w.peak = 0
        js2 = mgr.create_jobset([f"mytool b{i}.sp" for i in range(40)],
                                job_keys=[f"b{i}" for i in range(40)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(js2, auto_poll=False, workers=2)   # 내리는 건 된다
        assert w.peak <= 2, w.peak
    finally:
        mgr.shutdown()


def test_kill_quiesce_still_scoped_to_its_own_cycle(qtbot, fake_lsf):
    """전역 상한을 세마포어로 얹은 이유 — 풀을 합치면 A의 kill이 B의 제출을
    기다리게 된다. 사이클별 waitForDone 의미가 유지돼야 한다."""
    w = _Peak(fake_lsf, cost=0.05)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None),
                        runner=w, workers=4)
    try:
        a = mgr.create_jobset([f"mytool a{i}.sp" for i in range(200)],
                              job_keys=[f"a{i}" for i in range(200)])
        b = mgr.create_jobset([f"mytool b{i}.sp" for i in range(4)],
                              job_keys=[f"b{i}" for i in range(4)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(b, auto_poll=False)          # b는 먼저 끝내 둔다
        mgr.submit(a, auto_poll=False)              # a는 길게 돈다
        qtbot.wait(100)
        t0 = time.perf_counter()
        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill(b)                             # 다른 jobset kill
        waited = time.perf_counter() - t0
        assert waited < 3.0, (
            f"b의 kill이 a의 제출을 기다렸다({waited:.1f}s) — 풀이 합쳐진 셈")
        mgr.cancel_submit(a)
        qtbot.wait(500)
    finally:
        mgr.shutdown()


def test_cancel_is_not_stuck_behind_a_slot_wait(qtbot, fake_lsf):
    """슬롯 대기 중에도 취소가 먹혀야 한다 — 안 그러면 kill이 상한만큼 밀린다."""
    w = _Peak(fake_lsf, cost=0.1)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None),
                        runner=w, workers=2)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(300)],
                               job_keys=[f"k{i}" for i in range(300)])
        mgr.submit(js, auto_poll=False)
        qtbot.wait(200)                             # 대부분 슬롯 대기 중
        t0 = time.perf_counter()
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            mgr.cancel_submit(js)
        assert time.perf_counter() - t0 < 5.0
    finally:
        mgr.shutdown()
