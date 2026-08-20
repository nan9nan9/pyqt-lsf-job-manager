"""workers는 **전역** 상한 — 공용 QThreadPool 하나가 잡는다.

예전에는 제출 사이클(=진행 중인 jobset)마다 풀을 새로 만들어서, jobset 3개를
동시에 제출하면 wrapper가 8이 아니라 24개 떴다. workers를 낮게 잡아
eauth/mbatchd 과부하를 막으려는 사이트에서 그 보호가 동시 제출 수만큼
무력화된다(8코어 실측: 동시 64개면 GUI main 최대 지연 193ms).

풀을 합치면 pool.waitForDone이 "모든 jobset의 제출이 멎었는가"가 되므로,
전체 kill의 quiesce는 **사이클 카운터**(done/total)로 판정하게 바꿨다 —
그게 아래 test_kill_quiesce_is_scoped_to_its_own_cycle이 지키는 계약이다.
"""
from __future__ import annotations

import threading
import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager


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


def _submit_many(qtbot, fake_lsf, n_sets, per_set=40, call_workers=None, **kw):
    w = _Peak(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=w, **kw)
    try:
        sets = [mgr.create_jobset(
            [f"mytool {k}_{i}.sp" for i in range(per_set)],
            job_keys=[f"k{k}_{i}" for i in range(per_set)])
            for k in range(n_sets)]
        done = [0]
        mgr.submit_finished.connect(
            lambda j, r: done.__setitem__(0, done[0] + 1))
        extra = {} if call_workers is None else {"workers": call_workers}
        for js in sets:
            mgr.submit(js, auto_poll=False, **extra)
        qtbot.waitUntil(lambda: done[0] >= n_sets, timeout=120000)
        for js in sets:
            assert all(r.state is JobState.PEND for r in js.jobs())
        return w.peak
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("n_sets", [1, 3, 8])
def test_concurrent_wrappers_never_exceed_workers(qtbot, fake_lsf, n_sets):
    peak = _submit_many(qtbot, fake_lsf, n_sets, workers=4)
    assert peak <= 4, (
        f"jobset {n_sets}개 동시 제출에 wrapper {peak}개 — 전역 상한 4 초과")


def test_the_limit_is_actually_used(qtbot, fake_lsf):
    """상한까지는 실제로 쓴다 — 과보호로 직렬화되면 안 된다."""
    assert _submit_many(qtbot, fake_lsf, 3, workers=6) >= 5


def test_call_level_workers_can_only_lower(qtbot, fake_lsf):
    """호출별 workers는 전역 상한 **아래로 낮추는** 용도다."""
    assert _submit_many(qtbot, fake_lsf, 1, workers=4, call_workers=32) <= 4
    assert _submit_many(qtbot, fake_lsf, 1, workers=8, call_workers=2) <= 2


def test_kill_quiesce_is_scoped_to_its_own_cycle(qtbot, fake_lsf):
    """풀을 합쳤어도 A jobset의 kill이 B jobset의 제출을 기다리면 안 된다.

    pool.waitForDone으로 정지를 판정하면 정확히 그 일이 생긴다 — 그래서
    사이클 카운터(done/total)로 바꿨다."""
    w = _Peak(fake_lsf, cost=0.05)
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=w, workers=4)
    try:
        long_js = mgr.create_jobset([f"mytool a{i}.sp" for i in range(400)],
                                    job_keys=[f"a{i}" for i in range(400)])
        short = mgr.create_jobset([f"mytool b{i}.sp" for i in range(4)],
                                  job_keys=[f"b{i}" for i in range(4)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(short, auto_poll=False)      # b는 먼저 끝내 둔다
        mgr.submit(long_js, auto_poll=False)        # a는 오래 돈다
        qtbot.wait(150)
        t0 = time.perf_counter()
        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill(short)
        waited = time.perf_counter() - t0
        assert waited < 3.0, (
            f"b의 kill이 a의 제출을 기다렸다({waited:.1f}s) — quiesce가 "
            f"공용 풀 전체를 보고 있다")
        mgr.cancel_submit(long_js)
        qtbot.wait(800)
    finally:
        mgr.shutdown()


def test_full_kill_still_quiesces_its_own_submit(qtbot, fake_lsf):
    """반대 방향 — 제출 중인 그 jobset을 kill하면 제출이 실제로 멎어야 한다
    (미착수분 CANCELLED, 이미 나간 것만 EXIT)."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=_Peak(fake_lsf, cost=0.02), workers=4)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(200)],
                               job_keys=[f"k{i}" for i in range(200)])
        seen = set()
        mgr.submit_finished.connect(lambda j, r: seen.add("submit"))
        mgr.kill_finished.connect(lambda j, r: seen.add("kill"))
        mgr.submit(js, auto_poll=False)
        qtbot.wait(120)
        # quiesce가 사이클을 소진시키므로 submit_finished는 kill_finished
        # **앞에** 온다 — 둘 다 왔는지만 본다.
        mgr.kill(js)
        qtbot.waitUntil(lambda: seen >= {"submit", "kill"}, timeout=60000)
        qtbot.wait(300)
        s = mgr.summary(js.id)
        assert s["total"] == 200
        assert all(JobState(k).is_terminal for k in s if k != "total"), s
        assert s.get("CANCELLED", 0) > 0, s      # 미착수분이 있었어야 정상
        assert not fake_lsf.alive_jobs(), "kill 후 살아있는 job"
    finally:
        mgr.shutdown()


def test_cancel_is_not_stuck_behind_a_slot_wait(qtbot, fake_lsf):
    """슬롯 대기 중에도 취소가 먹혀야 한다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=_Peak(fake_lsf, cost=0.1), workers=2)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(300)],
                               job_keys=[f"k{i}" for i in range(300)])
        mgr.submit(js, auto_poll=False)
        qtbot.wait(200)
        t0 = time.perf_counter()
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            mgr.cancel_submit(js)
        assert time.perf_counter() - t0 < 5.0
    finally:
        mgr.shutdown()


def test_thread_count_does_not_grow_with_jobsets(qtbot, fake_lsf):
    """풀이 하나라 스레드가 사이클 수에 비례해 늘지 않는다.
    (사이클별 풀 시절: jobset 8개 제출 → OS 스레드 67개)"""
    def os_threads():
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("Threads:"):
                    return int(line.split()[1])
        return -1

    base = os_threads()
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(),
                        runner=_Peak(fake_lsf, cost=0.05), workers=8)
    try:
        sets = [mgr.create_jobset([f"mytool {k}_{i}.sp" for i in range(30)],
                                  job_keys=[f"k{k}_{i}" for i in range(30)])
                for k in range(8)]
        done = [0]
        mgr.submit_finished.connect(
            lambda j, r: done.__setitem__(0, done[0] + 1))
        for js in sets:
            mgr.submit(js, auto_poll=False)
        qtbot.waitUntil(lambda: done[0] >= 8, timeout=120000)
        grew = os_threads() - base
    finally:
        mgr.shutdown()
    # 제출 8 + 조율 8 + 폴링/killer/handler/completion 여유.
    # 사이클마다 풀을 만들면 8 x 8 = 64개라 이 선을 훌쩍 넘는다.
    assert grew <= 40, f"스레드가 {grew}개 늘었다 (풀이 사이클마다 생기는 셈)"
