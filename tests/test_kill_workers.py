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
        config=LsfConfig(**cfg),
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


def test_default_bounds_concurrency(qtbot, fake_lsf):
    """기본 4 x chunk 16 = 동시 64건 — submit의 workers=8이 거는 부하와 같은 급.
    기본값이 조용히 커지면 mbatchd 부하가 그만큼 늘어난다."""
    assert (LsfConfig().kill_workers, LsfConfig().kill_chunk_size) == (4, 16)
    w = _WatchBkill(fake_lsf)
    _dt, rpt, recs = _kill_all(qtbot, w, 100, kill_chunk_size=16)
    assert w.calls == 7 and set(w.sizes) == {16, 4}
    assert w.peak <= 4, f"기본값 상한(4)을 넘겨 {w.peak}개 동시 실행"
    assert rpt.unconfirmed == 0
    assert all(r.state is JobState.EXIT for r in recs)


def test_workers_one_is_still_serial(qtbot, fake_lsf):
    """1로 두면 옛 동작(직렬) 그대로 — 부하를 못 늘리는 사이트의 탈출구."""
    w = _WatchBkill(fake_lsf)
    _dt, rpt, recs = _kill_all(qtbot, w, 100,
                               kill_chunk_size=16, kill_workers=1)
    assert w.calls == 7
    assert w.peak == 1, f"kill_workers=1인데 동시 실행 {w.peak}"
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
                                 kill_chunk_size=16, kill_workers=1)   # 옛 동작
    par, _r2, _j2 = _kill_all(qtbot, _WatchBkill(fake_lsf, delay=0.08), 96,
                              kill_chunk_size=16, kill_workers=6)
    print(f"\n직렬 {serial:.2f}s → 병렬(6) {par:.2f}s")
    assert par < serial * 0.6, f"직렬 {serial:.2f}s / 병렬 {par:.2f}s"


def test_progress_never_goes_backwards(qtbot, fake_lsf):
    """병렬 chunk가 섞여도 진행 누적치가 되감기면 안 된다."""
    seen = []
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(kill_chunk_size=4,
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


def test_out_of_range_workers_is_rejected_on_both_paths():
    """예전엔 LsfConfig가 조용히 보정하고 옵션 계층만 거부했다 — 같은 값이
    경로에 따라 다르게 처리돼 오타가 묻혔다. 이제 둘 다 거부한다."""
    for bad in (0, 999):
        with pytest.raises(ValueError):
            LsfConfig(kill_workers=bad)
        with pytest.raises(ValueError):
            LsfJobManager(kill_workers=bad)
    assert LsfConfig(kill_workers=1).kill_workers == 1
    assert LsfConfig(kill_workers=32).kill_workers == 32


# ----------------------------------------------------------------------
# 병렬화가 새로 만든 스레드 경계
# ----------------------------------------------------------------------
def test_qt_signals_are_not_emitted_from_executor_threads(qtbot, fake_lsf):
    """chunk를 병렬로 돌려도 Qt 신호는 **Qt가 아는 스레드**에서만 나가야 한다.

    이 라이브러리의 다른 발화 지점은 전부 main이거나 QThread(Pool) 위다.
    순수 파이썬 스레드(ThreadPoolExecutor)에서 쏘면 Qt가 그 스레드를 임시로
    입양했다 스레드 종료와 함께 파기하는 경로가 kill마다 반복된다 — 여기만
    예외를 만들 이유가 없다. 집계·통지는 호출 스레드가 한다(as_completed)."""
    import lsfmgr.killer as killer_mod

    emitted_from = []
    real = killer_mod._KillTask._emit_progress

    def spy(self, done, total):
        emitted_from.append(threading.current_thread().name)
        return real(self, done, total)

    killer_mod._KillTask._emit_progress = spy
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(kill_chunk_size=4,
                         kill_workers=4, progress_min_interval_s=0.0,
                         progress_min_step_ratio=0.0),
        runner=_WatchBkill(fake_lsf, delay=0.01))
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
            js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(40)],
                             auto_poll=False)
        with qtbot.waitSignal(mgr.kill_finished, timeout=30000):
            mgr.kill(js)
        qtbot.wait(200)
    finally:
        killer_mod._KillTask._emit_progress = real
        mgr.shutdown()
    assert emitted_from, "진행 통지가 아예 없었다 — 테스트가 무의미"
    bad = sorted({t for t in emitted_from if t.startswith("lsfmgr-bkill")})
    assert not bad, f"executor 스레드에서 Qt 신호 발화: {bad}"


def test_shutdown_during_a_parallel_kill_leaves_no_threads(qtbot, fake_lsf):
    """chunk가 병렬로 도는 한복판에 shutdown — 좀비 스레드가 남으면 안 된다."""
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(kill_chunk_size=4,
                         kill_workers=8),
        runner=_WatchBkill(fake_lsf, delay=0.3))
    with qtbot.waitSignal(mgr.submit_finished, timeout=30000):
        js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(200)],
                         auto_poll=False)
    mgr.kill(js)
    qtbot.wait(150)                            # chunk가 도는 한복판
    mgr.shutdown()                             # 진행 중 bkill이 끝날 때까지만
    qtbot.wait(200)
    left = [t.name for t in threading.enumerate()
            if t.name.startswith("lsfmgr-bkill")]
    assert not left, f"executor 스레드 잔존: {left}"


def test_parallel_kill_under_stress_keeps_every_contract(qtbot, fake_lsf):
    """jobset 4개를 verify와 함께 동시에 병렬 kill — 계약이 다 버티나."""
    import random

    problems = []
    progress = []
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(kill_chunk_size=4,
                         kill_workers=16, kill_retry_delay_s=0.01,
                         progress_min_interval_s=0.0,
                         progress_min_step_ratio=0.0),
        runner=_WatchBkill(fake_lsf, delay=0.005))
    mgr.error_occurred.connect(lambda j, m: problems.append(f"error: {m}"))
    mgr.kill_progress.connect(lambda j, d, t: progress.append((j, d)))
    try:
        sets = []
        for k in range(4):
            with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
                sets.append(submit_cmds(
                    mgr, [f"mytool {k}_{i}.sp" for i in range(120)],
                    auto_poll=False, workers=8))
        for js in sets:
            mgr.kill(js, verify=True)
        qtbot.waitUntil(lambda: all(not mgr.is_killing(js.id) for js in sets),
                        timeout=120000)
        qtbot.wait(300)

        for js in sets:
            if any(r.state is not JobState.EXIT for r in js.jobs()):
                problems.append(f"{js.id}: 안 죽은 레코드")
            s = dict(mgr.summary(js.id))
            total = s.pop("total")
            if sum(s.values()) != total:
                problems.append(f"{js.id}: 요약 불변식 {s} != {total}")
            if not mgr.store._debug_counts_ok(js.id):
                problems.append(f"{js.id}: 증분 카운트 불일치")
        if fake_lsf.alive_jobs():
            problems.append(f"kill 후 살아있는 job "
                            f"{len(fake_lsf.alive_jobs())}건")
        seen = {}
        for jsid, done in progress:            # jobset별 진행 단조증가
            if done < seen.get(jsid, 0):
                problems.append(f"{jsid}: 진행 되감김 {seen[jsid]} → {done}")
            seen[jsid] = done
    finally:
        mgr.shutdown()
    assert not problems, "\n".join(problems[:10])


# ----------------------------------------------------------------------
# kill_workers는 **전역** 상한 — kill 명령이 몇 건 동시에 돌든
# ----------------------------------------------------------------------
def test_kill_workers_is_a_global_cap(qtbot, fake_lsf):
    """bkill 실행 풀을 kill 호출마다 새로 만들면 kill_workers가 kill 1건의
    상한일 뿐이라, 동시에 kill이 여러 건 돌면 그 배수만큼 bkill이 뜬다.
    (Killer 풀은 4인데 quiesce 중 releaseThread로 슬롯을 반납하므로 4보다
     많은 kill이 chunk 단계에 겹친다 — 실측 동시 16개.)
    workers를 전역으로 만든 것과 같은 이유로 여기도 공용 풀 하나를 쓴다."""
    w = _WatchBkill(fake_lsf, delay=0.05)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(kill_chunk_size=4, kill_workers=4),
        runner=w)
    try:
        sets = []
        for k in range(6):                   # kill 명령 6건을 동시에
            with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
                sets.append(submit_cmds(
                    mgr, [f"mytool {k}_{i}.sp" for i in range(60)],
                    auto_poll=False, workers=8))
        for js in sets:
            mgr.kill(js)
        qtbot.waitUntil(lambda: all(not mgr.is_killing(js.id) for js in sets),
                        timeout=120000)
        assert w.peak <= 4, (
            f"동시 bkill {w.peak}개 — 전역 상한 4를 넘었다")
        assert w.peak > 1, "병렬로 안 돌았다(테스트가 무의미)"
        for js in sets:
            assert all(r.state is JobState.EXIT for r in js.jobs())
    finally:
        mgr.shutdown()


def test_bkill_pool_is_closed_after_the_killer(qtbot, fake_lsf):
    """풀을 killer보다 먼저 닫으면 진행 중 kill이 RuntimeError로 무산된다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(kill_chunk_size=4, kill_workers=4),
                        runner=_WatchBkill(fake_lsf, delay=0.05))
    with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
        js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(200)],
                         auto_poll=False, workers=8)
    errors = []
    mgr.error_occurred.connect(lambda j, m: errors.append(m))
    mgr.kill(js)
    qtbot.wait(80)                           # chunk가 도는 한복판
    mgr.shutdown()                           # killer → bkill 풀 순서
    qtbot.wait(200)
    assert not errors, errors
    assert mgr.command._bkill_pool is None
    left = [t.name for t in threading.enumerate()
            if t.name.startswith("lsfmgr-bkill")]
    assert not left, left


def test_serial_mode_creates_no_pool():
    """kill_workers=1이면 스레드를 아예 안 만든다."""
    from lsfmgr.command import LsfCommand
    assert LsfCommand(LsfConfig(kill_workers=1))._bkill_pool is None
    assert LsfCommand(LsfConfig(kill_workers=4))._bkill_pool is not None
