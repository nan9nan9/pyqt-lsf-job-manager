"""단계 순서 계약 — kill과 submit 착수는 순서를 바꾸면 조용히 깨진다.

두 함수(_run_kill, _launch_cycle)는 예전에 100줄이 넘는 한 덩어리였고, 순서
제약이 **주석으로만** 지켜졌다. 단계를 이름 붙은 메서드로 나눴지만 이름만으로는
순서가 강제되지 않는다 — 여기서 실행 순서를 직접 관찰해 고정한다.

각 순서가 왜 그래야 하는지:
  kill  ① quiesce → ② key→target 해석 : 먼저 해석하면 제출 중이라 job_id가
          없던 job이 대상에서 빠져 kill을 빠져나간다.
        ③ verify → ④ 마킹 : 먼저 EXIT로 찍으면 그 레코드가 재조회 대상에서
          빠져 verify가 생존을 영영 못 본다(verify 무력화).
  submit ③ task 전부 생성 → ⑤ fan-out : 생성이 실패하면 어느 task도 시작하기
          전에 예외가 나야 한다(부분 착수 금지).
         ④ 무장 신호 → ⑤ fan-out : 즉시 완주하는 task의 finished가 무장보다
          먼저 도착하면 post_process 판정이 no-op으로 유실된다.
"""
from __future__ import annotations

import threading

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds


def _trace(obj, names, log, lock=None):
    """메서드 호출 순서를 기록하는 스파이를 건다."""
    lock = lock or threading.Lock()
    for n in names:
        real = getattr(obj, n)

        def spy(*a, _n=n, _r=real, **kw):
            with lock:
                log.append(_n)
            return _r(*a, **kw)
        setattr(obj, n, spy)


# ----------------------------------------------------------------------
# kill — quiesce → 해석 → confirm → verify → 마킹
# ----------------------------------------------------------------------
def test_kill_phase_order(qtbot, fake_lsf, monkeypatch):
    import lsfmgr.killer as K

    log, lock = [], threading.Lock()
    for name in ("_quiesce", "_resolve_keys", "_kill_confirm", "_verify",
                 "_mark_killed"):
        real = getattr(K._KillTask, name)

        def spy(self, *a, _n=name, _r=real, **kw):
            with lock:
                log.append(_n)
            return _r(self, *a, **kw)
        monkeypatch.setattr(K._KillTask, name, spy)

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            js = submit_cmds(mgr, ["mytool a.sp", "mytool b.sp"],
                             auto_poll=False)
        keys = [r.job_key for r in js.jobs()]
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
            mgr.kill_jobs(js, keys, verify=True)     # key 경로 + verify
    finally:
        mgr.shutdown()

    assert log == ["_quiesce", "_resolve_keys", "_kill_confirm",
                   "_verify", "_mark_killed"], log


def test_kill_marking_never_precedes_verify(qtbot, fake_lsf, monkeypatch):
    """verify가 마킹보다 뒤로 가면 항상 0을 반환한다 — EXIT로 찍힌 레코드는
    재조회 대상(_ON_LSF)에서 빠지기 때문이다."""
    import lsfmgr.killer as K

    order = []
    for name in ("_verify", "_mark_killed"):
        real = getattr(K._KillTask, name)

        def spy(self, *a, _n=name, _r=real, **kw):
            order.append(_n)
            return _r(self, *a, **kw)
        monkeypatch.setattr(K._KillTask, name, spy)

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000) as blk:
            mgr.kill(js, verify=True)
        assert order == ["_verify", "_mark_killed"], order
        assert blk.args[1].still_alive == 0
        assert js.jobs()[0].state is JobState.EXIT
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# submit 착수 — started → 리셋 → task 생성 → 무장 → fan-out
# ----------------------------------------------------------------------
def test_launch_phase_order(qtbot, fake_lsf, monkeypatch):
    from lsfmgr.submitter import BulkSubmitter

    log, lock = [], threading.Lock()

    def note(name):
        def deco(real):
            def spy(self, *a, **kw):
                with lock:
                    log.append(name)
                return real(self, *a, **kw)
            return spy
        return deco

    monkeypatch.setattr(BulkSubmitter, "_reset_records",
                        note("reset")(BulkSubmitter._reset_records))
    monkeypatch.setattr(BulkSubmitter, "_make_resubmit_task",
                        note("make_task")(BulkSubmitter._make_resubmit_task))

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    mgr.submitter.records_reset.connect(
        lambda j, t: log.append("arm") if "arm" not in log else None)
    started = []
    mgr.submit_started.connect(lambda j: started.append(j))
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            submit_cmds(mgr, [f"mytool {i}.sp" for i in range(3)],
                        auto_poll=False)
        qtbot.wait(200)
    finally:
        mgr.shutdown()

    assert started, "started가 안 나갔다"
    assert log[0] == "reset", log
    # task는 **전부** 만든 뒤에 무장 신호가 나가야 한다(부분 착수 금지)
    assert log.count("make_task") == 3
    assert log.index("arm") > max(i for i, v in enumerate(log)
                                  if v == "make_task"), log


def test_arming_precedes_fanout(qtbot, fake_lsf):
    """무장(records_reset)이 첫 task 실행보다 먼저여야 한다 — 즉시 완주하는
    task의 finished가 무장보다 먼저 도착하면 post_process가 유실된다."""
    seen = []
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                        runner=fake_lsf)
    mgr.submitter.records_reset.connect(lambda j, t: seen.append("arm"))
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        out = []
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False, post_process=out.append)
        mgr.kill(js)
        qtbot.wait(400)
        mgr.query_once(js)
        qtbot.waitUntil(lambda: bool(out), timeout=20000)
        assert "arm" in seen, "무장 신호가 안 나갔다"
        assert out, "post_process가 유실됐다"
    finally:
        mgr.shutdown()
