"""백그라운드 kill 진행 조회 — is_killing / kill_state (pull 스냅샷).

대량 chunked kill(특히 MC envpath는 chunk마다 env source, verify는 재조회
루프)도 시간이 걸리니, submit과 대칭으로 진행을 아무 때나 pull로 조회한다.
"""
from __future__ import annotations

import threading

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager, KillProgress
from tests.fake_lsf import FakeLsf
from tests.conftest import submit_cmds


def _gated_bkill_runner(fake: FakeLsf, gate: threading.Event):
    """bkill(및 tcsh 경유)만 gate가 풀릴 때까지 붙잡아 kill을 in-flight로 유지."""
    def runner(argv, timeout, cwd=None):
        prog = argv[0].rsplit("/", 1)[-1]
        if prog in ("bkill", "tcsh"):
            gate.wait(10)
        return fake(argv, timeout)
    return runner


def test_kill_is_nonblocking_and_snapshot(qtbot, tmp_path):
    fake = FakeLsf()
    gate = threading.Event()
    # chunk_size 작게 → 여러 chunk로 증분 진행 발생. 부착물 없이 id-chunk 강제
    cfg = LsfConfig(chunk_size=5)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg,
                        runner=_gated_bkill_runner(fake, gate),
                        lsf_group_root="")     # group 부착물 없이 → chunk 경로
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"echo {i}" for i in range(20)], auto_poll=False)
        fake.set_all("RUN")
        mgr.querier.query(js.id)

        gate.clear()
        mgr.kill(js)                              # 비동기 — 즉시 반환, bkill 붙잡힘
        # kill이 도는 중인지 pull로 확인
        qtbot.waitUntil(lambda: js.is_killing, timeout=5000)
        snap = js.kill_state
        assert isinstance(snap, KillProgress)
        assert snap.total >= 0 and 0 <= snap.done <= max(snap.total, 20)
        assert snap.remaining == max(0, snap.total - snap.done)

        gate.set()                             # kill 완료
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            pass
        assert not js.is_killing               # 끝나면 False
        assert js.kill_state is None
    finally:
        gate.set()
        mgr.shutdown()


def test_kill_snapshot_none_when_idle(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    assert not js.is_killing
    assert js.kill_state is None


def test_kill_snapshot_progresses(qtbot, tmp_path):
    """chunk가 진행되며 done이 단조 증가하는지 (envpath+chunk 경로)."""
    fake = FakeLsf()
    gate = threading.Event(); gate.set()       # 붙잡지 않음 — 자연 완료
    cfg = LsfConfig(chunk_size=3)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake,
                        collect_clusters=True)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"echo {i}" for i in range(12)], auto_poll=False)
        for r in js.jobs():
            fake.jobs[str(r.job_id)].stat = "RUN"
            fake.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        seen = []
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill(js, envpath="/lsf/busan/cshrc.lsf")   # id-chunk sourced
            for _ in range(50):
                s = js.kill_state
                if s is not None:
                    seen.append(s.done)
                if not js.is_killing:
                    break
                qtbot.wait(1)
        assert fake.alive_jobs() == []
        assert seen == sorted(seen)            # done 비감소
    finally:
        mgr.shutdown()
