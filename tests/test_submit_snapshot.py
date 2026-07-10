"""백그라운드 submit 진행 조회 — is_submitting / submit_state (pull 스냅샷).

submit()은 즉시 반환(비동기, worker 스레드)이라 앱은 진행 dialog를 닫고 딴
작업을 할 수 있다. 그 뒤에도 이 API로 현재 진행을 아무 때나 조회한다.
"""
from __future__ import annotations

import threading
import time

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager, SubmitProgress
from tests.fake_lsf import FakeLsf
from tests.conftest import submit_cmds


def _gated_runner(fake: FakeLsf, gate: threading.Event):
    """bsub만 gate가 풀릴 때까지 붙잡는 runner — 제출을 in-flight로 유지."""
    def runner(argv, timeout):
        prog = argv[0].rsplit("/", 1)[-1]
        if prog == "bsub":
            gate.wait(10)
        return fake(argv, timeout)
    return runner


def test_submit_is_nonblocking_and_snapshot(qtbot, tmp_path):
    fake = FakeLsf()
    gate = threading.Event()
    cfg = LsfConfig(script_dir=str(tmp_path / "s"))
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg,
                        runner=_gated_runner(fake, gate))
    try:
        t0 = time.monotonic()
        js = submit_cmds(mgr, [f"echo {i}" for i in range(20)], auto_poll=False)
        # submit()은 즉시 반환 (bsub가 붙잡혀 있어도 블로킹 안 함)
        assert time.monotonic() - t0 < 1.0
        assert js.is_submitting                      # 백그라운드로 도는 중

        # 진행 스냅샷 pull — signal 없이도 조회 가능
        snap = js.submit_state
        assert isinstance(snap, SubmitProgress)
        assert snap.total == 20
        assert 0 <= snap.done <= 20
        assert 0.0 <= snap.fraction <= 1.0
        assert snap.remaining == snap.total - snap.done

        # 게이트 풀어 완료
        gate.set()
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            pass
        # 완료 후: is_submitting False, 스냅샷 None (최종은 summary로)
        assert not js.is_submitting
        assert js.submit_state is None
        assert js.summary["PEND"] == 20
    finally:
        gate.set()
        mgr.shutdown()


def test_snapshot_none_when_not_submitting(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    assert not js.is_submitting
    assert js.submit_state is None


def test_snapshot_counts_progress(qtbot, tmp_path):
    """done이 성공/실패를 합산해 증가하는지 — 일부 실패 섞어 확인."""
    fake = FakeLsf()
    fake.fail_next_bsub = 5           # 앞 5건 실패(재시도 없음)
    gate = threading.Event()
    gate.set()                        # 붙잡지 않음 — 자연 완료
    cfg = LsfConfig(script_dir=str(tmp_path / "s"))
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg,
                        runner=_gated_runner(fake, gate))
    try:
        seen_snaps = []
        # 진행 중 여러 번 스냅샷 — done 단조 증가 관찰
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"echo {i}" for i in range(10)], auto_poll=False, max_retry=0)
            for _ in range(30):
                s = js.submit_state
                if s is not None:
                    seen_snaps.append(s.done)
                if not js.is_submitting:
                    break
                qtbot.wait(2)
        # 최종 요약: 5 PEND + 5 SUBMIT_FAILED
        assert js.summary.get("PEND", 0) == 5
        assert js.summary.get("SUBMIT_FAILED", 0) == 5
        # 관찰된 done은 비감소
        assert seen_snaps == sorted(seen_snaps)
    finally:
        mgr.shutdown()
