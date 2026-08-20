"""착수 통지 규칙 — 왜 경로마다 submit_started를 내기도 하고 안 내기도 하나.

한 줄 규칙: **착수를 알리는 신호가 하나도 없이 submit_finished만 나가는 일은
없다.** 그 조건을 만족시키는 방법이 경로마다 다를 뿐이다.

  게이트 통과      pre_submit_started → pre_submit_finished(True)
                   → submit_started → submit_finished
  게이트 거부/예외  pre_submit_started → pre_submit_finished(False)
                   → submit_finished          ← submit_started 없음(의도)
  born-cancelled   submit_started → submit_finished
                   (kill barrier 중 시작 — 게이트를 통째로 건너뛰어
                    pre_submit_* 가 아예 안 나가므로 여기서 짝을 맞춘다)
  게이트 없음       submit_started → submit_finished

한쪽만 보고 "게이트 거부에도 started를 내야 짝이 맞는다"고 고치면 README
§4.4의 "(ok=True일 때만) submit_started" 계약이 깨진다. 반대로 born-cancelled
에서 빼면 아무 착수 신호 없이 finished만 나간다. 양쪽을 함께 고정해 둔다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager


class _Log:
    """착수/완료 계열 신호를 순서대로 기록."""

    def __init__(self, mgr):
        self.seq = []
        mgr.pre_submit_started.connect(lambda j: self.seq.append("pre_started"))
        mgr.pre_submit_finished.connect(
            lambda j, ok: self.seq.append(f"pre_finished({ok})"))
        mgr.submit_started.connect(lambda j: self.seq.append("started"))
        mgr.submit_finished.connect(lambda j, r: self.seq.append("finished"))

    @property
    def opened(self):
        """finished 앞에 착수를 알린 신호가 하나라도 있었나."""
        return any(s in ("started", "pre_started") for s in self.seq)


@pytest.fixture
def mgr(qtbot, fake_lsf):
    m = LsfJobManager(store=InMemoryStore(), config=LsfConfig(),
                      runner=fake_lsf)
    yield m
    m.shutdown()


def _js(mgr, n=4):
    return mgr.create_jobset([f"mytool {i}.sp" for i in range(n)],
                             job_keys=[f"k{i}" for i in range(n)])


def test_plain_submit(qtbot, mgr):
    log = _Log(mgr)
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(_js(mgr), auto_poll=False)
    assert log.seq == ["started", "finished"]


def test_gate_pass(qtbot, mgr):
    log = _Log(mgr)
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(_js(mgr), auto_poll=False, pre_submit=lambda c: True)
    assert log.seq == ["pre_started", "pre_finished(True)",
                       "started", "finished"]


def test_gate_reject_has_no_submit_started(qtbot, mgr):
    """README §4.4: submit_started는 **게이트 통과 시에만**.
    착수를 알린 것은 pre_submit_started 쪽이다."""
    log = _Log(mgr)
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(_js(mgr), auto_poll=False, pre_submit=lambda c: False)
    assert log.seq == ["pre_started", "pre_finished(False)", "finished"]
    assert log.opened


def test_gate_exception_has_no_submit_started(qtbot, mgr):
    log = _Log(mgr)

    def boom(cmds):
        raise RuntimeError("게이트 폭발")

    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        mgr.submit(_js(mgr), auto_poll=False, pre_submit=boom)
    assert "started" not in log.seq, log.seq
    assert log.opened


def test_born_cancelled_emits_submit_started(qtbot, mgr):
    """kill barrier 중 시작 — 게이트를 건너뛰어 pre_submit_*가 아예 안 나간다.
    여기서 started를 빼면 아무 착수 신호 없이 finished만 나간다."""
    log = _Log(mgr)
    js = _js(mgr)
    scope = mgr._gate.kill_scope(js.id, None)
    scope.begin()                                  # barrier ↑
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
    finally:
        scope.release()
    assert log.seq == ["started", "finished"], log.seq


def test_finished_never_arrives_unannounced(qtbot, mgr):
    """규칙 자체 — 어느 경로든 finished 앞에 착수 신호가 있다."""
    cases = [
        ("게이트 없음", {}),
        ("게이트 통과", {"pre_submit": lambda c: True}),
        ("게이트 거부", {"pre_submit": lambda c: False}),
    ]
    for label, kw in cases:
        js = _js(mgr)                     # 사이클마다 새 jobset (제출 가드)
        log = _Log(mgr)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False, **kw)
        assert log.seq[-1] == "finished", (label, log.seq)
        assert log.opened, f"{label}: 착수 신호 없이 finished만 나갔다"
