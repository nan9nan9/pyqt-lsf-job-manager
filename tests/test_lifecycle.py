"""SubmitGate/KillScope 단위 테스트 — kill 우선권의 구조적 보장 (Qt 무관).

핵심 불변식: barrier 확인과 활동 등록이 한 lock 아래 원자적이므로,
어떤 submit 사이클도 'kill의 취소를 빠져나가는' 세 번째 경우가 없다 —
barrier보다 먼저 등록됐으면 acquire가 취소+대기하고, 나중이면 등록이
거부된다.
"""
from __future__ import annotations

import threading

from lsfmgr.lifecycle import SubmitGate


def _activity(gate, jsid="js1", *, done=True):
    """테스트용 활동 등록 — (token, cancel_event, wait 호출 기록)."""
    ev = threading.Event()
    calls = []

    def wait(timeout_s):
        calls.append(timeout_s)
        return done

    token = gate.register(jsid, ev, wait, 5.0)
    return token, ev, calls


def test_acquire_cancels_and_awaits_registered_activity():
    gate = SubmitGate()
    token, ev, calls = _activity(gate)
    assert token is not None

    scope = gate.kill_scope("js1")
    assert scope.acquire() is True

    assert ev.is_set()                    # barrier 시점 활동이 취소됨
    assert calls == [5.0]                 # 그 활동의 정지를 대기함
    scope.release()


def test_register_refused_while_barrier_up_and_allowed_after_release():
    gate = SubmitGate()
    scope = gate.kill_scope("js1")
    scope.acquire()

    ev = threading.Event()
    assert gate.register("js1", ev, lambda t: True, 5.0) is None  # 거부
    assert gate.register("js2", ev, lambda t: True, 5.0) is not None  # 무관 jobset은 허용

    scope.release()
    assert gate.register("js1", ev, lambda t: True, 5.0) is not None  # 해제 후 허용


def test_nested_kill_barriers_both_must_release():
    gate = SubmitGate()
    s1 = gate.kill_scope("js1")
    s2 = gate.kill_scope("js1")
    s1.acquire()
    s2.acquire()

    ev = threading.Event()
    s1.release()
    assert gate.register("js1", ev, lambda t: True, 5.0) is None  # 아직 s2
    s2.release()
    assert gate.register("js1", ev, lambda t: True, 5.0) is not None


def test_acquire_reports_timeout():
    gate = SubmitGate()
    _activity(gate, done=False)           # 정지 대기 초과를 흉내
    scope = gate.kill_scope("js1")
    assert scope.acquire() is False       # killer가 errors로 보고할 신호
    scope.release()


def test_unregister_idempotent_and_scoped():
    gate = SubmitGate()
    token, _ev, _ = _activity(gate)
    gate.unregister("js1", token)
    gate.unregister("js1", token)         # 중복 해제 — no-op

    scope = gate.kill_scope("js1")
    assert scope.acquire() is True        # 남은 활동 없음 — 즉시 True
    scope.release()


def test_no_deadlock_under_concurrent_stress():
    """다중 스레드 register/unregister/kill-barrier 충돌 — 데드락 없이
    모두 유한 시간 내 종료해야 한다. gate lock은 leaf(쥔 채 호출/대기
    없음)라는 설계 불변식의 스모크 검증."""
    gate = SubmitGate()
    stop = threading.Event()
    errors = []

    def submitter(jsid):
        try:
            while not stop.is_set():
                ev = threading.Event()
                tok = gate.register(jsid, ev, lambda t: True, 0.01)
                if tok is not None:
                    gate.unregister(jsid, tok)
        except Exception as e:            # noqa: BLE001
            errors.append(e)

    def killer(jsid):
        try:
            while not stop.is_set():
                s = gate.kill_scope(jsid)
                s.acquire()
                s.release()
        except Exception as e:            # noqa: BLE001
            errors.append(e)

    threads = ([threading.Thread(target=submitter, args=(f"js{i % 2}",))
                for i in range(4)]
               + [threading.Thread(target=killer, args=(f"js{i % 2}",))
                  for i in range(4)])
    for t in threads:
        t.start()
    stop_timer = threading.Timer(0.5, stop.set)
    stop_timer.start()
    for t in threads:
        t.join(10)                        # 데드락이면 여기서 잡힌다
    stop_timer.cancel()
    stop.set()

    assert not errors, errors
    assert all(not t.is_alive() for t in threads), "데드락/행 감지"
    assert gate._barriers == {}           # barrier 전부 해제됨
