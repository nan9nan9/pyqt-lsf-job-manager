"""SubmitGate/KillScope 단위 테스트 — kill 우선권의 구조적 보장 (Qt 무관).

핵심 불변식: barrier 확인과 활동 등록이 한 lock 아래 원자적이므로,
어떤 submit 사이클도 'kill의 취소를 빠져나가는' 세 번째 경우가 없다 —
barrier보다 먼저 등록됐으면 acquire가 취소+대기하고, 나중이면 등록이
거부된다.

범위(Scope): barrier·취소·대기는 kill이 겨냥한 job에만 걸린다. `None`은
"그 jobset 전체"(전체 kill), key 집합이면 그 job만 — 선택 kill이 jobset
제출 전체를 무너뜨리지 않으면서도 겨냥한 job은 반드시 멈추게 하는 규약.
"""
from __future__ import annotations

import threading

from lsfmgr.lifecycle import SubmitGate


def _activity(gate, jsid="js1", keys=("a", "b"), *, done=True):
    """테스트용 활동 등록 — (Registration, 취소된 범위 기록, 대기 호출 기록)."""
    cancels = []
    waits = []

    def cancel(scope):
        cancels.append(scope)

    def wait(scope, timeout_s):
        waits.append((scope, timeout_s))
        return done

    reg = gate.register(jsid, keys, cancel, wait, 5.0)
    return reg, cancels, waits


def test_acquire_cancels_and_awaits_registered_activity():
    gate = SubmitGate()
    reg, cancels, waits = _activity(gate)
    assert reg.activity is not None and not reg.refused

    scope = gate.kill_scope("js1")            # 전체 kill
    assert scope.acquire() is True

    assert cancels == [None]                  # 범위 전체로 취소됨
    assert waits == [(None, 5.0)]             # 그 활동의 정지를 대기함
    scope.release()


def test_scoped_acquire_passes_only_targeted_keys():
    """범위 kill은 겨냥한 key만 취소·대기한다 — 나머지 job은 계속 제출된다."""
    gate = SubmitGate()
    _reg, cancels, waits = _activity(gate, keys=["a", "b", "c"])

    scope = gate.kill_scope("js1", ["b"])
    assert scope.acquire() is True
    assert cancels == [frozenset({"b"})]
    assert waits == [(frozenset({"b"}), 5.0)]
    scope.release()


def test_register_refused_while_barrier_up_and_allowed_after_release():
    gate = SubmitGate()
    scope = gate.kill_scope("js1")            # 전체 barrier
    scope.acquire()

    reg = gate.register("js1", ["a"], lambda s: None, lambda s, t: True, 5.0)
    assert reg.activity is None and reg.refused == frozenset({"a"})   # 거부
    other = gate.register("js2", ["a"], lambda s: None,
                          lambda s, t: True, 5.0)
    assert other.activity is not None         # 무관 jobset은 허용

    scope.release()
    after = gate.register("js1", ["a"], lambda s: None,
                          lambda s, t: True, 5.0)
    assert after.activity is not None         # 해제 후 허용


def test_scoped_barrier_refuses_only_its_keys():
    """범위 barrier는 그 key만 거부하고 나머지는 정상 등록시킨다 —
    선택 kill 중에도 대상 아닌 job의 새 제출은 시작될 수 있다."""
    gate = SubmitGate()
    scope = gate.kill_scope("js1", ["b"])
    scope.acquire()

    reg = gate.register("js1", ["a", "b", "c"], lambda s: None,
                        lambda s, t: True, 5.0)
    assert reg.activity is not None           # 사이클 자체는 살아 있다
    assert reg.refused == frozenset({"b"})    # b만 born-cancelled
    scope.release()


def test_scoped_barrier_refusing_every_key_rejects_the_cycle():
    """범위가 그 사이클의 전 key를 덮으면 등록 자체를 거부한다 — 제출이
    하나도 없는 사이클을 kill이 '기다려야 할 활동'으로 넘겨받지 않게."""
    gate = SubmitGate()
    scope = gate.kill_scope("js1", ["a", "b"])
    scope.acquire()

    reg = gate.register("js1", ["a", "b"], lambda s: None,
                        lambda s, t: True, 5.0)
    assert reg.activity is None
    assert reg.refused == frozenset({"a", "b"})
    scope.release()


def test_nested_kill_barriers_both_must_release():
    gate = SubmitGate()
    s1 = gate.kill_scope("js1")
    s2 = gate.kill_scope("js1")
    s1.acquire()
    s2.acquire()

    def _reg():
        return gate.register("js1", ["a"], lambda s: None,
                             lambda s, t: True, 5.0).activity

    s1.release()
    assert _reg() is None                     # 아직 s2
    s2.release()
    assert _reg() is not None


def test_overlapping_scoped_barriers_release_independently():
    """겹친 범위 barrier도 각자 자기 것만 내린다 — 같은 key 집합이어도
    identity로 제거하므로 남의 barrier를 지우지 않는다."""
    gate = SubmitGate()
    s1 = gate.kill_scope("js1", ["a"])
    s2 = gate.kill_scope("js1", ["a"])        # 값이 같은 별개 barrier
    s1.acquire()
    s2.acquire()

    def _refused():
        return gate.register("js1", ["a", "b"], lambda s: None,
                             lambda s, t: True, 5.0).refused

    s1.release()
    assert _refused() == frozenset({"a"})     # s2가 아직 막고 있다
    s2.release()
    assert _refused() == frozenset()
    assert gate._barriers == {}


def test_acquire_reports_timeout():
    gate = SubmitGate()
    _activity(gate, done=False)               # 정지 대기 초과를 흉내
    scope = gate.kill_scope("js1")
    assert scope.acquire() is False           # killer가 errors로 보고할 신호
    scope.release()


def test_unregister_idempotent_and_scoped():
    gate = SubmitGate()
    reg, _cancels, _waits = _activity(gate)
    gate.unregister("js1", reg.activity)
    gate.unregister("js1", reg.activity)      # 중복 해제 — no-op
    gate.unregister("js1", None)              # 미등록 토큰 — no-op

    scope = gate.kill_scope("js1")
    assert scope.acquire() is True            # 남은 활동 없음 — 즉시 True
    scope.release()


def test_empty_cycle_registers_normally():
    """제출할 job이 0건인 사이클은 barrier가 없으면 정상 등록된다 —
    '전 key가 거부됨'(빈 집합 ⊇ 빈 집합)으로 오판하지 않는다."""
    gate = SubmitGate()
    reg = gate.register("js1", [], lambda s: None, lambda s, t: True, 5.0)
    assert reg.activity is not None and not reg.refused


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
                reg = gate.register(jsid, ["a", "b"], lambda s: None,
                                    lambda s, t: True, 0.01)
                if reg.activity is not None:
                    gate.unregister(jsid, reg.activity)
        except Exception as e:            # noqa: BLE001
            errors.append(e)

    def killer(jsid, keys):
        try:
            while not stop.is_set():
                s = gate.kill_scope(jsid, keys)
                s.acquire()
                s.release()
        except Exception as e:            # noqa: BLE001
            errors.append(e)

    threads = ([threading.Thread(target=submitter, args=(f"js{i % 2}",))
                for i in range(4)]
               + [threading.Thread(target=killer,
                                   args=(f"js{i % 2}",
                                         None if i % 2 else ["a"]))
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
