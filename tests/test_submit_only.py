"""submit(js, only=[...]) — jobset의 일부 job만 제출.

가드의 핵심: '전원 비활성' 요구가 **제출 대상에만** 걸린다. 그래서 다른
job이 RUN 중이어도 선택분만 돌릴 수 있다. 대상 job은 리셋(이전 job_id/이력
소거) 후 재제출되므로, 대상 자신이 활성이면 여전히 거부된다 — 안 그러면
LSF에 살아있는 job을 추적 불가로 만든다.
"""
from __future__ import annotations

import pytest

from lsfmgr import JobState, SubmitNotAllowedError
from lsfmgr.errors import JobNotFoundError


def _mk(manager):
    return manager.create_jobset(
        ["customwrapper_sub a.sp", "customwrapper_sub b.sp",
         "customwrapper_sub c.sp"], merge_ids=["a", "b", "c"])


def _states(js):
    return {r.merge_id: r.state for r in js.jobs()}


def test_only_submits_selected_and_leaves_rest(qtbot, manager, fake_lsf):
    """선택분만 제출되고 나머지는 CREATED 그대로."""
    js = _mk(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, only=["a", "c"], auto_poll=False)

    assert blk.args[1].total == 2                    # 리포트도 선택분 기준
    st = _states(js)
    assert st["a"] is JobState.PEND and st["c"] is JobState.PEND
    assert st["b"] is JobState.CREATED               # 손 안 댐
    assert len(fake_lsf.jobs) == 2                   # LSF에도 2건만


def test_only_allowed_while_others_active(qtbot, manager, fake_lsf):
    """다른 job이 활성이어도 선택분만이면 제출된다 — only의 존재 이유."""
    js = _mk(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["a"], auto_poll=False)   # a = PEND(활성)

    assert manager.can_submit(js) is False            # 전체는 막힌다
    assert manager.can_submit(js, only=["b"]) is True  # 선택분은 열린다

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["b"], auto_poll=False)
    st = _states(js)
    assert st["a"] is JobState.PEND and st["b"] is JobState.PEND
    assert st["c"] is JobState.CREATED


def test_only_rejects_active_target(qtbot, manager, fake_lsf):
    """대상 자신이 활성이면 거부 — 리셋이 살아있는 job을 추적 불가로 만든다."""
    js = _mk(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["a"], auto_poll=False)

    with pytest.raises(SubmitNotAllowedError, match="활성") as ei:
        manager.submit(js, only=["a"], auto_poll=False)
    assert ei.value.job_keys
    assert manager.can_submit(js, only=["a"]) is False


def test_only_ref_forms_and_dedup(qtbot, manager, fake_lsf):
    """ref는 job_key / merge_id / job_id 아무거나 — 같은 job은 1회만."""
    js = _mk(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["a"], auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)

    rec = next(r for r in js.jobs() if r.merge_id == "a")
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, only=[rec.job_key, rec.merge_id, rec.job_id],
                       auto_poll=False)
    assert blk.args[1].total == 1                    # 세 형태 → 같은 job 1건


def test_only_unknown_ref_and_empty(qtbot, manager, fake_lsf):
    """없는 ref는 JobNotFoundError, 빈 선택은 SubmitNotAllowedError.

    빈 선택을 조용히 no-op으로 넘기면 '선택했는데 안 돌았다'가 묻힌다."""
    js = _mk(manager)
    with pytest.raises(JobNotFoundError):
        manager.submit(js, only=["nope"], auto_poll=False)
    with pytest.raises(SubmitNotAllowedError, match="빈 선택"):
        manager.submit(js, only=[], auto_poll=False)
    assert all(r.state is JobState.CREATED for r in js.jobs())


def test_only_rearms_handler_for_selected_only(qtbot, manager, fake_lsf):
    """재제출의 handler 장부 리셋도 선택분에만 적용된다 —
    안 돌린 job의 완료 이력이 되살아나 handler가 다시 돌면 안 된다."""
    js = _mk(manager)
    seen = []
    manager.add_handler(js.id, "h", lambda ctx: 1)
    manager.handler_finished.connect(lambda j, n, r: seen.append(r.job_key))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["a"], auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(js)
    qtbot.wait(200)
    a_key = next(r.job_key for r in js.jobs() if r.merge_id == "a")
    assert a_key in seen                              # a는 돌았다

    seen.clear()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, only=["b"], auto_poll=False)   # a는 손 안 댐
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(js)
    qtbot.wait(200)
    assert a_key not in seen, "안 돌린 job의 handler가 재발화했다"


def test_submit_without_only_still_submits_all(qtbot, manager, fake_lsf):
    """only 미지정은 종전대로 전 job — 기본 동작이 바뀌지 않는다."""
    js = _mk(manager)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, auto_poll=False)
    assert blk.args[1].total == 3
    assert all(r.state is JobState.PEND for r in js.jobs())
