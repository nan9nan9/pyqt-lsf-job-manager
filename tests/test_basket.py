"""바구니 흐름 — create_jobset(빈 핸들) → add_pending(누적) → submit(제출).

GUI의 CREATE 단계부터 jobset이 존재해야 하는 요구: 제출 전엔 핸들이 None
이라 테이블 리셋·None 분기·merge 불가가 생기던 구조를, 바구니 선생성으로
해소한다. 같은 jobset/job_key가 CREATED→SUBMITTING→PEND로 전이되므로
테이블이 처음부터 끝까지 같은 핸들에 바인딩된다.
"""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState
from lsfmgr.errors import LsfmgrError


def test_create_returns_handle_before_submit(manager):
    """제출 전에 핸들이 즉시 나온다 — GUI가 처음부터 바인딩할 대상."""
    js = manager.create_jobset(label="basket")
    assert js.id
    assert js.jobs() == []                    # 빈 바구니
    assert js.summary["total"] == 0


def test_add_pending_accumulates_created(qtbot, manager, fake_lsf):
    """add_pending은 CREATED 레코드를 누적하고 즉시 jobs_updated를 발행한다
    — 표가 제출 전부터 채워진다. intended_count도 함께 는다."""
    js = manager.create_jobset(label="basket")
    batches = []
    js.jobs_updated.connect(batches.append)

    recs = js.add_pending(["customwrapper_sub -i a.sp",
                           ["customwrapper_sub", "-i", "b.sp"]])

    assert [r.state for r in recs] == [JobState.CREATED] * 2
    assert all(r.via_wrapper for r in recs)
    assert js.summary["total"] == 2         # intended_count 갱신
    assert js.summary["CREATED"] == 2
    assert batches and len(batches[0]) == 2   # 표 즉시 갱신
    assert fake_lsf.calls_of("bsub") == []    # 아직 아무것도 제출 안 됨

    js.add_pending("customwrapper_sub -i c.sp")   # 단일 문자열도 허용
    assert js.summary["total"] == 3


def test_submit_transitions_same_jobset(qtbot, manager, fake_lsf):
    """submit은 같은 jobset/job_key를 전이시킨다 — 핸들 교체·테이블 리셋
    없음 (바구니 구조의 핵심 계약)."""
    js = manager.create_jobset(label="basket")
    js.add_pending([f"customwrapper_sub run_{i}.sp" for i in range(3)])
    keys_before = {r.job_key for r in js.jobs()}

    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blocker:
        js.submit(auto_poll=False)

    assert blocker.args[0] == js.id           # 같은 jobset으로 finished
    assert {r.job_key for r in js.jobs()} == keys_before   # key 유지
    assert all(r.state is JobState.PEND for r in js.jobs())
    assert all(r.job_id is not None for r in js.jobs())
    assert len(fake_lsf.calls_of("customwrapper_sub")) == 3   # wrapper 경로


def test_add_pending_jobspec_preserves_options(qtbot, manager, fake_lsf):
    """JobSpec 항목은 bsub 경로 + 옵션(queue 등) 보존 제출."""
    js = manager.create_jobset()
    js.add_pending([JobSpec(command="make sim", queue="priority")])
    rec = js.jobs()[0]
    assert rec.via_wrapper is False and rec.spec_json

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.submit(auto_poll=False)

    assert js.jobs()[0].state is JobState.PEND
    assert fake_lsf.jobs[str(js.jobs()[0].job_id)].queue == "priority"


def test_merge_baskets_in_create_state(qtbot, manager, fake_lsf):
    """CREATE 상태의 바구니끼리 merge — 임시 jobset 없이 누적 목록을 합쳐
    한 번에 제출할 수 있다 (기존 구조에선 불가능하던 유스케이스)."""
    a = manager.create_jobset(label="basket-a")
    b = manager.create_jobset(label="basket-b")
    a.add_pending(["customwrapper_sub a1.sp", "customwrapper_sub a2.sp"])
    b.add_pending(["customwrapper_sub b1.sp"])

    merged = a.merge_with(b)                  # CREATE 상태 merge

    assert merged.summary["total"] == 3
    assert merged.summary["CREATED"] == 3
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        merged.submit(auto_poll=False)
    assert all(r.state is JobState.PEND for r in merged.jobs())


def test_submit_pending_only_touches_created(qtbot, manager, fake_lsf):
    """submit 후 추가된 pending만 다시 제출된다 — 이미 PEND/RUN인 job은
    건드리지 않는다 (증분 제출)."""
    js = manager.create_jobset()
    js.add_pending("customwrapper_sub first.sp")
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.submit(auto_poll=False)
    first_id = js.jobs()[0].job_id

    js.add_pending("customwrapper_sub second.sp")     # 제출 후 추가 누적
    assert js.summary["CREATED"] == 1
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.submit(auto_poll=False)                    # 증분 제출

    by_key = {r.job_key: r for r in js.jobs()}
    assert all(r.state is JobState.PEND for r in by_key.values())
    assert js.jobs()[0].job_id is not None
    # 기존 job은 재제출되지 않았다 (job_id 불변)
    first = next(r for r in js.jobs() if r.job_id == first_id)
    assert first.job_id == first_id
    assert len(fake_lsf.jobs) == 2                    # 총 제출 2건뿐


def test_submit_empty_basket_rejected(manager):
    js = manager.create_jobset()
    with pytest.raises(LsfmgrError, match="CREATED job이 없습니다"):
        js.submit()


def test_remove_pending_keeps_invariant(manager):
    """pending 제거 시 intended_count도 줄어 유령 CREATED가 안 남는다."""
    js = manager.create_jobset()
    recs = js.add_pending(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    js.remove_job(recs[0].job_key)
    assert js.summary["total"] == 1
    assert js.summary["CREATED"] == 1


def test_add_pending_after_remove_no_collision(manager):
    """remove로 비워진 연번이 재사용돼도 중복 없이 동작한다 — 살아있는
    key와의 충돌만 없으면 됨 (add_pending_jobs 선검사가 보장)."""
    js = manager.create_jobset()
    r = js.add_pending(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    js.remove_job(r[1].job_key)               # _1 제거 (빈 자리)
    r2 = js.add_pending("customwrapper_sub c.sp")
    keys = {rec.job_key for rec in js.jobs()}
    assert len(keys) == 2 and r2[0].job_key in keys   # 중복 없음
    assert js.summary["total"] == 2
