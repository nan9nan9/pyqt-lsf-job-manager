"""전체 정독 리뷰 사이클 18 — merge_id 삭제/job_key 통합 직후의 정독.

- C18-1: 같은 ref(job_id)를 API마다 다르게 해석했다. _find_job은 레코드를
  직접 훑어 `hits[0]`을 골랐고, _resolve_refs는 parent 전용 색인을 썼다.
  job_id가 array parent와 element에 겹칠 때 _find_job은 **먼저 나온 것**을
  집으므로, 레코드 삽입 순서에 따라 set_user_data(js, 9500)이 element를,
  remove_jobs(js, [9500])은 parent를 가리킨다. ref는 하나의 어휘여야 한다.
  → _find_job이 _resolve_refs에 위임한다(해석 규칙 한 벌).

- C18-3: 교체가 delete+add라 그 job이 목록 **끝으로 밀렸다**. 키는 유지되지만
  get_jobs() 순서가 바뀌어, 순서로 렌더링하는 표에서는 행이 점프한다 —
  "교체해도 행이 이어진다"는 계약의 요점이 순서 연속성인데 그게 깨졌다.
  → 제자리 교체(update_job)로 바꿔 삽입 순서를 유지한다.

- C18-2: array job_id 안내가 **따르면 또 막히는** 문구였다. 공용 resolver가
  "element는 job_key로 지정하세요"라고 했는데, 삭제는 그게 되지만 제출은
  element를 job_key로 줘도 거부한다("array element는 개별 제출할 수 없습니다").
  안내대로 했다가 또 막히면 사용자는 두 번 헤맨다.
  → resolver는 사실만 말하고(그 id의 parent 레코드가 없음), 무엇이 가능한지는
    각 명령의 오류가 이어서 알린다.
"""
from __future__ import annotations

from dataclasses import replace as dc_replace

import pytest

from lsfmgr import JobRecord, JobState
from tests.conftest import mk_jobset


def _elements_first(manager, jsid, parent_id=9500, n=3):
    """element를 먼저, parent를 나중에 넣는다 — 순서 의존을 드러내는 배치."""
    manager.store.store_add_jobs([JobRecord(
        job_id=parent_id, array_index=i, jobset_id=jsid,
        job_key=f"{jsid}[{i}]", state=JobState.DONE, command="r")
        for i in range(n)])
    manager.store.store_add_jobs([JobRecord(
        job_id=parent_id, array_index=None, jobset_id=jsid,
        job_key="p", state=JobState.DONE, command="r")])


def test_job_id_ref_resolves_the_same_everywhere(qtbot, manager, fake_lsf):
    """C18-1: job_id ref는 어느 API에서든 같은 job(=parent)을 가리킨다."""
    js = mk_jobset(manager, [])
    _elements_first(manager, js.id)

    assert manager._resolve_refs(js.id, [9500])[0].job_key == "p"
    assert manager._find_job(js.id, 9500).job_key == "p"
    # 공개 API로도 확인 — set_user_data는 _find_job, remove_jobs는 _resolve_refs
    assert manager.set_user_data(js, 9500, {"v": 1}).job_key == "p"
    assert [r.job_key for r in manager.remove_jobs(js, [9500])] == ["p"]


def test_array_job_id_message_is_not_self_contradicting(qtbot, manager,
                                                        fake_lsf):
    """C18-2: element만 있는 job_id 안내가 '따르면 또 막히는' 문구가 아니다."""
    js = mk_jobset(manager, [])
    manager.store.store_add_jobs([JobRecord(
        job_id=9500, array_index=i, jobset_id=js.id,
        job_key=f"{js.id}[{i}]", state=JobState.DONE, command="r")
        for i in range(2)])

    with pytest.raises(ValueError) as ei:
        manager.submit(js, only=[9500])
    msg = str(ei.value)
    assert "parent 레코드가 없음" in msg
    # 제출에서는 element를 job_key로 줘도 거부되므로, 그렇게 하라고 안내하면 안 된다
    assert "job_key로 지정" not in msg


def test_element_can_still_be_removed_by_its_key(qtbot, manager, fake_lsf):
    """삭제는 element를 job_key로 지목할 수 있다 — 제출과 다른 지점."""
    js = mk_jobset(manager, [])
    _elements_first(manager, js.id, n=2)
    removed = manager.remove_jobs(js, [f"{js.id}[1]"])
    assert [r.job_key for r in removed] == [f"{js.id}[1]"]
    assert f"{js.id}[1]" not in {r.job_key for r in js.jobs()}


def test_find_job_still_raises_for_unknown_ref(qtbot, manager, fake_lsf):
    """위임 후에도 없는 ref는 JobNotFoundError 그대로."""
    from lsfmgr.errors import JobNotFoundError

    js = mk_jobset(manager, ["c"], job_keys=["x"])
    with pytest.raises(JobNotFoundError):
        manager.set_user_data(js, "없는키", {"v": 1})
    assert manager.set_user_data(js, "x", {"v": 2}).user_data == {"v": 2}


def test_remove_jobs_keeps_ref_order_and_dedups(qtbot, manager, fake_lsf):
    """지정 순서를 따르고, 같은 job을 두 형태로 줘도 1회만 지운다."""
    js = mk_jobset(manager, ["a", "b", "c"], job_keys=["x", "y", "z"])
    rec = next(r for r in js.jobs() if r.job_key == "z")
    manager.store.update_job(dc_replace(rec, job_id=777))

    removed = manager.remove_jobs(js, ["z", "x", 777])   # 777 == "z"
    assert [r.job_key for r in removed] == ["z", "x"]
    assert [r.job_key for r in js.jobs()] == ["y"]


def test_replace_keeps_position_not_just_key(qtbot, manager, fake_lsf):
    """C18-3: 교체된 job이 목록 끝으로 밀리지 않는다.

    키만 같고 순서가 바뀌면, 순서로 렌더링하는 표에서는 행이 점프한다."""
    js = mk_jobset(manager, ["a", "b", "c"], job_keys=["k1", "k2", "k3"])
    manager.replace_jobs(js, ["a2"], job_keys=["k1"])
    assert [r.job_key for r in js.jobs()] == ["k1", "k2", "k3"]
    assert js.jobs()[0].command == "a2"

    # upsert의 교체분도 제자리, 추가분만 뒤에 붙는다
    manager.upsert_jobs(js, ["b2", "d"], job_keys=["k2", "k4"])
    assert [r.job_key for r in js.jobs()] == ["k1", "k2", "k3", "k4"]
    assert js.jobs()[1].command == "b2"
