"""전체 정독 리뷰 6차 — 아직 정독 안 한 영역(set_user_data / bkill 응답 파싱).

- R6-1: set_user_data가 **읽고-고쳐-쓰기**였다. 스냅샷을 update_job으로 통째
  되쓰기 때문에, 그 사이 submit worker가 기록한 job_id/PEND가 지워진다.
  LSF에서 도는 job이 추적 불가가 되고 레코드는 SUBMITTING에 고착돼 결국
  LOST로 확정된다. → store의 원자적 부분 갱신(transition(new_state=None))
  으로 user_data만 얹는다.

- R6-2: element 하나만 겨냥한 kill("1000[3]")의 응답이 bare 부모 "1000"까지
  '해소'로 만들었다. JobRecord.array_index는 늘 None(집계 레코드)이라 그
  레코드가 target "1000"에 매칭돼 job 전체가 EXIT로 찍힌다 — LSF에선 나머지
  element가 도는데 앱에는 죽은 것으로 보이고, terminal이라 폴링 대상에서도
  빠져 영영 안 고쳐진다. → 부모는 **실제로 요청했을 때만** 유도한다.
"""
from __future__ import annotations

from lsfmgr import JobState
from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# R6-1 — set_user_data는 동시 전이를 덮지 않는다
# ----------------------------------------------------------------------
def test_set_user_data_does_not_clobber_a_concurrent_submit(manager):
    """제출 성공(job_id 확보)과 겹치면 그 job_id가 지워졌다 — LSF에 살아있는
    job이 추적 불가가 되고 레코드는 SUBMITTING에 고착된다."""
    js = manager.create_jobset(["mytool a.sp"], job_keys=["a"])
    store = manager.store
    store.transition(js.id, "a", JobState.SUBMITTING)

    # set_user_data가 대상을 읽은 **직후** worker가 제출 성공을 기록하는 순간
    real_find = manager._find_job

    def find_then_race(jsid, ref):
        rec = real_find(jsid, ref)
        store.transition(jsid, "a", JobState.PEND, job_id=98765)
        return rec

    manager._find_job = find_then_race
    try:
        new = manager.set_user_data(js, "a", {"n": 1})
    finally:
        manager._find_job = real_find

    assert new.user_data == {"n": 1}
    assert new.job_id == 98765, "제출 성공의 job_id가 지워졌다(추적 불가)"
    assert new.state is JobState.PEND, f"상태가 되감겼다: {new.state.value}"


# ----------------------------------------------------------------------
# R6-2 — element kill이 job 레코드 전체를 죽이지 않는다
# ----------------------------------------------------------------------
def test_element_kill_does_not_mark_the_whole_job_exited(qtbot, manager):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND and rec.job_id is not None

    # element 3만 겨냥한 raw kill — 이 job에는 element가 없다
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill_jobs([f"{rec.job_id}[3]"], jobset_id=js.id)

    after = js.jobs()[0]
    assert after.state is JobState.PEND, (
        f"element만 겨냥했는데 job 레코드 전체가 {after.state.value}")
    assert not after.killed


def test_whole_array_kill_still_resolves_from_element_replies(qtbot, manager):
    """반대 방향 — bare 부모로 array를 kill하면 LSF는 element별로 확인 행을
    낸다. 그 응답으로 부모 target이 해소돼야 불필요한 재시도가 없다."""
    from lsfmgr.command import _parse_bkill_resolved
    text = ("Job <1000[0]> is being terminated\n"
            "Job <1000[1]> is being terminated\n")
    assert _parse_bkill_resolved(text, {"1000"}) == {
        "1000[0]", "1000[1]", "1000"}
