"""전체 정독 리뷰 사이클 6에서 확정된 회귀 테스트.

사이클 5 수정(started 발화점 이동·kill verify None-fallback)의 5차 결함:

- C6-1: kill verify가 array_index=None(monitor가 접은 집계/비array) 레코드를
  element/범위 target으로 잔존 집계해 형제 element를 과대집계하던 것 —
  사이클 3의 '형제 미집계' 원칙을 되살려, 전체 kill(bare id)만 집계한다.
- C6-2: submit_started 발화점을 do_launch로 옮긴 뒤 born-cancelled(kill barrier
  중 시작)가 started 없이 finished(cancelled)만 내 started↔finished 짝이
  깨지던 것 — born-cancelled도 started를 먼저 내 최소 짝을 맞춘다.
"""
from __future__ import annotations

from lsfmgr import JobRecord, JobState


# ----------------------------------------------------------------------
# C6-1: 집계(array_index=None) 레코드에 단일 element kill — 형제 과대집계 없음
# ----------------------------------------------------------------------
def test_verify_collapsed_array_not_element_overcounted(qtbot, manager, fake_lsf):
    from lsfmgr.killer import _KillTask
    from tests.fake_lsf import FakeJob

    js = manager.create_jobset(intended_count=1)
    jsid = js.id
    # monitor가 wrapper array를 접은 집계 레코드 (id, None) — RUN 유지
    manager.store.store_add_jobs([JobRecord(
        job_id=8800, array_index=None, jobset_id=jsid,
        lsf_job_name=f"{jsid}", state=JobState.RUN, command="r")])
    fake_lsf.jobs["8800"] = FakeJob(
        job_id=8800, array_index=None, name=f"{jsid}", group=None,
        queue="q", command="r", stat="RUN")
    t = _KillTask(manager.killer, jobset_id=jsid)
    # element/범위 target으로는 집계 레코드를 잔존으로 세지 않는다(형제 과대집계 방지).
    # 집계 레코드는 '여러 element의 합'이라 특정 element의 생사를 판정할 수 없다.
    assert t._verify({"8800[3]"})[0] == 0
    assert t._verify({"8800[0-9]"})[0] == 0
    # 전체 kill(bare id)만 그 job을 잔존으로 집계한다.
    assert t._verify({"8800"})[0] == 1


# ----------------------------------------------------------------------
# C6-2: born-cancelled(kill barrier 중 시작)도 started↔finished 짝을 유지
# ----------------------------------------------------------------------
def test_born_cancelled_pairs_started_finished(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)

    starts, finishes = [], []
    manager.submit_started.connect(lambda j: starts.append(j))
    manager.submit_finished.connect(lambda j, r: finishes.append((j, r)))

    # kill barrier를 올린 채 재제출 → 등록 거부 → born-cancelled
    scope = manager._gate.kill_scope(js.id)
    scope.acquire()
    try:
        with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
            manager.submit(js, auto_poll=False)
    finally:
        scope.release()

    # started·finished가 각각 정확히 1회 — finished만 고아로 나가지 않는다
    # (짝을 세는 구독자의 카운터가 음수로 내려가지 않음).
    assert starts == [js.id]
    assert len(finishes) == 1
    assert blk.args[1].cancelled == 1          # 전원 취소로 정산


# ----------------------------------------------------------------------
# C6-3: 정상 착수는 여전히 started 1회 → finished 1회 (born-cancelled 수정이
#       일반 경로의 짝을 깨지 않았는지 회귀)
# ----------------------------------------------------------------------
def test_normal_submit_still_pairs_once(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    starts, finishes = [], []
    manager.submit_started.connect(lambda j: starts.append(j))
    manager.submit_finished.connect(lambda j, r: finishes.append(j))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    assert starts == [js.id]
    assert finishes == [js.id]
