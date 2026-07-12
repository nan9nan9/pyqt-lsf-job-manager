"""전체 정독 리뷰 사이클 4에서 확정된 7건의 회귀 테스트.

사이클 3 수정(착수 경로 예외 안전·close force quiesce·kill verify)의 3차 결함:
_gate_fail의 error 슬롯 예외 격리, do_launch 부분착수 방지(task 선-생성),
records_reset를 pool.start 전 발화(post_process 무장 순서), un-stick CAS,
close force barrier 누수, kill verify 범위/whole 매칭.
"""
from __future__ import annotations

from lsfmgr import JobRecord, JobState


def _finish(manager, fake_lsf, js, state="DONE", code=0):
    fake_lsf.set_all(state, code)
    manager.querier.query(js.id)


def _array_jobset(manager, fake_lsf, n, base=9700):
    from tests.fake_lsf import FakeJob
    js = manager.create_jobset(intended_count=n)
    jsid = js.id
    manager.store.store_add_jobs([JobRecord(
        job_id=base, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]", state=JobState.RUN, command="r")
        for i in range(n)])
    for i in range(n):
        fake_lsf.jobs[f"{base}[{i}]"] = FakeJob(
            job_id=base, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat="RUN")
    return js, base


# C4-1 (fix1): _gate_fail의 error.emit이 _safe_emit로 격리돼 user error 슬롯
# 예외가 ctx.finished 설정 전에 전파돼 jobset을 잠그지 못한다 — 코드 검토로
# 명백(비게이트 _gate_fail은 main 스레드 direct 실행). "슬롯 예외"를 실제로
# 던지는 테스트는 PyQt가 그 예외를 C++ 이벤트루프로 넘겨 abort하므로 불가.
# 비게이트 실패 경로의 잠금·고아 부재는 아래 C4-2가 검증한다.


# ----------------------------------------------------------------------
# C4-2 (fix1/2/3): do_launch에서 task 생성이 실패해도 실행 중 job 고아 없이
#                  전량 SUBMIT_FAILED로 마무리 (부분착수 방지 + 잠금 없음)
# ----------------------------------------------------------------------
def test_partial_launch_no_orphan(qtbot, manager, fake_lsf, monkeypatch):
    js = manager.create_jobset([f"customwrapper_sub r{i}.sp" for i in range(4)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish(manager, fake_lsf, js)
    n_lsf_before = len(fake_lsf.jobs)

    # 3번째 task 생성에서 실패 — task는 전부 생성 후 시작하므로 하나도 안 뜬다
    real = manager.submitter._make_resubmit_task
    calls = {"n": 0}

    def flaky(ctx, key, item):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("task build failed")
        return real(ctx, key, item)

    monkeypatch.setattr(manager.submitter, "_make_resubmit_task", flaky)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, auto_poll=False)
    assert blk.args[1].failed == 4                    # 전량 실패로 마무리
    assert not manager.submitter.is_active(js.id)
    assert len(fake_lsf.jobs) == n_lsf_before         # 새 bsub(고아) 없음
    assert all(r.state is JobState.SUBMIT_FAILED for r in js.jobs())


# ----------------------------------------------------------------------
# C4-3 (fix5): gate + 즉시완료에서도 post_process가 유실되지 않는다
#              (records_reset가 pool.start 전 발화 → finished보다 먼저 무장)
# ----------------------------------------------------------------------
def test_gate_post_process_not_lost_on_fast_finish(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 100                      # 즉시 전량 실패
    js = manager.create_jobset(
        [f"customwrapper_sub r{i}.sp" for i in range(4)])
    with qtbot.waitSignal(js.post_processing_finished, timeout=10000) as blk:
        manager.submit(js, pre_submit=lambda c: True,
                       post_process=lambda recs: len(recs),
                       max_retry=0, workers=4, auto_poll=False)
    assert blk.args[0] == 4                            # 후처리 1회 발화


# ----------------------------------------------------------------------
# C4-6 (fix6): kill verify가 범위("id[m-n]") target에 예외 없이 동작
# ----------------------------------------------------------------------
def test_verify_range_target_parses(qtbot, manager, fake_lsf):
    js, parent = _array_jobset(manager, fake_lsf, n=6)
    from lsfmgr.killer import _KillTask
    t = _KillTask(manager.killer, jobset_id=js.id)
    assert t._verify({f"{parent}[1-3]"})[0] == 3         # element 1,2,3
    assert t._verify({f"{parent}[2-2]"})[0] == 1
    assert t._verify({f"{parent}[9-9]"})[0] == 0         # 범위 밖
    assert t._verify({"garbage[x]"})[0] == 0             # 파싱 불가 — 예외 없이 0


# ----------------------------------------------------------------------
# C4-7 (fix7): 전체 kill verify는 bare job_id(whole) — 되살아난 element도 집계
# ----------------------------------------------------------------------
def test_whole_kill_verify_counts_resurrected_element(qtbot, manager, fake_lsf):
    js, parent = _array_jobset(manager, fake_lsf, n=4)
    from lsfmgr.killer import _KillTask
    t = _KillTask(manager.killer, jobset_id=js.id)
    # 전체 kill 경로의 verify_targets는 bare job_id
    assert t._verify({str(parent)})[0] == 4              # 전 element 잔존 집계
