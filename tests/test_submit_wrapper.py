"""submit_wrapper 테스트 — wrapper 커맨드로 제출하고 job_id 로 관리.

lsfmgr 는 각 wrapper 커맨드(customwrapper_sub 등)를 그대로 실행하고 'Job <id>' 를
파싱해 job_id 기반으로 관리한다(‑q/‑J/‑g 조립·주입 없음, 그룹/이름 부착물 없음).
재시도는 비정상 종료(non-zero)만 대상이며 파싱 실패는 재시도하지 않는다.
"""
from __future__ import annotations

from lsfmgr import JobState


def _finish(qtbot, mgr, timeout=15000):
    with qtbot.waitSignal(mgr.submit_finished, timeout=timeout) as blocker:
        pass
    return blocker.args          # (jobset_id, SubmitReport)


# ----------------------------------------------------------------------
# 기본 — 커맨드 실행 + job_id 확보 + 부착물 없음
# ----------------------------------------------------------------------
def test_submit_wrapper_captures_job_id(qtbot, manager, fake_lsf):
    cmds = [f"customwrapper_sub -q normal run_{i}.sp" for i in range(20)]
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        js = manager.submit_wrapper(cmds, workers=8)
    _, report = blocker.args
    assert report.succeeded == 20 and report.failed == 0

    recs = manager.get_jobs(js.id)
    assert len(recs) == 20
    assert all(r.job_id is not None for r in recs)       # 전원 job_id 확보
    assert all(r.state == JobState.PEND for r in recs)

    # 실제로 customwrapper_sub 가 실행됐는지 (bsub 직접 호출 아님)
    assert fake_lsf.calls_of("customwrapper_sub"), "customwrapper_sub 가 실행되지 않음"

    # job_id 만으로 관리 → 그룹/이름 부착물 없음
    jsrec = manager.store.get_jobset(js.id)
    assert jsrec.lsf_group_paths == []
    assert jsrec.name_patterns == []


def test_submit_wrapper_token_list_and_mixed(qtbot, manager, fake_lsf):
    # 문자열 / 토큰 리스트 혼용 + job 마다 다른 커맨드
    cmds = [
        "customwrapper_sub -q normal a.sp",
        ["customwrapper_sub", "-q", "long", "b.v"],
        ["customwrapper_sub", "c.sp"],
    ]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(cmds)
    assert all(r.job_id is not None for r in manager.get_jobs(js.id))
    # 세 커맨드 모두 wrapper를 그대로 거쳐 실행됨
    assert len(fake_lsf.calls_of("customwrapper_sub")) == 3


# ----------------------------------------------------------------------
# 재시도 — 비정상 종료(non-zero)만 재시도
# ----------------------------------------------------------------------
def test_wrapper_retry_on_nonzero(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 2          # 처음 2회 rc!=0 → 재시도로 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        js = manager.submit_wrapper(["customwrapper_sub x.sp"], max_retry=3)
    _, report = blocker.args
    assert report.succeeded == 1
    assert report.retried >= 1
    assert manager.get_jobs(js.id)[0].job_id is not None


def test_wrapper_no_retry_on_parse_fail(qtbot, manager, fake_lsf):
    # rc==0 이지만 'Job <id>' 없음 → NO_JOBID_PARSED. 재시도하면 중복 제출
    # 위험이 있어, max_retry 가 있어도 재시도하지 않고 즉시 실패해야 한다.
    fake_lsf.no_jobid_next_bsub = 5
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blocker:
        js = manager.submit_wrapper(["customwrapper_sub x.sp"], max_retry=3)
    _, report = blocker.args
    assert report.failed == 1 and report.succeeded == 0
    assert report.retried == 0           # 재시도 안 함

    rec = manager.get_jobs(js.id)[0]
    assert rec.state == JobState.SUBMIT_FAILED
    assert rec.fail_reason == "NO_JOBID_PARSED"
    # bsub 흉내는 딱 1회만 호출됐어야 한다 (재시도 없음)
    assert len(fake_lsf.calls_of("customwrapper_sub")) == 1


# ----------------------------------------------------------------------
# 관리 — job_id 기반 kill
# ----------------------------------------------------------------------
def test_wrapper_kill_by_id(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=15000):
        js = manager.submit_wrapper(
            [f"customwrapper_sub run_{i}.sp" for i in range(10)], workers=8)
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(js)
    _, report = blocker.args
    # 부착물이 없으므로 id chunk 전략으로 kill 된다
    assert "chunk" in " ".join(report.strategies)
    assert not fake_lsf.alive_jobs()     # 전원 종료


def test_submit_wrapper_empty_raises(manager):
    import pytest
    with pytest.raises(ValueError):
        manager.submit_wrapper([])
