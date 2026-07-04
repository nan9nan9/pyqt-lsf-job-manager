"""submit / retry / progress / cancel 테스트 (pytest-qt, NFR-8)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, ArrayJobSpec, JobState


def wait_submit_finished(qtbot, mgr, timeout=10000):
    with qtbot.waitSignal(mgr.submit_finished, timeout=timeout) as blocker:
        pass
    return blocker.args          # (jobset_id, SubmitReport)


# ----------------------------------------------------------------------
# 대량 submit (FR-1.1 / FR-1.2)
# ----------------------------------------------------------------------
def test_bulk_submit_parallel(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"run {i}") for i in range(100)]
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = manager.submit_bulk(jobs, parallel=True, workers=8)
    rpt_jsid, report = blocker.args
    assert rpt_jsid == jsid
    assert report.succeeded == 100
    assert report.failed == 0

    s = manager.summary(jsid)
    assert s["total"] == 100
    assert s["PEND"] == 100
    # 전 job이 ID 확보 (수용 기준 1)
    assert all(r.job_id is not None for r in manager.get_jobs(jsid))
    # 부착물 자동 부여 확인 (FR-1.4)
    js = manager.store.get_jobset(jsid)
    assert len(js.lsf_group_paths) == 1
    assert js.name_patterns == [f"{jsid}_*"]


def test_bulk_submit_sequential(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"run {i}") for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs, parallel=False)
    assert manager.summary(jsid)["PEND"] == 10


def test_submit_started_signal(qtbot, manager):
    with qtbot.waitSignal(manager.submit_started, timeout=5000) as blocker:
        jsid = manager.submit_bulk([JobSpec(command="x")])
    assert blocker.args == [jsid]
    qtbot.waitSignal(manager.submit_finished, timeout=5000)


def test_progress_signal(qtbot, manager):
    seen = []
    manager.submit_progress.connect(lambda j, d, t: seen.append((d, t)))
    jobs = [JobSpec(command=f"r {i}") for i in range(50)]
    with qtbot.waitSignal(manager.submit_finished, timeout=15000):
        manager.submit_bulk(jobs)
    assert seen, "progress Signal이 한 번도 오지 않음"
    assert seen[-1] == (50, 50)          # 마지막 통지는 반드시 (total, total)
    assert all(d <= t for d, t in seen)


# ----------------------------------------------------------------------
# retry (FR-2)
# ----------------------------------------------------------------------
def test_retry_then_success(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 2          # 처음 2회 실패 → 재시도로 성공
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = manager.submit_bulk([JobSpec(command="x")], max_retry=3)
    _, report = blocker.args
    assert report.succeeded == 1
    assert report.retried == 1
    rec = manager.get_jobs(jsid)[0]
    assert rec.state is JobState.PEND
    assert rec.retry_count == 2


def test_submit_failed_after_max_retry(qtbot, manager, fake_lsf):
    fake_lsf.fail_next_bsub = 99
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        jsid = manager.submit_bulk([JobSpec(command="x")], max_retry=2)
    _, report = blocker.args
    assert report.failed == 1
    rec = manager.get_jobs(jsid)[0]
    assert rec.state is JobState.SUBMIT_FAILED
    assert rec.fail_reason == "BSUB_EXIT_1"
    assert report.fail_reasons == {"BSUB_EXIT_1": 1}


def test_no_jobid_parse_failure_classified(qtbot, manager, fake_lsf):
    fake_lsf.no_jobid_next_bsub = 99
    with qtbot.waitSignal(manager.submit_finished, timeout=15000) as blocker:
        manager.submit_bulk([JobSpec(command="x")], max_retry=1)
    _, report = blocker.args
    assert report.fail_reasons == {"NO_JOBID_PARSED": 1}


# ----------------------------------------------------------------------
# cancel (QT-6)
# ----------------------------------------------------------------------
def test_cancel_submit(qtbot, manager, fake_lsf):
    # rate limit으로 느리게 만들어 중간 취소 여지를 확보
    jobs = [JobSpec(command=f"r {i}") for i in range(50)]
    with qtbot.waitSignal(manager.submit_finished, timeout=30000) as blocker:
        jsid = manager.submit_bulk(jobs, workers=1, rate_limit_per_s=20)
        manager.cancel_submit(jsid)
    _, report = blocker.args
    assert report.total == 50
    assert report.succeeded + report.failed + report.cancelled == 50
    assert report.cancelled > 0
    # 이미 submit된 job은 JobSet에 정상 기록 (QT-6)
    pend = manager.get_jobs(jsid, states={JobState.PEND})
    assert len(pend) == report.succeeded


# ----------------------------------------------------------------------
# array (FR-1.3)
# ----------------------------------------------------------------------
def test_array_submit(qtbot, manager, fake_lsf):
    spec = ArrayJobSpec(command="run.sh $LSB_JOBINDEX", count=20)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blocker:
        jsid = manager.submit_array(spec)
    _, report = blocker.args
    assert report.succeeded == 1          # bsub 1회
    assert len(fake_lsf.calls_of("bsub")) == 1

    recs = manager.get_jobs(jsid)
    assert len(recs) == 20
    assert all(r.state is JobState.PEND for r in recs)
    assert len({r.job_id for r in recs}) == 1        # 동일 array id
    assert sorted(r.array_index for r in recs) == list(range(1, 21))
    js = manager.store.get_jobset(jsid)
    assert js.array_job_ids == [recs[0].job_id]


def test_array_submit_with_command_list(qtbot, manager, fake_lsf, tmp_path):
    cmds = [f"hspice tt_{i}.sp" for i in range(5)]
    spec = ArrayJobSpec(commands=tuple(cmds))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_array(spec)
    # dispatch 스크립트 생성 확인
    import os
    sdir = manager.config.resolve_script_dir()
    assert os.path.exists(os.path.join(sdir, f"{jsid}.sh"))
    cmds_file = os.path.join(sdir, f"{jsid}.cmds")
    assert open(cmds_file).read().splitlines() == cmds
    # 레코드에는 element별 command 보존 (retry 재submit용)
    recs = sorted(manager.get_jobs(jsid), key=lambda r: r.array_index)
    assert [r.command for r in recs] == cmds
