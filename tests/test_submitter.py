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


def test_submit_emits_jobset_updated_with_initial_pend(qtbot, manager,
                                                       fake_lsf):
    """submit 완료 시 초기 PEND 상태가 jobset_updated로 즉시 발화된다 —
    폴링(첫 조회)이나 상태 변화 없이도 js.jobset_updated가 PEND를 받아야 한다.
    (submit_bulk는 auto_poll도 안 하므로 이 발화가 없으면 갱신이 영영 안 옴)"""
    updates = []
    manager.jobset_updated.connect(lambda jsid, s: updates.append(s))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command=f"r {i}")
                                    for i in range(5)])
    assert updates, "submit 완료 후 jobset_updated 미발화"
    assert updates[-1]["PEND"] == 5 and updates[-1]["total"] == 5


def test_submit_emits_submitting_immediately(qtbot, manager, fake_lsf):
    """submit 직후(완료 전) 초기 SUBMITTING이 jobs_updated로 즉시 온다 — 대량
    submit이 오래 걸려도 표가 바로 채워진다(resubmit과 통일)."""
    with qtbot.waitSignal(manager.jobs_updated, timeout=10000) as blk:
        manager.submit_bulk([JobSpec(command=f"r {i}") for i in range(3)])
    _, recs = blk.args
    assert len(recs) == 3
    assert all(r.state is JobState.SUBMITTING for r in recs)   # 첫 발행 SUBMITTING


def test_submit_emits_jobs_updated_progressively(qtbot, manager, fake_lsf):
    """submit 진행 중 jobs_updated가 점진 발행되어, 완료를 안 기다리고 각 job이
    SUBMITTING→PEND로 갱신된다. 최종적으로 전 job이 PEND(job_id 확보)."""
    seen = {}    # job_key → 마지막 상태
    manager.jobs_updated.connect(
        lambda jsid, recs: seen.update({r.job_key: r for r in recs}))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit_bulk([JobSpec(command=f"r {i}") for i in range(4)])
    qtbot.wait(50)                       # 마지막 배치 소진
    assert len(seen) == 4
    assert all(r.job_id is not None and r.state is JobState.PEND
               for r in seen.values())


def test_submit_failure_emits_failed_once(qtbot, manager, fake_lsf):
    """제출 실패 시 js.jobs_failed가 정확히 1회만 발화 (완료 emit과 _h_finished의
    이중 발행 제거 확인)."""
    fake_lsf.fail_next_bsub = 99
    js = manager.submit(["x"], max_retry=0, auto_poll=False, mode="bulk")
    failed_batches = []
    js.jobs_failed.connect(failed_batches.append)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    qtbot.wait(50)                       # 후속 큐 신호 소진
    assert len(failed_batches) == 1      # 이중 아님
    assert failed_batches[0][0].state is JobState.SUBMIT_FAILED


def test_submit_updated_relayed_to_handle(qtbot, manager, fake_lsf):
    """핸들 js.jobset_updated로도 초기 PEND 요약이 온다 (사용자 예제 경로)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="x")])
    js = manager.jobset(jsid)
    got = []
    js.jobset_updated.connect(lambda s: got.append(s))
    # 완료 후 재조회 없이도 이미 발화됐으므로, refresh로 한 번 더 확인
    with qtbot.waitSignal(js.jobset_updated, timeout=10000):
        manager.query_once(js)
    assert got and got[-1]["total"] == 1


def test_submit_started_signal(qtbot, manager):
    with qtbot.waitSignal(manager.submit_started, timeout=5000) as blocker:
        jsid = manager.submit_bulk([JobSpec(command="x")])
    assert blocker.args == [jsid]
    qtbot.waitSignal(manager.submit_finished, timeout=5000)


def test_progress_throttle_option_reduces_emits(qtbot, fake_lsf, config):
    """progress throttle 옵션을 성기게 하면 jobs_updated 발화 수가 준다."""
    from dataclasses import replace
    from lsfmgr import InMemoryStore, LsfJobManager

    def count_emits(**cfgkw):
        mgr = LsfJobManager(store=InMemoryStore(),
                            config=replace(config, **cfgkw), runner=fake_lsf)
        c = [0]
        mgr.jobs_updated.connect(lambda j, rs: c.__setitem__(0, c[0] + 1))
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit_bulk([JobSpec(command=f"r {i}") for i in range(300)],
                            workers=32)
        mgr.shutdown()
        return c[0]

    default = count_emits()
    coarse = count_emits(progress_min_interval_s=0.5, progress_min_step_ratio=0.1)
    assert coarse < default                    # 성긴 설정이 덜 발화


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
    cmds = [f"mytool tt_{i}.sp" for i in range(5)]
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
