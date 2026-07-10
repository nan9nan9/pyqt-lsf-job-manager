"""정독 리뷰 4차 — 시나리오/엣지케이스 리뷰에서 발견된 버그의 회귀 테스트.

1. merge된 wrapper+bsub jobset kill: group 전략 성공(covered)만 믿고
   fallback을 생략해 부착물이 없는 wrapper job이 영원히 살아남던 버그
   (+ optimistic 정책이 그 생존 job을 EXIT로 오표시하던 파생 버그).
2. manager.kill_jobs(js, array element key): parent job_id로 변환하며 array_index가
   소실돼 전체 array가 죽던 버그.
3. array submit 최종 실패: SUBMIT_FAILED 전이가 jobs_updated/jobs_failed로
   발행되지 않아 UI 표가 SUBMITTING에 고착되던 버그.
4. 연도 없는 LSF 시각 포맷: 기본연도 1900(비윤년) 때문에 윤년 2/29 시각이
   파싱 실패로 소실되던 버그.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from lsfmgr.command import _parse_lsf_time
from tests.conftest import submit_cmds
from lsfmgr.states import JobState


# ----------------------------------------------------------------------
# 1. merge된 wrapper+bsub jobset kill
# ----------------------------------------------------------------------
def test_kill_merged_wrapper_and_bsub_jobset(qtbot, manager, fake_lsf):
    """group 전략이 bsub job을 커버해도, 부착물이 없는 wrapper job은
    chunk fallback으로 반드시 죽여야 한다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js_w = submit_cmds(manager, 
            ["customwrapper_sub -i a.sp", "customwrapper_sub -i b.sp"], auto_poll=False, wrapper=True)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js_b = submit_cmds(manager, ["echo x", "echo y"],
                              auto_poll=False)
    merged = js_b
    manager.merge(js_b, js_w, force=True)   # 활성 — force 레코드 흡수
    assert len(fake_lsf.alive_jobs()) == 4

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(merged)

    alive = fake_lsf.alive_jobs()
    assert alive == [], (
        f"kill 후 생존 job: {[(j.job_id, j.name) for j in alive]}")


def test_kill_merged_optimistic_no_false_exit(qtbot, manager, fake_lsf):
    """optimistic 정책이 LSF에 살아있는 job을 EXIT로 오표시하면 안 된다 —
    kill이 wrapper job까지 실제로 커버해야 store가 거짓말하지 않는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js_w = submit_cmds(manager, ["customwrapper_sub -i a.sp"], wrapper=True,
                                      auto_poll=False)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js_b = submit_cmds(manager, ["echo x"], auto_poll=False)
    merged = js_b
    manager.merge(js_b, js_w, force=True)            # 활성 — force 레코드 흡수

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(merged)

    still_alive_ids = {j.job_id for j in fake_lsf.alive_jobs()}
    lying = [r for r in merged.jobs(states={JobState.EXIT})
             if r.job_id in still_alive_ids]
    assert not lying, (
        f"LSF에 살아있는데 store가 EXIT로 표시: "
        f"{[(r.job_key, r.job_id) for r in lying]}")


def test_kill_pure_bsub_jobset_still_skips_chunk(qtbot, manager, fake_lsf):
    """수정 후에도 부착물이 커버하는 순수 bsub jobset은 chunk fallback을
    생략해야 한다 (LSF 부하 최소화 설계 유지)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo x", "echo y"],
                            auto_poll=False)
    fake_lsf.calls.clear()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(js)
    assert fake_lsf.alive_jobs() == []
    assert "chunk" not in blocker.args[1].strategies


# ----------------------------------------------------------------------
# 2. array element 부분 kill
# ----------------------------------------------------------------------
def test_kill_jobs_single_array_element(qtbot, manager, fake_lsf):
    """manager.kill_jobs(js, ["jsid[2]"])는 element 2만 죽여야 한다 — parent id로
    변환되면 나머지 element까지 전부 죽는다."""
    from tests.fake_lsf import FakeJob
    from lsfmgr import JobRecord

    js = manager.create_jobset(intended_count=3)
    jsid, parent = js.id, 9400
    manager.store.add_jobs([JobRecord(
        job_id=parent, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]", state=JobState.RUN, command="r")
        for i in (1, 2, 3)])
    for i in (1, 2, 3):
        fake_lsf.jobs[f"{parent}[{i}]"] = FakeJob(
            job_id=parent, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat="RUN")

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill_jobs(js, [f"{js.id}[2]"])

    alive_idx = sorted(j.array_index for j in fake_lsf.alive_jobs())
    assert alive_idx == [1, 3]
    # optimistic 전이도 대상 element에만 적용
    exited = js.jobs(states={JobState.EXIT})
    assert [r.job_key for r in exited] == [f"{js.id}[2]"]


# ----------------------------------------------------------------------
# 3. array submit 최종 실패 시 레코드 발행
# ----------------------------------------------------------------------
def test_array_submit_failure_emits_failed_records(qtbot, manager, fake_lsf):
    """array bsub이 재시도 끝에 최종 실패하면 SUBMIT_FAILED 레코드가
    jobs_updated로 발행돼야 한다 — 누락 시 표가 SUBMITTING에 고착된다."""
    fake_lsf.fail_next_bsub = 10
    updates = []
    manager.jobs_updated.connect(lambda jsid, recs: updates.append(recs))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, "run_task", count=3, auto_poll=False,
                            max_retry=1)

    assert all(r.state is JobState.SUBMIT_FAILED for r in js.jobs())
    qtbot.wait(100)
    failed_emitted = [r for batch in updates for r in batch
                      if r.state is JobState.SUBMIT_FAILED]
    assert len(failed_emitted) == 3


# ----------------------------------------------------------------------
# 4. 윤년 2/29 시각 파싱
# ----------------------------------------------------------------------
def test_parse_lsf_time_feb29(monkeypatch):
    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2028, 3, 1, 10, 0, 0)   # 윤년 직후

    monkeypatch.setattr("lsfmgr.command.datetime", _FakeDT)
    dt = _parse_lsf_time("Feb 29 12:00:00")
    assert dt is not None
    assert (dt.year, dt.month, dt.day) == (2028, 2, 29)


def test_parse_lsf_time_year_boundary(monkeypatch):
    """연말 경계 보정(12월 시작 job을 1월에 조회)이 수정 후에도 유지되는지."""
    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2027, 1, 2, 10, 0, 0)

    monkeypatch.setattr("lsfmgr.command.datetime", _FakeDT)
    dt = _parse_lsf_time("Dec 30 23:00:00")
    assert dt is not None
    assert (dt.year, dt.month, dt.day) == (2026, 12, 30)
