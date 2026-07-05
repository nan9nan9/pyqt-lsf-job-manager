"""JobSet 관리 테스트 — 손실 감지 / merge / close / add_job (FR-5)."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState
from lsfmgr.errors import LsfmgrError
from lsfmgr.states import JobRecord


@pytest.fixture
def submitted(qtbot, manager, fake_lsf):
    jobs = [JobSpec(command=f"r {i}") for i in range(10)]
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(jobs)
    return jsid


# ----------------------------------------------------------------------
# 손실 감지 (FR-5.3)
# ----------------------------------------------------------------------
def test_detect_lost_recovers_by_name(qtbot, manager, fake_lsf, submitted):
    # ID를 잃어버린 상황 재현: 레코드의 job_id를 지우고 SUBMITTING으로 되돌림
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    lost = manager.detect_lost(submitted)
    assert lost == []                          # name 패턴으로 ID 복구 성공
    recovered = manager.store.get_job(submitted, rec.job_key)
    assert recovered.job_id == rec.job_id
    assert recovered.state is JobState.PEND


def test_detect_lost_marks_lost(qtbot, manager, fake_lsf, submitted):
    rec = manager.get_jobs(submitted)[0]
    manager.store.transition(submitted, rec.job_key, JobState.SUBMITTING,
                             job_id=None)
    fake_lsf.vanish_job(rec.job_id)            # LSF에서도 소멸
    lost = manager.detect_lost(submitted)
    assert len(lost) == 1
    assert lost[0].state is JobState.LOST


# ----------------------------------------------------------------------
# merge (FR-5.5)
# ----------------------------------------------------------------------
def test_merge_jobsets(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command=f"a {i}") for i in range(5)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command=f"b {i}") for i in range(7)])

    merged = manager.merge_jobsets([a, b], keep_originals=False)
    js = manager.store.get_jobset(merged)
    assert js.intended_count == 12
    assert js.merged_from == [a, b]
    assert len(js.lsf_group_paths) == 2        # 부착물 누적
    assert len(js.name_patterns) == 2
    assert len(manager.get_jobs(merged)) == 12
    # 원본 삭제 확인
    from lsfmgr.errors import JobSetNotFoundError
    with pytest.raises(JobSetNotFoundError):
        manager.store.get_jobset(a)
    # merge된 jobset 요약 불변식
    s = manager.summary(merged)
    assert sum(v for k, v in s.items() if k != "total") == 12


def test_merge_keep_originals(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command="a")])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command="b")])
    merged = manager.merge_jobsets([a, b], keep_originals=True)
    assert manager.store.get_jobset(a) is not None
    assert len(manager.get_jobs(merged)) == 2


# ----------------------------------------------------------------------
# merge된 jobset kill — 부착물 전부 순회 (§1.1)
# ----------------------------------------------------------------------
def test_merged_jobset_kill_iterates_attachments(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command=f"a {i}") for i in range(5)])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command=f"b {i}") for i in range(5)])
    merged = manager.merge_jobsets([a, b])
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill_jobset(merged)
    _, report = blocker.args
    assert report.command_calls == 2           # group 2개 순회
    assert fake_lsf.alive_jobs() == []


# ----------------------------------------------------------------------
# close (FR-5.7)
# ----------------------------------------------------------------------
def test_close_requires_all_terminal(qtbot, manager, fake_lsf, submitted):
    with pytest.raises(LsfmgrError):
        manager.close_jobset(submitted)        # 전원 PEND — 불가


def test_close_after_terminal(qtbot, manager, fake_lsf, submitted):
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(submitted)
    manager.close_jobset(submitted)
    assert manager.store.get_jobset(submitted).closed is True
    # bgdel은 worker 스레드에서 비동기 수행 (main 스레드 LSF 호출 금지)
    qtbot.waitUntil(lambda: len(fake_lsf.calls_of("bgdel")) == 1,
                    timeout=10000)


# ----------------------------------------------------------------------
# add_job (FR-5.4)
# ----------------------------------------------------------------------
def test_add_job_with_lsf_sync(qtbot, manager, fake_lsf, submitted):
    # 외부에서 submit된 job을 편입
    cmd = manager.command
    ext_id = cmd.bsub("external job", job_name="ext_1")
    rec = JobRecord(job_id=ext_id, array_index=None, jobset_id=submitted,
                    lsf_job_name="ext_1", state=JobState.PEND,
                    command="external job")
    manager.add_job(submitted, rec, sync_lsf=True)

    js = manager.store.get_jobset(submitted)
    assert js.intended_count == 11             # 불변식 유지 위해 증가
    assert len(manager.get_jobs(submitted)) == 11
    # bmod -g 호출됨
    assert any("-g" in c for c in fake_lsf.calls_of("bmod"))
    assert fake_lsf.jobs[str(ext_id)].group == js.lsf_group_paths[0]


def test_remove_job_decrements_intended_count(qtbot, manager, fake_lsf, submitted):
    # 10건 중 1건 제거 → intended_count 감소, 유령 CREATED 없이 합계 유지
    victim = manager.get_jobs(submitted)[0]
    before = manager.summary(submitted)
    assert before["total"] == 10

    rec = manager.remove_job(submitted, victim.job_key)
    assert rec.job_key == victim.job_key       # 제거된 레코드 반환

    s = manager.summary(submitted)
    assert s["total"] == 9                      # add_job의 역연산 — intended 감소
    assert len(manager.get_jobs(submitted)) == 9
    assert sum(v for k, v in s.items() if k != "total") == 9  # 유령 CREATED 없음

    # 제거 후 재추가 → 왕복 일관성 (다시 10)
    manager.add_job(submitted, victim, sync_lsf=False)
    s2 = manager.summary(submitted)
    assert s2["total"] == 10
    assert sum(v for k, v in s2.items() if k != "total") == 10


# ----------------------------------------------------------------------
# resubmit_jobs — 상태 기반 재실행 (kill 후 재제출, 레코드 재사용)
# ----------------------------------------------------------------------
def test_resubmit_jobs_reuses_records_and_kills_live(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(
            [JobSpec(command=f"run {i}") for i in range(4)])
    before = {r.job_key: r for r in manager.get_jobs(jsid)}
    assert all(r.state is JobState.PEND and r.job_id is not None
               for r in before.values())
    live_keys = [f"{jsid}_0", f"{jsid}_1"]
    old_ids = {k: before[k].job_id for k in live_keys}

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, live_keys)

    after = {r.job_key: r for r in manager.get_jobs(jsid)}
    # 1) 레코드 재사용 — 같은 key, 새 job_id, 다시 PEND
    for k in live_keys:
        assert after[k].state is JobState.PEND
        assert after[k].job_id != old_ids[k]        # 새로 제출됨
    # 2) 기존 LSF job은 kill됨 (fake bkill → EXIT)
    for oid in old_ids.values():
        assert fake_lsf.jobs[str(oid)].stat == "EXIT"
    # 3) 대상 아닌 job은 그대로
    assert after[f"{jsid}_2"].job_id == before[f"{jsid}_2"].job_id
    # 4) 목록 크기·intended_count·불변식 유지 (삭제/재생성 아님)
    s = manager.summary(jsid)
    assert s["total"] == 4 and len(manager.get_jobs(jsid)) == 4
    assert sum(v for k, v in s.items() if k != "total") == 4


def test_resubmit_jobs_terminal_no_kill_and_command_override(
        qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="orig")])
    key = f"{jsid}_0"
    # 종료(EXIT) 상태 — 살아있지 않으므로 kill 대상 아님
    manager.store.transition(jsid, key, JobState.EXIT, exit_code=1)
    n_bkill = len(fake_lsf.calls_of("bkill"))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, [key], commands={key: "newcmd"})

    after = manager.get_jobs(jsid)[0]
    assert after.state is JobState.PEND
    assert after.command == "newcmd"                # 새 커맨드 반영
    assert after.exit_code is None                  # 이전 exit_code 초기화
    assert len(fake_lsf.calls_of("bkill")) == n_bkill  # terminal → kill 없음


def test_resubmit_jobs_wrapper_path(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper(["primesim_sub -q normal a.sp"],
                                    auto_poll=False)
    jsid = js.id
    key = f"{jsid}_0"
    old_id = manager.get_jobs(jsid)[0].job_id

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([key])

    after = manager.get_jobs(jsid)[0]
    assert after.state is JobState.PEND
    assert after.job_id != old_id                   # wrapper 재실행 → 새 job
    # 부착물 없는 jobset이므로 wrapper(primesim_sub)로 재제출됨
    assert len(fake_lsf.calls_of("primesim_sub")) == 2


def test_resubmit_jobs_missing_key_raises(qtbot, manager):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="x")])
    with pytest.raises(Exception):
        manager.resubmit_jobs(jsid, [f"{jsid}_9"])   # 없는 key


def test_resubmit_wrapper_argv_roundtrip_preserves_quoting(qtbot, manager,
                                                           fake_lsf):
    """공백 포함 인자를 가진 wrapper job도 재제출 시 원본 argv가 복원된다
    (shlex.join 저장 ↔ shlex.split 복원)."""
    argv = ["primesim_sub", "-q", "normal", "my file.sp"]   # 공백 포함 인자
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit_wrapper([argv], auto_poll=False)
    jsid = js.id
    rec = manager.get_jobs(jsid)[0]
    assert rec.via_wrapper is True                    # 제출 경로 job 단위 기록

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js.resubmit_jobs([rec.job_key])

    # 재제출된 wrapper 호출의 argv가 원본과 동일해야 한다 (분해 손상 없음)
    calls = fake_lsf.calls_of("primesim_sub")
    assert len(calls) == 2
    assert calls[1][1:] == ["-q", "normal", "my file.sp"], calls[1]


def test_resubmit_mixed_merge_dispatches_per_job(qtbot, manager, fake_lsf):
    """wrapper jobset + bsub jobset을 merge한 혼합 jobset에서 각 job이
    자기 제출 경로(via_wrapper)로 재제출된다 — jobset 부착물 오판 방지."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        w = manager.submit_wrapper(["primesim_sub -q normal a.sp"],
                                   auto_poll=False)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        b = manager.submit_bulk([JobSpec(command="make sim")])
    merged = manager.merge_jobsets([w.id, b], keep_originals=False)
    recs = {r.job_key: r for r in manager.get_jobs(merged)}
    wkey = next(k for k, r in recs.items() if r.via_wrapper)
    bkey = next(k for k, r in recs.items() if not r.via_wrapper)
    n_wrap = len(fake_lsf.calls_of("primesim_sub"))
    n_bsub = len(fake_lsf.calls_of("bsub"))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(merged, [wkey, bkey])

    # wrapper job은 wrapper로, bsub job은 bsub로 — 교차 오판 없음
    assert len(fake_lsf.calls_of("primesim_sub")) == n_wrap + 1
    assert len(fake_lsf.calls_of("bsub")) == n_bsub + 1


def test_resubmit_reset_clears_previous_run_traces(qtbot, manager, fake_lsf):
    """리셋이 이전 실행의 시간/위치 흔적까지 지운다 — 재제출 실패 시
    새 커맨드에 이전 실행의 start/finish/working_dir가 붙어 잔존하지 않게."""
    from datetime import datetime
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="orig")])
    key = f"{jsid}_0"
    manager.store.transition(
        jsid, key, JobState.DONE, exit_code=0, run_time_s=10,
        start_time=datetime(2026, 7, 1), finish_time=datetime(2026, 7, 2),
        working_dir="/old/dir")

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, [key])

    rec = manager.get_jobs(jsid)[0]
    # 재제출 성공 → PEND, 이전 실행 흔적은 리셋됨 (폴링 전이므로 None 유지)
    assert rec.state is JobState.PEND
    assert rec.run_time_s is None and rec.start_time is None
    assert rec.finish_time is None and rec.working_dir is None


def test_resubmit_preserves_submit_options(qtbot, manager, fake_lsf):
    """재제출이 원 제출 옵션(queue/resources/outfile)을 복원한다 —
    command만 재구성하면 옵션이 기본값으로 조용히 소실된다 (spec_json)."""
    spec = JobSpec(command="sim run", queue="night",
                   resources="rusage[mem=32G]", outfile="/tmp/out.log")
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([spec])
    key = f"{jsid}_0"

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, [key])

    resub = fake_lsf.calls_of("bsub")[-1]     # 재제출 bsub 호출
    argv = " ".join(str(a) for a in resub)
    assert "-q night" in argv                 # queue 보존
    assert "rusage[mem=32G]" in argv          # resources 보존
    assert "/tmp/out.log" in argv             # outfile 보존


def test_resubmit_jobs_dedupes_keys(qtbot, manager, fake_lsf):
    """같은 key를 중복으로 넘겨도 1회만 재제출 — 미추적 좀비 job 방지."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="x")])
    key = f"{jsid}_0"
    n = len(fake_lsf.calls_of("bsub"))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.resubmit_jobs(jsid, [key, key, key])   # 중복 3회
    assert blk.args[1].total == 1                       # 1건으로 처리
    assert len(fake_lsf.calls_of("bsub")) == n + 1      # bsub 1회만


def test_explicit_stop_polling_not_revived_by_resubmit(qtbot, manager,
                                                       fake_lsf):
    """사용자가 명시적으로 끈 polling은 resubmit이 되살리지 않는다
    (재개는 AUTO-2 자동중지 복구 용도만)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="x")])
    manager.start_polling(jsid, interval_s=60)
    manager.stop_polling(jsid)                # 명시적 중지 — 기억도 삭제
    assert jsid not in manager._poll_intervals

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.resubmit_jobs(jsid, [f"{jsid}_0"])
    qtbot.wait(100)
    assert jsid not in manager._poll_intervals   # 재개 안 됨


def test_cancel_during_kill_phase_cancels_resubmit(qtbot, manager, fake_lsf):
    """kill-phase 대기 중 cancel_submit → plan 취소, 재제출 없음,
    submit_finished(전원 cancelled)로 submit_started와 짝이 맞는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk([JobSpec(command="x")])
    n_bsub = len(fake_lsf.calls_of("bsub"))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.resubmit_jobs(jsid, [f"{jsid}_0"], verify=True)
        manager.cancel_submit(jsid)          # kill-phase 사이에 취소
    rpt = blk.args[1]
    assert rpt.cancelled == 1 and rpt.succeeded == 0
    qtbot.wait(200)                          # kill task 완료 대기
    assert len(fake_lsf.calls_of("bsub")) == n_bsub   # 재제출 안 됨


def test_resubmit_jobs_rejects_concurrent_call(qtbot, manager, fake_lsf):
    """kill-phase 진행 중 같은 jobset에 재호출하면 거부 — plan 덮어쓰기 방지.
    (kill-phase 동안엔 submitter ctx가 없어 is_active만으로 못 막는다)"""
    from lsfmgr.errors import LsfmgrError
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = manager.submit_bulk(
            [JobSpec(command=f"r {i}") for i in range(2)])
    keys = [f"{jsid}_0", f"{jsid}_1"]
    # 1번째 호출 — 살아있는 job이 있어 kill-phase(worker)로 넘어간다
    manager.resubmit_jobs(jsid, [keys[0]], verify=True)
    # kill-phase가 끝나기 전 재호출 → 거부되어야 한다
    with pytest.raises(LsfmgrError):
        manager.resubmit_jobs(jsid, [keys[1]])
    # 1번째 resubmit은 정상 완료
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        pass
    assert manager.get_jobs(jsid)[0].state is JobState.PEND


# ----------------------------------------------------------------------
# 메타데이터/검색 (FR-5.6)
# ----------------------------------------------------------------------
def test_search_by_tag(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = manager.submit_bulk([JobSpec(command="x")],
                                label="tt_sweep", tags=["sweep", "tt"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit_bulk([JobSpec(command="y")], tags=["other"])
    hits = manager.search_jobsets(tag="sweep")
    assert [j.jobset_id for j in hits] == [a]
    assert manager.search_jobsets(label="tt_sweep")[0].jobset_id == a
