"""v9 jobset 계약 — create_jobset(commands·merge_id·ud_data로 job까지 생성)/
merge_from(in-place replace)/remove·clear(force 가드)/can_*/submit(전체 재제출).

GUI가 job control을 직접 갖는 구조: 라이브러리는 CRUD+submit+kill+poll만
제공하고, job 생성은 create_jobset 한 곳, 이후 추가는 merge로만, 재실행은
'merge로 교체 후 submit'으로 표현한다 (resubmit·create_job(s) 제거).
"""
from __future__ import annotations

import shlex

import pytest

from lsfmgr import JobSpec, JobState
from lsfmgr.errors import JobNotFoundError, LsfmgrError


def _finish_all(manager, fake_lsf, js):
    """jobset의 살아있는 job을 전부 DONE으로 종료시키고 반영한다."""
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)


# ----------------------------------------------------------------------
# create_jobset — job까지 함께 생성 (유일한 생성 경로)
# ----------------------------------------------------------------------
def test_create_jobset_empty_returns_handle_in_created_state(manager):
    js = manager.create_jobset(label="basket")     # commands 없이 → 빈 바구니
    assert js.id and js.jobs() == []
    assert js.summary["total"] == 0                 # CREATED 상태, job 없음


def test_create_jobset_with_merge_id_and_ud_data(qtbot, manager, fake_lsf):
    batches = []
    manager.jobs_updated.connect(lambda _j, recs: batches.append(recs))

    js = manager.create_jobset(
        ["customwrapper_sub -i a.sp"], merge_ids=["job-a"],
        ud_datas=[{"run": "customwrapper_sub -i a.sp", "n": 1}])

    rec = js.jobs()[0]
    assert rec.state is JobState.CREATED
    assert rec.merge_id == "job-a"
    assert rec.ud_data == {"run": "customwrapper_sub -i a.sp", "n": 1}
    assert rec.via_wrapper is True
    assert batches and batches[0][0].job_key == rec.job_key   # 표 즉시 갱신
    assert js.summary["total"] == 1            # intended 자동 증가


def test_create_jobset_paths(manager):
    """항목 타입별 제출 경로 — JobSpec=bsub / argv=wrapper / str=wrapper 기본."""
    js = manager.create_jobset([
        JobSpec(command="make sim", queue="priority"),   # bsub
        ["customwrapper_sub", "-i", "b sp.sp"],          # argv → wrapper (공백 인자)
        "customwrapper_sub c.sp",                        # str → wrapper 기본
    ])
    r1, r2, r3 = js.jobs()
    assert r1.via_wrapper is False and r1.spec_json
    assert r2.via_wrapper is True
    assert shlex.split(r2.command) == ["customwrapper_sub", "-i", "b sp.sp"]
    assert r3.via_wrapper is True

    # wrapper=False → 문자열도 bsub 경로
    js2 = manager.create_jobset(["echo x"], wrapper=False)
    assert js2.jobs()[0].via_wrapper is False


def test_create_jobset_duplicate_merge_id_rejected(manager):
    with pytest.raises(ValueError, match="merge_id 중복"):
        manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
                              merge_ids=["m1", "m1"])
    # None은 중복 아님
    js = manager.create_jobset(["customwrapper_sub c.sp", "customwrapper_sub d.sp"],
                              merge_ids=[None, None])
    assert js.summary["total"] == 2


def test_create_jobset_length_mismatch_rejected(manager):
    with pytest.raises(ValueError, match="길이"):
        manager.create_jobset(["a", "b"], merge_ids=["m1"])
    with pytest.raises(ValueError, match="길이"):
        manager.create_jobset(["a", "b"], ud_datas=[{"x": 1}])


def test_set_ud_data_by_refs(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["m1"])

    manager.set_ud_data(js, js.jobs()[0].job_key, {"v": 1})   # job_key로
    assert js.jobs()[0].ud_data == {"v": 1}
    manager.set_ud_data(js, "m1", {"v": 2})              # merge_id로
    assert js.jobs()[0].ud_data == {"v": 2}

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    jid = js.jobs()[0].job_id
    manager.set_ud_data(js, jid, {"v": 3})               # job_id(int)로
    assert js.jobs()[0].ud_data == {"v": 3}


# ----------------------------------------------------------------------
# merge_from — merge_id 규칙 (생성 후 job 추가는 merge로만)
# ----------------------------------------------------------------------
def test_merge_from_replace_keeps_physical_key(qtbot, manager, fake_lsf):
    """같은 merge_id → replace: 물리 키(job_key)는 target 것 유지(테이블 행
    연속), 내용(command/ud_data)은 source 것으로 교체."""
    a = manager.create_jobset(
        ["customwrapper_sub v1.sp", "customwrapper_sub keep.sp"],
        merge_ids=["m1", "keep"], ud_datas=[{"ver": 1}, None], label="target")
    old = next(r for r in a.jobs() if r.merge_id == "m1")
    b = manager.create_jobset(
        ["customwrapper_sub v2.sp"], merge_ids=["m1"],
        ud_datas=[{"ver": 2}], label="source")

    changed = manager.merge(a, b)

    by_mid = {r.merge_id: r for r in a.jobs()}
    rep = by_mid["m1"]
    assert rep.job_key == old.job_key           # 물리 키 유지
    assert rep.command == "customwrapper_sub v2.sp"   # 내용은 source
    assert rep.ud_data == {"ver": 2}
    assert by_mid["keep"].command == "customwrapper_sub keep.sp"
    assert a.summary["total"] == 2
    assert any(r.merge_id == "m1" for r in changed)


def test_merge_from_adds_new_and_none_merge_ids(manager):
    """merge_id가 target에 없거나 None이면 신규 추가."""
    a = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["m1"])
    b = manager.create_jobset(
        ["customwrapper_sub new.sp", "customwrapper_sub anon.sp"],
        merge_ids=["m2", None])              # m2=미존재 추가 / None=추가

    manager.merge(a, b)

    assert a.summary["total"] == 3
    assert {r.merge_id for r in a.jobs()} == {"m1", "m2", None}


def test_merge_from_destroys_source(qtbot, manager, fake_lsf):
    from lsfmgr import JobSetClosedError

    a = manager.create_jobset()
    b = manager.create_jobset(["customwrapper_sub x.sp"])
    manager.merge(a, b)
    with pytest.raises(JobSetClosedError):
        b.jobs()                                # source 핸들 파괴
    with pytest.raises(LsfmgrError):
        manager.summary(b.id)                   # jobset 자체 삭제


def test_merge_from_guard_and_force(qtbot, manager, fake_lsf):
    """활성(RUN/PEND) job이 있으면 거부, force면 레코드만 교체 진행."""
    a = manager.create_jobset(["customwrapper_sub run.sp"], merge_ids=["m1"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)               # m1이 PEND(활성)로

    b = manager.create_jobset(["customwrapper_sub v2.sp"], merge_ids=["m1"])

    assert manager.can_merge(a, b) is False
    with pytest.raises(LsfmgrError, match="활성"):
        manager.merge(a, b)

    live_id = a.jobs()[0].job_id
    manager.merge(a, b, force=True)                 # 레코드만 강제 교체
    rec = a.jobs()[0]
    assert rec.state is JobState.CREATED        # source 상태로 교체됨
    assert rec.command == "customwrapper_sub v2.sp"
    # LSF의 실제 job은 그대로 산다 — 정리는 caller(GUI) 책임
    assert any(j.job_id == live_id for j in fake_lsf.alive_jobs())


def test_can_merge_true_when_all_inactive(qtbot, manager, fake_lsf):
    a = manager.create_jobset(["customwrapper_sub a.sp"])
    b = manager.create_jobset(["customwrapper_sub b.sp"])
    assert manager.can_merge(a, b) is True               # 전원 CREATED

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)
    assert manager.can_merge(a, b) is False              # PEND 활성
    _finish_all(manager, fake_lsf, a)
    assert manager.can_merge(a, b) is True               # DONE(종료) — 다시 가능


# ----------------------------------------------------------------------
# remove_job / clear
# ----------------------------------------------------------------------
def test_remove_job_by_merge_id_and_job_id(qtbot, manager, fake_lsf):
    js = manager.create_jobset(
        ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
        merge_ids=["m1", "m2"])

    removed = manager.remove_job(js, merge_id="m1")      # CREATED — 비활성이라 즉시
    assert [r.merge_id for r in removed] == ["m1"]
    assert js.summary["total"] == 1

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish_all(manager, fake_lsf, js)          # DONE(종료 상태)
    jid = js.jobs()[0].job_id
    manager.remove_job(js, job_id=jid)                   # job_id 기준, 종료라 허용
    assert js.jobs() == [] and js.summary["total"] == 0


def test_remove_job_active_requires_force(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["m1"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # PEND(활성)

    with pytest.raises(LsfmgrError, match="활성"):
        manager.remove_job(js, merge_id="m1")
    manager.remove_job(js, merge_id="m1", force=True)    # 레코드만 강제 삭제
    assert js.jobs() == []

    with pytest.raises(JobNotFoundError):
        manager.remove_job(js, merge_id="없는것")


def test_clear_guard_and_force(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp", "customwrapper_sub b.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    with pytest.raises(LsfmgrError, match="활성"):
        manager.clear(js)
    _finish_all(manager, fake_lsf, js)
    manager.clear(js)                                  # 전원 종료 — 허용
    assert js.jobs() == [] and js.summary["total"] == 0


# ----------------------------------------------------------------------
# submit — 전 job (재)제출 + can_submit
# ----------------------------------------------------------------------
def test_submit_resubmits_all_inactive(qtbot, manager, fake_lsf):
    """DONE/EXIT 포함 전 job이 리셋 후 재제출된다 — 같은 job_key 유지."""
    js = manager.create_jobset(
        ["customwrapper_sub a.sp", JobSpec(command="make sim", queue="priority")],
        merge_ids=["m1", None], ud_datas=[{"keep": True}, None])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish_all(manager, fake_lsf, js)
    keys = {r.job_key for r in js.jobs()}
    old_ids = {r.job_id for r in js.jobs()}

    assert manager.can_submit(js) is True              # 전원 종료 → 재제출 가능
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # 전체 재실행

    assert {r.job_key for r in js.jobs()} == keys      # 물리 키 유지
    assert all(r.state is JobState.PEND for r in js.jobs())
    assert {r.job_id for r in js.jobs()}.isdisjoint(old_ids)  # 새 실행
    by_mid = {r.merge_id: r for r in js.jobs()}
    assert by_mid["m1"].ud_data == {"keep": True}      # ud_data 보존
    # bsub 경로 옵션(queue) 보존
    spec_rec = next(r for r in js.jobs() if not r.via_wrapper)
    assert fake_lsf.jobs[str(spec_rec.job_id)].queue == "priority"


def test_submit_rejected_while_active(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # PEND(활성)

    assert manager.can_submit(js) is False
    with pytest.raises(LsfmgrError, match="활성"):
        manager.submit(js)

    _finish_all(manager, fake_lsf, js)
    assert manager.can_submit(js) is True


def test_submit_empty_jobset_rejected(manager):
    js = manager.create_jobset()
    assert manager.can_submit(js) is False
    with pytest.raises(LsfmgrError, match="job이 없습니다"):
        manager.submit(js)


def test_submit_resets_previous_run_traces(qtbot, manager, fake_lsf):
    """재제출 리셋이 이전 실행 흔적(exit_code/run_time/fail_message)을
    지운다 (구 resubmit의 리셋 계약 이식)."""
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    jid = js.jobs()[0].job_id
    fake_lsf.set_job(jid, "EXIT", 9)
    fake_lsf.jobs[str(jid)].run_time_s = 55
    manager.querier.query(js.id)
    assert js.jobs()[0].exit_code == 9

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)

    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.exit_code is None and rec.run_time_s is None
    assert rec.fail_message is None


def test_rerun_pattern_merge_then_submit(qtbot, manager, fake_lsf):
    """v9 재실행 패턴: 실패 job을 같은 merge_id로 교체(merge_from) 후
    전체 submit — resubmit 없이 재실행이 표현된다."""
    js = manager.create_jobset(
        ["customwrapper_sub bad.sp", "customwrapper_sub ok.sp"],
        merge_ids=["m1", "m2"], label="run")
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    # m1은 EXIT(실패), m2는 DONE으로 종료
    recs = {r.merge_id: r for r in js.jobs()}
    fake_lsf.set_job(recs["m1"].job_id, "EXIT", 1)
    fake_lsf.set_job(recs["m2"].job_id, "DONE", 0)
    manager.querier.query(js.id)

    fix = manager.create_jobset(              # 수정본 바구니
        ["customwrapper_sub fixed.sp"], merge_ids=["m1"], label="fix")
    assert manager.can_merge(js, fix) is True
    manager.merge(js, fix)                          # m1만 교체 (m2 결과 유지)

    recs = {r.merge_id: r for r in js.jobs()}
    assert recs["m1"].state is JobState.CREATED
    assert recs["m2"].state is JobState.DONE
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # 전체 재실행 (요구: 전 job)
    assert all(r.state is JobState.PEND for r in js.jobs())


def test_merge_from_transfers_polling(qtbot, manager, fake_lsf):
    """source가 폴링 중이었으면 target이 이어받는다 (연속성)."""
    a = manager.create_jobset(["customwrapper_sub a.sp"])
    b = manager.create_jobset(["customwrapper_sub b.sp"])
    manager.start_polling(b.id, 1.0)            # source만 폴링 사용

    manager.merge(a, b)

    assert manager._poll_intervals.get(a.id) == 1.0
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000,
                          check_params_cb=lambda j, _s: j == a.id):
        pass                                    # target 폴링이 실제로 돈다


# ----------------------------------------------------------------------
# pre_submit 게이트 — mgr.submit(js, pre_submit=fn) (A안, FR-9)
# ----------------------------------------------------------------------
def test_submit_jobset_pre_submit_gate_pass(qtbot, manager, fake_lsf):
    """게이트 통과 시 정상 제출 — 검사 대상은 커맨드 리스트 전체."""
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    seen = []

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=lambda cmds: seen.append(cmds) or True,
                       auto_poll=False)

    assert seen and "customwrapper_sub a.sp" in seen[0][0]
    assert js.jobs()[0].state is JobState.PEND


def test_submit_jobset_gate_reject_keeps_records(qtbot, manager, fake_lsf):
    """게이트 False → 레코드 원상 유지(리셋 없음) + 제출 없음.
    게이트는 리셋 **이전**에 돈다 — DONE 결과가 지워지지 않는다."""
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish_all(manager, fake_lsf, js)
    old = js.jobs()[0]                          # DONE + job_id 보유
    n_lsf = len(fake_lsf.jobs)

    with qtbot.waitSignal(manager.ready_finished, timeout=10000) as blk:
        manager.submit(js, pre_submit=lambda cmds: False, auto_poll=False)
    assert blk.args == [js.id, False]

    rec = js.jobs()[0]
    assert rec.state is JobState.DONE           # 원상 유지 (리셋 안 됨)
    assert rec.job_id == old.job_id
    assert len(fake_lsf.jobs) == n_lsf          # 제출 없음


def test_submit_jobset_gate_exception_keeps_records(qtbot, manager, fake_lsf):
    """게이트 예외 → 레코드 원상 + error_occurred + finished(failed)."""
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    errors = []
    manager.error_occurred.connect(lambda _j, m: errors.append(m))

    def boom(_cmds):
        raise RuntimeError("gate blew up")

    with qtbot.waitSignal(manager.submit_finished, timeout=10000) as blk:
        manager.submit(js, pre_submit=boom, auto_poll=False)

    assert js.jobs()[0].state is JobState.CREATED   # 원상 유지
    assert errors and "gate blew up" in errors[0]
    _jsid, report = blk.args
    assert report.failed == 1 and report.succeeded == 0
