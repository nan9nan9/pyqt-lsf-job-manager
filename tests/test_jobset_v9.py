"""jobset 계약 — create_jobset(commands·merge_id·user_data로 job까지 생성)/
편집 3형제(add_jobs·replace_jobs·upsert_jobs)/remove·clear(force 가드)/
can_submit/submit(전체 재제출).

GUI가 job control을 직접 갖는 구조: 라이브러리는 CRUD+submit+kill+poll만
제공하고, 재실행은 별도 파이프라인이 아니라 "replace_jobs로 교체 후 submit"
이라는 데이터 조작으로 표현한다 (resubmit 제거).
job 추가는 add_jobs로 직접 한다 — 임시 jobset을 만들어 흡수하던 merge는
삭제됐다(그 간접성이 폴링·handler 규칙을 애매하게 만든 원인이었다).
"""
from __future__ import annotations

import shlex

import pytest

from tests.conftest import mk_jobset

from lsfmgr import JobEditNotAllowedError, JobState
from lsfmgr.errors import JobNotFoundError, LsfmgrError


def _finish_all(manager, fake_lsf, js):
    """jobset의 살아있는 job을 전부 DONE으로 종료시키고 반영한다."""
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)


# ----------------------------------------------------------------------
# create_jobset — job까지 함께 생성 (유일한 생성 경로)
# ----------------------------------------------------------------------
def test_create_jobset_empty_returns_handle_in_created_state(manager):
    js = mk_jobset(manager, label="basket")     # commands 없이 → 빈 바구니
    assert js.id and js.jobs() == []
    assert js.summary["total"] == 0                 # CREATED 상태, job 없음


def test_create_jobset_with_merge_id_and_user_data(qtbot, manager, fake_lsf):
    batches = []
    manager.jobs_updated.connect(lambda _j, recs: batches.append(recs))

    js = mk_jobset(manager, 
        ["customwrapper_sub -i a.sp"], job_keys=["job-a"],
        user_datas=[{"run": "customwrapper_sub -i a.sp", "n": 1}])

    rec = js.jobs()[0]
    assert rec.state is JobState.CREATED
    assert rec.job_key == "job-a"
    assert rec.user_data == {"run": "customwrapper_sub -i a.sp", "n": 1}
    assert batches and batches[0][0].job_key == rec.job_key   # 표 즉시 갱신
    assert js.summary["total"] == 1            # intended 자동 증가


def test_create_jobset_paths(manager):
    """항목 타입 — argv/str 모두 wrapper 단일 경로 (v10: bsub 경로 삭제)."""
    js = mk_jobset(manager, [
        ["customwrapper_sub", "-i", "b sp.sp"],          # argv (공백 인자 보존)
        "customwrapper_sub c.sp",                        # str → shlex 분해
    ])
    r2, r3 = js.jobs()
    assert shlex.split(r2.command) == ["customwrapper_sub", "-i", "b sp.sp"]


def test_create_jobset_duplicate_job_key_rejected(manager):
    with pytest.raises(ValueError, match="job_key 중복"):
        mk_jobset(manager, ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
                              job_keys=["m1", "m1"])
    js = mk_jobset(manager, ["customwrapper_sub c.sp", "customwrapper_sub d.sp"],
                   job_keys=["m1", "m2"])
    assert js.summary["total"] == 2


def test_create_jobset_length_mismatch_rejected(manager):
    with pytest.raises(ValueError, match="길이"):
        mk_jobset(manager, ["a", "b"], job_keys=["m1"])
    with pytest.raises(ValueError, match="길이"):
        mk_jobset(manager, ["a", "b"], user_datas=[{"x": 1}])


def test_set_user_data_by_refs(qtbot, manager, fake_lsf):
    js = mk_jobset(manager, ["customwrapper_sub a.sp"], job_keys=["m1"])

    manager.set_user_data(js, js.jobs()[0].job_key, {"v": 1})   # job_key로
    assert js.jobs()[0].user_data == {"v": 1}
    manager.set_user_data(js, "m1", {"v": 2})              # merge_id로
    assert js.jobs()[0].user_data == {"v": 2}

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    jid = js.jobs()[0].job_id
    manager.set_user_data(js, jid, {"v": 3})               # job_id(int)로
    assert js.jobs()[0].user_data == {"v": 3}


# ----------------------------------------------------------------------
# job 편집 3형제 — add_jobs / replace_jobs / upsert_jobs
# ----------------------------------------------------------------------
def test_replace_jobs_keeps_physical_key(qtbot, manager, fake_lsf):
    """교체는 물리 키(job_key)를 유지한다 — GUI 표의 행이 이어진다.
    내용(command/user_data)만 새것으로 바뀐다."""
    a = mk_jobset(manager, 
        ["customwrapper_sub v1.sp", "customwrapper_sub keep.sp"],
        job_keys=["m1", "keep"], user_datas=[{"ver": 1}, None], label="target")
    old = next(r for r in a.jobs() if r.job_key == "m1")

    changed = manager.replace_jobs(
        a, ["customwrapper_sub v2.sp"], job_keys=["m1"],
        user_datas=[{"ver": 2}])

    by_mid = {r.job_key: r for r in a.jobs()}
    rep = by_mid["m1"]
    assert rep.job_key == old.job_key                  # 물리 키 유지
    assert rep.command == "customwrapper_sub v2.sp"    # 내용은 새것
    assert rep.user_data == {"ver": 2}
    assert by_mid["keep"].command == "customwrapper_sub keep.sp"
    assert a.summary["total"] == 2                     # 늘지 않는다
    assert [r.job_key for r in changed] == ["m1"]


def test_add_jobs_appends_and_rejects_duplicate(manager):
    """추가는 순수 추가 — job_key가 이미 있으면 ValueError."""
    a = mk_jobset(manager, ["customwrapper_sub a.sp"], job_keys=["m1"])

    manager.add_jobs(a, ["customwrapper_sub new.sp", "customwrapper_sub x.sp"],
                     job_keys=["m2", "m3"])
    assert a.summary["total"] == 3
    assert {r.job_key for r in a.jobs()} == {"m1", "m2", "m3"}

    with pytest.raises(ValueError, match="이미 있습니다"):
        manager.add_jobs(a, ["customwrapper_sub dup.sp"], job_keys=["m1"])
    assert a.summary["total"] == 3                # 한 건도 안 들어갔다


def test_job_keys_are_required(manager):
    """job_keys는 **필수**다 — 라이브러리가 이름을 대신 지어주지 않는다.

    자동 생성하면 그 job을 나중에 가리킬 방법이 앱에 없다(replace/remove/
    only의 ref가 전부 이 키다). 조용히 넘어가지 않고 거부한다."""
    with pytest.raises(ValueError, match="job_keys는 필수"):
        manager.create_jobset(["c0", "c1"])
    js = mk_jobset(manager, [])
    with pytest.raises(ValueError, match="job_keys는 필수"):
        manager.add_jobs(js, ["c0"])
    # 항목이 None인 것도 거부 — 일부만 지어주지도 않는다
    with pytest.raises(ValueError, match="job_key는"):
        manager.create_jobset(["c0", "c1"], job_keys=["a", None])
    assert js.jobs() == []


def test_empty_jobset_needs_no_keys(manager):
    """commands가 없으면 키도 필요 없다 — 빈 jobset은 그대로 만들어진다."""
    js = manager.create_jobset(label="empty")
    assert js.jobs() == [] and js.summary["total"] == 0


def test_job_key_is_what_the_caller_gave(manager):
    """저장된 키가 앱이 준 이름 그대로다."""
    js = mk_jobset(manager, ["c0", "c1"], job_keys=["case-a", "case-b"])
    assert [r.job_key for r in js.jobs()] == ["case-a", "case-b"]


def test_add_jobs_key_does_not_collide_after_remove(manager):
    """remove_job으로 중간이 빈 뒤 추가해도 job_key가 충돌하지 않는다."""
    a = mk_jobset(manager, 
        ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
        job_keys=["m1", "m2"])
    manager.remove_jobs(a, ["m1"])          # _0 이 비었다
    manager.add_jobs(a, ["customwrapper_sub c.sp"], job_keys=["m3"])
    keys = [r.job_key for r in a.jobs()]
    assert len(keys) == len(set(keys)) == 2


def test_replace_jobs_requires_existing_target(manager):
    """교체 대상이 없으면 JobNotFoundError — 추가하려면 add/upsert."""
    a = mk_jobset(manager, ["customwrapper_sub a.sp"], job_keys=["m1"])
    with pytest.raises(JobNotFoundError):
        manager.replace_jobs(a, ["customwrapper_sub z.sp"], job_keys=["nope"])
    with pytest.raises(ValueError, match="job_key는"):
        manager.replace_jobs(a, ["customwrapper_sub z.sp"], job_keys=[None])


def test_upsert_jobs_replaces_or_adds(manager):
    """있으면 교체, 없으면 추가 — 한 번에 반영."""
    a = mk_jobset(manager, ["customwrapper_sub a.sp"], job_keys=["m1"])
    old = a.jobs()[0]

    manager.upsert_jobs(
        a, ["customwrapper_sub a2.sp", "customwrapper_sub b.sp"],
        job_keys=["m1", "m2"])

    by_mid = {r.job_key: r for r in a.jobs()}
    assert a.summary["total"] == 2
    assert by_mid["m1"].job_key == old.job_key            # 교체(키 유지)
    assert by_mid["m1"].command == "customwrapper_sub a2.sp"
    assert by_mid["m2"].command == "customwrapper_sub b.sp"   # 추가


def test_edit_rejects_duplicate_merge_id_in_one_call(manager):
    """한 호출 안에서 같은 merge_id가 두 번 오면 ValueError."""
    a = mk_jobset(manager, [])
    with pytest.raises(ValueError, match="중복"):
        manager.add_jobs(a, ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
                         job_keys=["same", "same"])


def test_replace_jobs_guard_and_force(qtbot, manager, fake_lsf):
    """교체 대상이 활성이면 거부, force면 레코드만 교체 진행."""
    a = mk_jobset(manager, ["customwrapper_sub run.sp"], job_keys=["m1"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)               # m1이 PEND(활성)로

    with pytest.raises(JobEditNotAllowedError, match="활성"):
        manager.replace_jobs(a, ["customwrapper_sub v2.sp"], job_keys=["m1"])

    live_id = a.jobs()[0].job_id
    manager.replace_jobs(a, ["customwrapper_sub v2.sp"], job_keys=["m1"],
                         force=True)                     # 레코드만 강제 교체
    rec = a.jobs()[0]
    assert rec.state is JobState.CREATED
    assert rec.command == "customwrapper_sub v2.sp"
    # LSF의 실제 job은 그대로 산다 — 정리는 caller(GUI) 책임
    assert any(j.job_id == live_id for j in fake_lsf.alive_jobs())


# ----------------------------------------------------------------------
# submit — 전 job (재)제출 + can_submit
# ----------------------------------------------------------------------
def test_submit_resubmits_all_inactive(qtbot, manager, fake_lsf):
    """DONE/EXIT 포함 전 job이 리셋 후 재제출된다 — 같은 job_key 유지."""
    js = mk_jobset(
        manager, ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
        job_keys=["m1", "m2"], user_datas=[{"keep": True}, None])
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
    by_mid = {r.job_key: r for r in js.jobs()}
    assert by_mid["m1"].user_data == {"keep": True}      # user_data 보존


def test_submit_rejected_while_active(qtbot, manager, fake_lsf):
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # PEND(활성)

    assert manager.can_submit(js) is False
    with pytest.raises(LsfmgrError, match="활성"):
        manager.submit(js)

    _finish_all(manager, fake_lsf, js)
    assert manager.can_submit(js) is True


def test_submit_empty_jobset_rejected(manager):
    js = mk_jobset(manager)
    assert manager.can_submit(js) is False
    with pytest.raises(LsfmgrError, match="job이 없습니다"):
        manager.submit(js)


def test_submit_resets_previous_run_traces(qtbot, manager, fake_lsf):
    """재제출 리셋이 이전 실행 흔적(exit_code/run_time/fail_message)을
    지운다 (구 resubmit의 리셋 계약 이식)."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
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
    """재실행 패턴: 실패 job을 같은 merge_id로 교체(replace_jobs) 후
    전체 submit — resubmit 없이 재실행이 표현된다."""
    js = mk_jobset(manager, 
        ["customwrapper_sub bad.sp", "customwrapper_sub ok.sp"],
        job_keys=["m1", "m2"], label="run")
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    # m1은 EXIT(실패), m2는 DONE으로 종료
    recs = {r.job_key: r for r in js.jobs()}
    fake_lsf.set_job(recs["m1"].job_id, "EXIT", 1)
    fake_lsf.set_job(recs["m2"].job_id, "DONE", 0)
    manager.querier.query(js.id)

    manager.replace_jobs(js, ["customwrapper_sub fixed.sp"],
                         job_keys=["m1"])          # m1만 교체 (m2 결과 유지)

    recs = {r.job_key: r for r in js.jobs()}
    assert recs["m1"].state is JobState.CREATED
    assert recs["m2"].state is JobState.DONE
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)              # 전체 재실행 (요구: 전 job)
    assert all(r.state is JobState.PEND for r in js.jobs())


def test_add_jobs_resumes_polling(qtbot, manager, fake_lsf):
    """job이 추가되면 관찰 대상이 생기므로 폴링이 (재)시작된다.
    (폴링 tick에 tie된 handler가 새 job에 침묵하지 않도록 — 사이클 15)"""
    a = mk_jobset(manager, ["customwrapper_sub a.sp"])
    manager.start_polling(a.id, 5.0)
    manager.stop_polling(a.id)                  # 껐다 — 기억도 지워진다
    assert manager._poll_intervals.get(a.id) is None

    manager.add_jobs(a, ["customwrapper_sub b.sp"], job_keys=["m2"])

    with qtbot.waitSignal(manager.jobset_updated, timeout=10000,
                          check_params_cb=lambda j, _s: j == a.id):
        pass                                    # 폴링이 실제로 돈다


# ----------------------------------------------------------------------
# pre_submit 게이트 — mgr.submit(js, pre_submit=fn) (A안)
# ----------------------------------------------------------------------
def test_submit_jobset_pre_submit_gate_pass(qtbot, manager, fake_lsf):
    """게이트 통과 시 정상 제출 — 검사 대상은 커맨드 리스트 전체."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
    seen = []

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, pre_submit=lambda cmds: seen.append(cmds) or True,
                       auto_poll=False)

    assert seen and "customwrapper_sub a.sp" in seen[0][0]
    assert js.jobs()[0].state is JobState.PEND


def test_submit_jobset_gate_reject_keeps_records(qtbot, manager, fake_lsf):
    """게이트 False → 레코드 원상 유지(리셋 없음) + 제출 없음.
    게이트는 리셋 **이전**에 돈다 — DONE 결과가 지워지지 않는다."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    _finish_all(manager, fake_lsf, js)
    old = js.jobs()[0]                          # DONE + job_id 보유
    n_lsf = len(fake_lsf.jobs)

    with qtbot.waitSignal(manager.pre_submit_finished, timeout=10000) as blk:
        manager.submit(js, pre_submit=lambda cmds: False, auto_poll=False)
    assert blk.args == [js.id, False]

    rec = js.jobs()[0]
    assert rec.state is JobState.DONE           # 원상 유지 (리셋 안 됨)
    assert rec.job_id == old.job_id
    assert len(fake_lsf.jobs) == n_lsf          # 제출 없음


def test_submit_jobset_gate_exception_keeps_records(qtbot, manager, fake_lsf):
    """게이트 예외 → 레코드 원상 + error_occurred + finished(failed)."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
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
