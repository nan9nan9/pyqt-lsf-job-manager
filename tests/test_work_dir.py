"""per-job 작업 디렉토리(work_dirs) — 제출 subprocess의 cwd 지정 (wrapper 포함).

wrapper 경로는 bsub 인자 -cwd를 못 주므로 subprocess cwd로 실행 디렉토리를
지정한다(스레드 안전). create_jobset(commands=[...], work_dirs=[...]).
"""
from __future__ import annotations

import pytest

from lsfmgr import JobState


def _wrapper_calls(fake_lsf):
    """제출(wrapper) 호출만 (argv, cwd) 쌍으로 — bjobs/bkill 제외."""
    return [(argv, cwd)
            for argv, cwd in zip(fake_lsf.calls, fake_lsf.call_cwds)
            if argv and argv[0].rsplit("/", 1)[-1] == "customwrapper_sub"]


# ----------------------------------------------------------------------
# work_dirs가 각 job의 submit_cwd로 저장된다
# ----------------------------------------------------------------------
def test_work_dirs_set_submit_cwd(qtbot, manager):
    js = manager.create_jobset(
        ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
        work_dirs=["/scratch/a", "/scratch/b"])
    cwds = sorted(r.submit_cwd for r in js.jobs())
    assert cwds == ["/scratch/a", "/scratch/b"]


# ----------------------------------------------------------------------
# jobset 단위 기본 work_dir — 전 job에 적용
# ----------------------------------------------------------------------
def test_jobset_default_work_dir_applies_to_all(qtbot, manager):
    js = manager.create_jobset(
        ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
        work_dir="/scratch/common")
    assert all(r.submit_cwd == "/scratch/common" for r in js.jobs())


# ----------------------------------------------------------------------
# work_dir와 work_dirs는 동시 지정 불가 (혼합모드 제거) — ValueError
# ----------------------------------------------------------------------
def test_work_dir_and_work_dirs_mutually_exclusive(qtbot, manager):
    with pytest.raises(ValueError):
        manager.create_jobset(
            ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
            work_dir="/scratch/common",
            work_dirs=["/scratch/a", "/scratch/b"])


# ----------------------------------------------------------------------
# merge: merge_id 일치 job은 work_dir도 신규(source) 것으로 교체된다
# ----------------------------------------------------------------------
def test_merge_replaces_work_dir_by_merge_id(qtbot, manager):
    tgt = manager.create_jobset(["customwrapper_sub a.sp"],
                                merge_ids=["a"], work_dir="/old")
    src = manager.create_jobset(["customwrapper_sub a.sp"],
                                merge_ids=["a"], work_dir="/new")
    manager.merge(tgt, src)
    rec = next(r for r in tgt.jobs() if r.merge_id == "a")
    assert rec.submit_cwd == "/new"      # 신규 source work_dir로 교체(교체 규칙)


# ----------------------------------------------------------------------
# 제출 subprocess가 그 work_dir을 cwd로 실행한다 (wrapper 경로)
# ----------------------------------------------------------------------
def test_submit_uses_work_dir_as_subprocess_cwd(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub run.sp"],
                               work_dirs=["/scratch/run_a"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    subs = _wrapper_calls(fake_lsf)
    assert subs, "wrapper 제출 호출 없음"
    assert all(cwd == "/scratch/run_a" for _argv, cwd in subs)


# ----------------------------------------------------------------------
# work_dir 미지정 job은 cwd=None (부모 프로세스 cwd)
# ----------------------------------------------------------------------
def test_no_work_dir_is_none(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub run.sp"])   # work_dirs 없음
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    subs = _wrapper_calls(fake_lsf)
    assert subs and all(cwd is None for _a, cwd in subs)


# ----------------------------------------------------------------------
# 재제출에도 work_dir이 보존된다 (레코드 필드라 리셋이 안 지운다)
# ----------------------------------------------------------------------
def test_work_dir_preserved_on_resubmit(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub run.sp"],
                               work_dirs=["/scratch/run_a"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)
    fake_lsf.calls.clear()
    fake_lsf.call_cwds.clear()
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)     # 재제출
    subs = _wrapper_calls(fake_lsf)
    assert subs and all(cwd == "/scratch/run_a" for _a, cwd in subs)


# ----------------------------------------------------------------------
# work_dirs 길이가 commands와 다르면 ValueError
# ----------------------------------------------------------------------
def test_work_dirs_length_mismatch_raises(qtbot, manager):
    with pytest.raises(ValueError):
        manager.create_jobset(["a", "b"], work_dirs=["/only-one"])


# ----------------------------------------------------------------------
# bsub 경로(wrapper=False)도 work_dir을 cwd로 전달한다
# ----------------------------------------------------------------------
def test_bsub_path_uses_work_dir(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["run.sp"], work_dirs=["/scratch/bsub_a"],
                               wrapper=False)
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    subs = [(argv, cwd)
            for argv, cwd in zip(fake_lsf.calls, fake_lsf.call_cwds)
            if argv and argv[0].rsplit("/", 1)[-1] == "bsub"]
    assert subs, "bsub 호출 없음"
    assert all(cwd == "/scratch/bsub_a" for _a, cwd in subs)


# ----------------------------------------------------------------------
# 하위호환: 구 2-arg runner((argv, timeout))도 어댑터로 감싸져 동작한다
# (Runner 계약이 cwd 추가로 확장됐지만 기존 주입 runner를 깨지 않는다)
# ----------------------------------------------------------------------
def test_legacy_two_arg_runner_still_works(qtbot, fake_lsf, config):
    from lsfmgr import InMemoryStore, LsfJobManager

    def legacy_runner(argv, timeout):        # 구 2-arg — cwd 미지원
        return fake_lsf(argv, timeout)
    mgr = LsfJobManager(store=InMemoryStore(), config=config,
                        runner=legacy_runner)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blk:
            mgr.submit(js, auto_poll=False)
        assert blk.args[1].succeeded == 1    # TypeError 없이 제출 성공
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 존재하지 않는 work_dir → 분류된 SUBMIT_FAILED(BSUB_OSERROR), 불투명 crash 아님
# ----------------------------------------------------------------------
def test_invalid_work_dir_classified_submit_error():
    import pytest
    from lsfmgr import LsfConfig, SubmitError
    from lsfmgr.command import LsfCommand

    def raising_runner(argv, timeout, cwd=None):
        raise FileNotFoundError(2, "No such file or directory", cwd)
    cmd = LsfCommand(config=LsfConfig(), runner=raising_runner)
    for call in (lambda: cmd.run_submit(["customwrapper_sub", "a.sp"],
                                        cwd="/nope"),
                 lambda: cmd.bsub("run.sh", cwd="/nope")):
        with pytest.raises(SubmitError) as ei:
            call()
        assert ei.value.fail_reason == "BSUB_OSERROR"
        assert ei.value.retryable is False


def test_submit_invalid_work_dir_lands_submit_failed(qtbot, fake_lsf, config):
    import os

    from lsfmgr import InMemoryStore, JobState, LsfJobManager

    def cwd_checking_runner(argv, timeout, cwd=None):
        if cwd is not None and not os.path.isdir(cwd):
            raise FileNotFoundError(2, "No such file or directory", cwd)
        return fake_lsf(argv, timeout, cwd)
    mgr = LsfJobManager(store=InMemoryStore(), config=config,
                        runner=cwd_checking_runner)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"],
                               work_dirs=["/definitely/does/not/exist"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blk:
            mgr.submit(js, auto_poll=False, max_retry=0)
        assert blk.args[1].failed == 1
        rec = js.jobs()[0]
        assert rec.state is JobState.SUBMIT_FAILED
        assert rec.fail_reason == "BSUB_OSERROR"    # INTERNAL_ERROR 아님
    finally:
        mgr.shutdown()
