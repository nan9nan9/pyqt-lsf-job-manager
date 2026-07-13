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
