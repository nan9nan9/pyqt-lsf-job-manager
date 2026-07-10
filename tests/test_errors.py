"""도메인 예외 계층 — 명령별 '불가' 상황이 전용 예외로 구분되는지.

전부 LsfmgrError(→ JobSetStateError) 하위라 기존 `except LsfmgrError` 는 그대로
잡히고, 세분화된 타입으로 개별 catch·구조화 정보(jobset_id/job_keys) 접근이 된다.
"""
from __future__ import annotations

import pytest

from lsfmgr import (
    CloseNotAllowedError,
    JobSetStateError,
    LsfmgrError,
    MergeNotAllowedError,
    RemoveNotAllowedError,
    SubmitNotAllowedError,
)


def _finish(manager, fake_lsf, js):
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)


# ----------------------------------------------------------------------
# 계층 — 전부 JobSetStateError → LsfmgrError 하위
# ----------------------------------------------------------------------
def test_hierarchy():
    for exc in (SubmitNotAllowedError, MergeNotAllowedError,
                RemoveNotAllowedError, CloseNotAllowedError):
        assert issubclass(exc, JobSetStateError)
        assert issubclass(exc, LsfmgrError)


# ----------------------------------------------------------------------
# submit 불가 — 활성 job / job 없음
# ----------------------------------------------------------------------
def test_submit_not_allowed_active(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)          # PEND(활성)
    with pytest.raises(SubmitNotAllowedError) as ei:
        manager.submit(js)
    assert ei.value.jobset_id == js.id
    assert ei.value.job_keys                          # 막은 job_key 담김
    # 기존 코드 호환: LsfmgrError로도 잡힌다
    with pytest.raises(LsfmgrError):
        manager.submit(js)


def test_submit_not_allowed_empty(manager):
    js = manager.create_jobset()
    with pytest.raises(SubmitNotAllowedError, match="job이 없습니다"):
        manager.submit(js)


# ----------------------------------------------------------------------
# merge 불가 — 활성 job
# ----------------------------------------------------------------------
def test_merge_not_allowed_active(qtbot, manager, fake_lsf):
    a = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["m1"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)
    b = manager.create_jobset(["customwrapper_sub v2.sp"], merge_ids=["m1"])
    with pytest.raises(MergeNotAllowedError) as ei:
        manager.merge(a, b)
    assert ei.value.job_keys


# ----------------------------------------------------------------------
# remove / clear 불가 — 활성 job
# ----------------------------------------------------------------------
def test_remove_not_allowed_active(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"], merge_ids=["m1"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    with pytest.raises(RemoveNotAllowedError):
        manager.remove_job(js, merge_id="m1")
    with pytest.raises(RemoveNotAllowedError):
        manager.clear(js)


# ----------------------------------------------------------------------
# close 불가 — 전원 terminal 아님
# ----------------------------------------------------------------------
def test_close_not_allowed_active(qtbot, manager, fake_lsf):
    js = manager.create_jobset(["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)          # PEND
    with pytest.raises(CloseNotAllowedError) as ei:
        manager.close(js)
    assert ei.value.job_keys
    _finish(manager, fake_lsf, js)
    manager.close(js)                                # 전원 terminal — 허용
