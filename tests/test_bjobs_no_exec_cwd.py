"""bjobs exec_cwd 제거 회귀 (v10.4).

배경: 작업 디렉토리를 관측값(bjobs exec_cwd → JobRecord.working_dir)과
요청값(submit_cwd) 두 곳에 두니 같은 경로를 가리키는 필드가 둘이라
헷갈렸고, exec_cwd는 RUN 이후에야 채워지면서 조회 포맷만 무겁게 했다.
working_dir을 없애고 submit_cwd 하나로 본다. 계약:
  - 어떤 bjobs 호출의 -o 포맷에도 exec_cwd를 넣지 않는다 (강등 전 단계 포함)
  - JobRecord/JobStatus에 working_dir 필드가 없다
  - 작업 디렉토리는 create_jobset의 work_dir(s) 요청값으로만 정해진다
    (미지정이면 None = 부모 프로세스 cwd)
"""
from __future__ import annotations

import dataclasses

from lsfmgr import LsfConfig
from lsfmgr.command import JobStatus, LsfCommand
from lsfmgr.states import JobRecord
from tests.conftest import mk_jobset, submit_cmds


def _fields(cls):
    return {f.name for f in dataclasses.fields(cls)}


def test_no_bjobs_call_requests_exec_cwd(qtbot, manager, fake_lsf):
    """어떤 bjobs 호출에도 exec_cwd를 요청하지 않는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    fake_lsf.set_all("DONE")
    manager.querier.query(js.id)

    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert bjobs_calls, "bjobs가 한 번도 안 불렸다"
    offending = [c for c in bjobs_calls if any("exec_cwd" in a for a in c)]
    assert not offending, offending


def test_no_bjobs_format_mentions_exec_cwd():
    """강등 전 단계(FULL+MC/FULL/CORE) 포맷 정의 자체에 exec_cwd가 없다 —
    MC 사이트에서만 쓰이는 첫 단계까지 포함해 고정한다."""
    for fmt in (LsfCommand._BJOBS_CORE_FMT, LsfCommand._BJOBS_FULL_FMT,
                LsfCommand._BJOBS_FULL_MC_FMT, LsfCommand._BJOBS_CLUSTER_FMT):
        assert "exec_cwd" not in fmt, fmt


def test_working_dir_field_is_gone():
    """되살리기 방지 — 레코드/조회결과 어디에도 working_dir이 없다."""
    assert "working_dir" not in _fields(JobRecord)
    assert "working_dir" not in _fields(JobStatus)
    assert "submit_cwd" in _fields(JobRecord)    # 작업 디렉토리는 이쪽 하나


def test_submit_cwd_is_request_value_only(qtbot, manager):
    """작업 디렉토리는 요청값으로만 정해진다 — 폴링이 채우지 않는다.
    미지정이면 None(= 부모 프로세스 cwd)이 그대로 유지된다."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
    assert js.jobs()[0].submit_cwd is None
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    manager.querier.query(js.id)                 # 폴링해도 안 채워진다
    assert js.jobs()[0].submit_cwd is None

    js2 = mk_jobset(manager, ["customwrapper_sub b.sp"],
                                work_dir="/scratch/b")
    assert js2.jobs()[0].submit_cwd == "/scratch/b"
