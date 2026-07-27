"""bjobs -a 제거 회귀 + id 단독 조회(v10) 계약.

배경: bjobs -a는 CLEAN_PERIOD 내 이미 끝난 다른 job까지 끌어와 결과를
오염시켰다. v10에서 group/name 조회 자체가 제거되어 explicit job id
chunked 조회가 유일한 경로다. 계약:
  - 어떤 bjobs 호출에도 -a를 붙이지 않는다
  - 종료 상태(DONE/EXIT)는 explicit job id 조회로 잡는다 (LSF는 id 지정 시
    -a 없이도 CLEAN_PERIOD 내 종료 job을 보여줌)
  - purge된(CLEAN_PERIOD 밖) job만 bhist fallback으로 넘어감
"""
from __future__ import annotations

from lsfmgr import LsfConfig
from lsfmgr.command import LsfCommand
from lsfmgr.states import JobState
from tests.fake_lsf import FakeJob
from tests.conftest import submit_cmds


def _cmd(fake):
    return LsfCommand(config=LsfConfig(), runner=fake)


def test_no_bjobs_call_uses_all_flag(qtbot, manager, fake_lsf):
    """어떤 bjobs 호출에도 -a가 붙지 않는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    fake_lsf.set_all("DONE")
    manager.querier.query(js.id)

    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert bjobs_calls, "bjobs가 한 번도 안 불렸다"
    assert all("-a" not in c for c in bjobs_calls), \
        [c for c in bjobs_calls if "-a" in c]


def test_no_bjobs_call_uses_group_or_name(qtbot, manager, fake_lsf):
    """v10: 폴링은 -g/-J 조회를 하지 않는다 — explicit id 조회뿐."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)

    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert bjobs_calls, "bjobs가 한 번도 안 불렸다"
    assert all("-g" not in c and "-J" not in c for c in bjobs_calls), \
        [c for c in bjobs_calls if "-g" in c or "-J" in c]


def test_explicit_id_still_returns_done(fake_lsf):
    """explicit job id 조회는 -a 없이도 종료 job(DONE/EXIT)을 돌려준다."""
    fake_lsf.jobs["1001"] = FakeJob(1001, None, "j_done", "grp", "q", "echo",
                                    stat="DONE", exit_code=0)
    got, _failed = _cmd(fake_lsf).bjobs_by_ids([1001])
    assert [(s.job_id, s.state) for s in got] == [(1001, JobState.DONE)]


def test_done_detected_via_id_query(qtbot, manager, fake_lsf):
    """job 종료는 explicit-id 조회로 잡힌다 — bhist 없이 (설계 무결성)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    jid = js.jobs()[0].job_id
    fake_lsf.set_job(jid, "RUN")
    manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.set_job(jid, "DONE")
    result = manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.DONE
    assert jid in {r.job_id for r in result.changed}
    assert not fake_lsf.calls_of("bhist"), "explicit-id로 잡혔는데 bhist 호출됨"
