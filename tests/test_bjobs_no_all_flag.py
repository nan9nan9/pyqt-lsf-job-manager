"""bjobs -a 제거 회귀 — group/name 조회의 '과거 종료 job' 오염 방지.

배경: bjobs -a는 -g/-J(group/name) 조회에 CLEAN_PERIOD 내 이미 끝난 다른
job까지 끌어와 by_name 풀을 오염시켰다(이름 재사용/ job_id 없는 레코드에서
옛 job의 DONE/EXIT가 로드됨). 제거 후 계약:
  - group/name 조회 → active(RUN/PEND 등)만 반환
  - 종료 상태(DONE/EXIT) → explicit job id 재조회로만 (LSF는 id 지정 시
    -a 없이도 CLEAN_PERIOD 내 종료 job을 보여줌)
  - purge된(CLEAN_PERIOD 밖) job만 bhist fallback으로 넘어감
"""
from __future__ import annotations

from lsfmgr import LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand
from lsfmgr.states import JobState
from tests.fake_lsf import FakeJob


def _cmd(fake):
    return LsfCommand(config=LsfConfig(), runner=fake)


def test_no_bjobs_call_uses_all_flag(qtbot, manager, fake_lsf):
    """어떤 bjobs 호출에도 -a가 붙지 않는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], mode="bulk", auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    fake_lsf.set_all("DONE")
    manager.querier.query(js.id)

    bjobs_calls = fake_lsf.calls_of("bjobs")
    assert bjobs_calls, "bjobs가 한 번도 안 불렸다"
    assert all("-a" not in c for c in bjobs_calls), \
        [c for c in bjobs_calls if "-a" in c]


def test_group_query_returns_active_only(fake_lsf):
    """-g(group) 조회는 종료 job을 반환하지 않는다(active만)."""
    fake_lsf.jobs["1000"] = FakeJob(1000, None, "j_run", "grp", "q", "echo",
                                    stat="RUN")
    fake_lsf.jobs["1001"] = FakeJob(1001, None, "j_done", "grp", "q", "echo",
                                    stat="DONE")
    states = {s.job_id: s.state for s in _cmd(fake_lsf).bjobs_by_group("grp")}
    assert states == {1000: JobState.RUN}          # DONE(1001)은 빠짐


def test_name_query_excludes_stale_finished(fake_lsf):
    """-J(name) 조회는 같은 이름의 과거 종료 job을 끌어오지 않는다 —
    오염 방지의 핵심 시나리오."""
    # 현재 살아있는 job과, 이름은 같지만 id가 다른 '옛날 끝난' job(이름 재사용)
    fake_lsf.jobs["1000"] = FakeJob(1000, None, "js1_0", None, "q", "echo",
                                    stat="RUN")
    fake_lsf.jobs["500"] = FakeJob(500, None, "js1_0", None, "q", "echo",
                                   stat="DONE")
    ids = {s.job_id for s in _cmd(fake_lsf).bjobs_by_name("js1_0")}
    assert ids == {1000}, f"옛 종료 job(500)이 딸려옴: {ids}"


def test_explicit_id_still_returns_done(fake_lsf):
    """explicit job id 조회는 -a 없이도 종료 job(DONE/EXIT)을 돌려준다."""
    fake_lsf.jobs["1001"] = FakeJob(1001, None, "j_done", "grp", "q", "echo",
                                    stat="DONE", exit_code=0)
    got, _failed = _cmd(fake_lsf).bjobs_by_ids([1001])
    assert [(s.job_id, s.state) for s in got] == [(1001, JobState.DONE)]


def test_group_tracked_done_detected_via_id_requery(qtbot, manager, fake_lsf):
    """group으로 추적하는 job이 종료되면 group probe(active만)가 아니라
    leftover explicit-id 재조회로 DONE이 잡힌다 — bhist 없이 (설계 무결성)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], mode="bulk", auto_poll=False)
    jid = js.jobs()[0].job_id
    fake_lsf.set_job(jid, "RUN")
    manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.RUN

    fake_lsf.set_job(jid, "DONE")
    result = manager.querier.query(js.id)
    assert js.jobs()[0].state is JobState.DONE
    assert jid in {r.job_id for r in result.changed}
    assert not fake_lsf.calls_of("bhist"), "explicit-id로 잡혔는데 bhist 호출됨"
