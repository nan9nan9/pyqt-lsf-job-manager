"""LsfCommand 단위 테스트 — mock runner 주입 (Qt 불필요)."""
from __future__ import annotations

import pytest

from lsfmgr.command import CommandResult, LsfCommand, chunk_args
from lsfmgr.config import LsfConfig
from lsfmgr.errors import ArgMaxExceededError, SubmitError
from lsfmgr.states import JobState
from tests.fake_lsf import FakeLsf


@pytest.fixture
def cmd(fake_lsf):
    return LsfCommand(LsfConfig(), fake_lsf)


# ----------------------------------------------------------------------
# lsid 클러스터 진단 (DEBUG 전용)
# ----------------------------------------------------------------------
def test_lsid_diagnostic_at_debug(fake_lsf, caplog):
    import logging
    caplog.set_level(logging.DEBUG, logger="lsfmgr.command")
    LsfCommand(LsfConfig(), fake_lsf)
    # bare "lsid"로 실행 — 경로 고정 없이 프로세스 PATH가 잡는 클러스터 진단
    assert fake_lsf.calls_of("lsid") == [["lsid"]]
    assert "fake_cluster" in caplog.text


def test_lsid_skipped_without_debug(fake_lsf, caplog):
    import logging
    caplog.set_level(logging.INFO, logger="lsfmgr.command")
    LsfCommand(LsfConfig(), fake_lsf)
    assert not fake_lsf.calls_of("lsid")     # DEBUG 아니면 실행 자체를 안 함


def test_lsid_failure_is_swallowed(caplog):
    import logging
    caplog.set_level(logging.DEBUG, logger="lsfmgr.command")

    def runner(argv, timeout, cwd=None):
        raise OSError("lsid: command not found")

    LsfCommand(LsfConfig(), runner)          # 예외가 밖으로 안 새면 성공
    assert "lsid 진단 실패" in caplog.text


# ----------------------------------------------------------------------
# 제출 (run_submit — v10: wrapper 단일 경로)
# ----------------------------------------------------------------------
def _submit(cmd, command="run", name=None):
    """테스트 헬퍼 — customwrapper_sub 경유 제출(내부적으로 bsub 흉내에
    위임하므로 -J 이름/array 지정 가능)."""
    argv = ["customwrapper_sub"]
    if name:
        argv += ["-J", name]
    argv.append(command)
    return cmd.run_submit(argv)


def test_run_submit_parses_job_id(cmd, fake_lsf):
    jid = cmd.run_submit(
        ["customwrapper_sub", "-q", "normal", "-J", "t_0", "echo hi"])
    assert jid == 1000
    job = fake_lsf.jobs["1000"]
    assert job.name == "t_0"
    assert job.queue == "normal"


def test_run_submit_failure_classified(cmd, fake_lsf):
    fake_lsf.fail_next_bsub = 1
    with pytest.raises(SubmitError) as ei:
        _submit(cmd, "echo hi")
    assert ei.value.fail_reason == "BSUB_EXIT_1"


def test_run_submit_no_jobid_parsed(cmd, fake_lsf):
    fake_lsf.no_jobid_next_bsub = 1
    with pytest.raises(SubmitError) as ei:
        _submit(cmd, "echo hi")
    assert ei.value.fail_reason == "NO_JOBID_PARSED"


def test_run_submit_timeout():
    import subprocess

    def timeout_runner(argv, timeout, cwd=None):
        raise subprocess.TimeoutExpired(argv, timeout)

    cmd = LsfCommand(LsfConfig(), timeout_runner)
    with pytest.raises(SubmitError) as ei:
        cmd.run_submit(["wrap", "echo hi"])
    assert ei.value.fail_reason == "BSUB_TIMEOUT"


# ----------------------------------------------------------------------
# bjobs
# ----------------------------------------------------------------------
def test_bjobs_uses_json_output(cmd, fake_lsf):
    """모든 bjobs 호출은 -json으로 나간다 (v10 — delimiter 파싱 제거)."""
    jid = _submit(cmd, name="t_0")
    out, _failed = cmd.bjobs_by_ids([jid])
    assert [s.job_name for s in out] == ["t_0"]
    for call in fake_lsf.calls_of("bjobs"):
        assert "-json" in call
        assert "-g" not in call and "-J" not in call


def test_bjobs_by_ids_chunked(fake_lsf):
    cfg = LsfConfig(chunk_size=10)
    cmd = LsfCommand(cfg, fake_lsf)
    ids = [_submit(cmd, f"run {i}") for i in range(25)]
    out, failed = cmd.bjobs_by_ids(ids)
    assert len(out) == 25
    assert failed == set()
    # 25개 / chunk 10 → bjobs 3회
    assert len(fake_lsf.calls_of("bjobs")) == 3


def test_bjobs_by_ids_chunk_failure_isolated(fake_lsf):
    """chunk 하나의 실패는 그 chunk의 id만 실패 집합에 귀속 — 나머지 chunk는
    정상 조회된다 (bhist_states와 동일한 격리)."""
    cmd = LsfCommand(LsfConfig(chunk_size=1), fake_lsf)
    ids = [_submit(cmd, f"run {i}") for i in range(3)]
    fake_lsf.bjobs_fail_ids = {ids[1]}       # 가운데 id의 chunk만 rc=255

    out, failed = cmd.bjobs_by_ids(ids)

    assert failed == {ids[1]}
    assert {s.job_id for s in out} == {ids[0], ids[2]}


def test_bjobs_empty_result(cmd):
    # 없는 id 조회 — no-match는 '장애'가 아니라 빈 결과 (failed에도 안 들어감)
    out, failed = cmd.bjobs_by_ids([999999])
    assert out == []
    assert failed == set()


def test_bjobs_array_elements(cmd, fake_lsf):
    jid = _submit(cmd, "run.sh", name="arr[1-5]")
    out, _failed = cmd.bjobs_by_ids([jid])
    assert len(out) == 5
    assert {s.array_index for s in out} == {1, 2, 3, 4, 5}
    assert all(s.job_id == jid for s in out)


def test_bjobs_exit_code_parsing(cmd, fake_lsf):
    jid = _submit(cmd)
    fake_lsf.set_job(jid, "EXIT", exit_code=42)
    out, _failed = cmd.bjobs_by_ids([jid])
    assert out[0].state is JobState.EXIT
    assert out[0].exit_code == 42


def test_bjobs_downgrades_on_unsupported_field():
    """확장 -o 필드를 거부하는 LSF에서 CORE 포맷으로 강등해 폴링을 살린다
    (강등 안 하면 bjobs가 매번 죽어 job이 PEND에 고착)."""
    core = ('{"COMMAND":"bjobs","JOBS":2,"RECORDS":['
            '{"JOBID":"111","STAT":"PEND","EXIT_CODE":"","JOB_NAME":"j0"},'
            '{"JOBID":"222","STAT":"RUN","EXIT_CODE":"","JOB_NAME":"j1"}]}')

    def runner(argv, timeout, cwd=None):
        fmt = argv[argv.index("-o") + 1]
        if "exec_cwd" in fmt:            # 확장 포맷 거부
            return CommandResult(255, "", "bjobs: Unknown field: exec_cwd\n")
        return CommandResult(0, core + "\n", "")

    cmd = LsfCommand(LsfConfig(), runner)
    assert cmd._bjobs_fmt is cmd._BJOBS_FULL_FMT
    out, failed = cmd.bjobs_by_ids([111, 222])
    assert cmd._bjobs_fmt is cmd._BJOBS_CORE_FMT      # 강등됨
    assert failed == set()
    assert [(s.job_id, s.state) for s in out] == \
        [(111, JobState.PEND), (222, JobState.RUN)]   # 상태는 정상 파싱


def test_bjobs_transient_error_no_downgrade():
    """일시 장애(필드 오류 아님)는 강등하지 않고 전파 — 확장필드 보존."""
    from lsfmgr.errors import LsfCommandError

    def runner(argv, timeout, cwd=None):
        return CommandResult(255, "", "LSF error: cannot reach mbatchd\n")

    cmd = LsfCommand(LsfConfig(), runner)
    with pytest.raises(LsfCommandError):
        cmd._bjobs(["111"])
    assert cmd._bjobs_fmt is cmd._BJOBS_FULL_FMT      # 강등 안 됨


# ----------------------------------------------------------------------
# bkill
# ----------------------------------------------------------------------
def test_bkill_targets_chunked(fake_lsf):
    cmd = LsfCommand(LsfConfig(chunk_size=20), fake_lsf)
    ids = [_submit(cmd, f"r {i}") for i in range(45)]
    calls = cmd.bkill_targets([str(i) for i in ids])
    assert calls == 3
    assert fake_lsf.alive_jobs() == []


def test_bkill_no_matching_job_is_ok(cmd):
    # 이미 종료된 job kill은 에러 아님 (no-match는 예외가 아님)
    cmd.bkill_targets(["999999"])


def test_bkill_confirm_parses_terminating(cmd, fake_lsf):
    ids = [_submit(cmd, f"r {i}") for i in range(3)]
    resolved, calls = cmd.bkill_targets_confirm([str(i) for i in ids])
    assert calls == 1
    assert resolved == {str(i) for i in ids}        # 전부 'is being terminated'


def test_bkill_resolved_parser_variants():
    from lsfmgr.command import _parse_bkill_resolved
    text = (
        "Job <101> is being terminated\n"
        "Job <102>: Job has already finished\n"
        "Job <103>: No matching job found\n"
        "Job <104>: LSF error: cannot reach mbatchd\n"   # 미해소 → 재시도 대상
        "Job <105[2]> is being terminated\n"
    )
    resolved = _parse_bkill_resolved(text)
    # 105[2]는 element + 부모 105 둘 다 해소 (bare 부모 id kill 매칭용)
    assert resolved == {"101", "102", "103", "105[2]", "105"}
    assert "104" not in resolved


def test_bkill_confirm_array_parent_id(cmd, fake_lsf):
    """bare 부모 id로 array kill 시 element 확인 행이 부모 pending과 매칭돼
    한 라운드에 해소된다 (불필요 재시도 없음)."""
    jid = _submit(cmd, "run.sh", name="arr[1-4]")   # array 부모 id
    resolved, calls = cmd.bkill_targets_confirm([str(jid)])
    assert calls == 1
    assert str(jid) in resolved                     # 부모 id 해소됨


# ----------------------------------------------------------------------
# bhist
# ----------------------------------------------------------------------
def test_bhist_states(cmd, fake_lsf):
    j1 = _submit(cmd, "a")
    j2 = _submit(cmd, "b")
    fake_lsf.set_job(j1, "DONE", 0)
    fake_lsf.set_job(j2, "EXIT", 7)
    fake_lsf.vanish_job(j1)
    fake_lsf.vanish_job(j2)
    hist, failed = cmd.bhist_states([j1, j2])
    assert hist[(j1, None)] == (JobState.DONE, 0)
    assert hist[(j2, None)] == (JobState.EXIT, 7)
    assert failed == set()


def test_bhist_distinguishes_array_elements(cmd, fake_lsf):
    """array element별 상태 구분 — id 단일 키면 마지막 블록이 덮어쓴다."""
    jid = _submit(cmd, "run.sh", name="arr[1-3]")
    fake_lsf.set_job(jid, "DONE", 0, array_index=1)
    fake_lsf.set_job(jid, "EXIT", 9, array_index=2)
    fake_lsf.set_job(jid, "DONE", 0, array_index=3)
    hist, _failed = cmd.bhist_states([jid])
    assert hist[(jid, 1)] == (JobState.DONE, 0)
    assert hist[(jid, 2)] == (JobState.EXIT, 9)
    assert hist[(jid, 3)] == (JobState.DONE, 0)


# ----------------------------------------------------------------------
# chunk_args
# ----------------------------------------------------------------------
def test_chunk_args_by_count():
    chunks = list(chunk_args([str(i) for i in range(10)], 3, 10000))
    assert [len(c) for c in chunks] == [3, 3, 3, 1]


def test_chunk_args_by_arg_max():
    items = ["x" * 50] * 10
    chunks = list(chunk_args(items, 100, 120))
    assert all(sum(len(i) + 1 for i in c) <= 120 for c in chunks)
    assert sum(len(c) for c in chunks) == 10


def test_chunk_args_single_item_too_long():
    with pytest.raises(ArgMaxExceededError):
        list(chunk_args(["y" * 200], 10, 100))
