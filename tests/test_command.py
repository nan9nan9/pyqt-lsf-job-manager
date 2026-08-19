"""LsfCommand 단위 테스트 — mock runner 주입 (Qt 불필요)."""
from __future__ import annotations

import pytest

from lsfmgr.command import CommandResult, LsfCommand, chunk_args
from lsfmgr.config import LsfConfig
from lsfmgr.errors import ArgMaxExceededError, SubmitError
from lsfmgr.states import JobState


@pytest.fixture
def cmd(fake_lsf):
    return LsfCommand(LsfConfig(rate_limit_per_s=None, ), fake_lsf)


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

    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), timeout_runner)
    with pytest.raises(SubmitError) as ei:
        cmd.run_submit(["wrap", "echo hi"])
    assert ei.value.fail_reason == "BSUB_TIMEOUT"


# ----------------------------------------------------------------------
# bjobs
# ----------------------------------------------------------------------
def test_bjobs_uses_noheader_delimiter(cmd, fake_lsf):
    """v10.2: bjobs는 -noheader + delimiter=';' (폭 지정 없음, -json 아님)."""
    jid = _submit(cmd, name="t_0")
    out, _failed = cmd.bjobs_by_ids([jid])
    assert [s.job_id for s in out] == [jid]
    for call in fake_lsf.calls_of("bjobs"):
        assert "-noheader" in call and "-json" not in call
        assert "-g" not in call and "-J" not in call
        fmt = call[call.index("-o") + 1]
        assert "delimiter=';'" in fmt
        assert ":" not in fmt                    # 필드 폭 지정 없음


def test_bjobs_by_ids_chunked(fake_lsf):
    cfg = LsfConfig(rate_limit_per_s=None, chunk_size=10)
    cmd = LsfCommand(cfg, fake_lsf)
    ids = [_submit(cmd, f"run {i}") for i in range(25)]
    out, failed = cmd.bjobs_by_ids(ids)
    assert len(out) == 25
    assert failed == set()
    # 25개 / chunk 10 → bjobs 3회
    assert len(fake_lsf.calls_of("bjobs")) == 3


def test_bjobs_by_ids_chunk_failure_isolated(fake_lsf):
    """chunk 하나의 실패는 그 chunk의 id만 실패 집합에 귀속 — 나머지 chunk는
    정상 조회된다 (chunk 격리)."""
    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, chunk_size=1), fake_lsf)
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
    core = "111;PEND;-;j0\n222;RUN;-;j1\n"

    def runner(argv, timeout, cwd=None):
        fmt = argv[argv.index("-o") + 1]
        if "run_time" in fmt:            # 확장 포맷 거부
            return CommandResult(255, "", "bjobs: Unknown field: run_time\n")
        return CommandResult(0, core, "")

    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), runner)
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

    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), runner)
    with pytest.raises(LsfCommandError):
        cmd._bjobs(["111"])
    assert cmd._bjobs_fmt is cmd._BJOBS_FULL_FMT      # 강등 안 됨


# ----------------------------------------------------------------------
# bkill
# ----------------------------------------------------------------------
def test_bkill_targets_chunked(fake_lsf):
    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, chunk_size=20), fake_lsf)
    ids = [_submit(cmd, f"r {i}") for i in range(45)]
    resolved, calls = cmd.bkill_targets_confirm([str(i) for i in ids])
    assert calls == 3
    assert resolved == {str(i) for i in ids}
    assert fake_lsf.alive_jobs() == []


def test_bkill_no_matching_job_is_ok(cmd):
    # 이미 없는 job kill — no-match는 '해소'로 분류 (재시도 불필요)
    resolved, calls = cmd.bkill_targets_confirm(["999999"])
    assert "999999" in resolved and calls == 1


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
