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
# bsub
# ----------------------------------------------------------------------
def test_bsub_parses_job_id(cmd, fake_lsf):
    jid = cmd.bsub("echo hi", queue="normal", job_name="t_0",
                   group_path="/lsfmgr/u/t")
    assert jid == 1000
    job = fake_lsf.jobs["1000"]
    assert job.name == "t_0"
    assert job.group == "/lsfmgr/u/t"
    assert job.queue == "normal"


def test_bsub_failure_classified(cmd, fake_lsf):
    fake_lsf.fail_next_bsub = 1
    with pytest.raises(SubmitError) as ei:
        cmd.bsub("echo hi")
    assert ei.value.fail_reason == "BSUB_EXIT_1"


def test_bsub_no_jobid_parsed(cmd, fake_lsf):
    fake_lsf.no_jobid_next_bsub = 1
    with pytest.raises(SubmitError) as ei:
        cmd.bsub("echo hi")
    assert ei.value.fail_reason == "NO_JOBID_PARSED"


def test_bsub_group_rejected_retries_without_attachment(cmd, fake_lsf):
    """FR-1.4 — 부착물 지정 실패해도 submit은 진행."""
    fake_lsf.reject_group = True
    jid = cmd.bsub("echo hi", job_name="t_0", group_path="/bad/group")
    assert jid == 1000
    assert fake_lsf.jobs["1000"].group is None


def test_bsub_timeout():
    import subprocess

    def timeout_runner(argv, timeout):
        raise subprocess.TimeoutExpired(argv, timeout)

    cmd = LsfCommand(LsfConfig(), timeout_runner)
    with pytest.raises(SubmitError) as ei:
        cmd.bsub("echo hi")
    assert ei.value.fail_reason == "BSUB_TIMEOUT"


def test_bsub_arg_max(cmd):
    with pytest.raises(ArgMaxExceededError):
        cmd.bsub("x" * 200000)


# ----------------------------------------------------------------------
# bjobs
# ----------------------------------------------------------------------
def test_bjobs_by_group(cmd, fake_lsf):
    for i in range(3):
        cmd.bsub(f"run {i}", job_name=f"t_{i}", group_path="/g/a")
    cmd.bsub("other", job_name="x_0", group_path="/g/b")
    out = cmd.bjobs_by_group("/g/a")
    assert len(out) == 3
    assert {s.job_name for s in out} == {"t_0", "t_1", "t_2"}


def test_bjobs_by_name_pattern(cmd, fake_lsf):
    for i in range(3):
        cmd.bsub(f"run {i}", job_name=f"t_{i}")
    cmd.bsub("other", job_name="x_0")
    out = cmd.bjobs_by_name("t_*")
    assert len(out) == 3


def test_bjobs_by_ids_chunked(fake_lsf):
    cfg = LsfConfig(chunk_size=10)
    cmd = LsfCommand(cfg, fake_lsf)
    ids = [cmd.bsub(f"run {i}") for i in range(25)]
    out = cmd.bjobs_by_ids(ids)
    assert len(out) == 25
    # 25개 / chunk 10 → bjobs 3회
    assert len(fake_lsf.calls_of("bjobs")) == 3


def test_bjobs_empty_result(cmd):
    assert cmd.bjobs_by_group("/none") == []


def test_bjobs_array_elements(cmd, fake_lsf):
    jid = cmd.bsub("run.sh", job_name="arr[1-5]")
    out = cmd.bjobs_by_ids([jid])
    assert len(out) == 5
    assert {s.array_index for s in out} == {1, 2, 3, 4, 5}
    assert all(s.job_id == jid for s in out)


def test_bjobs_exit_code_parsing(cmd, fake_lsf):
    jid = cmd.bsub("run")
    fake_lsf.set_job(jid, "EXIT", exit_code=42)
    out = cmd.bjobs_by_ids([jid])
    assert out[0].state is JobState.EXIT
    assert out[0].exit_code == 42


# ----------------------------------------------------------------------
# bkill
# ----------------------------------------------------------------------
def test_bkill_group_single_call(cmd, fake_lsf):
    for i in range(50):
        cmd.bsub(f"r {i}", group_path="/g/kill")
    calls = cmd.bkill_by_group("/g/kill")
    assert calls == 1
    assert fake_lsf.alive_jobs() == []


def test_bkill_by_ids_chunked(fake_lsf):
    cmd = LsfCommand(LsfConfig(chunk_size=20), fake_lsf)
    ids = [cmd.bsub(f"r {i}") for i in range(45)]
    calls = cmd.bkill_by_ids(ids)
    assert calls == 3
    assert fake_lsf.alive_jobs() == []


def test_bkill_no_matching_job_is_ok(cmd):
    # 이미 종료된 job kill은 에러 아님
    cmd.bkill_by_group("/empty")


# ----------------------------------------------------------------------
# bhist
# ----------------------------------------------------------------------
def test_bhist_states(cmd, fake_lsf):
    j1 = cmd.bsub("a")
    j2 = cmd.bsub("b")
    fake_lsf.set_job(j1, "DONE", 0)
    fake_lsf.set_job(j2, "EXIT", 7)
    fake_lsf.vanish_job(j1)
    fake_lsf.vanish_job(j2)
    hist = cmd.bhist_states([j1, j2])
    assert hist[j1] == (JobState.DONE, 0)
    assert hist[j2] == (JobState.EXIT, 7)


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
