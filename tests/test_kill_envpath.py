"""kill 시 envpath — MC forward job을 그 클러스터 env를 source한 bkill로 죽인다.

job마다 forward된 클러스터가 다를 수 있어, 호출자가 forward_cluster로 분류한 뒤
클러스터별로 mgr.kill_jobs(js, keys, envpath="/path/clusterX/cshrc.lsf")를 호출한다.
그러면 `tcsh -c "source <envpath> && exec bkill <ids>"` 로 실행된다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.command import LsfCommand, CommandResult
from lsfmgr.states import JobState


CSHRC = "/user/mcr1spool/lsfmcr1/conf/cshrc.lsf"


@pytest.fixture
def config(tmp_path):
    return LsfConfig(retry_delay_s=0.05, kill_retry_delay_s=0.02,
                     script_dir=str(tmp_path / "s"))


# ----------------------------------------------------------------------
# 명령 형태 — envpath 있으면 tcsh -c "source ... && exec bkill", 없으면 plain
# ----------------------------------------------------------------------
def test_bkill_argv_with_envpath():
    calls = []
    def runner(argv, timeout):
        calls.append(argv)
        return CommandResult(0, "Job <100> is being terminated\n", "")
    cmd = LsfCommand(LsfConfig(), runner)
    cmd.bkill_targets(["100", "101"], envpath=CSHRC)
    assert calls[-1] == ["tcsh", "-c",
                         f"source {CSHRC} && set noglob && exec bkill 100 101"]


def test_bkill_argv_array_element_noglob():
    """array element("id[idx]")는 대괄호가 tcsh globbing되지 않게 set noglob."""
    calls = []
    def runner(argv, timeout):
        calls.append(argv)
        return CommandResult(0, "Job <1000[2]> is being terminated\n", "")
    cmd = LsfCommand(LsfConfig(), runner)
    cmd.bkill_targets_confirm(["1000[2]", "1000[3]"], envpath=CSHRC)
    inner = calls[-1][2]
    assert "set noglob" in inner
    assert inner.endswith("exec bkill 1000[2] 1000[3]")


def test_bkill_argv_no_envpath_plain():
    calls = []
    def runner(argv, timeout):
        calls.append(argv); return CommandResult(0, "", "")
    cmd = LsfCommand(LsfConfig(), runner)
    cmd.bkill_targets(["100"])                       # envpath 없음
    assert calls[-1] == ["bkill", "100"]


# ----------------------------------------------------------------------
# 선택 kill — forward job은 envpath source해야 죽는다
# ----------------------------------------------------------------------
def test_kill_jobs_envpath_kills_forwarded(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True                # 로컬 bkill로는 안 죽음
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True, kill_status_policy="actual")
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a", "echo b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())

        # envpath 없이 죽이면 forward job이 안 죽음(문제 재현)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs(js, keys[:1])
        assert len(fake_lsf.alive_jobs()) == 2       # 안 죽음

        # envpath 주면 죽음
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill_jobs(js, keys, envpath=CSHRC, verify=True)
        assert fake_lsf.alive_jobs() == []           # sourced bkill로 죽음
        assert b.args[1].still_alive == 0
        assert any(c[0] == "tcsh" and CSHRC in c[2]
                   for c in fake_lsf.calls_of("tcsh"))
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 여러 클러스터 섞임 — 호출자가 분류해 각 envpath로 나눠 호출
# ----------------------------------------------------------------------
def test_multi_cluster_split_by_caller(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b", "c"], auto_poll=False)
        recs = sorted(js.jobs(), key=lambda r: r.job_key)
        clusters = ["busan", "busan", "daegu"]
        profiles = {"busan": "/lsf/busan/cshrc.lsf",
                    "daegu": "/lsf/daegu/cshrc.lsf"}
        for r, c in zip(recs, clusters):
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = c
        mgr.querier.query(js.id)

        # 호출자: forward_cluster별로 분류해 각 envpath로 kill
        by_cluster = {}
        for r in js.jobs():
            by_cluster.setdefault(r.forward_cluster, []).append(r.job_key)
        for cluster, keys in by_cluster.items():
            with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
                mgr.kill_jobs(js, keys, envpath=profiles[cluster])
        assert fake_lsf.alive_jobs() == []           # 전 클러스터 job 죽음
        # 각 클러스터 cshrc가 실제로 source됐는지
        srcs = [c[2] for c in fake_lsf.calls_of("tcsh")]
        assert any("/lsf/busan/cshrc.lsf" in s for s in srcs)
        assert any("/lsf/daegu/cshrc.lsf" in s for s in srcs)
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# whole-jobset kill + envpath (단일 클러스터) — id 기반 sourced
# ----------------------------------------------------------------------
def test_whole_kill_envpath(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill(js, envpath=CSHRC)
        assert fake_lsf.alive_jobs() == []
        assert any("chunk(sourced)" in s for s in b.args[1].strategies)
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# envpath 없으면 기존 group 전략 그대로 (회귀)
# ----------------------------------------------------------------------
def test_no_envpath_keeps_group_strategy(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["a", "b"], auto_poll=False)
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as b:
        manager.kill(js)
    assert fake_lsf.alive_jobs() == []
    assert any("group:" in s for s in b.args[1].strategies)
    assert not fake_lsf.calls_of("tcsh")


# ----------------------------------------------------------------------
# array element를 envpath로 kill — 대괄호 target이 globbing 없이 그 element만
# ----------------------------------------------------------------------
def test_kill_array_element_envpath(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    try:
        # v9: array는 wrapper 제출 산물로만 존재 — 레코드/LSF 수동 구성
        from tests.fake_lsf import FakeJob
        from lsfmgr import JobRecord

        js = mgr.create_jobset(intended_count=3)
        jsid, aid = js.id, 9300
        mgr.store.store_add_jobs([JobRecord(
            job_id=aid, array_index=i, jobset_id=jsid,
            lsf_job_name=f"{jsid}[{i}]", state=JobState.RUN, command="r")
            for i in (1, 2, 3)])
        for i in (1, 2, 3):
            fake_lsf.jobs[f"{aid}[{i}]"] = FakeJob(
                job_id=aid, array_index=i, name=f"{jsid}[{i}]", group=None,
                queue="q", command="r", stat="RUN",
                forward_cluster="busan")
        mgr.querier.query(js.id)
        # element 2만 kill (id[idx] target)
        key2 = next(r.job_key for r in js.jobs() if r.array_index == 2)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs(js, [key2], envpath=CSHRC)
        alive_idx = sorted(j.array_index for j in fake_lsf.alive_jobs())
        assert alive_idx == [1, 3]           # element 2만 죽음
        # 명령에 set noglob + id[2] 포함
        assert any("set noglob" in c[2] and f"{aid}[2]" in c[2]
                   for c in fake_lsf.calls_of("tcsh"))
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# optimistic 정책 + envpath — sourced bkill 확인분이 EXIT로 전이
# ----------------------------------------------------------------------
def test_optimistic_exit_with_envpath(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)  # optimistic(기본)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs(js, keys, envpath=CSHRC)
        # optimistic: sourced bkill 확인 → 즉시 EXIT
        assert all(r.state is JobState.EXIT for r in js.jobs())
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# resubmit_jobs + envpath — kill 단계도 sourced bkill (안 주면 좀비+중복 제출)
# ----------------------------------------------------------------------
