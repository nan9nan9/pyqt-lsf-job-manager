"""MC 분류 kill — forward job을 그 클러스터 env를 source한 bkill로 죽인다.

매핑은 **생성자 옵션**이다(앱 환경 속성이라 호출마다 주지 않는다):

    LsfJobManager(cluster_envpaths={"clusterA": "/path/a/cshrc.lsf",
                                    "*": "/path/default/cshrc.lsf"})

kill 시 라이브러리가 대상의 cluster를 확인해(미상이면 bkill 직전에 최소
포맷으로 1회 조회) 클러스터별로 나눠 `tcsh -c "source <cshrc> && exec bkill
<ids>"`를 실행한다. 미상/미매핑은 `"*"`(없으면 plain bkill).
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.conftest import mk_jobset, submit_cmds
from lsfmgr.command import LsfCommand, CommandResult
from lsfmgr.states import JobState


CSHRC = "/user/mcr1spool/lsfmcr1/conf/cshrc.lsf"


@pytest.fixture
def config(tmp_path):
    return LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05, kill_retry_delay_s=0.02)


# ----------------------------------------------------------------------
# 명령 형태 — envpath 있으면 tcsh -c "source ... && exec bkill", 없으면 plain
# ----------------------------------------------------------------------
def test_bkill_argv_with_envpath():
    calls = []
    def runner(argv, timeout, cwd=None):
        calls.append(argv)
        return CommandResult(0, "Job <100> is being terminated\n", "")
    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), runner)
    cmd.bkill_targets_confirm(["100", "101"], envpath=CSHRC)
    assert calls[-1] == ["tcsh", "-c",
                         f"source {CSHRC} && set noglob && exec bkill 100 101"]


def test_bkill_argv_array_element_noglob():
    """array element("id[idx]")는 대괄호가 tcsh globbing되지 않게 set noglob."""
    calls = []
    def runner(argv, timeout, cwd=None):
        calls.append(argv)
        return CommandResult(0, "Job <1000[2]> is being terminated\n", "")
    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), runner)
    cmd.bkill_targets_confirm(["1000[2]", "1000[3]"], envpath=CSHRC)
    inner = calls[-1][2]
    assert "set noglob" in inner
    assert inner.endswith("exec bkill 1000[2] 1000[3]")


def test_bkill_argv_no_envpath_plain():
    calls = []
    def runner(argv, timeout, cwd=None):
        calls.append(argv); return CommandResult(0, "", "")
    cmd = LsfCommand(LsfConfig(rate_limit_per_s=None, ), runner)
    cmd.bkill_targets_confirm(["100"])               # envpath 없음
    assert calls[-1] == ["bkill", "100"]


# ----------------------------------------------------------------------
# 선택 kill — forward job은 envpath source해야 죽는다
# ----------------------------------------------------------------------
def test_kill_jobs_envpath_kills_forwarded(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True                # 로컬 bkill로는 안 죽음
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True, kill_status_policy="actual",
                        cluster_envpaths={"busan": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a", "echo b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())

        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill_jobs(js, keys, verify=True)
        assert fake_lsf.alive_jobs() == []           # sourced bkill로 죽음
        assert b.args[1].still_alive == 0
        assert any(c[0] == "tcsh" and CSHRC in c[2]
                   for c in fake_lsf.calls_of("tcsh"))
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 여러 클러스터 섞임 — 라이브러리가 클러스터별로 나눠 각 env로 kill
# ----------------------------------------------------------------------
def test_multi_cluster_split_by_library(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True,
                        cluster_envpaths={"busan": "/lsf/busan/cshrc.lsf",
                                          "daegu": "/lsf/daegu/cshrc.lsf"})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b", "c"], auto_poll=False)
        recs = sorted(js.jobs(), key=lambda r: r.job_key)
        clusters = ["busan", "busan", "daegu"]
        for r, c in zip(recs, clusters):
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = c
        mgr.querier.query(js.id)

        # 한 번의 kill로 라이브러리가 클러스터별로 나눠 실행한다
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill(js)
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
                        collect_clusters=True,
                        cluster_envpaths={"*": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as b:
            mgr.kill(js)
        assert fake_lsf.alive_jobs() == []
        assert "sourced" in b.args[1].strategies
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 제출 직후(관측 0) 즉시 kill — bkill 직전 cluster 조회로 분류
# ----------------------------------------------------------------------
def test_cluster_envpaths_kill_right_after_submit(qtbot, fake_lsf, config):
    """사용자 시나리오(MC): 제출 직후 폴링 전이라 레코드에 cluster가 없다 —
    kill은 bkill **직전에** 미상 대상만 최소 포맷으로 조회해 cluster를 채우고,
    forward job은 그 클러스터 env를 source한 bkill로 죽인다."""
    from lsfmgr import InMemoryStore, LsfJobManager
    fake_lsf.forward_needs_env = True        # forward job은 sourced bkill만 유효
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True,
                        cluster_envpaths={"cluster_busan": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"m {i}" for i in range(4)],
                             auto_poll=False)
        # LSF상 전원 RUN + 일부 forward — 아직 **폴링 전**이라 레코드는 미상
        for i, r in enumerate(js.jobs()):
            fj = fake_lsf.jobs[str(r.job_id)]
            fj.stat = "RUN"
            if i < 2:
                fj.forward_cluster = "cluster_busan"
        assert all(r.forward_cluster is None for r in js.jobs())

        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as blk:
            mgr.kill(js)
        report = blk.args[1]
        assert report.unconfirmed == 0, report.errors
        assert fake_lsf.alive_jobs() == []          # forward 포함 전원 사망
        # forward 2건은 tcsh(source) 경유, 로컬 2건은 plain bkill
        assert len(fake_lsf.calls_of("tcsh")) == 1
        assert any(c[0].endswith("bkill") for c in fake_lsf.calls)
        # cluster 조회(bjobs)가 bkill보다 먼저 나갔다
        order = [c[0].rsplit("/", 1)[-1] for c in fake_lsf.calls]
        assert order.index("bjobs") < min(
            i for i, p_ in enumerate(order) if p_ in ("bkill", "tcsh"))
    finally:
        mgr.shutdown()


def test_cluster_envpaths_raw_ids_without_jobset(qtbot, fake_lsf, config):
    """jobset 컨텍스트 없는 원시 id kill도 대상 id를 조회해 cluster를
    알아내 분류 kill한다 (store 레코드 불요)."""
    from lsfmgr import InMemoryStore, LsfJobManager
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True,
                        cluster_envpaths={"cluster_busan": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["w 0", "w 1"], auto_poll=False)
        ids = [r.job_id for r in js.jobs()]
        for jid in ids:                          # LSF상 forward된 RUN
            fake_lsf.jobs[str(jid)].stat = "RUN"
            fake_lsf.jobs[str(jid)].forward_cluster = "cluster_busan"
        fake_lsf.calls.clear()
        with qtbot.waitSignal(mgr.killer.finished, timeout=10000) as blk:
            # jobset_id 없이 — 레코드 매핑이 아닌 직접 관측으로 분류돼야 함
            mgr.killer.kill_jobs(ids)
        assert blk.args[1].unconfirmed == 0
        assert fake_lsf.alive_jobs() == []
        order = [c[0].rsplit("/", 1)[-1] for c in fake_lsf.calls]
        assert "bjobs" in order and "tcsh" in order
        assert order.index("bjobs") < order.index("tcsh")   # 조회가 kill 선행
    finally:
        mgr.shutdown()


def test_cluster_envpaths_unknown_falls_back_to_default(qtbot, fake_lsf,
                                                        config):
    """조회 후에도 cluster 미상(매핑에 없는 클러스터/로컬)이면 기본 env로
    kill — kill 자체는 반드시 나간다."""
    from lsfmgr import InMemoryStore, LsfJobManager
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True,
                        cluster_envpaths={"cluster_x": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["u 0"], auto_poll=False)
        fake_lsf.set_all("RUN")                  # cluster 없음(로컬)
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000) as blk:
            mgr.kill(js)
        assert blk.args[1].unconfirmed == 0
        assert fake_lsf.alive_jobs() == []
        assert not fake_lsf.calls_of("tcsh")     # 미상 → 기본(plain) bkill
    finally:
        mgr.shutdown()


def test_no_envpath_uses_plain_bkill(qtbot, manager, fake_lsf):
    """envpath 미지정이면 tcsh source 없이 plain bkill chunk로 죽인다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["a", "b"], auto_poll=False)
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as b:
        manager.kill(js)
    assert fake_lsf.alive_jobs() == []
    assert "chunk" in b.args[1].strategies
    assert not fake_lsf.calls_of("tcsh")


# ----------------------------------------------------------------------
# array element를 envpath로 kill — 대괄호 target이 globbing 없이 그 element만
# ----------------------------------------------------------------------
def test_kill_array_element_envpath(qtbot, fake_lsf, config):
    fake_lsf.forward_needs_env = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True,
                        cluster_envpaths={"*": CSHRC})
    try:
        # v9: array는 wrapper 제출 산물로만 존재 — 레코드/LSF 수동 구성
        from tests.fake_lsf import FakeJob
        from lsfmgr import JobRecord

        js = mk_jobset(mgr, intended_count=3)
        jsid, aid = js.id, 9300
        mgr.store.store_add_jobs([JobRecord(
            job_id=aid, array_index=i, jobset_id=jsid,
            job_key=f"{jsid}[{i}]", state=JobState.RUN, command="r")
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
            mgr.kill_jobs(js, [key2])
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
                        collect_clusters=True,   # optimistic(기본)
                        cluster_envpaths={"*": CSHRC})
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["a", "b"], auto_poll=False)
        for r in js.jobs():
            fake_lsf.jobs[str(r.job_id)].stat = "RUN"
            fake_lsf.jobs[str(r.job_id)].forward_cluster = "busan"
        mgr.querier.query(js.id)
        keys = sorted(r.job_key for r in js.jobs())
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs(js, keys)
        # optimistic: sourced bkill 확인 → 즉시 EXIT
        assert all(r.state is JobState.EXIT for r in js.jobs())
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# resubmit_jobs + envpath — kill 단계도 sourced bkill (안 주면 좀비+중복 제출)
# ----------------------------------------------------------------------
