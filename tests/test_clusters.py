"""LSF MultiCluster forwarding 정보 수집 (collect_clusters).

켜면 bjobs -o에 source_cluster·forward_cluster 필드를 추가해 JobRecord에
채운다. 기본은 꺼짐(opt-in). MC 미지원 사이트에선 3단 강등(FULL+MC → FULL →
CORE)으로 그 필드만 포기하고 run_time 등은 유지한다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.command import LsfCommand
from lsfmgr.states import JobState


@pytest.fixture
def mc_manager(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    yield mgr
    mgr.shutdown()


def _submit_running(qtbot, mgr, fake_lsf, src=None, fwd=None):
    with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
        js = submit_cmds(mgr, ["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fj = fake_lsf.jobs[str(rec.job_id)]
    fj.stat = "RUN"
    fj.source_cluster, fj.forward_cluster = src, fwd
    return js


# ----------------------------------------------------------------------
# ON — source/forward_cluster가 JobRecord에 채워지고 jobs_updated로 발행
# ----------------------------------------------------------------------
def test_collect_clusters_populates_record(qtbot, mc_manager, fake_lsf):
    js = _submit_running(qtbot, mc_manager, fake_lsf, src="seoul", fwd="busan")
    seen = []
    mc_manager.jobs_updated.connect(lambda j, rs: seen.extend(rs))
    # query_once는 폴링 워커 경유 → jobs_updated 발화
    with qtbot.waitSignal(mc_manager.jobs_updated, timeout=10000):
        mc_manager.query_once(js.id)
    rec = js.jobs()[0]
    assert rec.source_cluster == "seoul"
    assert rec.forward_cluster == "busan"
    assert any(r.forward_cluster == "busan" for r in seen)   # 신호로도 발행


def test_collect_clusters_forward_only(qtbot, mc_manager, fake_lsf):
    """포워딩 안 된 job('-')은 None으로."""
    js = _submit_running(qtbot, mc_manager, fake_lsf, src="seoul", fwd=None)
    mc_manager.querier.query(js.id)
    rec = js.jobs()[0]
    assert rec.source_cluster == "seoul"
    assert rec.forward_cluster is None


# ----------------------------------------------------------------------
# OFF (기본) — 필드 요청도 안 하고 레코드도 안 채워짐
# ----------------------------------------------------------------------
def test_default_off_no_cluster_fields(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fj = fake_lsf.jobs[str(rec.job_id)]
    fj.stat = "RUN"; fj.source_cluster = "seoul"
    fake_lsf.calls.clear()
    manager.querier.query(js.id)
    assert js.jobs()[0].source_cluster is None
    # bjobs -o 포맷에 cluster 필드가 없어야
    for call in fake_lsf.calls_of("bjobs"):
        assert "source_cluster" not in " ".join(call)


# ----------------------------------------------------------------------
# 3단 강등 — MC 필드 미지원이면 FULL로만 내려가 run_time 유지
# ----------------------------------------------------------------------
def test_cluster_field_unsupported_degrades_to_full(qtbot, fake_lsf, config):
    fake_lsf.reject_clusters = True          # MC 필드 요청 시 rc=255
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a"], auto_poll=False)
        rec = js.jobs()[0]
        fj = fake_lsf.jobs[str(rec.job_id)]
        fj.stat = "RUN"; fj.run_time_s = 42; fj.source_cluster = "seoul"
        mgr.querier.query(js.id)
        rec = js.jobs()[0]
        assert rec.run_time_s == 42          # FULL 확장 필드는 유지
        assert rec.source_cluster is None    # MC 필드만 포기
        # 포맷이 FULL(인덱스 1)로 강등됐고 MC 필드는 빠졌다
        assert "source_cluster" not in mgr.command._bjobs_fmt
    finally:
        mgr.shutdown()


def test_cluster_degradation_is_permanent(qtbot, fake_lsf, config):
    """한 번 강등되면 이후 사이클엔 MC 필드를 다시 요청하지 않는다."""
    fake_lsf.reject_clusters = True
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        collect_clusters=True)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a"], auto_poll=False)
        fake_lsf.set_all("RUN")
        mgr.querier.query(js.id)             # 여기서 강등
        fake_lsf.calls.clear()
        mgr.querier.query(js.id)             # 이후 사이클
        for call in fake_lsf.calls_of("bjobs"):
            assert "source_cluster" not in " ".join(call)
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# sqlite 영속 — 클러스터 필드 저장/복원
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 파서 단위 — 10필드(FULL+MC) / 8필드(FULL) / 4필드(CORE)
# ----------------------------------------------------------------------
def test_parse_bjobs_cluster_fields():
    line10 = "1000;RUN;-;js_0;120 second(s);-;-;/work;seoul;busan"
    (st,) = LsfCommand._parse_bjobs(line10 + "\n")
    assert st.source_cluster == "seoul" and st.forward_cluster == "busan"
    assert st.run_time_s == 120 and st.working_dir == "/work"

    line8 = "1000;RUN;-;js_0;120 second(s);-;-;/work"
    (st8,) = LsfCommand._parse_bjobs(line8 + "\n")
    assert st8.source_cluster is None and st8.run_time_s == 120

    line4 = "1000;RUN;-;js_0"
    (st4,) = LsfCommand._parse_bjobs(line4 + "\n")
    assert st4.source_cluster is None and st4.run_time_s is None


# ----------------------------------------------------------------------
# resubmit — 재제출 시 이전 실행의 클러스터 정보가 초기화된다
# ----------------------------------------------------------------------
def test_full_resubmit_clears_cluster(qtbot, mc_manager, fake_lsf):
    js = _submit_running(qtbot, mc_manager, fake_lsf, src="seoul", fwd="busan")
    mc_manager.querier.query(js.id)
    rec = js.jobs()[0]
    assert rec.forward_cluster == "busan"
    # v9: 살아있는 job은 먼저 kill(GUI 직접 제어) → 종료 후 전체 재제출.
    # 재제출 리셋이 이전 클러스터 흔적을 지워야 한다
    with qtbot.waitSignal(mc_manager.kill_finished, timeout=10000):
        mc_manager.kill(js)
    with qtbot.waitSignal(mc_manager.submit_finished, timeout=10000):
        mc_manager.submit(js)
    rec = js.jobs()[0]
    assert rec.state is JobState.PEND
    assert rec.source_cluster is None
    assert rec.forward_cluster is None


# ----------------------------------------------------------------------
# 2단 강등 — MC + run_time 둘 다 미지원이면 한 호출에서 CORE까지 (개선)
# ----------------------------------------------------------------------
def test_double_field_error_degrades_to_core():
    from lsfmgr.command import LsfCommand, CommandResult

    def runner(argv, timeout):
        fmt = argv[argv.index("-o") + 1]
        if "source_cluster" in fmt:
            return CommandResult(255, "", "bad field name: source_cluster\n")
        if "exec_cwd" in fmt:
            return CommandResult(255, "", "Unknown field: exec_cwd\n")
        return CommandResult(0, "111;RUN;-;j0\n", "")

    cmd = LsfCommand(LsfConfig(collect_clusters=True), runner)
    out = cmd.bjobs_by_group("/g")           # 한 호출에서 CORE까지 강등
    assert cmd._bjobs_fmt is cmd._BJOBS_CORE_FMT
    assert out[0].job_id == 111


# ----------------------------------------------------------------------
# wrapper가 제출한 array가 forward되면 집계 레코드에 클러스터가 실린다
# (예전엔 _aggregate_elements가 cluster 필드를 안 넣어 소실됐다)
# ----------------------------------------------------------------------
def test_wrapper_array_aggregate_carries_cluster(qtbot, mc_manager, fake_lsf):
    with qtbot.waitSignal(mc_manager.submit_finished, timeout=10000):
        js = submit_cmds(mc_manager, 
            [["customwrapper_sub", "-J", "arr[1-3]", "echo", "hi"]], auto_poll=False, wrapper=True)
    rec = js.jobs()[0]
    aid = rec.job_id
    for i in (1, 2, 3):
        fj = fake_lsf.jobs[f"{aid}[{i}]"]
        fj.stat = "RUN"
        fj.source_cluster, fj.forward_cluster = "seoul", "busan"
    mc_manager.querier.query(js.id)
    rec = js.jobs()[0]
    assert rec.state is JobState.RUN
    assert rec.source_cluster == "seoul"
    assert rec.forward_cluster == "busan"


# ----------------------------------------------------------------------
# collect_clusters=False 폴링이 저장된 클러스터 필드를 덮어 소실시키지 않음
# (persistent+recover: 이전 세션이 채운 forward_cluster를 보존)
# ----------------------------------------------------------------------
def test_off_polling_preserves_stored_cluster(qtbot, fake_lsf, tmp_path):
    store = InMemoryStore()
    # collect off 매니저지만 store엔 이미 forward_cluster가 채워진 job이 있다
    mgr = LsfJobManager(store=store, config=LsfConfig(), runner=fake_lsf)  # off
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo a"], auto_poll=False)
        rec = js.jobs()[0]
        # 이전 세션이 채운 것처럼 store에 직접 클러스터 주입 + LSF도 RUN
        store.transition(js.id, rec.job_key, JobState.RUN,
                         forward_cluster="busan", source_cluster="seoul")
        fake_lsf.jobs[str(rec.job_id)].stat = "RUN"
        fake_lsf.jobs[str(rec.job_id)].run_time_s = 33   # 다른 필드 변화 유발
        # collect off 폴링 — 클러스터 필드를 건드리면 안 됨
        mgr.querier.query(js.id)
        r = js.jobs()[0]
        assert r.forward_cluster == "busan"      # 보존
        assert r.source_cluster == "seoul"
        # 상태 전이(RUN→DONE) 시에도 보존
        fake_lsf.set_job(rec.job_id, "DONE")
        mgr.querier.query(js.id)
        r = js.jobs()[0]
        assert r.state is JobState.DONE and r.forward_cluster == "busan"
    finally:
        mgr.shutdown()
