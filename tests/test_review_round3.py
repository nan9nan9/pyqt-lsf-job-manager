"""3차 정독 리뷰(병렬 에이전트 교차 검토)에서 발견된 버그 회귀 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobRecord, JobSpec, JobState, LsfJobManager
from tests.conftest import submit_cmds
from tests.test_store_contract import make_job, make_jobset


# ----------------------------------------------------------------------
# R3-1: $LSB_JOBINDEX 뒤에 식별자 문자가 이어지면 셸이 변수명을 흡수
#       ("run_$LSB_JOBINDEX_final" → 미정의 변수 → "run_.sp" 오실행)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# R3-2: 조회 수단 전부 실패(LSF 순단) 시 전원 LOST 확정하던 문제 — 보류해야 함
# ----------------------------------------------------------------------
def test_probe_failure_defers_lost(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, [f"r {i}" for i in range(10)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass

    fake_lsf.fail_all_queries = True             # LSF 순단 시뮬레이션
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(js)
    _, summary = blocker.args
    assert summary.get("LOST", 0) == 0, "순단 1회로 LOST 확정되면 안 됨"
    assert summary["PEND"] == 10                 # 판단 보류 — 상태 유지

    fake_lsf.fail_all_queries = False            # 복구 후 정상 갱신
    fake_lsf.set_all("RUN")
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000) as blocker:
        manager.query_once(js)
    assert blocker.args[1]["RUN"] == 10


def test_real_loss_still_detected_after_recovery(qtbot, manager, fake_lsf):
    """순단 보류가 진짜 소실 감지(FR-4.3)를 막으면 안 된다."""
    js = submit_cmds(manager, [f"r {i}" for i in range(3)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    rec = js.jobs()[0]
    fake_lsf.vanish_job(rec.job_id, in_bhist=False)
    with qtbot.waitSignal(manager.job_lost, timeout=10000):
        manager.query_once(js)
    assert js.summary["LOST"] == 1


# ----------------------------------------------------------------------
# R3-3: kill — 부착물 일부가 예외로 실패하면 covered여도 fallback 필요
#       (merge된 jobset에서 group A 성공 + group B 장애 → B 소속 전원 생존)
# ----------------------------------------------------------------------
def test_kill_falls_back_when_one_attachment_errors(qtbot, manager, fake_lsf):
    a = submit_cmds(manager, [f"a {i}" for i in range(5)],
                       auto_poll=False)
    b = submit_cmds(manager, [f"b {i}" for i in range(5)],
                       auto_poll=False)
    # merge 가드 조건(submit 마감) 자체를 기다린다 — summary가 전원 PEND여도
    # ctx 마감 전이면 merge가 거부되는 창을 피한다 (신호 타이밍 무관)
    qtbot.waitUntil(lambda: not manager.submitter.is_active(a.id)
                    and not manager.submitter.is_active(b.id), timeout=10000)
    merged = a                                    # in-place 흡수 (v9)
    manager.merge(a, b, force=True)                  # PEND 활성 — force로 레코드 흡수

    fake_lsf.fail_next_bkill = 1                 # 첫 group bkill만 장애
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill(merged)
    rpt = blocker.args[1]
    assert rpt.errors, "장애가 errors에 기록되어야 함"
    assert fake_lsf.alive_jobs() == [], \
        "부착물 하나가 장애여도 fallback으로 전원 kill되어야 함"


# ----------------------------------------------------------------------
# R3-4: merge 후 삭제된 원본 jobset을 영구 polling
# ----------------------------------------------------------------------
def test_merge_stops_polling_of_originals(qtbot, manager, fake_lsf):
    a = submit_cmds(manager, [f"a {i}" for i in range(3)])   # AUTO-1
    b = submit_cmds(manager, [f"b {i}" for i in range(3)])
    qtbot.waitUntil(lambda: not manager.submitter.is_active(a.id)
                    and not manager.submitter.is_active(b.id), timeout=10000)
    manager.start_polling(a, 0.1)
    manager.start_polling(b, 0.1)
    qtbot.wait(300)

    errors = []
    manager.error_occurred.connect(lambda j, m: errors.append((j, m)))
    manager.merge(a, b, force=True)                  # source(b) 삭제 + 핸들 파괴
    qtbot.wait(600)                              # 몇 polling 주기 경과
    assert errors == [], f"삭제된 원본 polling으로 error 발생: {errors}"
    assert a.summary["total"] == 6


# ----------------------------------------------------------------------
# R3-5: shutdown 시 RETRY_WAIT 잔류 — SUBMIT_FAILED 확정 + finished 발행
# ----------------------------------------------------------------------
def test_shutdown_finalizes_pending_retries(qtbot, fake_lsf, config):
    from lsfmgr import InMemoryStore
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    fake_lsf.fail_next_bsub = 99
    reports = []
    mgr.submit_finished.connect(lambda j, r: reports.append(r))
    # 긴 retry delay — shutdown 시점에 RETRY_WAIT로 잔류하도록
    jsid = submit_cmds(mgr, [JobSpec(command="x")], max_retry=5).id
    mgr._defaults["retry_backoff"] = "fixed:30"  # (다음 retry만 느리게)
    qtbot.waitUntil(
        lambda: any(r.state is JobState.RETRY_WAIT
                    for r in mgr.get_jobs(jsid)), timeout=10000)

    mgr.shutdown()
    recs = mgr.get_jobs(jsid)
    assert recs[0].state is JobState.SUBMIT_FAILED, \
        "shutdown 후 RETRY_WAIT가 비terminal로 영구 잔류"
    assert reports and reports[-1].failed == 1   # finished도 발행됨


# ----------------------------------------------------------------------
# R3-6: mode="array" 강제 시 JobSpec 옵션 소실 방지
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# R3-7: JobSpec.env가 조용히 무시되던 문제 — bsub -env로 전달
# ----------------------------------------------------------------------
def test_jobspec_env_passed_to_bsub(qtbot, manager, fake_lsf):
    spec = JobSpec(command="sim.sh", env=(("OMP_NUM_THREADS", "4"),))
    js = submit_cmds(manager, [spec], auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    argv = fake_lsf.calls_of("bsub")[0]
    assert "-env" in argv
    assert "OMP_NUM_THREADS=4" in argv[argv.index("-env") + 1]
    job = list(fake_lsf.jobs.values())[0]
    assert "OMP_NUM_THREADS=4" in job.env


# ----------------------------------------------------------------------
# R3-8: 빈 jobset / cancel로 CREATED만 잔존 시 polling 영구 지속 (AUTO-2 확장)
# ----------------------------------------------------------------------
def test_polling_autostops_on_empty_jobset(qtbot, manager, fake_lsf):
    js = manager.create_jobset()          # v9: 빈 jobset은 생성만 가능
    updates = []
    js.jobset_updated.connect(lambda s: updates.append(s))
    manager.start_polling(js, 0.1)
    qtbot.waitUntil(lambda: len(updates) >= 2, timeout=10000)
    qtbot.wait(500)                              # idle 2사이클 후 자동 중지
    n = len(updates)
    qtbot.wait(400)
    assert len(updates) == n, "빈 jobset polling이 자동 중지되지 않음"


# ----------------------------------------------------------------------
# R3-9: merge job_key 충돌 시 silent overwrite → 선검사로 거부
# ----------------------------------------------------------------------
def test_merge_rejects_duplicate_job_keys(qtbot, manager, fake_lsf):
    a = submit_cmds(manager, ["a 1", "a 2"], auto_poll=False)
    b = submit_cmds(manager, ["b 1"], auto_poll=False)
    qtbot.waitUntil(lambda: not manager.submitter.is_active(a.id)
                    and not manager.submitter.is_active(b.id), timeout=10000)
    # b에 a의 job_key와 동명인 레코드를 수동 편입 — 충돌 시나리오
    dup_key = a.jobs()[0].job_key
    manager.store.add_job(JobRecord(
        job_id=None, array_index=None, jobset_id=b.id,
        lsf_job_name=dup_key, state=JobState.CREATED, command=""))
    with pytest.raises(ValueError, match="충돌"):
        # dup_key(merge_id 없음 → 신규 추가 경로)가 양쪽에 존재 — force로
        # 활성 가드를 지나도 key 충돌은 거부된다
        manager.merge(a.id, b.id, force=True)


# ----------------------------------------------------------------------
# R3-10: get_jobs(states=빈 set) 계약 — 두 백엔드 모두 0건
# ----------------------------------------------------------------------
def test_get_jobs_empty_states_contract(store):
    store.create_jobset(make_jobset(n=2))
    store.add_job(make_job(idx=0, state=JobState.PEND, job_id=1))
    assert store.get_jobs("js1", states=set()) == []
    assert len(store.get_jobs("js1", states=None)) == 1


# ----------------------------------------------------------------------
# R3-11: add_jobs 부분 적용 방지 — 실패 시 전량 미반영 (두 백엔드 계약)
# ----------------------------------------------------------------------
def test_add_jobs_atomic_on_failure(store):
    from lsfmgr.errors import JobSetNotFoundError
    store.create_jobset(make_jobset(n=2))
    with pytest.raises(JobSetNotFoundError):
        store.add_jobs([make_job(idx=0), make_job(jsid="nope", idx=1)])
    assert store.get_jobs("js1") == [], "실패한 배치의 일부가 반영됨"


# ----------------------------------------------------------------------
# R3-12: bhist가 array element를 구분 못해 전 element에 동일 상태 오기록
# ----------------------------------------------------------------------
def test_array_bhist_fallback_per_element(qtbot, manager, fake_lsf):
    """array element별 bhist fallback — element 단위 (id,idx) 키로 최종
    상태가 구분돼야 한다 (v9: 레코드/LSF 수동 구성)."""
    from tests.fake_lsf import FakeJob

    js = manager.create_jobset(intended_count=3)
    jsid, parent = js.id, 9100
    manager.store.add_jobs([JobRecord(
        job_id=parent, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]", state=JobState.RUN, command="r")
        for i in (1, 2, 3)])
    stats = {1: ("DONE", 0), 2: ("EXIT", 9), 3: ("DONE", 0)}
    for i, (st, ec) in stats.items():
        fake_lsf.jobs[f"{parent}[{i}]"] = FakeJob(
            job_id=parent, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat=st, exit_code=ec,
            vanished=True, in_bhist=True)      # bjobs 소실 → bhist fallback

    manager.querier.query(jsid)

    got = {r.array_index: (r.state, r.exit_code)
           for r in manager.get_jobs(jsid)}
    assert got[1] == (JobState.DONE, 0)
    assert got[2] == (JobState.EXIT, 9)
    assert got[3] == (JobState.DONE, 0)

# ----------------------------------------------------------------------
# R3-13: SQLite commit 실패 시 pending 잔존 → 다음 commit에 유령 반영
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# R3-14: transition으로 키 필드 변경 시 이중 계상 → 거부
# ----------------------------------------------------------------------
def test_transition_rejects_key_fields(store):
    store.create_jobset(make_jobset(n=1))
    store.add_job(make_job(idx=0))
    with pytest.raises(ValueError):
        store.transition("js1", "js1_0", JobState.PEND,
                         lsf_job_name="other")
    # jobset_id는 위치 인자와 충돌해 Python 수준(TypeError)에서 원천 차단됨
    with pytest.raises(TypeError):
        store.transition("js1", "js1_0", JobState.PEND, jobset_id="js2")


# ----------------------------------------------------------------------
# R3-15: bmod/bgdel timeout이 호출자로 전파되던 문제 — 경고 후 진행
# ----------------------------------------------------------------------
def test_bmod_bgdel_timeout_swallowed(fake_lsf):
    import subprocess
    from lsfmgr.command import LsfCommand
    from lsfmgr.config import LsfConfig

    def timeout_runner(argv, timeout):
        raise subprocess.TimeoutExpired(argv, timeout)

    cmd = LsfCommand(LsfConfig(), timeout_runner)
    cmd.bmod_group([1, 2], "/g/x")               # 예외 없이 경고만
    cmd.bgdel("/g/x")
