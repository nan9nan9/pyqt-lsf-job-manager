"""3차 정독 리뷰(병렬 에이전트 교차 검토)에서 발견된 버그 회귀 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobRecord, JobState, LsfJobManager
from tests.conftest import mk_jobset, submit_cmds
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
    """순단 보류가 진짜 소실 감지를 막으면 안 된다."""
    js = submit_cmds(manager, [f"r {i}" for i in range(3)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    rec = js.jobs()[0]
    fake_lsf.vanish_job(rec.job_id)
    grace = manager.command.config.lost_after_missing_polls
    for _ in range(grace - 1):                   # 유예 사이클 소진
        manager.querier.query(js.id)
    with qtbot.waitSignal(manager.job_lost, timeout=10000):
        manager.query_once(js)
    assert js.summary["LOST"] == 1


# ----------------------------------------------------------------------
# (R3-3: 부착물 일부 실패 fallback — v10에서 kill tier 삭제로 시나리오 소멸)
# ----------------------------------------------------------------------
# R3-4: 삭제된 jobset을 영구 polling (merge source였던 시나리오 → remove_jobset)
# ----------------------------------------------------------------------
def test_merge_stops_polling_of_originals(qtbot, manager, fake_lsf):
    a = submit_cmds(manager, [f"a {i}" for i in range(3)])   # auto_poll 기본
    b = submit_cmds(manager, [f"b {i}" for i in range(3)])
    qtbot.waitUntil(lambda: not manager.submitter.is_active(a.id)
                    and not manager.submitter.is_active(b.id), timeout=10000)
    manager.start_polling(a, 0.1)
    manager.start_polling(b, 0.1)
    qtbot.wait(300)

    errors = []
    manager.error_occurred.connect(lambda j, m: errors.append((j, m)))
    manager.remove_jobset(b, force=True)         # b 삭제 + 핸들 파괴
    qtbot.wait(600)                              # 몇 polling 주기 경과
    assert errors == [], f"삭제된 jobset polling으로 error 발생: {errors}"
    assert a.summary["total"] == 3


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
    jsid = submit_cmds(mgr, ["x"], max_retry=5).id
    mgr._defaults["retry_backoff"] = "fixed:30"  # (다음 retry만 느리게)
    qtbot.waitUntil(
        lambda: any(r.state is JobState.RETRY_WAIT
                    for r in mgr.get_jobs(jsid)), timeout=10000)

    mgr.shutdown()
    recs = mgr.get_jobs(jsid)
    assert recs[0].state is JobState.CANCELLED, \
        "shutdown 후 RETRY_WAIT가 비terminal로 영구 잔류"
    assert reports and reports[-1].cancelled == 1   # finished도 발행됨


# ----------------------------------------------------------------------
# R3-6: mode="array" 강제 시 JobSpec 옵션 소실 방지
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# (R3-7: JobSpec.env — v10에서 bsub 조립 경로 삭제로 기능 소멸)
# ----------------------------------------------------------------------
# R3-8: 빈 jobset / cancel로 CREATED만 잔존 시 polling 영구 지속 (자동 중지 확장)
# ----------------------------------------------------------------------
def test_polling_autostops_on_empty_jobset(qtbot, manager, fake_lsf):
    js = mk_jobset(manager)          # v9: 빈 jobset은 생성만 가능
    updates = []
    js.jobset_updated.connect(lambda s: updates.append(s))
    manager.start_polling(js, 0.1)
    qtbot.waitUntil(lambda: len(updates) >= 2, timeout=10000)
    qtbot.wait(500)                              # idle 2사이클 후 자동 중지
    n = len(updates)
    qtbot.wait(400)
    assert len(updates) == n, "빈 jobset polling이 자동 중지되지 않음"


# ----------------------------------------------------------------------
# R3-10: get_jobs(states=빈 set) 계약 — 두 백엔드 모두 0건
# ----------------------------------------------------------------------
def test_get_jobs_empty_states_contract(store):
    store.store_insert_jobset(make_jobset(n=2))
    store.store_add_job(make_job(idx=0, state=JobState.PEND, job_id=1))
    assert store.get_jobs("js1", states=set()) == []
    assert len(store.get_jobs("js1", states=None)) == 1


# ----------------------------------------------------------------------
# R3-11: add_jobs 부분 적용 방지 — 실패 시 전량 미반영 (두 백엔드 계약)
# ----------------------------------------------------------------------
def test_add_jobs_atomic_on_failure(store):
    from lsfmgr.errors import JobSetNotFoundError
    store.store_insert_jobset(make_jobset(n=2))
    with pytest.raises(JobSetNotFoundError):
        store.store_add_jobs([make_job(idx=0), make_job(jsid="nope", idx=1)])
    assert store.get_jobs("js1") == [], "실패한 배치의 일부가 반영됨"



# ----------------------------------------------------------------------
# (R3-15: bgdel timeout — v10에서 bgdel 자체가 삭제됨)
# ----------------------------------------------------------------------
