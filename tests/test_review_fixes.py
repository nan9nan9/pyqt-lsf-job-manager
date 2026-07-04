"""정독 리뷰에서 발견된 버그들의 회귀 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobSpec, JobState, LsfJobManager
from lsfmgr.command import LsfCommand
from lsfmgr.config import LsfConfig
from lsfmgr.errors import LsfmgrError
from lsfmgr.jobset_core import detect_array_template
from lsfmgr.options import resolve_options
from tests.test_store_contract import make_job, make_jobset


# ----------------------------------------------------------------------
# 버그 1: leading-zero 인덱스를 $LSB_JOBINDEX로 오치환 → 잘못된 파일 실행
# ----------------------------------------------------------------------
def test_template_rejects_leading_zero_indices():
    # "run_01" vs $LSB_JOBINDEX(=1) → run_1 이 되므로 array 불가 판정이어야 함
    cmds = [f"hspice run_{i:02d}.sp" for i in range(1, 11)]
    assert detect_array_template(cmds) is None

    # zero-padding 없는 1..N은 여전히 array 가능
    cmds2 = [f"hspice run_{i}.sp" for i in range(1, 11)]
    assert detect_array_template(cmds2) == "hspice run_$LSB_JOBINDEX.sp"


def test_leading_zero_commands_submitted_verbatim(qtbot, manager, fake_lsf):
    cmds = [f"sim case_{i:03d}.sp" for i in range(1, 6)]
    js = manager.submit(cmds, auto_poll=False)        # auto → bulk여야 함
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    submitted = sorted(j.command for j in fake_lsf.jobs.values())
    assert submitted == sorted(cmds)                  # 원문 그대로 submit


# ----------------------------------------------------------------------
# 버그 2: SQLite 대량 submit 시 건당 트랜잭션 → caller 스레드 블로킹
# ----------------------------------------------------------------------
def test_store_add_jobs_batch_contract(store):
    """add_jobs 배치 API — 두 백엔드 동일 계약."""
    store.create_jobset(make_jobset(n=100))
    recs = store.add_jobs([make_job(idx=i) for i in range(100)])
    assert len(recs) == 100
    assert all(r.updated_at is not None for r in recs)
    assert len(store.get_jobs("js1")) == 100
    s = store.summary("js1")
    assert s["CREATED"] == 100 and s["total"] == 100


def test_store_add_jobs_missing_jobset(store):
    from lsfmgr.errors import JobSetNotFoundError
    with pytest.raises(JobSetNotFoundError):
        store.add_jobs([make_job(jsid="nope")])


def test_sqlite_bulk_submit_fast(qtbot, fake_lsf, config, tmp_path):
    """5,000건 CREATED 선생성이 단일 트랜잭션으로 즉시 끝나야 함 (NFR-3)."""
    import time
    from lsfmgr import SqliteStore
    mgr = LsfJobManager(store=SqliteStore(str(tmp_path / "big.db")),
                        config=config, runner=fake_lsf)
    try:
        t0 = time.monotonic()
        jsid = mgr.submit_bulk([JobSpec(command=f"r {i}")
                                for i in range(5000)])
        elapsed = time.monotonic() - t0            # submit_bulk 반환까지
        assert elapsed < 2.0, f"submit() 반환에 {elapsed:.2f}s — main 블로킹"
        assert mgr.summary(jsid)["total"] == 5000
        # 완주 대기는 불필요 — 반환 시간이 검증 대상. 취소로 빠르게 마무리.
        mgr.cancel_submit(jsid)
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            pass
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 버그 3: close 실패(전원 terminal 아님) 시 polling이 부수효과로 중지됨
# ----------------------------------------------------------------------
def test_failed_close_keeps_polling_and_handle(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(5)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    js.start_polling(interval_s=0.2)
    updates = []
    js.updated.connect(updates.append)
    qtbot.waitUntil(lambda: len(updates) >= 1, timeout=10000)

    with pytest.raises(LsfmgrError):
        js.close()                                   # 전원 PEND — 거부

    # 핸들 살아있고 polling도 계속 돈다
    assert js.summary["total"] == 5
    n = len(updates)
    qtbot.waitUntil(lambda: len(updates) > n, timeout=10000)


# ----------------------------------------------------------------------
# 버그 4: bsub group 거부 재시도가 job_name(-J)까지 버림 + 무한재귀 가능성
# ----------------------------------------------------------------------
def test_bsub_group_reject_keeps_job_name(fake_lsf):
    fake_lsf.reject_group = True
    cmd = LsfCommand(LsfConfig(), fake_lsf)
    jid = cmd.bsub("echo hi", job_name="js1_0", group_path="/bad/group")
    job = fake_lsf.jobs[str(jid)]
    assert job.group is None                # group만 포기
    assert job.name == "js1_0"              # name은 유지 (fallback 식별자)


# ----------------------------------------------------------------------
# 버그 5: tags="sweep" (str) → ('s','w','e','e','p')로 분해
# ----------------------------------------------------------------------
def test_tags_string_not_exploded():
    opts = resolve_options({}, {"tags": "sweep"})
    assert opts.tags == ("sweep",)


def test_tags_string_end_to_end(qtbot, manager, fake_lsf):
    js = manager.submit(["x"], tags="sweep", auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    assert manager.store.get_jobset(js.id).tags == ["sweep"]
    assert [j.jobset_id for j in manager.search_jobsets(tag="sweep")] == [js.id]


# ----------------------------------------------------------------------
# 버그 6: manager 전용 kwargs(chunk_size 등) 범위 미검증
# ----------------------------------------------------------------------
def test_manager_only_kwargs_validated(fake_lsf):
    with pytest.raises(ValueError):
        LsfJobManager(runner=fake_lsf, chunk_size=0)


# ----------------------------------------------------------------------
# 버그 7: submit([]) — finished가 핸들 생성 전에 동기 emit되어 유실
# ----------------------------------------------------------------------
def test_empty_submit_finished_reaches_handle(qtbot, manager, fake_lsf):
    js = manager.submit([], auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=5000) as blocker:
        pass
    rpt = blocker.args[0]
    assert rpt.total == 0 and rpt.ok == 0
    assert js.summary["total"] == 0


# ----------------------------------------------------------------------
# 버그 8: ReconcileReport.checked 이중 계산
# ----------------------------------------------------------------------
def test_query_result_checked_count(qtbot, manager, fake_lsf):
    js = manager.submit([f"r {i}" for i in range(10)], mode="bulk",
                        auto_poll=False)
    with qtbot.waitSignal(js.finished, timeout=10000):
        pass
    fake_lsf.set_all("RUN")
    result = manager.querier.query(js.id)
    assert result.checked == 10                 # 조회 대상 수 그대로
    assert len(result.changed) == 10            # PEND→RUN 전부 변경
