"""정독 리뷰에서 발견된 버그들의 회귀 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import JobRecord, JobSpec, JobState, LsfJobManager
from tests.conftest import submit_cmds
from lsfmgr.command import LsfCommand
from lsfmgr.config import LsfConfig
from lsfmgr.errors import LsfmgrError
from lsfmgr.options import resolve_options
from tests.test_store_contract import make_job, make_jobset


# ----------------------------------------------------------------------
# 버그 1: leading-zero 인덱스를 $LSB_JOBINDEX로 오치환 → 잘못된 파일 실행
# ----------------------------------------------------------------------
def test_leading_zero_commands_submitted_verbatim(qtbot, manager, fake_lsf):
    cmds = [f"sim case_{i:03d}.sp" for i in range(1, 6)]
    js = submit_cmds(manager, cmds, auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
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


# ----------------------------------------------------------------------
# 버그 3: close 실패(전원 terminal 아님) 시 polling이 부수효과로 중지됨
# ----------------------------------------------------------------------
def test_failed_close_keeps_polling_and_handle(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, [f"r {i}" for i in range(5)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    manager.start_polling(js, 0.2)
    updates = []
    js.jobset_updated.connect(updates.append)
    qtbot.waitUntil(lambda: len(updates) >= 1, timeout=10000)

    with pytest.raises(LsfmgrError):
        manager.close(js)                                   # 전원 PEND — 거부

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
    js = submit_cmds(manager, ["x"], tags="sweep", auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
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
def test_empty_jobset_submit_rejected(manager, fake_lsf):
    """v9: 빈 jobset(commands 없이 생성)은 허용되지만 job이 없으니
    submit은 LsfmgrError로 거부된다. job은 이후 merge로만 채운다."""
    js = manager.create_jobset()          # 빈 jobset — 생성만 (허용)
    assert js.summary["total"] == 0
    with pytest.raises(LsfmgrError):
        manager.submit(js)                # job 없음 → 거부
    # merge_ids/ud_datas 길이 불일치는 ValueError
    with pytest.raises(ValueError):
        manager.create_jobset(["a", "b"], merge_ids=["m1"])


# ----------------------------------------------------------------------
# 버그 9 (2차): kill 전략의 no-match를 커버 성공으로 오판
#   — group 부착이 거부된 jobset은 bkill -g가 no-match인데도 covered=True
#     처리되어 fallback을 건너뛰고 job이 하나도 죽지 않았음
# ----------------------------------------------------------------------
def test_kill_falls_through_when_group_rejected(qtbot, manager, fake_lsf):
    fake_lsf.reject_group = True            # 모든 job이 group 없이 submit됨
    js = submit_cmds(manager, [f"r {i}" for i in range(20)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert all(j.group is None for j in fake_lsf.jobs.values())

    with qtbot.waitSignal(js.kill_finished, timeout=10000) as blocker:
        manager.kill(js)
    rpt = blocker.args[0]
    # group 전략은 no-match로 표시되고 name 패턴으로 fallback해 전부 kill
    assert any("(no-match)" in s for s in rpt.strategies)
    assert fake_lsf.alive_jobs() == [], "group 커버 오판으로 job이 살아남음"


# ----------------------------------------------------------------------
# 버그 10 (2차): array 부분 kill이 parent id로 전체를 죽임
# ----------------------------------------------------------------------
def test_array_partial_kill_only_pend(qtbot, manager, fake_lsf):
    """array element 부분 kill — parent id가 아니라 "id[idx]"로 지정해
    PEND element만 죽어야 한다 (v9: array는 wrapper 제출 산물로만 존재 —
    레코드/LSF를 수동 구성해 element 계약을 검증)."""
    from tests.fake_lsf import FakeJob

    js = manager.create_jobset(intended_count=10)
    jsid, parent = js.id, 9000
    manager.store.add_jobs([JobRecord(
        job_id=parent, array_index=i, jobset_id=jsid,
        lsf_job_name=f"{jsid}[{i}]",
        state=JobState.RUN if i <= 5 else JobState.PEND, command="r")
        for i in range(1, 11)])
    for i in range(1, 11):
        fake_lsf.jobs[f"{parent}[{i}]"] = FakeJob(
            job_id=parent, array_index=i, name=f"{jsid}[{i}]", group=None,
            queue="q", command="r", stat="RUN" if i <= 5 else "PEND")

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js, only_state=JobState.PEND)

    stats = {f"{parent}[{i}]": fake_lsf.jobs[f"{parent}[{i}]"].stat
             for i in range(1, 11)}
    assert all(v == "RUN" for k, v in stats.items()
               if int(k.split("[")[1][:-1]) <= 5), stats   # RUN은 생존
    assert all(v == "EXIT" for k, v in stats.items()
               if int(k.split("[")[1][:-1]) > 5), stats    # PEND만 kill

# ----------------------------------------------------------------------
# 버그 11 (2차): SqliteStore._Tx — connection 생성 실패 시 wlock 누수
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 버그 12 (2차): tags="..." 문자열이 문자 단위로 분해되던 버그
# ----------------------------------------------------------------------
def test_lowlevel_submit_tags_string(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        jsid = submit_cmds(manager, [JobSpec(command="x")], tags="sweep").id
    assert manager.store.get_jobset(jsid).tags == ["sweep"]


# ----------------------------------------------------------------------
# 버그 8: ReconcileReport.checked 이중 계산
# ----------------------------------------------------------------------
def test_query_result_checked_count(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, [f"r {i}" for i in range(10)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    fake_lsf.set_all("RUN")
    result = manager.querier.query(js.id)
    assert result.checked == 10                 # 조회 대상 수 그대로
    assert len(result.changed) == 10            # PEND→RUN 전부 변경
