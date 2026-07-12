"""전체 정독 리뷰 사이클 8에서 확정된 회귀 테스트.

사이클 1~7이 집중한 submit/kill 신호 영역 밖(store·config·monitor·command)을
정독해 나온 '새 결함' 5건:

- C8-1: optimistic kill이 verify보다 먼저 EXIT로 찍어 verify가 생존 job을 못 봄
- C8-2: collect_clusters=True인데 포맷 저하 시 저장된 forward_cluster를 None으로 덮음
- C8-3: LsfConfig가 poll_interval_s를 검증 안 해 auto_poll 시 Qt slot에서 앱이 죽음
- C8-4: find_jobs가 순회 중 삭제된 jobset의 예외를 흘려 전역 kill이 중단됨
- C8-5: bhist "Exited with exit code 0"(성공)을 EXIT(실패)로 오분류
"""
from __future__ import annotations

import pytest

from lsfmgr import (InMemoryStore, JobRecord, JobState, LsfConfig,
                    LsfJobManager)


# ----------------------------------------------------------------------
# C8-1: optimistic 정책 + verify — bkill 확인됐어도 실제 생존한 job을 잡는다
#       (verify를 optimistic 마킹보다 먼저 수행, 생존분은 EXIT로 안 덮음)
# ----------------------------------------------------------------------
def test_optimistic_verify_reports_survivor(qtbot, manager, fake_lsf, monkeypatch):
    js = manager.create_jobset(["echo a"], wrapper=False)   # bsub 경로
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    jid = js.jobs()[0].job_id

    # bkill은 확인('is being terminated')하지만 job은 실제로 살아남는 환경
    # (도달 불가 exec host 등) — bkill 직후 stat을 RUN으로 되돌려 흉내낸다.
    real_bkill = fake_lsf._do_bkill

    def survive_bkill(args, sourced=False):
        res = real_bkill(args, sourced=sourced)
        for j in fake_lsf.jobs.values():
            if j.job_id == jid:
                j.stat = "RUN"
                j.exit_code = None
        return res
    monkeypatch.setattr(fake_lsf, "_do_bkill", survive_bkill)

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as b:
        manager.kill(js, verify=True)            # 기본 optimistic 정책
    assert b.args[1].still_alive == 1            # 생존 job을 verify가 잡는다
    # 생존분은 optimistic EXIT로 덮이지 않는다 — 폴링/재kill에 남는다
    assert js.jobs()[0].state is JobState.RUN


# ----------------------------------------------------------------------
# C8-2: collect_clusters=True인데 bjobs가 MC 필드를 거부(포맷 저하)해도
#       저장된 forward_cluster를 None으로 덮지 않는다
# ----------------------------------------------------------------------
def test_cluster_preserved_on_format_downgrade(qtbot, fake_lsf, tmp_path):
    from tests.fake_lsf import FakeJob
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0,
                    kill_retry_delay_s=0.05, collect_clusters=True)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    try:
        js = mgr.create_jobset(intended_count=1)
        jsid = js.id
        # forward_cluster가 이미 채워진 RUN 레코드 (이전 MC 폴링/복원 결과)
        mgr.store.store_add_jobs([JobRecord(
            job_id=700, array_index=None, jobset_id=jsid,
            lsf_job_name=f"{jsid}_0", state=JobState.RUN, command="r",
            source_cluster="cA", forward_cluster="cB")])
        # 이후 폴링에서 job이 DONE으로 바뀌고(갱신 유발), 사이트가 MC 필드를
        # 거부해 포맷이 저하된다(cluster None) — 저장값이 소실되면 안 된다.
        fake_lsf.jobs["700"] = FakeJob(
            job_id=700, array_index=None, name=f"{jsid}_0", group=None,
            queue="q", command="r", stat="DONE", exit_code=0)
        fake_lsf.reject_clusters = True          # -o source_cluster 거부 → FULL 저하
        mgr.querier.query(jsid)
        rec = js.jobs()[0]
        assert rec.state is JobState.DONE        # 상태 갱신은 반영됐고
        assert rec.forward_cluster == "cB"       # forward_cluster는 보존
        assert rec.source_cluster == "cA"
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# C8-3: LsfConfig가 poll_interval_s를 생성 시점에 검증한다 (Qt slot에서 죽지 않게)
# ----------------------------------------------------------------------
def test_config_validates_poll_interval():
    # LsfConfig는 저수준 dataclass — 구조 불변식(양수)만 강제한다.
    # 5~60 정책 범위는 상위 options 계층의 몫(여기서 강제하면 poll=2 같은
    # 정당한 저수준 값을 죽여 하위호환이 깨진다 — 사이클 9에서 좁힘).
    with pytest.raises(ValueError):
        LsfConfig(poll_interval_s=0)             # 0/음수 = runtime 가드 위반
    with pytest.raises(ValueError):
        LsfConfig(poll_interval_s=-1)
    with pytest.raises(ValueError):
        LsfConfig(submit_timeout_s=0)
    # 양수면 통과 — 5~60 밖(2, 100)도 저수준에선 허용
    LsfConfig(poll_interval_s=2)
    LsfConfig(poll_interval_s=100)
    LsfConfig(poll_interval_s=10, submit_timeout_s=30)


# ----------------------------------------------------------------------
# C8-4: find_jobs 순회 중 jobset이 삭제돼도 예외를 흘리지 않고 건너뛴다
# ----------------------------------------------------------------------
def test_find_jobs_skips_deleted_jobset(store):
    from lsfmgr.errors import JobSetNotFoundError
    from lsfmgr.states import JobSetRecord
    store.store_insert_jobset(JobSetRecord(jobset_id="JS-A", intended_count=1))
    store.store_insert_jobset(JobSetRecord(jobset_id="JS-B", intended_count=1))
    store.store_add_jobs([JobRecord(
        job_id=10, array_index=None, jobset_id="JS-A",
        lsf_job_name="JS-A_0", state=JobState.RUN, command="r")])

    # get_jobs가 특정 jobset에서 삭제 경합을 흉내내 예외를 던지게 한다
    real_get = store.get_jobs

    def flaky_get(jsid, **kw):
        if jsid == "JS-B":
            raise JobSetNotFoundError("JS-B")     # 순회 중 삭제됨
        return real_get(jsid, **kw)
    store.get_jobs = flaky_get
    try:
        found = store.find_jobs({10})             # 예외 없이 JS-A 결과 반환
    finally:
        store.get_jobs = real_get
    assert [r.job_id for r in found] == [10]


# ----------------------------------------------------------------------
# C8-5→C9: bhist "Exited"는 exit code와 무관하게 EXIT — LSF 분류가 권위다.
#          "Exited with exit code 0"(kill/requeue-exit)을 DONE으로 바꾸면
#          죽인/실패 job이 성공으로 감춰진다(사이클 9에서 EXIT로 되돌림·동결).
#          정상 완료는 "Done successfully"로 온다.
# ----------------------------------------------------------------------
def test_bhist_exited_stays_exit_regardless_of_code():
    from lsfmgr.command import LsfCommand
    out = (
        "Job <100>, User <u>, ...\n"
        "  ... Done successfully. The CPU time used is 1.0 seconds.\n"
        "Job <101>, User <u>, ...\n"
        "  ... Exited with exit code 0. The CPU time used is 1.0 seconds.\n"
        "Job <102>, User <u>, ...\n"
        "  ... Exited with exit code 7. The CPU time used is 1.0 seconds.\n"
    )
    res = LsfCommand._parse_bhist(out)
    assert res[(100, None)] == (JobState.DONE, 0)   # 정상 완료만 DONE
    assert res[(101, None)] == (JobState.EXIT, 0)   # "Exited" → EXIT (code 0도)
    assert res[(102, None)] == (JobState.EXIT, 7)
