"""v10(wrapper 단일 경로) 전면 기능 검증 — 단위 테스트가 안 덮는 E2E/스트레스.

시나리오: 대량 제출 왕복, kill×submit 무작위 경합 반복, 연속 재제출,
부분 kill+verify 상호작용, bjobs 부분 장애 보류→복구, deprecated 옵션
하위 호환, merge 후 kill, shutdown 안전성.
"""
from __future__ import annotations

import threading

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager, JobState
from tests.conftest import submit_cmds
from tests.fake_lsf import FakeLsf


def _summary_invariant(mgr, jsid):
    s = mgr.summary(jsid)
    assert sum(v for k, v in s.items() if k != "total") == s["total"], s
    return s


# ----------------------------------------------------------------------
# 1. 대량 1000 job 왕복 — submit → RUN → DONE/EXIT 혼합 → close
# ----------------------------------------------------------------------
def test_bulk_1000_roundtrip(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=30000) as blk:
        js = submit_cmds(manager, [f"sim case_{i}.sp" for i in range(1000)],
                         workers=16, auto_poll=False)
    assert blk.args[1].succeeded == 1000
    recs = js.jobs()
    assert len({r.job_id for r in recs}) == 1000       # 전원 유일 id
    assert all(r.via_wrapper for r in recs)

    fake_lsf.set_all("RUN")
    manager.querier.query(js.id)
    assert _summary_invariant(manager, js.id)["RUN"] == 1000

    # 일부는 실패로 종료
    fake_lsf.set_all("DONE", 0)
    for r in recs[:37]:
        fake_lsf.set_job(r.job_id, "EXIT", 9)
    manager.querier.query(js.id)
    s = _summary_invariant(manager, js.id)
    assert s["DONE"] == 963 and s["EXIT"] == 37

    manager.close(js)                                   # 전원 terminal — 성공
    assert manager.store.get_jobset(js.id).closed


# ----------------------------------------------------------------------
# 2. kill × submit 무작위 경합 반복 — 유출/고착 불변식
# ----------------------------------------------------------------------
def test_kill_submit_race_repeated(qtbot, fake_lsf, config):
    """반복 10회: 제출 직후(타이밍 가변) kill — 어떤 교차에서도
    ① LSF 생존자 0 ② 상태는 EXIT/CREATED/SUBMIT_FAILED만
    ③ submit_finished/kill_finished 모두 도착."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        for round_no in range(10):
            got = {"submit": 0, "kill": 0}
            mgr.submit_finished.connect(
                lambda *_a, g=got: g.__setitem__("submit", g["submit"] + 1))
            mgr.kill_finished.connect(
                lambda *_a, g=got: g.__setitem__("kill", g["kill"] + 1))
            js = submit_cmds(mgr, [f"r{round_no}_{i}" for i in range(30)],
                             workers=4, auto_poll=False)
            if round_no % 3 == 0:
                qtbot.wait(round_no % 5)     # 타이밍 가변
            mgr.kill(js)
            qtbot.waitUntil(lambda g=got: g["submit"] >= 1 and g["kill"] >= 1,
                            timeout=15000)
            # 이 jobset의 job은 LSF에 살아있으면 안 된다
            ids = {r.job_id for r in js.jobs() if r.job_id is not None}
            alive = [j for j in fake_lsf.alive_jobs() if j.job_id in ids]
            assert alive == [], f"round {round_no}: 생존 유출 {alive}"
            states = {r.state for r in js.jobs()}
            assert states <= {JobState.EXIT, JobState.CREATED,
                              JobState.SUBMIT_FAILED}, \
                f"round {round_no}: 고착 상태 {states}"
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 3. 연속 재제출 3회 — 물리 키 유지, id 갱신, 불변식 유지
# ----------------------------------------------------------------------
def test_three_resubmit_cycles(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, [f"c {i}" for i in range(20)],
                         auto_poll=False)
    keys = {r.job_key for r in js.jobs()}
    seen_ids = set()
    for _cycle in range(3):
        fake_lsf.set_all("DONE", 0)
        manager.querier.query(js.id)
        assert manager.can_submit(js)
        with qtbot.waitSignal(manager.submit_finished, timeout=10000):
            manager.submit(js, auto_poll=False)
        recs = js.jobs()
        assert {r.job_key for r in recs} == keys        # 물리 키 유지
        ids = {r.job_id for r in recs}
        assert ids.isdisjoint(seen_ids)                 # 항상 새 실행
        seen_ids |= ids
        assert all(r.state is JobState.PEND for r in recs)
        _summary_invariant(manager, js.id)


# ----------------------------------------------------------------------
# 4. 부분 kill(only_state) + verify + optimistic 상호작용
# ----------------------------------------------------------------------
def test_partial_kill_verify_optimistic(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, [f"p {i}" for i in range(10)],
                         auto_poll=False)
    recs = js.jobs()
    for r in recs[:5]:
        fake_lsf.set_job(r.job_id, "RUN")
    manager.querier.query(js.id)                        # RUN 5 / PEND 5

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill(js, only_state=JobState.PEND, verify=True)
    report = blk.args[1]
    assert report.requested == 5
    assert report.still_alive == 0                      # 대상만 집계
    assert report.unconfirmed == 0 and not report.errors
    s = _summary_invariant(manager, js.id)
    assert s["RUN"] == 5 and s["EXIT"] == 5             # RUN은 무사


# ----------------------------------------------------------------------
# 5. bjobs chunk 부분 장애 — 보류 후 다음 사이클 복구 (LOST 오확정 금지)
# ----------------------------------------------------------------------
def test_bjobs_partial_failure_defers_then_recovers(qtbot, fake_lsf):
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0, chunk_size=1)
    mgr = LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["f 0", "f 1", "f 2"], auto_poll=False)
        recs = js.jobs()
        fake_lsf.set_all("RUN")
        # 가운데 job의 chunk만 장애 + bhist도 장애 (이중 실패 → 보류)
        fake_lsf.bjobs_fail_ids = {recs[1].job_id}
        fake_lsf.bhist_fail_ids = {recs[1].job_id}
        mgr.querier.query(js.id)
        after = {r.job_key: r.state for r in js.jobs()}
        assert after[recs[0].job_key] is JobState.RUN
        assert after[recs[2].job_key] is JobState.RUN
        assert after[recs[1].job_key] is JobState.PEND  # 보류 (LOST 아님)

        fake_lsf.bjobs_fail_ids = set()                 # 장애 해소
        fake_lsf.bhist_fail_ids = set()
        mgr.querier.query(js.id)
        assert js.jobs()[1].state is JobState.RUN       # 복구
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 6. deprecated 옵션 하위 호환 — 경고 후 무시, 기능 정상
# ----------------------------------------------------------------------
def test_deprecated_kwargs_ignored_end_to_end(qtbot, fake_lsf, config, caplog):
    import logging
    caplog.set_level(logging.WARNING, logger="lsfmgr.options")
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf,
                        queue="short", default_queue="priority",
                        bsub_path="/opt/lsf/bin/bsub",
                        lsf_group_root="/grp", output_dir="/out")
    try:
        assert caplog.text.count("무시합니다") >= 5
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blk:
            js = submit_cmds(mgr, ["dep 0"], resource_req="rusage[mem=1]",
                             auto_poll=False)
        assert blk.args[1].succeeded == 1
        assert js.jobs()[0].job_id is not None
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 7. merge 후 조회·kill — 부착물 없는 merge가 정상 동작
# ----------------------------------------------------------------------
def test_merge_then_query_and_kill(qtbot, manager, fake_lsf):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = submit_cmds(manager, ["m a0", "m a1"], auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(a.id)

    b = manager.create_jobset(["m b0", "m b1"], merge_ids=["x", "y"])
    manager.merge(a, b)                                  # in-place 흡수
    assert manager.summary(a.id)["total"] == 4

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(a, auto_poll=False)               # 전 job 재제출
    fake_lsf.set_all("RUN")
    manager.querier.query(a.id)
    assert _summary_invariant(manager, a.id)["RUN"] == 4

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blk:
        manager.kill(a)
    assert blk.args[1].requested == 4
    # set_all이 이전 실행의 종료 job까지 되살리므로 현재 id로만 판정
    cur_ids = {r.job_id for r in a.jobs()}
    assert not [j for j in fake_lsf.alive_jobs() if j.job_id in cur_ids]
    assert all(r.state is JobState.EXIT for r in a.jobs())


# ----------------------------------------------------------------------
# 8. 제출 직후 즉시 shutdown — hang/예외 없이 정리
# ----------------------------------------------------------------------
def test_shutdown_immediately_after_submit(qtbot, fake_lsf, config):
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    submit_cmds(mgr, [f"s {i}" for i in range(50)], workers=8,
                auto_poll=False)
    mgr.shutdown()                                       # 즉시 종료 — hang 금지
    # 이중 shutdown 멱등
    mgr.shutdown()
