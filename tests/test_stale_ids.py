"""job_id를 레코드에서 떨어뜨리는 모든 경로가 원장에서도 버린다.

콜백 조회원(job_status_fetcher)의 관심 집합·원장은 "조회할 id"의 목록이다.
레코드에서 id가 사라졌는데 여기 남으면 아무도 조회하지 않는 id가 영영
남는다 — 만료는 **종료(DONE/EXIT)** 항목만 걷어내므로, 마지막으로 진행
중으로 보였던 id는 만료 대상조차 아니다. 게다가 관심에 있는 동안 매 폴링의
병합·경과시간 갱신이 계속 훑는다.

규칙: **id를 지우는 쪽이 버리는 것도 책임진다.**
경로마다 주인이 다르므로(삭제=remove_*, 재제출=submitter, 교체=
_rearm_tracking) 새 경로가 생기면 여기 한 줄을 추가하도록 강제한다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager


@pytest.fixture
def mgr(qtbot, fake_lsf):
    live: dict = {}
    m = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(job_status_fetcher=lambda: {"jobs": [
            {"dataId": f"{i}.c1", "stat": "RUN"} for i in live]},
            internal_refresh_min_s=0.0),
        runner=fake_lsf)
    m._probe_live = live                      # fetcher가 돌려줄 id 집합
    try:
        yield m
    finally:
        m.shutdown()


def _submitted(qtbot, mgr, keys):
    js = mgr.create_jobset([f"mytool {k}.sp" for k in keys], job_keys=list(keys))
    with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
        mgr.submit(js, auto_poll=False)
    ids = [r.job_id for r in js.jobs()]
    mgr._probe_live.update({i: 1 for i in ids})
    mgr.query_once(js)                        # 관심 등록
    qtbot.wait(200)
    src = mgr.command.internal_status
    assert set(ids) <= set(src._interest), "관심 등록이 안 됐다 — 전제가 깨졌다"
    return js, ids


def _quiesce(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
        mgr.kill(js)


def _resubmit(m, js, qtbot):
    with qtbot.waitSignal(m.submit_finished, timeout=20000):
        m.submit(js, auto_poll=False)


DROPPERS = {
    "resubmit":      lambda m, js, qtbot: _resubmit(m, js, qtbot),
    "replace_jobs":  lambda m, js, qtbot: m.replace_jobs(
        js, ["mytool new.sp"], job_keys=["k0"]),
    "upsert_jobs":   lambda m, js, qtbot: m.upsert_jobs(
        js, ["mytool new.sp"], job_keys=["k0"]),
    "remove_jobs":   lambda m, js, qtbot: m.remove_jobs(js, ["k0"], force=True),
    "clear_jobs":    lambda m, js, qtbot: m.clear_jobs(js, force=True),
    "remove_jobset": lambda m, js, qtbot: m.remove_jobset(js, force=True),
}


@pytest.mark.parametrize("name", sorted(DROPPERS))
def test_dropping_a_job_id_also_forgets_it(name, mgr, qtbot):
    js, ids = _submitted(qtbot, mgr, ["k0", "k1"])
    _quiesce(qtbot, mgr, js)                  # 편집 가드 통과용
    src = mgr.command.internal_status

    DROPPERS[name](mgr, js, qtbot)
    qtbot.wait(200)

    # 레코드에 남아 있는 id만 관심에 있어야 한다
    alive = set()
    for jsr in mgr.store.list_jobsets():
        alive |= {r.job_id for r in mgr.store.get_jobs(jsr.jobset_id) if r.job_id}
    ghosts = set(src._interest) - alive
    assert not ghosts, (
        f"{name}: 레코드에서 사라진 job_id가 원장에 남았다 {sorted(ghosts)}")
    assert not (set(src._ledger) - alive)


def test_partial_resubmit_keeps_untargeted_ids_in_the_ledger(mgr, qtbot):
    """submit(only=)은 **대상 아닌** job의 원장 항목을 버리면 안 된다 —
    증분 payload는 안 바뀐 job을 다시 보내지 않으므로, 버리는 순간 그
    RUN job이 매 폴링 미발견으로 몰려 LOST(되돌릴 수 없음)로 확정될 수
    있다. forget은 이 사이클이 리셋하는 key의 id에만 걸려야 한다."""
    js, _ids = _submitted(qtbot, mgr, ["k0", "k1"])
    src = mgr.command.internal_status
    k1_id = next(r.job_id for r in js.jobs() if r.job_key == "k1")
    assert k1_id in src._ledger                  # 전제 — 원장에 있다
    with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
        mgr.kill_jobs(js, ["k0"])                # k0만 비활성으로
    with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
        mgr.submit(js, only=["k0"], auto_poll=False)   # k1은 RUN인 채
    assert k1_id in src._interest and k1_id in src._ledger, \
        "only= 재제출이 대상 아닌 RUN job의 원장 항목을 버렸다"


def test_the_matrix_covers_every_dropping_api():
    """job_id를 떨어뜨릴 수 있는 공개 API가 위 표에 다 있는지.

    레코드를 새로 쓰거나 지우는 명령은 전부 후보다 — 새 명령이 생기면
    표에 넣고 규칙을 지키는지 확인하라는 뜻이다."""
    CANDIDATES = {"submit", "replace_jobs", "upsert_jobs", "remove_jobs",
                  "clear_jobs", "remove_jobset"}
    covered = set(DROPPERS) | {"submit"}      # resubmit == submit 재호출
    assert CANDIDATES <= covered, f"표에 없는 경로: {sorted(CANDIDATES - covered)}"
    for name in CANDIDATES:
        assert hasattr(LsfJobManager, name), f"없는 API가 표에: {name}"
