"""전체 정독 리뷰 사이클 17 — merge 삭제/submit(only=) 도입 직후의 정독.

- C17-1: can_submit이 **술어인데 예외를 던졌다**. _submit_targets가 잘못된
  only(array element)에 ValueError를 던지는데, can_submit의 방어는
  `except LsfmgrError`뿐이라 그대로 샜다. 버튼 갱신 루프에서 부르는 GUI가
  죽는다. → 술어는 "이 인자로 submit이 되겠는가"에 답한다: 잘못된 only면
  답은 False. (앱 버그 자체는 실제 submit 호출이 예외로 드러낸다.)

- C17-2: only=[job_id]가 array parent를 못 집었다. by_jid를 만들 때 element도
  넣어서, parent와 job_id를 공유하는 element가 마지막에 덮어썼다. 그래서
  parent를 지정했는데 "array element는 개별 제출 불가"로 엉뚱하게 거부된다.
  → 색인은 parent만 담고, 그 id가 element로만 존재하면 정확한 이유를 알린다.

- C17-3 (효율): PollingService.start_polling이 **항상** 타이머를 갈아끼우고
  즉시 1회 조회까지 했다. job 편집마다 부르는 _resume_polling_if_watchable이
  생기면서 노출도가 크게 올랐다 — 편집 1회당 bjobs 1회가 나가고(실측: 편집
  10회 → bjobs 10회), interval이 계속 리셋돼 주기 tick이 영영 안 오며,
  _idle_counts가 지워져 auto-stop 조건 ②도 성립하지 않는다.
  → 같은 주기로 이미 돌고 있으면 no-op. 즉시 갱신은 query_once가 담당한다.
"""
from __future__ import annotations

from tests.conftest import mk_jobset
from lsfmgr import (InMemoryStore, JobRecord, JobState, LsfConfig,
                    LsfJobManager)
from tests.fake_lsf import FakeLsf


def _array_records(jsid, parent, n):
    return [JobRecord(job_id=parent, array_index=i, jobset_id=jsid,
                      job_key=f"{jsid}[{i}]", state=JobState.DONE,
                      command="r") for i in range(n)]


def test_can_submit_never_raises_on_bad_only(qtbot, manager, fake_lsf):
    """C17-1: 술어는 어떤 only에도 bool을 돌려준다."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"], job_keys=["a"])
    manager.store.store_add_jobs(_array_records(js.id, 9500, 2))

    assert manager.can_submit(js, only=[f"{js.id}[0]"]) is False  # array element
    assert manager.can_submit(js, only=["없는키"]) is False        # 없는 ref
    assert manager.can_submit(js, only=[]) is False               # 빈 선택
    assert manager.can_submit(js, only=["a"]) is True             # 정상


def test_only_job_id_picks_array_parent(qtbot, manager, fake_lsf):
    """C17-2: job_id로 지정하면 element가 아니라 parent가 잡힌다."""
    from dataclasses import replace as dc_replace

    js = mk_jobset(manager, ["customwrapper_sub p.sp"], job_keys=["p"])
    parent = js.jobs()[0]
    manager.store.update_job(
        dc_replace(parent, job_id=9500, state=JobState.DONE))
    manager.store.store_add_jobs(_array_records(js.id, 9500, 3))

    targets = manager._submit_targets(js.id, [9500])
    assert [r.job_key for r in targets] == [parent.job_key]
    assert targets[0].array_index is None


def test_only_job_id_of_element_only_reports_why(qtbot, manager, fake_lsf):
    """그 id가 element로만 존재하면 이유를 정확히 알린다(없는 ref가 아니다)."""
    import pytest

    js = mk_jobset(manager, intended_count=2)
    manager.store.store_add_jobs(_array_records(js.id, 9500, 2))
    with pytest.raises(ValueError, match="array element"):
        manager._submit_targets(js.id, [9500])


def test_start_polling_is_noop_when_already_running(qtbot):
    """C17-3: 같은 주기로 이미 돌고 있으면 재시작하지 않는다.

    job 편집마다 폴링 재개를 부르므로, 여기서 매번 타이머를 갈아끼우면
    편집 1회당 bjobs 1회가 나간다."""
    fake = FakeLsf()
    calls = {"n": 0}

    def counting(argv, timeout, cwd=None):
        if str(argv[0]).rsplit("/", 1)[-1] == "bjobs":
            calls["n"] += 1
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05), runner=counting)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)    # 조회 대상(job_id) 확보
        mgr.start_polling(js, 30)              # 주기 tick은 안 올 만큼 길게
        qtbot.wait(200)
        base = calls["n"]

        for i in range(10):                    # job 을 하나씩 10번 추가
            mgr.add_jobs(js, [f"customwrapper_sub {i}.sp"],
                         job_keys=[f"m{i}"])
            qtbot.wait(30)
        assert calls["n"] == base, (
            f"편집 10회에 bjobs {calls['n'] - base}회 — 재시작이 억제되지 않았다")
    finally:
        mgr.shutdown()


def test_start_polling_restarts_when_interval_changes(qtbot):
    """반대 방향 — 주기가 바뀌면 실제로 재시작한다(즉시 1회 조회 포함)."""
    fake = FakeLsf()
    calls = {"n": 0}

    def counting(argv, timeout, cwd=None):
        if str(argv[0]).rsplit("/", 1)[-1] == "bjobs":
            calls["n"] += 1
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05), runner=counting)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"], job_keys=["a"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        mgr.start_polling(js, 30)
        qtbot.wait(200)
        base = calls["n"]

        mgr.start_polling(js, 45)              # 다른 주기 → 재시작
        qtbot.waitUntil(lambda: calls["n"] > base, timeout=5000)
    finally:
        mgr.shutdown()
