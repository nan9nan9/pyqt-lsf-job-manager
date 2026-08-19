"""전체 정독 리뷰 사이클 19 — 새 실행이 옛 실행의 LOST 유예를 물려받았다.

C19-1 (live 결함): 재제출/교체가 그 job의 **LOST 미발견 스트릭**을 지우지
않았다.

JobsetQuerier는 사이클마다 스트릭을 재구축한다(_pop_streaks → cur →
_set_streaks). 그런데 조회 대상(on-LSF)이 하나도 없는 사이클은 query()가
targets 비어 있음으로 **조기 반환**해 옛 값이 그대로 남는다 — 재제출 사이가
정확히 그 상태다(전원 terminal/CREATED).

그래서:
  ① 실행 A의 job이 bjobs에서 2회 미발견 → streak=2 (grace=3이라 아직 유예)
  ② kill/교체로 terminal·CREATED가 됨 → 이후 query는 조기 반환, streak 잔존
  ③ 재제출 → 새 job_id
  ④ 제출 직후 등록 지연으로 **1회만** 안 보여도 2+1=3 → LOST 확정

LOST는 되돌릴 수 없는 terminal이고, grace(lost_after_missing_polls)는 바로
"제출 직후 등록 지연처럼 한 사이클만 안 보이는 상황에서 멀쩡한 job을 죽은
것으로 만들지 않는다"고 주석에 적힌 방어다. 그 방어가 무력화됐다.

→ 착수 확정(_on_records_reset)과 편집(_edit_jobs)에서 그 키들의 스트릭을
  버린다. handler 장부(rearm)를 그 자리에서 리셋하는 것과 같은 이유·같은 지점.
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _streaks(mgr, jsid):
    return dict(mgr.querier._missing_streak.get(jsid, {}))


def _vanish_all(fake):
    for j in list(fake.jobs.values()):
        j.vanished = True


def _mgr(fake, grace=3):
    return LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05, lost_after_missing_polls=grace),
        runner=fake)


def test_resubmit_clears_missing_streak(qtbot):
    """C19-1: 재제출하면 새 실행이 옛 미발견 횟수를 물려받지 않는다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], job_keys=["k1"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        mgr.querier.query(js.id)

        _vanish_all(fake)                     # 실행 A: 2회 미발견(유예 중)
        mgr.querier.query(js.id)
        mgr.querier.query(js.id)
        assert _streaks(mgr, js.id) == {"k1": 2}
        assert js.jobs()[0].state is JobState.RUN     # 아직 LOST 아님

        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill(js)                      # terminal로 만들어 재제출 가능하게
        fake.jobs.clear()
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        assert _streaks(mgr, js.id) == {}, "착수 시 옛 스트릭이 안 지워졌다"

        # 새 실행이 첫 조회부터 안 보여도(등록 지연) 1회로는 LOST가 아니다
        _vanish_all(fake)
        mgr.querier.query(js.id)
        assert _streaks(mgr, js.id) == {"k1": 1}
        assert js.jobs()[0].state is not JobState.LOST
    finally:
        mgr.shutdown()


def test_grace_still_reaches_lost_within_one_run(qtbot):
    """반대 방향 — 같은 실행 안에서는 grace를 채우면 여전히 LOST가 된다.
    (스트릭을 너무 자주 지우면 LOST가 영영 확정되지 않는다.)"""
    fake = FakeLsf()
    mgr = _mgr(fake, grace=2)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], job_keys=["k1"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        mgr.querier.query(js.id)

        _vanish_all(fake)
        mgr.querier.query(js.id)
        assert js.jobs()[0].state is JobState.RUN     # 1회 — 유예
        mgr.querier.query(js.id)
        assert js.jobs()[0].state is JobState.LOST    # 2회 — 확정
    finally:
        mgr.shutdown()


def test_replace_clears_missing_streak(qtbot):
    """교체도 같은 자리를 다른 job으로 바꾸는 것이라 스트릭을 물려주지 않는다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], job_keys=["k1"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        mgr.querier.query(js.id)
        _vanish_all(fake)
        mgr.querier.query(js.id)
        assert _streaks(mgr, js.id) == {"k1": 1}

        mgr.replace_jobs(js, ["customwrapper_sub a2.sp"], job_keys=["k1"],
                         force=True)          # RUN이던 자리를 교체
        assert _streaks(mgr, js.id) == {}
    finally:
        mgr.shutdown()
