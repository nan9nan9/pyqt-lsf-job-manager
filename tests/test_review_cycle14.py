"""전체 정독 리뷰 사이클 14에서 확정된 회귀 테스트.

close→remove_jobset 전환(레코드 물리 삭제) 이후의 정독. 삭제가 '플래그'에서
'소멸'로 바뀌면서, **사라진 레코드에 뒤늦게 쓰는 경로**가 새 검토 대상이 됐다.

- C14-1 (live 결함): submitter만 '레코드 소실'을 방어하지 않았다.
  killer(_mark_exited/_flag_killed)·monitor(_poll)·handlers(tick)는 전부
  jobset이 사라지는 경우를 잡아 조용히 넘어간다 — merge가 source jobset을
  지우기 때문에 그 세계를 이미 겪었기 때문이다. 반면 submitter는 merge가
  "submit 진행 중이면 무조건 거부"라 그 세계를 겪은 적이 없었다.
  그래서 in-flight 제출 도중 레코드가 지워지면:
    ① "submit worker 예외" ERROR traceback
    ② "crash 후 전이 실패" ERROR traceback
    ③ error_occurred Signal — 방금 지운 jobset에 대해 GUI가 오류를 띄운다
    ④ INTERNAL_ERROR로 오분류
  가 났다. 원래 clear(force)/remove_job(force)에 있던 구멍이고,
  remove_jobset(force)가 같은 구멍을 하나 더 넓혔다.

  ⑤가 본질이다: 제출은 **이미 성공**해 job_id를 받았는데 기록할 레코드가
  없어, LSF에 살아있는 job이 아무 흔적 없이 남았다. 이전 close()는 레코드를
  남겨 뒀으므로 caller가 get_jobs로 그 id를 찾아 정리할 수 있었다.
  → 소실을 정상 경로로 흡수하되, 이미 잡힌 job_id는 WARNING으로 남긴다
  (레코드가 없으니 그 로그가 유일한 흔적 — force 삭제 계약상 정리는
  caller 책임이지만, 못 찾는 것을 정리할 수는 없다).
"""
from __future__ import annotations

import logging
import threading

import pytest

from tests.conftest import mk_jobset

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _mgr_blocking_submit(fake):
    """제출 subprocess 안에서 멈춰 세울 수 있는 manager — in-flight 창을
    테스트가 직접 잡기 위한 장치. 반환: (mgr, entered, release)."""
    entered, release = threading.Event(), threading.Event()

    def runner(argv, timeout, cwd=None):
        if str(argv[0]).endswith("_sub"):          # 제출만 붙잡는다
            entered.set()
            release.wait(5)
        return fake(argv, timeout, cwd)

    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05), runner=runner)
    return mgr, entered, release


@pytest.mark.parametrize("delete", ["remove_jobset", "clear_jobs"])
def test_delete_during_inflight_submit_is_not_internal_error(
        qtbot, caplog, delete):
    """C14-1: in-flight 제출 중 강제 삭제 — error Signal도 ERROR도 없어야 한다.

    jobset/job 레코드를 지우는 두 경로 모두 같은 창을 연다:
    worker가 wrapper subprocess 안에 있는 동안 main 스레드가 레코드를 지운다.
    """
    fake = FakeLsf()
    mgr, entered, release = _mgr_blocking_submit(fake)
    errors = []
    mgr.error_occurred.connect(lambda j, m: errors.append((j, m)))
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"])
        with caplog.at_level(logging.WARNING, logger="lsfmgr.submit"):
            mgr.submit(js, auto_poll=False, workers=1)
            assert entered.wait(3), "제출 subprocess 진입 실패"
            # 이 순간 레코드는 SUBMITTING, worker는 wrapper 실행 중
            getattr(mgr, delete)(js, force=True)
            release.set()
            qtbot.wait(500)

        assert errors == [], f"삭제된 jobset에서 error Signal: {errors}"
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []

        # 이미 잡힌 job_id는 유일한 흔적으로 남아야 한다 (고아 추적)
        warns = [r.getMessage() for r in caplog.records
                 if r.levelno == logging.WARNING]
        assert any("추적 없이" in m for m in warns), warns
        assert any(str(jid) in m
                   for m in warns for jid in fake.jobs), warns
    finally:
        release.set()
        mgr.shutdown()


@pytest.mark.parametrize("delete", ["remove_jobset", "clear_jobs"])
def test_force_delete_of_live_jobs_leaves_a_trace(qtbot, caplog, delete,
                                                  fake_lsf, config):
    """C14-2: force 삭제는 LSF에 살아있는 job의 job_id를 흔적 없이 지웠다.

    계약은 "레코드만 지운다, LSF 정리는 caller 책임"인데 레코드가 사라지면
    caller가 그 id를 **조회할 방법이 없다**. 이전 close()는 레코드를 남겨
    get_jobs로 찾을 수 있었으므로, 이건 remove 전환이 만든 구멍이다.
    """
    mgr = LsfJobManager(store=InMemoryStore(), config=config,
                        runner=fake_lsf)
    try:
        js = mk_jobset(mgr, ["customwrapper_sub a.sp"])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        live = [r.job_id for r in js.jobs()]          # PEND — LSF에 존재
        assert live and live[0] is not None

        with caplog.at_level(logging.WARNING, logger="lsfmgr.jobset"):
            getattr(mgr, delete)(js, force=True)
        warns = [r.getMessage() for r in caplog.records
                 if r.levelno == logging.WARNING]
        assert any(str(live[0]) in m for m in warns), warns
    finally:
        mgr.shutdown()


def test_non_force_delete_is_silent(qtbot, caplog, manager, fake_lsf):
    """전원 terminal이면 살아있는 job이 없으니 경고도 없어야 한다 —
    정상 종결마다 WARNING이 나면 이 로그가 무의미해진다."""
    js = mk_jobset(manager, ["customwrapper_sub a.sp"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    fake_lsf.set_all("DONE", 0)
    with qtbot.waitSignal(manager.jobset_updated, timeout=10000):
        manager.query_once(js)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="lsfmgr.jobset"):
        manager.remove_jobset(js)
    assert [r.getMessage() for r in caplog.records
            if r.levelno >= logging.WARNING] == []


def test_delete_during_inflight_submit_still_finishes(qtbot):
    """소실 흡수가 계상(_count)을 건너뛰면 done<total로 고착돼 submit_finished가
    영영 안 나온다 — 진행 dialog가 영구 고착되는 형태라 별도로 못박는다."""
    fake = FakeLsf()
    mgr, entered, release = _mgr_blocking_submit(fake)
    try:
        js = mk_jobset(mgr, [f"customwrapper_sub {i}.sp" for i in range(3)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000) as blocker:
            mgr.submit(js, auto_poll=False, workers=1)
            assert entered.wait(3)
            mgr.remove_jobset(js, force=True)
            release.set()
        rpt = blocker.args[1]              # Facade Signal = (jsid, report)
        # 3건 전부 어느 칸으로든 계상됐다 (한 건도 유실 없음)
        assert rpt.ok + rpt.failed + rpt.cancelled == 3
    finally:
        release.set()
        mgr.shutdown()
