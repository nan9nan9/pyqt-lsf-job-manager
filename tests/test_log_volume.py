"""대량 실패 시 로그가 부하가 되지 않는다.

mbatchd/eauth 과부하로 수천 건이 같은 이유(BSUB_EXIT_255 "User permission
denied")로 한꺼번에 떨어지는 일이 실제 환경에서 일어난다. job당 한 줄이면
그 자체가 부하다 — 앱의 로그 핸들러가 GUI 위젯이면 이벤트루프까지 막는다.
사유별로 앞 N건만 남기고 접되, 전체 내역은 완료 로그의 요약이 준다.
"""
from __future__ import annotations

import logging

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.util import LogSampler


def _captured(caplog, level, needle):
    return [r for r in caplog.records
            if r.levelno == level and needle in r.getMessage()]


def test_sampler_folds_per_kind():
    s = LogSampler(limit=3)
    assert [s.allow("A") for _ in range(5)] == [True, True, True, False, False]
    # 한도를 처음 넘긴 그 순간에만 접힘 통지
    s2 = LogSampler(limit=2)
    s2.allow("A"); s2.allow("A")
    s2.allow("A"); assert s2.just_folded("A") is True
    s2.allow("A"); assert s2.just_folded("A") is False
    # 부류가 다르면 따로 센다
    assert s2.allow("B") is True


def test_mass_failure_does_not_flood_the_log(qtbot, fake_lsf, caplog):
    N = 200
    fake_lsf.fail_next_bsub = N                 # 전량 실패 → 재시도 없이 확정
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(max_retry=0, workers=8),
                        runner=fake_lsf)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(N)],
                               job_keys=[f"k{i}" for i in range(N)])
        with caplog.at_level(logging.INFO, logger="lsfmgr.submit"):
            with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
                mgr.submit(js, auto_poll=False)

        per_job = [r for r in caplog.records
                   if r.getMessage().startswith(("submit 실패 [",
                                                 "SUBMIT_FAILED 확정 ["))]
        assert len(per_job) <= 2 * LogSampler().limit + 2, (
            f"{N}건 실패에 job별 로그 {len(per_job)}줄 — 접히지 않았다")
        assert _captured(caplog, logging.ERROR, "이후 같은 사유의 로그를 접습니다")

        # 접힌 대신 완료 로그가 전체 내역을 준다
        done = _captured(caplog, logging.INFO, "submit 완료")
        assert done, "완료 로그가 없다"
        msg = done[-1].getMessage()
        assert "실패 사유:" in msg and f"{N}건" in msg, msg
    finally:
        mgr.shutdown()


def test_normal_run_stays_quiet(qtbot, fake_lsf, caplog):
    """정상 경로는 job 수와 무관하게 몇 줄뿐이어야 한다."""
    N = 200
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(), runner=fake_lsf)
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(N)],
                               job_keys=[f"k{i}" for i in range(N)])
        with caplog.at_level(logging.INFO, logger="lsfmgr"):
            with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
                mgr.submit(js, auto_poll=False)
            with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
                mgr.kill(js)
        assert len(caplog.records) <= 12, (
            f"{N}건 정상 처리에 로그 {len(caplog.records)}줄:\n  "
            + "\n  ".join(r.getMessage()[:80] for r in caplog.records[:15]))
    finally:
        mgr.shutdown()
