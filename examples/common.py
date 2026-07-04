"""예제 공통 유틸 — manager 생성, 로깅, 자동 종료(스모크 테스트용).

모든 예제는 기본으로 SimulatedLsf(시뮬레이터)를 사용한다.
실제 LSF cluster에서 돌리려면 환경변수 LSFMGR_REAL=1 을 설정하면 된다.
"""
from __future__ import annotations

import logging
import os
import sys

from qtpy.QtCore import QTimer

from lsfmgr import LsfJobManager
from mock_lsf import SimulatedLsf


def install_logging(level: int = logging.INFO) -> None:
    """lsfmgr.* 로그를 콘솔로 (스레드명 포함 — docs/logging.md 권장 포맷)."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"))
    logger = logging.getLogger("lsfmgr")
    logger.setLevel(level)
    logger.addHandler(handler)


def make_manager(sim: SimulatedLsf | None = None,
                 **mgr_kwargs) -> tuple[LsfJobManager, SimulatedLsf | None]:
    """예제용 manager 생성. (manager, simulator|None) 반환."""
    if os.environ.get("LSFMGR_REAL") == "1":
        return LsfJobManager(**mgr_kwargs), None       # 실제 LSF 사용
    sim = sim or SimulatedLsf()
    return LsfJobManager(runner=sim, **mgr_kwargs), sim


def maybe_autoquit(app) -> None:
    """LSFMGR_DEMO_AUTOQUIT=<초> 설정 시 자동 종료 (CI 스모크 테스트용)."""
    sec = float(os.environ.get("LSFMGR_DEMO_AUTOQUIT", "0"))
    if sec > 0:
        QTimer.singleShot(int(sec * 1000), app.quit)


def format_summary(s: dict) -> str:
    """요약 dict → 한 줄 문자열."""
    keys = [k for k in ("PEND", "RUN", "DONE", "EXIT", "SUBMIT_FAILED",
                        "RETRY_WAIT", "LOST", "CREATED", "SUBMITTING")
            if s.get(k)]
    body = "  ".join(f"{k}={s[k]}" for k in keys)
    return f"total={s.get('total', 0)}  {body}"
