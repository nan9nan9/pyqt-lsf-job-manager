"""qtpy re-export 단일 지점.

lsfmgr 내부의 모든 Qt import는 반드시 이 모듈을 통해서만 수행한다.
qtpy가 PyQt5 / PySide2 / PyQt6 / PySide6 간 API 차이(pyqtSignal ↔ Signal 등)를
흡수하므로, 바인딩별 분기가 필요하면 이 모듈 안에서만 처리한다.
"""
from qtpy.QtCore import (  # noqa: F401
    QCoreApplication,
    QEvent,
    QObject,
    QRunnable,
    QThread,
    QThreadPool,
    QTimer,
    Signal,
    Slot,
)

#: QEvent.DeferredDelete 상수 — 바인딩별 위치 차이(PyQt6/PySide6는 .Type.*) 흡수.
#: sendPostedEvents로 특정 스레드의 deleteLater를 즉시 flush할 때 쓴다.
try:
    DEFERRED_DELETE = QEvent.Type.DeferredDelete       # PyQt6 / PySide6
except AttributeError:                                 # PyQt5 / PySide2
    DEFERRED_DELETE = QEvent.DeferredDelete

import logging as _logging

_log = _logging.getLogger("lsfmgr.qt")

#: QTimer의 ms 인자는 **int32**다 (약 24.8일). 넘기면 OverflowError가 나는데,
#: 그게 slot 안에서 터지면 PyQt는 그 slot을 죽인다 — 타이머가 영영 안 걸리고
#: (폴링/전이 표시가 통째로 멈춘다) 호출자는 그 사실을 모른다.
MAX_TIMER_MS = 2_147_483_647


def timer_ms(seconds: float) -> int:
    """초 → QTimer가 받을 수 있는 ms. int32 상한으로 clamp하고 음수는 0.

    **QTimer에 넘기는 모든 값은 이 함수를 거친다** — 검증은 공개 진입점
    (manager.start_polling, LsfConfig)이 따로 하지만, 그건 값의 출처마다
    복사돼야 하는 규칙이라 하나 빠뜨리면 조용히 slot이 죽는다. 여기는
    기계적인 마지막 방어선이다."""
    ms = seconds * 1000.0
    if ms != ms or ms >= MAX_TIMER_MS:         # NaN 또는 상한 초과
        if ms == ms:
            _log.warning("타이머 간격 %.0fs가 QTimer 상한(약 24.8일)을 넘어 "
                         "잘랐습니다", seconds)
        return MAX_TIMER_MS
    return max(0, int(ms + 0.5))


class CallTask(QRunnable):
    """임의 callable을 pool에서 1회 실행하는 공용 QRunnable —
    fire-and-forget용. 예외는 로그로 격리한다."""

    def __init__(self, fn):
        super().__init__()
        self.setAutoDelete(True)
        self._fn = fn

    def run(self):
        try:
            self._fn()
        except Exception:                    # noqa: BLE001
            _log.exception("백그라운드 작업 실패")


__all__ = [
    "CallTask",
    "MAX_TIMER_MS",
    "timer_ms",
    "QCoreApplication",
    "QEvent",
    "DEFERRED_DELETE",
    "QObject",
    "QRunnable",
    "QThread",
    "QThreadPool",
    "QTimer",
    "Signal",
    "Slot",
]
