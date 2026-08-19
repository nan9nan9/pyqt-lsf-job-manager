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
