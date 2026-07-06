"""qtpy re-export 단일 지점.

lsfmgr 내부의 모든 Qt import는 반드시 이 모듈을 통해서만 수행한다 (NFR-1).
qtpy가 PyQt5 / PySide2 / PyQt6 / PySide6 간 API 차이(pyqtSignal ↔ Signal 등)를
흡수하므로, 바인딩별 분기가 필요하면 이 모듈 안에서만 처리한다.
"""
from qtpy.QtCore import (  # noqa: F401
    QCoreApplication,
    QEvent,
    QMutex,
    QMutexLocker,
    QObject,
    QRunnable,
    Qt,
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

__all__ = [
    "QCoreApplication",
    "QEvent",
    "DEFERRED_DELETE",
    "QMutex",
    "QMutexLocker",
    "QObject",
    "QRunnable",
    "Qt",
    "QThread",
    "QThreadPool",
    "QTimer",
    "Signal",
    "Slot",
]
