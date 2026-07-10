"""lsfmgr — LSF Job Manager for Qt Applications.

qtpy 기반 (PyQt5/PySide2/PyQt6/PySide6 호환). 모든 LSF 호출은 백그라운드
스레드에서 실행되고 결과는 Qt Signal로 통지된다 — GUI freeze 없음.
"""
from .config import JobSpec, LsfConfig
from .errors import (
    ArgMaxExceededError,
    JobNotFoundError,
    JobSetNotFoundError,
    LsfCommandError,
    LsfmgrError,
    SubmitError,
)
from .errors import JobSetClosedError
from .handle import JobSet
from .handlers import HandlerContext, HandlerResult
from .manager import LsfJobManager
from .options import Options
from .reports import (
    KillProgress,
    KillReport,
    SubmitProgress,
    SubmitReport,
)
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore
from .store.memory import InMemoryStore

__version__ = "0.2.0"

__all__ = [
    "LsfJobManager",
    "JobSet",
    "Options",
    "JobSetClosedError",
    "LsfConfig",
    "JobSpec",
    "JobState",
    "JobRecord",
    "JobSetRecord",
    "JobSetStore",
    "InMemoryStore",
    "SubmitReport",
    "SubmitProgress",
    "KillReport",
    "KillProgress",
    "HandlerContext",
    "HandlerResult",
    "LsfmgrError",
    "JobSetNotFoundError",
    "JobNotFoundError",
    "LsfCommandError",
    "SubmitError",
    "ArgMaxExceededError",
]
