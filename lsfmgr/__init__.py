"""lsfmgr — LSF Job Manager for Qt Applications.

qtpy 기반 (PyQt5/PySide2/PyQt6/PySide6 호환). 모든 LSF 호출은 백그라운드
스레드에서 실행되고 결과는 Qt Signal로 통지된다 — GUI freeze 없음.
"""
from .config import ArrayJobSpec, JobSpec, LsfConfig
from .errors import (
    ArgMaxExceededError,
    JobNotFoundError,
    JobSetNotFoundError,
    LsfCommandError,
    LsfmgrError,
    PersistenceNotSupportedError,
    SubmitError,
)
from .errors import JobSetClosedError
from .handle import JobSet
from .handlers import HandlerContext, HandlerResult
from .manager import LsfJobManager
from .options import Options
from .reports import KillReport, ReconcileReport, SubmitProgress, SubmitReport
from .states import JobRecord, JobSetRecord, JobState
from .store import InMemoryStore, JobSetStore, SqliteStore

__version__ = "0.2.0"

__all__ = [
    "LsfJobManager",
    "JobSet",
    "Options",
    "JobSetClosedError",
    "LsfConfig",
    "JobSpec",
    "ArrayJobSpec",
    "JobState",
    "JobRecord",
    "JobSetRecord",
    "JobSetStore",
    "InMemoryStore",
    "SqliteStore",
    "SubmitReport",
    "SubmitProgress",
    "KillReport",
    "ReconcileReport",
    "HandlerContext",
    "HandlerResult",
    "LsfmgrError",
    "PersistenceNotSupportedError",
    "JobSetNotFoundError",
    "JobNotFoundError",
    "LsfCommandError",
    "SubmitError",
    "ArgMaxExceededError",
]
