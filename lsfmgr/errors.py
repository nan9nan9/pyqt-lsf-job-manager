"""lsfmgr 예외 계층 (Qt 비의존 순수 Python)."""
from __future__ import annotations

from typing import Optional


class LsfmgrError(Exception):
    """lsfmgr 모든 예외의 base."""


class PersistenceNotSupportedError(LsfmgrError):
    """InMemoryStore에서 Sqlite 전용 API 호출 시 발생."""


class JobSetNotFoundError(LsfmgrError):
    """존재하지 않는 jobset_id 접근."""


class JobSetClosedError(LsfmgrError):
    """close/삭제되어 파괴된 JobSet 핸들 접근 (v7 §1.3)."""


class JobNotFoundError(LsfmgrError):
    """jobset 내에 없는 job_key 접근."""


class LsfCommandError(LsfmgrError):
    """LSF 명령 실행 실패 (bsub 제외 — bsub는 SubmitError)."""

    def __init__(self, message: str, returncode: Optional[int] = None,
                 stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


class SubmitError(LsfmgrError):
    """bsub 실패. fail_reason은 JobRecord.fail_reason으로 그대로 기록된다."""

    def __init__(self, message: str, fail_reason: str,
                 returncode: Optional[int] = None, stderr: str = "",
                 stdout: str = "", retryable: bool = True):
        super().__init__(message)
        self.fail_reason = fail_reason
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout
        # 재시도 대상 여부. submit_wrapper 경로는 '비정상 종료(non-zero)'만
        # 재시도하고, 파싱 실패/timeout 은 중복 제출 위험 때문에 재시도하지 않는다.
        self.retryable = retryable

    def diagnostic(self) -> str:
        """터미널에서 봤을 실패 메시지 원문 — JobRecord.fail_message 저장용.
        stderr 우선, 파싱 실패처럼 stdout에 단서가 있으면 함께 담는다."""
        parts = [t for t in (self.stderr.strip(), self.stdout.strip()) if t]
        return "\n".join(parts) or str(self)


class ArgMaxExceededError(LsfmgrError):
    """단일 chunk가 ARG_MAX 한도를 초과 — chunk_size 조정 필요 (NFR-5)."""
