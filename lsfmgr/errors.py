"""lsfmgr 예외 계층 (Qt 비의존 순수 Python)."""
from __future__ import annotations

from typing import Optional


class LsfmgrError(Exception):
    """lsfmgr 모든 예외의 base."""


class JobSetStateError(LsfmgrError):
    """현재 상태에서 허용되지 않는 명령 — 전제조건 위반의 base.

    "지금은 이 명령을 할 수 없다"(활성 job 존재, submit/kill 진행 중, 전원
    terminal 아님 등)를 뜻한다. 원인을 프로그램에서 다루기 쉽도록 `jobset_id`와
    걸린 `job_keys`(있으면)를 함께 담는다 — GUI가 메시지 파싱 없이 어느 job이
    막았는지 알 수 있다."""

    def __init__(self, message: str, *,
                 jobset_id: Optional[str] = None,
                 job_keys: Optional[list] = None):
        super().__init__(message)
        self.jobset_id = jobset_id
        self.job_keys = list(job_keys) if job_keys else []


class SubmitNotAllowedError(JobSetStateError):
    """submit 불가 — 활성(진행 중) job 존재 / 제출할 job 없음 / submit·kill
    진행 중. `mgr.can_submit(js)`로 사전 확인할 수 있다.
    (제출 subprocess 실행 자체의 실패는 SubmitError — 별개)."""


class JobEditNotAllowedError(JobSetStateError):
    """job 목록 편집(add_jobs/replace_jobs/upsert_jobs) 불가.

    ① submit·kill 진행 중 — 진행 중인 사이클의 스냅샷과 어긋나므로 거부한다.
    ② 교체 대상 job이 활성(진행 중) — force=True면 레코드만 강제 교체하되,
       LSF에 살아있는 job의 정리는 caller 책임이 된다(레코드가 사라져 그
       job_id를 조회할 수 없으므로 삭제 직전 WARNING 로그가 유일한 흔적).

    (입력 자체의 오류는 예외가 다르다: job_key 중복/누락 = ValueError,
     replace 대상 부재 = JobNotFoundError.)"""


class RemoveNotAllowedError(JobSetStateError):
    """remove_jobs/clear_jobs 불가 — 활성(진행 중) job은 삭제 거부.
    force=True면 레코드만 강제 삭제(LSF job 정리는 caller 책임)."""


class RemoveJobSetNotAllowedError(JobSetStateError):
    """remove_jobset 불가 — 전원 terminal이 아님. force=True면 강제 삭제."""


class JobSetNotFoundError(LsfmgrError):
    """존재하지 않는 jobset_id 접근 — 삭제된(remove_jobset) jobset도
    여기에 해당한다."""


class JobSetRemovedError(LsfmgrError):
    """삭제되어 파괴된 JobSet 핸들 접근 (remove_jobset)."""


class JobNotFoundError(LsfmgrError):
    """jobset 내에 없는 job_key 접근."""


#: 대상 job/jobset 레코드가 **동시 삭제로 사라졌다**는 신호 묶음.
#: worker의 부기(전이/마킹/계상) 경로는 이를 장애가 아니라 정상 소실로
#: 흡수한다 — submitter/killer가 공유하는 단일 정의.
RECORD_GONE = (JobNotFoundError, JobSetNotFoundError)


class LsfCommandError(LsfmgrError):
    """LSF 조회/kill 명령 실행 실패 (제출 실패는 SubmitError)."""

    def __init__(self, message: str, returncode: Optional[int] = None,
                 stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


class SubmitError(LsfmgrError):
    """제출(wrapper 실행) 실패. fail_reason은 JobRecord.fail_reason으로
    그대로 기록된다 (분류명 BSUB_*는 과거 호환 유지)."""

    def __init__(self, message: str, fail_reason: str,
                 returncode: Optional[int] = None, stderr: str = "",
                 stdout: str = "", retryable: bool = True):
        super().__init__(message)
        self.fail_reason = fail_reason
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout
        # 재시도 대상 여부. wrapper 제출 경로는 '비정상 종료(non-zero)'만
        # 재시도하고, 파싱 실패/timeout 은 중복 제출 위험 때문에 재시도하지 않는다.
        self.retryable = retryable

    def diagnostic(self) -> str:
        """터미널에서 봤을 실패 메시지 원문 — JobRecord.fail_message 저장용.
        stderr 우선, 파싱 실패처럼 stdout에 단서가 있으면 함께 담는다."""
        parts = [t for t in (self.stderr.strip(), self.stdout.strip()) if t]
        return "\n".join(parts) or str(self)


class ArgMaxExceededError(LsfmgrError):
    """단일 chunk가 ARG_MAX 한도를 초과 — chunk_size 조정 필요."""
