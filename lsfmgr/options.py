"""옵션 3단 계층 해석 — defaults → manager kwargs → call kwargs (§1.2, v7).

- OPT-1: 해석은 resolve_options() 한 함수로 일원화, frozen Options 반환
- OPT-2: 알 수 없는 키워드 → TypeError (오타 조기 발견)
- OPT-3: 범위 검증 위반 → ValueError
- Qt 비의존 순수 Python.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Dict, Optional, Tuple

# ----------------------------------------------------------------------
# 옵션 카탈로그 — 적용 계층별 키 집합 (§1.2 표와 1:1)
# ----------------------------------------------------------------------
#: ②(manager)·③(call) 공통 튜닝 옵션
SHARED_KEYS = frozenset({
    "workers", "max_retry", "retry_backoff", "rate_limit_per_s",
    "poll_interval_s", "auto_poll", "queue", "resource_req", "output_dir",
    "submit_timeout_s", "verify_kill",
})
#: ③(call) 전용
CALL_ONLY_KEYS = frozenset({"mode", "label", "tags", "description"})
#: ②(manager) 전용 — Options에 포함되지 않고 config/store 구성에 쓰이는 키
MANAGER_ONLY_KEYS = frozenset({
    "chunk_size", "persistent", "db_path", "default_queue", "lsf_group_root",
    "script_dir", "arg_max",
    "bsub_path", "bjobs_path", "bkill_path", "bhist_path", "bmod_path",
    "bgdel_path",
    "kill_status_policy", "kill_max_retry", "kill_retry_delay_s",
})

#: ① 라이브러리 내장 기본값
BUILTIN_DEFAULTS: Dict[str, Any] = {
    "workers": 16,
    "max_retry": 3,
    "retry_backoff": "fixed:2",
    "rate_limit_per_s": None,
    "poll_interval_s": 10.0,
    "auto_poll": True,
    "queue": "",                 # 빈 문자열 == LSF 기본 queue
    "resource_req": None,
    "output_dir": None,
    "submit_timeout_s": 30.0,
    "chunk_size": 200,
    "verify_kill": False,
    "mode": "auto",
    "label": "",
    "tags": (),
    "description": "",
}


@dataclass(frozen=True)
class Options:
    """1회 호출에 적용될 최종 옵션 (frozen — Signal/스레드 공유 안전)."""
    workers: int = 16
    max_retry: int = 3
    retry_backoff: str = "fixed:2"
    rate_limit_per_s: Optional[float] = None
    poll_interval_s: float = 10.0
    auto_poll: bool = True
    queue: str = ""
    resource_req: Optional[str] = None
    output_dir: Optional[str] = None
    submit_timeout_s: float = 30.0
    chunk_size: int = 200
    verify_kill: bool = False
    mode: str = "auto"
    label: str = ""
    tags: Tuple[str, ...] = ()
    description: str = ""

    def retry_delay_s(self, attempt: int) -> float:
        """attempt번째(0부터) 실패 후 재시도 대기 시간 (FR-2.2)."""
        kind, base = parse_retry_backoff(self.retry_backoff)
        if kind == "fixed":
            return base
        return base * (2.0 ** attempt)          # expo


def parse_retry_backoff(value: str) -> Tuple[str, float]:
    """'fixed:N' | 'expo:base' → (kind, seconds). 형식 오류 시 ValueError."""
    try:
        kind, num = value.split(":", 1)
        base = float(num)
    except (ValueError, AttributeError):
        raise ValueError(
            f"retry_backoff 형식 오류: {value!r} — 'fixed:N' 또는 'expo:N'")
    if kind not in ("fixed", "expo") or base < 0:
        raise ValueError(
            f"retry_backoff 형식 오류: {value!r} — 'fixed:N' 또는 'expo:N'")
    return kind, base


# ----------------------------------------------------------------------
# 검증 (OPT-3)
# ----------------------------------------------------------------------
def _validate(key: str, value: Any) -> Any:
    """옵션 1개 검증/정규화. 위반 시 ValueError."""
    if key == "workers":
        v = int(value)
        if not 1 <= v <= 32:
            raise ValueError(f"workers는 1~32 (got {value})")
        return v
    if key == "max_retry":
        v = int(value)
        if v < 0:
            raise ValueError(f"max_retry는 0 이상 (got {value})")
        return v
    if key == "retry_backoff":
        parse_retry_backoff(value)               # 형식 검증만
        return str(value)
    if key == "rate_limit_per_s":
        if value is not None and float(value) <= 0:
            raise ValueError(f"rate_limit_per_s는 양수 또는 None (got {value})")
        return None if value is None else float(value)
    if key == "poll_interval_s":
        v = float(value)
        if not 5.0 <= v <= 60.0:
            raise ValueError(f"poll_interval_s는 5~60 (got {value})")
        return v
    if key == "submit_timeout_s":
        v = float(value)
        if v <= 0:
            raise ValueError(f"submit_timeout_s는 양수 (got {value})")
        return v
    if key == "chunk_size":
        v = int(value)
        if not 1 <= v <= 5000:
            raise ValueError(f"chunk_size는 1~5000 (got {value})")
        return v
    if key == "mode":
        if value not in ("auto", "array", "bulk"):
            raise ValueError(f"mode는 auto/array/bulk (got {value!r})")
        return value
    if key == "kill_status_policy":
        if value not in ("optimistic", "actual"):
            raise ValueError(
                f"kill_status_policy는 optimistic/actual (got {value!r})")
        return value
    if key == "kill_max_retry":
        v = int(value)
        if v < 0:
            raise ValueError(f"kill_max_retry는 0 이상 (got {value})")
        return v
    if key == "kill_retry_delay_s":
        v = float(value)
        if v < 0:
            raise ValueError(f"kill_retry_delay_s는 0 이상 (got {value})")
        return v
    if key in ("auto_poll", "verify_kill"):
        return bool(value)
    if key == "tags":
        if isinstance(value, str):
            return (value,)               # tuple("ab") == ('a','b') 방지
        return tuple(value)
    if key in ("label", "description", "queue"):
        return str(value)
    return value                                 # resource_req/output_dir 등


def validate_options(kwargs: Dict[str, Any], *, allowed: frozenset,
                     where: str) -> Dict[str, Any]:
    """키 집합 검증(OPT-2) + 값 검증(OPT-3) 후 정규화된 dict 반환."""
    out: Dict[str, Any] = {}
    for key, value in kwargs.items():
        if key not in allowed:
            raise TypeError(
                f"{where}: 알 수 없는 옵션 {key!r} — 사용 가능: "
                f"{', '.join(sorted(allowed))}")
        out[key] = _validate(key, value)
    return out


def resolve_options(defaults: Dict[str, Any], call_kwargs: Dict[str, Any], *,
                    context: str = "submit") -> Options:
    """OPT-1 — 옵션 해석 단일 지점.

    defaults(①내장+②manager가 이미 merge된 값) 위에 ③call kwargs를 덮어
    frozen Options를 만든다. context에 따라 허용 키가 다르다:
    - "submit": 공통 + mode/label/tags/description
    - "kill":   verify_kill만
    """
    if context == "submit":
        allowed = SHARED_KEYS | CALL_ONLY_KEYS
    elif context == "kill":
        allowed = frozenset({"verify_kill"})
    else:
        allowed = SHARED_KEYS
    call = validate_options(call_kwargs, allowed=allowed,
                            where=f"{context}()")

    merged = dict(BUILTIN_DEFAULTS)
    merged.update(defaults)
    merged.update(call)
    valid_fields = {f.name for f in fields(Options)}
    return Options(**{k: v for k, v in merged.items() if k in valid_fields})
