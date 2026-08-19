"""옵션 3단 계층 해석 — defaults → manager kwargs → call kwargs (§1.2, v7).

- 해석은 resolve_options() 한 함수로 일원화, frozen Options 반환
- 알 수 없는 키워드 → TypeError (오타 조기 발견)
- 범위 검증 위반 → ValueError
- Qt 비의존 순수 Python.
"""
from __future__ import annotations

import logging

from .config import validate_cmd_path
from dataclasses import dataclass, fields
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("lsfmgr.options")   # 모듈 로거 (코드베이스 관례)

# ----------------------------------------------------------------------
# 옵션 카탈로그 — 적용 계층별 키 집합 (§1.2 표와 1:1)
# ----------------------------------------------------------------------
#: ②(manager)·③(call) 공통 튜닝 옵션
SHARED_KEYS = frozenset({
    "workers", "max_retry", "retry_backoff", "rate_limit_per_s",
    "poll_interval_s", "auto_poll",
    "submit_timeout_s", "verify_kill",
})
#: 제거된 옵션 — 받으면 TypeError 대신 경고 후 무시 (기존 앱 하위 호환).
#: script_dir: array dispatch 제거(v9)로 무용.
#: queue/resource_req/output_dir/default_queue/lsf_group_root/bsub_path/
#: bgdel_path: bsub 인자 조립 제출·group 부착물 제거(v10)로 무용 —
#: 제출 옵션은 wrapper 커맨드 문자열에 직접 쓴다.
#: bhist_path: bhist 조회(LOST 판정 fallback·상세 원문) 제거(v10.3)로 무용.
#: label/tags/description: submit이 jobset을 만들던 시절(v9 이전)의 메타
#: 인자 — jobset 메타는 create_jobset 인자다. 받아서 검증만 하고 아무도
#: 읽지 않던 함정이라 경고-무시로 강등.
DEPRECATED_KEYS = frozenset({
    "script_dir",
    "queue", "resource_req", "output_dir",
    "default_queue", "lsf_group_root", "bsub_path", "bgdel_path",
    "bhist_path",
    "label", "tags", "description",
})

#: ②(manager) 전용 — Options에 포함되지 않고 config/store 구성에 쓰이는 키
MANAGER_ONLY_KEYS = frozenset({
    "chunk_size",
    "arg_max",
    "bjobs_path", "bkill_path",
    "kill_status_policy", "kill_max_retry", "kill_retry_delay_s",
    "progress_min_interval_s", "progress_min_step_ratio",
    "poll_runtime_updates", "submit_finished_on_gate_reject",
    "lost_after_missing_polls",
    "internal_refresh_min_s", "internal_retention_days",
    "internal_lost_grace_s",
    "collect_clusters", "min_state_dwell_s",
    "test_submit_wrapper_pattern_cmd",
})

#: 재시도 대기 상한(1일) — QTimer int32(ms) 한도(~24.8일) 안쪽으로 clamp
MAX_RETRY_DELAY_S = 86400.0


@dataclass(frozen=True)
class Options:
    """1회 호출에 적용될 최종 옵션 (frozen — Signal/스레드 공유 안전)."""
    workers: int = 8
    max_retry: int = 3
    retry_backoff: str = "fixed:2"
    rate_limit_per_s: Optional[float] = 20.0
    poll_interval_s: float = 10.0
    auto_poll: bool = True
    submit_timeout_s: float = 30.0
    verify_kill: bool = False

    def retry_delay_s(self, attempt: int) -> float:
        """attempt번째(0부터) 실패 후 재시도 대기 시간.

        MAX_RETRY_DELAY_S로 clamp — QTimer.singleShot의 ms 인자는 int32라
        약 24.8일을 넘으면 OverflowError가 나고, slot 안 예외는 PyQt에서
        abort로 이어진다 (expo:2는 attempt 21부터 한도 초과)."""
        kind, base = parse_retry_backoff(self.retry_backoff)
        if kind == "fixed":
            return min(base, MAX_RETRY_DELAY_S)
        if attempt > 62:                        # 2.0**attempt float 오버플로 방지
            return MAX_RETRY_DELAY_S if base > 0 else 0.0
        return min(base * (2.0 ** attempt), MAX_RETRY_DELAY_S)   # expo


def parse_retry_backoff(value: str) -> Tuple[str, float]:
    """'fixed:N' | 'expo:base' → (kind, seconds). 형식 오류 시 ValueError."""
    try:
        kind, num = value.split(":", 1)
        base = float(num)
    except (ValueError, AttributeError):
        raise ValueError(
            f"retry_backoff 형식 오류: {value!r} — 'fixed:N' 또는 'expo:N'")
    if kind not in ("fixed", "expo") or not (base >= 0):
        # not(base>=0): 음수뿐 아니라 NaN도 거른다 — NaN이 통과하면
        # retry_delay_s가 NaN이 되어 QTimer ms 인자로 흘러든다
        raise ValueError(
            f"retry_backoff 형식 오류: {value!r} — 'fixed:N' 또는 'expo:N'")
    return kind, base


#: ① 라이브러리 내장 기본값 — Options 필드 기본값에서 **파생**한다.
#: 별도 dict를 손으로 이중 유지하면 필드 추가/변경 때 드리프트가 생긴다.
BUILTIN_DEFAULTS: Dict[str, Any] = {
    f.name: getattr(Options(), f.name) for f in fields(Options)
}


# ----------------------------------------------------------------------
# 검증 — 옵션별 검증기 레지스트리 (선언적).
# 새 옵션은 키 집합(SHARED/MANAGER_ONLY)과 여기 한 줄이면 끝난다 —
# if-체인 시절엔 분기 추가를 빠뜨리면 검증 없이 통과했다.
# ----------------------------------------------------------------------
def _int_in(lo: int, hi: Optional[int] = None):
    """정수 범위 검증기 — hi=None이면 하한만."""
    label = f"{lo}~{hi}" if hi is not None else f"{lo} 이상"

    def check(key: str, value: Any) -> int:
        v = int(value)
        if v < lo or (hi is not None and v > hi):
            raise ValueError(f"{key}는 {label} (got {value})")
        return v
    return check


def _float_in(lo: float, hi: float):
    label = f"{lo:g}~{hi:g}"

    def check(key: str, value: Any) -> float:
        v = float(value)
        if not (lo <= v <= hi):                  # NaN도 거른다
            raise ValueError(f"{key}는 {label} (got {value})")
        return v
    return check


def _float_min(lo: float = 0.0, *, positive: bool = False):
    label = "양수" if positive else f"{lo:g} 이상"

    def check(key: str, value: Any) -> float:
        v = float(value)
        if v < lo or (positive and v <= 0):
            raise ValueError(f"{key}는 {label} (got {value})")
        return v
    return check


def _bool(_key: str, value: Any) -> bool:
    return bool(value)


def _backoff(_key: str, value: Any) -> str:
    parse_retry_backoff(value)                   # 형식 검증만
    return str(value)


def _rate_limit(key: str, value: Any) -> Optional[float]:
    if value is not None and float(value) <= 0:
        raise ValueError(f"{key}는 양수 또는 None (got {value})")
    return None if value is None else float(value)


def _opt_float_min(lo: float = 0.0):
    """None 허용 float 하한 검증 — None은 '기본값에 맡김'."""
    def check(key: str, value: Any) -> Optional[float]:
        if value is None:
            return None
        v = float(value)
        if v < lo:
            raise ValueError(f"{key}는 {lo:g} 이상 또는 None (got {value})")
        return v
    return check


def _policy(key: str, value: Any) -> str:
    if value not in ("optimistic", "actual"):
        raise ValueError(f"{key}는 optimistic/actual (got {value!r})")
    return value


def _cmd_path(key: str, value: Any) -> Any:
    validate_cmd_path(value, key)
    return value


#: key → 검증기(key, value) -> 정규화 값. 없는 키는 무검증 통과
#: (test_submit_wrapper_pattern_cmd는 LsfConfig.__post_init__가 구조 검증).
_VALIDATORS: Dict[str, Any] = {
    "workers": _int_in(1, 64),
    "max_retry": _int_in(0),
    "retry_backoff": _backoff,
    "rate_limit_per_s": _rate_limit,
    "poll_interval_s": _float_in(5.0, 60.0),
    "submit_timeout_s": _float_min(positive=True),
    "auto_poll": _bool,
    "verify_kill": _bool,
    "chunk_size": _int_in(1, 5000),
    "arg_max": _int_in(4096),
    "bjobs_path": _cmd_path,
    "bkill_path": _cmd_path,
    "kill_status_policy": _policy,
    "kill_max_retry": _int_in(0),
    "kill_retry_delay_s": _float_min(0.0),
    "progress_min_interval_s": _float_min(0.0),
    "progress_min_step_ratio": _float_in(0.0, 1.0),
    "poll_runtime_updates": _bool,
    "submit_finished_on_gate_reject": _bool,
    "lost_after_missing_polls": _int_in(1),
    "internal_refresh_min_s": _opt_float_min(0.0),
    "internal_retention_days": _float_min(0.0),
    "internal_lost_grace_s": _float_min(0.0),
    "collect_clusters": _bool,
    "min_state_dwell_s": _float_min(0.0),
}


def _validate(key: str, value: Any) -> Any:
    """옵션 1개 검증/정규화. 위반 시 ValueError."""
    fn = _VALIDATORS.get(key)
    return fn(key, value) if fn is not None else value


def validate_options(kwargs: Dict[str, Any], *, allowed: frozenset,
                     where: str) -> Dict[str, Any]:
    """키 집합 검증 + 값 검증 후 정규화된 dict 반환."""
    out: Dict[str, Any] = {}
    for key, value in kwargs.items():
        if key in DEPRECATED_KEYS:
            # allowed 검사보다 먼저 — 원래 그 컨텍스트에서 불허였던 키도
            # 경고-무시로 통과한다(키 검증보다 하위 호환 우선). 제거 키를
            # TypeError로 죽이면 기존 앱이 업그레이드만으로 깨진다.
            log.warning(
                "%s: 옵션 %r은 제거됨(v9/v10) — 무시합니다", where, key)
            continue
        if key not in allowed:
            raise TypeError(
                f"{where}: 알 수 없는 옵션 {key!r} — 사용 가능: "
                f"{', '.join(sorted(allowed))}")
        out[key] = _validate(key, value)
    return out


def resolve_options(defaults: Dict[str, Any], call_kwargs: Dict[str, Any], *,
                    context: str = "submit") -> Options:
    """옵션 해석 단일 지점.

    defaults(①내장+②manager가 이미 merge된 값) 위에 ③call kwargs를 덮어
    frozen Options를 만든다. context에 따라 허용 키가 다르다:
    - "submit": 공통(SHARED_KEYS)
    - "kill":   verify_kill만
    """
    if context == "kill":
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
