"""옵션 3단 계층 해석 — defaults → manager kwargs → call kwargs (§1.2, v7).

- 해석은 resolve_options() 한 함수로 일원화, frozen Options 반환
- 알 수 없는 키워드 → TypeError (오타 조기 발견)
- 범위 검증 위반 → ValueError
- Qt 비의존 순수 Python.
"""
from __future__ import annotations

import logging
import math

from .config import NUMERIC_RANGES, _in_range, validate_cmd_path
from dataclasses import dataclass, fields
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("lsfmgr.options")   # 모듈 로거 (코드베이스 관례)

# manager·call 공통 옵션
SHARED_KEYS = frozenset({
    "workers", "max_retry", "retry_backoff",
    "poll_interval_s", "auto_poll",
    "submit_timeout_s",
})
#: 제거된 옵션은 기존 앱 호환을 위해 경고 후 무시한다.
#: 제출 옵션은 wrapper 커맨드에, jobset 메타데이터는 create_jobset에 지정한다.
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
    "verify_kill",
    "arg_max",
    "bjobs_path", "bkill_path",
    "kill_status_policy", "kill_chunk_size", "kill_workers",
    "kill_max_retry", "kill_retry_delay_s",
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
    poll_interval_s: float = 10.0
    auto_poll: bool = True
    submit_timeout_s: float = 30.0

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
    if kind not in ("fixed", "expo") or not (math.isfinite(base) and base >= 0):
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


# 옵션 검증기: 새 옵션은 허용 키 집합과 검증기에 함께 등록한다.
def _float_in(lo: float, hi: float):
    label = f"{lo:g}~{hi:g}"

    def check(key: str, value: Any) -> float:
        v = float(value)
        if not (lo <= v <= hi):                  # NaN도 거른다
            raise ValueError(f"{key}는 {label} (got {value})")
        return v
    return check


def _bool(_key: str, value: Any) -> bool:
    return bool(value)


def _backoff(_key: str, value: Any) -> str:
    parse_retry_backoff(value)                   # 형식 검증만
    return str(value)


def _opt_float_min(lo: float = 0.0):
    """None 허용 float 하한 검증 — None은 '기본값에 맡김'."""
    def check(key: str, value: Any) -> Optional[float]:
        if value is None:
            return None
        return _in_range(value, key, float, lo, None, True)
    return check


def _policy(key: str, value: Any) -> str:
    if value not in ("optimistic", "actual"):
        raise ValueError(f"{key}는 optimistic/actual (got {value!r})")
    return value


def _cmd_path(key: str, value: Any) -> Any:
    validate_cmd_path(value, key)
    return value


#: 수치 검증기는 config.NUMERIC_RANGES에서 파생한다. 나머지 형식은 아래에서 정의한다.
#: 검증기 없는 값은 통과하며, wrapper 치환 구조는 LsfConfig에서 검증한다.
def _range_check(name: str):
    cast, lo, hi, incl = NUMERIC_RANGES[name]

    def check(key: str, value: Any):
        return _in_range(value, key, cast, lo, hi, incl)
    return check


#: poll_interval_s만 상위 계층이 더 좁게 건다 — 5~60은 **UX 정책**이고,
#: 저수준 LsfConfig는 양수만 본다(poll_interval_s=2 같은 빠른 로컬 폴링 허용).
#: 유일한 의도적 예외라 여기 한 줄로 드러낸다.
MIN_POLL_INTERVAL_S, MAX_POLL_INTERVAL_S = 5.0, 60.0

_VALIDATORS: Dict[str, Any] = {name: _range_check(name)
                               for name in NUMERIC_RANGES}
_VALIDATORS.update({
    "poll_interval_s": _float_in(MIN_POLL_INTERVAL_S, MAX_POLL_INTERVAL_S),
    "retry_backoff": _backoff,
    "auto_poll": _bool,
    "verify_kill": _bool,
    "poll_runtime_updates": _bool,
    "submit_finished_on_gate_reject": _bool,
    "collect_clusters": _bool,
    "bjobs_path": _cmd_path,
    "bkill_path": _cmd_path,
    "kill_status_policy": _policy,
    "internal_refresh_min_s": _opt_float_min(0.0),   # None = 폴링 주기에서 유도
})


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


def resolve_options(defaults: Dict[str, Any],
                    call_kwargs: Dict[str, Any]) -> Options:
    """옵션 해석 단일 지점 — ③call kwargs를 defaults(①내장+②manager) 위에.

    kwargs로 옵션을 받는 호출 지점은 submit 하나뿐이다. kill은 kwargs를
    받지 않고 `verify` 인자 하나로 ②를 덮으므로 이 경로를 타지 않는다.
    """
    call = validate_options(call_kwargs, allowed=SHARED_KEYS,
                            where="submit()")

    merged = dict(BUILTIN_DEFAULTS)
    merged.update(defaults)
    merged.update(call)
    valid_fields = {f.name for f in fields(Options)}
    return Options(**{k: v for k, v in merged.items() if k in valid_fields})
