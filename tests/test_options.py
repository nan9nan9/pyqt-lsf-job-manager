"""옵션 3단 계층 테스트 (v7 §1.2, 수용 기준 15) — Qt 불필요."""
from __future__ import annotations

import pytest

from lsfmgr.options import (
    BUILTIN_DEFAULTS,
    Options,
    parse_retry_backoff,
    resolve_options,
    validate_options,
    SHARED_KEYS,
)


# ----------------------------------------------------------------------
# 우선순위: 내장 기본값 < manager(②) < call(③)
# ----------------------------------------------------------------------
def test_builtin_defaults_only():
    opts = resolve_options({}, {})
    assert opts.workers == 16
    assert opts.max_retry == 3
    assert opts.retry_backoff == "fixed:2"
    assert opts.rate_limit_per_s is None
    assert opts.poll_interval_s == 10.0
    assert opts.auto_poll is True
    assert opts.mode == "auto"
    assert opts.verify_kill is False


def test_manager_layer_overrides_builtin():
    opts = resolve_options({"workers": 32, "max_retry": 5}, {})
    assert opts.workers == 32
    assert opts.max_retry == 5
    assert opts.poll_interval_s == 10.0        # 미지정은 내장 기본값 유지


def test_call_layer_overrides_manager():
    manager_defaults = {"workers": 32, "max_retry": 5, "queue": "priority"}
    opts = resolve_options(manager_defaults,
                           {"workers": 8, "max_retry": 0, "queue": "short"})
    assert opts.workers == 8
    assert opts.max_retry == 0                 # 0 == 재시도 없음
    assert opts.queue == "short"


def test_frozen_options():
    import dataclasses
    opts = resolve_options({}, {})
    with pytest.raises(dataclasses.FrozenInstanceError):
        opts.workers = 1                       # type: ignore[misc]


# ----------------------------------------------------------------------
# OPT-2: 알 수 없는 키워드 → TypeError
# ----------------------------------------------------------------------
def test_unknown_keyword_typeerror():
    with pytest.raises(TypeError, match="wokers"):
        resolve_options({}, {"wokers": 8})     # 오타


def test_manager_only_key_rejected_at_call():
    with pytest.raises(TypeError):
        resolve_options({}, {"chunk_size": 100})   # ②전용을 ③에서 사용


def test_kill_context_allows_only_verify():
    opts = resolve_options({}, {"verify_kill": True}, context="kill")
    assert opts.verify_kill is True
    with pytest.raises(TypeError):
        resolve_options({}, {"workers": 8}, context="kill")


# ----------------------------------------------------------------------
# OPT-3: 범위 검증 → ValueError
# ----------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [
    {"workers": 0}, {"workers": 33},
    {"max_retry": -1},
    {"poll_interval_s": 4}, {"poll_interval_s": 61},
    {"mode": "banana"},
    {"retry_backoff": "linear:3"}, {"retry_backoff": "fixed"},
    {"rate_limit_per_s": 0},
    {"submit_timeout_s": -1},
])
def test_range_violation_valueerror(kwargs):
    with pytest.raises(ValueError):
        resolve_options({}, kwargs)


# ----------------------------------------------------------------------
# retry_backoff 파싱/지연 계산
# ----------------------------------------------------------------------
def test_parse_retry_backoff():
    assert parse_retry_backoff("fixed:2") == ("fixed", 2.0)
    assert parse_retry_backoff("expo:1.5") == ("expo", 1.5)


def test_retry_delay_fixed():
    opts = resolve_options({}, {"retry_backoff": "fixed:3"})
    assert opts.retry_delay_s(0) == 3.0
    assert opts.retry_delay_s(2) == 3.0


def test_retry_delay_expo():
    opts = resolve_options({}, {"retry_backoff": "expo:1"})
    assert opts.retry_delay_s(0) == 1.0
    assert opts.retry_delay_s(1) == 2.0
    assert opts.retry_delay_s(3) == 8.0


# ----------------------------------------------------------------------
# 카탈로그 정합성 — 공통 키는 전부 내장 기본값 보유
# ----------------------------------------------------------------------
def test_all_shared_keys_have_builtin_defaults():
    assert SHARED_KEYS <= set(BUILTIN_DEFAULTS)


def test_tags_normalized_to_tuple():
    opts = resolve_options({}, {"tags": ["a", "b"]})
    assert opts.tags == ("a", "b")
