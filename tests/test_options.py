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
    assert opts.workers == 32
    assert opts.max_retry == 3
    assert opts.retry_backoff == "fixed:2"
    assert opts.rate_limit_per_s == 5.0     # bsub 초당 호출 제한(기본 켜짐)
    assert opts.poll_interval_s == 10.0
    assert opts.auto_poll is True
    assert opts.verify_kill is False


def test_manager_layer_overrides_builtin():
    opts = resolve_options({"workers": 32, "max_retry": 5}, {})
    assert opts.workers == 32
    assert opts.max_retry == 5
    assert opts.poll_interval_s == 10.0        # 미지정은 내장 기본값 유지


def test_call_layer_overrides_manager():
    manager_defaults = {"workers": 32, "max_retry": 5,
                        "rate_limit_per_s": 5.0}
    opts = resolve_options(manager_defaults,
                           {"workers": 8, "max_retry": 0,
                            "rate_limit_per_s": 2.0})
    assert opts.workers == 8
    assert opts.max_retry == 0                 # 0 == 재시도 없음
    assert opts.rate_limit_per_s == 2.0


def test_frozen_options():
    import dataclasses
    opts = resolve_options({}, {})
    with pytest.raises(dataclasses.FrozenInstanceError):
        opts.workers = 1                       # type: ignore[misc]


# ----------------------------------------------------------------------
# 알 수 없는 키워드 → TypeError
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
# 범위 검증 → ValueError
# ----------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [
    {"workers": 0}, {"workers": 65},
    {"max_retry": -1},
    {"poll_interval_s": 4}, {"poll_interval_s": 61},
    {"retry_backoff": "linear:3"}, {"retry_backoff": "fixed"},
    {"rate_limit_per_s": 0},
    {"submit_timeout_s": -1},
])
def test_range_violation_valueerror(kwargs):
    with pytest.raises(ValueError):
        resolve_options({}, kwargs)


def test_progress_throttle_option_validation():
    """progress throttle 옵션 검증 + config 반영."""
    from lsfmgr import LsfConfig
    with pytest.raises(ValueError):
        LsfConfig(rate_limit_per_s=None, progress_min_step_ratio=2.0)      # 0~1 초과
    with pytest.raises(ValueError):
        LsfConfig(rate_limit_per_s=None, progress_min_interval_s=-0.1)      # 음수


def test_config_retry_backoff_string_rejected():
    """LsfConfig.retry_backoff는 숫자 — 'fixed:N' 문자열(Options/kwarg 형식)을
    잘못 넣으면 예전엔 통과 후 manager 생성 시 str<=float 크래시였다. 이제
    생성 시점에 명확한 ValueError."""
    from lsfmgr import LsfConfig
    with pytest.raises(ValueError):
        LsfConfig(rate_limit_per_s=None, retry_backoff="fixed:2")
    # 숫자는 float로 정규화
    assert LsfConfig(rate_limit_per_s=None, retry_backoff=2).retry_backoff == 2.0
    assert isinstance(LsfConfig(rate_limit_per_s=None, retry_backoff=1).retry_backoff, float)
    cfg = LsfConfig(rate_limit_per_s=None, progress_min_interval_s=0.5, progress_min_step_ratio=0.1)
    assert cfg.progress_min_interval_s == 0.5
    assert cfg.progress_min_step_ratio == 0.1


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


def test_jobset_meta_kwargs_deprecated():
    """label/tags/description은 submit이 jobset을 만들던 시절(v9 이전)의
    잔재 — 검증만 되고 아무도 읽지 않던 함정이라 경고 후 무시된다.
    jobset 메타는 create_jobset 인자로만 준다."""
    opts = resolve_options({}, {"tags": ["a", "b"], "label": "x",
                                "description": "y"})
    assert not hasattr(opts, "tags")
    assert not hasattr(opts, "label")
    assert opts.workers == 32                  # 나머지 해석은 정상


def test_options_defaults_match_lsfconfig():
    """Options 필드 기본값과 LsfConfig 기본값이 어긋나면, manager를 안 거치는
    resolve_options({}, {}) 경로가 다른 값을 준다 — 둘 다 손으로 유지하다
    한쪽만 고치는 드리프트를 여기서 잡는다(rate_limit_per_s에서 실제로 겪음)."""
    from dataclasses import fields
    from lsfmgr import LsfConfig
    from lsfmgr.options import Options

    # retry_backoff는 표현이 일부러 다르다 — LsfConfig는 지수 밑(float),
    # Options는 v7 옵션 문자열("fixed:2"). manager가 변환해서 넘긴다.
    DIFFERENT_BY_DESIGN = {"retry_backoff"}

    cfg, opts = LsfConfig(), Options()
    shared = ({f.name for f in fields(Options)}
              & {f.name for f in fields(LsfConfig)}) - DIFFERENT_BY_DESIGN
    assert shared, "공유 필드가 하나도 없다 — 이 가드가 무의미해졌다"
    mismatch = {name: (getattr(opts, name), getattr(cfg, name))
                for name in sorted(shared)
                if getattr(opts, name) != getattr(cfg, name)}
    assert not mismatch, f"Options/LsfConfig 기본값 불일치 {{필드: (Options, LsfConfig)}}: {mismatch}"
