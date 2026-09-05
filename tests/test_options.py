"""옵션 3단 계층 테스트 (v7 §1.2, 수용 기준 15) — Qt 불필요."""
from __future__ import annotations

import pytest
from lsfmgr import LsfConfig

from lsfmgr.options import (
    BUILTIN_DEFAULTS,
    Options,
    parse_retry_backoff,
    resolve_options,
    validate_options,
    SHARED_KEYS,
    MANAGER_ONLY_KEYS,
)


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("key", ["submit_timeout_s", "internal_refresh_min_s",
                                 "internal_retention_days", "workers"])
def test_nonfinite_config_and_options_are_rejected(key, value):
    with pytest.raises(ValueError):
        LsfConfig(**{key: value})
    with pytest.raises(ValueError):
        validate_options({key: value}, allowed=SHARED_KEYS | MANAGER_ONLY_KEYS,
                         where="manager")


@pytest.mark.parametrize("value", ["inf", "-inf", "nan"])
def test_nonfinite_retry_backoff_is_rejected(value):
    with pytest.raises(ValueError):
        LsfConfig(retry_backoff=float(value))
    for kind in ("fixed", "expo"):
        with pytest.raises(ValueError):
            parse_retry_backoff(f"{kind}:{value}")


# ----------------------------------------------------------------------
# 우선순위: 내장 기본값 < manager(②) < call(③)
# ----------------------------------------------------------------------
def test_builtin_defaults_only():
    opts = resolve_options({}, {})
    assert opts.workers == 8
    assert opts.max_retry == 3
    assert opts.retry_backoff == "fixed:2"
    assert opts.poll_interval_s == 10.0
    assert opts.auto_poll is True


def test_manager_layer_overrides_builtin():
    opts = resolve_options({"workers": 32, "max_retry": 5}, {})
    assert opts.workers == 32
    assert opts.max_retry == 5
    assert opts.poll_interval_s == 10.0        # 미지정은 내장 기본값 유지


def test_call_layer_overrides_manager():
    manager_defaults = {"workers": 32, "max_retry": 5}
    opts = resolve_options(manager_defaults, {"workers": 8, "max_retry": 0})
    assert opts.workers == 8
    assert opts.max_retry == 0                 # 0 == 재시도 없음


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


def test_kill_does_not_go_through_option_resolution():
    """kill의 옵션 경로는 kwargs가 아니라 verify 인자 하나다.

    예전엔 resolve_options에 "kill" context가 있었지만 라이브러리 안에서
    아무도 부르지 않는 죽은 분기였다(이 테스트만 살려두고 있었다).
    실제 규칙: ②LsfConfig(verify_kill=…)가 기본이고, kill(verify=…)가 덮는다.
    """
    import inspect

    from lsfmgr import LsfConfig, LsfJobManager

    # kill은 **kwargs를 받지 않는다 — 옵션 3단 계층의 ③이 아예 없다
    params = inspect.signature(LsfJobManager.kill).parameters
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD
                   for p in params.values())
    assert "verify" in params
    # ②는 LsfConfig로 들어온다
    assert LsfConfig(verify_kill=True).verify_kill is True


# ----------------------------------------------------------------------
# 범위 검증 → ValueError
# ----------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [
    {"workers": 0}, {"workers": 65},
    {"max_retry": -1},
    {"poll_interval_s": 4}, {"poll_interval_s": 61},
    {"retry_backoff": "linear:3"}, {"retry_backoff": "fixed"},
    {"submit_timeout_s": -1},
])
def test_range_violation_valueerror(kwargs):
    with pytest.raises(ValueError):
        resolve_options({}, kwargs)


def test_progress_throttle_option_validation():
    """progress throttle 옵션 검증 + config 반영."""
    from lsfmgr import LsfConfig
    with pytest.raises(ValueError):
        LsfConfig(progress_min_step_ratio=2.0)      # 0~1 초과
    with pytest.raises(ValueError):
        LsfConfig(progress_min_interval_s=-0.1)      # 음수


def test_config_retry_backoff_string_rejected():
    """LsfConfig.retry_backoff는 숫자 — 'fixed:N' 문자열(Options/kwarg 형식)을
    잘못 넣으면 예전엔 통과 후 manager 생성 시 str<=float 크래시였다. 이제
    생성 시점에 명확한 ValueError."""
    from lsfmgr import LsfConfig
    with pytest.raises(ValueError):
        LsfConfig(retry_backoff="fixed:2")
    # 숫자는 float로 정규화
    assert LsfConfig(retry_backoff=2).retry_backoff == 2.0
    assert isinstance(LsfConfig(retry_backoff=1).retry_backoff, float)
    cfg = LsfConfig(progress_min_interval_s=0.5, progress_min_step_ratio=0.1)
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
    assert opts.workers == 8                   # 나머지 해석은 정상


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


def test_removed_rate_limit_is_rejected_everywhere():
    """rate_limit_per_s는 완전히 제거됐다(v11) — 경고-무시가 아니라 거부다.

    조용히 무시하면 앱은 초당 상한이 걸린 줄 알고 계속 그 값을 넘긴다.
    제출 부하 노브는 이제 workers 하나뿐이다."""
    import pytest

    from lsfmgr import LsfConfig
    from lsfmgr.options import DEPRECATED_KEYS, SHARED_KEYS, resolve_options

    assert "rate_limit_per_s" not in SHARED_KEYS
    assert "rate_limit_per_s" not in DEPRECATED_KEYS
    with pytest.raises(TypeError):
        resolve_options({}, {"rate_limit_per_s": 20})
    with pytest.raises(TypeError):
        LsfConfig(rate_limit_per_s=20)


def test_range_rules_agree_between_config_and_options():
    """같은 필드의 허용 범위를 두 계층이 따로 들고 있으면 어긋난다.

    실제로 어긋났었다: LsfConfig는 조용히 보정(clamp)하고 옵션 계층은 거부해서
    LsfConfig(chunk_size=0)이 500이 됐다. 이제 범위 규칙의 소유자는
    config.NUMERIC_RANGES 하나이고, 두 경로가 같은 값을 같게 판정해야 한다.
    """
    import pytest

    from lsfmgr.config import NUMERIC_RANGES, LsfConfig
    from lsfmgr.options import (
        MANAGER_ONLY_KEYS, SHARED_KEYS, validate_options,
    )

    # poll_interval_s만 의도적으로 다르다 — 상위 계층은 5~60이라는 **UX 정책**
    # 범위를 더 좁게 건다(저수준 LsfConfig는 양수만 본다: 빠른 로컬 폴링 허용).
    POLICY_DIFFERS = {"poll_interval_s"}

    def config_ok(name, value):
        try:
            LsfConfig(**{name: value})
            return True
        except ValueError:
            return False

    def options_ok(name, value):
        allowed = SHARED_KEYS | MANAGER_ONLY_KEYS
        if name not in allowed:
            return None                       # 그 계층에 없는 필드
        try:
            validate_options({name: value}, allowed=allowed, where="x")
            return True
        except (ValueError, TypeError):
            return False

    mismatches = []
    for name, (cast, lo, hi, incl) in NUMERIC_RANGES.items():
        if name in POLICY_DIFFERS:
            continue
        probes = [lo if incl else lo + 1, -1, 0]
        if hi is not None:
            probes += [hi, hi + 1]
        for v in probes:
            v = cast(v)
            a, b = config_ok(name, v), options_ok(name, v)
            if b is not None and a != b:
                mismatches.append(
                    f"{name}={v}: LsfConfig={'허용' if a else '거부'} / "
                    f"옵션={'허용' if b else '거부'}")
    assert not mismatches, "\n".join(mismatches)


def test_verify_kill_is_a_config_not_a_submit_option():
    """verify_kill은 앱 전역 정책 — LsfConfig/생성자로만 준다.

    예전엔 SHARED_KEYS에 있어 세 경로가 제각각이었다:
      submit(verify_kill=True)     → 통과 후 **조용히 무시**(kill만 읽는데
                                     그건 manager 레벨 값이라 닿지 않음)
      LsfJobManager(verify_kill=T) → 동작
      LsfConfig(verify_kill=True)  → TypeError (필드 자체가 없었다)
    이제 LsfConfig 필드 하나가 소유하고, kill(verify=…)가 호출별로 덮는다.
    """
    import pytest

    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
    from tests.fake_lsf import FakeLsf

    assert LsfConfig(verify_kill=True).verify_kill is True
    mgr = LsfJobManager(store=InMemoryStore(), runner=FakeLsf(), verify_kill=True)
    try:
        assert mgr.config.verify_kill is True          # 생성자 경로도 config로
    finally:
        mgr.shutdown()
    with pytest.raises(TypeError):                     # 무시가 아니라 거부
        resolve_options({}, {"verify_kill": True})


def test_config_verify_kill_actually_verifies(qtbot, fake_lsf):
    """설정이 실제로 kill 동작을 바꾸는지 — 읽히지 않으면 위 테스트가
    통과해도 무의미하다. still_alive는 검증했을 때만 값이 있다(안 하면 None)."""
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager

    def run(**cfg):
        mgr = LsfJobManager(store=InMemoryStore(),
                            config=LsfConfig(**cfg), runner=fake_lsf)
        try:
            js = mgr.create_jobset(["mytool a.sp"], job_keys=["k"])
            with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
                mgr.submit(js, auto_poll=False)
            with qtbot.waitSignal(mgr.kill_finished, timeout=20000) as blk:
                mgr.kill(js)
            return blk.args[1]
        finally:
            mgr.shutdown()

    assert run().still_alive is None                       # 기본 = 미검증
    assert run(verify_kill=True).still_alive is not None    # 설정이 먹는다
