"""wrapper 커맨드 지원 유틸 검증 (v10: 제출은 wrapper 단일 경로 —
CmdPath 토큰 규약과 chunk base_len 계산만 남는다)."""
from __future__ import annotations

from lsfmgr import LsfConfig
from lsfmgr.command import LsfCommand
from lsfmgr.config import cmd_tokens


def test_cmd_tokens_str_and_list():
    assert cmd_tokens("bsub") == ["bsub"]
    assert cmd_tokens(["customwrapper_sub", "--proj", "X"]) == \
        ["customwrapper_sub", "--proj", "X"]


def test_wrapper_argmax_accounts_prefix(fake_lsf):
    """chunk base_len이 wrapper 토큰 총 길이를 반영 (ARG_MAX 안전)."""
    cfg = LsfConfig(rate_limit_per_s=None, bkill_path=["bkill", "--force"])
    cmd = LsfCommand(cfg, runner=fake_lsf)
    assert cmd._prog_len(cfg.bkill_path) == len("bkill") + 1 \
        + len("--force") + 1
