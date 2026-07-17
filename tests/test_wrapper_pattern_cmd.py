"""submit_wrapper_pattern_cmd — wrapper 제출 프로그램 치환.

wrapper 경로는 argv를 그대로 실행하므로(프로그램명이 커맨드에 박혀 있다)
bsub_path 같은 노브가 없다. 이 옵션은 argv[0]만 갈아끼워 커맨드를 안 고치고
mock 실행 파일로 돌린다. 옵션을 줄지 말지(테스트 환경에서만 켜기 등)는
호출자가 정한다 — 라이브러리는 환경을 읽지 않는다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.conftest import submit_cmds

MOCK = "/mock/bin/customwrapper_sub"


def _make(fake_lsf, **kwargs):
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0, max_retry=0,
                    **kwargs)
    return LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)


def _submit_wrapper(mgr, qtbot, cmd="mytool_sub -q normal a.sp"):
    with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
        js = submit_cmds(mgr, [cmd], wrapper=True, auto_poll=False)
    return js


# ----------------------------------------------------------------------
# 치환
# ----------------------------------------------------------------------
def test_pattern_match_replaces_program(qtbot, fake_lsf):
    """argv[0]만 대체되고 나머지 인자는 그대로 — 그 결과 실제로 제출된다."""
    mgr = _make(fake_lsf, submit_wrapper_pattern_cmd=("*_sub", MOCK))
    try:
        js = _submit_wrapper(mgr, qtbot)
        # mock wrapper(basename customwrapper_sub)가 실행되어 job_id 파싱까지 성공
        rec = mgr.get_jobs(js.id)[0]
        assert rec.state is JobState.PEND
        subs = fake_lsf.calls_of("customwrapper_sub")
        assert len(subs) == 1
        assert subs[0] == [MOCK, "-q", "normal", "a.sp"]
        # 원본 프로그램은 실행되지 않았다
        assert fake_lsf.calls_of("mytool_sub") == []
    finally:
        mgr.shutdown()


def test_record_keeps_original_command(qtbot, fake_lsf):
    """치환은 실행만 바꾼다 — 레코드의 command는 원본(표시·재제출 기준)."""
    mgr = _make(fake_lsf, submit_wrapper_pattern_cmd=("*_sub", MOCK))
    try:
        js = _submit_wrapper(mgr, qtbot)
        assert mgr.get_jobs(js.id)[0].command == "mytool_sub -q normal a.sp"
    finally:
        mgr.shutdown()


def test_token_list_prepends_fixed_args(qtbot, fake_lsf):
    """대체값이 토큰 목록이면 고정 인자가 앞에 붙는다 (bsub_path와 같은 규약)."""
    mgr = _make(fake_lsf,
                submit_wrapper_pattern_cmd=("*_sub", [MOCK, "--mock"]))
    try:
        _submit_wrapper(mgr, qtbot)
        assert fake_lsf.calls_of("customwrapper_sub")[0] == [
            MOCK, "--mock", "-q", "normal", "a.sp"]
    finally:
        mgr.shutdown()


def test_matches_on_basename_not_full_path(qtbot, fake_lsf):
    """커맨드가 경로째로 와도 같은 규칙이 걸린다 — basename으로 매칭."""
    mgr = _make(fake_lsf, submit_wrapper_pattern_cmd=("*_sub", MOCK))
    try:
        _submit_wrapper(mgr, qtbot, cmd="/prod/bin/mytool_sub -q normal a.sp")
        assert fake_lsf.calls_of("customwrapper_sub")[0][0] == MOCK
    finally:
        mgr.shutdown()


def test_non_matching_program_untouched(qtbot, fake_lsf):
    """패턴에 안 맞는 프로그램은 원본 그대로 실행된다 (치환은 *_sub만)."""
    mgr = _make(fake_lsf, submit_wrapper_pattern_cmd=("*_sub", MOCK))
    try:
        _submit_wrapper(mgr, qtbot, cmd="mytool_run -x a.sp")
        assert fake_lsf.calls_of("mytool_run")[0] == ["mytool_run", "-x",
                                                      "a.sp"]
        assert fake_lsf.calls_of("customwrapper_sub") == []
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 기본값 — 옵션을 안 주면 종전과 동일 (앱이 '안 주기'로 끄는 경로)
# ----------------------------------------------------------------------
def test_no_option_is_noop(qtbot, fake_lsf):
    """옵션 미지정이면 wrapper는 원본 그대로 실행된다."""
    mgr = _make(fake_lsf)
    try:
        _submit_wrapper(mgr, qtbot)
        assert fake_lsf.calls_of("mytool_sub")[0] == ["mytool_sub", "-q",
                                                      "normal", "a.sp"]
        assert fake_lsf.calls_of("customwrapper_sub") == []
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# 형식 검증 — 생성 시점에 잡는다
# ----------------------------------------------------------------------
@pytest.mark.parametrize("bad", [
    "*_sub",                      # 2-튜플이 아님
    ("*_sub",),                   # 길이 1
    ("*_sub", MOCK, "extra"),     # 길이 3
    ("", MOCK),                   # 빈 패턴
    ("*_sub", ""),                # 빈 명령
    ("*_sub", []),                # 빈 토큰 목록
    ("*_sub", ["ok", ""]),        # 빈 토큰 포함
    ("*_sub", 3),                 # 타입 오류
    (3, MOCK),                    # 패턴 타입 오류
])
def test_invalid_option_rejected(bad):
    with pytest.raises(ValueError):
        LsfConfig(submit_wrapper_pattern_cmd=bad)


def test_manager_kwarg_and_typo(fake_lsf):
    """②(생성자) 계층 옵션 — kwarg로도 받고 오타는 TypeError (OPT-2)."""
    mgr = LsfJobManager(store=InMemoryStore(), runner=fake_lsf,
                        submit_wrapper_pattern_cmd=("*_sub", MOCK))
    try:
        assert mgr.config.submit_wrapper_pattern_cmd == ("*_sub", MOCK)
    finally:
        mgr.shutdown()
    with pytest.raises(TypeError):
        LsfJobManager(submit_wrapper_patern_cmd=("*_sub", MOCK))
