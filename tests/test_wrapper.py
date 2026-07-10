"""bsub wrapper(예: customwrapper_sub) 지원 검증.

실제 환경에서는 bsub를 직접 부르지 않고, 전처리 후 bsub를 호출하는 wrapper
스크립트로 submit한다. wrapper가 bsub 출력을 그대로 뱉으므로 job_id 파싱과
이후 추적(group/name/id)은 bsub와 동일하게 동작해야 한다.
"""
from __future__ import annotations

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand
from lsfmgr.config import cmd_tokens
from lsfmgr.states import JobState


def _make(fake_lsf, tmp_path, **kwargs):
    cfg = LsfConfig(retry_delay_s=0.05, retry_backoff=1.0,
                    script_dir=str(tmp_path / "scripts"), **kwargs)
    return LsfJobManager(store=InMemoryStore(), config=cfg, runner=fake_lsf)


def test_cmd_tokens_str_and_list():
    assert cmd_tokens("bsub") == ["bsub"]
    assert cmd_tokens(["customwrapper_sub", "--proj", "X"]) == \
        ["customwrapper_sub", "--proj", "X"]


def test_submit_through_wrapper_parses_jobid(qtbot, fake_lsf, tmp_path):
    """wrapper로 submit해도 bsub 출력에서 job_id를 파싱해 PEND로 전이."""
    mgr = _make(fake_lsf, tmp_path,
                bsub_path=["customwrapper_sub", "--proj", "demo"])
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000) as blk:
            js = mgr.submit([f"run {i}" for i in range(3)], mode="bulk")
        _, report = blk.args
        assert report.succeeded == 3
        recs = mgr.get_jobs(js.id)
        assert all(r.state is JobState.PEND for r in recs)
        assert all(r.job_id is not None for r in recs)

        # wrapper 프로그램 + 자기 인자로 호출되고, 표준 bsub 옵션도 전달됨
        subs = fake_lsf.calls_of("customwrapper_sub")
        assert len(subs) == 3
        first = subs[0]
        assert first[:3] == ["customwrapper_sub", "--proj", "demo"]
        assert "-J" in first and "-g" in first      # 추적용 부착물 전달
    finally:
        mgr.shutdown()


def test_wrapper_submit_then_group_kill(qtbot, fake_lsf, tmp_path):
    """wrapper submit 후 group 기반 kill(표준 bkill)까지 end-to-end."""
    mgr = _make(fake_lsf, tmp_path, bsub_path=["customwrapper_sub"])
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
            js = mgr.submit([f"run {i}" for i in range(4)], mode="bulk")
        with qtbot.waitSignal(mgr.kill_finished, timeout=15000) as blk:
            mgr.kill(js.id)
        _, report = blk.args
        assert report.requested == 4
        # bkill은 wrapper가 아니라 표준 명령을 그대로 사용
        assert fake_lsf.calls_of("bkill")
    finally:
        mgr.shutdown()


def test_wrapper_via_manager_kwarg(qtbot, fake_lsf, tmp_path):
    """config뿐 아니라 manager kwarg로도 wrapper 지정 가능."""
    mgr = LsfJobManager(store=InMemoryStore(), runner=fake_lsf,
                        bsub_path=["customwrapper_sub", "--proj", "kw"],
                        retry_backoff="fixed:0.05")
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
            mgr.submit("echo hi")
        assert fake_lsf.calls_of("customwrapper_sub")
    finally:
        mgr.shutdown()


def test_wrapper_argmax_accounts_prefix(fake_lsf):
    """chunk base_len이 wrapper 토큰 총 길이를 반영 (ARG_MAX 안전)."""
    cfg = LsfConfig(bkill_path=["bkill", "--force"])
    cmd = LsfCommand(cfg, runner=fake_lsf)
    assert cmd._prog_len(cfg.bkill_path) == len("bkill") + 1 \
        + len("--force") + 1
