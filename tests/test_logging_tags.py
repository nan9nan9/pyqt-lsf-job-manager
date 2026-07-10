"""명령별 로깅 — INFO 착수/완료 라인(로거별) + DEBUG 상위 명령 태그.

- 각 명령(submit/kill/resubmit)의 착수·완료가 해당 로거로 INFO 발행되는지
- 모든 LSF subprocess DEBUG 로그에 [submit]/[kill]/[poll]/[resubmit] 태그가
  붙어 어느 명령이 실행했는지 구분되는지
"""
from __future__ import annotations

import logging

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand
from lsfmgr.states import JobState


# ----------------------------------------------------------------------
# DEBUG 태그 — command 레이어 단위 (operation 컨텍스트)
# ----------------------------------------------------------------------
def test_run_debug_tagged_by_operation(fake_lsf, caplog):
    """operation() 블록 안의 _run DEBUG 로그에 태그가 붙고, 밖은 기본 cmd."""
    cmd = LsfCommand(config=LsfConfig(), runner=fake_lsf)
    with caplog.at_level(logging.DEBUG, logger="lsfmgr.command"):
        with cmd.operation("kill"):
            cmd.bkill_by_ids([1000])
        cmd.bjobs_by_ids([1000])              # 태그 밖 → cmd

    lines = [r.message for r in caplog.records if "실행:" in r.message]
    assert any(m.startswith("[kill] 실행:") for m in lines), lines
    assert any(m.startswith("[cmd] 실행:") for m in lines), lines


def test_operation_restores_previous_tag(fake_lsf, caplog):
    """중첩 operation은 빠져나올 때 이전 태그를 복원한다."""
    cmd = LsfCommand(config=LsfConfig(), runner=fake_lsf)
    with caplog.at_level(logging.DEBUG, logger="lsfmgr.command"):
        with cmd.operation("poll"):
            with cmd.operation("kill"):
                cmd.bkill_by_ids([1000])
            cmd.bjobs_by_ids([1000])          # 복원되어 poll
    msgs = [r.message for r in caplog.records if "실행:" in r.message]
    assert any(m.startswith("[kill] 실행:") for m in msgs)
    assert any(m.startswith("[poll] 실행:") for m in msgs)


# ----------------------------------------------------------------------
# INFO 착수/완료 — 명령별 로거
# ----------------------------------------------------------------------
def test_submit_logs_start_and_finish(qtbot, manager, fake_lsf, caplog):
    with caplog.at_level(logging.INFO, logger="lsfmgr.submit"):
        with qtbot.waitSignal(manager.submit_finished, timeout=10000):
            manager.submit(["echo a", "echo b"], mode="bulk", auto_poll=False)
    text = "\n".join(r.message for r in caplog.records
                     if r.name == "lsfmgr.submit")
    assert "submit 착수" in text
    assert "submit 완료" in text


def test_kill_logs_start_and_finish(qtbot, manager, fake_lsf, caplog):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = manager.submit(["echo a"], mode="bulk", auto_poll=False)
    with caplog.at_level(logging.INFO, logger="lsfmgr.kill"):
        with qtbot.waitSignal(manager.kill_finished, timeout=10000):
            manager.kill_jobset(js.id)
    msgs = [r.message for r in caplog.records if r.name == "lsfmgr.kill"]
    assert any(m.startswith("kill 착수") for m in msgs), msgs
    assert any(m.startswith("kill 완료") for m in msgs), msgs


def test_submit_bsub_debug_tagged_submit(qtbot, config, fake_lsf, caplog):
    """실제 submit 경로에서 bsub DEBUG 로그가 [submit]으로 태깅된다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        with caplog.at_level(logging.DEBUG, logger="lsfmgr.command"):
            with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
                mgr.submit(["echo a"], mode="bulk", auto_poll=False)
        submit_runs = [r.message for r in caplog.records
                       if r.message.startswith("[submit] 실행:")]
        assert submit_runs, "bsub DEBUG가 [submit]으로 태깅되지 않음"
    finally:
        mgr.shutdown()
