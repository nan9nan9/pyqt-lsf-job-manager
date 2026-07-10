"""명령별 로깅 — INFO 착수/완료 라인이 해당 로거로 발행되는지.

각 명령(submit/kill/resubmit)의 착수·완료가 대응 로거로 INFO 발행되어,
INFO만 켜도 명령 흐름이 로거별로 추적되는지 검증한다.
"""
from __future__ import annotations

import logging

from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# INFO 착수/완료 — 명령별 로거
# ----------------------------------------------------------------------
def test_submit_logs_start_and_finish(qtbot, manager, fake_lsf, caplog):
    with caplog.at_level(logging.INFO, logger="lsfmgr.submit"):
        with qtbot.waitSignal(manager.submit_finished, timeout=10000):
            submit_cmds(manager, ["echo a", "echo b"], auto_poll=False)
    text = "\n".join(r.message for r in caplog.records
                     if r.name == "lsfmgr.submit")
    assert "submit 착수" in text
    assert "submit 완료" in text


def test_kill_logs_start_and_finish(qtbot, manager, fake_lsf, caplog):
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    with caplog.at_level(logging.INFO, logger="lsfmgr.kill"):
        with qtbot.waitSignal(manager.kill_finished, timeout=10000):
            manager.kill(js.id)
    msgs = [r.message for r in caplog.records if r.name == "lsfmgr.kill"]
    assert any(m.startswith("kill 착수") for m in msgs), msgs
    assert any(m.startswith("kill 완료") for m in msgs), msgs
