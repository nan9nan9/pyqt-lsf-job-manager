"""신호 slot 안에서 manager API를 다시 불러도 안전하다.

GUI는 늘 이렇게 쓴다 — jobs_updated에서 표를 그리다 summary를 읽고,
submit_finished에서 곧바로 kill을 누르고, kill_finished에서 폴링을 멈춘다.
Qt는 slot을 빠져나온 예외를 **프로세스 abort**로 처리하므로, 재진입 한 번이
크래시가 된다. 모든 신호 × 여러 API 조합(144개)을 프로브로 훑어 확인했고,
여기서는 그 결과를 한 번의 수명주기로 압축해 회귀만 지킨다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager

SIGNALS = ["submit_started", "submit_progress", "submit_finished",
           "jobs_updated", "jobset_updated", "kill_started", "kill_finished",
           "kill_progress", "jobset_finished", "post_processing_started",
           "post_processing_finished", "handler_finished",
           "pre_submit_started", "pre_submit_finished", "error_occurred"]


def test_calling_apis_from_every_slot_is_safe(qtbot, fake_lsf):
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=5.0, min_state_dwell_s=0.0),
        runner=fake_lsf)
    errors, seen = [], set()
    try:
        js = mgr.create_jobset([f"mytool {i}.sp" for i in range(4)],
                               job_keys=[f"k{i}" for i in range(4)])

        # 부작용 없는(멱등) API만 — submit 재진입은 '진행 중 거부'가 정상이라
        # 여기서 섞으면 정상 거부와 결함을 구분할 수 없다(별도 테스트 소관).
        def probe(name):
            def slot(*a):
                seen.add(name)
                try:
                    mgr.summary(js)
                    js.jobs()
                    mgr.is_submitting(js)
                    mgr.is_killing(js)
                    mgr.list_jobsets()
                    mgr.jobset(js.id)
                    mgr.query_once(js)
                    mgr.start_polling(js, 5.0)
                    mgr.stop_polling(js)
                except Exception as e:              # noqa: BLE001
                    errors.append(f"{name}: {type(e).__name__}: {e}")
            return slot

        for name in SIGNALS:
            getattr(mgr, name).connect(probe(name))

        mgr.add_handler(js, "h", lambda c: None)
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=True, pre_submit=lambda cmds: True,
                       post_process=lambda r: None)
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000):
            mgr.kill(js)
        qtbot.wait(500)

        assert not errors, "\n".join(errors)
        # 실제로 여러 신호를 타고 들어갔는지 — 통과가 무의미해지지 않게
        assert len(seen) >= 8, f"발화된 신호가 너무 적다: {sorted(seen)}"
    finally:
        mgr.shutdown()


def test_resubmit_from_slot_is_rejected_not_crashed(qtbot, fake_lsf):
    """진행 중 재제출은 '거부'다 — 크래시도, 조용한 무시도 아니다."""
    from lsfmgr.errors import SubmitNotAllowedError

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(), runner=fake_lsf)
    caught = []
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["k"])

        def on_started():
            try:
                mgr.submit(js, auto_poll=False)
            except SubmitNotAllowedError as e:
                caught.append(str(e))

        mgr.submit_started.connect(on_started)
        with qtbot.waitSignal(mgr.submit_finished, timeout=20000):
            mgr.submit(js, auto_poll=False)
        assert caught, "진행 중 재제출이 거부되지 않았다"
    finally:
        mgr.shutdown()
