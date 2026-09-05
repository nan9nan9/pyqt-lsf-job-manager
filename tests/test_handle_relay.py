"""Facade Signal → JobSet 핸들 Signal 중계가 **하나도 빠지지 않는다**.

핸들 신호는 manager의 중계 목록에 이름을 적어야 나간다. 신호를 새로 만들면서
목록에 추가하는 것을 잊으면 아무 오류 없이 **핸들에서만 조용히 안 나간다** —
앱이 js.kill_failed를 연결해두고 영영 못 받는 식이다. 목록을 눈으로 맞추는
대신, 핸들의 모든 Signal을 실제로 발화시켜 확인한다.
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.handle import JobSet
from lsfmgr.reports import KillReport

# 핸들 Signal 이름 → Facade 쪽 emit 인자(jsid 뒤에 붙는 것들)
FACADE_ARGS = {
    "jobset_updated":           ({"total": 1},),
    "jobs_updated":             ([],),
    "submit_started":           (),
    "submit_progress":          (1, 2),
    "submit_finished":          (object(),),
    "kill_started":             (),
    "kill_finished":            (KillReport(jobset_id="", requested=0),),
    "kill_progress":            (1, 2),
    "kill_failed":              ("사유",),
    "error_occurred":           ("오류",),
    "handler_finished":         ("h", object()),
    "pre_submit_started":       (),
    "pre_submit_finished":      (True,),
    "jobset_finished":          ({},),
    "post_processing_started":  (),
    "post_processing_finished": (object(),),
}
# Facade에 대응 신호가 없고 다른 신호에서 파생되는 것
DERIVED = {"jobs_failed": "jobs_updated에서 실패분만 걸러 파생"}


def _handle_signal_names():
    from qtpy.QtCore import Signal
    return {n for n, v in vars(JobSet).items() if type(v).__name__ in
            ("Signal", "pyqtSignal", "SignalInstance") or isinstance(v, type(Signal()))}


def test_arg_table_covers_every_handle_signal():
    """새 핸들 Signal이 생기면 이 표에 추가하도록 강제한다."""
    names = _handle_signal_names()
    assert names, "핸들 Signal을 하나도 못 찾았다 — 이 가드가 무의미해졌다"
    missing = names - set(FACADE_ARGS) - set(DERIVED)
    assert not missing, f"표에 없는 핸들 Signal: {sorted(missing)}"


@pytest.mark.parametrize("name", sorted(FACADE_ARGS))
def test_every_facade_signal_reaches_the_handle(name, qtbot, fake_lsf):
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(), runner=fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["k"])
        assert hasattr(mgr, name), f"Facade에 {name} 신호가 없다"
        got = []
        getattr(js, name).connect(lambda *a: got.append(a))
        getattr(mgr, name).emit(js.id, *FACADE_ARGS[name])
        qtbot.wait(20)
        assert got, f"js.{name}이 중계되지 않았다 — manager의 중계 목록 확인"
    finally:
        mgr.shutdown()


def test_removed_handle_stops_receiving(qtbot, fake_lsf):
    """삭제된 jobset의 핸들로는 중계하지 않는다(죽은 위젯에 대한 발화 방지)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(), runner=fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["k"])
        got = []
        js.kill_failed.connect(lambda s: got.append(s))
        mgr.remove_jobset(js, force=True)   # 미제출 job이 있어도 삭제
        mgr.kill_failed.emit(js.id, "사유")
        qtbot.wait(20)
        assert not got
    finally:
        mgr.shutdown()


def test_is_removed_never_raises(qtbot, fake_lsf):
    """삭제된 핸들인지 묻는 것만은 예외를 던지면 안 된다.

    Qt는 slot을 빠져나온 예외를 abort로 처리한다. 지연 콜백이 삭제 뒤에
    도착하는 건 흔한 일인데, 확인할 방법이 '예외를 일으켜 보는 것'뿐이면
    앱은 모든 핸들 접근을 try로 감싸야 하고 하나만 빠뜨려도 프로세스가 죽는다.
    """
    from lsfmgr.errors import JobSetRemovedError

    mgr = LsfJobManager(store=InMemoryStore(), config=LsfConfig(), runner=fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["k"])
        assert js.is_removed is False
        mgr.remove_jobset(js, force=True)
        assert js.is_removed is True         # 예외 없이 답한다
        assert "removed" in repr(js)
        with pytest.raises(JobSetRemovedError):
            js.summary                       # 나머지는 종전대로 거부
    finally:
        mgr.shutdown()
