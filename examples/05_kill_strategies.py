#!/usr/bin/env python3
"""예제 05 — kill 전략 데모 (FR-3: 전체 / 부분 / verify).

- 전체 kill: 부착물(LSF group) 기반 bkill 1회 — 수천 개도 명령 1회
- 부분 kill: only_state=PEND (RUN 중인 것은 유지)
- verify=True: kill 후 재조회로 실제 종료 확인 (KillReport.still_alive)

실행:  python examples/05_kill_strategies.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtWidgets import (
    QApplication, QHBoxLayout, QLabel, QPlainTextEdit, QPushButton,
    QVBoxLayout, QWidget,
)

from lsfmgr import JobState
from mock_lsf import SimulatedLsf
from common import format_summary, install_logging, make_manager, maybe_autoquit

N = 200


class Window(QWidget):
    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.js = None
        self.setWindowTitle("lsfmgr — kill 전략")

        self.summary = QLabel("...")
        self.log = QPlainTextEdit(readOnly=True)
        btn_submit = QPushButton(f"{N}개 submit")
        btn_kill_pend = QPushButton("PEND만 kill")
        btn_kill_all = QPushButton("전체 kill")
        btn_kill_verify = QPushButton("전체 kill + verify")

        top = QHBoxLayout()
        for b in (btn_submit, btn_kill_pend, btn_kill_all, btn_kill_verify):
            top.addWidget(b)
        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(self.summary)
        lay.addWidget(self.log)

        btn_submit.clicked.connect(self.submit)
        btn_kill_pend.clicked.connect(
            lambda: self.kill(only_state=JobState.PEND))
        btn_kill_all.clicked.connect(lambda: self.kill())
        btn_kill_verify.clicked.connect(lambda: self.kill(verify=True))

    def submit(self):
        self.js = self.mgr.submit([f"long_sim {i}" for i in range(N)],
                                  label="kill_demo")
        self.js.start_polling(interval_s=1)
        self.js.updated.connect(
            lambda s: self.summary.setText(format_summary(s)))
        self.js.killed.connect(self.on_killed)
        self.log.appendPlainText(f"submit: jobset {self.js.id}")

    def kill(self, **kw):
        if self.js is None:
            self.log.appendPlainText("먼저 submit 하세요")
            return
        self.js.kill(**kw)                    # 비동기 — 결과는 killed Signal
        self.log.appendPlainText(f"kill 요청 {kw or '(전체)'} ...")

    def on_killed(self, rpt):
        self.log.appendPlainText(
            f"  KillReport: 대상 {rpt.requested}개, LSF 호출 "
            f"{rpt.command_calls}회, 전략 {rpt.strategies}"
            + (f", 잔존 {rpt.still_alive}개" if rpt.still_alive is not None
               else "")
            + (f", 오류 {rpt.errors}" if rpt.errors else ""))
        self.js.refresh()


app = QApplication(sys.argv)
install_logging()
# RUN이 오래 지속되는 시뮬레이터 → kill 대상이 살아있는 상태를 관찰 가능
mgr, sim = make_manager(SimulatedLsf(pend_s=(1, 5), run_s=(20, 40)))
win = Window(mgr)
win.resize(640, 420)
win.show()
maybe_autoquit(app)
app.exec()
