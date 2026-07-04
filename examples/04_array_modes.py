#!/usr/bin/env python3
"""예제 04 — AUTO-4 mode 자동 선택 데모 (array vs bulk).

버튼 4개로 각 경로를 실행하고 실제 bsub 호출 횟수를 비교한다:
  ① 인덱스 패턴 list  → auto가 array 선택 ($LSB_JOBINDEX 치환, bsub 1회)
  ② 단일 command+count → array (bsub 1회)
  ③ 서로 다른 command  → auto가 bulk 선택 (bsub N회)
  ④ 다른 command 강제 array → dispatch 스크립트 (bsub 1회)

실행:  python examples/04_array_modes.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtWidgets import (
    QApplication, QLabel, QPlainTextEdit, QPushButton, QVBoxLayout, QWidget,
)

from common import install_logging, make_manager, maybe_autoquit

N = 50


class Window(QWidget):
    def __init__(self, mgr, sim):
        super().__init__()
        self.mgr = mgr
        self.sim = sim
        self.setWindowTitle("lsfmgr — AUTO-4 array/bulk 자동 선택")

        self.log = QPlainTextEdit(readOnly=True)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("각 버튼을 눌러 bsub 호출 횟수를 비교해 보세요"))

        cases = [
            ("① 패턴 list → auto=array",
             lambda: self.mgr.submit([f"hspice tt_{i}.sp"
                                      for i in range(1, N + 1)])),
            ("② 단일 command + count → array",
             lambda: self.mgr.submit("run_sim.sh $LSB_JOBINDEX", count=N)),
            ("③ 상이 command → auto=bulk",
             lambda: self.mgr.submit([f"tool_{i % 3} input_{i * 7}.dat"
                                      for i in range(N)])),
            ("④ 상이 command 강제 array (dispatch)",
             lambda: self.mgr.submit([f"tool_{i % 3} input_{i * 7}.dat"
                                      for i in range(N)], mode="array")),
        ]
        for title, fn in cases:
            btn = QPushButton(title)
            btn.clicked.connect(lambda _=False, t=title, f=fn: self.run(t, f))
            lay.addWidget(btn)
        lay.addWidget(self.log)

    def run(self, title, submit_fn):
        before = self.sim.bsub_calls if self.sim else 0
        js = submit_fn()
        js.finished.connect(
            lambda rpt, t=title, b=before, h=js: self.report(t, b, h, rpt))
        self.log.appendPlainText(f"{title}  → jobset {js.id} submit...")

    def report(self, title, before, js, rpt):
        calls = (self.sim.bsub_calls - before) if self.sim else -1
        recs = js.jobs()
        arrays = {r.job_id for r in recs if r.array_index is not None}
        self.log.appendPlainText(
            f"    bsub {calls}회 / job {len(recs)}개 / "
            f"array_id={sorted(arrays) if arrays else '없음(bulk)'} / "
            f"ok={rpt.ok} failed={rpt.failed}\n")


app = QApplication(sys.argv)
install_logging()
mgr, sim = make_manager()
win = Window(mgr, sim)
win.resize(560, 480)
win.show()
maybe_autoquit(app)
app.exec()
