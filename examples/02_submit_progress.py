#!/usr/bin/env python3
"""예제 02 — 대량 submit 진행률 + 취소 (QProgressBar, QT-5/QT-6).

- js.progress → QProgressBar (throttled — UI 폭주 없음)
- js.cancel() → 안전 지점에서 중단, 이미 submit된 job은 유지
- rate_limit_per_s 로 일부러 느리게 해서 취소해 볼 시간을 만든다

실행:  python examples/02_submit_progress.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtWidgets import (
    QApplication, QLabel, QProgressBar, QPushButton, QVBoxLayout, QWidget,
)

from common import format_summary, install_logging, make_manager, maybe_autoquit

N = 300


class Window(QWidget):
    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.js = None
        self.setWindowTitle("lsfmgr — submit 진행률/취소")

        self.bar = QProgressBar()
        self.status = QLabel("대기 중")
        self.btn_start = QPushButton(f"{N}개 submit 시작")
        self.btn_cancel = QPushButton("취소")
        self.btn_cancel.setEnabled(False)

        lay = QVBoxLayout(self)
        lay.addWidget(self.status)
        lay.addWidget(self.bar)
        lay.addWidget(self.btn_start)
        lay.addWidget(self.btn_cancel)

        self.btn_start.clicked.connect(self.start)
        self.btn_cancel.clicked.connect(self.cancel)

    def start(self):
        self.btn_start.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        # 초당 40건으로 제한 → 취소해볼 시간이 생긴다 (NFR-4 rate limit)
        self.js = self.mgr.submit([f"sim job_{i}" for i in range(N)],
                                  rate_limit_per_s=40, workers=4)
        self.js.progress.connect(self.on_progress)
        self.js.finished.connect(self.on_finished)
        self.js.updated.connect(
            lambda s: self.status.setText(format_summary(s)))
        self.status.setText(f"submit 중... jobset={self.js.id}")

    def cancel(self):
        if self.js is not None:
            self.js.cancel()                      # QT-6 안전 중단
            self.status.setText("취소 요청됨 — job 경계에서 중단")

    def on_progress(self, done, total):
        self.bar.setMaximum(total)
        self.bar.setValue(done)

    def on_finished(self, rpt):
        self.btn_cancel.setEnabled(False)
        self.btn_start.setEnabled(True)
        self.status.setText(
            f"완료: ok={rpt.ok} failed={rpt.failed} cancelled={rpt.cancelled} "
            f"retried={rpt.retried} ({rpt.duration_s:.1f}s)")


app = QApplication(sys.argv)
install_logging()
mgr, sim = make_manager()
win = Window(mgr)
win.resize(420, 180)
win.show()
maybe_autoquit(app)
app.exec()                                        # shutdown은 AUTO-3가 처리
