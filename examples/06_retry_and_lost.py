#!/usr/bin/env python3
"""예제 06 — 실패 처리 데모: retry / SUBMIT_FAILED / LOST / failed Signal.

시뮬레이터에 실패를 주입한다:
- submit_fail_rate=0.4 → bsub 40% 실패 → RETRY_WAIT → 재시도 (FR-2)
- lost_rate=0.1        → job 10%가 흔적 없이 소실 → bhist fallback → LOST
- exit_rate=0.3        → 30%는 exit != 0 (EXIT)

js.failed Signal로 SUBMIT_FAILED/EXIT/LOST 변경분이 배치로 도착하고,
요약 합계는 항상 total과 일치한다 (불변식 — 손실 job도 반드시 집계됨).

실행:  python examples/06_retry_and_lost.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtWidgets import (
    QApplication, QLabel, QPlainTextEdit, QPushButton, QVBoxLayout, QWidget,
)

from mock_lsf import SimulatedLsf
from common import format_summary, install_logging, make_manager, maybe_autoquit

N = 80


class Window(QWidget):
    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.setWindowTitle("lsfmgr — retry / LOST / failed Signal")

        self.summary = QLabel("...")
        self.log = QPlainTextEdit(readOnly=True)
        btn_submit = QPushButton(f"{N}개 submit (실패 주입 40%)")
        btn_lost = QPushButton("detect_lost() — 손실 감지/복구")

        lay = QVBoxLayout(self)
        lay.addWidget(btn_submit)
        lay.addWidget(self.summary)
        lay.addWidget(self.log)
        lay.addWidget(btn_lost)

        btn_submit.clicked.connect(self.submit)
        btn_lost.clicked.connect(self.detect_lost)
        self.js = None

    def submit(self):
        # retry_backoff는 call 옵션으로 이번 호출만 빠르게
        self.js = self.mgr.submit([f"flaky_sim {i}" for i in range(N)],
                             max_retry=3, retry_backoff="fixed:0.3",
                             workers=8)
        self.js.start_polling(interval_s=1)
        self.js.updated.connect(
            lambda s: self.summary.setText(format_summary(s)))
        self.js.failed.connect(self.on_failed)
        self.js.finished.connect(lambda rpt: self.log.appendPlainText(
            f"submit 최종: ok={rpt.ok} failed={rpt.failed} "
            f"retried={rpt.retried} 사유={dict(rpt.fail_reasons)}"))
        self.log.appendPlainText(f"submit: jobset {self.js.id}")

    def on_failed(self, records):
        """SUBMIT_FAILED/EXIT/LOST 변경분 배치 (README §3.1)."""
        for r in records[:8]:
            self.log.appendPlainText(
                f"  [failed] {r.lsf_job_name}: {r.state.value}"
                f" (reason={r.fail_reason}, exit={r.exit_code},"
                f" retry={r.retry_count})")
        if len(records) > 8:
            self.log.appendPlainText(f"  ... 외 {len(records) - 8}건")

    def detect_lost(self):
        if self.js is None:
            return
        lost = self.js.detect_lost()          # name 패턴 복구 시도 포함
        self.log.appendPlainText(
            f"detect_lost: 이번에 LOST 확정 {len(lost)}건")


app = QApplication(sys.argv)
install_logging()
sim = SimulatedLsf(pend_s=(0.2, 1.0), run_s=(0.5, 3.0),
                   submit_fail_rate=0.4, lost_rate=0.1, exit_rate=0.3)
mgr, _ = make_manager(sim)
win = Window(mgr)
win.resize(680, 480)
win.show()
maybe_autoquit(app)
app.exec()
