#!/usr/bin/env python3
"""Signal 연결 데모 GUI — README §5 'GUI 통합 규칙'의 패턴을 1:1로 구현.

basic_example.py(통합 대시보드)와 달리, 이 데모는 **위젯별로 어떤 Signal을
어떻게 연결하는가**만 최소 코드로 보여준다 (README §5.2 표와 같은 구성):

    요약 배지    ← js.jobset_updated(summary)
    job 테이블   ← js.jobs_updated([JobRecord])   # 변경분만 — 해당 행만 갱신
    진행 바      ← js.submit_progress / js.kill_progress
    상태(스피너) ← js.kill_started 켜고 js.kill_finished / js.submit_finished 끄기
    실패 알림    ← js.jobs_failed / js.error_occurred

실행 (mocklsf 가상 LSF — 실제 LSF는 LSFMGR_REAL=1):
    python examples/gui_demo.py
스모크 (headless — 기동 후 자동 submit→kill→종료):
    LSFMGR_DEMO_AUTORUN=1 LSFMGR_DEMO_AUTOQUIT=12 \
        QT_QPA_PLATFORM=offscreen python examples/gui_demo.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from qtpy.QtCore import QTimer
from qtpy.QtGui import QColor
from qtpy.QtWidgets import (
    QApplication, QHBoxLayout, QLabel, QProgressBar, QPushButton,
    QTableWidget, QTableWidgetItem, QTextEdit, QVBoxLayout, QWidget,
)

from common import (DEFAULT_WRAPPER, format_summary, install_logging,
                    make_manager, maybe_autoquit, wrapper)

N_JOBS = 12

#: 상태별 셀 배경 — 표에서 전이가 한눈에 보이게
STATE_COLORS = {
    "SUBMITTING": "#ffe8c2", "PEND": "#cfe3ff", "RUN": "#c9f0c9",
    "DONE": "#e6e6e6", "EXIT": "#ffc9c9", "SUBMIT_FAILED": "#ffc9c9",
    "RETRY_WAIT": "#fff3b0", "LOST": "#e0c9ff", "CREATED": "#f5f5f5",
}


class DemoWindow(QWidget):
    """README §5.2의 '위젯별 권장 신호' 표를 그대로 코드로 옮긴 창."""

    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.js = None                    # 현재 JobSet 핸들
        self._rows = {}                   # job_key → 테이블 row (행 단위 갱신)
        self.setWindowTitle("lsfmgr Signal 연결 데모")
        self.resize(760, 520)

        # --- 위젯 구성 ---------------------------------------------------
        self.btn_submit = QPushButton(f"Submit {N_JOBS}")
        self.btn_kill = QPushButton("Kill")
        self.btn_cancel = QPushButton("Cancel submit")
        self.btn_resubmit = QPushButton("Resubmit 실패분")
        self.badge = QLabel("(요약 없음)")          # ← jobset_updated
        self.status = QLabel("대기 중")             # ← *_started / *_finished
        self.bar = QProgressBar()                   # ← submit/kill_progress
        self.table = QTableWidget(0, 5)             # ← jobs_updated (변경 행만)
        self.table.setHorizontalHeaderLabels(
            ["job_key", "state", "job_id", "exit", "fail_reason"])
        self.log = QTextEdit(readOnly=True)         # ← jobs_failed/error

        top = QHBoxLayout()
        for b in (self.btn_submit, self.btn_kill, self.btn_cancel,
                  self.btn_resubmit):
            top.addWidget(b)
        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(self.badge)
        lay.addWidget(self.bar)
        lay.addWidget(self.status)
        lay.addWidget(self.table, stretch=3)
        lay.addWidget(self.log, stretch=1)

        self.btn_submit.clicked.connect(self.submit)
        self.btn_kill.clicked.connect(self.kill)
        self.btn_cancel.clicked.connect(self.cancel)
        self.btn_resubmit.clicked.connect(self.resubmit_failed)

    # --- 사용자 명령 (비동기 — 결과는 전부 Signal로) ----------------------
    def submit(self):
        cmds = [wrapper(DEFAULT_WRAPPER, "-q", "normal", f"run_{i}.sp")
                for i in range(N_JOBS)]
        js = self.mgr.submit_wrapper(cmds, label="gui-demo", auto_poll=False)
        js.start_polling(interval_s=1.0)   # 데모용 빠른 폴링 (기본 10s)
        self._attach(js)
        self.status.setText(f"submit 접수 — jobset {js.id}")

    def kill(self):
        if self.js is not None:
            self.js.kill()                # 착수 통지는 kill_started로 도착

    def cancel(self):
        if self.js is not None:
            self.js.cancel()              # 미제출분 CREATED 복귀 (QT-6)

    def resubmit_failed(self):
        if self.js is None:
            return
        failed = [r.job_key for r in self.js.jobs() if r.state.is_failed]
        if failed:
            self.js.resubmit_jobs(failed)
            self.status.setText(f"resubmit 접수 — {len(failed)}건")

    # --- Signal 연결 (README §5.2 표의 구현) -----------------------------
    def _attach(self, js):
        """새 JobSet 핸들에 위젯을 연결한다 — 핸들 Signal이라 jsid 필터 불필요."""
        self.js = js
        self.table.setRowCount(0)
        self._rows.clear()

        js.jobset_updated.connect(self._on_summary)        # 요약 배지
        js.jobs_updated.connect(self._apply_changed)       # 표: 변경 행만
        js.submit_progress.connect(self._on_progress)      # 진행 바
        js.kill_progress.connect(self._on_progress)
        js.submit_finished.connect(
            lambda rep: self.status.setText(
                f"submit 완료 — 성공 {rep.succeeded} / 실패 {rep.failed}"
                f" / 취소 {rep.cancelled}"))
        # kill: 접수 즉시 kill_started(동기) → 완료 시 kill_finished.
        # 진행 중 submit 정지 대기(quiesce)로 완료가 늦어도 UI는 바로 반응.
        js.kill_started.connect(
            lambda: self.status.setText("kill 접수 — 진행 중..."))
        js.kill_finished.connect(
            lambda rep: self.status.setText(
                f"kill 완료 — 요청 {rep.requested}건"
                + (f", 오류 {len(rep.errors)}건" if rep.errors else "")))
        js.jobs_failed.connect(self._on_failed)            # 실패 알림
        js.error_occurred.connect(
            lambda msg: self.log.append(f"[error] {msg}"))

    # --- slot들 (전부 main 스레드 — 위젯 직접 갱신 OK) --------------------
    def _on_summary(self, summary: dict):
        self.badge.setText(format_summary(summary))

    def _apply_changed(self, records: list):
        """변경분 배치만 반영 — 표 전체 리로드 금지 (README §5.3).
        job_key로 행을 찾아 그 행만 갱신하고, 처음 보는 key는 추가한다."""
        for rec in records:
            row = self._rows.get(rec.job_key)
            if row is None:
                row = self.table.rowCount()
                self.table.insertRow(row)
                self._rows[rec.job_key] = row
            values = (rec.job_key, rec.state.name,
                      "" if rec.job_id is None else str(rec.job_id),
                      "" if rec.exit_code is None else str(rec.exit_code),
                      rec.fail_reason or "")
            color = QColor(STATE_COLORS.get(rec.state.name, "#ffffff"))
            for col, text in enumerate(values):
                item = QTableWidgetItem(text)
                item.setBackground(color)
                self.table.setItem(row, col, item)

    def _on_progress(self, done: int, total: int):
        self.bar.setMaximum(max(total, 1))
        self.bar.setValue(done)

    def _on_failed(self, records: list):
        for r in records:
            self.log.append(f"[failed] {r.job_key}: {r.state.name}"
                            f" ({r.fail_reason or '-'})")


def main():
    app = QApplication(sys.argv)
    install_logging()
    mgr, _ = make_manager()
    win = DemoWindow(mgr)
    win.show()

    # 스모크/데모 자동 실행: 기동 1초 후 submit → 4초 후 kill.
    if os.environ.get("LSFMGR_DEMO_AUTORUN") == "1":
        QTimer.singleShot(1000, win.submit)
        QTimer.singleShot(4000, win.kill)
    maybe_autoquit(app)                   # LSFMGR_DEMO_AUTOQUIT=<초>

    rc = app.exec_() if hasattr(app, "exec_") else app.exec()
    mgr.shutdown()
    return rc


if __name__ == "__main__":
    sys.exit(main())
