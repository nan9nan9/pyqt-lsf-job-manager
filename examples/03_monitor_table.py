#!/usr/bin/env python3
"""예제 03 — QTableView 실시간 모니터링 (batch 갱신, QT-4).

- QAbstractTableModel 기반 job 테이블
- mgr.jobs_updated(변경분 리스트)를 배치로 반영 — row 단위 Signal 아님
- 상태별 색상 표시, 요약은 상단 라벨

실행:  python examples/03_monitor_table.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtCore import QAbstractTableModel, QModelIndex, Qt
from qtpy.QtGui import QBrush, QColor
from qtpy.QtWidgets import (
    QApplication, QLabel, QPushButton, QTableView, QVBoxLayout, QWidget,
)

from lsfmgr import JobState
from common import format_summary, install_logging, make_manager, maybe_autoquit

N = 120

_STATE_COLOR = {
    JobState.PEND: QColor("#808080"),
    JobState.RUN: QColor("#1565c0"),
    JobState.DONE: QColor("#2e7d32"),
    JobState.EXIT: QColor("#c62828"),
    JobState.SUBMIT_FAILED: QColor("#c62828"),
    JobState.LOST: QColor("#8e24aa"),
    JobState.RETRY_WAIT: QColor("#ef6c00"),
}


class JobTableModel(QAbstractTableModel):
    """JobRecord 목록 모델 — 변경분 배치 반영."""

    HEADERS = ["job name", "job id", "state", "exit", "retry", "fail reason"]

    def __init__(self):
        super().__init__()
        self._rows = []                    # [JobRecord]
        self._index = {}                   # job_key → row

    # --- Qt 모델 인터페이스 ---
    def rowCount(self, parent=QModelIndex()):
        return len(self._rows)

    def columnCount(self, parent=QModelIndex()):
        return len(self.HEADERS)

    def headerData(self, sec, orient, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orient == Qt.Horizontal:
            return self.HEADERS[sec]
        return None

    def data(self, index, role=Qt.DisplayRole):
        rec = self._rows[index.row()]
        if role == Qt.DisplayRole:
            return [rec.lsf_job_name, rec.job_id, rec.state.value,
                    rec.exit_code, rec.retry_count,
                    rec.fail_reason or ""][index.column()]
        if role == Qt.ForegroundRole:
            color = _STATE_COLOR.get(rec.state)
            return QBrush(color) if color else None
        return None

    # --- 갱신 API ---
    def load(self, records):
        self.beginResetModel()
        self._rows = sorted(records, key=lambda r: r.lsf_job_name)
        self._index = {r.job_key: i for i, r in enumerate(self._rows)}
        self.endResetModel()

    def apply_changes(self, changed):
        """jobs_updated 변경분 배치 반영 (README §5-4)."""
        for rec in changed:
            row = self._index.get(rec.job_key)
            if row is None:
                continue
            self._rows[row] = rec
            self.dataChanged.emit(self.index(row, 0),
                                  self.index(row, self.columnCount() - 1))


class Window(QWidget):
    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.setWindowTitle("lsfmgr — 실시간 job 테이블")

        self.summary_label = QLabel("...")
        self.model = JobTableModel()
        view = QTableView()
        view.setModel(self.model)
        view.verticalHeader().setDefaultSectionSize(20)
        btn = QPushButton("지금 새로고침 (refresh)")

        lay = QVBoxLayout(self)
        lay.addWidget(self.summary_label)
        lay.addWidget(view)
        lay.addWidget(btn)

        # submit — 핸들 Signal(updated)과 Facade Signal(jobs_updated)을 함께 사용
        self.js = mgr.submit([f"hspice corner_{i}.sp" for i in range(N)],
                             label="corner_sweep")
        self.js.start_polling(interval_s=1)           # 데모용 빠른 주기
        self.js.finished.connect(
            lambda rpt: self.model.load(self.js.jobs()))
        self.js.updated.connect(
            lambda s: self.summary_label.setText(format_summary(s)))
        mgr.jobs_updated.connect(self.on_jobs_updated)  # 변경분 batch
        btn.clicked.connect(self.js.refresh)

        self.model.load(self.js.jobs())

    def on_jobs_updated(self, jsid, changed):
        if jsid == self.js.id:
            self.model.apply_changes(changed)


app = QApplication(sys.argv)
install_logging()
mgr, sim = make_manager()
win = Window(mgr)
win.resize(640, 480)
win.show()
maybe_autoquit(app)
app.exec()
