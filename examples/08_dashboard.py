#!/usr/bin/env python3
"""예제 08 — 종합 대시보드 (옵션 3단 계층 + 다중 JobSet + Facade Signal).

- 좌측: submit 옵션 폼 — call 계층(③) 옵션을 폼 값으로 전달
  (manager는 생성 시 ② 계층 기본값: workers=8, default_queue="normal")
- 중앙: JobSet 목록 — 요약 실시간 갱신, 선택 후 kill/refresh/cancel/close
- 하단: Low-level Facade Signal 이벤트 로그 (모든 JobSet 공통 스트림)

실행:  python examples/08_dashboard.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtCore import Qt
from qtpy.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFormLayout, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QPlainTextEdit, QPushButton, QSpinBox, QSplitter,
    QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget,
)

from lsfmgr import JobState
from common import format_summary, install_logging, make_manager, maybe_autoquit


class SubmitForm(QGroupBox):
    """③ call 계층 옵션 폼 — 비워두면 ①② 기본값이 적용된다."""

    def __init__(self, on_submit):
        super().__init__("Submit 옵션 (이번 호출만, ③)")
        self.count = QSpinBox(minimum=1, maximum=10000, value=100)
        self.workers = QSpinBox(minimum=1, maximum=32, value=8)
        self.max_retry = QSpinBox(minimum=0, maximum=10, value=3)
        self.mode = QComboBox()
        self.mode.addItems(["auto", "array", "bulk"])
        self.queue = QLineEdit()                 # 비우면 default_queue(②)
        self.queue.setPlaceholderText("(비우면 manager 기본값)")
        self.label = QLineEdit("sweep")
        self.auto_poll = QCheckBox("auto_poll (AUTO-1)")
        self.auto_poll.setChecked(True)
        btn = QPushButton("Submit")
        btn.clicked.connect(on_submit)

        form = QFormLayout(self)
        form.addRow("job 수", self.count)
        form.addRow("workers", self.workers)
        form.addRow("max_retry", self.max_retry)
        form.addRow("mode", self.mode)
        form.addRow("queue", self.queue)
        form.addRow("label", self.label)
        form.addRow(self.auto_poll)
        form.addRow(btn)

    def call_kwargs(self) -> dict:
        kw = dict(workers=self.workers.value(),
                  max_retry=self.max_retry.value(),
                  mode=self.mode.currentText(),
                  label=self.label.text(),
                  auto_poll=self.auto_poll.isChecked())
        if self.queue.text().strip():
            kw["queue"] = self.queue.text().strip()
        return kw


class Dashboard(QWidget):
    COLS = ["jobset", "label", "total", "PEND", "RUN", "DONE",
            "EXIT", "FAILED", "LOST"]

    def __init__(self, mgr):
        super().__init__()
        self.mgr = mgr
        self.setWindowTitle("lsfmgr — 종합 대시보드")
        self._items = {}                          # jobset_id → QTreeWidgetItem

        # --- 좌: 옵션 폼 / 우: JobSet 목록 + 제어 ---
        self.form = SubmitForm(self.submit)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(self.COLS)
        self.tree.setRootIsDecorated(False)
        for i in range(2, len(self.COLS)):
            self.tree.setColumnWidth(i, 56)

        ctrl = QHBoxLayout()
        for text, fn in [("Kill", self.kill), ("PEND만 Kill", self.kill_pend),
                         ("Refresh", self.refresh), ("Cancel", self.cancel),
                         ("Close", self.close_jobset)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            ctrl.addWidget(b)

        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.addWidget(self.tree)
        rlay.addLayout(ctrl)

        top = QSplitter(Qt.Horizontal)
        top.addWidget(self.form)
        top.addWidget(right)
        top.setStretchFactor(1, 1)

        # --- 하단: Facade 이벤트 로그 (Low-level, README §9) ---
        self.log = QPlainTextEdit(readOnly=True)
        self.log.setMaximumBlockCount(500)

        lay = QVBoxLayout(self)
        lay.addWidget(top, stretch=3)
        lay.addWidget(QLabel("Facade Signal 이벤트 스트림 (전 JobSet 공통):"))
        lay.addWidget(self.log, stretch=1)

        # 전역 Facade Signal — 여러 JobSet 통합 스트림
        mgr.submit_started.connect(
            lambda j: self._log(j, "submit_started"))
        mgr.submit_finished.connect(
            lambda j, r: self._log(j, f"submit_finished ok={r.ok}"
                                      f" failed={r.failed}"))
        mgr.jobset_updated.connect(self._on_updated)
        mgr.job_lost.connect(
            lambda j, rec: self._log(j, f"job_lost {rec.lsf_job_name}"))
        mgr.kill_finished.connect(
            lambda j, r: self._log(j, f"kill_finished 대상={r.requested}"
                                      f" 호출={r.command_calls}회"
                                      f" 전략={r.strategies}"))
        mgr.error_occurred.connect(
            lambda j, msg: self._log(j, f"ERROR {msg}"))

    # ------------------------------------------------------------------
    def submit(self):
        kw = self.form.call_kwargs()
        n = self.form.count.value()
        js = self.mgr.submit([f"hspice run_{i}.sp" for i in range(1, n + 1)],
                             **kw)
        item = QTreeWidgetItem([js.id, kw.get("label", "")]
                               + [""] * (len(self.COLS) - 2))
        self.tree.addTopLevelItem(item)
        self._items[js.id] = item
        if not kw["auto_poll"]:
            js.start_polling(interval_s=2)       # 데모에서는 항상 관찰

    def _selected_id(self):
        item = self.tree.currentItem()
        return item.text(0) if item else None

    def kill(self):
        jsid = self._selected_id()
        if jsid:
            self.mgr.jobset(jsid).kill(verify=True)

    def kill_pend(self):
        jsid = self._selected_id()
        if jsid:
            self.mgr.jobset(jsid).kill(only_state=JobState.PEND)

    def refresh(self):
        jsid = self._selected_id()
        if jsid:
            self.mgr.jobset(jsid).refresh()

    def cancel(self):
        jsid = self._selected_id()
        if jsid:
            self.mgr.jobset(jsid).cancel()

    def close_jobset(self):
        jsid = self._selected_id()
        if not jsid:
            return
        try:
            self.mgr.jobset(jsid).close()
            self._log(jsid, "closed")
        except Exception as e:                    # 전원 terminal 아니면 거부
            self._log(jsid, f"close 거부: {e}")

    # ------------------------------------------------------------------
    def _on_updated(self, jsid, s):
        item = self._items.get(jsid)
        if item is None:
            return
        vals = [s.get("total", 0), s.get("PEND", 0), s.get("RUN", 0),
                s.get("DONE", 0), s.get("EXIT", 0),
                s.get("SUBMIT_FAILED", 0) + s.get("RETRY_WAIT", 0),
                s.get("LOST", 0)]
        for col, v in enumerate(vals, start=2):
            item.setText(col, str(v))

    def _log(self, jsid, msg):
        self.log.appendPlainText(f"[{jsid[-8:]}] {msg}")


app = QApplication(sys.argv)
install_logging()
# ② manager 계층 기본값 — 폼(③)에서 호출별로 덮어쓸 수 있다
mgr, sim = make_manager(workers=8, default_queue="normal", max_retry=3)
win = Dashboard(mgr)
win.resize(980, 640)
win.show()
maybe_autoquit(app)
app.exec()
