#!/usr/bin/env python3
"""예제 07 — SQLite 영속 모드: 세션 복원 (orphan → recover → reconcile).

한 프로세스 안에서 "앱 재시작"을 시뮬레이션한다:
  ① persistent manager로 submit
  ② [앱 비정상 종료] 버튼 — shutdown 없이 manager 폐기 (close 마킹 없음)
  ③ [앱 재시작] 버튼 — 같은 db로 새 manager → orphan 감지 다이얼로그
     → recover_jobset() 핸들 → js.reconcile() → 죽어있던 동안의
       DONE/EXIT/LOST 반영 → 미종결 job 있으면 자동 polling 재개

실행:  python examples/07_session_restore.py
"""
import os
import sys
import tempfile

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtWidgets import (
    QApplication, QLabel, QMessageBox, QPlainTextEdit, QPushButton,
    QVBoxLayout, QWidget,
)

from lsfmgr import LsfJobManager
from mock_lsf import SimulatedLsf
from common import format_summary, install_logging, maybe_autoquit

DB = os.path.join(tempfile.gettempdir(), "lsfmgr_demo_restore.db")
N = 60

# LSF는 앱이 죽어도 살아있다 — 시뮬레이터를 manager 밖에서 공유
SIM = SimulatedLsf(pend_s=(0.5, 2), run_s=(3, 10), exit_rate=0.1)
AUTOMATED = float(os.environ.get("LSFMGR_DEMO_AUTOQUIT", "0")) > 0


class Window(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("lsfmgr — 세션 복원 (SQLite)")
        self.mgr = None
        self.js = None

        self.state = QLabel(f"db: {DB}")
        self.log = QPlainTextEdit(readOnly=True)
        self.btn_start = QPushButton("① 앱 시작 + submit")
        self.btn_crash = QPushButton("② 앱 비정상 종료 (shutdown 없이 폐기)")
        self.btn_restart = QPushButton("③ 앱 재시작 → orphan 복원")

        lay = QVBoxLayout(self)
        for w in (self.state, self.btn_start, self.btn_crash,
                  self.btn_restart, self.log):
            lay.addWidget(w)

        self.btn_start.clicked.connect(self.start_app)
        self.btn_crash.clicked.connect(self.crash)
        self.btn_restart.clicked.connect(self.restart)

    def _new_manager(self):
        # persistent=True + db_path — 옵션 kwargs 방식 (README §6)
        return LsfJobManager(persistent=True, db_path=DB, runner=SIM)

    def start_app(self):
        self.mgr = self._new_manager()
        self.js = self.mgr.submit([f"sweep {i}" for i in range(N)],
                                  label="restore_demo")
        self.js.start_polling(interval_s=1)
        self.js.updated.connect(
            lambda s: self.state.setText(format_summary(s)))
        self.log.appendPlainText(
            f"세션1({self.mgr.store.session_id}) submit: {self.js.id}")

    def crash(self):
        if self.mgr is None:
            return
        # 실제 crash 흉내 — close_jobset 없이 스레드만 정리하고 버림.
        # (LSF에서는 job이 계속 돌아간다)
        self.mgr.shutdown()
        self.mgr = None
        self.js = None
        self.log.appendPlainText("앱 사망 — JobSet은 db에, job은 LSF에 잔존\n")

    def restart(self):
        if self.mgr is not None:
            self.log.appendPlainText("먼저 ②로 종료하세요")
            return
        self.mgr = self._new_manager()
        self.log.appendPlainText(f"세션2({self.mgr.store.session_id}) 시작")
        orphans = self.mgr.list_orphan_jobsets()      # FR-6.1 (자동 복원 없음)
        if not orphans:
            self.log.appendPlainText("orphan 없음")
            return
        for rec in orphans:
            desc = (f"{rec.jobset_id}\nlabel={rec.label} "
                    f"intended={rec.intended_count}")
            if not AUTOMATED:
                ans = QMessageBox.question(self, "이전 세션 JobSet 복원",
                                           f"복원할까요?\n\n{desc}")
                if ans != QMessageBox.StandardButton.Yes:
                    continue
            js = self.mgr.recover_jobset(rec.jobset_id)   # 핸들 반환
            js.updated.connect(
                lambda s: self.state.setText(format_summary(s)))
            js.updated.connect(lambda s, j=js: self.log.appendPlainText(
                "  reconcile/poll 반영: " + format_summary(s)))
            js.reconcile()      # 죽어있던 동안의 DONE/EXIT/LOST 반영(비동기)
                                # → 미종결 job 있으면 자동 polling 재개
            self.log.appendPlainText(f"recover+reconcile 요청: {js.id}")


app = QApplication(sys.argv)
install_logging()
win = Window()
win.resize(640, 480)
win.show()
if AUTOMATED:                       # 스모크 테스트: ①→②→③ 자동 진행
    from qtpy.QtCore import QTimer
    QTimer.singleShot(200, win.start_app)
    QTimer.singleShot(1500, win.crash)
    QTimer.singleShot(2000, win.restart)
maybe_autoquit(app)
app.exec()
if os.path.exists(DB) and AUTOMATED:
    os.remove(DB)                   # 스모크 테스트 정리
