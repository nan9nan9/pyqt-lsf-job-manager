#!/usr/bin/env python3
"""lsfmgr 기본 예제 — 단일 GUI 대시보드로 주요 기능을 모두 다룬다.

하나의 화면에서 lsfmgr 의 주요 기능을 모두 다룬다:
  - create_jobset → create_jobs → submit 으로 제출 + 진행률 바 / 취소 (QT-5/QT-6, rate limit)
  - job 마다 wrapper 커맨드 (customwrapper_sub 등) 또는 '혼합'
  - 다중 JobSet 관리 + 요약 실시간 갱신        (Facade Signal, README §8)
  - 선택 JobSet 의 job 단위 모니터링 테이블     (상태별 색, jobs_updated 배치)
  - kill 전략: 전체 / PEND만 / verify           (FR-3, KillReport — job_id 기반)
  - 실패 처리: retry(비정상 종료) / SUBMIT_FAILED / EXIT / detect_lost

제출은 wrapper 커맨드(예: `customwrapper_sub -q normal run_3.sp`)를 그대로 실행하고
그 결과의 `Job <id>` 로 job 을 관리한다. 테스트 환경은 저장소 동봉 mocklsf 이며,
실제 LSF 에서 돌리려면 LSFMGR_REAL=1 을 준다.

실행:  python examples/basic_example.py
"""
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from qtpy.QtCore import QObject, Qt, QTimer, Signal
from qtpy.QtGui import QBrush, QColor
from qtpy.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFormLayout, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QPlainTextEdit, QProgressBar, QPushButton, QSpinBox,
    QSplitter, QTableWidget, QTableWidgetItem, QTreeWidget, QTreeWidgetItem,
    QVBoxLayout, QWidget,
)

from lsfmgr import JobState, LsfJobManager
from lsfmgr.command import default_runner
from common import (
    WRAPPERS, configure_mocklsf, install_logging, maybe_autoquit,
    mocklsf_paths, wrapper,
)


class _LogBus(QObject):
    """worker 스레드 → GUI 로 로그를 안전하게 전달하는 Signal 버스.

    runner 는 QThreadPool worker(비-GUI 스레드)에서 돌기 때문에, 위젯을 직접
    건드리지 않고 Signal 로 보낸다(크로스 스레드 → 자동 queued 연결).
    """
    line = Signal(str)


_JOB_ID_RE = re.compile(r"Job <([^>]+)>")


def make_logging_runner(bus: _LogBus):
    """default_runner 를 감싸, 실제 실행된 '제출 명령'과 할당된 job_id 를 로깅.

    lsfmgr 는 submit 마다 이 runner 로 argv(맨 앞이 customwrapper_sub wrapper)를
    subprocess 실행한다. 여기서 그 argv 원문과 stdout 의 'Job <id>' 를 남긴다.
    조회/종료(bjobs/bkill 등)는 제외하고 제출 명령만 로깅한다.
    """
    def runner(argv, timeout, cwd=None):        # cwd: work_dirs 제출 디렉토리
        res = default_runner(argv, timeout, cwd)
        prog = os.path.basename(str(argv[0]))
        if prog.endswith("_sub") or prog == "bsub":       # 제출 명령만
            shown = " ".join([prog] + [str(a) for a in argv[1:]])
            m = _JOB_ID_RE.search(res.stdout)
            if m:
                bus.line.emit(f"  $ {shown}")
                bus.line.emit(f"      → 할당 job_id = {m.group(1)} (rc=0)")
            else:
                bus.line.emit(f"  $ {shown}")
                bus.line.emit(f"      → 제출 실패 (rc={res.returncode}): "
                              f"{res.stderr.strip()[:80]}")
        return res
    return runner

# 데모용 타이밍/실패 주입 — retry(SUBMIT_FAILED)·EXIT 상태를 관찰 가능하게 한다.
configure_mocklsf(pend=(1, 4), run=(4, 10), submit_fail_rate=0.12,
                  exit_rate=0.12)

# job 상태별 색 (모니터링 테이블).
_STATE_COLOR = {
    JobState.PEND: "#808080",
    JobState.RUN: "#1565c0",
    JobState.DONE: "#2e7d32",
    JobState.EXIT: "#c62828",
    JobState.SUBMIT_FAILED: "#c62828",
    JobState.LOST: "#8e24aa",
    JobState.RETRY_WAIT: "#ef6c00",
}


class SubmitForm(QGroupBox):
    """submit 옵션 폼. job 마다 wrapper(customwrapper_sub 등)로 제출한다."""

    def __init__(self, on_submit):
        super().__init__("Submit 옵션")
        self.count = QSpinBox(minimum=1, maximum=10000, value=30)
        self.workers = QSpinBox(minimum=1, maximum=32, value=8)
        self.max_retry = QSpinBox(minimum=0, maximum=10, value=3)
        self.rate = QSpinBox(minimum=0, maximum=1000, value=0)   # 0=제한 없음
        self.wrapper = QComboBox()
        # 개별 wrapper + '혼합' — job 마다 다른 wrapper 를 쓰는 실제 환경 시연.
        self.wrapper.addItems(WRAPPERS + ["혼합(mix)"])
        self.queue = QLineEdit("normal")         # wrapper 에 -q 로 전달됨
        self.label = QLineEdit("sweep")
        self.auto_poll = QCheckBox("auto_poll (AUTO-1)")
        self.auto_poll.setChecked(True)
        # job 당 PEND/RUN 시간(초) 범위 — 이 submit 의 job 들에 적용된다.
        # (mocklsf 는 submit 시점에 job 별 계획값을 min~max 사이에서 정한다)
        self.pend_min = QSpinBox(minimum=0, maximum=600, value=1)
        self.pend_max = QSpinBox(minimum=0, maximum=600, value=4)
        self.run_min = QSpinBox(minimum=0, maximum=3600, value=4)
        self.run_max = QSpinBox(minimum=0, maximum=3600, value=10)
        btn = QPushButton("Submit")
        btn.clicked.connect(on_submit)

        def _range(lo, hi):
            h = QHBoxLayout()
            h.setContentsMargins(0, 0, 0, 0)
            h.addWidget(lo)
            h.addWidget(QLabel("~"))
            h.addWidget(hi)
            return h

        form = QFormLayout(self)
        form.addRow("job 수", self.count)
        form.addRow("workers", self.workers)
        form.addRow("max_retry", self.max_retry)
        form.addRow("rate_limit/s", self.rate)
        form.addRow("wrapper", self.wrapper)
        form.addRow("queue", self.queue)
        form.addRow("label", self.label)
        form.addRow("PEND 시간(초)", _range(self.pend_min, self.pend_max))
        form.addRow("RUN 시간(초)", _range(self.run_min, self.run_max))
        form.addRow(self.auto_poll)
        form.addRow(btn)

    def commands(self):
        """job 마다 wrapper 커맨드(토큰 리스트)를 만든다.

        '혼합' 선택 시 job 마다 다른 wrapper 를 순환 사용한다 — 실제 환경에서
        job 별로 서로 다른 wrapper 커맨드가 섞이는 상황을 재현한다.
        """
        n = self.count.value()
        q = self.queue.text().strip()
        sel = self.wrapper.currentText()

        def one(i):
            tool = WRAPPERS[i % len(WRAPPERS)] if sel == "혼합(mix)" else sel
            args = (["-q", q] if q else []) + [f"run_{i}.sp"]
            return wrapper(tool, *args)          # bin/<tool> 절대경로 + 인자

        return [one(i) for i in range(n)]

    def timing(self):
        """이 submit 에 적용할 (PEND, RUN) 시간 범위(초) — mocklsf 계획값용."""
        return ((self.pend_min.value(), self.pend_max.value()),
                (self.run_min.value(), self.run_max.value()))

    def call_kwargs(self) -> dict:
        kw = dict(workers=self.workers.value(),
                  max_retry=self.max_retry.value(),
                  label=self.label.text(),
                  auto_poll=self.auto_poll.isChecked())
        if self.rate.value() > 0:
            kw["rate_limit_per_s"] = self.rate.value()
        return kw


class Dashboard(QWidget):
    COLS = ["jobset", "label", "total", "PEND", "RUN", "DONE",
            "EXIT", "FAILED", "LOST"]
    JOBCOLS = ["job name", "job id", "state", "exit", "retry", "fail reason"]

    def __init__(self):
        super().__init__()
        self.setWindowTitle("lsfmgr — 통합 대시보드 (mocklsf / customwrapper_sub)")
        self.mgr = None
        self._items = {}                 # jobset_id → QTreeWidgetItem
        self._active_submit = None       # 진행률 바가 추적 중인 jobset_id
        self._laststate = {}             # lsf_job_name → 마지막 관측 상태(전이 로깅)
        self._row_of = {}                # lsf_job_name → 테이블 행 (증분 upsert용)

        # 제출 명령/‏job_id 를 worker 스레드에서 로그로 전달하는 버스.
        self.bus = _LogBus()
        self.bus.line.connect(self._append)

        # --- 좌: 옵션 폼 ---
        self.form = SubmitForm(self.submit)

        # --- 우상: JobSet 목록 + 제어 ---
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(self.COLS)
        self.tree.setRootIsDecorated(False)
        for i in range(2, len(self.COLS)):
            self.tree.setColumnWidth(i, 52)
        self.tree.currentItemChanged.connect(lambda *_: self._reload_jobs())

        ctrl = QHBoxLayout()
        for text, fn in [("Kill+verify", self.kill),
                         ("PEND만 Kill", self.kill_pend),
                         ("Cancel", self.cancel), ("Refresh", self.refresh),
                         ("detect_lost", self.detect_lost),
                         ("Close", self.close_jobset)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            ctrl.addWidget(b)

        # --- 우하: 선택 JobSet 의 job 모니터링 테이블 ---
        self.jobtable = QTableWidget(0, len(self.JOBCOLS))
        self.jobtable.setHorizontalHeaderLabels(self.JOBCOLS)
        self.jobtable.verticalHeader().setVisible(False)
        self.jobtable.setEditTriggers(QTableWidget.NoEditTriggers)

        state_top = QWidget()
        stlay = QVBoxLayout(state_top)
        stlay.setContentsMargins(0, 0, 0, 0)
        stlay.addWidget(QLabel("JobSet 목록 (선택 후 아래 제어):"))
        stlay.addWidget(self.tree, stretch=1)
        stlay.addLayout(ctrl)
        stlay.addWidget(QLabel("선택 JobSet 의 job 상태:"))
        stlay.addWidget(self.jobtable, stretch=2)

        top = QSplitter(Qt.Horizontal)
        top.addWidget(self.form)
        top.addWidget(state_top)
        top.setStretchFactor(1, 1)

        # --- 진행률 바 ---
        self.bar = QProgressBar()
        self.bar.setFormat("submit %v/%m")
        midbar = QHBoxLayout()
        midbar.addWidget(self.bar, stretch=1)

        # --- 하단: Facade 이벤트 로그 (실행 명령·job_id·상태 전이) ---
        self.log = QPlainTextEdit(readOnly=True)
        self.log.setMaximumBlockCount(500)

        lay = QVBoxLayout(self)
        lay.addWidget(top, stretch=3)
        lay.addLayout(midbar)
        lay.addWidget(QLabel("Facade Signal 이벤트 스트림 (실행 명령·job_id·상태 전이):"))
        lay.addWidget(self.log, stretch=1)

    # ------------------------------------------------------------------
    # manager 바인딩
    # ------------------------------------------------------------------
    def _new_manager(self):
        """로깅 runner 를 주입한 manager 생성 (제출 명령/‏job_id 로그용)."""
        return new_manager(runner=make_logging_runner(self.bus))

    def bind_manager(self, mgr):
        self.mgr = mgr
        mgr.submit_started.connect(lambda j: self._log(j, "submit_started"))
        mgr.submit_progress.connect(self._on_progress)
        mgr.submit_finished.connect(self._on_finished)
        mgr.jobset_updated.connect(self._on_updated)
        mgr.jobs_updated.connect(self._on_jobs)
        mgr.job_lost.connect(
            lambda j, rec: self._log(j, f"job_lost {rec.lsf_job_name}"))
        mgr.kill_finished.connect(
            lambda j, r: self._log(j, f"kill_finished 대상={r.requested} "
                                      f"호출={r.command_calls}회 "
                                      f"전략={r.strategies} 잔존={r.still_alive}"))
        mgr.post_processing_started.connect(
            lambda j: self._log(j, "post_processing_started — 후처리 실행 중"))
        mgr.post_processing_finished.connect(self._on_post_process)
        mgr.error_occurred.connect(lambda j, msg: self._log(j, f"ERROR {msg}"))

    def _add_row(self, jsid, label):
        item = QTreeWidgetItem([jsid, label] + [""] * (len(self.COLS) - 2))
        self.tree.addTopLevelItem(item)
        self._items[jsid] = item
        return item

    # ------------------------------------------------------------------
    # submit
    # ------------------------------------------------------------------
    def submit(self):
        if self.mgr is None:
            return
        # 이 submit 의 job 들에 적용할 PEND/RUN 시간을 mocklsf 에 반영한다.
        # (submit 직전 env 설정 → 뒤이어 뜨는 bsub 프로세스가 job 별 계획값에 사용)
        pend, run = self.form.timing()
        configure_mocklsf(pend=pend, run=run)
        kw = self.form.call_kwargs()
        label = kw.pop("label", "")
        cmds = self.form.commands()
        # v9: jobset 생성 시 job까지 함께 만들고 → jobset 기준 제출.
        # wrapper 커맨드는 그대로 실행되고 'Job <id>' 파싱으로 관리된다.
        js = self.mgr.create_jobset(cmds, label=label)
        # post_process: 전 job이 terminal에 도달하면 worker에서 1회 집계 실행.
        self.mgr.submit(js, post_process=self._summarize_results, **kw)
        self._add_row(js.id, label)
        self._active_submit = js.id
        self.bar.setMaximum(self.form.count.value())
        self.bar.setValue(0)
        self.mgr.start_polling(js, 1)            # 데모: 상태 전이를 촘촘히 관찰

    def _on_progress(self, jsid, done, total):
        if jsid == self._active_submit:
            self.bar.setMaximum(total)
            self.bar.setValue(done)

    def _on_finished(self, jsid, rpt):
        # wrapper 제출은 커맨드 1개 = job 1개(job_id 1개).
        recs = self.mgr.jobset(jsid).jobs()
        n_ids = len({r.job_id for r in recs if r.job_id is not None})
        self._log(jsid, f"submit_finished ok={rpt.ok} failed={rpt.failed} "
                        f"retried={rpt.retried} job_id확보={n_ids} "
                        f"({rpt.duration_s:.1f}s)")

    @staticmethod
    def _summarize_results(records):
        """[worker 스레드] 완료 후처리 콜백 — **GUI 접근 금지**. 전원 terminal
        도달 시 호출되며, 최종 레코드로 성공/실패를 집계해 반환한다(반환값은
        post_processing_finished로 전달). 실제 앱에선 결과 파일 수합·리포트
        생성 등을 여기서 수행한다."""
        from collections import Counter
        c = Counter(r.state.name for r in records)
        return {"total": len(records), "DONE": c.get("DONE", 0),
                "EXIT": c.get("EXIT", 0),
                "failed": c.get("SUBMIT_FAILED", 0) + c.get("LOST", 0)}

    def _on_post_process(self, jsid, result):
        """[main] 후처리 완료 — 반환값(result) 또는 None(콜백 예외)."""
        if result is None:
            self._log(jsid, "post_processing_finished — 후처리 실패(로그 참조)")
            return
        self._log(jsid, f"post_processing_finished — DONE {result['DONE']} / "
                        f"EXIT {result['EXIT']} / 실패 {result['failed']} "
                        f"(총 {result['total']})")

    # ------------------------------------------------------------------
    # 선택 JobSet 제어
    # ------------------------------------------------------------------
    def _selected_id(self):
        item = self.tree.currentItem()
        return item.text(0) if item else None

    def _handle(self):
        jsid = self._selected_id()
        return self.mgr.jobset(jsid) if (jsid and self.mgr) else None

    def kill(self):
        js = self._handle()
        if js:
            self.mgr.kill(js, verify=True)

    def kill_pend(self):
        js = self._handle()
        if js:
            self.mgr.kill(js, only_state=JobState.PEND)

    def cancel(self):
        js = self._handle()
        if js:
            self.mgr.cancel_submit(js)

    def refresh(self):
        js = self._handle()
        if js:
            self.mgr.query_once(js)

    def detect_lost(self):
        js = self._handle()
        if js:
            lost = self.mgr.detect_lost(js)
            self._log(js.id, f"detect_lost: LOST 확정 {len(lost)}건")

    def close_jobset(self):
        js = self._handle()
        if not js:
            return
        try:
            self.mgr.close(js)
            self._log(js.id, "closed")
        except Exception as e:               # 전원 terminal 아니면 거부
            self._log(js.id, f"close 거부: {e}")

    # ------------------------------------------------------------------
    # 요약/모니터링 갱신
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

    def _on_jobs(self, jsid, records):
        # 변경분 배치 도착 — 상태 전이를 로그로 남긴다. 소량이면 job 별로,
        # 대량 배치면 요약 1줄로 (5000개 초기 버스트가 로그를 도배하지 않게).
        trans = []
        for r in records:
            key = r.lsf_job_name
            cur = r.state.value
            prev = self._laststate.get(key)
            if prev != cur:
                self._laststate[key] = cur
                trans.append((key, prev, cur, r))
        if len(trans) <= 12:
            for key, prev, cur, r in trans:
                arrow = f"{prev} → {cur}" if prev else f"(신규) → {cur}"
                extra = (f" (exit={r.exit_code})"
                         if r.state == JobState.EXIT else "")
                self._log(jsid, f"상태 {key}: {arrow}{extra}")
        elif trans:
            c = Counter(f"{p or '신규'}→{cur}" for _, p, cur, _ in trans)
            summ = ", ".join(f"{k} x{v}" for k, v in c.items())
            self._log(jsid, f"상태 전이 {len(trans)}건: {summ}")
        # 선택된 JobSet 이면 테이블을 **증분** 갱신한다 — 변경분 배치만 upsert.
        # (전체 재구성이 아니라 바뀐 행만 손대므로 5000개여도 부드럽다)
        if jsid == self._selected_id():
            self._apply_jobs(records)

    def _apply_jobs(self, records):
        """변경분 레코드만 job_key 기준으로 행 upsert (있으면 갱신, 없으면 추가).
        대량 job에서도 바뀐 행 수만큼만 작업 — 매 배치 전체 재그리기 금지."""
        self.jobtable.setUpdatesEnabled(False)      # 배치 중 재렌더 억제
        self.jobtable.setSortingEnabled(False)
        try:
            for r in records:
                key = r.lsf_job_name
                row = self._row_of.get(key)
                if row is None:                     # 신규 job → 행 추가
                    row = self.jobtable.rowCount()
                    self.jobtable.insertRow(row)
                    self._row_of[key] = row
                self._set_row(row, r)
        finally:
            self.jobtable.setUpdatesEnabled(True)

    def _set_row(self, row, r):
        color = _STATE_COLOR.get(r.state)
        cells = [r.lsf_job_name, str(r.job_id or "-"), r.state.value,
                 "" if r.exit_code is None else str(r.exit_code),
                 str(r.retry_count), r.fail_reason or ""]
        for col, text in enumerate(cells):
            it = QTableWidgetItem(text)
            if color and col == 2:
                it.setForeground(QBrush(QColor(color)))
            self.jobtable.setItem(row, col, it)

    def _reload_jobs(self):
        """JobSet 선택이 바뀌면 전체 재구성(+행 맵 재빌드) 1회."""
        self.jobtable.setRowCount(0)
        self._row_of = {}
        js = self._handle()
        if js is None:
            return
        self._apply_jobs(js.jobs())

    def _append(self, line):
        """로그 한 줄 그대로 출력 (버스 Signal 슬롯)."""
        self.log.appendPlainText(line)

    def _log(self, jsid, msg):
        tag = jsid if jsid == "*" else jsid[-8:]
        self.log.appendPlainText(f"[{tag}] {msg}")


def new_manager(runner=None):
    """② manager 계층 기본값 + mocklsf 경로로 생성.

    runner 를 주면 subprocess 실행을 가로채 로깅한다(제출 명령/‏job_id).
    """
    return LsfJobManager(workers=8,
                         default_queue="normal", max_retry=3,
                         runner=runner, **mocklsf_paths())


def main():
    app = QApplication(sys.argv)
    install_logging()
    win = Dashboard()
    win.bind_manager(win._new_manager())
    win.resize(1040, 760)
    win.show()
    # 스트레스 테스트 훅: LSFMGR_DEMO_SUBMIT=5000 이면 기동 직후 그 개수로
    # 자동 제출한다 (대량 job 렌더링/반응 확인용). 워커도 32로 올린다.
    n = os.environ.get("LSFMGR_DEMO_SUBMIT")
    if n:
        # 스트레스 테스트는 실패 주입/제출 지연을 꺼 깨끗한 대량 경로를 본다
        # (기본 예제는 retry 시연용으로 submit_fail_rate=0.12를 켜 둔다).
        configure_mocklsf(submit_fail_rate=0, submit_delay=0)
        win.form.count.setValue(int(n))
        win.form.workers.setValue(32)
        QTimer.singleShot(300, win.submit)
    maybe_autoquit(app)
    app.exec()


if __name__ == "__main__":
    main()
