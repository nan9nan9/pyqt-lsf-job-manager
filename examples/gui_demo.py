#!/usr/bin/env python3
"""lsfmgr 통합 GUI 데모 — 단일 대시보드에서 전 기능을 다룬다.

(v10.1: basic_example / handler_example / mc_example 을 이 파일 하나로 통합)

  - create_jobset → submit: wrapper 커맨드 그대로 실행, `Job <id>` 캡처 관리
    (진행률 바 / 취소 / rate limit / retry)
  - 다중 JobSet 트리 + 요약 실시간 갱신 + job 테이블 증분 upsert (README §5)
  - job 추가는 **merge**: 별도 batch jobset 생성 후 흡수 (v9 규칙)
  - 실패 재실행: 같은 merge_id 로 교체(merge) 후 전체 재제출
  - kill: 전체(verify, **MC-aware** — 생성자 cluster_envpaths 로 분류 kill) /
    PEND만 / 선택 행만 (job_id chunk 단일 경로)
  - JobSet handler: RUN 중 폴링마다 job 출력 파싱 + 종료 시 최종 1회
  - post_process: 전원 terminal 도달 시 worker 에서 1회 종합 집계
  - job 더블클릭 → 로컬 레코드 상세 (LSF 호출 0)

테스트 환경은 저장소 동봉 mocklsf(가상 LSF)이며, 실제 LSF 는 LSFMGR_REAL=1.

실행:            python examples/gui_demo.py
스트레스:        LSFMGR_DEMO_SUBMIT=5000 python examples/gui_demo.py
스모크(헤드리스): LSFMGR_DEMO_AUTORUN=1 LSFMGR_DEMO_AUTOQUIT=15 \
                     QT_QPA_PLATFORM=offscreen python examples/gui_demo.py
"""
import os
import re
import shlex
import sys
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from qtpy.QtCore import QObject, Qt, QTimer, Signal
from qtpy.QtGui import QBrush, QColor
from qtpy.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFormLayout, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QPlainTextEdit, QProgressBar, QPushButton, QSpinBox,
    QSplitter, QTableWidget, QTableWidgetItem, QTreeWidget, QTreeWidgetItem,
    QVBoxLayout, QWidget,
)

from lsfmgr import JobState
from lsfmgr.command import default_runner
from common import (
    WRAPPERS, cluster_env_path, configure_mocklsf, install_logging,
    make_manager, maybe_autoquit, wrapper,
)

#: MC forward 흉내에 쓸 원격 클러스터들 (실제 MC 의 원격 cluster 흉내).
FORWARD_CLUSTERS = ["cluster_busan", "cluster_daegu"]
HANDLER = "collect"                  # 출력 파싱 handler 이름


class _LogBus(QObject):
    """worker 스레드 → GUI 로그 전달 버스 (크로스 스레드 → queued 연결)."""
    line = Signal(str)


_JOB_ID_RE = re.compile(r"Job <([^>]+)>")


def make_logging_runner(bus: _LogBus):
    """default_runner 래핑 — 실제 실행된 '제출 명령'과 할당 job_id 를 로깅.
    조회/kill(bjobs/bkill 등)은 제외하고 제출 wrapper 호출만 남긴다."""
    def runner(argv, timeout, cwd=None):
        res = default_runner(argv, timeout, cwd)
        prog = os.path.basename(str(argv[0]))
        if prog.endswith("_sub"):                     # 제출 명령만
            shown = " ".join([prog] + [str(a) for a in argv[1:]])
            m = _JOB_ID_RE.search(res.stdout)
            bus.line.emit(f"  $ {shown}")
            if m:
                bus.line.emit(f"      → 할당 job_id = {m.group(1)} (rc=0)")
            else:
                bus.line.emit(f"      → 제출 실패 (rc={res.returncode}): "
                              f"{res.stderr.strip()[:80]}")
        return res
    return runner


# 데모용 타이밍/실패 주입 — retry(SUBMIT_FAILED)·EXIT 상태를 관찰 가능하게.
configure_mocklsf(pend=(1, 4), run=(4, 10), submit_fail_rate=0.12,
                  exit_rate=0.12)

_STATE_COLOR = {
    JobState.PEND: "#808080", JobState.RUN: "#1565c0",
    JobState.DONE: "#2e7d32", JobState.EXIT: "#c62828",
    JobState.SUBMIT_FAILED: "#c62828", JobState.LOST: "#8e24aa",
    JobState.RETRY_WAIT: "#ef6c00",
}


def parse_job_output(ctx):
    """[worker] JobSet handler 본체 — RUN 중 폴링마다 + 종료 시 최종 1회.
    mocklsf 의 job 출력 파일을 파싱한다 (실제라면
    `ctx.submit_cwd or os.getcwd()` 아래 시뮬레이션 로그를 파싱).
    blocking I/O 여도 GUI 를 안 막는다."""
    out = os.path.join(os.environ.get("MOCKLSF_HOME", ""), "jobout",
                       f"{ctx.job_id}.out")
    try:
        with open(out, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        lines = []
    rec = ctx.record
    if ctx.final and rec.run_time_s is not None:
        elapsed = rec.run_time_s
    elif rec.start_time is not None:
        elapsed = int((datetime.now() - rec.start_time).total_seconds())
    else:
        elapsed = None
    return {"lines": len(lines), "elapsed_s": elapsed,
            "last": lines[-1] if lines else "(출력 없음)"}


class SubmitForm(QGroupBox):
    """submit 옵션 폼 — job 마다 wrapper 커맨드(혼합 가능)로 제출."""

    def __init__(self, on_submit, on_add):
        super().__init__("Submit 옵션")
        self.count = QSpinBox(minimum=1, maximum=10000, value=30)
        self.workers = QSpinBox(minimum=1, maximum=32, value=8)
        self.max_retry = QSpinBox(minimum=0, maximum=10, value=3)
        self.rate = QSpinBox(minimum=0, maximum=1000, value=0)   # 0=무제한
        self.wrapper = QComboBox()
        self.wrapper.addItems(WRAPPERS + ["혼합(mix)"])
        self.queue = QLineEdit("normal")          # wrapper 에 -q 로 전달
        self.label = QLineEdit("sweep")
        self.auto_poll = QCheckBox("auto_poll")
        self.auto_poll.setChecked(True)
        self.use_handler = QCheckBox("handler 등록 (RUN 중 출력 파싱)")
        self.mc_forward = QCheckBox(
            f"MC forward 흉내 ({'/'.join(FORWARD_CLUSTERS)}, 70%)")
        self.pend_min = QSpinBox(minimum=0, maximum=600, value=1)
        self.pend_max = QSpinBox(minimum=0, maximum=600, value=4)
        self.run_min = QSpinBox(minimum=0, maximum=3600, value=4)
        self.run_max = QSpinBox(minimum=0, maximum=3600, value=10)
        btn = QPushButton("Submit (새 JobSet)")
        btn.clicked.connect(on_submit)
        btn_add = QPushButton("Add to selected (merge)")
        btn_add.clicked.connect(on_add)

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
        form.addRow(self.use_handler)
        form.addRow(self.mc_forward)
        form.addRow(btn)
        form.addRow(btn_add)

    def commands(self, start=0):
        """job 마다 wrapper 커맨드(토큰 리스트) — '혼합'은 wrapper 순환."""
        q = self.queue.text().strip()
        sel = self.wrapper.currentText()

        def one(i):
            tool = WRAPPERS[i % len(WRAPPERS)] if sel == "혼합(mix)" else sel
            args = (["-q", q] if q else []) + [f"run_{i}.sp"]
            return wrapper(tool, *args)
        return [one(start + i) for i in range(self.count.value())]

    def apply_mocklsf(self):
        """이 submit 의 타이밍/MC forward 설정을 mocklsf 에 반영."""
        configure_mocklsf(
            pend=(self.pend_min.value(), self.pend_max.value()),
            run=(self.run_min.value(), self.run_max.value()),
            forward_clusters=(FORWARD_CLUSTERS if self.mc_forward.isChecked()
                              else []),
            forward_rate=0.7 if self.mc_forward.isChecked() else 0)

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
    JOBCOLS = ["job key", "job id", "state", "exit", "retry",
               "fail reason", "cluster"]

    def __init__(self):
        super().__init__()
        self.setWindowTitle("lsfmgr — 통합 데모 (mocklsf / wrapper 제출)")
        self.mgr = None
        self._items = {}                 # jobset_id → QTreeWidgetItem
        self._active_submit = None       # 진행률 바가 추적 중인 jobset_id
        self._laststate = {}             # job_key → 마지막 관측 상태
        self._row_of = {}                # job_key → 테이블 행

        self.bus = _LogBus()
        self.bus.line.connect(self._append)

        # --- 좌: 옵션 폼 ---
        self.form = SubmitForm(self.submit, self.add_to_selected)

        # --- 우상: JobSet 트리 + 제어 ---
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(self.COLS)
        self.tree.setRootIsDecorated(False)
        for i in range(2, len(self.COLS)):
            self.tree.setColumnWidth(i, 52)
        self.tree.currentItemChanged.connect(lambda *_: self._reload_jobs())

        ctrl = QHBoxLayout()
        for text, fn in [("Kill+verify (MC-aware)", self.kill),
                         ("PEND만 Kill", self.kill_pend),
                         ("선택 행 Kill", self.kill_selected),
                         ("Cancel", self.cancel),
                         ("Rerun failed", self.rerun_failed),
                         ("Refresh", self.refresh),
                         ("detect_lost", self.detect_lost),
                         ("Remove jobset", self.remove_jobset)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            ctrl.addWidget(b)

        # --- 우하: 선택 JobSet 의 job 테이블 (더블클릭 → 상세) ---
        self.jobtable = QTableWidget(0, len(self.JOBCOLS))
        self.jobtable.setHorizontalHeaderLabels(self.JOBCOLS)
        self.jobtable.verticalHeader().setVisible(False)
        self.jobtable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.jobtable.setSelectionBehavior(QTableWidget.SelectRows)
        self.jobtable.itemDoubleClicked.connect(self._on_double_click)

        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.setContentsMargins(0, 0, 0, 0)
        rlay.addWidget(QLabel("JobSet 목록 (선택 후 아래 제어):"))
        rlay.addWidget(self.tree, stretch=1)
        rlay.addLayout(ctrl)
        rlay.addWidget(QLabel("선택 JobSet 의 job 상태 (더블클릭 → 상세):"))
        rlay.addWidget(self.jobtable, stretch=2)

        top = QSplitter(Qt.Horizontal)
        top.addWidget(self.form)
        top.addWidget(right)
        top.setStretchFactor(1, 1)

        self.bar = QProgressBar()
        self.bar.setFormat("progress %v/%m")

        self.log = QPlainTextEdit(readOnly=True)
        self.log.setMaximumBlockCount(500)

        lay = QVBoxLayout(self)
        lay.addWidget(top, stretch=3)
        lay.addWidget(self.bar)
        lay.addWidget(QLabel("이벤트 스트림 (실행 명령·job_id·상태 전이·"
                             "handler/후처리 결과):"))
        lay.addWidget(self.log, stretch=1)

    # ------------------------------------------------------------------
    # manager 바인딩
    # ------------------------------------------------------------------
    def bind_manager(self, mgr):
        self.mgr = mgr
        mgr.submit_started.connect(lambda j: self._log(j, "submit_started"))
        mgr.submit_progress.connect(self._on_progress)
        mgr.kill_progress.connect(self._on_progress)
        mgr.submit_finished.connect(self._on_finished)
        mgr.jobset_updated.connect(self._on_updated)
        mgr.jobs_updated.connect(self._on_jobs)
        mgr.job_lost.connect(
            lambda j, rec: self._log(j, f"job_lost {rec.job_key}"))
        mgr.kill_started.connect(lambda j: self._log(j, "kill_started"))
        mgr.kill_finished.connect(
            lambda j, r: self._log(
                j, f"kill_finished 대상={r.requested} 호출={r.command_calls}회"
                   f" 미확인={r.unconfirmed} 잔존={r.still_alive}"
                   + (f" 오류={len(r.errors)}건" if r.errors else "")))
        mgr.handler_finished.connect(self._on_handler)
        mgr.jobset_finished.connect(
            lambda j, s: self._log(
                j, f"jobset_finished — 전원 terminal (총 {s.get('total', 0)}건)"))
        mgr.post_processing_started.connect(
            lambda j: self._log(j, "post_processing_started"))
        mgr.post_processing_finished.connect(self._on_post_process)
        mgr.error_occurred.connect(lambda j, msg: self._log(j, f"ERROR {msg}"))

    def _add_row(self, jsid, label):
        item = QTreeWidgetItem([jsid, label] + [""] * (len(self.COLS) - 2))
        self.tree.addTopLevelItem(item)
        self._items[jsid] = item
        self.tree.setCurrentItem(item)

    def _drop_row(self, jsid):
        """삭제된 jobset의 행 제거 — 남겨두면 선택 시 JobSetNotFoundError."""
        item = self._items.pop(jsid, None)
        if item is not None:
            idx = self.tree.indexOfTopLevelItem(item)
            if idx >= 0:
                self.tree.takeTopLevelItem(idx)

    # ------------------------------------------------------------------
    # submit / merge 추가 / 실패 재실행
    # ------------------------------------------------------------------
    def submit(self):
        self.form.apply_mocklsf()
        kw = self.form.call_kwargs()
        label = kw.pop("label", "")
        js = self.mgr.create_jobset(self.form.commands(), label=label)
        self.mgr.submit(js, post_process=self._summarize_results, **kw)
        if self.form.use_handler.isChecked():
            # RUN 중 폴링 사이클마다 parse_job_output, 종료 시 최종 1회
            self.mgr.add_handler(js, HANDLER, parse_job_output,
                                 start_states={JobState.RUN},
                                 end_states={JobState.DONE, JobState.EXIT})
            self._log(js.id, f"handler '{HANDLER}' 등록")
        self._add_row(js.id, label)
        self._active_submit = js.id
        self.bar.setMaximum(self.form.count.value())
        self.bar.setValue(0)
        self.mgr.start_polling(js, 1)        # 데모: 촘촘한 폴링(기본 10s)

    def add_to_selected(self):
        """job 추가는 merge 로만 (v9) — batch jobset 을 만들어 흡수한다."""
        js = self._handle()
        if js is None:
            return
        n = len(js.jobs())
        cmds = self.form.commands(start=n)
        batch = self.mgr.create_jobset(
            cmds, merge_ids=[f"run_{n + i}" for i in range(len(cmds))])
        if not self.mgr.can_merge(js, batch):
            # 임시 batch 폐기 — CREATED 는 terminal 이 아니라 force 필요
            self.mgr.remove_jobset(batch, force=True)
            self._log(js.id, "추가 불가 — 활성 job 존재 (먼저 완료/kill)")
            return
        self.mgr.merge(js, batch)
        self._log(js.id, f"{len(cmds)}건 merge 추가 — 총 {len(js.jobs())}건 "
                         f"(미제출, Submit 은 전체 재제출)")

    def rerun_failed(self):
        """실패 job 을 같은 merge_id 로 교체(merge) 후 전체 재제출 (v9 패턴)."""
        js = self._handle()
        if js is None:
            return
        failed = [r for r in js.jobs() if r.state.is_failed]
        if not failed or not self.mgr.can_submit(js):
            self._log(js.id, "rerun 불가 — 실패분 없음 또는 활성 job 존재")
            return
        fix = self.mgr.create_jobset(
            [shlex.split(r.command) for r in failed],
            merge_ids=[r.merge_id for r in failed],
            user_datas=[r.user_data for r in failed])
        self.mgr.merge(js, fix)              # 같은 merge_id → 교체
        self.mgr.submit(js, post_process=self._summarize_results,
                        auto_poll=False)
        self.mgr.start_polling(js, 1)
        self._log(js.id, f"rerun — 실패 {len(failed)}건 교체 후 전체 재제출")

    # ------------------------------------------------------------------
    # kill / 제어
    # ------------------------------------------------------------------
    def kill(self):
        """전체 kill (verify) — MC 자동 분류 kill.
        매핑(cluster_envpaths)은 매니저 생성 시 준다. 제출 직후처럼 cluster
        미상이어도 라이브러리가 bkill 직전에 미상 대상만 조회해 채운 뒤,
        forward job 은 그 클러스터 env 를 source 한 bkill 로, 나머지는 일반
        bkill 로 나눠 죽인다."""
        js = self._handle()
        if js is None:
            return
        self.mgr.kill(js, verify=True)

    def kill_pend(self):
        """PEND 만 kill — cancel_submit=True 로 전체 kill 과 같은 제출
        우선권을 건다(제출 중인 것까지 확실히 멈춘다, v10.3 opt-in)."""
        js = self._handle()
        if js:
            self.mgr.kill(js, only_state=JobState.PEND, cancel_submit=True)

    def kill_selected(self):
        """테이블 선택 행만 kill (kill_jobs — array element 는 id[idx])."""
        js = self._handle()
        if js is None:
            return
        rows = {i.row() for i in self.jobtable.selectedIndexes()}
        keys = [self.jobtable.item(r, 0).text() for r in sorted(rows)]
        if keys:
            self.mgr.kill_jobs(js, keys, verify=True)
            self._log(js.id, f"선택 {len(keys)}건 kill 요청")

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

    def remove_jobset(self):
        js = self._handle()
        if not js:
            return
        jsid = js.id
        try:
            self.mgr.remove_jobset(js)
        except Exception as e:               # 전원 terminal 아니면 거부
            self._log(jsid, f"remove_jobset 거부: {e}")
            return
        self._drop_row(jsid)                 # 레코드가 사라졌으니 행도 제거
        self._log(jsid, "removed — 이 jobset은 더 이상 조회할 수 없습니다")

    # ------------------------------------------------------------------
    # 후처리 / handler / 상세
    # ------------------------------------------------------------------
    @staticmethod
    def _summarize_results(records):
        """[worker] post_process — 전원 terminal 도달 시 1회, GUI 접근 금지."""
        c = Counter(r.state.name for r in records)
        return {"total": len(records), "DONE": c.get("DONE", 0),
                "EXIT": c.get("EXIT", 0),
                "failed": c.get("SUBMIT_FAILED", 0) + c.get("LOST", 0)}

    def _on_post_process(self, jsid, result):
        if result is None:
            self._log(jsid, "post_processing_finished — 후처리 실패")
            return
        self._log(jsid, f"post_processing_finished — DONE {result['DONE']} / "
                        f"EXIT {result['EXIT']} / 실패 {result['failed']} "
                        f"(총 {result['total']})")

    def _on_handler(self, jsid, name, res):
        if name != HANDLER:
            return
        if res.error:
            self._log(jsid, f"[handler 오류] {res.job_key}: {res.error}")
            return
        kind = "최종" if res.final else "중간"
        d = res.data
        self._log(jsid, f"[handler {kind}] {res.job_key}: {d['lines']}줄 "
                        f"경과={d['elapsed_s']}s '{d['last'][:40]}'")

    def _on_double_click(self, item):
        """행 상세 — 로컬 스냅샷만 읽는다 (v10.3: bhist 온디맨드 조회 삭제)."""
        js = self._handle()
        if js is None:
            return
        key = self.jobtable.item(item.row(), 0).text()
        rec = next((r for r in js.jobs() if r.job_key == key), None)
        if rec is None:
            return
        fields = [("job_id", rec.job_id), ("state", rec.state.name),
                  ("exit_code", rec.exit_code), ("run_time_s", rec.run_time_s),
                  ("start", rec.start_time), ("finish", rec.finish_time),
                  ("cluster", rec.forward_cluster or rec.source_cluster),
                  ("cwd", rec.submit_cwd or os.getcwd()),
                  ("command", rec.command),
                  ("fail", rec.fail_message)]
        body = "\n".join(f"  {k}={v}" for k, v in fields if v not in (None, ""))
        self._log(js.id, f"상세 {key}:\n{body}")

    # ------------------------------------------------------------------
    # 요약/테이블 갱신
    # ------------------------------------------------------------------
    def _selected_id(self):
        item = self.tree.currentItem()
        return item.text(0) if item else None

    def _handle(self):
        jsid = self._selected_id()
        return self.mgr.jobset(jsid) if (jsid and self.mgr) else None

    def _on_progress(self, jsid, done, total):
        if jsid == self._active_submit or jsid == self._selected_id():
            self.bar.setMaximum(max(total, 1))
            self.bar.setValue(done)

    def _on_finished(self, jsid, rpt):
        recs = self.mgr.jobset(jsid).jobs()
        n_ids = len({r.job_id for r in recs if r.job_id is not None})
        self._log(jsid, f"submit_finished ok={rpt.ok} failed={rpt.failed} "
                        f"retried={rpt.retried} job_id확보={n_ids} "
                        f"({rpt.duration_s:.1f}s)")

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
        trans = []
        for r in records:
            cur = r.state.value
            prev = self._laststate.get(r.job_key)
            if prev != cur:
                self._laststate[r.job_key] = cur
                trans.append((r.job_key, prev, cur, r))
        if len(trans) <= 12:
            for key, prev, cur, r in trans:
                arrow = f"{prev} → {cur}" if prev else f"(신규) → {cur}"
                extra = (f" (exit={r.exit_code})"
                         if r.state == JobState.EXIT else "")
                self._log(jsid, f"상태 {key}: {arrow}{extra}")
        elif trans:
            c = Counter(f"{p or '신규'}→{cur}" for _, p, cur, _ in trans)
            self._log(jsid, f"상태 전이 {len(trans)}건: "
                            + ", ".join(f"{k} x{v}" for k, v in c.items()))
        if jsid == self._selected_id():
            self._apply_jobs(records)

    def _apply_jobs(self, records):
        """변경분만 job_key 기준 행 upsert — 전체 재그리기 금지 (README §5.3)."""
        self.jobtable.setUpdatesEnabled(False)
        try:
            for r in records:
                row = self._row_of.get(r.job_key)
                if row is None:
                    row = self.jobtable.rowCount()
                    self.jobtable.insertRow(row)
                    self._row_of[r.job_key] = row
                self._set_row(row, r)
        finally:
            self.jobtable.setUpdatesEnabled(True)

    def _set_row(self, row, r):
        color = _STATE_COLOR.get(r.state)
        cluster = r.forward_cluster or r.source_cluster or ""
        cells = [r.job_key, str(r.job_id or "-"), r.state.value,
                 "" if r.exit_code is None else str(r.exit_code),
                 str(r.retry_count), r.fail_reason or "", cluster]
        for col, text in enumerate(cells):
            it = QTableWidgetItem(text)
            if color and col == 2:
                it.setForeground(QBrush(QColor(color)))
            self.jobtable.setItem(row, col, it)

    def _reload_jobs(self):
        self.jobtable.setRowCount(0)
        self._row_of = {}
        js = self._handle()
        if js is not None:
            self._apply_jobs(js.jobs())

    def _append(self, line):
        self.log.appendPlainText(line)

    def _log(self, jsid, msg):
        self.log.appendPlainText(f"[{jsid[-8:]}] {msg}")


def main():
    app = QApplication(sys.argv)
    install_logging()
    win = Dashboard()
    # collect_clusters=True — MC forward 흉내 시 forward_cluster 폴링 수집
    # cluster_envpaths — MC 분류 kill 매핑(앱 환경 속성이라 생성자에서 한 번)
    win.bind_manager(make_manager(
        workers=8, max_retry=3, collect_clusters=True,
        cluster_envpaths={c: cluster_env_path(c) for c in FORWARD_CLUSTERS},
        runner=make_logging_runner(win.bus)))
    win.resize(1180, 800)
    win.show()

    # 스트레스 훅: LSFMGR_DEMO_SUBMIT=5000 → 기동 직후 대량 제출.
    n = os.environ.get("LSFMGR_DEMO_SUBMIT")
    if n:
        configure_mocklsf(submit_fail_rate=0, submit_delay=0)
        win.form.count.setValue(int(n))
        win.form.workers.setValue(32)
        QTimer.singleShot(300, win.submit)
    # 스모크 훅: 기동 → 소량 제출(handler 포함) → kill → autoquit.
    if os.environ.get("LSFMGR_DEMO_AUTORUN") == "1":
        win.form.count.setValue(8)
        win.form.use_handler.setChecked(True)
        QTimer.singleShot(500, win.submit)
        QTimer.singleShot(6000, win.kill)
    maybe_autoquit(app)                       # LSFMGR_DEMO_AUTOQUIT=<초>

    rc = app.exec_() if hasattr(app, "exec_") else app.exec()
    win.mgr.shutdown()
    return rc


if __name__ == "__main__":
    sys.exit(main())
