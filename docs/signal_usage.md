# Signal 사용 가이드 — 대량 job 명령을 "바로바로" 반영하기

대량 job에 대한 **사용자 명령**(submit / kill / resubmit)은 폴링(bjobs) 주기를
기다리지 않고 **명령 직후 ms 단위로** 테이블·프로그레스바에 반영됩니다. 이 문서는
그렇게 되도록 어떤 Signal을 어떻게 연결하는지 정리합니다.

실측 반응(mocklsf): submit `SUBMITTING` 1.5ms → PEND 점진 · kill `EXIT` ~100ms ·
resubmit `EXIT→SUBMITTING→PEND` ~175ms.

---

## 0. 세 가지 원칙 (이것만 지키면 됨)

1. **테이블은 `jobs_updated`로만 그린다.** `kill_finished` 같은 완료 보고서로
   상태를 **수동 추론하지 마라** — 폴링이 준 실제 상태와 충돌해 깜빡인다.
2. **행은 `job_key`로 증분 upsert** 한다. 매 배치마다 테이블 전체를 다시 그리면
   5000개에서 렌더링만 ~17초 걸려 GUI가 언다(증분은 ~0.3초, 52배).
3. **명령을 부르기 전에 Signal을 connect** 한다. 모든 제어 API는 비동기라 결과가
   나중에 Signal로 온다 — 늦게 연결하면 초기 발화를 놓친다.

---

## 1. Signal 지도 — 명령별로 무엇을 듣나

`mgr = LsfJobManager(...)` 의 Signal (모든 인자 첫 번째는 `jobset_id`):

| 사용자 명령 | 진행률 | 개별 job 상태 | 요약 | 완료 |
|---|---|---|---|---|
| **submit(_wrapper/_bulk)** | `submit_progress` | `jobs_updated` | `jobset_updated` | `submit_finished` |
| **kill** (대량 chunk) | `kill_progress` | `jobs_updated` | `jobset_updated` | `kill_finished` |
| **resubmit** | `submit_progress` | `jobs_updated` | `jobset_updated` | `submit_finished` |

공통: `error_occurred(jsid, msg)` (워커 예외), `job_lost(jsid, rec)` (LSF 소실).

- **`jobs_updated(jsid, [JobRecord])`** — 상태가 바뀐 job들의 **배치**. 대량이어도
  throttle로 묶여 온다(job당 아님). 테이블은 이걸로만 갱신한다.
- **`jobset_updated(jsid, summary)`** — 상태별 카운트 dict
  `{"total", "PEND", "RUN", "DONE", "EXIT", "SUBMITTING", "SUBMIT_FAILED", ...}`.
- **`*_progress(jsid, done, total)`** — 프로그레스바용. 마지막은 항상 `(total, total)`.

> ⚠️ 개별 job 스트림(`jobs_updated`)은 **`mgr.*`에만** 있다. JobSet 핸들(`js.*`)엔
> per-job이 없으니, **테이블은 단일 JobSet이라도 `mgr.jobs_updated`를 쓴다**
> (필요하면 `if jsid == 내JobSet:` 로 거른다).

---

## 2. 연결은 한 번만 (앱 시작 시)

```python
from lsfmgr import LsfJobManager, JobState

class Dashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.mgr = LsfJobManager()            # bsub_path 등은 실제 환경/‏mocklsf에 맞게
        self.table = JobTable()               # §5의 증분 테이블
        self._wire_signals()

    def _wire_signals(self):
        m = self.mgr
        # 개별 job 상태 — 테이블(모든 명령의 상태 전이가 여기로 온다)
        m.jobs_updated.connect(self._on_jobs)
        # 요약 — 카운트 라벨/막대
        m.jobset_updated.connect(self._on_summary)
        # 진행률 — submit / kill 공용 프로그레스바
        m.submit_progress.connect(self._on_progress)
        m.kill_progress.connect(self._on_progress)
        # 완료 보고 — 통계/토스트만 (상태 반영은 위 jobs_updated가 이미 함)
        m.submit_finished.connect(self._on_submit_done)
        m.kill_finished.connect(self._on_kill_done)
        # 예외/소실
        m.error_occurred.connect(lambda jsid, msg: self.statusBar().showMessage(msg))
        m.job_lost.connect(lambda jsid, rec: self._on_jobs(jsid, [rec]))

    # --- 핵심: 테이블은 jobs_updated로만, job_key 증분 upsert ---
    def _on_jobs(self, jsid, records):
        if jsid == self._current_jsid:        # 지금 보고 있는 JobSet만
            self.table.apply(records)         # 바뀐 행만 갱신 (§5)

    def _on_summary(self, jsid, s):
        if jsid == self._current_jsid:
            self.count_label.setText(
                f"PEND {s.get('PEND',0)} / RUN {s.get('RUN',0)} / "
                f"DONE {s.get('DONE',0)} / EXIT {s.get('EXIT',0)} / 총 {s['total']}")

    def _on_progress(self, jsid, done, total):
        self.bar.setMaximum(total); self.bar.setValue(done)
```

---

## 3. 대량 submit (wrapper)

```python
def on_submit_clicked(self):
    cmds = [f"primesim_sub -q normal run_{i}.sp" for i in range(5000)]
    js = self.mgr.submit_wrapper(cmds, workers=32, max_retry=3, label="sweep")
    self._current_jsid = js.id                # 이 JobSet을 테이블에 표시
    self.bar.setValue(0)
```

일어나는 일 (자동):
- **명령 직후** 5000개가 `SUBMITTING`으로 `jobs_updated`에 한 번에 온다 → 표 즉시 채워짐.
- 각 job이 bsub 완료되는 대로 `PEND`로 `jobs_updated` 점진 배치 → 표가 실시간 갱신.
- `submit_progress`로 막대 진행. 끝나면 `submit_finished(jsid, SubmitReport)`.

```python
def _on_submit_done(self, jsid, rep):         # SubmitReport
    self.statusBar().showMessage(
        f"제출 완료: 성공 {rep.succeeded}/{rep.total}, 실패 {rep.failed} "
        f"({rep.duration_s:.1f}s)")
```

> `submit_wrapper`는 `JobSet`을, `submit_bulk`는 `jobset_id` 문자열을 반환한다.
> 어느 쪽이든 `mgr.*` Signal은 `jobset_id`로 오므로 위 배선 그대로 동작한다.

---

## 4. 대량 kill

특정 JobSet의 선택 행만 죽일 때는 **`js.kill_jobs(job_keys)`** 를 쓴다 — jobset
컨텍스트가 있어 optimistic EXIT(즉시 EXIT 반영) + 결과 중계가 모두 켜진다.

```python
def on_kill_selected(self):
    js = self.mgr.jobset(self._current_jsid)
    keys = self.table.selected_job_keys()     # 선택 행의 job_key들
    js.kill_jobs(keys)                        # → 즉시 EXIT가 jobs_updated로 옴
```

전체/상태별 kill:
```python
js.kill()                          # 전체 (bkill -g 1회, 대량도 빠름)
js.kill(only_state=JobState.PEND)  # PEND만
```

- 대량 chunk kill이면 `kill_progress`로 진행률(막대). 전체 group kill은 명령 1회라 바로 완료.
- optimistic(기본) 정책이라 확인되는 대로 **즉시 `EXIT`가 `jobs_updated`로** 온다.
  → 폴링 안 기다림. **`kill_finished`로 상태를 수동 EXIT 처리하지 말 것**(깜빡임 원인).

```python
def _on_kill_done(self, jsid, rep):           # KillReport — 통계만
    if rep.unconfirmed:
        self.statusBar().showMessage(
            f"kill: {rep.requested - rep.unconfirmed}/{rep.requested} 확인, "
            f"{rep.unconfirmed} 미확인")
```

> `mgr.kill_jobs([job_ids])` 를 `jobset_id` 없이 부르면 결과가 특정 JobSet으로
> 중계되지 않는다. **`js.kill_jobs(keys)`** (또는 `mgr.kill_jobs(ids, jobset_id=...)`)를
> 권장한다.

---

## 5. resubmit (파이프라인)

```python
def on_resubmit_selected(self):
    js = self.mgr.jobset(self._current_jsid)
    keys = self.table.selected_job_keys()
    js.resubmit_jobs(keys)                     # 상태 기반 자동 분기
    # 커맨드를 바꿔 재실행하려면:
    # js.resubmit_jobs(keys, commands={key: "primesim_sub -q long a.sp"})
```

선택 job이 섞여 있어도 **조건별로** 처리되고, 그 과정이 `jobs_updated`로 단계별 발행:

| 대상 job 상태 | kill? | 테이블에 보이는 순서 |
|---|---|---|
| 살아있음(PEND/RUN) | O | **EXIT** → **SUBMITTING** → **PEND** |
| 이미 terminal(EXIT/DONE) | X | SUBMITTING → PEND |
| 미제출(SUBMIT_FAILED/LOST) | X | SUBMITTING → PEND |

즉 §2에서 `jobs_updated`만 연결해 두면 **추가 코드 없이** 파이프라인 단계가 그대로
표에 나타난다. 최종은 submit과 동일하게 `submit_finished`.

---

## 6. 테이블 — `job_key` 증분 upsert (대량 필수)

매 배치마다 전체를 다시 그리면 대량에서 언다. **바뀐 행만** 갱신한다.

```python
class JobTable(QTableWidget):
    COLS = ["job", "job_id", "state", "exit", "retry", "reason"]
    COLOR = {JobState.RUN: "#1565c0", JobState.DONE: "#2e7d32",
             JobState.EXIT: "#c62828", JobState.SUBMIT_FAILED: "#c62828",
             JobState.PEND: "#f9a825", JobState.SUBMITTING: "#6a1b9a"}

    def __init__(self):
        super().__init__(0, len(self.COLS))
        self.setHorizontalHeaderLabels(self.COLS)
        self._row_of = {}                      # job_key → 행 번호

    def reset_for(self, records):              # JobSet 선택이 바뀔 때 1회
        self.setRowCount(0); self._row_of = {}
        self.apply(records)

    def apply(self, records):                  # jobs_updated 배치 — 증분
        self.setUpdatesEnabled(False)
        try:
            for r in records:
                row = self._row_of.get(r.job_key)
                if row is None:                # 신규 → 행 추가
                    row = self.rowCount(); self.insertRow(row)
                    self._row_of[r.job_key] = row
                self._set_row(row, r)
        finally:
            self.setUpdatesEnabled(True)

    def _set_row(self, row, r):
        cells = [r.job_key, str(r.job_id or "-"), r.state.value,
                 "" if r.exit_code is None else str(r.exit_code),
                 str(r.retry_count), r.fail_reason or ""]
        for col, text in enumerate(cells):
            it = QTableWidgetItem(text)
            c = self.COLOR.get(r.state)
            if c and col == 2:
                it.setForeground(QBrush(QColor(c)))
            self.setItem(row, col, it)
```

- JobSet **선택이 바뀌면** `reset_for(js.jobs())`로 전체 1회 재구성 + 맵 재빌드.
- 이후 `jobs_updated` 배치는 `apply(records)`로 바뀐 행만 손댐.
- 수만 행이면 `QAbstractTableModel`로 가면 더 가볍다(원리는 동일 — key로 upsert).

---

## 7. 로그 위젯 도배 방지

대량이면 job별 전이 로그가 위젯을 마비시킨다. **소량은 per-job, 대량은 요약**:

```python
from collections import Counter

def _on_jobs(self, jsid, records):
    ...                                        # 테이블 갱신
    trans = [(r.job_key, r.state.value) for r in records
             if self._last.get(r.job_key) != r.state.value]
    for k, s in trans:
        self._last[k] = s
    if len(trans) <= 12:
        for k, s in trans:
            self.log.appendPlainText(f"{k} → {s}")
    elif trans:
        c = Counter(s for _, s in trans)
        self.log.appendPlainText(
            "전이 " + ", ".join(f"{s} x{n}" for s, n in c.items()))
```

---

## 8. 발화 빈도(부하) 조절

모든 progress/`jobs_updated`는 이미 throttle된다(기본 0.1초 OR 1% 진행마다 배치 →
job 수와 무관하게 ~100회). 더 성기게(부하↓) 하려면 생성 시:

```python
mgr = LsfJobManager(progress_min_interval_s=0.25,   # 기본 0.1
                    progress_min_step_ratio=0.02)   # 기본 0.01
```

---

## 9. 종료 — core dump 방지

shutdown은 `aboutToQuit`/`atexit`에 자동 연결되지만, **명시 호출이 가장 확실**하다:

```python
def closeEvent(self, e):
    self.mgr.shutdown()        # 멱등 — 중복 안전. 스레드 정리 후 종료.
    super().closeEvent(e)
```

---

## 10. `mgr.*` vs `js.*`

- **`mgr.*`** — 모든 JobSet 이벤트가 `jobset_id`와 함께. **개별 job(`jobs_updated`)은
  여기에만 있으니 테이블은 항상 `mgr.*`.** 여러 JobSet 대시보드에 적합.
- **`js.*`** — 특정 JobSet으로 좁힌 편의 계층(같은 이름, `jobset_id` 인자만 없음).
  요약(`js.jobset_updated`)·완료(`js.submit_finished`/`js.kill_finished`)·진행
  (`js.submit_progress`/`js.kill_progress`)·실패(`js.jobs_failed`)만 온다.
  단일 JobSet 위젯의 요약/진행 표시에 편리.

정리: **개별 job 테이블 = `mgr.jobs_updated`(필요 시 jsid 필터), 요약/진행/완료 =
아무거나 편한 쪽.**
