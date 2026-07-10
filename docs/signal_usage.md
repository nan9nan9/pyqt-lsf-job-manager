# Signal 사용 가이드 — 대량 job 명령을 "바로바로" 반영하기

대량 job에 대한 **사용자 명령**(submit / kill / 재실행)은 폴링(bjobs) 주기를
기다리지 않고 **명령 직후 ms 단위로** 테이블·프로그레스바에 반영됩니다. 이 문서는
그렇게 되도록 어떤 Signal을 어떻게 연결하는지 정리합니다.

실측 반응(mocklsf): submit `SUBMITTING` 1.5ms → PEND 점진 · kill `EXIT` ~100ms ·
재실행(merge+submit) `SUBMITTING→PEND` ~175ms.

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
| **재실행** (`mgr.submit(js)`) | `submit_progress` | `jobs_updated` | `jobset_updated` | `submit_finished` |

공통: `error_occurred(jsid, msg)` (워커 예외), `job_lost(jsid, rec)` (LSF 소실).

- **`jobs_updated(jsid, [JobRecord])`** — 상태가 바뀐 job들의 **배치**. 대량이어도
  throttle로 묶여 온다(job당 아님). 테이블은 이걸로만 갱신한다.
- **`jobset_updated(jsid, summary)`** — 상태별 카운트 dict
  `{"total", "PEND", "RUN", "DONE", "EXIT", "SUBMITTING", "SUBMIT_FAILED", ...}`.
- **`*_progress(jsid, done, total)`** — 프로그레스바용. 마지막은 항상 `(total, total)`.

> 개별 job 스트림은 `mgr.jobs_updated(jsid, records)` 와
> `js.jobs_updated(records)` **둘 다** 있다 — 단일 JobSet 테이블은 `js.*`
> (필터 불필요), 다중 JobSet 대시보드는 `mgr.*`(jsid로 분기).

---

## 1.1 발화 주기 (cadence) — 언제/얼마나 자주 오나

발화 주기는 크게 **①throttle(고빈도 진행) · ②폴링 주기 · ③일회성** 세 종류다.
이를 지배하는 노브는 딱 둘:

```python
progress_min_interval_s = 0.5   # ① 진행 시그널 최소 발화 간격(초)
progress_min_step_ratio = 0.01  # ① 최소 진행 비율(전체의 1%)
poll_interval_s         = 10    # ② 폴링 주기(5~60)
```

**throttle 규칙** — 다음 중 하나라도 만족하면 발화한다:
`done == total`(**마지막은 항상**) **또는** 마지막 발화 후 `0.5초` 경과 **또는**
`max(1, total의 1%)` 만큼 진행. → 즉 진행 시그널은 **초당 최대 ~2회 또는 1%
단위**(먼저 오는 것) + **마지막 100% 1회 보장**.

| Signal | 발화 주기 | 지배 |
|---|---|---|
| `submit_progress` | **throttled** (≤2/s, 1%씩) + 마지막 `(total,total)` | ① |
| `jobs_updated` (제출 중) | submit_progress와 **동일 cadence** (changed 배치) | ① |
| `jobset_updated` (제출 중) | 위 배치와 함께 | ① |
| `jobset_updated` (제출 완료) | **1회** (초기 전원 PEND) | 이벤트 |
| `kill_progress` | **throttled** (chunk kill일 때만) + 마지막 100% | ① |
| `jobset_updated` (폴링) | **폴링 사이클마다 매번 1회** | ② |
| `jobs_updated` (폴링) | 폴링 사이클마다, **변경분 있을 때만** | ② |
| `job_lost` | 폴링에서 LOST 확정된 **레코드마다** | ② |
| `handler_finished` | **폴링 사이클마다** job당 1회 (+종료 직후 final 보충 1회) | ② |
| `submit_started`/`ready_started`/`ready_finished` | **일회성** | 이벤트 |
| `submit_finished` / `kill_finished` | **일회성** (retry 포함 최종) | 이벤트 |
| `error_occurred` | worker 예외 **발생 시마다** | 이벤트 |
| `job_detail_ready` | `fetch_job_detail()` 호출당 **1회** | 온디맨드 |

- `js.jobs_failed`는 `jobs_updated`에서 실패분(SUBMIT_FAILED/EXIT/LOST)만 걸러
  발화하므로 `jobs_updated`와 **같은 주기**다.
- **전체 JobSet kill**(`mgr.kill(js)`)은 `bkill -g` 1회라 증분 없이 바로 완료 —
  `kill_progress`가 유의미한 건 **대량 chunk/부분 kill, MC `envpath`(chunk마다
  env source), `verify`(재조회 루프)** 일 때.
- 폴링 주기는 생성 시 `poll_interval_s=` 또는 `mgr.start_polling(js, …)`로,
  throttle은 `progress_min_interval_s`/`progress_min_step_ratio`로 조절(§8).

### pull 스냅샷 — 시그널을 놓친 뒤 "지금 상태"를 직접 조회

진행 시그널은 **push**라 놓치면(진행 dialog를 닫는 등) 다시 안 온다. 백그라운드로
돌려놓고 나중에 상태 패널을 다시 그릴 때는 **아무 때나 pull로 현재 진행을 조회**한다
(시그널 연결과 무관, 즉시 반환):

```python
if js.is_submitting:                    # 제출 작업 자체가 도는 중?
    s = js.submit_state                 # SubmitProgress(done/total/succeeded/failed/…) | None
    bar.setValue(int(s.fraction * 100))
if js.is_killing:                       # kill이 도는 중?
    s = js.kill_state                   # KillProgress(done/total) | None
    bar.setValue(int(s.fraction * 100))
```

- 진행 중이 아니면 `None`. 완료 후 최종 결과는 `submit_finished`/`kill_finished`
  또는 `js.summary`로 본다.
- pull은 throttle과 무관하게 **항상 최신값**이다(throttle로 건너뛴 진행도 반영).

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

## 2.1 프로그레스바 배선 (submit)

가장 단순한 형태 — 시작 시 표시, 진행 시 갱신, 완료 시 숨김:

```python
# 1) submit 시작 → 바 초기화/표시
mgr.submit_started.connect(lambda jsid: (
    bar.setValue(0), bar.setVisible(True)))

# 2) 진행 → 값 갱신  (done, total 그대로 옴, 0.5초/1% throttle이라 스팸 없음)
mgr.submit_progress.connect(lambda jsid, done, total: (
    bar.setMaximum(total), bar.setValue(done)))

# 3) 완료 → 마무리/숨김  (마지막 progress는 항상 (total, total) 보장)
mgr.submit_finished.connect(lambda jsid, rpt: bar.setVisible(False))
```

- `submit_progress(jsid, done, total)`: **done = bsub 완료된 job 수**(성공+실패+취소
  합산), total = 전체. `setMaximum(total)` + `setValue(done)` 이면 끝.
- **throttle**: 0.5초 또는 1% 변화마다만 발화 → 5000개여도 GUI 안 막힘.
- **마지막 통지는 반드시 `(total, total)`** → 바가 항상 100%로 끝남.

단일 JobSet이면 (jsid 필터 불필요):

```python
js.submit_progress.connect(lambda done, total: (
    bar.setMaximum(total), bar.setValue(done)))
```

## 2.2 프로그레스바 배선 (kill) — 완전히 동일

대량 chunk kill도 `submit_progress`와 같은 시그니처(`done, total`)로 온다:

```python
mgr.kill_progress.connect(lambda jsid, done, total: (
    bar.setMaximum(total), bar.setValue(done)))
mgr.kill_finished.connect(lambda jsid, rpt: bar.setVisible(False))
# 단일 JobSet: js.kill_progress.connect(lambda done, total: ...)
```

- 전체 JobSet kill(`mgr.kill(js)`)은 `bkill -g` 1회라 진행 없이 바로 완료된다 —
  `kill_progress`가 유용한 건 **대량 개별 kill/부분 kill/chunk fallback**일 때.
- submit·kill이 같은 막대를 공유하면 두 `*_progress`를 같은 슬롯에 연결하면 된다.

---

## 3. 대량 submit (wrapper)

```python
def on_submit_clicked(self):
    cmds = [f"customwrapper_sub -q normal run_{i}.sp" for i in range(5000)]
    js = self.mgr.create_jobset(cmds, label="sweep")   # wrapper 커맨드 그대로
    self.mgr.submit(js, workers=32, max_retry=3)
    self._current_jsid = js.id                # 이 JobSet을 테이블에 표시
    self.bar.setValue(0)
```

일어나는 일 (자동):
- `create_jobset` 직후 5000개가 `CREATED`로, `submit` 착수 직후 `SUBMITTING`
  리셋이 `jobs_updated`에 한 번에 온다 → 표 즉시 채워짐.
- 각 job이 bsub 완료되는 대로 `PEND`로 `jobs_updated` 점진 배치 → 표가 실시간 갱신.
- `submit_progress`로 막대 진행. 끝나면 `submit_finished(jsid, SubmitReport)`.

```python
def _on_submit_done(self, jsid, rep):         # SubmitReport
    self.statusBar().showMessage(
        f"제출 완료: 성공 {rep.succeeded}/{rep.total}, 실패 {rep.failed} "
        f"({rep.duration_s:.1f}s)")
```

> `mgr.*` Signal은 첫 인자 `jobset_id`(문자열)로 오므로 위 배선 그대로
> 동작한다 — 핸들이 필요하면 `mgr.jobset(jsid)`로 재획득한다.

---

## 4. 대량 kill

특정 JobSet의 선택 행만 죽일 때는 **`mgr.kill_jobs(js, job_keys)`** 를 쓴다 —
jobset 컨텍스트가 있어 optimistic EXIT(즉시 EXIT 반영) + 결과 중계가 모두 켜진다.

```python
def on_kill_selected(self):
    js = self.mgr.jobset(self._current_jsid)
    keys = self.table.selected_job_keys()     # 선택 행의 job_key들
    self.mgr.kill_jobs(js, keys)              # → 즉시 EXIT가 jobs_updated로 옴
```

전체/상태별 kill:
```python
mgr.kill(js)                           # 전체 (bkill -g 1회, 대량도 빠름)
mgr.kill(js, only_state=JobState.PEND) # PEND만
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

> `mgr.kill_jobs([job_ids])` 를 jobset 컨텍스트 없이 부르면 결과가 특정
> JobSet으로 중계되지 않는다. **`mgr.kill_jobs(js, keys)`** (또는
> `mgr.kill_jobs(ids, jobset_id=...)`)를 권장한다.

---

## 5. 재실행 — merge + submit (v9, resubmit 제거)

재실행은 별도 API가 아니라 **데이터 조작 + 일반 submit**이다 — 실패/수정
job을 같은 `merge_id`로 담은 jobset을 만들어 흡수한 뒤 전체 재제출한다:

```python
def on_rerun_failed(self):
    js = self.mgr.jobset(self._current_jsid)
    failed = [r for r in js.jobs() if r.state.is_failed]
    if not failed or not self.mgr.can_submit(js):
        return                                 # 활성 job 있으면 먼저 kill
    fix = self.mgr.create_jobset(
        [shlex.split(r.command) for r in failed],
        merge_ids=[r.merge_id for r in failed],
        ud_datas=[r.ud_data for r in failed], label="rerun")
    self.mgr.merge(js, fix)                    # 같은 merge_id → CREATED 교체
    self.mgr.submit(js)                        # 전 job 재제출
```

- `mgr.merge`의 replace는 **물리 key(job_key)를 유지**하므로 테이블 행이
  그대로 이어진다(§6의 upsert 맵이 안 깨진다).
- 살아있는 job이 남았으면 `mgr.submit`이 거부한다 — `mgr.kill(js)` 후
  `kill_finished`에서 이어가거나 `can_submit`으로 버튼을 비활성화한다.
- 테이블에 보이는 순서: 교체분 **CREATED** → submit 후 전원
  **SUBMITTING → PEND** (§2에서 `jobs_updated`만 연결해 두면 자동).
  최종은 submit과 동일하게 `submit_finished`.

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

모든 progress/`jobs_updated`는 이미 throttle된다(기본 0.5초 OR 1% 진행마다 배치).
더 성기게(부하↓) 하려면 생성 시:

```python
mgr = LsfJobManager(progress_min_interval_s=1.0,    # 기본 0.5
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

## 10. `mgr.*` vs `js.*` — 역할 분리 (v9)

- **명령은 전부 `mgr.*`** — `mgr.submit(js)` / `mgr.kill(js)` /
  `mgr.merge(a, b)` / `mgr.create_jobset(…)` … 핸들(또는 jobset_id)을
  인자로 넘긴다. 핸들에는 명령 메서드가 없다 — API가 한 곳뿐이라
  "어디를 불러야 하나" 고민이 없다.
- **조회(pull)와 Signal은 `js.*`가 편리** — `js.jobs()`/`js.summary`/
  `js.is_done`/`js.kill_state` + 신호 전부(핸들 신호는 jsid 인자만 없음).
  다중 JobSet 대시보드면 `mgr.*` 신호를 jsid로 분기.

정리: **명령 = mgr, 상태·신호 = js.** 단일 JobSet 위젯은 js 신호에
바인딩하고 버튼 핸들러에서 mgr를 부른다.
