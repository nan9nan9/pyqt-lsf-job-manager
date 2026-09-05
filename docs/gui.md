# GUI 연동 가이드 — Signal 배선 · 진행 표시 · 대량 갱신

PyQt/qtpy 앱에서 `LsfJobManager`로 대량 job을 제출·감시·kill할 때, 명령 결과를
**폴링 주기를 기다리지 않고 ms 단위로** 화면에 반영하는 방법과 **반드시 지켜야 할
주의사항**을 정리한다. 실행 가능한 통합 예제는
[`../examples/gui_demo.py`](../examples/gui_demo.py).

실측 반응(mocklsf): submit `SUBMITTING` 1.5ms → PEND 점진 · kill `EXIT` ~100ms ·
재실행(replace_jobs+submit) `SUBMITTING→PEND` ~175ms.

---

## 0. 네 가지 원칙 (이것만 지키면 됨)

1. **테이블은 `jobs_updated`로만 그린다.** `kill_finished` 같은 완료 보고서로 상태를
   **수동 추론하지 마라** — 폴링이 준 실제 상태와 충돌해 깜빡인다.
2. **행은 `job_key`로 증분 upsert** 한다. 매 배치마다 테이블 전체를 다시 그리면
   5000개에서 렌더링만 ~17초 걸려 GUI가 언다(증분은 ~0.3초, 52배).
3. **명령을 부르기 전에 Signal을 connect** 한다. 모든 제어 API는 비동기라 결과가
   나중에 Signal로 온다 — 늦게 연결하면 초기 발화를 놓친다.
4. **콜백에서 위젯을 만지지 마라.** Signal slot은 main 스레드지만
   `pre_submit`/`post_process`/handler 콜백은 **worker 스레드**다.
5. **slot 에서 예외를 흘리지 마라 — 프로세스가 죽는다.** PyQt 는 slot 밖으로
   나간 예외를 `qFatal()` 로 처리해 **abort(core dump)** 한다. direct 든
   queued(worker→main) 든 마찬가지다(둘 다 실측 exit 134).

   ```python
   def on_jobs_updated(self, jsid, records):
       try:
           self.table.apply(records)
       except Exception:                     # 앱 코드의 버그가 GUI를 죽이지 않게
           log.exception("jobs_updated 처리 실패")
   ```

   라이브러리는 **자기 코드**가 slot 에서 터지는 경로를 전부 막아 두었지만
   (`handlers.tick`, `pacer._drain`, `_emit_summary` …), **앱이 연결한 slot**
   안에서 나는 예외는 라이브러리가 가로챌 수 없다. 특히 `_safe_emit` 은
   **direct 연결에서만** 방어가 된다 — queued 면 `emit()` 은 이벤트를 post 만
   하고 slot 은 나중에 이벤트 루프에서 돌기 때문이다.

---

## 1. 스레딩 모델 — 어디서 실행되나

```
[GUI/main 스레드]                     [worker 스레드 (QThreadPool 등)]
  mgr.submit(js)  ──명령 접수(동기)──▶  wrapper 실행, 재시도, 폴링, kill...
      │                                        │
      │        ◀── Signal (queued) ────────────┘
      ▼
  slot 실행 (위젯 갱신)
```

| 실행 위치 | 안전한가 | 규칙 |
|---|---|---|
| **Signal slot** (`submit_finished`, `jobs_updated`…) | ✅ main 스레드 | 위젯 갱신 OK |
| **콜백** (`pre_submit`, `post_process`, handler fn) | ❌ **worker 스레드** | **위젯·QWidget 접근 절대 금지** |

콜백에서 `label.setText(...)` 같은 GUI 호출을 하면 **크래시하거나 정의되지 않은
동작**을 일으킨다. 콜백에서는 순수 계산만 하고 결과를 **반환**하라 — 그 값은
`post_processing_finished`(main 스레드 Signal)로 전달되어 거기서 UI에 반영한다.

```python
# ❌ 잘못됨 — 콜백(worker)에서 위젯 접근
def post(records):
    self.label.setText("완료")        # 크래시 위험

# ✅ 올바름 — 콜백은 값만 반환, UI는 signal slot에서
def post(records):
    return {"done": sum(1 for r in records if r.state.name == "DONE")}
js.post_processing_finished.connect(lambda result: self.label.setText(str(result)))
mgr.submit(js, post_process=post)
```

### 두 계층의 API — 명령은 manager, 구독은 handle

- **`LsfJobManager`(mgr)**: 모든 **명령**의 유일한 진입점(`create_jobset`/`submit`/
  `kill`/`add_jobs`/`remove_jobset`…). Signal도 갖지만 첫 인자로 `jobset_id`가 붙는다 —
  여러 jobset을 한 곳에서 처리하는 대시보드용.
- **`JobSet`(js) 핸들**: `mgr.create_jobset(...)`가 반환하거나 `mgr.jobset(id)`로
  얻는다. **조회(pull) + Signal(view) 전용** — 명령 메서드가 없다. 핸들 Signal은
  그 jobset 것만 오므로 `jsid` 필터가 필요 없어 위젯 연결에 편하다.

---

## 2. Signal 지도 — 명령별로 무엇을 듣나

| 사용자 명령 | 진행률 | 개별 job 상태 | 요약 | 완료 |
|---|---|---|---|---|
| **submit** (재실행 포함) | `submit_progress` | `jobs_updated` | `jobset_updated` | `submit_finished` |
| **kill** | `kill_progress` | `jobs_updated` | `jobset_updated` | `kill_finished` |

공통: `error_occurred` (워커 예외), `job_lost` (LSF에서 소실 확정).

### 2.1 Manager Signal (`mgr.*`) — 첫 인자는 항상 `jobset_id`

| Manager Signal | 시그니처 | 이 Signal 을 발생시키는(트리거) 함수 |
|---|---|---|
| `pre_submit_started` | `(jobset_id)` | `pre_submit` 게이트 시작 (지정 시에만) |
| `pre_submit_finished` | `(jobset_id, ok)` | 게이트 종료. `ok=True`면 이어서 `submit_started` |
| `submit_started` | `(jobset_id)` | `mgr.submit(js)` — 제출 시작 즉시(게이트 지정 시엔 통과 후) |
| `submit_progress` | `(jobset_id, done, total)` | `mgr.submit(js)` 진행 중(throttled) |
| `submit_finished` | `(jobset_id, SubmitReport)` | `mgr.submit(js)` 완료 · `cancel_submit`(중단 마무리) |
| `jobset_updated` | `(jobset_id, summary)` | submit 완료(초기 PEND) · polling · `query_once` |
| `jobs_updated` | `(jobset_id, [JobRecord])` | submit 착수/진행 · polling **변경분이 있을 때만** · kill |
| `job_lost` | `(jobset_id, JobRecord)` | polling · `query_once` · `detect_lost` — 소실 확정 |
| `kill_started` | `(jobset_id)` | `kill(js)` · `kill_jobs(...)` 접수 즉시(동기) |
| `kill_progress` | `(jobset_id, done, total)` | chunk kill 진행(throttled) |
| `kill_failed` | `(jobset_id, message)` | kill 미확인/실패 사유 — `KillReport.errors` 항목마다 1 회, `kill_finished` **직전** |
| `kill_finished` | `(jobset_id, KillReport)` | `kill(js)` · `kill_jobs(...)` |
| `handler_finished` | `(jobset_id, handler_name, HandlerResult)` | `add_handler` 로 등록한 handler 1회 실행 완료 시 |
| `jobset_finished` | `(jobset_id, summary)` | 전 job 이 terminal 도달 — polling · `query_once` · submit 완료(전량 실패 시). **사용자 kill 로 끝난 완료는 발화 안 함** |
| `post_processing_started` | `(jobset_id)` | `submit(post_process=fn)` — 전원 terminal 후처리 착수 |
| `post_processing_finished` | `(jobset_id, result)` | 후처리 콜백 완료 (반환값, 예외 시 `None`) |
| `error_occurred` | `(jobset_id, message)` | 모든 async 경로의 워커 예외 |

### 2.2 JobSet Signal (`js.*`) — 같은 이벤트를 이 JobSet으로 좁혀 발행

**이름은 Manager Signal과 동일**하고 인자에서 `jsid`만 빠진다. 단일 JobSet 위젯이면
필터 없이 이걸 쓰고, 여러 JobSet을 한 곳에서 보면 `mgr.*`를 쓴다.

```python
js.jobs_updated.connect(table.apply)             # ([JobRecord])
js.jobset_updated.connect(badge.set_counts)      # (summary dict)
js.submit_progress.connect(bar.update)           # (done, total)
js.submit_finished.connect(on_done)              # (SubmitReport)
js.kill_started.connect(spinner.start)           # ()
js.kill_finished.connect(on_kill_done)           # (KillReport)
```

- **`js.jobs_failed([JobRecord])`** 는 `mgr.*`에 대응이 없는 **파생 Signal**이다 —
  제출 최종 결과에 `SUBMIT_FAILED`가 있거나 폴링 변경분에 실패 상태(`is_failed`)가
  섞이면 발화한다. `mgr` 계층에서는 `jobs_updated`에서 `is_failed`로 거른다.
- `job_lost`는 `mgr.*`에만 있다 — 단일 JobSet에서도 `mgr.job_lost`를 jsid로 걸러
  쓰거나, `jobs_updated`의 LOST 레코드로 처리한다.

> **`mgr.kill_jobs(job_ids)`를 jobset 컨텍스트 없이 부르면** kill 진행·완료 Signal은
> Manager에서 받는다. `verify=True`는 이 경로에서도 재조회하며, 추적 중인
> 레코드의 상태 변경은 해당 JobSet의 `jobs_updated`/`jobset_updated`로 전달된다.
> 특정 JobSet의 일부 job을 종료하고 그 핸들에서 kill 진행·완료도 받으려면
> **`mgr.kill_jobs(js, job_keys)`** 를 사용한다.

---

## 3. 발화 주기 (cadence) — 언제 얼마나 자주 오나

발화 주기는 **①throttle(고빈도 진행) · ②폴링 주기 · ③일회성** 세 종류다.
이를 지배하는 노브는 딱 셋:

```python
progress_min_interval_s = 0.5   # ① 진행 시그널 최소 발화 간격(초)
progress_min_step_ratio = 0.01  # ① 최소 진행 비율(전체의 1%)
poll_interval_s         = 10    # ② 폴링 주기(5~60)
```

**throttle 규칙** — 다음 중 하나라도 만족하면 발화한다:
`done == total`(**마지막은 항상**) **또는** 마지막 발화 후 `0.5초` 경과 **또는**
`max(1, total의 1%)` 만큼 진행. → 진행 시그널은 **초당 최대 ~2회 또는 1% 단위**
(먼저 오는 것) + **마지막 100% 1회 보장**.

| Signal | 발화 주기 | 지배 |
|---|---|---|
| `submit_progress` | **throttled** (≤2/s, 1%씩) + 마지막 `(total,total)` | ① |
| `jobs_updated` (제출 중) | submit_progress와 **동일 cadence** (changed 배치) | ① |
| `jobset_updated` (제출 중) | 위 배치와 함께 | ① |
| `jobset_updated` (제출 완료) | **1회** (초기 전원 PEND) | 이벤트 |
| `kill_progress` | **throttled** + 마지막 100% | ① |
| `jobset_updated` (폴링) | **폴링 사이클마다 매번 1회** | ② |
| `jobs_updated` (폴링) | 폴링 사이클마다, **변경분 있을 때만** | ② |
| `job_lost` | 폴링에서 LOST 확정된 **레코드마다** | ② |
| `handler_finished` | **폴링 사이클마다** job당 1회 (+종료 직후 final 보충 1회) | ② |
| `submit_started`/`pre_submit_*` | **일회성** | 이벤트 |
| `post_processing_*` | **일회성** (전원 terminal 후처리, `post_process` 지정 시) | 이벤트 |
| `submit_finished` / `kill_finished` | **일회성** (retry 포함 최종) | 이벤트 |
| `error_occurred` | worker 예외 **발생 시마다** | 이벤트 |

- `js.jobs_failed`는 `jobs_updated`에서 실패분만 걸러 발화하므로 **같은 주기**다.
- kill 은 job_id 를 `kill_chunk_size`(기본 16) 단위로 나눠 `bkill` 한다. 기본은
  chunk 를 `kill_workers`(기본 4) 개까지 **동시에** 돌린다 — 직렬이면 소요가
  `ceil(N/chunk) x bkill 1회`로 늘어선다. 대상이 한
  chunk 안에 들어가면 호출 1회로 끝나 진행이 바로 100%가 되고, `kill_progress`가
  의미 있는 건 **대량 kill**과 **`verify`(재조회 루프)** 일 때다.
- **`min_state_dwell_s`**(기본 0=끔)는 위 cadence 위에 얹히는 **표시 간격**이다.
  켜면 `jobs_updated`만 job별로 "한 상태가 그 시간 머문 뒤 다음 전이" 순서로 늦춰
  발화된다 — 자세한 내용과 주의점은 README §6.5.

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

## 4. 연결은 한 번만 (앱 시작 시)

```python
from lsfmgr import LsfJobManager, JobState

class Dashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.mgr = LsfJobManager()            # bjobs_path 등은 실제 환경/mocklsf에 맞게
        self.table = JobTable()               # §7의 증분 테이블
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
            self.table.apply(records)         # 바뀐 행만 갱신 (§7)

    def _on_summary(self, jsid, s):
        if jsid == self._current_jsid:
            self.count_label.setText(
                f"PEND {s.get('PEND',0)} / RUN {s.get('RUN',0)} / "
                f"DONE {s.get('DONE',0)} / EXIT {s.get('EXIT',0)} / 총 {s['total']}")

    def _on_progress(self, jsid, done, total):
        self.bar.setMaximum(total); self.bar.setValue(done)
```

---

## 5. 진행 표시

### 5.1 QProgressBar (상시 표시형 — 권장, 더 단순)

```python
# 1) submit 시작 → 바 초기화/표시
mgr.submit_started.connect(lambda jsid: (bar.setValue(0), bar.setVisible(True)))

# 2) 진행 → 값 갱신 (done, total 그대로 옴, 0.5초/1% throttle이라 스팸 없음)
mgr.submit_progress.connect(lambda jsid, done, total: (
    bar.setMaximum(total), bar.setValue(done)))
mgr.kill_progress.connect(lambda jsid, done, total: (      # kill도 같은 시그니처
    bar.setMaximum(total), bar.setValue(done)))

# 3) 완료 → 마무리/숨김 (마지막 progress는 항상 (total, total) 보장)
mgr.submit_finished.connect(lambda jsid, rpt: bar.setVisible(False))
```

- `submit_progress(done, total)`: **done = 제출이 끝난 job 수**(성공+실패+취소 합산),
  total = 전체. `setMaximum(total)` + `setValue(done)` 이면 끝.
- 단일 JobSet이면 jsid 필터가 필요 없다:
  `js.submit_progress.connect(lambda done, total: ...)`

### 5.2 QProgressDialog — `exec()` 금지, `show()` 사용

진행은 **Signal 구동(비동기)**이다. `QProgressDialog`를 `exec()`로 띄우면 자체 모달
루프가 블록해 진행 처리 흐름과 어긋난다. 반드시 **`show()`(non-modal)** 로 띄우고
Signal이 값을 채우게 하라.

```python
from qtpy.QtWidgets import QProgressDialog
from qtpy.QtCore import Qt

def submit_with_dialog(self, js):
    if not self.mgr.can_submit(js):
        return

    dlg = QProgressDialog("제출 중...", "취소", 0, 0, self)
    dlg.setWindowModality(Qt.WindowModal)   # 부모만 모달 (앱 전체 블록 X)
    dlg.setMinimumDuration(300)             # 300ms 미만이면 안 띄움(깜빡임 방지)
    dlg.setAutoClose(False)                 # 완료 처리는 우리가 직접
    dlg.setAutoReset(False)

    def on_progress(done, total):
        dlg.setMaximum(total)               # total=0이면 busy 인디케이터
        dlg.setValue(done)
    js.submit_progress.connect(on_progress)

    def on_finished(report):                # 이름 있는 함수로 연결해야 해제가 쉽다
        js.submit_progress.disconnect(on_progress)
        js.submit_finished.disconnect(on_finished)
        dlg.reset(); dlg.close()
        self.status.setText(
            f"제출 완료 — 성공 {report.succeeded} / 실패 {report.failed}")
    js.submit_finished.connect(on_finished)

    # 취소 버튼: 아직 제출 안 된 job만 CANCELLED로 확정한다.
    #   ⚠️ 이미 제출된 job은 이걸로 안 멈춘다 — 그건 kill의 영역.
    dlg.canceled.connect(lambda: self.mgr.cancel_submit(js))

    dlg.show()                              # exec() 아님! non-modal
    self.mgr.submit(js, workers=8)
```

주의점:

1. **`exec()` 금지, `show()` 사용.** 앱 전체를 막고 싶지 않으면 `Qt.WindowModal`.
2. **`submit_progress`는 throttled**다 — 매 job마다 오지 않는다. 완료 판정과
   다이얼로그 닫기는 `submit_finished`에서.
3. **취소 버튼의 의미**: `cancel_submit`은 **아직 제출되지 않은** job만 되돌린다.
   이미 LSF에 들어간 job을 죽이려면 `mgr.kill(js)`. 둘 다 원하면 순서대로 호출한다.
4. **연결 해제**: 다이얼로그마다 slot을 새로 연결했다면 완료 시 `disconnect`하라.
5. **kill 진행 다이얼로그**도 동일 패턴 — `kill_started`(접수 즉시 동기)로 먼저
   "접수됨"을 표시하고, `kill_progress`/`kill_finished`로 이어간다.

---

## 6. 명령별 배선

### 6.1 대량 submit

```python
def on_submit_clicked(self):
    cmds = [f"customwrapper_sub -q normal run_{i}.sp" for i in range(5000)]
    js = self.mgr.create_jobset(cmds, label="sweep")   # wrapper 커맨드 그대로
    self._current_jsid = js.id                # 이 JobSet을 테이블에 표시
    self.mgr.submit(js, workers=32, max_retry=3)
    self.bar.setValue(0)
```

일어나는 일 (자동):
- `create_jobset` 직후 5000개가 `CREATED`로, `submit` 착수 직후 `SUBMITTING` 리셋이
  `jobs_updated`에 한 번에 온다 → 표 즉시 채워짐.
- 각 job이 제출되는 대로 `PEND`로 `jobs_updated` 점진 배치 → 표가 실시간 갱신.
- `submit_progress`로 막대 진행. 끝나면 `submit_finished(jsid, SubmitReport)`.

```python
def _on_submit_done(self, jsid, rep):         # SubmitReport
    self.statusBar().showMessage(
        f"제출 완료: 성공 {rep.succeeded}/{rep.total}, 실패 {rep.failed} "
        f"({rep.duration_s:.1f}s)")
```

### 6.2 kill / 선택 kill

```python
mgr.kill(js)                                  # jobset 전체
mgr.kill(js, only_state=JobState.PEND)        # PEND만
mgr.kill(js, verify=True)                     # bkill 후 재조회로 실제 종료 확인
mgr.kill_jobs(js, self.table.selected_job_keys())   # 선택 행만
```

- `kill_started`(동기)로 즉시 "접수됨"을 표시하고, 완료는 `kill_finished`의
  `KillReport`(`requested`/`unconfirmed`/`errors`, verify=True면 `still_alive`)로 받는다.
- optimistic(기본) 정책이라 확인되는 대로 **즉시 `EXIT`가 `jobs_updated`로** 온다 →
  폴링을 안 기다린다. **`kill_finished`로 상태를 수동 EXIT 처리하지 말 것**(깜빡임 원인).
- 진행 중 submit이 있으면 kill이 **우선권**을 갖는다(제출을 멈추고 kill).
- **kill 실패는 상태를 바꾸지 않는다.** 실패했다면 그 job 은 LSF 에서 여전히
  PEND/RUN 이라 그게 맞는 상태이고, EXIT 로 찍으면 거짓말이 된다. 그래서 표에는
  아무 변화가 없다 — `kill_failed` 를 반드시 구독해라. 안 그러면
  사용자가 kill 을 눌렀는데 표도 그대로, 알림도 없는 **완전 무반응**이 된다.

```python
js.kill_failed.connect(               # kill_finished 직전에 온다
    lambda msg: self.statusBar().showMessage(f"kill 실패: {msg}", 10000))

def _on_kill_done(self, jsid, rep):           # KillReport — 통계만
    if rep.unconfirmed:
        self.statusBar().showMessage(
            f"kill: {rep.requested - rep.unconfirmed}/{rep.requested} 확인, "
            f"{rep.unconfirmed} 미확인")
```

### 6.3 재실행 — replace_jobs + submit

재실행은 별도 API가 아니라 **데이터 조작 + 일반 submit**이다 — 실패/수정 job을 같은
`job_key`로 담은 jobset을 만들어 흡수한 뒤 전체 재제출한다:

```python
def on_rerun_failed(self):
    js = self.mgr.jobset(self._current_jsid)
    failed = [r for r in js.jobs() if r.state.is_failed]
    if not failed or not self.mgr.can_submit(js):
        return                                 # 활성 job 있으면 먼저 kill
    self.mgr.replace_jobs(                     # 같은 job_key → CREATED 교체
        js, [shlex.split(r.command) for r in failed],
        job_keys=[r.job_key for r in failed],
        user_datas=[r.user_data for r in failed])
    self.mgr.submit(js)                        # 전 job 재제출
```

- `replace_jobs`는 **물리 key(job_key)를 유지**하므로 테이블 행이 그대로
  이어진다(§7의 upsert 맵이 안 깨진다).
- 살아있는 job이 남았으면 `mgr.submit`이 거부한다 — `mgr.kill(js)` 후
  `kill_finished`에서 이어가거나 `can_submit`으로 버튼을 비활성화한다.
- 테이블에 보이는 순서: 교체분 **CREATED** → submit 후 전원 **SUBMITTING → PEND**
  (`jobs_updated`만 연결해 두면 자동). 최종은 submit과 동일하게 `submit_finished`.

---

## 7. 테이블 — `job_key` 증분 upsert (대량 필수)

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

## 8. 로그 위젯 도배 방지

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

## 9. 여러 JobSet 합산 — `mgr.total_summary()`

전체 store를 가로질러 상태를 한 번에 집계하는 내장 메서드가 있다.

```python
agg = mgr.total_summary()
# → {"total": 전체 합계, "RUN": .., "PEND": .., "EXIT": .., "DONE": .., ...}
```

- 키는 `JobState.value` 문자열과 `"total"`. 어떤 jobset에도 없는 상태 키는 dict에
  없으므로 `agg.get("RUN", 0)`으로 읽는다.
- `"total"`은 각 jobset 합계의 합이고, 상태 합계 == `"total"` 불변식이 유지된다.
- 조회 중 다른 스레드가 jobset을 remove_jobset으로 지워도 **내부에서 건너뛰고 계속**
  하므로 호출자가 방어할 필요가 없다.
- **Store 스냅샷일 뿐 LSF를 호출하지 않는다** — 값이 최신이려면 각 jobset이 폴링
  되고 있어야 한다.

일부만 합산하려면 `search_jobsets()`로 좁혀 직접 더한다:

```python
from collections import Counter
from lsfmgr.errors import JobSetNotFoundError

agg = Counter()
for js in mgr.search_jobsets(tag="nightly"):
    try:
        agg.update(mgr.summary(js.jobset_id))
    except JobSetNotFoundError:
        continue                        # 조회 사이에 삭제된 jobset은 스킵
```

> 실시간 갱신 화면이라면 매번 합산을 돌리기보다, `jobset_updated(jobset_id, summary)`
> 를 받아 jobset별 최신 summary를 캐시에 두고 그 캐시를 합산하는 편이 정확하다.

---

## 10. 발화 빈도(부하) 조절

모든 progress/`jobs_updated`는 이미 throttle된다(기본 0.5초 OR 1% 진행마다 배치).
더 성기게(부하↓) 하려면 생성 시:

```python
mgr = LsfJobManager(progress_min_interval_s=1.0,    # 기본 0.5
                    progress_min_step_ratio=0.02)   # 기본 0.01
```

`poll_runtime_updates`는 기본 False다 — 켜면 RUN 전원이 매 주기 배치에 실린다(README §6.4).

---

## 11. 종료 처리

`shutdown()`은 `aboutToQuit`/`atexit`에 자동 연결되지만, **명시 호출이 가장 확실**하다:

```python
def closeEvent(self, e):
    self.mgr.shutdown()        # 멱등 — 중복 안전. worker join 후 종료.
    super().closeEvent(e)
```

- 진행 중이던 제출은 완료까지 기다리되(job_id 유실 방지) 아직 제출 안 된 몫은
  취소된다.
- `shutdown` 후의 `submit`/`kill`은 무시된다(no-op 가드).

---

## 12. 흔한 실수 체크리스트

- [ ] **콜백(pre_submit/post_process/handler)에서 위젯을 만졌다** → worker 스레드다.
      값만 반환하고 UI는 signal slot에서.
- [ ] **`QProgressDialog.exec()`로 띄웠다** → `show()`(non-modal)로. 진행은 Signal 구동.
- [ ] **`can_submit` 없이 `submit`했다** → 활성 job이 있으면 예외. 선확인 필수.
- [ ] **취소 버튼이 이미 제출된 job도 멈출 거라 기대** → `cancel_submit`은 미제출분만.
      진행분은 `kill`.
- [ ] **`submit_progress`만으로 완료 판단** → throttled다. 완료는 `submit_finished`.
- [ ] **`jobs_updated`마다 테이블 전체 재그리기** → 변경분만 갱신.
- [ ] **`kill_finished`로 표를 EXIT로 칠했다** → 표는 `jobs_updated`로만.
- [ ] **앱 종료 시 `shutdown()` 누락** → worker join 안 됨, 프로세스 매달림.
- [ ] **핸들(JobSet)로 명령 시도** → 명령은 `mgr.*`만. 핸들은 조회+Signal 전용.
- [ ] **'진행 중 submit' 추적을 카운터로** → 경로에 따라 카운터가 음수로 샌다.
      `jobset_id`를 **집합**에 넣고(`submit_started`/`pre_submit_started`에서 add)
      `submit_finished`에서 discard하라.
