# LsfJobManager GUI 개발 가이드

PyQt5/qtpy GUI 앱에서 `LsfJobManager`로 LSF job을 제출·감시·kill하는 방법과
**반드시 지켜야 할 주의사항**을 정리한다. 진행 표시(`QProgressDialog`/
`QProgressBar`) 연결 패턴을 포함한다.

> 예제 코드는 `examples/gui_demo.py`(전체 위젯 통합), `examples/basic_example.py`,
> `examples/handler_example.py`를 참고하라. 이 문서는 그 패턴의 근거와 함정을 설명한다.

---

## 1. 아키텍처 — 스레딩 모델부터 이해하라

`LsfJobManager`는 **비동기 실행 + Signal 통지** 구조다.

```
[GUI/main 스레드]                     [worker 스레드 (QThreadPool 등)]
  mgr.submit(js)  ──명령 접수(동기)──▶  bsub 실행, 재시도, 폴링, kill...
      │                                        │
      │        ◀── Signal (queued) ────────────┘
      ▼
  slot 실행 (위젯 갱신)
```

- **명령**(`submit`/`kill`/`close`…)은 main 스레드에서 호출하면 즉시 반환하고,
  실제 작업은 worker에서 진행된다.
- **결과·진행**은 전부 **Signal**로 main 스레드에 큐잉되어 도착한다. 그래서
  Signal **slot 안에서는 위젯을 자유롭게 갱신해도 안전**하다.

### ⚠️ 가장 중요한 규칙 두 가지

| 실행 위치 | 안전한가 | 규칙 |
|---|---|---|
| **Signal slot** (`submit_finished`, `jobs_updated`…) | ✅ main 스레드 | 위젯 갱신 OK |
| **콜백** (`pre_submit`, `post_process`, handler fn) | ❌ **worker 스레드** | **위젯·QWidget 접근 절대 금지** |

`pre_submit`/`post_process`/handler 콜백은 worker 스레드에서 실행된다. 여기서
`label.setText(...)` 같은 GUI 호출을 하면 **크래시하거나 정의되지 않은 동작**을
일으킨다. 콜백에서는 순수 계산만 하고, 결과를 **반환**하라 — 그 값은
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

---

## 2. 두 계층의 API — 명령은 manager, 구독은 handle

- **`LsfJobManager`(mgr)**: 모든 **명령**의 유일한 진입점. `create_jobset`,
  `submit`, `kill`, `close`, `merge` 등. Signal도 갖지만 `jobset_id` 인자가
  붙어 있어(전역 리스너용) 여러 jobset을 한 곳에서 처리할 때 쓴다.
- **`JobSet`(js) 핸들**: `mgr.create_jobset(...)`가 반환하거나 `mgr.jobset(id)`로
  얻는다. **조회(pull) + Signal(view) 전용** — 여기에는 명령 메서드가 없다.
  핸들 Signal은 그 jobset 것만 오므로 `jsid` 필터가 필요 없어 위젯 연결에 편하다.

```python
js = mgr.create_jobset(["customwrapper_sub -q normal run_0.sp"], label="sweep")
js.jobs_updated.connect(self._apply_changed)   # 이 jobset 변경분만 도착
```

> **핸들로 명령하지 말 것**: job 추가는 오직 `create_jobset` + `merge`로만.
> 자세한 v9 제어 구조는 요구사항 문서(`docs/LSF_JOB_MANAGER_REQUIREMENTS_v9.md`) 참고.

---

## 3. 기본 흐름

```python
# 1) 생성 — job 생성은 create_jobset 한 곳뿐(v9)
js = mgr.create_jobset(
    ["customwrapper_sub -q normal run_0.sp",
     "customwrapper_sub -q long tb_1.v"],
    merge_ids=["run_0", "tb_1"],       # 논리 키(선택)
    label="sweep")

# 2) 제출 가능 여부 선확인 — 활성 job이 있으면 제출 불가(예외)
if mgr.can_submit(js):
    mgr.submit(js, workers=8, auto_poll=True)   # 비동기 — 즉시 반환

# 3) 결과는 Signal로
js.submit_finished.connect(lambda rep: print(rep.succeeded, rep.failed))
```

- `submit`은 jobset의 **전 job을 (재)제출**한다. 전원 비활성(CREATED/DONE/EXIT/
  SUBMIT_FAILED/LOST)이어야 하며, 활성 job이 하나라도 있으면 `LsfmgrError`.
  **항상 `can_submit(js)`로 선확인**하라.
- 같은 jobset/job_key가 전이되므로 재제출해도 테이블 행이 유지된다(리셋 없음).
- `auto_poll=True`면 제출 후 자동으로 `bjobs` 폴링이 시작되어 상태가 갱신된다.

---

## 4. Signal 카탈로그 (핸들 기준)

| Signal | 인자 | 용도 |
|---|---|---|
| `jobset_updated` | `dict` | 요약 카운트 `{"total":N,"RUN":..,"DONE":..}` — 배지/집계 |
| `jobs_updated` | `list[JobRecord]` | **변경분만** — 테이블 행 갱신 |
| `jobs_failed` | `list[JobRecord]` | SUBMIT_FAILED/EXIT/LOST 변경분 — 실패 알림 |
| `submit_progress` | `(done, total)` | 제출 진행 — **진행 바** (throttled) |
| `submit_finished` | `SubmitReport` | 제출 종료(재시도 포함 최종) |
| `pre_submit_started` / `pre_submit_finished` | `()` / `(ok: bool)` | 게이트 시작/종료 |
| `post_processing_started` / `post_processing_finished` | `()` / `(result)` | 완료 후처리 |
| `kill_started` | `()` | kill **접수 즉시(동기)** — 착수 피드백 |
| `kill_finished` | `KillReport` | kill 종료 |
| `kill_progress` | `(done, total)` | chunk kill 진행 |
| `error_occurred` | `str` | worker 예외 등 |
| `handler_finished` | `(handler_name, HandlerResult)` | 핸들러 완료 |
| `job_detail_ready` | `(job_key, text)` | `fetch_job_detail` 결과 |

manager 계층은 같은 Signal에 `jobset_id`가 앞에 하나 더 붙는다(예:
`mgr.submit_finished(jobset_id, report)`).

---

## 5. QProgressDialog 연결 (제출/kill 진행)

진행은 **Signal 구동(비동기)**이다. 그래서 `QProgressDialog`를 **`exec()`로 띄우면
안 된다** — `exec()`는 자체 모달 루프로 블록해 버려 진행 Signal 처리 흐름과
어긋난다. 반드시 **`show()`(non-modal)** 로 띄우고 Signal이 값을 채우게 하라.

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

    # 진행: submit_progress(done,total) → 바 갱신
    def on_progress(done, total):
        dlg.setMaximum(total)               # total=0이면 busy 인디케이터
        dlg.setValue(done)
    js.submit_progress.connect(on_progress)

    # 완료: 다이얼로그 닫고 연결 해제
    def on_finished(report):
        js.submit_progress.disconnect(on_progress)
        js.submit_finished.disconnect(on_finished)
        dlg.reset(); dlg.close()
        self.status.setText(
            f"제출 완료 — 성공 {report.succeeded} / 실패 {report.failed}")
    js.submit_finished.connect(on_finished)

    # 취소 버튼: 아직 제출 안 된 job만 CREATED로 되돌린다(QT-6).
    #   ⚠️ 이미 bsub된 job은 이걸로 안 멈춘다 — 그건 kill의 영역.
    dlg.canceled.connect(lambda: self.mgr.cancel_submit(js))

    dlg.show()                              # exec() 아님! non-modal
    self.mgr.submit(js, workers=8)
```

### 주의점

1. **`exec()` 금지, `show()` 사용**. 진행은 Signal로 온다 — 모달 루프로 막으면
   안 된다. 앱 전체를 막고 싶지 않으면 `Qt.WindowModal`(부모 창만 모달).
2. **`submit_progress`는 throttled**다. 매 job마다 오지 않을 수 있으니 최종
   상태는 `submit_finished`에서 확정하라. 다이얼로그 닫기는 `submit_finished`에서.
3. **취소 버튼의 의미**: `cancel_submit`은 **아직 제출되지 않은(CREATED 대기)**
   job만 되돌린다. 이미 LSF에 들어간 job을 죽이려면 `mgr.kill(js)`를 써야 한다.
   취소 버튼에 두 동작(미제출 취소 + 진행분 kill)을 모두 걸고 싶다면
   `cancel_submit` 후 `kill`을 함께 호출한다.
4. **연결 해제**: 다이얼로그마다 slot을 새로 연결했다면 완료 시 `disconnect`하라.
   람다를 연결했다면 해제가 어려우니, 위처럼 **이름 있는 함수**로 연결하라.
5. **kill 진행 다이얼로그**도 동일 패턴 — `kill_progress`/`kill_finished`를 쓰고,
   kill은 `kill_started`(접수 즉시 동기)로 먼저 "접수됨"을 표시할 수 있다.

---

## 6. QProgressBar (상시 표시형, 더 단순)

다이얼로그 대신 창에 붙박이 `QProgressBar`를 쓰면 연결/해제가 더 단순하다
(`gui_demo.py`가 이 방식).

```python
self.bar = QProgressBar()
js.submit_progress.connect(self._on_progress)
js.kill_progress.connect(self._on_progress)     # 제출·kill 진행을 한 바로

def _on_progress(self, done, total):
    self.bar.setMaximum(total if total else 0)  # total=0 → busy
    self.bar.setValue(done)
```

---

## 7. 테이블 갱신 — 변경분만

`jobs_updated`는 **바뀐 행만** 담아 온다. 매번 전체를 다시 그리지 말고 변경분만
갱신하라(수만 job에서 성능 차이가 크다).

```python
def _apply_changed(self, records):
    for r in records:
        row = self._rows.get(r.job_key)     # job_key → 행 인덱스 캐시
        if row is None:
            row = self.table.rowCount(); self.table.insertRow(row)
            self._rows[r.job_key] = row
        self.table.item(row, COL_STATE).setText(r.state.name)
```

전체 요약(배지/카운트)은 `jobset_updated`의 `dict`로 갱신한다.

---

## 8. pre_submit 게이트 & post_process

- **`pre_submit(commands) -> bool`**: 제출 **직전** worker에서 명령 리스트 전체를
  검사. `False`/예외면 레코드를 건드리지 않고 제출을 취소한다(입력 검증, 디스크
  용량 확인 등). 게이트 진행은 `pre_submit_started`/`pre_submit_finished(ok)`로.
- **`post_process(records) -> Any`**: 이 제출의 **전 job이 terminal**(DONE/EXIT/
  SUBMIT_FAILED/LOST 무관)에 도달하면 worker에서 1회 실행. 반환값은
  `post_processing_finished(result)`로 전달.

```python
mgr.submit(js,
           pre_submit=lambda cmds: all(c.strip() for c in cmds),   # worker 스레드
           post_process=lambda recs: len([r for r in recs if r.state.name=="DONE"]))
```

> 두 콜백 모두 **worker 스레드**다 — GUI 접근 금지, 값만 반환.

---

## 9. kill / 선택 kill

```python
mgr.kill(js)                                  # jobset 전체 kill
mgr.kill(js, only_state=JobState.PEND)        # PEND만
mgr.kill_jobs(js, [job_key1, job_key2])       # 선택 행만(테이블 선택)
mgr.kill(js, verify=True)                     # bkill 후 재조회로 실제 종료 확인
```

- `kill_started`(동기)로 즉시 "접수됨"을 표시하고, 완료는 `kill_finished`의
  `KillReport`(요청 수, 오류, verify=True면 `still_alive`)로 받는다.
- 진행 중 submit이 있으면 kill이 **우선권**을 가진다(제출을 멈추고 kill).

---

## 10. 종료 처리 — `shutdown()`은 필수

앱을 닫을 때 **반드시 `mgr.shutdown()`** 을 호출하라. 안 하면 worker 스레드가
join되지 않아 프로세스가 매달리거나 종료 중 경합이 난다.

```python
class MainWindow(QMainWindow):
    def closeEvent(self, event):
        self.mgr.shutdown()      # worker join, 폴링/제출/kill 정리
        super().closeEvent(event)
```

- `shutdown` 후의 `submit`/`kill`은 무시되거나 예외다(no-op 가드).
- 진행 중인 제출/kill을 강제로 정리하려면 `close(js, force=True)`도 있다.

---

## 11. 흔한 실수 체크리스트

- [ ] **콜백(pre_submit/post_process/handler)에서 위젯을 만졌다** → worker 스레드다.
      값만 반환하고 UI는 signal slot에서.
- [ ] **`QProgressDialog.exec()`로 띄웠다** → `show()`(non-modal)로. 진행은 Signal 구동.
- [ ] **`can_submit` 없이 `submit`했다** → 활성 job이 있으면 예외. 선확인 필수.
- [ ] **취소 버튼이 이미 제출된 job도 멈출 거라 기대** → `cancel_submit`은 미제출분만.
      진행분은 `kill`.
- [ ] **`submit_progress`만으로 완료 판단** → throttled다. 완료는 `submit_finished`.
- [ ] **`jobs_updated`마다 테이블 전체 재그리기** → 변경분만 갱신.
- [ ] **앱 종료 시 `shutdown()` 누락** → worker join 안 됨, 프로세스 매달림.
- [ ] **핸들(JobSet)로 명령 시도** → 명령은 `mgr.*`만. 핸들은 조회+Signal 전용.
- [ ] **'진행 중 submit' 추적을 카운터로** → 경로에 따라 카운터가 음수로 샌다.
      `jobset_id`를 **집합**에 넣고(`submit_started`/`pre_submit_started`에서 add)
      `submit_finished`에서 discard하라.
```
