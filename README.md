# lsfmgr — LSF Job Manager for Qt Applications

대량 LSF job의 **submit / monitoring / kill / 묶음(JobSet) 관리** 라이브러리.
`qtpy` 기반으로 **PyQt5 / PySide2 / PyQt6 / PySide6** 어디서든 동일하게
동작하며, 모든 LSF 호출은 백그라운드 스레드에서 실행되고 결과는 Signal로
통지되므로 **GUI가 freeze되지 않습니다**.

```
의존성: qtpy + Qt 바인딩 1종 (그 외 stdlib only)    Python: 3.9+
```

---

## 1. Quick Start — 3줄이면 끝

```python
from lsfmgr import LsfJobManager

mgr = LsfJobManager()
js = mgr.create_jobset([f"mytool run_{i}.sp" for i in range(5000)], label="sweep")
mgr.submit(js)                                              # jobset 기준 제출
js.jobset_updated.connect(lambda s: print(f"RUN={s['RUN']} DONE={s['DONE']}/{s['total']}"))
```

이것만으로:
- 5,000개 job이 병렬 submit되고 (worker 32, 실패 시 3회 재시도)
- polling이 자동 시작되어 (10초 주기) 요약이 `js.jobset_updated`로 도착하고
- 전부 끝나면 polling도 자동 중지됩니다
- 앱 종료 시 스레드 정리(`shutdown`)도 자동입니다

> **API 계약**: 제어 API(submit/kill/refresh)는 전부 **즉시 반환(비동기)**,
> 결과는 Signal로 도착합니다. 조회 프로퍼티(summary/jobs)는 **동기**지만
> 로컬 스냅샷만 읽으므로 ms 단위입니다 (LSF 호출 없음). GUI가 멈추는
> public API는 없습니다.

---

## 2. 옵션 — 안 주면 기본값, 주면 그 호출에만

모든 튜닝 파라미터는 3단 계층으로 동작합니다:

```
내장 기본값  <  LsfJobManager(...) 생성 인자 (앱 전역)  <  submit(...) 인자 (이번 호출만)
```

```python
# 기본값만
mgr = LsfJobManager()

# 앱 전역 기본값 변경
mgr = LsfJobManager(workers=32, max_retry=5, poll_interval_s=5,
                    default_queue="priority")

# 이번 submit만 다르게
mgr.submit(js, workers=8, max_retry=0, queue="short", auto_poll=False)
```

### 옵션 카탈로그

| 옵션 | 기본값 | 지정 위치 | 설명 |
|---|---|---|---|
| `workers` | 32 | 생성자, submit | 병렬 submit worker 수 (1~64) |
| `max_retry` | 3 | 생성자, submit | submit 실패 재시도 (0=끔) |
| `retry_backoff` | `"fixed:2"` | 생성자, submit | `"fixed:N"`(N초 고정) / `"expo:N"`(지수) |
| `rate_limit_per_s` | 없음 | 생성자, submit | 초당 bsub 상한 (LSF 부하 보호) |
| `poll_interval_s` | 10 | 생성자, submit | polling 주기 (5~60) |
| `poll_runtime_updates` | True | 생성자 | RUN 중 `run_time_s`(경과시간) 변화도 `jobs_updated`로 live 발행. 수만 개 규모 부하 시 False로 끔 |
| `submit_finished_on_gate_reject` | True | 생성자 | `pre_submit` 게이트가 False면 `submit_finished`(cancelled=N)도 발화. False면 종료는 `pre_submit_finished(False)`만 |
| `collect_clusters` | False | 생성자 | LSF MultiCluster forwarding 정보 수집. 켜면 `JobRecord.source_cluster`/`forward_cluster`를 폴링으로 채움(MC 환경 opt-in) |
| `progress_min_interval_s` | 0.5 | 생성자 | progress/jobs_updated 최소 발화 간격(초). 키우면 부하↓·반응성↓ |
| `progress_min_step_ratio` | 0.01 | 생성자 | progress 최소 진행 비율(0~1). 키우면 발화↓ |
| `min_state_dwell_s` | 0 (끔) | 생성자 | 상태 전이 **표시** 최소 간격(초). 켜면 job별로 한 상태가 이만큼 표에 머문 뒤 다음 전이가 `jobs_updated`로 나간다 — 순식간에 지나가는 `SUBMITTING`→`PEND`, `EXIT`→`SUBMITTING`을 눈에 보이게 함 (§5.4) |
| `auto_poll` | True | 생성자, submit | submit 후 polling 자동 시작 |
| `queue` | LSF 기본 | 생성자(`default_queue`), submit | 대상 queue |
| `resource_req` | 없음 | 생성자, submit | `-R` 문자열 |
| `output_dir` | 없음 | 생성자, submit | `-o`/`-e` 경로 규칙 |
| `submit_timeout_s` | 30 | 생성자, submit | bsub 1건 timeout |
| `verify_kill` | False | 생성자, kill | kill 후 실제 종료 확인 |
| `kill_status_policy` | `"optimistic"` | 생성자 | `"optimistic"`=terminated 확인 시 즉시 EXIT / `"actual"`=실제 LSF 상태(폴링)로만 |
| `kill_max_retry` | 2 | 생성자 | kill 확인 실패 시 재시도 횟수 |
| `label` / `tags` / `description` | 빈 값 | submit | JobSet 메타데이터 |
| `chunk_size` | 200 | 생성자 | chunking fallback 크기 |
| `bsub_path` 등 | PATH 탐색 | 생성자 | LSF 명령 경로 (문자열 또는 wrapper 토큰 목록) |

- 오타 키워드는 즉시 `TypeError`, 범위 벗어나면 `ValueError` — 조용히
  무시되지 않습니다.
- 옵션이 많은 설정을 파일/객체로 관리하고 싶으면 `LsfConfig`를 만들어
  `LsfJobManager(config=cfg)`로 주입할 수 있습니다 (kwargs가 우선).

### 2.1 wrapper 커맨드로 제출 (예: `customwrapper_sub`)

실제 환경처럼 job 마다 `customwrapper_sub` 같은 제출 wrapper(job마다 다른 wrapper/커맨드 혼합 가능)를
쓰는 경우, `create_jobset`에 wrapper 커맨드 리스트를 그대로 넘깁니다(기본
`wrapper=True`). lsfmgr는 각 커맨드를 **그대로 실행**하고 출력의 `Job <id>`를
파싱해 **job_id 기반**으로 모니터링·kill 합니다(‑q/‑J/‑g 등 인자 조립·주입 없음).

```python
mgr = LsfJobManager()          # bsub_path 지정 불필요

js = mgr.create_jobset([
    "customwrapper_sub -q normal run_0.sp",         # job 마다 다른 wrapper 가능
    ["customwrapper_sub", "-q", "long", "tb_1.v"],   # 문자열 또는 토큰 리스트
    "customwrapper_sub -q short run_2.sp",
])
mgr.submit(js, workers=8, max_retry=3)
```

- wrapper는 결국 `bsub`를 호출하고 그 `Job <id>` 출력·exit code를 그대로 통과시키면
  됩니다. 재시도는 **비정상 종료(non-zero)만** 대상입니다.
- 모니터링·kill용 `bjobs`/`bkill`은 실제 LSF면 PATH, mocklsf면 경로를 지정합니다.

#### 제출 전 전처리 게이트 (`pre_submit`)

실제 제출 전에 **커맨드 리스트 전체를 한 번 검사/준비**하고 통과할 때만
제출을 진행하려면 `mgr.submit(js, pre_submit=...)` 콜백을 넘깁니다.
콜백은 **단일 worker 스레드**에서 1회 실행되고 `bool`을 반환합니다.

```python
def prepare(commands: list[str]) -> bool:      # 실행될 커맨드 문자열 목록
    stage_input_files(commands)                # 일괄 준비 (부수효과)
    return all_inputs_ready(commands)          # True면 제출, False면 제출 안 함

mgr.submit(js, pre_submit=prepare)
```

신호 순서는 **`pre_submit_started` → `pre_submit_finished(ok)` → (ok=True일 때만)
`submit_started` → … → `submit_finished`**. 게이트가 `False`면 제출하지 않고
job은 `CREATED`로 남습니다(기본은 `submit_finished(cancelled=N)`도 발화 —
`LsfJobManager(submit_finished_on_gate_reject=False)`로 끄면 종료 통지는
`pre_submit_finished(False)`만). 콜백이 **예외**를 던져도 제출하지 않으며 —
게이트는 레코드 리셋 **이전**에 돌므로 레코드는 **원상 유지**됩니다 —
`error_occurred` + `submit_finished(failed=N)`로 보고합니다.

> ⚠️ 콜백은 **worker 스레드**에서 돕니다 — Qt 위젯 등 **GUI 객체 접근 금지**.
> 재시도 시 재실행되므로 부수효과는 **멱등**이어야 합니다.

#### 완료 후처리 (`post_process`)

제출한 jobset의 **전 job이 끝나면(전원 terminal)** 결과 수집·정리 등을 자동
실행하려면 `mgr.submit(js, post_process=...)` 콜백을 넘깁니다. 완료는 폴링
(`auto_poll` 기본) 또는 `mgr.query_once(js)`로 감지되며, 감지 시점에 **단일
worker 스레드**에서 1회 실행됩니다. 성공/실패가 섞여도(EXIT/SUBMIT_FAILED/LOST
포함) **전원 terminal이면** 실행되므로, 콜백에서 결과를 분류하면 됩니다.

```python
def collect(records) -> dict:                  # 최종 JobRecord 목록
    done = [r for r in records if r.state.name == "DONE"]
    return {"ok": len(done), "failed": len(records) - len(done)}

mgr.submit(js, post_process=collect)           # pre_submit과 함께 써도 됨
```

신호 순서는 **`post_processing_started` → `post_processing_finished(result)`**
(`result`는 콜백 반환값, 예외 시 `None`). 콜백이 **예외**를 던지면
`error_occurred` + `post_processing_finished(None)`로 보고합니다. `pre_submit`
게이트와 대칭입니다(전자는 제출 **전**, 후자는 완료 **후**).

> ⚠️ 이 콜백도 **worker 스레드** 실행 — GUI 객체 접근 금지. 한 제출당 1회만
> 발화하며, 완료 전 재제출(`post_process` 없이)하면 이전 무장은 해제됩니다.

> 작성 규칙·실행 방식(멀티 프로세스)·검증·트러블슈팅, 그리고 lsfmgr가 직접 bsub를
> 조립하는 저수준 경로(`create_jobset(..., wrapper=False)`+`bsub_path`)는
> **[`docs/lsfmgr.md`](docs/lsfmgr.md)**
> 에 정리되어 있습니다.

---

## 3. JobSet — 모든 것의 중심

`create_jobset()`이 반환하는 **JobSet 핸들** 하나로 해당 묶음의
모니터링/조회를 전부 합니다 (명령은 `mgr.*`에 이 핸들을 넘깁니다).

### 3.0 v9 기본 흐름 — jobset 선생성 + GUI 직접 제어

GUI는 제출 전(CREATE 단계)부터 jobset을 갖습니다 — `create_jobset`으로
빈 jobset(핸들)을 먼저 만들고, job을 누적한 뒤, jobset 단위로 제출합니다.
같은 jobset/job_key가 전이되므로 **핸들 교체·테이블 리셋이 없습니다**.

**명령은 전부 manager 한 곳입니다** — `mgr.submit(js)` / `mgr.kill(js)` /
`mgr.merge(a, b)` 처럼 핸들을 인자로 넘깁니다. 핸들(JobSet)은 **조회
(`jobs()`/`summary`/`is_*`)와 Signal 전용 뷰**라 명령 API가 두 군데로
갈라지지 않습니다.

```python
js = mgr.create_jobset(                        # 생성 시 job까지 함께 만든다
    ["customwrapper_sub -i a.sp",              #   각 커맨드 = job 1건 (CREATED)
     "customwrapper_sub -i b.sp"],
    merge_ids=["case-a", "case-b"],           # 논리 키 (merge 시 replace 기준)
    user_datas=[{"run": "...", "rev": 3}, None],  # job별 사용자 데이터 (보존만)
    label="sweep")
js.jobs_updated.connect(table.apply_changed)  # GUI 테이블(앱 코드)을 이 핸들의
                                              # Signal에 연결 (초기값은 js.jobs())

if mgr.can_submit(js):                     # 전원 비활성 + job 존재?
    mgr.submit(js, workers=8)              # **전 job** (재)제출 — 이전
                                           # DONE/EXIT도 리셋 후 재실행
```

> **job 생성은 `create_jobset` 한 곳뿐**입니다 (v9). 생성 후 job을 더 넣는
> 유일한 방법은 **merge** — 별도 jobset을 만들어 `mgr.merge(js, src)`로
> 흡수합니다 (아래 재실행 패턴).

**재실행 패턴** (resubmit API 없음): 실패/수정 job을 같은 `merge_id`로 담은
새 jobset을 만들어 흡수 → 다시 submit:

```python
fix = mgr.create_jobset(["customwrapper_sub -i a_fixed.sp"],
                        merge_ids=["case-a"], label="fix")
if mgr.can_merge(js, fix):
    mgr.merge(js, fix)         # case-a만 CREATED로 교체 (다른 결과 유지,
                               # 물리 키 유지 — 테이블 행 연속), fix 소멸
mgr.submit(js)                 # 전체 재실행
```

**규칙 요약** — 전부 "비활성(CREATED/DONE/EXIT/SUBMIT_FAILED/LOST)" 기준:

| 명령 | 가드 | force |
|---|---|---|
| `mgr.submit(js)` | 전 job 비활성 + 1건 이상 (`can_submit`) | — (활성은 먼저 kill) |
| `mgr.merge(js, src)` | 양쪽 전 job 비활성 (`can_merge`) | 레코드만 강제 교체 (LSF 정리는 앱 책임) |
| `mgr.remove_job(js, ...)` / `mgr.clear(js)` | 대상 비활성 | 레코드만 강제 삭제 (〃) |
| `mgr.kill(js)` | 예외 — 활성(RUN/PEND/SUBMITTING)만 대상, 종료분은 자동 skip | — |

> 가드 위반 시 명령별 **전용 예외**가 납니다 — `SubmitNotAllowedError` /
> `MergeNotAllowedError` / `RemoveNotAllowedError` / `CloseNotAllowedError`
> (전부 `JobSetStateError` → `LsfmgrError` 하위라 `except LsfmgrError` 로도 잡힘).
> 예외 객체의 `.jobset_id` · `.job_keys`(막은 job들)로 메시지 파싱 없이 원인을
> 알 수 있습니다. 사전 확인은 `mgr.can_submit(js)` / `mgr.can_merge(a, b)`.

### 3.1 Signal (이 JobSet의 이벤트만 옴 — 필터링 불필요)

이름은 `mgr.*` Signal과 동일하다(인자에서 `jsid`만 빠짐). 여러 JobSet을 한 곳에서
보면 `mgr.*`(jsid 포함)를, 단일 JobSet 위젯이면 아래 `js.*`를 쓴다.

| Signal | 인자 | 시점 |
|---|---|---|
| `jobset_updated` | `dict` 요약 | **submit 완료 시(초기 PEND)** + polling/refresh 후 |
| `submit_progress` | `(done, total)` | submit 진행 (throttled) |
| `submit_finished` | `SubmitReport` | submit 완료 (retry 포함 최종) |
| `jobs_failed` | `list[JobRecord]` | SUBMIT_FAILED/EXIT/LOST 변경분 — `rec.fail_message`에 실패 진단 원문 |
| `kill_started` | — | kill 접수 즉시(동기) 착수 통지 — 정지 대기로 완료가 늦어져도 UI가 바로 표시 |
| `kill_progress` | `(done, total)` | 대량 chunk kill 진행 (throttled, 마지막 100%) |
| `kill_finished` | `KillReport` | kill 완료 |
| `handler_finished` | `(name, HandlerResult)` | 등록한 handler 1회 실행 완료마다 (§3.5) |
| `post_processing_started` | — | `post_process` 지정 시 전원 terminal 후처리 착수 |
| `post_processing_finished` | `object` | 후처리 콜백 완료 (반환값, 예외 시 `None`) |
| `error_occurred` | `str` | worker 예외 등 |

요약 dict 예:
```python
{"total": 5000, "RUN": 2100, "PEND": 2800, "DONE": 80, "EXIT": 12,
 "SUBMIT_FAILED": 5, "RETRY_WAIT": 2, "LOST": 1}
# 불변식: 합계 == total (손실 job도 반드시 어딘가에 집계됨)
```

### 3.2 제어 (비동기 — 즉시 반환, 결과는 Signal)

```python
mgr.kill(js)                           # 전체 kill (명령 1회, ARG_MAX 안전)
mgr.kill(js, only_state=JobState.PEND) # PEND만
mgr.kill(js, verify=True)              # 실제 종료까지 확인
mgr.kill_jobs(js, [job_key, ...])      # 선택 job만 kill (테이블 선택 행)
mgr.kill_jobs(js, keys, envpath="/lsf/busan/conf/cshrc.lsf")  # MC forward — env source 후 bkill
mgr.cancel_submit(js)                  # 진행 중 submit 중단 (된 것은 유지)
mgr.query_once(js)                     # 지금 즉시 1회 조회 요청
mgr.stop_polling(js); mgr.start_polling(js, 30)
mgr.submit(js, workers=8)              # 전 job (재)제출 — can_submit로 선확인
mgr.close(js)                          # 종결 (전원 terminal일 때)
```

> **kill 상태 정책** (`LsfJobManager(kill_status_policy=...)`):
> `bkill`은 비동기라 `Job <id> is being terminated`(요청 수락)와 실제 종료 사이에
> 시차가 있습니다.
> - **`"optimistic"`(기본)** — terminated 확인 시 **즉시 EXIT로 간주**하고
>   `jobs_updated`/`jobset_updated`로 바로 반영. 이후 폴링은 이 job을 조회하지
>   않습니다(EXIT는 terminal). `KillReport.changed`에 전이된 job이 담깁니다.
> - **`"actual"`** — terminated만으론 상태를 안 바꾸고, **실제 LSF 상태**
>   (`verify=True` 또는 폴링)로만 EXIT를 반영. 정확하지만 반영이 한 박자 늦습니다.

> **MultiCluster forward job kill** (`envpath`): job이 다른 클러스터로
> forward되어 로컬 `bkill`로 안 죽고 그 클러스터 LSF env를 source해야 죽는
> 환경을 지원합니다. `mgr.kill(js)`/`mgr.kill_jobs(js, keys)`에 `envpath="<cshrc 경로>"`를
> 주면 `tcsh -c "source <envpath> && exec bkill <ids>"` 로 실행됩니다.
> job마다 forward된 클러스터가 다를 수 있으므로, `collect_clusters=True`로
> 채워지는 `rec.forward_cluster`로 분류한 뒤 클러스터별로 나눠 각 `envpath`로
> 호출하면 됩니다. **`forward_cluster`가 곧 판별자**입니다 — forward된 job만
> 클러스터 이름이 들어가고, **forward 안 된 로컬 job은 `None`**이라 envpath
> 없이 그냥 로컬 `bkill`로 죽여야 합니다 (`None` 버킷 별도 처리):
> ```python
> by_cluster = {}
> for r in js.failed_jobs:                       # 또는 죽일 대상 목록
>     by_cluster.setdefault(r.forward_cluster, []).append(r.job_key)
> for cluster, keys in by_cluster.items():
>     if cluster is None:                        # forward 안 된 로컬 job
>         mgr.kill_jobs(js, keys)                # envpath 없이 (로컬 bkill)
>     else:                                      # forward된 job
>         mgr.kill_jobs(js, keys, envpath=CSHRC[cluster])
> ```
> `None`을 안 걸러내면 `CSHRC[None]`에서 `KeyError`가 나거나 로컬 job에
> 불필요하게 envpath를 씌우게 됩니다. (`source_cluster`는 LSF 버전에 따라 로컬
> job도 자기 클러스터명이 나올 수 있어 판별 기준으로는 부적합 — `forward_cluster`
> 로 판별하세요.)

### 3.3 조회 (동기 — 로컬 스냅샷, LSF 호출 없음)

```python
js.summary                 # 요약 dict
js.is_done                 # 전원 terminal?
js.is_active               # 하나라도 안 끝난(non-terminal) job이 있으면 True
js.is_inactive             # 전원 terminal(DONE/EXIT/SUBMIT_FAILED/LOST)이면 True
js.failed_jobs             # SUBMIT_FAILED/EXIT/LOST 목록
js.jobs()                  # 전체 JobRecord
js.jobs(states={JobState.RUN})
mgr.detect_lost(js)        # 손실 감지 (name 패턴 복구 시도 포함)
js.id                      # jobset_id 문자열 (로그/저장용)
```

> **`is_active` / `is_inactive`** — 이 JobSet을 다시 수행할지 판단할 때 씁니다.
> `inactive`는 **모든 job이 terminal**(DONE·EXIT·SUBMIT_FAILED·LOST)이라 더
> 진행할 게 없는 상태이고, `is_active = not is_inactive`(하나라도 CREATED/
> SUBMITTING/RETRY_WAIT/PEND/RUN/suspend 등 non-terminal). `is_done`과 거의
> 같지만 job이 하나도 없는 빈 JobSet은 `is_inactive=True`(is_done은 False)로
> 다릅니다.
> ```python
> if mgr.can_submit(js):    # 진행 중인 것 없음 → 전체 재수행 가능
>     mgr.submit(js)
> ```

> **대량 제출을 백그라운드로 돌리기** (`is_submitting` / `submit_state`) —
> `submit()`은 **즉시 반환**합니다. 실제 `customwrapper_sub`→`bsub`→JOBID 캡처는
> 전부 worker 스레드에서 돌아 **GUI를 막지 않습니다**. 그래서 1000개+ 제출이
> 오래 걸려도 진행 dialog를 **modeless로 띄우거나 아예 닫고 딴 작업**을 해도
> 됩니다 — 제출의 소유자는 매니저지 dialog가 아니라서, `js` 핸들만 들고
> 있으면 계속 진행됩니다. 진행 dialog(`QProgressDialog`)는 `submit_progress`
> Signal에 연결하되 **`setModal(False)`** 로 두고, 사용자가 닫아도 제출은
> 계속 두면 됩니다. 나중에 상태 패널을 다시 열 때는 그동안 놓친 Signal 대신
> **pull로 현재 진행을 조회**합니다:
> ```python
> mgr.submit(js)                     # 즉시 반환 — 아래는 언제든 호출 가능
> if js.is_submitting:               # 아직 제출 중?
>     s = js.submit_state            # SubmitProgress | None
>     bar.setValue(int(s.fraction * 100))          # done/total 진행률
>     label.setText(f"{s.done}/{s.total} "
>                   f"(성공 {s.succeeded} / 실패 {s.failed})")
> ```
> `submit_state`는 진행 중이 아니면 `None`이고, 완료 후 최종 결과는
> `submit_finished(SubmitReport)` 또는 `js.summary`로 봅니다.
> `is_submitting`은 제출이 도는 동안 True입니다(`jobs`의 PEND/RUN이
> 아니라 **제출 작업 자체**의 진행 여부).
> **kill도 대칭**입니다 — 대량 chunked kill(특히 MC `envpath`는 chunk마다 env
> source, `verify`는 재조회 루프)도 오래 걸릴 수 있어 `js.is_killing` /
> `js.kill_state`(`KillProgress(done/total)` + `.remaining`/`.fraction`)로 같은
> 방식으로 조회합니다. 완료 후 최종은 `kill_finished(KillReport)`.
> > 앱을 닫으면(`shutdown`) 진행 중이던 bsub는 완료까지 기다리되 아직 제출
> > 안 된 몫은 취소됩니다. 앱 재시작 후에도 이어서 추적하려면

> **실패 원인 표시** — 두 경로로 확인합니다.
> - **SUBMIT_FAILED/RETRY_WAIT**: `rec.fail_message`에 bsub/wrapper 실행의
>   stderr/stdout(터미널에서 봤을 메시지)이 자동 저장됩니다. 재시도 성공/
>   재제출(mgr.submit(js)) 시 자동으로 지워집니다.
> - **EXIT**: 자동 수집하지 않습니다(폴링 오버헤드 0). 상태 셀 클릭 등
>   필요한 시점에 `mgr.fetch_job_detail(js, job_key)`를 호출하면 `bhist -l`
>   원문이 `js.job_detail_ready(job_key, text)` Signal로 옵니다(bhist는
>   worker 스레드 — GUI 안 멎음). 동기 버전은 `mgr.job_detail(js, job_key)`.
>   제출 실패 job에 호출하면 저장된 fail_message를 돌려주므로, 클릭 핸들러
>   하나로 모든 실패 상태를 처리할 수 있습니다.

> 조회 값은 **마지막 polling 시점 스냅샷**입니다 (최대 `poll_interval_s`
> 지연). 단 `SUBMIT_FAILED`는 submit 과정에서 직접 기록되므로 항상 정확합니다.
> 지금 즉시 최신이 필요하면 `mgr.query_once(js)` 후 `jobset_updated` Signal에서 읽으세요.

### 3.4 그 외

```python
mgr.merge(js_a, js_b)                  # b를 a에 흡수(merge_id 규칙) — b 소멸
mgr.can_merge(js_a, js_b)              # 흡수 가능 여부 (전원 비활성)
js2 = mgr.jobset(jobset_id)            # ID로 JobSet 재획득
mgr.remove_job(js, merge_id="m1")      # 삭제 — job_id/merge_id/job_key 기준
mgr.remove_job(js, job_id=12345, force=True)  # 활성이면 force 필요 (레코드만)
mgr.clear(js)                          # 전 job 삭제 (동일 가드)
mgr.set_user_data(js, "m1", {"note": "..."})  # 사용자 데이터 교체
```

### 3.5 job별 handler — 폴링 사이클마다 실행 (파싱 + 최종 수집)

JobSet에 **이름 있는 handler**를 붙이면, 각 job이 지정한 state 구간에 있는
동안 **폴링 사이클마다**(= bjobs 갱신 직후) **worker 스레드에서** 실행됩니다.
별도 주기가 없어 `poll_interval_s`에 tie되고, `ctx.record`는 항상 최신 상태입니다.

```python
def collect(ctx):                          # worker 스레드 — GUI 안 막음
    # ctx.job_id / ctx.working_dir(LSF exec_cwd) / ctx.record / ctx.final
    return parse_outputs(ctx.working_dir)  # 반환값이 Signal로 전달됨

js.handler_finished.connect(
    lambda name, res: print(name, res.job_key, res.data, res.final))

mgr.add_handler(js, "collect", collect,
                start_states={JobState.RUN},            # RUN이 되면 시작 (기본)
                end_states={JobState.DONE, JobState.EXIT})  # 종료 시 최종 1회 (기본)
# start/end 미지정 시 기본값 = 시작 {RUN}, 종료 {DONE, EXIT}
mgr.remove_handler(js, "collect")          # 해제
```

- `handler_finished`는 **1회 실행이 끝날 때마다** job별로 옵니다 — 최종 실행은
  `res.final`로 구분. 예외는 `res.error`에 담겨 옵니다(다른 job에 영향 없음).
- **폴링이 돌고 있어야 동작**합니다(auto_poll 기본이면 자동). 첫 실행은 다음 폴링
  사이클이며, `mgr.query_once(js)`로 즉시 1회 유도 가능합니다.
- `mgr.submit(js)`로 전체 재실행하면 진행 상태가 자동 재무장되어 새 실행에서 다시 돕니다.
- 실행 예제: `examples/handler_example.py`, 상세 규칙:
  [`docs/lsfmgr.md`](docs/lsfmgr.md) §2.5.

---

## 4. 사용 예제

### 4.1 진행률 + 완료 처리

```python
js = mgr.create_jobset(cmds, label="tt_sweep", tags=["sweep", "rev2"])
mgr.submit(js)
js.submit_progress.connect(lambda d, t: bar.setValue(int(d / t * 100)))
js.submit_finished.connect(lambda rpt: statusbar.showMessage(
    f"submitted {rpt.ok}/{rpt.total} (failed {rpt.failed})"))
js.jobs_failed.connect(lambda recs: table.append_failures(recs))
```

### 4.2 완료 대기 후 후속 작업

```python
def on_update(summary):
    if js.is_done:
        launch_post_processing(js.jobs(states={JobState.DONE}))
js.jobset_updated.connect(on_update)
```

---

## 5. GUI 통합 규칙

1. **slot은 main 스레드에서 실행** — Signal은 자동 queued connection이므로
   slot에서 바로 위젯 갱신 OK.
2. **Signal로 받은 객체는 불변(frozen)** — 수정하지 말고 JobSet API를 쓰세요.
3. **shutdown은 자동** — `QApplication.aboutToQuit`에 자동 연결됩니다.
   명시적으로 부르고 싶으면 `mgr.shutdown()` (멱등, 중복 안전).
4. **대량 갱신은 batch** — `jobs_failed`/`jobset_updated`는 변경분/요약 단위로 오므로
   모델 뷰에 배치 반영하세요.
5. 바인딩 강제: `QT_API=pyside6` (pyqt5/pyside2/pyqt6/pyside6) 환경변수를
   Qt import 전에 설정. 미설정 시 앱이 import한 바인딩 자동 감지.

### 5.1 명령 → Signal 타임라인 (무엇이 언제 오나)

사용자 명령에 대한 신호는 아래 순서가 **보장**됩니다. 라이브러리가 store를
먼저 갱신한 뒤 신호를 쏘므로(store-first), 어떤 slot에서든 `js.jobs()`를
pull하면 신호 내용과 일치하는 상태를 봅니다.

```
submit (mgr.submit(js) — 재제출 포함):
  (pre_submit_started → pre_submit_finished)          # pre_submit 게이트 지정 시
  → submit_started                          # 제출 착수
  → jobs_updated([전원 SUBMITTING])          # 표가 즉시 채워짐
  → submit_progress + jobs_updated(변경분)    # 스로틀 배치 (0.5s 또는 1%)
  → submit_finished(SubmitReport)           # 반드시 마지막 배치 뒤에 도착
  → jobset_updated(최종 요약)
  ⋯ (이후 폴링으로 전원 terminal 도달 시, post_process 지정했으면)
  → post_processing_started → post_processing_finished(result)

kill:
  → kill_started                            # 접수 즉시(동기) — 스피너 켜는 지점
  → jobs_updated([CREATED 복귀 배치])        # 진행 중 submit이 있었으면 (취소분)
  → kill_progress                           # 대량 chunk kill일 때 (스로틀)
  → kill_finished(KillReport)               # 완료
  → jobs_updated([EXIT 전원 배치])           # 기본(optimistic) — 폴링 안 기다림
  → jobset_updated(요약)

polling(자동):
  → jobset_updated(요약) + jobs_updated(변경분만)   # jobset당 주기당 1회
  → job_lost                                 # LOST 확정 시에만
```

> `min_state_dwell_s`(§5.4)를 켜면 **`jobs_updated`만** 이 타임라인에서 최대
> dwell만큼 뒤로 밀립니다 — 그 신호에 한해 store-first/finished-last가
> 느슨해집니다. 기본값(0)이면 위 순서 그대로입니다.

> 명령별 내부 동작(스레드·상태 전이·kill 우선권 barrier)의 상세 도식은
> [docs/flows.md](docs/flows.md), 실행 가능한 최소 데모는
> [examples/gui_demo.py](examples/gui_demo.py) 참고.

### 5.2 연결 패턴 — 위젯별 권장 신호

| 위젯 | 연결할 Signal | 이유 |
|---|---|---|
| 요약 배지/카운터 | `jobset_updated(summary)` | dict 하나로 전 상태 카운트 — 표 순회 불필요 |
| job 테이블 | `jobs_updated([JobRecord])` | **변경분만 옴** — 전체 리로드 말고 해당 행만 갱신 |
| 진행 바 | `submit_progress` / `kill_progress` | 이미 스로틀됨 — 그대로 바인딩 |
| "실행 중" 스피너 | `kill_started`·`submit_started` 켜고 `*_finished` 끄기 | kill은 정지 대기로 완료가 늦을 수 있어 착수 신호가 따로 있음 |
| 실패 알림 | `jobs_failed` / `job_lost` / `error_occurred` | 실패 계열만 구독 |

```python
js = mgr.create_jobset(label="sweep")
js.jobs_updated.connect(table.apply_changed)     # 변경 행만 반영
js.jobset_updated.connect(badge.set_counts)
js.kill_started.connect(lambda: spinner.start("killing..."))
js.kill_finished.connect(lambda rep: spinner.stop())
```

### 5.3 성능 — 신호가 GUI를 버벅이게 하지 않으려면

- **빈도는 라이브러리가 제한**합니다: progress·변경분 배치는 0.5초 간격
  또는 진행률 1% 변화 시에만 발화(마지막 100%는 항상). 10,000개 submit도
  초당 최대 ~2회 배치입니다.
- **`jobs_updated`는 전체가 아니라 변경분**입니다 — 테이블 전체 리셋 대신
  `rec.job_key`로 해당 행만 갱신하세요 (`QAbstractTableModel`이면 해당 행
  `dataChanged`만).
- **RUN 수천 개 이상을 다루면 `poll_runtime_updates=False`** 권장 —
  기본값(True)은 실행 경과시간을 매 폴링 갱신하므로 RUN 전원이 매 주기
  변경분 배치에 실립니다. 끄면 상태 전이 시점에만 반영됩니다.
- `kill_status_policy`는 기본(`"optimistic"`)을 유지하세요 — kill 확인 즉시
  EXIT가 반영됩니다. `"actual"`로 바꾸면 다음 폴링(기본 10초)까지
  PEND/RUN으로 보입니다.

### 5.4 상태 전환을 눈에 보이게 — `min_state_dwell_s`

`SUBMITTING`→`PEND`는 bsub 왕복(수백 ms)만큼만, 재제출의 `EXIT`→`SUBMITTING`은
거의 0초 만에 지나갑니다. 표에서는 중간 상태가 깜빡이고 최종 상태만 남죠.
`min_state_dwell_s`를 켜면 job별로 한 상태가 그 시간만큼 머문 뒤에야 다음
전이가 `jobs_updated`로 나갑니다. **전이는 버리지 않고 순서대로** 밀립니다:

```python
mgr = LsfJobManager(min_state_dwell_s=1.0)   # 0(기본)이면 끔 — 종전과 동일
```

```
mgr.kill(js) → mgr.submit(js)     # dwell=1.0
  표: EXIT ──1s──> SUBMITTING ──1s──> PEND       # 각 상태가 1초씩 보인다
```

**표시만** 늦추는 기능이라, 켜는 순간 `jobs_updated`에 한해 §5.1의 두 계약이
느슨해집니다 (다른 신호·`store`·라이브러리 내부 판정은 영향 없음):

- **store-first 아님** — 지연된 `jobs_updated` slot에서 `js.jobs()`를 pull하면
  신호보다 **앞선** 상태가 보입니다. slot에서 pull로 표를 다시 그리면 이 기능이
  무효가 되니, 신호로 받은 `records`만 반영하세요 (§5.2 권장 패턴 그대로).
- **finished-last 아님** — `submit_finished`/`kill_finished`가 마지막 전이 배치보다
  먼저 도착할 수 있습니다. 스피너는 예정대로 꺼지고 표만 1초쯤 뒤따릅니다.
- 요약(`jobset_updated`)은 늦추지 않습니다 — dwell 동안 배지 카운트가 표보다
  앞섭니다. 배지도 함께 늦추려면 배지를 `jobs_updated`로 직접 집계하세요.
- 표시가 store보다 최대 (밀린 전이 수 × dwell)만큼 늦으므로, 값은 1초 안팎을
  권장합니다. 대량 제출이어도 같은 tick의 보류분은 jobset당 한 배치로 합쳐
  발화되어 신호 수는 늘지 않습니다.

---

## 6. 로깅 / 예외 수집

라이브러리 이벤트는 `lsfmgr.*` logger 계층으로 나갑니다:

```python
logger = logging.getLogger("lsfmgr")
logger.setLevel(logging.INFO)          # DEBUG면 LSF 명령 원문까지
logger.addHandler(my_file_handler)     # %(threadName)s 포함 포맷 권장
```

레벨 규약: DEBUG=LSF 명령/stdout/stderr 원문, INFO=submit/kill/전이,
WARNING=retry·부착물 실패, ERROR=SUBMIT_FAILED/LOST 확정·worker 예외(traceback).

**명령별 로거·레벨** — 명령 흐름은 INFO만 켜도 로거별로 추적된다:

| 명령 | INFO 로거 | 착수 | 완료 |
|---|---|---|---|
| submit | `lsfmgr.submit` | `submit 착수 <jsid>: N건` | `submit 완료 …` |
| kill | `lsfmgr.kill` | `kill 착수 <jsid> (전체/only/ids)` | `kill 완료 …: 요청/확인/미확인/잔존` |
| polling | `lsfmgr.monitor` | (정규 사이클은 DEBUG — 신호로 통지) | 자동 중지 시 INFO |

`lsfmgr.command`만 DEBUG로 올리면 원시 명령·stdout/stderr을 볼 수 있다
(argv에 bsub/bjobs/bkill이 그대로 나와 명령 구분 가능 — 예: wrapper의
`Job <id>` 파싱 문제 진단은 이 DEBUG로 stdout을 확인).

worker 예외는 스레드를 죽이지 않고 로그 + `js.error_occurred` Signal로 전달됩니다.
앱 쪽 slot 예외까지 완전 수집하려면 `sys.excepthook`, `threading.excepthook`,
`qInstallMessageHandler` 훅킹을 권장합니다 (상세는 docs/logging.md).

---

## 7. 하지 말아야 할 것

- 결과를 기다리며 busy-wait / `processEvents()` 루프 → Signal을 기다리세요.
- Signal로 받은 JobRecord 수정 → frozen이라 예외.
- `PyQt5`/`PySide6` 직접 import를 lsfmgr와 혼용 → qtpy 감지가 꼬일 수 있음.
- `js.jobs()`를 타이트 루프에서 반복 호출 → 스냅샷은 polling 주기로만
  갱신되므로 의미 없음. `jobset_updated` Signal 기반으로 반응하세요.
- `submit_finished`/`kill_finished` 핸들러 **안에서** 진행 스냅샷 pull
  (`submit_snapshot`/`kill_snapshot`) 호출 → 완료 시점이라 항상 None인 데다,
  같은 스레드 재획득 경합의 소지가 있음. 최종값은 핸들러 인자
  (`SubmitReport`/`KillReport`)에 이미 담겨 있으니 그걸 쓰세요.

---

## 8. Low-level API (고급)

여러 JobSet을 한 화면에서 통합 관리하는 대시보드처럼 전역 이벤트 스트림이
필요한 경우, manager의 전역 Signal을 직접 쓸 수 있습니다
(`jobset_updated(jsid, summary)`, `jobs_updated(jsid, records)` 등 —
JobSet Signal과 동일 이벤트의 이중 발행). 일반적인 경우엔 JobSet API로 충분합니다.

---

## 9. MockLSF — 실제 LSF 없이 테스트하기 (`mocklsf`)

실제 LSF 서버가 없는 환경에서도 lsfmgr를 개발·테스트할 수 있도록,
`bsub`/`bjobs`/`bkill` 등의 명령을 흉내내는 가상 스케줄러 `mocklsf` 패키지가
함께 포함되어 있습니다. 표준 라이브러리만 사용하며 별도 의존성이 없습니다.

`bin/`의 래퍼 스크립트를 PATH 앞에 두면 앱이 부르는 LSF 명령이 그대로 가상
구현으로 대체됩니다.

```bash
export PATH="$PWD/bin:$PATH"
mocklsfd start                 # 가상 스케줄러 데몬 기동 (bsub 최초 호출 시 자동 기동도 됨)

bsub -q normal -J myjob sleep 30
bjobs                          # PEND→RUN→DONE/EXIT 상태 전이를 시간에 따라 재현
```

- 상태는 SQLite(`$MOCKLSF_HOME/state.db`)에 저장되어 각 명령이 독립 프로세스로
  실행돼도 상태를 공유합니다(앱이 명령을 subprocess로 호출하는 구조와 일치).
- 큐·타이밍·실패율 등은 환경변수로 조정할 수 있습니다(`MOCKLSF_*`).

### 제출 wrapper 데모 (`customwrapper_sub`)

2.1절의 wrapper 제출 구조를 실제로 테스트할 수 있도록, 커스텀 툴 전용 제출
스크립트를 흉내낸 bash wrapper(`customwrapper_sub`)가 `bin/`에 들어 있습니다. 이
wrapper는 받은 인자를 그대로 같은 `bin/`의 `bsub`에 전달하고, bsub의 출력을
손대지 않고 통과시킬 뿐입니다.

```bash
customwrapper_sub -q normal run1.sp   # == bsub -q normal run1.sp → "Job <id> ..."
```

`create_jobset`에 이 커맨드들을 그대로 넘기고 `submit`하면 lsfmgr가 실행하고 `Job <id>`를
파싱해 job_id 기반으로 관리합니다. job 마다 다른 wrapper를 섞어 쓸 수 있습니다.

실제 환경에서 wrapper를 작성·지정하는 방법은 **[`docs/lsfmgr.md`](docs/lsfmgr.md)**,
대량 job 명령(submit/kill)을 GUI에 **바로바로 반영**하는 Signal 사용법은
**[`docs/signal_usage.md`](docs/signal_usage.md)**, 명령별 내부 동작 흐름은
[`docs/flows.md`](docs/flows.md), mocklsf 자체는
[`docs/mocklsf.md`](docs/mocklsf.md)를 참고하세요.

라이브러리의 **요구사항 명세(FR/QT/CS/NFR)와 v7→v9 변경 요약**은
**[`docs/LSF_JOB_MANAGER_REQUIREMENTS_v9.md`](docs/LSF_JOB_MANAGER_REQUIREMENTS_v9.md)**
에 정리되어 있습니다.
