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
js = mgr.submit([f"hspice run_{i}.sp" for i in range(5000)])
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
js = mgr.submit(jobs, workers=8, max_retry=0, queue="short",
                label="quick_check", auto_poll=False)
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
| `submit_finished_on_gate_reject` | True | 생성자 | `pre_submit` 게이트가 False면 `submit_finished`(cancelled=N)도 발화. False면 종료는 `ready_finished(False)`만 |
| `collect_clusters` | False | 생성자 | LSF MultiCluster forwarding 정보 수집. 켜면 `JobRecord.source_cluster`/`forward_cluster`를 폴링으로 채움(MC 환경 opt-in) |
| `progress_min_interval_s` | 0.1 | 생성자 | progress/jobs_updated 최소 발화 간격(초). 키우면 부하↓·반응성↓ |
| `progress_min_step_ratio` | 0.01 | 생성자 | progress 최소 진행 비율(0~1). 키우면 발화↓ |
| `auto_poll` | True | 생성자, submit | submit 후 polling 자동 시작 |
| `mode` | `"auto"` | submit | `"auto"`/`"array"`/`"bulk"` |
| `queue` | LSF 기본 | 생성자(`default_queue`), submit | 대상 queue |
| `resource_req` | 없음 | 생성자, submit | `-R` 문자열 |
| `output_dir` | 없음 | 생성자, submit | `-o`/`-e` 경로 규칙 |
| `submit_timeout_s` | 30 | 생성자, submit | bsub 1건 timeout |
| `verify_kill` | False | 생성자, kill | kill 후 실제 종료 확인 |
| `kill_status_policy` | `"optimistic"` | 생성자 | `"optimistic"`=terminated 확인 시 즉시 EXIT / `"actual"`=실제 LSF 상태(폴링)로만 |
| `kill_max_retry` | 2 | 생성자 | kill 확인 실패 시 재시도 횟수 |
| `label` / `tags` / `description` | 빈 값 | submit | JobSet 메타데이터 |
| `persistent` | False | 생성자 | True → SQLite 영속 모드 (§6) |
| `db_path` | `~/.lsfmgr/jobsets.db` | 생성자 | persistent=True일 때 |
| `chunk_size` | 200 | 생성자 | chunking fallback 크기 |
| `bsub_path` 등 | PATH 탐색 | 생성자 | LSF 명령 경로 (문자열 또는 wrapper 토큰 목록) |

- 오타 키워드는 즉시 `TypeError`, 범위 벗어나면 `ValueError` — 조용히
  무시되지 않습니다.
- 옵션이 많은 설정을 파일/객체로 관리하고 싶으면 `LsfConfig`를 만들어
  `LsfJobManager(config=cfg)`로 주입할 수 있습니다 (kwargs가 우선).

### 2.1 wrapper 커맨드로 제출 — `submit_wrapper` (예: `primesim_sub`)

실제 환경처럼 job 마다 `primesim_sub`/`verilog_sub` 등 **서로 다른 제출 wrapper**를
쓰는 경우, `submit_wrapper`에 wrapper 커맨드들을 그대로 넘깁니다. lsfmgr는 각
커맨드를 **그대로 실행**하고 출력의 `Job <id>`를 파싱해 **job_id 기반**으로
모니터링·kill 합니다(‑q/‑J/‑g 등 인자 조립·주입 없음).

```python
mgr = LsfJobManager()          # bsub_path 지정 불필요

js = mgr.submit_wrapper([
    "primesim_sub -q normal run_0.sp",         # job 마다 다른 wrapper 가능
    ["verilog_sub", "-q", "long", "tb_1.v"],   # 문자열 또는 토큰 리스트
    "primesim_sub -q short run_2.sp",
], workers=8, max_retry=3)
```

- wrapper는 결국 `bsub`를 호출하고 그 `Job <id>` 출력·exit code를 그대로 통과시키면
  됩니다. 재시도는 **비정상 종료(non-zero)만** 대상입니다.
- 모니터링·kill용 `bjobs`/`bkill`은 실제 LSF면 PATH, mocklsf면 경로를 지정합니다.

#### 제출 전 전처리 게이트 (`pre_submit`)

실제 제출 전에 **커맨드 리스트 전체를 한 번 검사/준비**하고 통과할 때만
제출을 진행하려면 `submit`/`submit_wrapper`에 `pre_submit` 콜백을 넘깁니다.
콜백은 **단일 worker 스레드**에서 1회 실행되고 `bool`을 반환합니다.

```python
def prepare(commands: list[str]) -> bool:      # 실행될 커맨드 문자열 목록
    stage_input_files(commands)                # 일괄 준비 (부수효과)
    return all_inputs_ready(commands)          # True면 제출, False면 제출 안 함

js = mgr.submit_wrapper([...], pre_submit=prepare)
```

신호 순서는 **`ready_started` → `ready_finished(ok)` → (ok=True일 때만)
`submit_started` → … → `submit_finished`**. 게이트가 `False`면 제출하지 않고
job은 `CREATED`로 남습니다(기본은 `submit_finished(cancelled=N)`도 발화 —
`LsfJobManager(submit_finished_on_gate_reject=False)`로 끄면 종료 통지는
`ready_finished(False)`만). 콜백이 **예외**를 던지면 전원 `SUBMIT_FAILED`
(`fail_reason="PRE_SUBMIT_FAILED"`, `fail_message`에 예외 원문) + `error_occurred`
+ `submit_finished(failed=N)`로 보고합니다.

> ⚠️ 콜백은 **worker 스레드**에서 돕니다 — Qt 위젯 등 **GUI 객체 접근 금지**.
> 재시도 시 재실행되므로 부수효과는 **멱등**이어야 합니다.

`resubmit_jobs`도 `pre_submit`을 받습니다. 재제출은 살아있는 job을 kill 후
재실행하는데, 게이트는 **kill 이전**에 돕니다 — 그래서 게이트가 `False`면
돌던 job을 **죽이지 않고 그대로** 두며 재제출도 안 합니다(레코드 미변경).
게이트 실행 중 `js.cancel()`을 부르면 kill 착수 전에 멈춥니다.
```python
js.resubmit_jobs([job_key, ...], pre_submit=prepare)
```

> 작성 규칙·실행 방식(멀티 프로세스)·검증·트러블슈팅, 그리고 lsfmgr가 직접 bsub를
> 조립하는 저수준 `submit`(+`bsub_path`)은 **[`docs/lsfmgr.md`](docs/lsfmgr.md)**
> 에 정리되어 있습니다.

---

## 3. JobSet — 모든 것의 중심

`submit()`은 문자열 ID가 아니라 **JobSet**을 반환합니다.
이 JobSet 하나로 해당 묶음의 모니터링/제어/조회를 전부 합니다.

### 3.1 Signal (이 JobSet의 이벤트만 옴 — 필터링 불필요)

이름은 `mgr.*` Signal과 동일하다(인자에서 `jsid`만 빠짐). 여러 JobSet을 한 곳에서
보면 `mgr.*`(jsid 포함)를, 단일 JobSet 위젯이면 아래 `js.*`를 쓴다.

| Signal | 인자 | 시점 |
|---|---|---|
| `jobset_updated` | `dict` 요약 | **submit 완료 시(초기 PEND)** + polling/refresh 후 |
| `submit_progress` | `(done, total)` | submit/resubmit 진행 (throttled) |
| `submit_finished` | `SubmitReport` | submit/resubmit 완료 (retry 포함 최종) |
| `jobs_failed` | `list[JobRecord]` | SUBMIT_FAILED/EXIT/LOST 변경분 — `rec.fail_message`에 실패 진단 원문 |
| `kill_progress` | `(done, total)` | 대량 chunk kill 진행 (throttled, 마지막 100%) |
| `kill_finished` | `KillReport` | kill 완료 |
| `handler_finished` | `(name, HandlerResult)` | 등록한 handler 1회 실행 완료마다 (§3.5) |
| `error_occurred` | `str` | worker 예외 등 |

요약 dict 예:
```python
{"total": 5000, "RUN": 2100, "PEND": 2800, "DONE": 80, "EXIT": 12,
 "SUBMIT_FAILED": 5, "RETRY_WAIT": 2, "LOST": 1}
# 불변식: 합계 == total (손실 job도 반드시 어딘가에 집계됨)
```

### 3.2 제어 (비동기 — 즉시 반환, 결과는 Signal)

```python
js.kill()                              # 전체 kill (명령 1회, ARG_MAX 안전)
js.kill(only_state=JobState.PEND)      # PEND만
js.kill(verify=True)                   # 실제 종료까지 확인
js.kill_jobs([job_key, ...])           # 선택 job만 kill (테이블 선택 행)
js.kill_jobs(keys, envpath="/lsf/busan/conf/cshrc.lsf")  # MC forward job — env source 후 bkill
js.cancel()                            # 진행 중 submit/resubmit 중단 (된 것은 유지)
js.refresh()                           # 지금 즉시 1회 조회 요청
js.stop_polling(); js.start_polling(interval_s=30)
js.resubmit_jobs([job_key, ...])       # 특정 job 재실행 — 살아있으면 kill 후
                                       # (레코드 재사용, 원 제출 옵션 보존)
js.close()                             # 종결 (전원 terminal일 때)
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
> 환경을 지원합니다. `js.kill()`/`js.kill_jobs()`에 `envpath="<cshrc 경로>"`를
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
>         js.kill_jobs(keys)                     # envpath 없이 (로컬 bkill)
>     else:                                      # forward된 job
>         js.kill_jobs(keys, envpath=CSHRC[cluster])
> ```
> `None`을 안 걸러내면 `CSHRC[None]`에서 `KeyError`가 나거나 로컬 job에
> 불필요하게 envpath를 씌우게 됩니다. (`source_cluster`는 LSF 버전에 따라 로컬
> job도 자기 클러스터명이 나올 수 있어 판별 기준으로는 부적합 — `forward_cluster`
> 로 판별하세요.)
> `js.resubmit_jobs(keys, envpath=...)`도 동일 — 재제출의 kill 단계에서 그 env를
> source합니다 (안 주면 forward job이 안 죽은 채 새 job이 중복 제출됨).

### 3.3 조회 (동기 — 로컬 스냅샷, LSF 호출 없음)

```python
js.summary                 # 요약 dict
js.is_done                 # 전원 terminal?
js.is_active               # 하나라도 안 끝난(non-terminal) job이 있으면 True
js.is_inactive             # 전원 terminal(DONE/EXIT/SUBMIT_FAILED/LOST)이면 True
js.failed_jobs             # SUBMIT_FAILED/EXIT/LOST 목록
js.jobs()                  # 전체 JobRecord
js.jobs(states={JobState.RUN})
js.detect_lost()           # 손실 감지 (name 패턴 복구 시도 포함)
js.id                      # jobset_id 문자열 (로그/저장용)
```

> **`is_active` / `is_inactive`** — 이 JobSet을 다시 수행할지 판단할 때 씁니다.
> `inactive`는 **모든 job이 terminal**(DONE·EXIT·SUBMIT_FAILED·LOST)이라 더
> 진행할 게 없는 상태이고, `is_active = not is_inactive`(하나라도 CREATED/
> SUBMITTING/RETRY_WAIT/PEND/RUN/suspend 등 non-terminal). `is_done`과 거의
> 같지만 job이 하나도 없는 빈 JobSet은 `is_inactive=True`(is_done은 False)로
> 다릅니다.
> ```python
> if js.is_inactive:        # 진행 중인 것 없음 → 재수행 판단
>     js.resubmit_jobs([r.job_key for r in js.jobs()])
> ```

> **대량 제출을 백그라운드로 돌리기** (`is_submitting` / `submit_state`) —
> `submit()`은 **즉시 반환**합니다. 실제 `primesim_sub`→`bsub`→JOBID 캡처는
> 전부 worker 스레드에서 돌아 **GUI를 막지 않습니다**. 그래서 1000개+ 제출이
> 오래 걸려도 진행 dialog를 **modeless로 띄우거나 아예 닫고 딴 작업**을 해도
> 됩니다 — 제출의 소유자는 매니저지 dialog가 아니라서, `js` 핸들만 들고
> 있으면 계속 진행됩니다. 진행 dialog(`QProgressDialog`)는 `submit_progress`
> Signal에 연결하되 **`setModal(False)`** 로 두고, 사용자가 닫아도 제출은
> 계속 두면 됩니다. 나중에 상태 패널을 다시 열 때는 그동안 놓친 Signal 대신
> **pull로 현재 진행을 조회**합니다:
> ```python
> js = mgr.submit(cmds)              # 즉시 반환 — 아래는 언제든 호출 가능
> if js.is_submitting:               # 아직 제출 중?
>     s = js.submit_state            # SubmitProgress | None
>     bar.setValue(int(s.fraction * 100))          # done/total 진행률
>     label.setText(f"{s.done}/{s.total} "
>                   f"(성공 {s.succeeded} / 실패 {s.failed})")
> ```
> `submit_state`는 진행 중이 아니면 `None`이고, 완료 후 최종 결과는
> `submit_finished(SubmitReport)` 또는 `js.summary`로 봅니다.
> `is_submitting`은 제출/재제출이 도는 동안 True입니다(`jobs`의 PEND/RUN이
> 아니라 **제출 작업 자체**의 진행 여부).
> > 앱을 닫으면(`shutdown`) 진행 중이던 bsub는 완료까지 기다리되 아직 제출
> > 안 된 몫은 취소됩니다. 앱 재시작 후에도 이어서 추적하려면
> > `persistent=True`(SQLite)로 두고 재시작 시 `list_orphan_jobsets()` →
> > `recover_jobset()`으로 복원하세요 (§6).

> **실패 원인 표시** — 두 경로로 확인합니다.
> - **SUBMIT_FAILED/RETRY_WAIT**: `rec.fail_message`에 bsub/wrapper 실행의
>   stderr/stdout(터미널에서 봤을 메시지)이 자동 저장됩니다. 재시도 성공/
>   재제출 시 자동으로 지워집니다.
> - **EXIT**: 자동 수집하지 않습니다(폴링 오버헤드 0). 상태 셀 클릭 등
>   필요한 시점에 `js.fetch_job_detail(job_key)`를 호출하면 `bhist -l`
>   원문이 `js.job_detail_ready(job_key, text)` Signal로 옵니다(bhist는
>   worker 스레드 — GUI 안 멎음). 동기 버전은 `js.job_detail(job_key)`.
>   제출 실패 job에 호출하면 저장된 fail_message를 돌려주므로, 클릭 핸들러
>   하나로 모든 실패 상태를 처리할 수 있습니다.

> 조회 값은 **마지막 polling 시점 스냅샷**입니다 (최대 `poll_interval_s`
> 지연). 단 `SUBMIT_FAILED`는 submit 과정에서 직접 기록되므로 항상 정확합니다.
> 지금 즉시 최신이 필요하면 `js.refresh()` 후 `jobset_updated` Signal에서 읽으세요.

### 3.4 그 외

```python
merged = js_a.merge_with(js_b)         # 병합 → 새 JobSet
js2 = mgr.jobset(jobset_id)            # ID로 JobSet 재획득
js.add_job(record); js.remove_job(key) # job 편입 / 편입 취소 (intended 유지)
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

js.add_handler("collect", collect,
               start_states={JobState.RUN},             # RUN이 되면 시작 (기본)
               end_states={JobState.DONE, JobState.EXIT})  # 종료 시 최종 1회 (기본)
# start/end 미지정 시 기본값 = 시작 {RUN}, 종료 {DONE, EXIT}
js.remove_handler("collect")               # 해제
```

- `handler_finished`는 **1회 실행이 끝날 때마다** job별로 옵니다 — 최종 실행은
  `res.final`로 구분. 예외는 `res.error`에 담겨 옵니다(다른 job에 영향 없음).
- **폴링이 돌고 있어야 동작**합니다(auto_poll 기본이면 자동). 첫 실행은 다음 폴링
  사이클이며, `js.refresh()`로 즉시 1회 유도 가능합니다.
- `resubmit_jobs`로 재실행하면 진행 상태가 자동 재무장되어 새 실행에서 다시 돕니다.
- 실행 예제: `examples/handler_example.py`, 상세 규칙:
  [`docs/lsfmgr.md`](docs/lsfmgr.md) §2.5.

---

## 4. 사용 예제

### 4.1 진행률 + 완료 처리

```python
js = mgr.submit(jobs, label="tt_sweep", tags=["sweep", "rev2"])
js.submit_progress.connect(lambda d, t: bar.setValue(int(d / t * 100)))
js.submit_finished.connect(lambda rpt: statusbar.showMessage(
    f"submitted {rpt.ok}/{rpt.total} (failed {rpt.failed})"))
js.jobs_failed.connect(lambda recs: table.append_failures(recs))
```

### 4.2 Array job (mode 자동/강제)

```python
# 동일 command 패턴 → mode="auto"가 알아서 array 선택
js = mgr.submit("run_sim.sh $LSB_JOBINDEX", count=1000)

# element별 command가 달라도 array 강제 가능 (dispatch 스크립트 자동 생성)
js = mgr.submit([f"hspice tt_{i}.sp" for i in range(1000)], mode="array")
```

### 4.3 완료 대기 후 후속 작업

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

---

## 6. SQLite 영속 모드 (옵션)

세션 간 복원·이력·통계가 필요할 때만 켭니다:

```python
mgr = LsfJobManager(persistent=True)               # ~/.lsfmgr/jobsets.db
mgr = LsfJobManager(persistent=True, db_path="/local_disk/jk/jobs.db")
```

### 앱 시작 시 이전 세션 복원

```python
if mgr.persistent:
    for rec in mgr.list_orphan_jobsets():          # 미종결 JobSet 목록
        if ask_user(rec.label):                    # 복원 여부는 앱이 결정
            js = mgr.recover_jobset(rec.jobset_id) # JobSet 반환
            js.reconcile()                         # 죽어있는 동안의
                                                   # DONE/EXIT/LOST 반영 (비동기)
            # reconcile 완료 → updated Signal → 이후 자동 polling
```

### 이력/통계 (Sqlite 전용)

```python
mgr.get_history(js.id)          # 상태 전이 이력
mgr.stats(since=last_week)      # 성공률, PEND→RUN 대기시간 분포 등
mgr.search_all_sessions(tag="sweep")
mgr.export_jobset(js.id, "report.json")
```

| | InMemory (기본) | SQLite (`persistent=True`) |
|---|---|---|
| 파일 생성 | 없음 | db 1개 |
| 앱 종료 시 JobSet | 소멸 (LSF job은 잔존) | 보존 |
| 복원/이력/통계 | `PersistenceNotSupportedError` | 지원 |

> 인메모리 모드에서 앱이 죽어도 job은 LSF에 남습니다 —
> `bjobs -g /lsfmgr/<user>/<jobset_id>`로 수동 확인/정리 가능.

---

## 7. 로깅 / 예외 수집

라이브러리 이벤트는 `lsfmgr.*` logger 계층으로 나갑니다:

```python
logger = logging.getLogger("lsfmgr")
logger.setLevel(logging.INFO)          # DEBUG면 LSF 명령 원문까지
logger.addHandler(my_file_handler)     # %(threadName)s 포함 포맷 권장
```

레벨 규약: DEBUG=LSF 명령/stdout/stderr 원문, INFO=submit/kill/전이,
WARNING=retry·부착물 실패, ERROR=SUBMIT_FAILED/LOST 확정·worker 예외(traceback).

worker 예외는 스레드를 죽이지 않고 로그 + `js.error_occurred` Signal로 전달됩니다.
앱 쪽 slot 예외까지 완전 수집하려면 `sys.excepthook`, `threading.excepthook`,
`qInstallMessageHandler` 훅킹을 권장합니다 (상세는 docs/logging.md).

---

## 8. 하지 말아야 할 것

- 결과를 기다리며 busy-wait / `processEvents()` 루프 → Signal을 기다리세요.
- Signal로 받은 JobRecord 수정 → frozen이라 예외.
- `PyQt5`/`PySide6` 직접 import를 lsfmgr와 혼용 → qtpy 감지가 꼬일 수 있음.
- SQLite db를 NFS에 두기 → lock 신뢰 불가, 로컬 디스크 권장 (경고 로그 남음).
- `js.jobs()`를 타이트 루프에서 반복 호출 → 스냅샷은 polling 주기로만
  갱신되므로 의미 없음. `jobset_updated` Signal 기반으로 반응하세요.

---

## 9. Low-level API (고급)

여러 JobSet을 한 화면에서 통합 관리하는 대시보드처럼 전역 이벤트 스트림이
필요한 경우, manager의 전역 Signal을 직접 쓸 수 있습니다
(`jobset_updated(jsid, summary)`, `jobs_updated(jsid, records)` 등 —
JobSet Signal과 동일 이벤트의 이중 발행). 일반적인 경우엔 JobSet API로 충분합니다.

---

## 10. MockLSF — 실제 LSF 없이 테스트하기 (`mocklsf`)

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

### 툴별 제출 wrapper (`primesim_sub` / `finesim_sub` / `spectrefx_sub` / `verilog_sub`)

2.1절의 `submit_wrapper` 구조를 실제로 테스트할 수 있도록, EDA 툴 전용 제출
스크립트를 흉내낸 bash wrapper 4종이 `bin/`에 함께 들어 있습니다. 각 wrapper는
받은 인자를 그대로 같은 `bin/`의 `bsub`에 전달하고, bsub의 출력을 손대지 않고
통과시킬 뿐입니다.

```bash
primesim_sub -q normal run1.sp   # == bsub -q normal run1.sp → "Job <id> ..."
```

`submit_wrapper`에 이 커맨드들을 그대로 넘기면 lsfmgr가 실행하고 `Job <id>`를
파싱해 job_id 기반으로 관리합니다. job 마다 다른 wrapper를 섞어 쓸 수 있습니다.

실제 환경에서 wrapper를 작성·지정하는 방법은 **[`docs/lsfmgr.md`](docs/lsfmgr.md)**,
대량 job 명령(submit/kill/resubmit)을 GUI에 **바로바로 반영**하는 Signal 사용법은
**[`docs/signal_usage.md`](docs/signal_usage.md)**, mocklsf 자체는
[`docs/mocklsf.md`](docs/mocklsf.md)를 참고하세요.
