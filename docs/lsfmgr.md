# lsfmgr 사용 가이드 (`submit_wrapper` + Signal)

실제 EDA 환경에서는 job 마다 `primesim_sub` / `verilog_sub` / `finesim_sub` 같은
**툴 전용 제출 wrapper** 를 쓴다. 각 wrapper 는 툴에 맞는 인자 처리·전처리를 한
뒤 내부에서 `bsub` 를 호출한다. lsfmgr 는 이 구조를 `submit_wrapper` 로 그대로
지원한다 — **각 wrapper 커맨드를 그대로 실행하고, 그 결과의 `Job <id>` 로 job 을
관리**한다.

> 핵심: lsfmgr 는 `bsub` 인자를 조립하지 않는다. `-q/-J/-g` 등을 붙이지 않고,
> 사용자가 준 wrapper 커맨드를 **그대로** subprocess 실행한다. job 관리(모니터링·
> kill)는 커맨드 출력에서 얻은 **job_id 만으로** 이뤄진다.

---

## 1. 기본 사용

```python
from lsfmgr import LsfJobManager

mgr = LsfJobManager()          # bsub_path 지정 불필요 (wrapper 가 커맨드에 포함됨)

# 모니터링은 매니저 통합 Signal 한 번만 연결하면 모든 JobSet 이벤트가 온다.
# (JobSet 마다 connect 할 필요 없음 — jobset_id 가 함께 온다)
mgr.jobset_updated.connect(lambda jsid, summary: print(jsid, summary))

js = mgr.submit_wrapper([
    "primesim_sub -q normal run_0.sp",       # job 마다 다른 wrapper 가능
    "verilog_sub -q long tb_1.v",
    "primesim_sub -q short run_2.sp",
])
# 특정 JobSet 하나만 볼 때는 JobSet Signal(js.jobset_updated 등)도 쓸 수 있다(같은 이벤트).
```

여러 JobSet 을 한 화면에서 관리한다면 매니저의 통합 Signal 을 쓰는 게 편하다 —
`jobset_updated(jsid, summary)` / `jobs_updated(jsid, records)` /
`submit_finished(jsid, report)` / `kill_finished(jsid, report)` /
`job_lost(jsid, record)` / `error_occurred(jsid, msg)`. JobSet(`js.*`) Signal 은 같은
이벤트의 이중 발행이라, 단일 JobSet 에 집중할 때의 편의용이다.

전체 Signal 과 그 Signal 을 발생시키는(트리거) 함수 대응은 아래
[2. Signal ↔ 함수 대응](#2-signal--함수-대응) 을 참고하라.

- **입력**: wrapper 커맨드들의 리스트. 각 항목이 job 1개다.
  - 문자열 `"primesim_sub -q normal run_0.sp"` → 내부에서 `shlex` 로 분해
  - 토큰 리스트 `["primesim_sub", "-q", "normal", "run_0.sp"]` → 셸 파싱 없이 그대로(공백/‏특수문자 안전)
  - 단일 문자열 하나만 주면 job 1개로 취급
- **반환**: `JobSet` 객체. 이후 `kill()`/`refresh()`/`updated` 등은 기존과 동일.
- job 마다 프로그램(wrapper)이 달라도 되고, job 이 3000개여도 각기 다른 wrapper 를
  섞어 쓸 수 있다.

---

## 2. Signal ↔ 함수 대응

lsfmgr 의 제어 API 는 대부분 **비동기(`[async→Signal]`)** 다. 함수는 작업을 워커
스레드에 던지고 **즉시 반환**하며, 결과는 **미리 connect 해 둔 Signal** 로 도착한다.
그러므로 **함수 호출 전에 해당 Signal 을 connect** 해 두어야 결과를 받을 수 있다.

Signal 은 두 계층이다.

- **Manager Signal** (`mgr.*`): 모든 JobSet 이벤트가 첫 인자 `jobset_id` 와 함께 온다.
  여러 JobSet 을 한 곳에서 볼 때 쓴다.
- **JobSet Signal** (`js.*`): Manager Signal 을 특정 JobSet 으로 중계한 **이중 발행**.
  단일 JobSet 에 집중할 때 편하다. 단, JobSet에 매핑되는 `jobset_id` 일 때만 발화한다
  (아래 ⚠️ 참고).

### 2.1 Manager Signal (`mgr.*`)

| Manager Signal | 시그니처(인자) | 이 Signal 을 발생시키는(트리거) 함수 |
|---|---|---|
| `submit_started` | `(jobset_id)` | `submit` · `submit_wrapper` · `submit_bulk` · `submit_array` · `resubmit_jobs` — 제출 시작 즉시 |
| `submit_progress` | `(jobset_id, done, total)` | 위 submit 계열 · `resubmit_jobs` — 제출 진행 중(throttled) |
| `submit_finished` | `(jobset_id, SubmitReport)` | 위 submit 계열 · `resubmit_jobs` 완료 · `cancel_submit`(중단 마무리) |
| `jobset_updated` | `(jobset_id, summary)` | **submit 완료(초기 PEND)** · `start_polling`(주기) · `query_once`(1회) · `reconcile` — 상태 갱신 |
| `jobs_updated` | `(jobset_id, [JobRecord])` | **submit 완료(전체 초기 레코드)** · polling **변경분이 있을 때만** |
| `job_lost` | `(jobset_id, JobRecord)` | `start_polling` · `query_once` · `detect_lost` — LSF 에서 소실 확정 |
| `kill_finished` | `(jobset_id, KillReport)` | `kill_jobset` · `kill_jobs` |
| `handler_finished` | `(jobset_id, handler_name, HandlerResult)` | `add_handler` 로 등록한 handler 1회 실행 완료 시 |
| `error_occurred` | `(jobset_id, message)` | 모든 async 경로의 워커 예외(submit · polling · kill) |

### 2.2 JobSet Signal (`js.*`) — Manager 이벤트의 이중 발행

**이름을 Manager Signal 과 동일하게** 맞췄다(인자에서 `jsid` 만 빠짐) — 두 계층
매핑이 1:1 로 명확하다. 단일 JobSet 위젯이면 `jsid` 필터 없이 이걸 쓰고, 여러
JobSet 을 한 곳에서 보면 `mgr.*` 를 쓴다.

| JobSet Signal | 시그니처 | 대응 Manager Signal | 이 Signal 을 유발하는 함수 |
|---|---|---|---|
| `js.submit_progress` | `(done, total)` | `submit_progress` | submit 계열 · `js.resubmit_jobs` |
| `js.submit_finished` | `(SubmitReport)` | `submit_finished` | submit 계열 · `js.resubmit_jobs` · `js.cancel` |
| `js.jobset_updated` | `(summary)` | `jobset_updated` | submit 완료(초기 PEND) · `js.start_polling` · `js.refresh` · `js.reconcile` |
| `js.jobs_failed` | `([JobRecord])` | (파생) | submit 완료 시 `SUBMIT_FAILED` + polling 중 실패 상태 전이분 |
| `js.kill_finished` | `(KillReport)` | `kill_finished` | `js.kill` · `js.kill_jobs` |
| `js.handler_finished` | `(handler_name, HandlerResult)` | `handler_finished` | `js.add_handler` 로 등록한 handler |
| `js.error_occurred` | `(message)` | `error_occurred` | 워커 예외 |

- `js.jobs_failed` 는 별도 트리거가 아니라 **파생 Signal** 이다 — 제출 최종 결과에
  `SUBMIT_FAILED` job 이 있거나, polling 변경분에 실패 상태(`is_failed`)가 섞이면
  발화한다. (`mgr.*` 엔 대응이 없어 `jobs_updated` 에서 `is_failed` 로 걸러 쓴다.)
- 개별 job 변경분(`mgr.jobs_updated`)은 JobSet Signal 로는 중계되지 않는다 —
  per-job 은 `mgr.jobs_updated(jsid, ...)` 를 쓴다.

> ⚠️ **`mgr.kill_jobs(job_ids)` 를 `jobset_id` 없이 부르면 JobSet Signal 로 중계되지
> 않고 `verify` 도 스킵된다** (`jobset_id=""`). 결과는 **Manager `kill_finished`** 로만
> 온다. 단 **optimistic 정책의 EXIT 전이는 전역 검색으로 적용**되어 `jobs_updated`/
> `jobset_updated` 는 해당 JobSet 으로 발화된다(테이블은 이걸로 갱신).
> 특정 JobSet 의 일부 job 만 죽이려면 **`js.kill_jobs(job_keys)`** (또는
> `mgr.kill_jobs(ids, jobset_id=...)`)를 쓰면 `js.kill_finished` 중계·verify 까지 모두 켜진다.

### 2.3 최소 연결 예시

```python
mgr = LsfJobManager()

# 제출·모니터링·kill 결과를 미리 연결 (호출 전에!)
mgr.submit_finished.connect(lambda jsid, rep: print("제출완료", jsid, rep))
mgr.jobset_updated.connect(lambda jsid, s:   print("갱신", jsid, s))
mgr.kill_finished.connect(lambda jsid, rep:  print("kill완료", jsid, rep))
mgr.error_occurred.connect(lambda jsid, msg: print("오류", jsid, msg))

js = mgr.submit_wrapper(["primesim_sub -q normal run_0.sp"])
mgr.start_polling(js.jobset_id)      # 이후 상태는 jobset_updated 로 도착
# ... 나중에 ...
mgr.kill_jobset(js.jobset_id, verify=True)   # 결과는 kill_finished 로 도착
```

### 2.4 job 재실행 — `resubmit_jobs`

이미 제출된 JobSet 안에서 **특정 job 들만 골라 다시 실행**할 때 쓴다(예: 일부만
실패해 재시도, 커맨드를 고쳐 재실행). 최초 제출(`submit*`)과 대체 관계가 아니라
**선후 관계** 다 — `submit*` 이 만들어 둔 레코드 위에서만 동작한다.

```python
js = mgr.submit_wrapper([...])                 # 최초 제출 (레코드 생성)
# ... 일부만 다시 돌리기 ...
js.resubmit_jobs([f"{js.id}_0", f"{js.id}_2"]) # 상태 기반 자동 분기
#   또는 커맨드를 바꿔서:
js.resubmit_jobs([f"{js.id}_0"], commands={f"{js.id}_0": "primesim_sub -q long a.sp"})
```

동작 원리 — 호출자가 submit/resubmit 을 고르지 않는다. **각 job 의 현재 상태**로
매니저가 알아서 분기한다:

- **살아있는 job**(`is_on_lsf`: PEND/RUN 등) → **kill(+verify) 후** 재제출
- **그 외**(CREATED/SUBMIT_FAILED/LOST/DONE/EXIT) → 그냥 재제출

레코드는 **재사용**된다 — 같은 `job_key`(-J 이름)로 다시 제출하고 `job_id`/`exit_code`
와 실행시간/위치 필드만 리셋하므로, 목록 슬롯과 `intended_count` 가 유지된다(삭제·
재생성 아님). 재실행 경로(조립 bsub vs wrapper)는 **job 단위 속성**
(`JobRecord.via_wrapper`)으로 판별한다 — merge 로 wrapper/bsub job 이 한 JobSet 에
섞여 있어도 각 job 이 자기 제출 경로로 정확히 재실행된다.

- **결과 Signal 은 submit 계열과 동일** — `submit_started` → (`submit_progress`) →
  `submit_finished`(= `js.submit_finished`). 즉 재실행 결과는 `submit_finished` 로 받는다.
- `verify=True`(기본)면 kill 후 실제 종료를 확인한 뒤 재제출한다.
- `commands={job_key: 새 커맨드}` 로 job 별 커맨드 교체 가능(생략 시 기존 커맨드 재사용).
- **polling 자동 재개** — 전원 terminal 로 polling 이 자동 중지(AUTO-2)된 JobSet 을
  재실행하면, polling 을 쓰던 JobSet 에 한해 마지막 interval 로 다시 켠다.
- **취소** — kill-phase 대기 중 `cancel_submit`(또는 `js.cancel()`)을 부르면 재제출이
  취소되고 `submit_finished`(전원 cancelled)가 온다. 이미 나간 bkill 은 되돌리지 않는다.
- 같은 JobSet 에 submit/resubmit 이 **진행 중이면 재호출은 거부**된다(LsfmgrError).
- 등록된 handler(§2.5)는 재실행되는 job 에 대해 **자동 재무장**된다 — 새 실행이
  start state 에 들면 주기 실행이 다시 돌고 종료 시 최종 실행도 다시 온다.

> ⚠️ **재실행의 내부 kill 은 `kill_finished` 로 오지 않는다.** `resubmit_jobs` 는
> 살아있는 job 을 `bkill` 로 직접 죽이는데(Killer 미경유), 이는 재제출을 위한 내부
> 단계라 `KillReport`/`kill_finished` 를 발행하지 않는다. 관측 지점은 상태 갱신
> (`jobset_updated`/`jobs_updated`, polling)과 최종 `submit_finished` 다.

### 2.5 JobSet handler — 주기 실행 (`add_handler`)

JobSet 에 **이름 있는 handler** 를 붙여, 지정한 state 구간 동안 몇 초마다 **worker
스레드에서** 실행한다. 예: job 이 도는 동안 출력 디렉토리를 주기적으로 파싱해 중간
결과를 수집하고, 완료 시 최종 수집을 한 번 더 하는 용도.

```python
def collect(ctx):                        # worker 스레드에서 실행됨 (GUI freeze 없음)
    # ctx.job_id / ctx.job_key / ctx.working_dir / ctx.record(JobRecord 전체) / ctx.final
    return parse_outputs(ctx.working_dir)   # 반환값이 그대로 Signal 로 전달됨

# 결과 구독 — handler 이름으로 필터
mgr.handler_finished.connect(
    lambda jsid, name, res: print(name, res.job_key, res.data, res.final))

js.add_handler("collect", collect,
               interval_s=5,                       # 5초마다
               start_states={JobState.RUN},        # RUN 되면 시작
               end_states={JobState.DONE, JobState.EXIT})  # 종료 시 최종 1회
# 필요 시 해제
js.remove_handler("collect")
```

동작 규칙 — job 별로 상태 기계처럼 움직인다:

- `start_states` 에 들어간 job 부터 `interval_s` 초마다 handler 를 실행한다.
  (`handler_finished` 는 **1회 실행이 끝날 때마다** job 별로 발행된다 — "handler
  전체 종료" Signal 이 아니다. 최종 실행 여부는 `res.final` 로 구분한다.)
- `end_states` 에 도달하면 **`final=True` 로 마지막에 한 번 더** 실행하고 그 job 은
  종료한다. 모든 job 이 최종 실행까지 끝나면 handler 는 **휴면**한다(타이머 정지,
  등록은 유지) — 완전 해제는 `remove_handler` 로 한다.
- `end_states` 에 없는 terminal 상태로 죽으면(예: `end_states={DONE}` 인데 EXIT)
  최종 실행 **없이** 그 job 은 조용히 종결된다 — 죽은 job 에 무한 발화하지 않는다.
- `resubmit_jobs` 로 재실행되는 job 은 진행 상태가 **자동 재무장**되고, 휴면 중이던
  handler 도 자동 재가동된다(§2.4).
- `remove_handler` 는 worker 스레드(handler fn 안 포함)에서 불러도 안전하다
  (main 으로 위임). `add_handler` 는 main 스레드 전용.
- handler 인자 `ctx`(`HandlerContext`)는 job 참조 포인트다 — `ctx.record`(JobRecord:
  `job_id`/`command`/`state`/`run_time_s`/…), 편의 프로퍼티 `ctx.job_id` ·
  `ctx.working_dir`(LSF `exec_cwd`) · `ctx.final`.
- 반환값·예외는 `HandlerResult` 로 전달된다 — `res.data`(반환값), `res.error`(예외
  repr, 정상이면 None), `res.final`, `res.job_key`, `res.job_id`.

> ℹ️ **polling 이 돌고 있어야 한다.** handler 는 Store 의 state 를 읽어 실행 여부를
> 판단하므로, `start_polling` 으로 상태가 갱신돼야 start/end 전이를 본다.
> `end_states` 는 사용자가 정한다 — 실패를 제외하려면 `{DONE, EXIT}` 만 지정하면 된다
> (기본은 terminal 전체: DONE/EXIT/SUBMIT_FAILED/LOST).

> 참고 — `working_dir`/`run_time_s`/`start_time`/`finish_time` 은 **LSF bjobs 에서
> 폴링으로 채워지는** `JobRecord` 필드다(사용자 입력 아님). 실행 시작 후 값이 생기며,
> `mgr.get_jobs()` 로도 조회할 수 있다.

---

## 3. wrapper 가 지켜야 할 계약 (2가지)

lsfmgr 는 커맨드를 그대로 실행만 하므로, wrapper 는 최종적으로 다음만 지키면 된다.

### ① `bsub` 의 성공 출력 `Job <id> ...` 를 stdout 으로 그대로 통과

lsfmgr 는 wrapper stdout 에서 정규식 **`Job <(\d+)>`** 로 job_id 를 뽑는다.

```
Job <12345> is submitted to queue <normal>.
```

- 이 문자열이 stdout 에 없으면 `NO_JOBID_PARSED` 로 **실패**(재시도 안 함).
- 진단/‏로그는 **stderr** 로 보내고, stdout 에는 `bsub` 출력만 남겨라.

### ② exit code 를 그대로 전파

- 성공 `0`, 실패는 `bsub` 의 non-zero 코드를 그대로 반환.
- 실패(non-zero)면 lsfmgr 가 `BSUB_EXIT_<rc>` 로 분류하고 `max_retry` 까지
  **재시도**한다.

> bash 에서 마지막에 `exec bsub "$@"` 로 넘기면 stdout/‏stderr/‏exit code 가 모두
> 자동으로 그대로 전파된다.

`-J`/`-g` 같은 추적용 옵션은 **lsfmgr 가 주입하지 않는다**. wrapper 가 tool 목적상
필요하면 스스로 붙이면 되지만, lsfmgr 의 job 관리는 job_id 로만 하므로 필수는
아니다.

---

## 4. 재시도 정책 (D4)

| 실패 (fail_reason) | 재시도? | 이유 |
|---|---|---|
| `BSUB_EXIT_<rc>` (non-zero 종료) | **O** | 일시적 오류로 보고 `max_retry` 까지 동일 커맨드 재실행 |
| `NO_JOBID_PARSED` (Job <id> 없음) | X | 이미 제출됐을 수 있어 재시도 시 **중복 제출 위험** |
| `BSUB_TIMEOUT` (timeout) | X | 마찬가지로 중복 제출 위험 |

즉 **비정상 종료만 재시도**한다.

---

## 5. 실행 방식 — 멀티 프로세스

- lsfmgr 는 **job(커맨드) 하나마다 wrapper 프로세스를 subprocess 로 하나** 띄운다
  (shell 미경유).
- 동시에 뜨는 프로세스 수 = `workers` 옵션(기본 16, 1~64).
  `rate_limit_per_s` 로 초당 실행 횟수도 제한한다.

```python
js = mgr.submit_wrapper(cmds, workers=8, rate_limit_per_s=20, max_retry=3)
```

- 대량 제출 시 wrapper 가 수십 개 병렬로 실행된다. wrapper 는 병렬·재진입에
  안전해야 한다(임시파일·로그 경로에 job 별 유일성 부여).

---

## 6. job 관리 — job_id 기반

- `submit_wrapper` 로 만든 JobSet 은 **그룹/이름 부착물이 없다**. 관리는 커맨드
  출력에서 얻은 **job_id 로만** 한다.
- **모니터링**: `bjobs <id>` 로 상태 조회(폴링).
- **kill**: `bkill <id1> <id2> ...` (id chunk). job 이 많으면 여러 번에 나뉘어
  호출된다(그룹 기반 "1회 kill" 최적화는 이 경로에선 적용되지 않는다).
- 모니터링·kill 에는 `bjobs`/`bkill` 명령이 필요하다. 실제 LSF 면 PATH 의
  `bjobs`/`bkill`, mocklsf 로 테스트하면 그 경로를 지정한다(아래).

---

## 7. 최소 wrapper 예시 (bash)

```bash
#!/usr/bin/env bash
set -eo pipefail

# (툴 전용 전처리: netlist 변환, 환경 로드 등) — 로그는 stderr 로
echo "primesim_sub: preprocessing..." >&2

exec bsub "$@"      # bsub 의 stdout("Job <id> ...")·exit code 를 그대로 전파
```

저장소 동봉 `bin/primesim_sub`·`bin/verilog_sub` 등은 테스트용으로 `bsub` 대신
mocklsf 의 가상 `bsub` 를 부르는 형태다. 실제 환경에서는 위처럼 진짜 `bsub` 를
호출하면 된다.

---

## 8. mocklsf 로 검증

실제 LSF 없이 검증하려면 동봉 mocklsf 를 쓴다. wrapper 프로그램은 절대경로로,
모니터링·kill 명령은 mocklsf 경로로 지정한다.

```python
import os
from lsfmgr import LsfJobManager

BIN = "/path/to/repo/bin"
mgr = LsfJobManager(
    bjobs_path=os.path.join(BIN, "bjobs"),      # 모니터링
    bkill_path=os.path.join(BIN, "bkill"),      # kill
    bhist_path=os.path.join(BIN, "bhist"),
)
js = mgr.submit_wrapper([
    f"{BIN}/primesim_sub -q normal run_0.sp",
    f"{BIN}/verilog_sub  -q long   tb_1.v",
])
```

- 더 쉬운 방법은 `examples/common.py` 의 `make_manager()` / `wrapper()` 헬퍼를
  쓰는 것이다.
- `examples/basic_example.py` 는 wrapper 선택/‏혼합으로 제출하고, **실제 실행된
  커맨드**·**할당된 job_id**·**상태 전이**를 로그로 보여준다.

```
$ primesim_sub -q normal run_0.sp
    → 할당 job_id = 12345 (rc=0)
$ verilog_sub -q long tb_1.v
    → 할당 job_id = 12346 (rc=0)
상태 js_..._0: (신규) → PEND → RUN → DONE
```

자세한 mocklsf 사용은 [`mocklsf.md`](mocklsf.md) 참고.

---

## 9. Troubleshooting

| 증상 (fail_reason) | 원인 / 해결 |
|---|---|
| `NO_JOBID_PARSED` | wrapper stdout 에 `Job <숫자>` 형식이 없음. `bsub` 출력을 그대로 통과하는지, stdout 에 다른 로그를 섞지 않는지 확인(로그는 stderr 로). **재시도되지 않는다.** |
| `BSUB_EXIT_<rc>` | wrapper/‏bsub 가 non-zero 반환. stderr 확인. `max_retry` 까지 재시도된다. |
| 모니터링/‏kill 안 됨 | `bjobs`/`bkill` 경로(또는 PATH)가 올바른지 확인. 관리는 job_id 로 하므로 job_id 확보(위 ①)가 전제. |
| 병렬 충돌 | `workers>1` 이면 wrapper 가 동시에 여러 개 실행된다. 임시파일·로그 경로에 job 별 유일성(`$LSB_JOBID`, PID 등)을 부여하라. |

---

## 10. (참고) 저수준 API — `submit` + `bsub_path`

lsfmgr 가 **직접 `bsub` 명령을 조립**하는 저수준 경로도 남아 있다. 모든 job 이
같은 `bsub`(또는 같은 wrapper) 를 쓰고, lsfmgr 가 `-q/-J/-g` 를 붙여 그룹·이름
기반으로 관리하길 원할 때 쓴다.

```python
mgr = LsfJobManager(bsub_path="bsub")          # 또는 ["primesim_sub", "--proj", "X"]
js = mgr.submit(["hspice run_1.sp", ...])       # lsfmgr 가 bsub 인자 조립
```

- 이 경로는 `-g` 그룹 기반 "kill 1회" 등 부착물 최적화를 쓴다.
- 하지만 **job 마다 다른 wrapper** 는 표현할 수 없다(프로그램이 매니저 1개 고정).
  그 경우는 위의 `submit_wrapper` 를 쓴다.
