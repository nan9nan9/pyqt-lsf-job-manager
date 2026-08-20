# 제출 가이드 — wrapper 커맨드 · handler · 재시도

실제 환경에서는 job 마다 `customwrapper_sub` 같은 **툴 전용 제출 wrapper** 를 쓴다.
각 wrapper 는 툴에 맞는 인자 처리·전처리를 한 뒤 내부에서 `bsub` 를 호출한다.
lsfmgr 는 이 구조를 그대로 지원한다 — `create_jobset` 에 넘긴 **각 wrapper 커맨드를
그대로 실행하고, 그 결과의 `Job <id>` 로 job 을 관리**한다.

> 핵심: lsfmgr 는 `bsub` 인자를 조립하지 않는다. `-q/-J/-g` 등을 붙이지 않고,
> 사용자가 준 wrapper 커맨드를 **그대로** subprocess 실행한다. job 관리(모니터링·
> kill)는 커맨드 출력에서 얻은 **job_id 만으로** 이뤄진다.

---

## 1. 기본 사용

```python
from lsfmgr import LsfJobManager

mgr = LsfJobManager()          # 제출 프로그램은 커맨드에 포함됨

js = mgr.create_jobset([
    "customwrapper_sub -q normal run_0.sp",       # job 마다 다른 wrapper 가능
    "customwrapper_sub -q long tb_1.v",
    "customwrapper_sub -q short run_2.sp",
])
mgr.submit(js)
```

- **입력**: wrapper 커맨드들의 리스트. 각 항목이 job 1개다.
  - 문자열 `"customwrapper_sub -q normal run_0.sp"` → 내부에서 `shlex` 로 분해
  - 토큰 리스트 `["customwrapper_sub", "-q", "normal", "run_0.sp"]` → 셸 파싱 없이
    그대로(공백·특수문자 안전)
  - 단일 문자열 하나만 주면 job 1개로 취급
- **반환**: `JobSet` 핸들. 이후 조회(`js.jobs()`/`js.summary`)와 Signal 구독에 쓴다.
- job 마다 프로그램(wrapper)이 달라도 되고, job 이 3000개여도 각기 다른 wrapper 를
  섞어 쓸 수 있다.
- 제출 결과 구독은 **호출 전에** connect 해 둔다 — 모든 제어 API 는 비동기다.
  Signal 배선은 [`gui.md`](gui.md) 참고.

### 1.1 작업 디렉토리 지정 (`work_dir` / `work_dirs`)

제출 프로세스를 **특정 디렉토리에서 실행**하고 싶을 때 쓴다. lsfmgr 가 bsub 인자를
조립하지 않아 `-cwd` 를 못 넘기므로, 제출 **subprocess 의 cwd** 로 실행 디렉토리를
지정한다(자식 프로세스에만 적용 → 동시 제출 worker 간 경합 없음, `os.chdir` 같은
프로세스 전역 변경은 쓰지 않는다). LSF 는 job 자체 `-cwd` 가 없으면 bsub 를 실행한
cwd 를 job 실행 디렉토리로 쓰므로 wrapper·bsub 양쪽에 유효하다.

```python
# (A) jobset 전체 동일 디렉토리
js = mgr.create_jobset(
    ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
    work_dir="/scratch/run")                       # 전 job 이 이 cwd 에서 실행

# (B) job 별 디렉토리 (commands 와 같은 길이)
js = mgr.create_jobset(
    ["customwrapper_sub a.sp", "customwrapper_sub b.sp"],
    work_dirs=["/scratch/a", "/scratch/b"])        # None 항목은 부모 cwd
```

- **`work_dir`(단일)** 과 **`work_dirs`(job 별 리스트)** 는 **동시 지정 불가** —
  둘 중 하나만 쓴다(같이 주면 `ValueError`). 둘 다 없으면 부모(GUI) 프로세스의 cwd.
- 각 job 의 `submit_cwd` 레코드 필드로 저장돼 **재제출·교체 에도 보존**된다.
  `replace_jobs`/`upsert_jobs` 로 같은 `job_key` 를 교체하면 내용 전체가 신규
  레코드로 바뀌므로 `work_dir` 도 신규 값으로 바뀐다.
- 존재하지 않는 디렉토리를 주면 그 job 은 `SUBMIT_FAILED`(fail_reason
  `BSUB_OSERROR`) 로 분류돼 마무리된다(불투명 크래시 아님).

> 참고: 구 `working_dir` 필드는 **삭제됐다** — bjobs `exec_cwd` 관측값이라
> RUN 이후에야 채워지는데, 결국 `submit_cwd` 와 같은 경로를 가리키면서 조회
> 포맷만 무겁게 하고 둘이 헷갈리기만 했다. 작업 디렉토리는 이제 `submit_cwd`
> 하나로 본다(`None` 이면 부모 프로세스 cwd). 쓰던 곳은 `submit_cwd` 로 바꾼다.

---

## 2. wrapper 가 지켜야 할 계약 (2가지)

lsfmgr 는 커맨드를 그대로 실행만 하므로, wrapper 는 최종적으로 다음만 지키면 된다.

### ① `bsub` 의 성공 출력 `Job <id> ...` 를 stdout 으로 그대로 통과

lsfmgr 는 wrapper stdout 에서 정규식 **`Job <(\d+)>`** 로 job_id 를 뽑는다.

```
Job <12345> is submitted to queue <normal>.
```

- 이 문자열이 stdout 에 없으면 `NO_JOBID_PARSED` 로 **실패**(재시도 안 함).
- 진단·로그는 **stderr** 로 보내고, stdout 에는 `bsub` 출력만 남겨라.

### ② exit code 를 그대로 전파

- 성공 `0`, 실패는 `bsub` 의 non-zero 코드를 그대로 반환.
- 실패(non-zero)면 lsfmgr 가 `BSUB_EXIT_<rc>` 로 분류하고 `max_retry` 까지
  **재시도**한다.

> bash 에서 마지막에 `exec bsub "$@"` 로 넘기면 stdout/stderr/exit code 가 모두
> 자동으로 그대로 전파된다.

`-J`/`-g` 같은 추적용 옵션은 **lsfmgr 가 주입하지 않는다**. wrapper 가 tool 목적상
필요하면 스스로 붙이면 되지만, lsfmgr 의 job 관리는 job_id 로만 하므로 필수는
아니다.

### 최소 wrapper 예시 (bash)

```bash
#!/usr/bin/env bash
set -eo pipefail

# (툴 전용 전처리: netlist 변환, 환경 로드 등) — 로그는 stderr 로
echo "customwrapper_sub: preprocessing..." >&2

exec bsub "$@"      # bsub 의 stdout("Job <id> ...")·exit code 를 그대로 전파
```

저장소 동봉 `bin/customwrapper_sub` 는 테스트용으로 `bsub` 대신 mocklsf 의 가상
`bsub` 를 부르는 형태다. 실제 환경에서는 위처럼 진짜 `bsub` 를 호출하면 된다.

---

## 3. 재시도 정책

| 실패 (fail_reason) | 재시도? | 이유 |
|---|---|---|
| `BSUB_EXIT_<rc>` (non-zero 종료) | **O** | 일시적 오류로 보고 `max_retry` 까지 동일 커맨드 재실행 |
| `NO_JOBID_PARSED` (`Job <id>` 없음) | X | 이미 제출됐을 수 있어 재시도 시 **중복 제출 위험** |
| `BSUB_TIMEOUT` (timeout) | X | 마찬가지로 중복 제출 위험 |
| `BSUB_OSERROR` (실행 자체 실패) | X | 경로·권한·cwd 문제라 재시도해도 같음 |

즉 **비정상 종료만 재시도**한다. 재시도 대기는 `retry_backoff`(`"fixed:N"` /
`"expo:N"`)를 따르며 QTimer 로 스케줄된다(스레드 sleep 없음). 대기 중 job 은
`RETRY_WAIT` 상태로 보이고, 실패 원문은 `JobRecord.fail_message` 에 남는다.

> **대량 실패에도 GUI 는 계속 돈다.** LSF 인증(eauth) 과부하로 수천 건이
> 한꺼번에 `BSUB_EXIT_255` 로 떨어지는 상황이 실제로 있다. 재시도 접수는
> main 스레드에서 일어나므로, 재시도 원장은 제출 카운터/신호 발화가 쓰는
> lock 과 **분리해서** 잠근다 — 안 그러면 재시도 1 건마다 main 이 worker
> 8 개가 붙잡은 lock 을 기다린다(2000 건 실측: main 이벤트 루프 응답 지연
> p99 1164ms → 2.0ms).

---

## 4. 실행 방식 — 멀티 프로세스

- lsfmgr 는 **job(커맨드) 하나마다 wrapper 프로세스를 subprocess 로 하나** 띄운다
  (shell 미경유).
- 동시에 뜨는 프로세스 수 = `workers` 옵션(기본 8, 1~64).
  `rate_limit_per_s` 로 초당 실행 횟수도 제한한다.

```python
mgr.submit(js, workers=8, rate_limit_per_s=20, max_retry=3)
```

- 대량 제출 시 wrapper 가 수십 개 병렬로 실행된다. wrapper 는 병렬·재진입에
  안전해야 한다(임시파일·로그 경로에 job 별 유일성 부여).
- 각 subprocess 의 실행 디렉토리는 `work_dir`/`work_dirs` 로 지정한다(§1.1).
  미지정 시 부모(앱) 프로세스의 cwd 를 상속한다.

---

## 5. job 관리 — job_id 기반

- wrapper 로 제출한 job 은 group·이름 같은 LSF 부착물을 **사용하지 않는다**.
  관리는 커맨드 출력에서 얻은 **job_id 로만** 한다.
- **모니터링**: `bjobs -noheader -o "jobid stat exit_code … delimiter=';'" <id...>` —
  id 를 `chunk_size`(기본 500) 단위로 나눠 조회한다.
- **kill**: `bkill <id1> <id2> ...` — 같은 chunk 규칙. MultiCluster 환경이면
  클러스터별 env 를 source 한 bkill 로 나눠 실행한다(README §5.4).
- 모니터링·kill 에는 `bjobs`/`bkill` 명령이 필요하다. 실제 LSF 면 PATH 의
  `bjobs`/`bkill`, mocklsf 로 테스트하면 그 경로를 지정한다(§9).

---

## 6. JobSet handler — 폴링 사이클 실행 (`add_handler`)

JobSet 에 **이름 있는 handler** 를 붙여, 지정한 state 구간 동안 **폴링 사이클마다**
(= `bjobs` 로 상태를 갱신한 직후) **worker 스레드에서** 실행한다. 예: job 이 도는
동안 출력 디렉토리를 파싱해 중간 결과를 수집하고, 완료 시 최종 수집을 한 번 더.

**별도 주기가 없다** — handler 는 `poll_interval_s` 에 tie 된다. 그래서 `ctx.record`
는 항상 방금 폴링된 **최신 상태**이고, 주기 설정도 하나로 통일된다.

```python
def collect(ctx):                        # worker 스레드에서 실행됨 (GUI freeze 없음)
    # ctx.job_id / ctx.job_key / ctx.submit_cwd / ctx.record(JobRecord 전체) / ctx.final
    cwd = ctx.submit_cwd or os.getcwd()  # 미지정 job 은 None → 부모 프로세스 cwd
    return parse_outputs(cwd)               # 반환값이 그대로 Signal 로 전달됨

# 결과 구독 — handler 이름으로 필터
mgr.handler_finished.connect(
    lambda jsid, name, res: print(name, res.job_key, res.data, res.final))

mgr.add_handler(js, "collect", collect,
                start_states={JobState.RUN},                # RUN 되면 시작 (기본)
                end_states={JobState.DONE, JobState.EXIT})  # 종료 시 최종 1회 (기본)
mgr.remove_handler(js, "collect")        # 해제
```

동작 규칙 — job 별로 상태 기계처럼 움직인다:

- `start_states`(기본 `{RUN}`) 에 들어간 job 부터 **폴링 사이클마다** handler 를
  실행한다. `handler_finished` 는 **1회 실행이 끝날 때마다** job 별로 발행된다 —
  "handler 전체 종료" Signal 이 아니다. 최종 실행 여부는 `res.final` 로 구분한다.
- `end_states`(기본 `{DONE, EXIT}`) 에 도달하면 **`final=True` 로 마지막에 한 번 더**
  실행하고 그 job 은 종료한다. 등록은 유지되며(재제출 재무장 대비), 완전 해제는
  `remove_handler` 로 한다.
- `end_states` 에 없는 terminal 상태로 죽으면(예: `end_states={DONE}` 인데 EXIT/
  LOST/SUBMIT_FAILED/CANCELLED) 최종 실행 **없이** 그 job 은 조용히 종결된다.
- **폴링이 돌고 있어야 동작한다** — handler 는 폴링 사이클에 tie 돼 있고, 첫 실행은
  다음 폴링 사이클이다(`mgr.query_once(js)` 로 즉시 1회 유도 가능).
  `auto_poll`(기본)이면 자동으로 돈다.
- `mgr.submit(js)` 으로 재실행되는 job 은 진행 상태가 **자동 재무장**되어 새 실행에서
  다시 돈다(§7).
- `remove_handler` 는 worker 스레드(handler fn 안 포함)에서 불러도 안전하다
  (main 으로 위임). `add_handler` 는 main 스레드 전용.
- handler 인자 `ctx`(`HandlerContext`)는 job 참조 포인트다 — `ctx.record`(JobRecord:
  `job_id`/`command`/`state`/`run_time_s`/…), 편의 프로퍼티 `ctx.job_id` ·
  `ctx.submit_cwd`(작업 디렉토리) · `ctx.final`.
  `ctx.submit_cwd` 는 **조회로 채워지는 값이 아니라** `create_jobset` 에 준
  `work_dir`/`work_dirs` 요청값 그대로다 — 안 줬으면 계속 `None` 이고, 그건
  "부모(GUI) 프로세스 cwd 에서 실행됨" 을 뜻한다. 경로가 필요한 handler 는
  `ctx.submit_cwd or os.getcwd()` 로 받는다(`os.chdir` 는 이 라이브러리가
  금지하므로 프로세스 cwd 는 변하지 않는다 — 이 폴백은 정확하다).
- 반환값·예외는 `HandlerResult` 로 전달된다 — `res.data`(반환값), `res.error`(예외
  repr, 정상이면 None), `res.final`, `res.job_key`, `res.job_id`.

> 참고 — `run_time_s`/`start_time`/`finish_time` 은 **LSF bjobs 에서 폴링으로
> 채워지는** `JobRecord` 필드다(사용자 입력 아님). 실행 시작 후 값이 생기며,
> `mgr.get_jobs()` 로도 조회할 수 있다. 작업 디렉토리는 여기 없다 — 폴링이
> 아니라 제출 시 지정하는 `submit_cwd`(`work_dir`/`work_dirs`) 다.
>
> `run_time_s`(경과 실행시간)는 기본적으로 **상태 전이 시점에만** 반영된다
> (`poll_runtime_updates=False`). 표에 흐르는 경과시간 열이 필요하면
> `LsfJobManager(poll_runtime_updates=True)` 로 켠다 — 그러면 RUN 중 매 폴링마다
> 갱신돼 `jobs_updated` 로 live 발행되지만, **RUN job 전원이 매 폴링 재전이**된다
> (5000건이면 사이클당 5000레코드 배치).

> 참고 — **LSF MultiCluster forwarding**: `LsfJobManager(collect_clusters=True)` 면
> 폴링이 `bjobs -o` 에 `source_cluster`·`forward_cluster` 필드를 추가해
> `JobRecord.source_cluster`(제출 클러스터)·`forward_cluster`(포워딩된 실행
> 클러스터)를 채운다. 기본은 꺼짐(MC 환경 opt-in). MC 필드를 모르는 사이트면
> 3단 강등(FULL+MC → FULL → CORE)으로 그 필드만 포기하고 `run_time` 등은 유지된다.
> `jobs_updated` 로 온 레코드에서 바로 읽어 테이블에 표시하면 된다.

> 참고 — **실패 원인 확인 (2경로)**:
> - **`JobRecord.fail_message`** — `SUBMIT_FAILED`/`RETRY_WAIT` 에서 wrapper/bsub 를
>   터미널에서 실행했을 때 나왔을 stderr/stdout 원문이 자동 저장된다
>   (예: `LSF error: queue unavailable`). 재시도 성공·재제출 리셋 시 지워진다.
>   `js.failed_jobs` 나 `jobs_failed` Signal 레코드에서 바로 읽는다.
> - **EXIT 원인** 은 LSF 이력을 따로 조회하지 않는다(폴링 오버헤드 0). 레코드 필드
>   (`exit_code`/`run_time_s`/`submit_cwd`/`start_time`/`finish_time`)로 보여 주면
>   된다 — 전부 로컬 스냅샷이라 LSF 호출이 0이다.

---

## 7. job 재실행 — replace_jobs + submit

재실행 전용 API 는 없다. 재실행은 **데이터 조작 + 일반 submit** 으로 표현한다 —
job control 은 앱(GUI)이 직접 갖는다:

```python
# 1) 살아있는 job 이 있으면 먼저 kill (앱이 직접)
mgr.kill(js); ...kill_finished 대기...

# 2) 다시 돌릴 job 을 같은 job_key 로 교체
mgr.replace_jobs(js, ["customwrapper_sub -q long a.sp"],
                 job_keys=["case-a"])
                       # case-a 가 CREATED 로 교체(같은 키 자리),
                       # 나머지 job 의 결과는 그대로

# 3) jobset 단위 재제출 — 전 job 이 리셋 후 재실행된다
if mgr.can_submit(js):
    mgr.submit(js)
```

- `mgr.submit(js)` 은 **전 job 재제출** 이다 — 전원 비활성(CREATED/DONE/EXIT/
  SUBMIT_FAILED/CANCELLED/LOST)이어야 하며 활성(RUN/PEND/SUBMITTING)이 있으면
  `SubmitNotAllowedError`. `can_submit()` 으로 선확인.
- 리셋이 이전 실행 흔적(job_id/exit_code/실행시간/fail_message/클러스터)을 지우고,
  `job_key`/`user_data`/`submit_cwd` 는 보존한다.
- 재실행되는 job 의 handler(§6)는 **자동 재무장**된다.
- polling 은 `auto_poll`(기본) 옵션으로 submit 시 자동 시작된다.

---

## 8. 완료 후처리 — `submit(post_process=fn)`

제출한 jobset 의 **전 job 이 terminal** 에 도달하면 결과 수집·정리 등을 자동
실행한다. `pre_submit` 게이트(제출 **전**)와 대칭인 완료 **후** 훅이다.

```python
def collect(records):                    # 최종 JobRecord 목록 (worker 스레드)
    done = [r for r in records if r.state is JobState.DONE]
    return {"ok": len(done), "failed": len(records) - len(done)}

mgr.post_processing_finished.connect(
    lambda jsid, result: print("후처리 결과", result))

mgr.submit(js, post_process=collect)     # pre_submit 과 함께 지정 가능
```

- **발화 시점**: 완료 감지(폴링 또는 `mgr.query_once(js)`) 시 전원 terminal 이면
  worker 에서 1회 실행. `auto_poll`(기본)이면 자동 감지된다.
- **결과 무관**: DONE/EXIT/SUBMIT_FAILED/CANCELLED/LOST 가 섞여도 **전원 terminal** 이면
  실행된다 — 콜백에서 성공/실패를 분류한다.
- **신호**: `jobset_finished(summary)` → `post_processing_started` →
  `post_processing_finished(result)` (`result` = 콜백 반환값, 예외 시 `None` +
  `error_occurred`). `jobset_finished`는 `post_process` 를 안 걸어도 나온다 —
  후처리 없이 완료만 알고 싶으면 그것만 연결하면 된다.
- **한 제출당 1회**. 완료 전에 `post_process` 없이 재제출하면 이전 무장은 해제된다.
- 콜백은 **worker 스레드** 실행 — GUI 객체 접근 금지.

---

## 9. mocklsf 로 검증

실제 LSF 없이 검증하려면 동봉 mocklsf 를 쓴다. wrapper 프로그램은 절대경로로,
모니터링·kill 명령은 mocklsf 경로로 지정한다.

```python
import os
from lsfmgr import LsfJobManager

BIN = "/path/to/repo/bin"
mgr = LsfJobManager(
    bjobs_path=os.path.join(BIN, "bjobs"),      # 모니터링
    bkill_path=os.path.join(BIN, "bkill"),      # kill
)
js = mgr.create_jobset([
    f"{BIN}/customwrapper_sub -q normal run_0.sp",
    f"{BIN}/customwrapper_sub -q long   tb_1.v",
])
mgr.submit(js)
```

- 커맨드를 고치지 않고 **전 제출을 mock 실행 파일로** 돌리려면 패턴 치환
  `test_submit_wrapper_pattern_cmd`(README §4.3)를 쓴다.
- 더 쉬운 방법은 `examples/common.py` 의 `make_manager()` / `wrapper()` 헬퍼다.
- `examples/gui_demo.py` 는 wrapper 선택·혼합으로 제출하고, **실제 실행된 커맨드** ·
  **할당된 job_id** · **상태 전이**를 로그로 보여준다.

```
$ customwrapper_sub -q normal run_0.sp
    → 할당 job_id = 12345 (rc=0)
$ customwrapper_sub -q long tb_1.v
    → 할당 job_id = 12346 (rc=0)
상태 js_..._0: (신규) → PEND → RUN → DONE
```

자세한 mocklsf 사용은 [`mocklsf.md`](mocklsf.md) 참고.

---

## 10. Troubleshooting

| 증상 (fail_reason) | 원인 / 해결 |
|---|---|
| `NO_JOBID_PARSED` | wrapper stdout 에 `Job <숫자>` 형식이 없음. `bsub` 출력을 그대로 통과하는지, stdout 에 다른 로그를 섞지 않는지 확인(로그는 stderr 로). **재시도되지 않는다.** |
| `BSUB_EXIT_<rc>` | wrapper/bsub 가 non-zero 반환. `fail_message` 의 stderr 확인. `max_retry` 까지 재시도된다. |
| `BSUB_OSERROR` | 실행 파일 경로·권한 문제이거나 `work_dir` 이 없는 디렉토리. |
| `BSUB_TIMEOUT` | 제출 1건이 `submit_timeout_s`(기본 30초)를 넘김. wrapper 전처리가 무거우면 값을 늘린다. |
| 모니터링·kill 안 됨 | `bjobs`/`bkill` 경로(또는 PATH)가 올바른지 확인. 관리는 job_id 로 하므로 job_id 확보(§2 ①)가 전제. |
| 병렬 충돌 | `workers>1` 이면 wrapper 가 동시에 여러 개 실행된다. 임시파일·로그 경로에 job 별 유일성(`$LSB_JOBID`, PID 등)을 부여하라. |
| 원인을 모르겠다 | `logging.getLogger("lsfmgr.command").setLevel(logging.DEBUG)` — 실행된 argv·cwd·rc·stdout/stderr 이 전부 남는다([`logging.md`](logging.md)). |
