# 제출 wrapper 사용 가이드 (`submit_wrapper`)

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
# 특정 JobSet 하나만 볼 때는 핸들 Signal(js.updated 등)도 쓸 수 있다(같은 이벤트).
```

여러 JobSet 을 한 화면에서 관리한다면 매니저의 통합 Signal 을 쓰는 게 편하다 —
`jobset_updated(jsid, summary)` / `jobs_updated(jsid, records)` /
`submit_finished(jsid, report)` / `kill_finished(jsid, report)` /
`job_lost(jsid, record)` / `error_occurred(jsid, msg)`. 핸들(`js.*`) Signal 은 같은
이벤트의 이중 발행이라, 단일 JobSet 에 집중할 때의 편의용이다.

- **입력**: wrapper 커맨드들의 리스트. 각 항목이 job 1개다.
  - 문자열 `"primesim_sub -q normal run_0.sp"` → 내부에서 `shlex` 로 분해
  - 토큰 리스트 `["primesim_sub", "-q", "normal", "run_0.sp"]` → 셸 파싱 없이 그대로(공백/‏특수문자 안전)
  - 단일 문자열 하나만 주면 job 1개로 취급
- **반환**: `JobSet` 핸들. 이후 `kill()`/`refresh()`/`updated` 등은 기존과 동일.
- job 마다 프로그램(wrapper)이 달라도 되고, job 이 3000개여도 각기 다른 wrapper 를
  섞어 쓸 수 있다.

---

## 2. wrapper 가 지켜야 할 계약 (2가지)

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

## 3. 재시도 정책 (D4)

| 실패 (fail_reason) | 재시도? | 이유 |
|---|---|---|
| `BSUB_EXIT_<rc>` (non-zero 종료) | **O** | 일시적 오류로 보고 `max_retry` 까지 동일 커맨드 재실행 |
| `NO_JOBID_PARSED` (Job <id> 없음) | X | 이미 제출됐을 수 있어 재시도 시 **중복 제출 위험** |
| `BSUB_TIMEOUT` (timeout) | X | 마찬가지로 중복 제출 위험 |

즉 **비정상 종료만 재시도**한다.

---

## 4. 실행 방식 — 멀티 프로세스

- lsfmgr 는 **job(커맨드) 하나마다 wrapper 프로세스를 subprocess 로 하나** 띄운다
  (shell 미경유).
- 동시에 뜨는 프로세스 수 = `workers` 옵션(기본 16, 1~32).
  `rate_limit_per_s` 로 초당 실행 횟수도 제한한다.

```python
js = mgr.submit_wrapper(cmds, workers=8, rate_limit_per_s=20, max_retry=3)
```

- 대량 제출 시 wrapper 가 수십 개 병렬로 실행된다. wrapper 는 병렬·재진입에
  안전해야 한다(임시파일·로그 경로에 job 별 유일성 부여).

---

## 5. job 관리 — job_id 기반

- `submit_wrapper` 로 만든 JobSet 은 **그룹/이름 부착물이 없다**. 관리는 커맨드
  출력에서 얻은 **job_id 로만** 한다.
- **모니터링**: `bjobs <id>` 로 상태 조회(폴링).
- **kill**: `bkill <id1> <id2> ...` (id chunk). job 이 많으면 여러 번에 나뉘어
  호출된다(그룹 기반 "1회 kill" 최적화는 이 경로에선 적용되지 않는다).
- 모니터링·kill 에는 `bjobs`/`bkill` 명령이 필요하다. 실제 LSF 면 PATH 의
  `bjobs`/`bkill`, mocklsf 로 테스트하면 그 경로를 지정한다(아래).

---

## 6. 최소 wrapper 예시 (bash)

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

## 7. mocklsf 로 검증

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
- `examples/dashboard.py` 는 wrapper 선택/‏혼합으로 제출하고, **실제 실행된
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

## 8. Troubleshooting

| 증상 (fail_reason) | 원인 / 해결 |
|---|---|
| `NO_JOBID_PARSED` | wrapper stdout 에 `Job <숫자>` 형식이 없음. `bsub` 출력을 그대로 통과하는지, stdout 에 다른 로그를 섞지 않는지 확인(로그는 stderr 로). **재시도되지 않는다.** |
| `BSUB_EXIT_<rc>` | wrapper/‏bsub 가 non-zero 반환. stderr 확인. `max_retry` 까지 재시도된다. |
| 모니터링/‏kill 안 됨 | `bjobs`/`bkill` 경로(또는 PATH)가 올바른지 확인. 관리는 job_id 로 하므로 job_id 확보(위 ①)가 전제. |
| 병렬 충돌 | `workers>1` 이면 wrapper 가 동시에 여러 개 실행된다. 임시파일·로그 경로에 job 별 유일성(`$LSB_JOBID`, PID 등)을 부여하라. |

---

## 9. (참고) 저수준 API — `submit` + `bsub_path`

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
