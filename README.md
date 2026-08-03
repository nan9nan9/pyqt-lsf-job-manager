# lsfmgr — LSF Job Manager for Qt Applications

대량 LSF job의 **submit / monitoring / kill / 묶음(JobSet) 관리** 라이브러리.
`qtpy` 기반으로 **PyQt5 / PySide2 / PyQt6 / PySide6** 어디서든 동일하게
동작하며, 모든 LSF 호출은 백그라운드 스레드에서 실행되고 결과는 Signal로
통지되므로 **GUI가 freeze되지 않습니다**.

```
의존성: qtpy + Qt 바인딩 1종 (그 외 stdlib only)    Python: 3.9+
```

| 문서 | 내용 |
|---|---|
| 이 파일 | 전체 매뉴얼 — 개념·옵션·API·Signal·GUI 규칙 |
| [`docs/submit.md`](docs/submit.md) | 제출 wrapper 계약, 작업 디렉토리, 재시도, handler, 트러블슈팅 |
| [`docs/gui.md`](docs/gui.md) | GUI 연동 실전 — Signal 배선, 진행 표시, 테이블 갱신, 흔한 실수 |
| [`docs/flows.md`](docs/flows.md) | 명령별 내부 동작 흐름(스레드·상태 전이·barrier) |
| [`docs/logging.md`](docs/logging.md) | 로깅 계층과 예외 수집 |
| [`docs/mocklsf.md`](docs/mocklsf.md) | 동봉 가상 LSF(`mocklsf`)로 실제 클러스터 없이 검증 |
| [`docs/requirements.md`](docs/requirements.md) | 요구사항 명세(FR/QT/CS/NFR) |

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

> **API 계약**: 제어 API(submit/kill/polling)는 전부 **즉시 반환(비동기)**,
> 결과는 Signal로 도착합니다. 조회 프로퍼티(summary/jobs)는 **동기**지만
> 로컬 스냅샷만 읽으므로 ms 단위입니다 (LSF 호출 없음). GUI가 멈추는
> public API는 없습니다.

---

## 2. 핵심 개념 — 이것만 알면 나머지는 옵션

| 개념 | 코드 | 뜻 |
|---|---|---|
| **JobSet** | `mgr.create_jobset(...)` → `JobSet` 핸들 | 논리적 job 묶음. 모든 기능의 기본 단위 |
| **job** | `JobRecord` | 커맨드 1건 = job 1건. jobset 안에서 `job_key`가 유일 물리 키 |
| **merge_id** | `JobRecord.merge_id` | job의 **논리 키**. merge 시 같은 merge_id의 기존 job을 교체하는 기준 |
| **user_data** | `JobRecord.user_data` | 앱이 job에 실어 두는 임의 dict. 라이브러리는 **보존만** 함 |

**역할 분리**가 API의 전부입니다:

```
명령  = mgr.*    mgr.submit(js) / mgr.kill(js) / mgr.merge(a, b) / mgr.create_jobset(...)
조회  = js.*     js.jobs() / js.summary / js.is_done          (동기 · 로컬 스냅샷)
신호  = js.*     js.jobs_updated / js.submit_finished ...     (해당 JobSet 것만)
        mgr.*    mgr.jobs_updated(jsid, ...) ...              (전 JobSet — 대시보드용)
```

핸들(JobSet)에는 **명령 메서드가 없습니다** — 명령 진입점이 `mgr` 한 곳이라
"어디를 불러야 하나"를 고민할 일이 없습니다.

### 2.1 상태

```python
class JobState(Enum):
    CREATED; SUBMITTING; RETRY_WAIT; SUBMIT_FAILED; LOST      # 라이브러리 내부 상태
    PEND; RUN; DONE; EXIT; PSUSP; USUSP; SSUSP; UNKWN; ZOMBI  # LSF native 상태
```

```
                    ┌──────────── cancel/kill(미제출) ────────────┐
                    ▼                                             │
 CREATED ──▶ SUBMITTING ──▶ PEND ──▶ RUN ──▶ DONE                 │
                │   ▲         │        │       (terminal)         │
                │   │재시도   │        ├──▶ EXIT (terminal)       │
                ▼   │         │        │     ▲ kill(optimistic)   │
             RETRY_WAIT ──────┼────────┼─────┘                    │
                │             │        └──▶ PSUSP/USUSP/SSUSP ⇄ RUN
                ▼             ▼
         SUBMIT_FAILED     LOST (조회는 전부 성공했는데 job이 안 보임)
          (terminal)        (terminal)
```

세 가지 술어가 API 전반의 판정 기준입니다:

- **`is_terminal`** = `DONE` / `EXIT` / `SUBMIT_FAILED` / `LOST` — 더 이상
  전이하지 않음. "전원 terminal"은 **모두 성공이 아니라 모두 끝남**을 뜻합니다
  (`post_process` 발화 조건).
- **`is_failed`** = `EXIT` / `SUBMIT_FAILED` / `LOST`
- **`is_inactive`** = `CREATED` **또는** terminal — submit/merge/remove 가드의
  공통 술어(terminal보다 넓습니다: CREATED는 "아직 제출 안 함"이라 terminal이
  아니지만 inactive).
- `is_on_lsf` = PEND/RUN/SUSP\*/UNKWN/ZOMBI — 폴링·kill 스냅샷의 대상.

---

## 3. 옵션 — 안 주면 기본값, 주면 그 호출에만

모든 튜닝 파라미터는 3단 계층으로 동작합니다:

```
내장 기본값  <  LsfJobManager(...) 생성 인자 (앱 전역)  <  submit(...) 인자 (이번 호출만)
```

```python
mgr = LsfJobManager()                                       # 기본값만
mgr = LsfJobManager(workers=32, max_retry=5, poll_interval_s=5)   # 앱 전역 변경
mgr.submit(js, workers=8, max_retry=0, auto_poll=False)     # 이번 submit만
```

### 3.1 옵션 카탈로그

**제출·폴링 (생성자 + `submit()` 양쪽)**

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `workers` | 32 | 병렬 submit worker 수 (1~64) |
| `max_retry` | 3 | submit 실패 재시도 (0=끔) |
| `retry_backoff` | `"fixed:2"` | `"fixed:N"`(N초 고정) / `"expo:N"`(지수) |
| `rate_limit_per_s` | 없음 | 초당 제출 상한 (LSF 부하 보호) |
| `submit_timeout_s` | 30 | 제출 1건 timeout(초) |
| `poll_interval_s` | 10 | polling 주기 (5~60) |
| `auto_poll` | True | submit 후 polling 자동 시작 |
| `verify_kill` | False | kill 후 실제 종료 확인 (`kill()` 인자로도 지정 가능) |
| `label` / `tags` / `description` | 빈 값 | JobSet 메타데이터 (`submit()` 전용) |

**앱 전역 동작 (생성자 전용)**

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `bjobs_path` / `bkill_path` | PATH 탐색 | 조회/kill 명령 경로. 토큰 목록이면 고정 인자가 앞에 붙음 |
| `chunk_size` | 500 | bjobs/bkill 한 번에 넘길 job 수 |
| `arg_max` | 131072 | 명령줄 인자 총 길이 상한 (초과 시 `ArgMaxExceededError`) |
| `lost_after_missing_polls` | 3 | bjobs에서 안 보이는 job을 **LOST로 확정하기까지** 필요한 연속 미발견 횟수. 1이면 즉시. 제출 직후 등록 지연으로 한두 사이클 안 보이는 job을 죽은 것으로 만들지 않기 위한 유예 |
| `poll_runtime_updates` | True | RUN 중 `run_time_s`(경과시간) 변화도 `jobs_updated`로 live 발행. 수만 개 규모면 False 권장 |
| `collect_clusters` | False | MultiCluster forwarding 정보 수집 — `JobRecord.source_cluster`/`forward_cluster`를 폴링으로 채움 |
| `cluster_envpaths` | 없음 | MC 분류 kill — `{클러스터명: cshrc경로}`. 지정하면 kill이 대상 cluster별로 env를 `source`한 bkill을 실행(§5.4). 와일드카드 `"*"`는 미상/미매핑의 기본 env |
| `kill_status_policy` | `"optimistic"` | `"optimistic"`=kill 수락 확인 시 즉시 EXIT / `"actual"`=실제 LSF 상태(폴링)로만 |
| `kill_max_retry` | 2 | kill 확인 실패 시 재시도 횟수 |
| `kill_retry_delay_s` | 3.0 | kill 재확인 간격(초) — `bkill`이 비동기라 확인까지 여유를 둠 |
| `progress_min_interval_s` | 0.5 | progress/`jobs_updated` 최소 발화 간격(초). 키우면 부하↓·반응성↓ |
| `progress_min_step_ratio` | 0.01 | progress 최소 진행 비율(0~1). 키우면 발화↓ |
| `min_state_dwell_s` | 0 (끔) | 상태 전이 **표시** 최소 간격(초) — 순식간에 지나가는 전이를 눈에 보이게 함(§6.5) |
| `submit_finished_on_gate_reject` | True | `pre_submit` 게이트가 False면 `submit_finished`(cancelled=N)도 발화. False면 종료 통지는 `pre_submit_finished(False)`만 |
| `test_submit_wrapper_pattern_cmd` | 없음 | wrapper 실행 프로그램 치환 — `("*_sub", "/path/to/mock_sub")` (§4.3) |

- 오타 키워드는 즉시 `TypeError`, 범위를 벗어나면 `ValueError` — 조용히
  무시되지 않습니다.
- 옵션이 많은 설정을 파일/객체로 관리하고 싶으면 `LsfConfig`를 만들어
  `LsfJobManager(config=cfg)`로 주입할 수 있습니다 (kwargs가 우선).
- **queue·자원 요구·출력 경로 같은 제출 옵션은 wrapper 커맨드 문자열에 직접
  씁니다.** 라이브러리는 `bsub` 인자를 조립하지 않으므로 그런 이름의
  키워드(`queue`/`bsub_path` 등)를 주면 경고 로그와 함께 무시됩니다.

---

## 4. 제출

### 4.1 wrapper 커맨드로 제출

실제 환경처럼 job마다 `customwrapper_sub` 같은 제출 wrapper를 쓰는 경우,
`create_jobset`에 커맨드 리스트를 그대로 넘깁니다. lsfmgr는 각 커맨드를
**그대로 실행**하고 출력의 `Job <id>`를 파싱해 **job_id 기반**으로
모니터링·kill 합니다 (`-q`/`-J`/`-g` 등 인자 조립·주입 없음).

```python
mgr = LsfJobManager()

js = mgr.create_jobset([
    "customwrapper_sub -q normal run_0.sp",           # job마다 다른 wrapper 가능
    ["customwrapper_sub", "-q", "long", "tb_1.v"],    # 문자열 또는 토큰 리스트
    "customwrapper_sub -q short run_2.sp",
])
mgr.submit(js, workers=8, max_retry=3)
```

wrapper가 지켜야 할 계약은 **두 가지뿐**입니다 — ① `bsub`의 `Job <id>` 출력을
stdout으로 그대로 통과, ② exit code 그대로 전파. bash라면 마지막에
`exec bsub "$@"` 한 줄이면 됩니다. 상세와 트러블슈팅은
[`docs/submit.md`](docs/submit.md).

### 4.2 작업 디렉토리 (`work_dir` / `work_dirs`)

제출 subprocess를 특정 디렉토리에서 실행합니다. LSF는 job 자체 `-cwd`가 없으면
bsub를 실행한 cwd를 job 실행 디렉토리로 쓰므로 wrapper·bsub 양쪽에 유효합니다.

```python
js = mgr.create_jobset(cmds, work_dir="/scratch/run")            # 전체 동일
js = mgr.create_jobset(cmds, work_dirs=["/scratch/a", None])     # job별 (None=부모 cwd)
```

두 옵션 동시 지정은 `ValueError`. 각 job의 `submit_cwd`로 저장돼 **재제출·merge에도
보존**됩니다. `os.chdir` 같은 프로세스 전역 변경을 쓰지 않으므로 동시 제출
worker 간 경합이 없습니다.

### 4.3 wrapper 실행 파일 갈아끼우기

`bjobs`/`bkill`은 `bjobs_path`로 mock을 가리킬 수 있지만, wrapper는 프로그램명이
**커맨드 문자열에 박혀 있어** 그런 노브가 없습니다. 커맨드를 하나도 고치지 않고
전 제출을 다른 실행 파일로 돌리려면 패턴 치환을 씁니다 — `argv[0]`의 basename이
glob에 맞으면 **그 프로그램만** 바뀌고 나머지 인자는 그대로입니다.

```python
mgr = LsfJobManager(
    test_submit_wrapper_pattern_cmd=("*_sub", "/path/to/mock/customwrapper_sub"))

#  "mytool_sub -q normal a.sp"
#    → 실행: /path/to/mock/customwrapper_sub -q normal a.sp
```

**끄고 켜기는 앱이 정합니다** — 라이브러리는 환경을 읽지 않습니다:

```python
kw = ({"test_submit_wrapper_pattern_cmd": ("*_sub", MOCK_SUB)}
      if os.environ.get("MY_TEST_MODE") else {})       # 변수 이름·규칙은 앱 마음
mgr = LsfJobManager(**kw)                              # 안 주면 원본 wrapper 실행
```

- 적용되면 시작 시 `lsfmgr.command` INFO 한 줄이 남습니다 — 실수로 켠 채 운영에
  제출하는 일을 로그에서 잡을 수 있습니다.
- **실행만** 바뀝니다 — `JobRecord.command`는 원본이라 표·재제출 기준이 그대로입니다.
- 대체값은 토큰 목록도 됩니다: `("*_sub", ["/path/mock_sub", "--dry-run"])`

### 4.4 제출 전 게이트 (`pre_submit`)

실제 제출 전에 **커맨드 리스트 전체를 한 번 검사/준비**하고 통과할 때만 제출하려면
`mgr.submit(js, pre_submit=...)`를 넘깁니다. 콜백은 **단일 worker 스레드**에서 1회
실행되고 `bool`을 반환합니다.

```python
def prepare(commands: list[str]) -> bool:      # 실행될 커맨드 문자열 목록
    stage_input_files(commands)                # 일괄 준비 (부수효과)
    return all_inputs_ready(commands)          # True면 제출, False면 제출 안 함

mgr.submit(js, pre_submit=prepare)
```

신호 순서는 **`pre_submit_started` → `pre_submit_finished(ok)` → (ok=True일 때만)
`submit_started` → … → `submit_finished`**. 게이트가 `False`면 제출하지 않고 job은
`CREATED`로 남습니다(기본은 `submit_finished(cancelled=N)`도 발화 —
`submit_finished_on_gate_reject=False`로 끄면 종료 통지는 `pre_submit_finished(False)`만).
콜백이 **예외**를 던져도 제출하지 않으며 — 게이트는 레코드 리셋 **이전**에 돌므로
레코드는 **원상 유지**됩니다 — `error_occurred` + `submit_finished(failed=N)`로
보고합니다.

> ⚠️ 콜백은 **worker 스레드**에서 돕니다 — Qt 위젯 등 **GUI 객체 접근 금지**.
> 재시도 시 재실행되므로 부수효과는 **멱등**이어야 합니다.

### 4.5 완료 후처리 (`post_process`)

제출한 jobset의 **전 job이 끝나면(전원 terminal)** 결과 수집·정리 등을 자동
실행합니다. 완료는 폴링(`auto_poll` 기본) 또는 `mgr.query_once(js)`로 감지되며,
감지 시점에 **단일 worker 스레드**에서 1회 실행됩니다. 성공/실패가 섞여도
(EXIT/SUBMIT_FAILED/LOST 포함) **전원 terminal이면** 실행되므로, 콜백에서 결과를
분류하면 됩니다.

```python
def collect(records) -> dict:                  # 최종 JobRecord 목록
    done = [r for r in records if r.state is JobState.DONE]
    return {"ok": len(done), "failed": len(records) - len(done)}

mgr.submit(js, post_process=collect)           # pre_submit과 함께 써도 됨
```

신호 순서는 **`post_processing_started` → `post_processing_finished(result)`**
(`result`는 콜백 반환값, 예외 시 `None` + `error_occurred`). `pre_submit` 게이트와
대칭입니다(전자는 제출 **전**, 후자는 완료 **후**).

> ⚠️ 이 콜백도 **worker 스레드** 실행 — GUI 객체 접근 금지. 한 제출당 1회만
> 발화하며, 완료 전 재제출(`post_process` 없이)하면 이전 무장은 해제됩니다.

### 4.6 완료 통지 (`jobset_finished`)

후처리 콜백 없이 **"이 jobset 다 끝났다"만** 알고 싶을 때 쓰는 신호입니다. LSF job
상태만 보고 판정하므로 `post_process` 등록 여부와 **무관**하며, 전 job이 terminal
(`DONE`/`EXIT`/`SUBMIT_FAILED`/`LOST`)이 된 순간 최종 요약과 함께 1회 발화합니다.

```python
js.jobset_finished.connect(                    # (summary dict)
    lambda s: status.setText(f"완료 — DONE {s.get('DONE', 0)}/{s['total']}"))

mgr.jobset_finished.connect(                   # 전역 계층 — (jobset_id, summary)
    lambda jsid, s: dashboard.mark_done(jsid, s))
```

- **감지 시점**은 `post_process`와 같습니다 — 폴링(`auto_poll` 기본)/`query_once`,
  그리고 제출이 전량 실패해 폴링 없이 끝난 경우는 submit 완료 지점.
- 둘 다 쓰면 순서는 **`jobset_finished` → `post_processing_started`**.
- **성공 여부와 무관** — 전원 실패로 끝나도 발화합니다("끝났다"는 신호이지
  "전부 성공"이 아님). 성공/실패는 요약이나 `js.failed_jobs`로 판단하세요.
- **내가 건 kill로 끝났으면 발화하지 않습니다.** `mgr.kill(js)`/`kill_jobs`로
  전 job이 EXIT가 된 완료는 사용자가 스스로 끝낸 것이라 알림이 필요 없습니다.
  반대로 **의도치 않은 종료**(자연 종료, LSF/관리자의 외부 `bkill`, 비정상 EXIT)는
  kill 요청을 거치지 않으므로 그대로 통지됩니다 — 알아야 하는 쪽만 옵니다.
  **부분 kill**(PEND만/선택 행)은 억제 대상이 아니라, 남은 job이 끝나면 통지됩니다.
  구분 근거는 레코드의 `rec.killed`(위 "실패 원인 표시" 참고)와 "kill이 끝낸
  완료냐"입니다 — 전원이 `killed`면 EXIT가 폴링으로 나중에 확인돼도 조용합니다.
  `post_process`는 이와 무관하게 kill로 끝나도 실행됩니다(결과 수집은 별개 계약).
- **1회**만 발화하지만, 재제출하거나 merge로 job이 늘어 다시 미완료가 되면 재무장돼
  다음 완료에 또 발화합니다. job이 하나도 없는 빈 jobset에서는 발화하지 않습니다.

---

## 5. JobSet — 모든 것의 중심

### 5.1 생성 → (필요하면 merge) → 제출

GUI는 제출 전(생성 단계)부터 jobset을 갖습니다 — `create_jobset`으로 job까지 함께
만들고, jobset 단위로 제출합니다. 같은 jobset/job_key가 전이되므로 **핸들 교체·테이블
리셋이 없습니다**.

```python
js = mgr.create_jobset(                        # 생성 시 job까지 함께 만든다
    ["customwrapper_sub -i a.sp",              #   각 커맨드 = job 1건 (CREATED)
     "customwrapper_sub -i b.sp"],
    merge_ids=["case-a", "case-b"],            # 논리 키 (merge 시 replace 기준)
    user_datas=[{"run": "...", "rev": 3}, None],   # job별 사용자 데이터 (보존만)
    label="sweep")
js.jobs_updated.connect(table.apply_changed)   # GUI 테이블(앱 코드)을 이 핸들의
                                               # Signal에 연결 (초기값은 js.jobs())

if mgr.can_submit(js):                     # 전원 비활성 + job 존재?
    mgr.submit(js, workers=8)              # **전 job** (재)제출 — 이전
                                           # DONE/EXIT도 리셋 후 재실행
```

> **job 생성은 `create_jobset` 한 곳뿐**입니다. 생성 후 job을 더 넣는 유일한 방법은
> **merge** — 별도 jobset을 만들어 `mgr.merge(js, src)`로 흡수합니다.

**재실행 패턴** (별도 resubmit API 없음): 실패/수정 job을 같은 `merge_id`로 담은
새 jobset을 만들어 흡수 → 다시 submit:

```python
fix = mgr.create_jobset(["customwrapper_sub -i a_fixed.sp"],
                        merge_ids=["case-a"], label="fix")
if mgr.can_merge(js, fix):
    mgr.merge(js, fix)         # case-a만 CREATED로 교체 (다른 결과 유지,
                               # 물리 키 유지 — 테이블 행 연속), fix 소멸
mgr.submit(js)                 # 전체 재실행
```

재제출 리셋은 이전 실행 흔적(job_id/exit_code/실행시간/fail_message/`killed`/
클러스터)을 지우고, `merge_id`/`user_data`/`submit_cwd`는 보존합니다. handler(§5.7)도 자동
재무장됩니다.

### 5.2 명령 가드 — 전부 "비활성(inactive)" 기준

| 명령 | 가드 | force |
|---|---|---|
| `mgr.submit(js)` | 전 job 비활성 + 1건 이상 (`can_submit`) | — (활성은 먼저 kill) |
| `mgr.merge(js, src)` | 양쪽 전 job 비활성 (`can_merge`) | 레코드만 강제 교체 (LSF 정리는 앱 책임) |
| `mgr.remove_job(js, ...)` / `mgr.clear_jobs(js)` | 대상 비활성 | 레코드만 강제 삭제 (〃) |
| `mgr.remove_jobset(js)` | 전원 terminal | 레코드만 강제 삭제 (〃) |
| `mgr.kill(js)` | 예외 — 활성(RUN/PEND/SUBMITTING)만 대상, 종료분은 자동 skip | — |

> 가드 위반 시 명령별 **전용 예외**가 납니다 — `SubmitNotAllowedError` /
> `MergeNotAllowedError` / `RemoveNotAllowedError` / `RemoveJobSetNotAllowedError`
> (전부 `JobSetStateError` → `LsfmgrError` 하위라 `except LsfmgrError`로도 잡힘).
> 예외 객체의 `.jobset_id` · `.job_keys`(막은 job들)로 메시지 파싱 없이 원인을
> 알 수 있습니다. 사전 확인은 `mgr.can_submit(js)` / `mgr.can_merge(a, b)`.

### 5.3 제어 (비동기 — 즉시 반환, 결과는 Signal)

```python
mgr.submit(js, workers=8)              # 전 job (재)제출 — can_submit로 선확인
mgr.cancel_submit(js)                  # 진행 중 submit 중단 (제출된 것은 유지)
mgr.kill(js)                           # 전체 kill
mgr.kill(js, only_state=JobState.PEND) # PEND만
mgr.kill(js, verify=True)              # 실제 종료까지 확인
mgr.kill_jobs(js, [job_key, ...])      # 선택 job만 kill (테이블 선택 행)
mgr.kill(js, only_state=JobState.PEND, cancel_submit=True)  # 부분 kill에도 제출 우선권
mgr.query_once(js)                     # 지금 즉시 1회 조회 요청
mgr.stop_polling(js); mgr.start_polling(js, 30)
mgr.remove_jobset(js)                  # jobset 자체 삭제 (전원 terminal일 때)
```

**kill은 진행 중 submit에 우선권**을 갖습니다 — 전체 kill이면 진행 중 제출을 취소하고
재시도를 포기시킨 뒤, barrier로 "kill을 빠져나가는 늦은 제출"을 구조적으로 막습니다.
부분/선택 kill은 기본적으로 제출을 건드리지 않고, `cancel_submit=True`로 같은
우선권을 켤 수 있습니다. 내부 도식은 [`docs/flows.md`](docs/flows.md).

> **kill 상태 정책** (`kill_status_policy`):
> `bkill`은 비동기라 `Job <id> is being terminated`(요청 수락)와 실제 종료 사이에
> 시차가 있습니다.
> - **`"optimistic"`(기본)** — 수락 확인 시 **즉시 EXIT로 간주**하고
>   `jobs_updated`/`jobset_updated`로 바로 반영. 이후 폴링은 이 job을 조회하지
>   않습니다(EXIT는 terminal). `KillReport.changed`에 전이된 job이 담깁니다.
> - **`"actual"`** — 수락 확인만으론 상태를 안 바꾸고, **실제 LSF 상태**
>   (`verify=True` 또는 폴링)로만 EXIT를 반영. 정확하지만 반영이 한 박자 늦습니다.

### 5.4 MultiCluster kill — 자동 분류 (`cluster_envpaths`)

forward된 job은 로컬 `bkill`로 안 죽고 그 클러스터 env를 source해야 죽는 환경에서,
**매니저 생성 시** `{클러스터명: cshrc경로}` 매핑 하나만 주면 이후 모든 kill을
라이브러리가 알아서 처리합니다(앱 환경 속성이라 호출마다 주지 않습니다):

```python
mgr = LsfJobManager(collect_clusters=True,
                    cluster_envpaths={"cluster_a": "/env/a.cshrc",
                                      "cluster_b": "/env/b.cshrc"})
mgr.kill(js)              # 이후 kill/kill_jobs 전부 자동 분류
```

동작:
1. 대상 중 cluster 미상(제출 직후 — 첫 폴링 전)이 있으면 **bkill 전에** 그 id들만
   **최소 포맷(`jobid source_cluster forward_cluster`)으로 1회 조회**해 채웁니다
   (jobset 컨텍스트 유무와 무관하게 동일).
2. 관측 cluster(forward 우선, 없으면 source)별로 그 env를 `source`한
   bkill(`tcsh -c "source ... && exec bkill ..."`)로 나눠 실행합니다.
3. 그래도 미상이면 **기본 env**로 죽입니다 — 기본 env는 매핑의 와일드카드 키 `"*"`,
   없으면 env source 없이 실행합니다(로그에 `env=None` — 앱 프로세스의 환경을
   그대로 씁니다).

**제출 직후 즉시 kill해도 cluster를 알고 죽는다**는 게 핵심입니다.
cluster 관측은 `collect_clusters=True`가 전제입니다.

> GUI 프로세스 환경은 대화형 셸에서 rc를 source한 환경과 다를 수 있습니다.
> 그 경우 `{"*": cshrc}`로 기본 env를 명시하세요. 분류 없이 **전부 한 env로**
> 돌리려면 `cluster_envpaths={"*": "<cshrc>"}` 하나만 주면 됩니다.

### 5.5 조회 (동기 — 로컬 스냅샷, LSF 호출 없음)

```python
js.summary                 # 요약 dict
js.is_done                 # 전원 terminal?
js.is_active               # 하나라도 안 끝난(non-terminal) job이 있으면 True
js.is_inactive             # 전원 terminal이면 True (빈 JobSet도 True)
js.failed_jobs             # SUBMIT_FAILED/EXIT/LOST 목록
js.jobs()                  # 전체 JobRecord
js.jobs(states={JobState.RUN})
js.id                      # jobset_id 문자열 (로그/저장용)

mgr.detect_lost(js)        # 손실 감지 — ID 미확보 SUBMITTING을 LOST 확정
mgr.total_summary()        # 전 JobSet 상태 카운트 합산
mgr.search_jobsets(tag="nightly")
mgr.jobset(jobset_id)      # ID로 핸들 재획득
```

요약 dict 예:
```python
{"total": 5000, "RUN": 2100, "PEND": 2800, "DONE": 80, "EXIT": 12,
 "SUBMIT_FAILED": 5, "RETRY_WAIT": 2, "LOST": 1}
# 불변식: 상태 합계 == total (손실 job도 반드시 어딘가에 집계됨)
```

> **`is_active` / `is_inactive`** — 이 JobSet을 다시 수행할지 판단할 때 씁니다.
> `is_done`과 거의 같지만 job이 하나도 없는 빈 JobSet은 `is_inactive=True`
> (`is_done`은 False)로 다릅니다.

> **대량 제출/kill을 백그라운드로 돌리기** — `submit()`/`kill()`은 **즉시 반환**하고
> 실제 작업은 worker에서 돕니다. 그래서 진행 dialog를 **modeless로 띄우거나 아예
> 닫고 딴 작업**을 해도 됩니다 — 작업의 소유자는 매니저지 dialog가 아니라서,
> `js` 핸들만 들고 있으면 계속 진행됩니다. 나중에 상태 패널을 다시 열 때는
> 그동안 놓친 Signal 대신 **pull로 현재 진행을 조회**합니다:
> ```python
> if js.is_submitting:               # 아직 제출 중?
>     s = js.submit_state            # SubmitProgress | None
>     bar.setValue(int(s.fraction * 100))
>     label.setText(f"{s.done}/{s.total} (성공 {s.succeeded} / 실패 {s.failed})")
> if js.is_killing:                  # kill도 대칭
>     s = js.kill_state              # KillProgress(done/total) | None
> ```
> 진행 중이 아니면 `None`이고, 완료 후 최종 결과는 `submit_finished(SubmitReport)` /
> `kill_finished(KillReport)` 또는 `js.summary`로 봅니다. pull은 throttle과 무관하게
> 항상 최신값입니다.

> **실패 원인 표시** — 두 경로로 확인합니다.
> - **SUBMIT_FAILED / RETRY_WAIT**: `rec.fail_message`에 wrapper/bsub 실행의
>   stderr/stdout(터미널에서 봤을 메시지)이 자동 저장됩니다. 재시도 성공이나
>   재제출 시 자동으로 지워집니다. `rec.fail_reason`은 분류 코드
>   (`BSUB_EXIT_<rc>` / `NO_JOBID_PARSED` / `BSUB_TIMEOUT` / `BSUB_OSERROR`).
> - **EXIT**: LSF 이력을 따로 조회하지 않습니다(폴링 오버헤드 0). 레코드 필드
>   (`exit_code` / `run_time_s` / `submit_cwd` / `start_time` / `finish_time`)로
>   보여 주면 됩니다 — 전부 로컬 스냅샷이라 LSF 호출이 0입니다.
> - **"내가 죽인 EXIT"인지**는 `rec.killed`로 구분합니다. `mgr.kill()`/`kill_jobs()`가
>   bkill 수용을 확인한 job에만 `True`이고, 자연 종료·외부 `bkill`(관리자/다른
>   세션)·비정상 EXIT은 `False`로 남습니다. `exit_code`(130/137/143)로는 구분되지
>   않습니다 — 외부 kill도 같은 코드를 남기니까요. 실패 목록을 보여줄 때
>   `js.failed_jobs`에서 `r.killed`를 걸러내면 "의도한 정지"와 "진짜 실패"가
>   나뉩니다. 재제출 리셋에서 `False`로 돌아갑니다.

> 조회 값은 **마지막 polling 시점 스냅샷**입니다 (최대 `poll_interval_s` 지연).
> 단 `SUBMIT_FAILED`는 submit 과정에서 직접 기록되므로 항상 정확합니다.
> 지금 즉시 최신이 필요하면 `mgr.query_once(js)` 후 `jobset_updated`에서 읽으세요.

### 5.6 그 밖의 명령

```python
mgr.merge(js_a, js_b)                  # b를 a에 흡수(merge_id 규칙) — b 소멸
mgr.can_merge(js_a, js_b)              # 흡수 가능 여부 (전원 비활성)
mgr.remove_job(js, merge_id="m1")      # 삭제 — job_id/merge_id/job_key 기준
mgr.remove_job(js, job_id=12345, force=True)  # 활성이면 force 필요 (레코드만)
mgr.clear_jobs(js)                     # job만 전부 삭제 — jobset은 남아 재사용 가능
mgr.remove_jobset(js)                  # jobset 자체 삭제 — 목록에서도 사라짐
mgr.set_user_data(js, "m1", {"note": "..."})  # 사용자 데이터 교체
mgr.shutdown()                         # 스레드 정리 (멱등 — 앱 종료 시 자동 호출)
```

> **삭제 3형제 — 이름에 대상이 드러납니다.**
>
> | 명령 | 지우는 것 | 남는 것 |
> |---|---|---|
> | `remove_job(js, ...)` | job 1건 | 나머지 job + jobset |
> | `clear_jobs(js)` | job 전부 | **jobset** — id·label·tags·handler·폴링·GUI 행이 그대로. 새 batch를 `merge`로 흡수시키면 그대로 **재사용** |
> | `remove_jobset(js)` | jobset 자체 | 없음 — `list_jobsets`/`search_jobsets`/`get_jobs` 어디에도 안 남음 |
>
> `remove_jobset`은 merge source와 똑같이 레코드를 실제로 지웁니다. **결과를
> 나중에 다시 볼 수 없으니** 필요하면 삭제 전에 스냅샷을 뜨세요 — 반환값이
> 삭제 직전의 `JobSetRecord`입니다. 삭제된 jobset의 핸들을 만지면
> `JobSetRemovedError`, id로 접근하면 `JobSetNotFoundError`입니다.
>
> ⚠️ **`force=True`로 살아있는 job을 지우면 그 `job_id`는 조회로 못 찾습니다.**
> 계약상 LSF job 정리는 앱 책임인데 레코드가 사라져 조회할 방법이 없으므로,
> 삭제 직전에 `lsfmgr.jobset` 로거가 해당 `job_id`들을 WARNING으로 남깁니다 —
> 그게 유일한 흔적입니다. 제출이 진행 중이었다면 그 사이 확보된 `job_id`는
> `lsfmgr.submit` 로거에 남습니다. 정리할 생각이면 **먼저 `mgr.kill(js)`로
> 정리한 뒤 삭제**하는 편이 안전합니다.

### 5.7 job별 handler — 폴링 사이클마다 실행

JobSet에 **이름 있는 handler**를 붙이면, 각 job이 지정한 state 구간에 있는 동안
**폴링 사이클마다**(= bjobs 갱신 직후) **worker 스레드에서** 실행됩니다.
별도 주기가 없어 `poll_interval_s`에 tie되고, `ctx.record`는 항상 최신 상태입니다.

```python
def collect(ctx):                          # worker 스레드 — GUI 안 막음
    # ctx.job_id / ctx.submit_cwd(작업 디렉토리) / ctx.record / ctx.final
    cwd = ctx.submit_cwd or os.getcwd()    # 미지정 job은 None → 부모 프로세스 cwd
    return parse_outputs(cwd)              # 반환값이 Signal로 전달됨

js.handler_finished.connect(
    lambda name, res: print(name, res.job_key, res.data, res.final))

mgr.add_handler(js, "collect", collect,
                start_states={JobState.RUN},                # RUN이 되면 시작 (기본)
                end_states={JobState.DONE, JobState.EXIT})  # 종료 시 최종 1회 (기본)
mgr.remove_handler(js, "collect")          # 해제
```

- `handler_finished`는 **1회 실행이 끝날 때마다** job별로 옵니다 — 최종 실행은
  `res.final`로 구분. 예외는 `res.error`에 담겨 옵니다(다른 job에 영향 없음).
- **폴링이 돌고 있어야 동작**합니다(auto_poll 기본이면 자동). 첫 실행은 다음 폴링
  사이클이며, `mgr.query_once(js)`로 즉시 1회 유도 가능합니다.
- `mgr.submit(js)`로 전체 재실행하면 진행 상태가 자동 재무장되어 새 실행에서 다시 돕니다.
- 상세 규칙은 [`docs/submit.md`](docs/submit.md), 실행 예제는
  `examples/gui_demo.py`(handler 체크박스).

---

## 6. Signal

### 6.1 카탈로그

`js.*`는 **이 JobSet의 이벤트만** 오므로 필터링이 필요 없습니다. `mgr.*`는 같은
이벤트를 첫 인자 `jobset_id`와 함께 전역으로 발행합니다(다중 JobSet 대시보드용) —
이름은 동일하고 인자에서 `jsid`만 빠집니다.

| Signal | `js.*` 인자 | 시점 |
|---|---|---|
| `jobset_updated` | `dict` 요약 | **submit 완료 시(초기 PEND)** + polling/query_once 후 |
| `jobs_updated` | `list[JobRecord]` | 상태 **변경분** 배치 — 테이블 행 갱신용 |
| `jobs_failed` | `list[JobRecord]` | SUBMIT_FAILED/EXIT/LOST 변경분 (파생 Signal — `mgr.*`엔 대응 없음) |
| `submit_started` | — | 제출 착수 (`pre_submit` 지정 시 게이트 통과 후) |
| `submit_progress` | `(done, total)` | submit 진행 (throttled) |
| `submit_finished` | `SubmitReport` | submit 완료 (retry 포함 최종) |
| `pre_submit_started` / `pre_submit_finished` | — / `bool` | `pre_submit` 게이트 시작/종료 |
| `jobset_finished` | `dict` 최종 요약 | 이 JobSet의 **전 job이 terminal** 도달 (등록물 무관, 1회) |
| `post_processing_started` / `post_processing_finished` | — / `object` | 전원 terminal 후처리 시작/완료 |
| `kill_started` | — | kill 접수 즉시(동기) — 정지 대기로 완료가 늦어져도 UI가 바로 표시 |
| `kill_progress` | `(done, total)` | chunk kill 진행 (throttled, 마지막 100%) |
| `kill_finished` | `KillReport` | kill 완료 |
| `handler_finished` | `(name, HandlerResult)` | 등록한 handler 1회 실행 완료마다 |
| `job_lost` | `JobRecord` | LOST 확정 시 (`mgr.job_lost(jsid, rec)` — 전역 계층에만 있음) |
| `error_occurred` | `str` | worker 예외 등 |

배선 예제·발화 주기(cadence)·진행 표시 패턴은 [`docs/gui.md`](docs/gui.md).

### 6.2 명령 → Signal 타임라인 (무엇이 언제 오나)

사용자 명령에 대한 신호는 아래 순서가 **보장**됩니다. 라이브러리가 store를 먼저
갱신한 뒤 신호를 쏘므로(store-first), 어떤 slot에서든 `js.jobs()`를 pull하면 신호
내용과 일치하는 상태를 봅니다.

```
submit (mgr.submit(js) — 재제출 포함):
  (pre_submit_started → pre_submit_finished)  # pre_submit 게이트 지정 시
  → submit_started                            # 제출 착수
  → jobs_updated([전원 SUBMITTING])           # 표가 즉시 채워짐
  → submit_progress + jobs_updated(변경분)     # 스로틀 배치 (0.5s 또는 1%)
  → submit_finished(SubmitReport)             # 반드시 마지막 배치 뒤에 도착
  → jobset_updated(최종 요약)
  ⋯ (이후 폴링으로 전원 terminal 도달 시)
  → jobset_finished(최종 요약)                # 항상 — 등록물과 무관
  → post_processing_started → post_processing_finished(result)   # post_process 지정 시

kill:
  → kill_started                     # 접수 즉시(동기) — 스피너 켜는 지점
  → jobs_updated([CREATED 복귀 배치]) # 진행 중 submit이 있었으면 (취소분)
  → kill_progress                    # chunk 진행 (스로틀)
  → kill_finished(KillReport)        # 완료
  → jobs_updated([EXIT 전원 배치])    # 기본(optimistic) — 폴링 안 기다림
  → jobset_updated(요약)

polling(자동):
  → jobset_updated(요약) + jobs_updated(변경분만)   # jobset당 주기당 1회
  → job_lost                                       # LOST 확정 시에만
```

> `min_state_dwell_s`(§6.5)를 켜면 **`jobs_updated`만** 이 타임라인에서 최대
> dwell만큼 뒤로 밀립니다 — 그 신호에 한해 store-first/finished-last가
> 느슨해집니다. 기본값(0)이면 위 순서 그대로입니다.

### 6.3 위젯별 권장 신호

| 위젯 | 연결할 Signal | 이유 |
|---|---|---|
| 요약 배지/카운터 | `jobset_updated(summary)` | dict 하나로 전 상태 카운트 — 표 순회 불필요 |
| job 테이블 | `jobs_updated([JobRecord])` | **변경분만 옴** — 전체 리로드 말고 해당 행만 갱신 |
| 진행 바 | `submit_progress` / `kill_progress` | 이미 스로틀됨 — 그대로 바인딩 |
| "실행 중" 스피너 | `kill_started`·`submit_started` 켜고 `*_finished` 끄기 | kill은 정지 대기로 완료가 늦을 수 있어 착수 신호가 따로 있음 |
| 실패 알림 | `jobs_failed` / `job_lost` / `error_occurred` | 실패 계열만 구독 |

```python
js.jobs_updated.connect(table.apply_changed)     # 변경 행만 반영
js.jobset_updated.connect(badge.set_counts)
js.kill_started.connect(lambda: spinner.start("killing..."))
js.kill_finished.connect(lambda rep: spinner.stop())
```

### 6.4 성능 — 신호가 GUI를 버벅이게 하지 않으려면

- **빈도는 라이브러리가 제한**합니다: progress·변경분 배치는 0.5초 간격 또는
  진행률 1% 변화 시에만 발화(마지막 100%는 항상). 10,000개 submit도 초당 최대
  ~2회 배치입니다.
- **`jobs_updated`는 전체가 아니라 변경분**입니다 — 테이블 전체 리셋 대신
  `rec.job_key`로 해당 행만 갱신하세요 (`QAbstractTableModel`이면 해당 행
  `dataChanged`만).
- **RUN 수천 개 이상을 다루면 `poll_runtime_updates=False`** 권장 — 기본값(True)은
  실행 경과시간을 매 폴링 갱신하므로 RUN 전원이 매 주기 변경분 배치에 실립니다.
- `kill_status_policy`는 기본(`"optimistic"`)을 유지하세요 — kill 확인 즉시 EXIT가
  반영됩니다. `"actual"`이면 다음 폴링(기본 10초)까지 PEND/RUN으로 보입니다.

### 6.5 상태 전환을 눈에 보이게 — `min_state_dwell_s`

`SUBMITTING`→`PEND`는 bsub 왕복(수백 ms)만큼만, 재제출의 `EXIT`→`SUBMITTING`은
거의 0초 만에 지나갑니다. 표에서는 중간 상태가 깜빡이고 최종 상태만 남죠.
`min_state_dwell_s`를 켜면 job별로 한 상태가 그 시간만큼 머문 뒤에야 다음 전이가
`jobs_updated`로 나갑니다. **전이는 버리지 않고 순서대로** 밀립니다:

```python
mgr = LsfJobManager(min_state_dwell_s=1.0)   # 0(기본)이면 끔
```

```
mgr.kill(js) → mgr.submit(js)     # dwell=1.0
  표: EXIT ──1s──> SUBMITTING ──1s──> PEND       # 각 상태가 1초씩 보인다
```

**표시만** 늦추는 기능이라, 켜는 순간 `jobs_updated`에 한해 §6.2의 두 계약이
느슨해집니다 (다른 신호·store·라이브러리 내부 판정은 영향 없음):

- **store-first 아님** — 지연된 `jobs_updated` slot에서 `js.jobs()`를 pull하면
  신호보다 **앞선** 상태가 보입니다. slot에서 pull로 표를 다시 그리면 이 기능이
  무효가 되니, 신호로 받은 `records`만 반영하세요.
- **finished-last 아님** — `submit_finished`/`kill_finished`가 마지막 전이 배치보다
  먼저 도착할 수 있습니다. 스피너는 예정대로 꺼지고 표만 1초쯤 뒤따릅니다.
- 요약(`jobset_updated`)은 늦추지 않습니다 — dwell 동안 배지 카운트가 표보다
  앞섭니다. 배지도 함께 늦추려면 배지를 `jobs_updated`로 직접 집계하세요.
- 표시가 store보다 최대 (밀린 전이 수 × dwell)만큼 늦으므로 1초 안팎을 권장합니다.
  대량 제출이어도 같은 tick의 보류분은 jobset당 한 배치로 합쳐 발화되어 신호 수는
  늘지 않습니다.

---

## 7. GUI 통합 규칙

1. **slot은 main 스레드에서 실행** — Signal은 자동 queued connection이므로 slot에서
   바로 위젯 갱신 OK.
2. **콜백(`pre_submit`/`post_process`/handler)은 worker 스레드** — 위젯 접근 금지.
   값만 반환하고 UI 반영은 Signal slot에서.
3. **Signal로 받은 객체는 불변(frozen)** — 수정하지 말고 JobSet API를 쓰세요.
4. **shutdown은 자동** — `QApplication.aboutToQuit`에 자동 연결됩니다. 명시적으로
   부르고 싶으면 `mgr.shutdown()` (멱등, 중복 안전).
5. **대량 갱신은 batch** — `jobs_updated`/`jobset_updated`는 변경분/요약 단위로
   오므로 모델 뷰에 배치 반영하세요.
6. 바인딩 강제: `QT_API=pyside6` (pyqt5/pyside2/pyqt6/pyside6) 환경변수를 Qt import
   전에 설정. 미설정 시 앱이 import한 바인딩 자동 감지.

### 하지 말아야 할 것

- 결과를 기다리며 busy-wait / `processEvents()` 루프 → Signal을 기다리세요.
- Signal로 받은 JobRecord 수정 → frozen이라 예외.
- `PyQt5`/`PySide6` 직접 import를 lsfmgr와 혼용 → qtpy 감지가 꼬일 수 있음.
- `js.jobs()`를 타이트 루프에서 반복 호출 → 스냅샷은 polling 주기로만 갱신되므로
  의미 없음. `jobset_updated` Signal 기반으로 반응하세요.
- `kill_finished`로 상태를 **수동 추론**해 표를 EXIT로 칠하기 → 폴링이 준 실제
  상태와 충돌해 깜빡입니다. 표는 `jobs_updated`로만 그리세요.
- `submit_finished`/`kill_finished` 핸들러 **안에서** 진행 스냅샷 pull
  (`submit_state`/`kill_state`) 호출 → 완료 시점이라 항상 None인 데다, 같은 스레드
  재획득 경합의 소지가 있음. 최종값은 핸들러 인자(`SubmitReport`/`KillReport`)에
  이미 담겨 있으니 그걸 쓰세요.

---

## 8. 로깅 / 예외 수집

라이브러리 이벤트는 `lsfmgr.*` logger 계층으로 나갑니다:

```python
logger = logging.getLogger("lsfmgr")
logger.setLevel(logging.INFO)          # DEBUG면 LSF 명령 원문까지
logger.addHandler(my_file_handler)     # %(threadName)s 포함 포맷 권장
```

레벨 규약: DEBUG=LSF 명령/stdout/stderr 원문, INFO=submit/kill 착수·완료·상태 전이,
WARNING=retry·조회 포맷 강등·LOST 판단 보류, ERROR=SUBMIT_FAILED/LOST 확정·worker
예외(traceback).

**명령별 로거** — INFO만 켜도 로거별로 흐름이 추적됩니다:

| 명령 | INFO 로거 | 착수 | 완료 |
|---|---|---|---|
| submit | `lsfmgr.submit` | `submit 착수 <jsid>: N건` | `submit 완료 …` |
| kill | `lsfmgr.kill` | `kill 착수 <jsid> (전체/only/ids)` | `kill 완료 …: 요청/확인/미확인/잔존` |
| polling | `lsfmgr.monitor` | (정규 사이클은 DEBUG — 신호로 통지) | 자동 중지 시 INFO |

`lsfmgr.command`만 DEBUG로 올리면 모든 LSF subprocess의 원시 argv·cwd·소요시간·rc·
stdout/stderr을 볼 수 있습니다 (wrapper의 `Job <id>` 파싱 문제 진단은 이 DEBUG로).

worker 예외는 스레드를 죽이지 않고 로그 + `js.error_occurred` Signal로 전달됩니다.
앱 쪽 slot 예외까지 완전 수집하려면 `sys.excepthook`, `threading.excepthook`,
`qInstallMessageHandler` 훅킹을 권장합니다 — 상세는
[`docs/logging.md`](docs/logging.md).

---

## 9. MockLSF — 실제 LSF 없이 테스트하기

실제 LSF 서버가 없는 환경에서도 개발·테스트할 수 있도록, `bsub`/`bjobs`/`bkill`
등을 흉내내는 가상 스케줄러 `mocklsf` 패키지가 함께 들어 있습니다. 표준 라이브러리만
사용하며 별도 의존성이 없습니다.

```bash
export PATH="$PWD/bin:$PATH"
mocklsfd start                 # 가상 스케줄러 데몬 기동 (bsub 최초 호출 시 자동 기동도 됨)

bsub -q normal -J myjob sleep 30
bjobs                          # PEND→RUN→DONE/EXIT 상태 전이를 시간에 따라 재현
```

- 상태는 SQLite(`$MOCKLSF_HOME/state.db`)에 저장되어 각 명령이 독립 프로세스로
  실행돼도 상태를 공유합니다(앱이 명령을 subprocess로 호출하는 구조와 일치).
- 큐·타이밍·실패율·MultiCluster forwarding 등은 환경변수(`MOCKLSF_*`)로 조정합니다.
- 제출 wrapper 데모 `bin/customwrapper_sub`가 함께 들어 있습니다 — 받은 인자를
  그대로 `bin/bsub`에 넘기고 출력을 손대지 않고 통과시키는 최소 wrapper입니다.

```bash
customwrapper_sub -q normal run1.sp   # == bsub -q normal run1.sp → "Job <id> ..."
```

상세는 [`docs/mocklsf.md`](docs/mocklsf.md), 실행 가능한 통합 데모는
[`examples/gui_demo.py`](examples/gui_demo.py)를 참고하세요.
