# LSF Job Manager — Requirements Specification

> **형태**: Qt 전용 Python 라이브러리 — **qtpy** 기반, PyQt5 / PySide2 / PyQt6 /
> PySide6 호환
> **환경**: Linux, NFS 다중 사용자(~300명), LSF cluster, 폐쇄망

이 문서는 구현의 **계약(무엇을 보장하는가)** 을 정의한다. 사용법은
[README](../README.md), 제출 계약은 [submit.md](submit.md), 내부 흐름은
[flows.md](flows.md) 참고.

---

## 0. 목적 및 범위

Qt GUI 애플리케이션에서 LSF cluster로 대량 시뮬레이션 job을 submit / monitoring /
kill 하는 라이브러리를 구현한다. **job 제어(무엇을 언제 제출·재실행·삭제할지)는
GUI 앱이 직접 갖고**, 라이브러리는 그 결정을 실행하는 CRUD + submit + kill + poll 만
제공한다.

핵심 문제:
1. 수천 개 job의 submit/kill/조회 시 LSF master 부하와 ARG_MAX 제한
2. submit 실패·ID 파싱 실패로 인한 job 손실 추적
3. 대량 job의 논리적 묶음(JobSet) 단위 관리
4. **GUI freeze 방지** — 모든 LSF 호출은 백그라운드, 통지는 Signal
5. **간결·단일 사용성** — 명령 진입점이 한 곳(`mgr.*`)이라 "어디를 불러야 하나"
   고민이 없다. 세부 옵션은 필요할 때만.

### 0.1 Qt 바인딩 호환 (필수)

- 모든 Qt import는 `qtpy` 경유만 허용 (내부는 `lsfmgr/qt.py` 단일 지점):
  ```python
  from qtpy.QtCore import QObject, QThread, QTimer, Signal, QThreadPool, QRunnable
  ```
- 지원: PyQt5, PySide2, PyQt6, PySide6 (`QT_API` 자동 감지)
- 바인딩별 차이는 qtpy shim, 불가피 시 compat 한 곳에서만 분기

---

## 1. API 구조

**명령은 전부 `mgr.*` 한 곳**이고, **JobSet 핸들은 조회(pull) + Signal 전용 뷰**다.

```
명령 (전부 async→Signal)   mgr.submit(js) / mgr.kill(js) / mgr.add_jobs(js,…) / ...
                          인자는 JobSet 핸들 또는 jobset_id 문자열 (_jsid 정규화)
조회 (전부 sync, snapshot)  js.jobs() / js.summary / js.is_* / mgr.get_jobs(js) ...
Signal                    js.<signal> (해당 JobSet만) 또는 mgr.<signal>(jsid, ...) (전역)
```

### 1.1 기본 흐름 — 생성 → (필요 시 편집) → submit

```python
mgr = LsfJobManager()

# job 생성은 create_jobset 한 곳 — 생성 시 job까지 함께 만든다
js = mgr.create_jobset(
    ["customwrapper_sub -i a.sp", "customwrapper_sub -i b.sp"],
    job_keys=["case-a", "case-b"],            # 논리 키 (교체 대상 기준)
    user_datas=[{"rev": 3}, None],             # job별 사용자 데이터 (보존만)
    label="sweep")

js.jobset_updated.connect(lambda s: ...)       # 이 핸들의 Signal에 GUI 바인딩

if mgr.can_submit(js):
    mgr.submit(js, workers=8)                  # 전 job (재)제출
```

- **생성 후 job 목록 편집은 3형제** — `add_jobs`(추가) / `replace_jobs`(교체) /
  `upsert_jobs`(있으면 교체, 없으면 추가). 인자는 `create_jobset`과 같은 모양이고,
  교체는 같은 job_key 자리를 갈아끼워 테이블 행이 이어진다.
  **job_keys는 필수**다 — 라이브러리가 이름을 대신 짓지 않는다(그 job을 나중에
  가리킬 수단이 이 키뿐이라, 자동 생성하면 앱이 자기 job을 못 찾는다).
- 라이프사이클 자동화:
  - **AUTO-1**: `submit()` 시 polling 자동 시작 (`auto_poll=False`로 해제)
  - **AUTO-2**: JobSet 전원 terminal(또는 활동 없음 2사이클) 도달 시 polling 자동 중지
  - **AUTO-3**: `LsfJobManager` 생성 시 `QApplication.aboutToQuit`에 `shutdown()`
    자동 연결 (명시 호출도 가능, 멱등)

### 1.2 옵션 처리 원칙 — 3단 계층

모든 튜닝 파라미터는 **안 주면 기본값, 주면 그 호출에만 적용**:

```
① 내장 기본값  <  ② LsfJobManager(...) 생성 인자 (앱 전역)  <  ③ submit(...) 인자 (이번 호출)
```

전체 카탈로그는 [README §3.1](../README.md). 계약은 다음 네 가지다:

- **OPT-1** 옵션 해석은 `resolve_options(defaults, call_kwargs) -> Options` 한 함수로 일원화 (호출 지점은 submit 하나 — kill은 verify 인자로만 받는다)
  (defaults → manager → call 순 merge, frozen dataclass 반환).
- **OPT-2** 알 수 없는 키워드는 즉시 `TypeError` (오타 조기 발견).
- **OPT-3** 범위 검증 (workers 1~64 등) 위반 시 `ValueError`.
- **OPT-4** `LsfConfig` 객체 주입도 지원 (`LsfJobManager(config=cfg)`) — kwargs 우선.

> 제출 옵션(queue·자원 요구·출력 경로)은 라이브러리가 `bsub` 인자를 조립하지 않으므로
> **wrapper 커맨드 문자열에 직접 쓴다**. 그런 이름의 키워드를 주면 경고 후 무시된다.

### 1.3 명령 API (전부 `mgr.*`, 인자는 핸들 또는 jobset_id)

```python
# --- 생성/구성 (sync) ---
mgr.create_jobset(commands=(), *, job_keys=None, user_datas=None,
                  work_dir=None, work_dirs=None,
                  label="", tags=(), intended_count=0) -> JobSet
mgr.add_jobs(js, commands, *, job_keys=None, user_datas=None,
             work_dir=None, work_dirs=None) -> list[JobRecord]
mgr.replace_jobs(js, commands, *, job_keys, ..., force=False) -> list[JobRecord]
mgr.upsert_jobs(js, commands, *, job_keys, ..., force=False) -> list[JobRecord]
mgr.remove_jobs(js, refs, *, force=False)       # refs = [job_key | job_id, ...]
mgr.clear_jobs(js, *, force=False)               # job만 전부 삭제 (jobset은 남음)
mgr.set_user_data(js, ref, user_data)            # ref = job_key | job_key | job_id
mgr.can_submit(js, *, only=None) -> bool
mgr.remove_jobset(js, *, force=False)            # jobset 자체 삭제 (전원 terminal)

# --- 실행 (async→Signal) ---
mgr.submit(js, *, only=None, pre_submit=None, post_process=None, **opts) -> JobSet
                                                 # only=None이면 전 job (재)제출
mgr.kill(js, *, only_state=None, verify=None)
mgr.kill_jobs(js, job_keys, *, verify=None)  # 선택 job만
mgr.kill_jobs(job_keys=[...])                # 핸들 없이 key만으로 (jobset 자동 추적)
mgr.cancel_submit(js)                        # 진행 중 submit 중단
mgr.start_polling(js, interval_s=None); mgr.stop_polling(js)
mgr.query_once(js)                           # 1회 강제 조회

# --- handler (FR-7) ---
mgr.add_handler(js, name, fn, *, start_states=None, end_states=None)
mgr.remove_handler(js, name)

# --- 조회 (sync, snapshot) ---
mgr.jobset(jobset_id) -> JobSet              # ID로 핸들 재획득
mgr.summary(js); mgr.total_summary(); mgr.get_jobs(js, states=None)
mgr.list_jobsets(); mgr.search_jobsets(...); mgr.detect_lost(js)
mgr.is_submitting(js); mgr.submit_state(js)
mgr.is_killing(js);  mgr.kill_state(js)
mgr.shutdown()
```

### 1.4 JobSet 핸들 — 조회 + Signal 전용 뷰 (QObject)

```python
class JobSet(QObject):
    # 이 JobSet 전용 Signal (이름은 mgr.* Signal과 동일, jsid 인자만 없음)
    jobset_updated  = Signal(dict)     # 요약 {"total":.., "RUN":.., ...}
    jobs_updated    = Signal(list)     # 변경분 [JobRecord]
    submit_progress = Signal(int, int) # (done, total), throttled
    submit_finished = Signal(object)   # SubmitReport
    jobs_failed     = Signal(list)     # SUBMIT_FAILED/EXIT/LOST 변경분 (파생)
    kill_started    = Signal()         # kill 접수 즉시(동기) — 착수 피드백
    kill_progress   = Signal(int, int) # (done, total)
    kill_finished   = Signal(object)   # KillReport
    handler_finished= Signal(str, object)      # name, HandlerResult
    pre_submit_started/pre_submit_finished        # pre_submit 게이트 (FR-9)
    jobset_finished = Signal(dict)     # 전 job terminal 도달, 최종 요약 (FR-11)
    post_processing_started/post_processing_finished      # post_process (FR-10)
    error_occurred  = Signal(str)

    # 조회 (전부 sync — Store 스냅샷, LSF 호출 없음)
    id; summary; is_done; is_active; is_inactive; is_submitting; submit_state
    is_killing; kill_state; failed_jobs
    def jobs(self, states=None) -> list[JobRecord]: ...
```

- **핸들에 명령 메서드는 없다** — kill/add_jobs/submit 등은 전부 `mgr.*`.
- JobSet 재획득: `mgr.jobset(jobset_id)`. 파괴된 핸들 접근 시 `JobSetRemovedError`.

---

## 2. 용어 정의 — 혼동 방지 필수

**코드에서 bare "group" 사용 금지.**

| 용어 | 코드 명칭 | 정의 |
|---|---|---|
| **JobSet** | `jobset_id`, `JobSet` 객체 | 논리적 job 묶음. 모든 기능의 기본 단위 |
| **job_key** | `JobRecord.job_key` | jobset 내 유일한 job의 키. **앱이 정한다(필수)**. 재제출·교체에도 유지 — 교체 대상·ref·표 행의 정체성 |
| **user_data** | `JobRecord.user_data` | 사용자 정의 dict(JSON-able). 라이브러리는 **보존만** |
| **Array Job** | `array_index` | wrapper 제출 산물로만 존재하는 element(라이브러리가 array를 직접 제출하지는 않는다) |

관계 규칙:
- JobSet이 유일한 논리 단위이고, LSF 쪽 추적 수단은 **job_id 하나뿐**이다
  (group/name 부착물을 만들지 않는다).
- **job_key**는 jobset 내 유일. 교체는 같은 키 자리를 갈아끼우므로 테이블 행이
  이어진다. LSF에 부착되지 않는 순수 내부 키라 형식 제약은 없다.
  (구 merge_id는 이 키와 역할이 겹쳐 삭제됐다 — merge API가 사라지면서
  "물리 키 vs 논리 키"를 나눌 이유가 없어졌다.)

---

## 3. 상태 모델

```python
class JobState(Enum):
    CREATED; SUBMITTING; RETRY_WAIT; SUBMIT_FAILED; LOST      # 내부
    PEND; RUN; DONE; EXIT; PSUSP; USUSP; SSUSP; UNKWN; ZOMBI  # LSF native
```

- **`terminal`(최종 상태)** = 더 이상 전이하지 않는 상태 — **성공만이 아니라 끝나는
  모든 방식**을 포괄한다. `is_terminal` = {`DONE`(정상 종료 exit 0),
  `EXIT`(비정상 종료 exit≠0), `SUBMIT_FAILED`(제출 재시도 소진), `LOST`(손실 확정)}.
  "전원 terminal"은 **모두 성공(DONE)이 아니라 모두 끝남**을 뜻한다(post_process
  발화 조건, FR-10).
- 그 밖: `is_failed` {EXIT, SUBMIT_FAILED, LOST} / `is_on_lsf` {PEND/RUN/SUSP\*/
  UNKWN/ZOMBI} / **`is_inactive`** = CREATED **또는** terminal (submit/편집/remove의
  공통 "비활성" 술어 — terminal보다 넓다: CREATED는 "아직 제출 안 함"이라 terminal은
  아니지만 inactive).
- 전이: `CREATED → SUBMITTING → PEND → RUN → DONE|EXIT`, 실패 시 `RETRY_WAIT`(n<N)
  또는 `SUBMIT_FAILED`(n==N), 조회 실패 없이 연속 미발견 → `LOST`.
  cancel/kill(미제출) 시 `SUBMITTING/RETRY_WAIT → CANCELLED`(terminal, 실패 아님).
- 전이는 Store 경유만(원자적 `transition`).
- `JobRecord`/`JobSetRecord`: frozen dataclass.
- **불변식: 요약 상태별 합계 == intended_count** (remove/편집도 유지).

---

## 3.5 예외 계층 (도메인 오류 계약)

모든 예외는 `LsfmgrError`를 base로 하며(순수 Python, Qt 비의존), 앱은 한 곳
(`except LsfmgrError`)에서 다 잡거나 세분화된 타입으로 개별 처리한다. 입력이 잘못된
**프로그래밍 오류**(길이 불일치·빈 커맨드·job_key 중복·범위 위반)는
`ValueError`/`TypeError`로 별도 신호한다 — 도메인 상태 오류와 구분.

```
Exception
├── ValueError / TypeError            # 잘못된 인자 (OPT-2/3, create_jobset 검증 등)
└── LsfmgrError                       # lsfmgr 모든 예외의 base
    ├── JobSetNotFoundError           # 존재하지 않는(삭제 포함) jobset_id 접근
    ├── JobSetRemovedError            # 삭제된 JobSet 핸들 접근
    ├── JobNotFoundError              # jobset 내 없는 job(job_id/job_key/job_key)
    ├── LsfCommandError               # LSF 명령 실행 실패 (제출 제외)
    │       .returncode / .stderr
    ├── SubmitError                   # 제출 **실행** 실패 (JobRecord.fail_reason 기록)
    │       .fail_reason / .returncode / .stderr / .stdout / .retryable / .diagnostic()
    ├── ArgMaxExceededError           # 단일 chunk가 ARG_MAX 초과 (NFR-5)
    └── JobSetStateError              # **전제조건 위반** — "지금은 이 명령 불가"
            .jobset_id / .job_keys    #   막은 원인을 구조화(메시지 파싱 불필요)
        ├── SubmitNotAllowedError     # 활성 job / 제출할 job 없음 / submit·kill 진행 중
        ├── JobEditNotAllowedError    # 교체 대상 활성 / submit·kill 진행 중
        ├── RemoveNotAllowedError     # remove_jobs·clear_jobs 대상이 활성 (force로 강제)
        └── RemoveJobSetNotAllowedError  # 전원 terminal 아님 (force=True로 강제)
```

- **ERR-1 단일 base**: 모든 예외는 `LsfmgrError` 하위 — `except LsfmgrError`로
  라이브러리 오류를 전부 포착할 수 있다.
- **ERR-2 상태 vs 입력 구분**: 현재 상태에서 허용되지 않는 명령은 `JobSetStateError`
  계열(도메인), 잘못된 인자는 `ValueError`/`TypeError`. 전자는
  `can_submit()`으로 사전 회피 가능.
- **ERR-3 구조화 정보**: `JobSetStateError`는 `.jobset_id`와 걸린 `.job_keys`를 담아,
  GUI가 메시지 문자열을 파싱하지 않고 어느 job이 막았는지 알 수 있다.
- **ERR-4 이름 구분**: `SubmitError`(제출 실행 실패)와 `SubmitNotAllowedError`
  (제출 전제 위반)는 별개다 — 혼동 금지.
- **ERR-5 worker 예외는 예외가 아니라 Signal**: worker 스레드(submit/polling/kill/
  handler/post_process) 내부 예외는 raise되지 않고 `error_occurred` Signal +
  `logger.exception`으로 전달된다 (CS-5, 스레드 보호).

---

## 4. Qt 스레딩 — GUI Freeze 방지

- **QT-0 (API 계약)**: 명령 API는 **모두 즉시 반환하는 비동기**, 결과는 Signal로만.
  조회 API(summary/jobs)는 **동기이지만 Store 스냅샷만** 읽음(LSF 호출 없음).
  `summary()`는 상태별 개수를 증분으로 들고 있어 job 수와 무관하게 O(1)이다 —
  전수 스캔이면 제출 중 main 스레드가 배치마다 그 값을 물어 store lock을
  쥐고, worker 전이까지 함께 밀린다(2만 건 실측 22.9s → 12.9s).
  public API docstring에 `[async→Signal]` / `[sync, snapshot]` 표기.
- QT-1: main 스레드에서 blocking LSF 호출 금지
- QT-2: worker → main 통지는 Signal (자동 queued connection)
- QT-3: Signal 인자는 불변(frozen) 객체만
- QT-4: batch Signal — job 단위 emit 금지, jobset 요약 + 변경분 리스트
- QT-5: progress Signal throttle (0.5초 또는 진행률 1%, 마지막 100%는 항상)
- QT-6: cancel은 job 경계 안전 지점에서, 이미 submit된 job은 정상 기록
- 스레딩: submit=QThreadPool+QRunnable / polling=전용 QThread+소속 QTimer /
  kill·단발조회=QThreadPool / retry 대기=QTimer 스케줄(sleep 금지)
- **store-first-signal-later**: Signal은 Store에 반영된 전이의 스냅샷이다.
  `js.jobs()` pull은 호출 시점의 현재 스냅샷이므로 신호보다 앞설 수 있다.
  같은 실행에서 이미 전달한 revision보다 오래된 결과는 전달하지 않는다.
  `min_state_dwell_s`는 `jobs_updated`에 추가 표시 지연을 준다(README §6.5).
- shutdown(): 진행 중 제출은 완료까지 대기(job_id 유실 방지), 미착수분 취소. 멱등.

---

## 5. 저장소 — InMemory 단일

```
JobSetStore(ABC) ── InMemoryStore
```

- 공통 API: JobSet/JobRecord CRUD, `transition()`(원자적 — `new_state=None`이면
  상태 유지·필드만 갱신하는 부분 갱신), `add_jobs`(배치), summary,
  search. InMemory는 파일을 만들지 않는다 — 앱이 죽어도 LSF의 job은 LSF에 잔존하므로
  수동 확인·정리가 가능하다.
- 영속 저장소·세션 복원·이력 통계는 제공하지 않는다(앱이 필요하면 `user_data`와
  `js.jobs()` 스냅샷으로 직접 저장한다).
- *(mocklsf 내부 SQLite는 가상 스케줄러 구현일 뿐 본 저장소와 무관.)*

**계층별 이름 규약(같은 개념도 계층마다 다른 동사 — 혼동 방지):**

| 개념 | 공개 API (`mgr.*`) | 도메인 (`jobsets.*`) | 저장소 (`store.*`) |
|---|---|---|---|
| jobset 생성 | `create_jobset(commands)` → JobSet 핸들 | `local_create_jobset(...)` → Record | `store_insert_jobset(record)` → Record |
| job 삭제 | `remove_job(js, ...)`/`clear_jobs(js)` (가드) | `local_remove_jobs(...)`/`local_clear_jobs()` | `store_delete_job(...)` |
| jobset 삭제 | `remove_jobset(js)` (레코드째 삭제) | `local_remove_jobset(jsid)` | `store_delete_jobset(jsid)` |
| 저장소 해제 | (매니저 `shutdown()`) | — | `store_dispose()` (저장소 자원 해제) |

- 읽기 통과(`get_jobs`/`summary`/`list_jobsets`)는 세 계층 동일 이름을 유지한다 —
  **의미가 같은 facade read-through**라 혼동 없음.

---

## 6. 기능 요구사항 (FR)

- **FR-1 Submission**: `mgr.submit(js)` — jobset의 **전 job (재)제출**(유일 경로).
  대상이 전원 비활성(`can_submit`)이어야 하며 활성이 있으면 `SubmitNotAllowedError`.
  리셋 후 재실행되므로 같은 job_key가 전이(핸들·테이블 연속).
  - **FR-1.0** `only=[ref, ...]`(job_key/job_key/job_id)를 주면 **그 job만**
    제출한다. 가드는 제출 대상에만 걸리므로 다른 job이 RUN이어도 진행되지만,
    대상 자신이 활성이면 거부한다(리셋이 살아있는 job을 추적 불가로 만든다).
    빈 리스트는 `SubmitNotAllowedError`. 사전 확인은 `can_submit(js, only=…)`.
    rearm/리포트 집계도 선택분 기준이다.
  - **FR-1.1** 입력은 wrapper 커맨드 — 토큰 리스트는 그대로, 문자열은 `shlex` 분해
    후 **그대로 subprocess 실행**한다(인자 조립·주입 없음).
  - **FR-1.2** stdout에서 `Job <(\d+)>` 파싱으로 job_id 확보. 실패 시
    `NO_JOBID_PARSED`.
  - **FR-1.3** 제출 subprocess의 cwd는 `work_dir`/`work_dirs`(job별 `submit_cwd`)로
    지정하며 재제출·교체에 보존된다. `os.chdir` 금지(스레드 안전).
  - **FR-1.4** job_key = `<jsid>_<idx>` (내부 키 — LSF에 부착하지 않음).
- **FR-2 Retry**: 실패 감지(exit≠0/파싱 실패/timeout) + fail_reason 분류, 최대
  `max_retry`회, `retry_backoff` 정책(**FR-2.1** timeout, **FR-2.2** backoff),
  재시도는 QTimer 스케줄(sleep 없음). **비정상 종료만 재시도**한다(파싱 실패·timeout은
  중복 제출 위험이라 재시도 안 함). 재제출 리셋 시 이전 실행 흔적 소거.
- **FR-3 Kill**: job_id chunked `bkill` **단일 경로**, 부분 kill(`only_state`)·
  선택 kill(`kill_jobs`). (v10.6: MC 분류 kill 삭제 — kill은 항상 plain bkill)
  (`{클러스터명: cshrc경로}`, `"*"`가 기본 env)로 클러스터별 env를 source한 bkill.
  제출 우선권(**FR-3.7**): kill이 **겨냥한 job에만, 항상** 걸린다 — 전체 kill은
  jobset 전체, 선택 kill(`kill_jobs`)은 선택한 key만(대상 아닌 job의 제출은 계속).
  jobset 제출을 통째로 멈추는 것은 `mgr.cancel_submit(js)`의 일이다
  (v10.5: `cancel_submit=True` 옵션 삭제 — 기본값이 유출이었다).
  - **FR-3.7** 어느 경로든 **kill 대상이 제출 중이면 반드시 처리된다** — 미착수분은
    `CREATED` 복귀, 이미 wrapper가 도는 분은 job_id 확보를 기다렸다가 kill. 제출
    중인 job은 job_id가 없어 bkill 대상이 될 수 없으므로, 기다리지 않으면 key→id
    해석에서 빠져 kill을 빠져나가고 나중에 `PEND`→`RUN`으로 부활한다. 정지 대기
    초과는 `KillReport.errors`에 남긴다.
  - **FR-3.4** 확인 문구 파싱 + 미확인분 재시도(`kill_max_retry`),
    `KillReport.unconfirmed`/`kill_retries`.
  - **FR-3.5** `kill_status_policy` — `"optimistic"`=확인 즉시 EXIT /
    `"actual"`=폴링·verify로만.
  - **FR-3.6** cluster 미상 대상은 bkill **직전에** 최소 포맷
    (`jobid source_cluster forward_cluster`)으로 1회 조회해 채운다 — 제출 직후 즉시
    kill해도 올바른 env로 죽는다.
  - **kill 우선권 (구조적 보장)**: kill은 진행 중 submit에 우선. `SubmitGate` barrier —
    barrier 확인과 submit 등록이 한 lock 아래 **원자적**이라 "kill의 취소를 빠져나가는
    늦은 제출"이 불가능(`lifecycle.py` SubmitGate/KillScope). `kill_started`는 접수
    즉시(동기) 발화 — 정지 대기로 완료가 늦어도 UI가 바로 표시.
- **FR-4 Monitoring**: 조회는 explicit job id chunked
  `bjobs -noheader -o "... delimiter=';'"` **단 하나**이며, `is_on_lsf` 상태만
  조회한다. 못 찾은 id는 **연속 `lost_after_missing_polls`회** 미발견일 때만 `LOST`
  확정한다(제출 직후 등록 지연·조회 클러스터 불일치로 멀쩡한 job이 죽는 것을 막는다).
  polling은 batch 반영 후 Signal(**FR-4.1** 요약+변경분, **FR-4.2** LOST 확정).
  - **FR-4.3 판단 보류**: 조회 실패(장애)와 부재(LOST)를 구분 — 미발견이라도 조회에
    **실패가 섞였으면 LOST 확정 안 함**(다음 사이클 재시도). chunk 단위 실패 격리 +
    연속 2회 실패 시 회로 차단(남은 chunk 즉시 실패, 전면 장애에서 스레드 블록 방지).
    보류 경고는 사이클당 1줄 집계.
  - **FR-4.4** MC forward 정보(`collect_clusters`), LSF 실행정보(run_time/start/
    finish)를 `bjobs -o`로 수집. 사이트가 확장 필드를 모르면 3단 강등
    (FULL+MC → FULL → CORE)으로 그 필드만 포기한다. 작업 디렉토리는 조회하지
    않는다 — 제출 요청값 `submit_cwd`로 본다(구 `exec_cwd` 수집 삭제).
- **FR-5 JobSet 관리**:
  - **FR-5.1** 요약(불변식 합계==intended_count), **FR-5.2** intended_count 정합,
  - **FR-5.3** 손실 감지(`detect_lost` — ID 미확보 SUBMITTING → LOST 확정),
  - **FR-5.4** 생성(`create_jobset` 한 곳: commands/job_keys/user_datas/work_dir(s))
    — 이후 편집은 FR-5.5,
  - **FR-5.5** 편집 3형제(add_jobs/replace_jobs/upsert_jobs): job_key가 이미
    있을 때의 처리만 다르다(거부/교체/교체). 교체는 같은 job_key 자리 —
    테이블 행 연속. 가드=submit·kill 미진행 + 교체 대상이 비활성,
    `force`=레코드만 강제(LSF 정리는 앱 책임). 편집 후 관찰 대상이 있으면
    폴링 자동 재개, 변경분의 handler 장부는 무효화,
  - **FR-5.6** remove_jobs(refs, force)·clear_jobs(force) — 비활성만,
    force로 레코드만 강제 삭제, intended_count 함께 감소,
  - **FR-5.7** remove_jobset — 전원 terminal일 때 jobset을 **레코드째 삭제**
    (force로 강제). 삭제분은 list/search/get_jobs 어디에도 남지 않는다 —
    결과가 필요하면 삭제 전에 스냅샷을 뜬다(반환값=삭제 직전 JobSetRecord).
- **FR-7 JobSet Handler**: 이름 있는 handler를 등록해 **폴링 사이클마다**(별도 타이머
  없이 `poll_interval_s`에 tie — bjobs 갱신 직후) job별로 worker 스레드에서 실행.
  `start_states`(기본 `{RUN}`)부터 시작, `end_states`(기본 `{DONE,EXIT}`) 도달 시
  `final=True`로 최종 1회 후 종결. 결과는 `handler_finished(name, HandlerResult)`.
  재제출 시 자동 재무장. 인자는 `HandlerContext`(record/job_id/submit_cwd/final).
  실행은 QThreadPool worker(GUI freeze 금지), 예외 격리(`HandlerResult.error`).
  폴링이 돌고 있어야 동작.
- **FR-9 pre_submit 게이트**: `mgr.submit(js, pre_submit=fn)` — 실제 제출 전에
  **커맨드 리스트 전체를 단일 worker에서 1회 검사**, `bool` 반환. 게이트는 레코드
  리셋 **이전**에 돌아 `False`/예외면 레코드 **원상 유지**(제출 없음). 신호 순서:
  `pre_submit_started → pre_submit_finished(ok) → (ok일 때만) submit_started → … →
  submit_finished`. 게이트 통과 후에 rearm/AUTO-1 polling. 콜백은 worker 스레드
  실행(GUI 접근 금지, 멱등 권장).
- **FR-10 post_process 후처리**: `mgr.submit(js, post_process=fn)` — 이 제출의
  **전 job이 terminal**(DONE/EXIT/SUBMIT_FAILED/CANCELLED/LOST — §3의 `is_terminal`)에 도달하면
  worker에서 **1회** 실행. 완료 감지는 폴링/`query_once`/submit·kill 결과 전달의
  공통 지점에서 이뤄지며, 감지 즉시 무장 해제해 중복 발화하지 않는다. **성공/실패 무관** — 전원
  terminal이면 실행하고, 콜백이 최종 JobRecord 목록을 받아 결과를 분류한다("이 실행이
  끝났다"는 시점이지 "전부 성공"이 아니다). 신호: `post_processing_started →
  post_processing_finished(result)`(반환값, 예외 시 `None` + `error_occurred`).
  한 제출당 1회, 완료 전 `post_process` 없이 재제출하면 이전 무장 해제.
- **FR-11 jobset_finished 완료 통지**: jobset의 **전 job이 terminal**에 도달하고
  관련 제출·kill 활동의 정산을 마치면
  `jobset_finished(jobset_id, summary)` 1회. 등록물(`post_process`/handler)과
  **무관**하게 판정하므로, 아무것도 등록하지 않은 jobset도 완료를
  통지받는다. 감지 지점은 FR-10과 같은 공통 지점(폴링/`query_once`/submit·kill 결과 전달)이며,
  `post_process`도 걸었다면 `jobset_finished → post_processing_started` 순서다.
  **재무장**: 다시 non-terminal이 되면(재제출·job 추가) latch가 풀려 다음
  완료에 또 발화한다 — "완료"는 제출 사이클이 아니라 jobset 상태의 성질이다.
  job이 하나도 없는 빈 jobset에서는 발화하지 않는다.
  **사용자 kill 억제**: 전원 terminal이며 전 job이 `JobRecord.killed=True`일 때만
  통지를 생략한다(latch만 세움). 자연 종료와 kill이 섞인 완료는 통지한다.
  관련 kill이 진행 중이면 verify와 killed 마킹 사이의 상태로 판정하지 않도록
  보류하고, kill 결과가 main에 전달되면 다시 판정한다. JobSet 없는 원시 ID kill과
  겹친 kill에도 적용한다. actual 정책에서 아직 살아 있으면 이후 폴링에서 판정한다.
  `already finished` 응답만 받은 자연 종료는 `killed=False`를 유지한다.
  post_process는 전원 kill이어도 실행하며, kill 귀속이 반영된 레코드를 받는다.

---

## 7. 동시성 안전 (CS)

- CS-1 Store thread-safe(QMutex/RLock), transition 원자성
- CS-2 frozen dataclass — Signal/스레드 공유 안전
- CS-4 동일 JobSet 중복 polling 방지
- CS-5 worker 예외 격리 → error Signal + logger.exception (traceback 로그)
- CS-6 rate limiter thread-safe
- CS-7 Store 쓰기 경로 일원화
- CS-8 shutdown 시 job_id 유실 방지 (제출 완료 단위 즉시 반영, queued 재제출 무시)
- **CS-11 kill 우선권 lock 규율**: SubmitGate lock은 leaf(쥔 채 대기·외부 호출 없음)
  — barrier↑/등록/취소가 한 lock 아래 원자적이되 데드락 없음.
- (multiprocessing 미사용 — subprocess는 GIL 해제, Qt는 fork-unsafe)

---

## 8. 비기능 요구사항 (NFR)

| ID | 요구사항 |
|---|---|
| NFR-1 | Qt import는 qtpy 경유만, 4개 바인딩 동일 동작 |
| NFR-2 | 의존성: qtpy + Qt 바인딩 1종 + stdlib (그 외 금지) |
| NFR-3 | GUI freeze 금지 — 5,000 job 처리 중 main 스레드 100ms 이상 정지 없음 |
| NFR-4 | LSF 부하 보호 — chunking, rate limit, 폴링 호출 수 ∝ jobset 수 |
| NFR-5 | ARG_MAX 안전 — 인자 길이 검사 + chunk 상한 |
| NFR-6 | 로깅: `lsfmgr.*` 계층. DEBUG=LSF 명령 원문/stdout/stderr, INFO=submit/kill 착수·완료, WARNING=retry·조회 실패·판단 보류, ERROR=SUBMIT_FAILED/LOST 확정·worker 예외(traceback) |
| NFR-7 | 설정 configurable — §1.2 옵션 계층 + LsfConfig 주입 |
| NFR-8 | 테스트: LSF mock 주입, Store 계약 테스트, 동시성, pytest-qt Signal, PyQt5+PySide6 |
| NFR-9 | Python 3.9+ |
| NFR-10 | **단일 진입점 사용성**: `create_jobset([...])` → `mgr.submit(js)` → Signal 연결로 동작 |

---

## 9. 모듈 구조

```
lsfmgr/
├── __init__.py          # LsfJobManager, JobSet, JobState, JobRecord, ... export
├── qt.py                # qtpy re-export 단일 지점
├── options.py           # Options(frozen), resolve_options(), 검증(OPT-1~4)
├── config.py            # LsfConfig (Qt 비의존)
├── states.py            # JobState, JobRecord, JobSetRecord
├── reports.py           # SubmitReport/Progress, KillReport/Progress
├── errors.py            # LsfmgrError 계층 (§3.5)
├── command.py           # LsfCommand 래퍼 (Qt 비의존, chunking, ARG_MAX, chunk 격리)
├── store/               # base(ABC) / memory
├── submitter.py         # QThreadPool submit + retry + progress/cancel + pre_submit 게이트
├── lifecycle.py         # SubmitGate / KillScope — kill 우선권 barrier (CS-11)
├── monitor.py           # PollingService (QThread+QTimer) + query_once + chunk 격리/회로차단
├── killer.py            # chunked bkill + env 분류 + verify + 확인 재시도
├── handlers.py          # JobSetHandlerService — job별 주기 handler (FR-7)
├── jobset_core.py       # JobSet 도메인 로직 — local_* (편집/삭제 공용 몸통)
├── handle.py            # JobSet 핸들 (조회 + Signal 전용 뷰)
├── pacer.py             # progress throttle / 상태 전이 dwell
├── util.py
└── manager.py           # LsfJobManager: 명령 진입점 + 옵션 해석 + AUTO-1~3 + shutdown
                         #   (+ _PostProcessTask — 전원 terminal 후처리, FR-10)
```

Qt 비의존 유지: options/config/states/command/store/jobset_core (Qt 없이 테스트 가능).

---

## 10. 수용 기준 (Acceptance Criteria)

1. 5,000개 submit — ID 파싱 100% 또는 실패분 정확 분류
2. 5,000개 kill — chunk 분할로 ARG_MAX 에러 없음, 미확인분 재시도
3. 요약 합계 == intended_count (생성/편집/remove 후에도)
4. polling 호출 횟수 ∝ JobSet 수 × chunk 수 (job 수에 선형 폭증 없음)
5. bjobs 소실 → LOST 누락 없음, 조회 실패 섞이면 보류(FR-4.3), 연속 미발견 유예 준수
6. GUI 응답성 — main 스레드 100ms 이상 정지 없음
7. PyQt5·PySide6 각각 전체 테스트 통과 (`QT_API` 전환만으로)
8. 동시성 — submit+polling+kill 동시 수행 시 무결성, **kill 우선권**(진행 중 submit
   중 kill 시 미제출분 CANCELLED 확정·제출분 kill, SUBMITTING 유출 없음)
9. Store 계약 테스트 통과, InMemory 파일 미생성
10. **명령 일원화**: 모든 명령은 `mgr.*`, JobSet 핸들에 명령 메서드 없음
11. shutdown 후 잔여 스레드 없음 (AUTO-3 자동 연결 포함)
12. LSF mock 주입 단위 테스트 가능
13. **옵션 계층**: 내장 < manager < call 우선순위, 오타 `TypeError`, 범위 위반 `ValueError`
14. **JobSet Signal**: 해당 JobSet 이벤트만 수신, `mgr.*` Signal과 이중 발행 일치
15. **handler (FR-7)**: start/end state 구간 준수(시작 전 미발화·종료 시 final 1회),
    예외 격리, 폴링 사이클 구동, 재제출 후 재무장
16. **생성/편집 (FR-5.4/5.5)**: create_jobset가 유일 생성 경로, 이후는 편집 3형제,
    replace 시 같은 job_key 자리 유지·요약 불변식, force는 레코드만
17. **재실행**: `mgr.replace_jobs(js, …) + mgr.submit(js, only=…)`로 실패분만 교체·재실행,
    job_key·user_data·submit_cwd 보존
18. **pre_submit 게이트 (FR-9)**: False/예외 시 레코드 원상, 신호 순서 보장,
    통과 후에만 rearm/AUTO-1
19. **post_process 후처리 (FR-10)**: 전원 terminal(성공/실패 무관) 시 1회 실행,
    최종 레코드 전달, 미완료 시 미발화, 완료 후 재발화 없음, 예외 격리
    (error_occurred + finished(None))
20. **MC kill (FR-3.6)**: 제출 직후 cluster 미상 상태에서 kill해도 최소 포맷 조회로
    env를 확정해 forward job이 죽는다
