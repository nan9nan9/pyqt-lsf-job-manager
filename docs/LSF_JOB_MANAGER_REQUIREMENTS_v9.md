# LSF Job Manager — Requirements Specification

> **버전**: v9 (2026-07-11) — v7 대비 **대규모 단순화**:
> ① 명령 일원화(모든 명령은 `mgr.*` 한 곳, JobSet 핸들은 조회+Signal 전용 뷰)
> ② job 생성은 `create_jobset` 한 곳(이후 추가는 merge만) — `merge_id`/`user_data` 도입
> ③ 재실행 = merge + submit (resubmit API 제거)
> ④ 저장소 InMemory 단일(SQLite/영속·세션복원 제거)
> ⑤ one-shot/array 제출·`mode` 옵션·`add_job` 제거
> ⑥ pre_submit 게이트(FR-9)·kill 우선권 구조(SubmitGate) 신설
> **형태**: Qt 전용 Python 라이브러리 — **qtpy** 기반, PyQt5 / PySide2 / PyQt6 / PySide6 호환
> **환경**: Linux, NFS 다중 사용자(~300명), LSF cluster, 폐쇄망

---

## 0. 목적 및 범위

Qt GUI 애플리케이션(SimManager 등)에서 LSF cluster로 대량 시뮬레이션 job을
submit / monitoring / kill 하는 라이브러리를 구현한다. **job 제어(무엇을 언제
제출·재실행·삭제할지)는 GUI 앱이 직접 갖고**, 라이브러리는 그 결정을 실행하는
CRUD + submit + kill + poll 만 제공한다.

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

## 1. API 구조 (v9 — 명령 일원화)

**명령은 전부 `mgr.*` 한 곳**이고, **JobSet 핸들은 조회(pull) + Signal 전용 뷰**다.
두 표면(구 High-level JobSet 메서드 vs Low-level Facade)의 혼란을 제거했다.

```
명령 (전부 async→Signal)   mgr.submit(js) / mgr.kill(js) / mgr.merge(a,b) / ...
                          인자는 JobSet 핸들 또는 jobset_id 문자열 (_jsid 정규화)
조회 (전부 sync, snapshot)  js.jobs() / js.summary / js.is_* / mgr.get_jobs(js) ...
Signal                    js.<signal> (해당 JobSet만) 또는 mgr.<signal>(jsid, ...) (전역)
```

### 1.1 기본 흐름 — 생성 → (필요 시 merge) → submit

```python
mgr = LsfJobManager()

# job 생성은 create_jobset 한 곳 — 생성 시 job까지 함께 만든다
js = mgr.create_jobset(
    ["customwrapper_sub -i a.sp", "customwrapper_sub -i b.sp"],
    merge_ids=["case-a", "case-b"],            # 논리 키 (merge 시 replace 기준)
    user_datas=[{"rev": 3}, None],               # job별 사용자 데이터 (보존만)
    label="sweep")

js.jobset_updated.connect(lambda s: ...)       # 이 핸들의 Signal에 GUI 바인딩

if mgr.can_submit(js):
    mgr.submit(js, workers=8)                  # 전 job (재)제출
```

- **생성 후 job 추가는 오직 merge** — 별도 jobset을 만들어 `mgr.merge(js, src)`로
  흡수한다. `create_job`/`create_jobs`/`add_job` 는 없다.
- 라이프사이클 자동화:
  - **AUTO-1**: `submit()` 시 polling 자동 시작 (`auto_poll=False`로 해제)
  - **AUTO-2**: JobSet 전원 terminal(또는 활동 없음 2사이클) 도달 시 polling 자동 중지
  - **AUTO-3**: `LsfJobManager` 생성 시 `QApplication.aboutToQuit`에 `shutdown()`
    자동 연결 (명시 호출도 가능, 멱등)
  - *(v7의 AUTO-4 array 자동선택은 array 제출 제거로 삭제됨)*

### 1.2 옵션 처리 원칙 — 3단 계층

모든 튜닝 파라미터는 **안 주면 기본값, 주면 그 호출에만 적용**:

```
① 내장 기본값  <  ② LsfJobManager(...) 생성 인자 (앱 전역)  <  ③ submit(...) 인자 (이번 호출)
```

**옵션 카탈로그** (전 계층 공통 이름):

| 옵션 | 내장 기본값 | 적용 계층 | 설명 |
|---|---|---|---|
| `workers` | 32 | ②③ | 병렬 submit worker 수 (1~64) |
| `max_retry` | 3 | ②③ | submit 실패 재시도 횟수 (0=없음) |
| `retry_backoff` | "fixed:2" | ②③ | "fixed:N초" 또는 "expo:base초" |
| `rate_limit_per_s` | None(무제한) | ②③ | 초당 bsub 상한 |
| `poll_interval_s` | 10 | ②③ | polling 주기 (5~60) |
| `auto_poll` | True | ②③ | submit 후 polling 자동 시작 |
| `queue` | LSF 기본 | ②(`default_queue`)③ | 대상 queue |
| `resource_req` | None | ②③ | `-R` 문자열 |
| `output_dir` | None | ②③ | `-o`/`-e` 경로 규칙 |
| `submit_timeout_s` | 30 | ②③ | bsub 1건 timeout |
| `verify_kill` | False | ②③(kill) | kill 후 실제 종료 확인 |
| `label`, `tags`, `description` | "" / () / "" | ②③ | JobSet 메타데이터 |
| `chunk_size` | 200 | ② | chunking fallback 크기 |
| `kill_status_policy` | "optimistic" | ② | kill 확인 시 즉시 EXIT / "actual"=폴링만 |
| `kill_max_retry` | 2 | ② | kill 확인 실패 재시도 |
| `collect_clusters` | False | ② | MC forward 정보 수집(opt-in) |
| `bsub_path` 등 명령 경로 | PATH 탐색 | ② | LSF 명령 위치 (문자열/wrapper 토큰) |

> v7 대비 삭제된 옵션: `mode`(array 자동/강제), `persistent`/`db_path`(SQLite),
> `bmod_path`(add_job 제거).

- **OPT-1** 옵션 해석은 `resolve_options(call_kwargs) -> Options` 한 함수로 일원화
  (defaults → manager → call 순 merge, frozen dataclass 반환).
- **OPT-2** 알 수 없는 키워드는 즉시 `TypeError` (오타 조기 발견).
- **OPT-3** 범위 검증 (workers 1~64 등) 위반 시 `ValueError`.
- **OPT-4** `LsfConfig` 객체 주입도 지원 (`LsfJobManager(config=cfg)`) — kwargs 우선.

### 1.3 명령 API (전부 `mgr.*`, 인자는 핸들 또는 jobset_id)

```python
# --- 생성/구성 (sync) ---
mgr.create_jobset(commands=(), *, merge_ids=None, user_datas=None, wrapper=True,
                  label="", tags=(), parent=None, intended_count=0) -> JobSet
mgr.merge(target, source, *, force=False) -> list[JobRecord]  # in-place 흡수
mgr.can_merge(target, source) -> bool
mgr.remove_job(js, *, job_id=None, merge_id=None, job_key=None, force=False)
mgr.clear(js, *, force=False)
mgr.set_user_data(js, ref, user_data)            # ref = job_key | merge_id | job_id
mgr.can_submit(js) -> bool
mgr.close(js)                                # 종결 (전원 terminal일 때)

# --- 실행 (async→Signal) ---
mgr.submit(js, *, pre_submit=None, **opts) -> JobSet   # 전 job (재)제출 (유일 경로)
mgr.kill(js, *, only_state=None, verify=None, envpath="")
mgr.kill_jobs(js, job_keys, *, verify=None, envpath="")  # 선택 job만
mgr.cancel_submit(js)                        # 진행 중 submit 중단
mgr.start_polling(js, interval_s=None); mgr.stop_polling(js)
mgr.query_once(js)                           # 1회 강제 조회
mgr.fetch_job_detail(js, job_key)            # bhist -l 온디맨드 (async)

# --- handler (FR-7) ---
mgr.add_handler(js, name, fn, *, start_states=None, end_states=None)
mgr.remove_handler(js, name)

# --- 조회/복원 ---
mgr.jobset(jobset_id) -> JobSet              # ID로 핸들 재획득
mgr.search_jobsets(...); mgr.detect_lost(js); mgr.job_detail(js, job_key)  # 동기
```

### 1.4 JobSet 핸들 — 조회 + Signal 전용 뷰 (QObject)

```python
class JobSet(QObject):
    # 이 JobSet 전용 Signal (이름은 mgr.* Signal과 동일, jsid 인자만 없음)
    jobset_updated  = Signal(dict)     # 요약 {"total":.., "RUN":.., ...}
    jobs_updated    = Signal(list)     # 변경분 [JobRecord]
    submit_progress = Signal(int, int) # (done, total), throttled
    submit_finished = Signal(object)   # SubmitReport
    jobs_failed     = Signal(list)     # SUBMIT_FAILED/EXIT/LOST 변경분
    kill_started    = Signal()         # kill 접수 즉시(동기) — 착수 피드백
    kill_progress   = Signal(int, int) # (done, total)
    kill_finished   = Signal(object)   # KillReport
    handler_finished= Signal(str, object)      # name, HandlerResult
    job_detail_ready= Signal(str, str)         # job_key, text
    ready_started/ready_finished        # pre_submit 게이트 (FR-9)
    error_occurred  = Signal(str)

    # 조회 (전부 sync — Store 스냅샷, LSF 호출 없음)
    id; summary; is_done; is_active; is_inactive; is_submitting; submit_state
    is_killing; kill_state; failed_jobs
    def jobs(self, states=None) -> list[JobRecord]: ...
```

- **핸들에 명령 메서드는 없다** — kill/merge/submit 등은 전부 `mgr.*`. 두 표면
  혼란 제거가 v9의 핵심.
- JobSet 재획득: `mgr.jobset(jobset_id)`. 파괴된 핸들 접근 시 `JobSetClosedError`.

---

## 2. 용어 정의 — 혼동 방지 필수

**코드에서 bare "group" 사용 금지.**

| 용어 | 코드 명칭 | 정의 |
|---|---|---|
| **JobSet** | `jobset_id`, `JobSet` 객체 | 논리적 job 묶음. 모든 기능의 기본 단위 |
| **merge_id** | `JobRecord.merge_id` | job의 **논리 키** — merge 시 같은 merge_id 기존 job을 replace |
| **user_data** | `JobRecord.user_data` | 사용자 정의 dict(JSON-able). 라이브러리는 **보존만** |
| **LSF Job Group** | `lsf_group_path` | LSF native (`bsub -g`). 1회 호출 최적화 **수단** |
| **LSF Job Name** | `lsf_job_name`(=job_key) | LSF native (`bsub -J`). 패턴 조회/kill **수단**, fallback |
| **Array Job** | `array_index` | wrapper 제출 산물로만 존재하는 element(직접 array 제출 없음) |

관계 규칙:
- JobSet이 유일한 논리 단위, LSF 부착물(group/name/array)은 **실행 수단**
- 부착물 전부 유실 시에도 job_id chunking으로 동작 (graceful degradation)
- **merge_id**는 jobset 내 유일(None 제외). replace 시 물리 키(`job_key`) 유지 →
  테이블 행 연속. 없거나 None이면 신규 추가.

---

## 3. 상태 모델

```python
class JobState(Enum):
    CREATED; SUBMITTING; RETRY_WAIT; SUBMIT_FAILED; LOST      # 내부
    PEND; RUN; DONE; EXIT; PSUSP; USUSP; SSUSP; UNKWN; ZOMBI  # LSF native
```

- 헬퍼: `is_terminal` {DONE, EXIT, SUBMIT_FAILED, LOST} / `is_failed` /
  `is_on_lsf` {PEND/RUN/SUSP*/UNKWN/ZOMBI} /
  **`is_inactive`** = CREATED 또는 terminal (submit/merge/remove의 공통 "비활성" 술어)
- 전이: `CREATED → SUBMITTING → PEND → RUN → DONE|EXIT`,
  실패 시 `RETRY_WAIT`(n<N) 또는 `SUBMIT_FAILED`(n==N), 조회 전부 실패 없이
  미발견 → `LOST`. cancel/kill(미제출) 시 `SUBMITTING/RETRY_WAIT → CREATED`.
- 전이는 Store 경유만(원자적 `transition`).
- `JobRecord`(+`merge_id`/`user_data`)/`JobSetRecord`: frozen dataclass.
- **불변식: 요약 상태별 합계 == intended_count** (remove/merge도 유지).

---

## 4. Qt 스레딩 — GUI Freeze 방지

- **QT-0 (API 계약)**: 명령 API는 **모두 즉시 반환하는 비동기**, 결과는 Signal로만.
  조회 API(summary/jobs)는 **동기이지만 Store 스냅샷만** 읽음(LSF 호출 없음).
  public API docstring에 [async→Signal] / [sync, snapshot] 표기.
- QT-1: main 스레드에서 blocking LSF 호출 금지
- QT-2: worker → main 통지는 Signal (자동 queued connection)
- QT-3: Signal 인자는 불변(frozen) 객체만
- QT-4: batch Signal — job 단위 emit 금지, jobset 요약 + 변경분 리스트
- QT-5: progress Signal throttle (0.5초 또는 진행률 1%, 마지막 100%는 항상)
- QT-6: cancel은 job 경계 안전 지점에서, 이미 submit된 job은 정상 기록
- 스레딩: submit=QThreadPool+QRunnable / polling=전용 QThread+소속 QTimer /
  kill·단발조회=QThreadPool / retry 대기=QTimer 스케줄(sleep 금지)
- **store-first-signal-later**: store 갱신 뒤 Signal → 어느 slot에서든 `js.jobs()`
  pull이 신호 내용과 일치.
- shutdown(): 진행 중 bsub는 완료까지 대기(job_id 유실 방지), 미착수분 취소. 멱등.

---

## 5. 저장소 — InMemory 단일

```
JobSetStore(ABC) ── InMemoryStore
```

- SQLite/영속 모드, 세션 복원(orphan/recover/reconcile), 이력/통계는 **v9에서 제거**.
  쓰지 않아 전체 구조를 왜곡하던 부분을 걷어냄.
- 공통 API: JobSet/JobRecord CRUD, `transition()`(원자적), `add_jobs`(배치), summary,
  search. InMemory는 파일 미생성 — 앱 사망 시에도 LSF group은 LSF에 잔존하므로
  수동 확인/정리 가능.
- *(mocklsf 내부 SQLite는 가상 스케줄러 구현일 뿐 본 저장소와 무관.)*

---

## 6. 기능 요구사항 (FR)

- **FR-1 Submission**: `mgr.submit(js)` — jobset의 **전 job(재)제출**(유일 경로).
  전원 비활성(`can_submit`)이어야 하며 활성이 있으면 `LsfmgrError`. 리셋 후
  재실행되므로 같은 job_key가 전이(핸들·테이블 연속). 항목 타입별 제출 경로:
  JobSpec=bsub 조립 / argv 토큰 리스트=wrapper 그대로 실행 / 문자열=wrapper 기본
  (`wrapper=False`면 bsub). 부착물 실패해도 submit 진행.
  - **FR-1.4** job_key = `<jsid>_<idx>` (`-J`), **FR-1.5** wrapper 경로는 `Job <id>`
    파싱으로 job_id만 관리(인자 조립 없음).
- **FR-2 Retry**: 실패 감지(exit≠0/파싱 실패/timeout) + fail_reason 분류,
  최대 `max_retry`회, `retry_backoff` 정책(**FR-2.1** timeout, **FR-2.2** backoff),
  재시도는 QTimer 스케줄(sleep 없음). 재제출 리셋 시 이전 실행 흔적 소거.
- **FR-3 Kill**: 전략 우선순위 ①`bkill -g` ②array ③`-J` 패턴 ④id chunking,
  부분 kill(`only_state`)·선택 kill(`kill_jobs`). MC forward는 `envpath`로 그
  클러스터 env를 source한 bkill.
  - **FR-3.1~3.3** 전략 순회/부착물 유실 fallback, **FR-3.4** 확인 문구 파싱 +
    미확인분 재시도(`kill_max_retry`), `KillReport.unconfirmed`/`kill_retries`,
    **FR-3.5** `kill_status_policy`("optimistic"=확인 즉시 EXIT / "actual"=폴링만).
  - **kill 우선권 (구조적 보장)**: kill은 진행 중 submit에 우선. `SubmitGate` barrier —
    barrier 확인과 submit 등록이 한 lock 아래 **원자적**이라 "kill의 취소를
    빠져나가는 늦은 제출"이 불가능(`lifecycle.py` SubmitGate/KillScope). `kill_started`
    는 접수 즉시(동기) 발화 — 정지 대기로 완료가 늦어도 UI가 바로 표시.
- **FR-4 Monitoring**: 조회 전략 group→array→name→id chunking, `is_on_lsf`만 조회,
  못 찾은 id → bhist chunk fallback → 그래도 없으면 `LOST`. polling은 batch 반영 후
  Signal(**FR-4.1** 요약+변경분, **FR-4.2** LOST 확정).
  - **FR-4.3 판단 보류**: 조회 실패(장애)와 부재(LOST)를 구분 — 미발견이라도
    조회에 **실패가 섞였으면 LOST 확정 안 함**(다음 사이클 재시도). chunk 단위
    실패 격리 + 연속 2회 실패 시 회로 차단(남은 chunk 즉시 실패, 전면 장애에서
    스레드 블록 방지). 보류 경고는 사이클당 1줄 집계.
  - **FR-4.4** MC forward 정보(`collect_clusters`), LSF 실행정보(run_time/start/
    finish/exec_cwd)를 bjobs -o로 수집.
- **FR-5 JobSet 관리**:
  - **FR-5.1** 요약(불변식 합계==intended_count), **FR-5.2** intended_count 정합,
  - **FR-5.3** 손실 감지(name 패턴 복구 시도 후 LOST),
  - **FR-5.4** 생성(`create_jobset` 한 곳: commands/merge_ids/user_datas) — **이후 추가는
    merge만**,
  - **FR-5.5** merge(`merge_from`): in-place 흡수(target 핸들/테이블 유지, source 소멸),
    merge_id 일치=replace(물리 키 유지)/불일치·None=추가, 가드=양쪽 전원 비활성
    (`can_merge`), `force`=레코드만 강제(LSF 정리는 앱 책임), 폴링 source→target 이관,
  - **FR-5.6** remove_job(job_id/merge_id/job_key, force)·clear(force) — 비활성만, force로
    레코드만 강제 삭제, intended_count 함께 감소,
  - **FR-5.7** close(전원 terminal일 때 종결, LSF group `bgdel` 정리).
- **FR-7 JobSet Handler**: 이름 있는 handler를 등록해 **폴링 사이클마다**(별도 타이머
  없이 `poll_interval_s`에 tie — bjobs 갱신 직후) job별로 worker 스레드에서 실행.
  `start_states`(기본 {RUN})부터 시작, `end_states`(기본 {DONE,EXIT}) 도달 시
  `final=True`로 최종 1회 후 종결. 결과는 `handler_finished(name, HandlerResult)`.
  재제출(merge+submit) 시 자동 재무장. 인자는 `HandlerContext`(record/job_id/
  working_dir/final). 실행은 QThreadPool worker(GUI freeze 금지), 예외 격리
  (`HandlerResult.error`). 폴링이 돌고 있어야 동작.
- **FR-9 pre_submit 게이트**: `mgr.submit(js, pre_submit=fn)` — 실제 제출 전에
  **커맨드 리스트 전체를 단일 worker에서 1회 검사**, `bool` 반환. 게이트는 레코드
  리셋 **이전**에 돌아 `False`/예외면 레코드 **원상 유지**(제출 없음). 신호 순서:
  `ready_started → ready_finished(ok) → (ok일 때만) submit_started → … → submit_finished`.
  게이트 통과 후에 rearm/AUTO-1 polling. 콜백은 worker 스레드 실행(GUI 접근 금지,
  멱등 권장).

> v7 대비 삭제: **FR-6**(SQLite 세션 복원) — 저장소 단일화로 제거.
> **FR-8**(resubmit_jobs) — 재실행은 `mgr.merge(js, fix) + mgr.submit(js)` 패턴으로 대체.

---

## 7. 동시성 안전 (CS)

- CS-1 Store thread-safe(QMutex/RLock), transition 원자성
- CS-2 frozen dataclass — Signal/스레드 공유 안전
- CS-4 동일 JobSet 중복 polling 방지
- CS-5 worker 예외 격리 → error Signal + logger.exception (traceback 로그)
- CS-6 rate limiter thread-safe
- CS-7 Store 쓰기 경로 일원화
- CS-8 shutdown 시 job_id 유실 방지 (bsub 완료 단위 즉시 반영, queued 재제출 무시)
- CS-10 LSF group 경로 사용자 격리
- **CS-11 kill 우선권 lock 규율**: SubmitGate lock은 leaf(쥔 채 대기/외부 호출
  없음) — barrier↑/등록/취소가 한 lock 아래 원자적이되 데드락 없음.
- (multiprocessing 미사용 — subprocess는 GIL 해제, Qt는 fork-unsafe)

> v7의 CS-3/CS-9(SQLite connection/NFS db_path)는 저장소 단일화로 무의미해져 제거.

---

## 8. 비기능 요구사항 (NFR)

| ID | 요구사항 |
|---|---|
| NFR-1 | Qt import는 qtpy 경유만, 4개 바인딩 동일 동작 |
| NFR-2 | 의존성: qtpy + Qt 바인딩 1종 + stdlib (그 외 금지) |
| NFR-3 | GUI freeze 금지 — 5,000 job 처리 중 main 스레드 100ms 이상 정지 없음 |
| NFR-4 | LSF 부하 보호 — 부착물 1회 호출 우선, chunking 최후, rate limit |
| NFR-5 | ARG_MAX 안전 — 인자 길이 검사 + chunk 상한 |
| NFR-6 | 로깅: `lsfmgr.*` 계층. DEBUG=LSF 명령 원문/stdout/stderr, INFO=submit/kill 착수·완료, WARNING=retry·부착물 실패·판단 보류, ERROR=SUBMIT_FAILED/LOST 확정·worker 예외(traceback) |
| NFR-7 | 설정 configurable — §1.2 옵션 카탈로그 + LsfConfig 주입 |
| NFR-8 | 테스트: LSF mock 주입, Store 계약 테스트, 동시성, pytest-qt Signal, PyQt5+PySide6 |
| NFR-9 | Python 3.9+ |
| NFR-10 | **단일 진입점 사용성**: `create_jobset([...])` → `mgr.submit(js)` → Signal 연결로 동작. 명령이 `mgr.*` 한 곳이라 API 교체·표면 이동 없음 |

---

## 9. 모듈 구조

```
lsfmgr/
├── __init__.py          # LsfJobManager, JobSet, JobSpec, JobState, JobRecord, ... export
├── qt.py                # qtpy re-export 단일 지점
├── options.py           # Options(frozen), resolve_options(), 검증(OPT-1~4)
├── config.py            # LsfConfig, JobSpec (Qt 비의존)
├── states.py            # JobState(+is_inactive), JobRecord(+merge_id/user_data), JobSetRecord
├── reports.py           # SubmitReport/Progress, KillReport/Progress
├── errors.py            # LsfmgrError, JobSet(Closed|NotFound)Error, JobNotFoundError, ...
├── command.py           # LsfCommand 래퍼 (Qt 비의존, chunking, ARG_MAX, chunk 격리)
├── store/               # base(ABC) / memory   (sqlite 제거)
├── submitter.py         # QThreadPool submit + retry + progress/cancel
│                        #   + resubmit_existing (레코드 리셋 재제출) + pre_submit 게이트
├── lifecycle.py         # SubmitGate / KillScope — kill 우선권 barrier (CS-11)
├── monitor.py           # PollingService (QThread+QTimer) + query_once + chunk 격리/회로차단
├── killer.py            # kill 전략 + verify + 확인 재시도 + slot ledger
├── handlers.py          # JobSetHandlerService — job별 주기 handler (FR-7)
├── jobset_core.py       # JobSet 도메인 로직 (create_jobs/merge_from/remove_jobs/clear)
├── handle.py            # JobSet 핸들 (조회 + Signal 전용 뷰)
└── manager.py           # LsfJobManager: 명령 일원화 진입점 + 옵션 해석 + AUTO-1~3 + shutdown
```

Qt 비의존 유지: options/config/states/command/store/jobset_core (Qt 없이 테스트 가능).
*(v7의 resubmit.py, store/sqlite.py, ArrayJobSpec는 제거됨.)*

---

## 10. 수용 기준 (Acceptance Criteria)

1. 5,000개 submit — ID 파싱 100% 또는 실패분 정확 분류
2. 5,000개 kill — 부착물 기반 명령 1회, ARG_MAX 에러 없음
3. 부착물 전부 유실 시에도 JobSet만으로 조회/kill 동작
4. 요약 합계 == intended_count (생성/merge/remove 후에도)
5. polling 호출 횟수 ∝ JobSet 수 (job 수 아님)
6. bjobs 소실 → bhist → LOST 누락 없음, 조회 실패 섞이면 보류(FR-4.3)
7. GUI 응답성 — main 스레드 100ms 이상 정지 없음
8. PyQt5·PySide6 각각 전체 테스트 통과 (`QT_API` 전환만으로)
9. 동시성 — submit+polling+kill 동시 수행 시 무결성, **kill 우선권**(진행 중 submit
   중 kill 시 미제출분 CREATED 복귀·제출분 kill, SUBMITTING 유출 없음)
10. Store 계약 테스트 통과, InMemory 파일 미생성
11. **명령 일원화**: 모든 명령은 `mgr.*`, JobSet 핸들에 명령 메서드 없음
12. shutdown 후 잔여 스레드 없음 (AUTO-3 자동 연결 포함)
13. LSF mock 주입 단위 테스트 가능
14. **단일 진입점**: `create_jobset([...])` → `mgr.submit(js)` → `jobset_updated` 연결로 동작
15. **옵션 계층**: 내장 < manager < call 우선순위, 오타 `TypeError`, 범위 위반 `ValueError`
16. **JobSet Signal**: 해당 JobSet 이벤트만 수신, `mgr.*` Signal과 이중 발행 일치
17. **handler (FR-7)**: start/end state 구간 준수(시작 전 미발화·종료 시 final 1회),
    예외 격리, 폴링 사이클 구동, 재제출 후 재무장
18. **생성/merge (FR-5.4/5.5)**: create_jobset가 유일 생성 경로, 추가는 merge만,
    merge_id replace 시 물리 키 유지·요약 불변식, force는 레코드만
19. **재실행**: `mgr.merge(js, fix) + mgr.submit(js)`로 실패분 교체 후 전체 재실행,
    원 제출 옵션(spec_json)·merge_id·user_data 보존
20. **pre_submit 게이트 (FR-9)**: False/예외 시 레코드 원상, 신호 순서 보장,
    통과 후에만 rearm/AUTO-1

---

## 11. v7 → v9 변경 요약

| 영역 | v7 | v9 |
|---|---|---|
| 명령 표면 | High-level JobSet 메서드 + Low-level Facade | **`mgr.*` 한 곳**, 핸들=조회+Signal 뷰 |
| job 생성 | `submit(list)`·`submit_wrapper`·`create_job(s)`·`add_job` | **`create_jobset([...])` 한 곳** |
| job 추가 | add_job / merge | **merge만** |
| 재실행 | `resubmit_jobs` (FR-8) | **merge + submit** |
| 제출 방식 | bulk/array 자동선택(`mode`, AUTO-4) | **jobset 전 job 재제출**(array 제출 없음) |
| 저장소 | InMemory + SQLite(영속·복원) | **InMemory 단일** |
| 신설 | — | **pre_submit 게이트(FR-9)**, **kill 우선권 SubmitGate**, merge_id/user_data, chunk 격리·회로차단 |
