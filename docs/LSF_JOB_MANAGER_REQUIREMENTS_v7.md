# LSF Job Manager — Requirements Specification

> **버전**: v7.1 (2026-07-06) — [v7.1] 표기 항목 추가:
> job별 주기 handler(FR-7), 상태 기반 재실행 resubmit_jobs(FR-8),
> remove_job, LSF 실행 정보 수집(run_time/start_time/finish_time/exec_cwd)
> **형태**: Qt 전용 Python 라이브러리 — **qtpy** 기반
> PyQt5 / PySide2 / PyQt6 / PySide6 전부 호환
> **환경**: Linux, NFS 다중 사용자(~300명), LSF cluster, 폐쇄망

---

## 0. 목적 및 범위

Qt GUI 애플리케이션(SimManager 등)에서 LSF cluster로 대량 시뮬레이션 job을
submit / monitoring / kill 하는 라이브러리를 구현한다.

핵심 문제:
1. 수천 개 job의 submit/kill/조회 시 LSF master 부하와 ARG_MAX 제한
2. submit 실패·ID 파싱 실패로 인한 job 손실 추적
3. 대량 job의 논리적 묶음(JobSet) 단위 관리
4. **GUI freeze 방지** — 모든 LSF 호출은 백그라운드, 통지는 Signal
5. 저장소 이중 모드: 인메모리(기본) / SQLite(영속)
6. **간결한 사용성** — 최소 3줄로 submit+모니터링, 세부 옵션은 필요할 때만

### 0.1 Qt 바인딩 호환 (필수)

- 모든 Qt import는 `qtpy` 경유만 허용 (내부는 `lsfmgr/qt.py` 단일 지점):
  ```python
  from qtpy.QtCore import QObject, QThread, QTimer, Signal, QThreadPool, QRunnable
  ```
- 지원: PyQt5, PySide2, PyQt6, PySide6 (`QT_API` 자동 감지)
- 바인딩별 차이는 qtpy shim, 불가피 시 compat 모듈 한 곳에서만 분기

---

## 1. API 계층 구조 (v7 신설)

두 계층을 제공하되 **내부 구현은 공유**한다.

```
High-level (기본 권장)     JobSet 객체 — js = mgr.submit(...); js.updated.connect(...)
Low-level  (고급/대시보드)  전역 Facade Signal — mgr.jobset_updated(jsid, ...) 등 (v6 유지)
```

### 1.1 High-level: JobSet

- `mgr.submit(...)`은 jobset_id 문자열이 아니라 **JobSet 객체(QObject)**를 반환
- JobSet 객체는 자신 전용 Signal을 가짐 → jsid 필터링 불필요
- 라이프사이클 자동화:
  - **AUTO-1**: submit 반환 시 polling 자동 시작 (`auto_poll=False`로 해제)
  - **AUTO-2**: JobSet 전원 terminal 도달 시 해당 polling 자동 중지
  - **AUTO-3**: `LsfJobManager` 생성 시 `QApplication.aboutToQuit`에
    `shutdown()` 자동 연결 (명시 호출도 가능, 중복 안전/멱등)
  - **AUTO-4**: `submit()`은 입력을 보고 방식 자동 선택 — 동일 command 패턴
    (또는 `$LSB_JOBINDEX` 치환 가능)이면 array, 아니면 bulk parallel.
    강제하려면 `mode="array" | "bulk"` 지정
- 입력 간소화: `submit()`은 `list[JobSpec]` 외에 `list[str]`(command 문자열)도
  허용 (JobSpec 자동 변환)

### 1.2 옵션 처리 원칙 (v7 핵심) — 3단 계층

모든 튜닝 파라미터는 **안 주면 기본값, 주면 그 호출에만 적용**:

```
① 라이브러리 내장 기본값 (defaults)
        ↓ 덮어씀
② LsfJobManager 생성 시 옵션 (앱 전역 기본값)
        ↓ 덮어씀
③ submit()/kill() 호출 시 키워드 인자 (per-call)
```

```python
# ① 내장 기본값만으로
mgr = LsfJobManager()
js = mgr.submit(jobs)

# ② 앱 전역 기본값 변경
mgr = LsfJobManager(workers=32, max_retry=5, poll_interval_s=5,
                    default_queue="priority")
js = mgr.submit(jobs)                        # 위 값이 적용됨

# ③ 이번 호출만 override
js = mgr.submit(jobs, workers=8, max_retry=0, queue="short",
                label="quick_check", auto_poll=False)
```

**옵션 카탈로그** (전 계층 공통 이름 — 이름 불일치 금지):

| 옵션 | 내장 기본값 | 적용 계층 | 설명 |
|---|---|---|---|
| `workers` | 16 | ②③ | 병렬 submit worker 수 (1~32) |
| `max_retry` | 3 | ②③ | submit 실패 재시도 횟수 (0=재시도 없음) |
| `retry_backoff` | "fixed:2" | ②③ | "fixed:N초" 또는 "expo:base초" |
| `rate_limit_per_s` | None(무제한) | ②③ | 초당 bsub 상한 |
| `poll_interval_s` | 10 | ②③ | polling 주기 (5~60) |
| `auto_poll` | True | ②③ | submit 후 polling 자동 시작 |
| `mode` | "auto" | ③ | "auto"/"array"/"bulk" |
| `queue` | LSF 기본 | ②(default_queue)③ | 대상 queue |
| `resource_req` | None | ②③ | `-R` 문자열 |
| `output_dir` | None | ②③ | `-o`/`-e` 경로 규칙 |
| `submit_timeout_s` | 30 | ②③ | bsub 1건 timeout |
| `chunk_size` | 200 | ② | chunking fallback 크기 |
| `verify_kill` | False | ②③(kill) | kill 후 실제 종료 확인 |
| `label`, `tags`, `description` | "" / () / "" | ③ | JobSet 메타데이터 |
| `persistent` | False | ② | True 시 SqliteStore 사용 |
| `db_path` | ~/.lsfmgr/jobsets.db | ② | persistent=True일 때 |
| `lsf_group_root` | /lsfmgr | ② | LSF group 경로 root |
| `bsub_path` 등 명령 경로 | PATH 탐색 | ② | LSF 명령 위치 |

- 구현 규칙: **OPT-1** 옵션 해석은 `resolve_options(call_kwargs) -> Options`
  한 함수로 일원화 (defaults → manager → call 순 merge, frozen dataclass 반환).
  **OPT-2** 알 수 없는 키워드는 즉시 `TypeError` (오타 조기 발견).
  **OPT-3** 범위 검증 (workers 1~32 등) 위반 시 `ValueError`.
  **OPT-4** 세부 제어가 필요한 사용자를 위해 `LsfConfig` 객체 주입도 계속 지원
  (`LsfJobManager(config=cfg)`) — kwargs와 config 동시 지정 시 kwargs 우선.

### 1.3 JobSet 인터페이스

```python
class JobSet(QObject):
    # 이 JobSet 전용 Signal (jobset_id 인자 없음)
    updated  = Signal(dict)            # 요약 {"total":.., "RUN":.., ...}
    progress = Signal(int, int)        # submit 진행 (done, total), throttled
    finished = Signal(object)          # SubmitReport (retry 포함 최종)
    failed   = Signal(list)            # SUBMIT_FAILED/EXIT/LOST 변경분
    killed   = Signal(object)          # KillReport
    handler_finished = Signal(str, object)  # [v7.1] handler_name, HandlerResult
    error    = Signal(str)             # worker 예외 등

    # 제어 (전부 비동기 — 즉시 반환, 결과는 Signal)
    def kill(self, only_state: JobState | None = None,
             verify: bool | None = None): ...
    def cancel(self): ...              # 진행 중 submit/resubmit 중단
    def refresh(self): ...             # 1회 강제 조회 (query_once)
    def start_polling(self, interval_s: int | None = None): ...
    def stop_polling(self): ...
    def resubmit_jobs(self, job_keys, *, commands=None,
                      verify: bool = True): ...  # [v7.1] 상태 기반 재실행 (FR-8)
    def close(self): ...               # 종결 (전원 terminal일 때)
    def merge_with(self, *others: "JobSet") -> "JobSet": ...
    def add_job(self, record: JobRecord, sync_lsf: bool = True): ...
    def remove_job(self, job_key: str) -> JobRecord: ...  # [v7.1] 편입 취소

    # [v7.1] job별 주기 handler (FR-7)
    def add_handler(self, name: str, fn, *, interval_s: float = 10.0,
                    start_states=None, end_states=None): ...
    def remove_handler(self, name: str): ...

    # 조회 (전부 동기 — Store 스냅샷, LSF 호출 없음)
    @property
    def id(self) -> str: ...
    @property
    def summary(self) -> dict: ...
    @property
    def is_done(self) -> bool: ...     # 전원 terminal
    @property
    def failed_jobs(self) -> list[JobRecord]: ...
    def jobs(self, states: set[JobState] | None = None) -> list[JobRecord]: ...
    def detect_lost(self) -> list[JobRecord]: ...
```

- JobSet 재획득: `mgr.jobset(jobset_id) -> JobSet` (복원/검색 결과에서)
- JobSet 수명: manager가 소유, JobSet close/삭제 시까지 유효.
  파괴된 JobSet 접근 시 `JobSetClosedError`

### 1.4 Low-level Facade (v6 유지, 요약)

여러 JobSet 통합 대시보드용. 전역 Signal:
`submit_started/progress/finished`, `jobset_updated`, `jobs_updated`,
`job_lost`, `kill_finished`, `handler_finished`, `error_occurred`
(모두 jobset_id 인자 포함).
JobSet Signal은 이 위에 얹힌 편의 계층 (동일 이벤트 이중 발행).

---

## 2. 용어 정의 — 혼동 방지 필수

**코드에서 bare "group" 사용 금지.**

| 용어 | 코드 명칭 | 정의 |
|---|---|---|
| **JobSet** | `jobset_id`, `JobSet` 객체 | 본 라이브러리의 논리적 job 묶음. 모든 기능의 기본 단위 |
| **LSF Job Group** | `lsf_group_path` | LSF native (`bsub -g`). 1회 호출 최적화 **수단** |
| **LSF Job Name** | `lsf_job_name` | LSF native (`bsub -J`). 패턴 조회/kill **수단**, fallback |
| **Array Job** | `array_job_id` | LSF native array. ID 하나로 N개 element 관리 **수단** |

관계 규칙 (v6 동일):
- JobSet이 유일한 논리 단위, LSF 세 가지는 **부착물(attachment)**
- 부착물 0개 이상·혼재 가능, merge 시 목록 누적, 조회/kill 시 전부 순회
- 부착물 전부 유실 시에도 job_id chunking으로 동작 (graceful degradation)
- JobSet 자체 기능: 내부 상태 관리, label/tags/description, parent 계층, 검색

---

## 3. 상태 모델 (v6 동일)

```python
class JobState(Enum):
    # 내부 상태 (LSF 도달 전 / 추적 불가)
    CREATED; SUBMITTING; RETRY_WAIT; SUBMIT_FAILED; LOST
    # LSF native
    PEND; RUN; DONE; EXIT; PSUSP; USUSP; SSUSP; UNKWN; ZOMBI
```

- 헬퍼: `is_terminal` {DONE, EXIT, SUBMIT_FAILED, LOST} / `is_failed` /
  `is_on_lsf`
- 전이: `CREATED → SUBMITTING → PEND → RUN → DONE|EXIT`,
  실패 시 `RETRY_WAIT`(n<N) 또는 `SUBMIT_FAILED`(n==N),
  bjobs·bhist 모두 조회 불가 → `LOST`
- 전이는 Store 경유만. Sqlite 모드는 전이마다 이력 event 기록
- `JobRecord`/`JobSetRecord`: frozen dataclass (v6 §2.3과 동일 필드)
- **불변식: 요약 상태별 합계 == intended_count**

---

## 4. Qt 스레딩 — GUI Freeze 방지 (v6 동일 + 명시 강화)

- **QT-0 (API 계약)**: 제어 API(submit/kill/refresh/polling)는 **모두 즉시
  반환하는 비동기**이며 결과는 Signal로만 도착. 조회 API(summary/jobs)는
  **동기이지만 Store 스냅샷만** 읽음(LSF 호출 없음, ms 단위).
  모든 public API의 docstring에 [async→Signal] / [sync, snapshot] 표기 필수
- QT-1: main 스레드에서 blocking LSF 호출 금지
- QT-2: worker → main 통지는 Signal (자동 queued connection)
- QT-3: Signal 인자는 불변 객체만
- QT-4: batch Signal — job 단위 emit 금지, jobset 요약 + 변경분 리스트
- QT-5: progress Signal throttle (1% 또는 100ms)
- QT-6: cancel은 job 경계 안전 지점에서, submit된 job은 정상 기록
- 스레딩 구성: submit=QThreadPool+QRunnable / polling=전용 QThread+소속 QTimer /
  kill·단발조회=QThreadPool / retry 대기=QTimer 스케줄 (sleep 금지)
- shutdown(): requestInterruption + join, 좀비 스레드 금지 (멱등)

---

## 5. 저장소 — 이중 백엔드 (v6 동일)

```
JobSetStore(ABC) ── InMemoryStore(기본) | SqliteStore(persistent=True)
```

- 선택: `LsfJobManager(persistent=True[, db_path=...])` 또는 store 객체 주입
- 공통 API: JobSet/JobRecord CRUD, `transition()`(원자적), summary, search
- **SqliteStore 전용** (InMemory에서 호출 시 `PersistenceNotSupportedError`):
  `list_orphan_jobsets / recover_jobset / reconcile / search_all_sessions /
  get_history / stats / archive / vacuum / export_jobset`
- 복원은 자동 수행 안 함 — 앱이 orphan 목록 보고 결정.
  `mgr.recover_jobset(id) -> JobSet` 반환 (High-level 통합)
- InMemory: 파일 미생성. 앱 사망 시에도 LSF group이 LSF에 잔존하므로
  수동 확인/정리 가능

---

## 6. 기능 요구사항 (FR) — v6 유지, 변경분만 표기

- **FR-1 Submission**: sequential / QThreadPool parallel / array,
  `$LSB_JOBINDEX` dispatch 자동 생성, 식별자(부착물) 자동 부여,
  부착물 실패해도 submit 진행. **[v7] `submit()` 통합 진입점 + mode 자동 선택
  (AUTO-4), 옵션은 §1.2 계층으로 해석**
- **FR-2 Retry**: 실패 감지(exit≠0/파싱 실패/timeout) + fail_reason 분류,
  최대 `max_retry`회, `retry_backoff` 정책, 성공 시 동일 JobSet 편입
- **FR-3 Kill**: 전략 우선순위 ①`bkill -g` ②array ③`-J` 패턴 ④chunking,
  부분 kill(`only_state`), verify 옵션. **[v7] `js.kill()` JobSet 메서드 추가**
  - **[v7.1] FR-3.4 확인+재시도**: bkill 출력의 `Job <id> is being terminated`
    확인 문구를 파싱해 미확인분을 재시도(`kill_max_retry`), `KillReport`에
    `unconfirmed`/`kill_retries` 보고
  - **[v7.1] FR-3.5 kill 상태 정책** (`kill_status_policy`): `"optimistic"`(기본)
    = terminated 확인 시 즉시 EXIT로 전이(`KillReport.changed`, jobs_updated/
    jobset_updated 발화, EXIT는 terminal이라 폴링 제외) / `"actual"` = 실제 LSF
    상태(verify/폴링)로만 EXIT 반영. bkill이 비동기임을 앱이 정책으로 선택
- **FR-4 Monitoring**: 조회 전략 group→array→name→chunking,
  `is_on_lsf`만 조회, bhist fallback→LOST, polling은 batch 반영 후 Signal.
  **[v7] AUTO-1/AUTO-2 자동 polling 라이프사이클**
- **FR-5 JobSet 관리**: 요약(불변식), 손실 감지(name 패턴 복구), 추가,
  merge(부착물 누적), 메타데이터/검색, close.
  **[v7] JobSet 메서드로도 노출 (§1.3)**
- **FR-6 세션 복원 (Sqlite)**: orphan 감지(자동 복원 없음), recover+reconcile,
  이력/통계
- **FR-7 JobSet Handler [v7.1]**: JobSet에 **이름 있는 handler**를 등록해
  `interval_s`초마다 job별로 worker 스레드에서 실행. `start_states`에 든 job부터
  시작, `end_states` 도달 시 `final=True`로 최종 1회 실행 후 그 job 종결.
  결과(반환값/예외)는 `handler_finished(jobset_id, name, HandlerResult)`로 전달
  (1회 실행 완료마다 발행). 전원 최종 실행 완료 시 **휴면**(타이머 정지, 등록
  유지) — resubmit 재실행 시 자동 재무장/재가동. end_states에 없는 terminal로
  죽은 job은 최종 실행 없이 종결(무한 발화 금지). handler 인자는
  `HandlerContext`(record/job_id/working_dir/final). tick은 main, 실행은
  QThreadPool worker (GUI freeze 금지), 예외는 격리(`HandlerResult.error`)
- **FR-8 재실행 (resubmit_jobs) [v7.1]**: 지정 job들을 **현재 상태 기반**으로
  재실행 — 살아있으면(is_on_lsf) kill(+verify) 후, 아니면 바로 재제출.
  레코드 재사용(같은 job_key, intended_count 유지), 제출 경로는 job 단위
  `via_wrapper`로 복원(merge 혼합 jobset 안전), 원 제출 옵션은 `spec_json`으로
  보존. 결과는 submit 계열 Signal(`submit_started→finished`). polling을 쓰던
  JobSet은 자동 재개(AUTO-2 복구), kill-phase 중 cancel 지원, 진행 중 재호출
  거부. LSF 실행 정보(run_time/start_time/finish_time/exec_cwd)는 FR-4
  polling이 bjobs -o로 수집해 JobRecord에 채운다

---

## 7. 동시성 안전 (CS) — v6 동일

- CS-1 Store thread-safe(QMutex/RLock), transition 원자성
- CS-2 frozen dataclass — Signal/스레드 공유 안전
- CS-3 SQLite connection 스레드 공유 금지 (thread-local 또는 단일 writer)
- CS-4 동일 JobSet 중복 polling 방지
- CS-5 worker 예외 격리 → error Signal + logger.exception (traceback 로그)
- CS-6 rate limiter thread-safe
- CS-7 Store 쓰기 경로 일원화
- CS-8 shutdown 시 job_id 유실 방지 (bsub 완료 단위 즉시 반영)
- CS-9 db_path 로컬 디스크 권장, NFS 시 경고+busy_timeout
- CS-10 LSF group 경로 사용자 격리
- (multiprocessing 미사용 — subprocess는 GIL 해제, Qt는 fork-unsafe)

---

## 8. 비기능 요구사항 (NFR)

| ID | 요구사항 |
|---|---|
| NFR-1 | Qt import는 qtpy 경유만, 4개 바인딩 동일 동작 |
| NFR-2 | 의존성: qtpy + Qt 바인딩 1종 + stdlib (그 외 금지) |
| NFR-3 | GUI freeze 금지 — 5,000 job 처리 중 main 스레드 100ms 이상 정지 없음 |
| NFR-4 | LSF 부하 보호 — 부착물 1회 호출 우선, chunking 최후, rate limit |
| NFR-5 | ARG_MAX 안전 — 인자 길이 검사 + chunk 상한 |
| NFR-6 | 로깅: `lsfmgr.*` 계층. 레벨 규약 — DEBUG=LSF 명령 원문/stdout/stderr, INFO=submit/kill/전이, WARNING=retry·부착물 실패·NFS, ERROR=SUBMIT_FAILED/LOST 확정·worker 예외(traceback). SimManager 중앙 로깅 연계 가능 |
| NFR-7 | 설정 configurable — §1.2 옵션 카탈로그 + LsfConfig 주입 |
| NFR-8 | 테스트: LSF mock 주입, 두 Store 계약 테스트, 동시성, pytest-qt Signal, PyQt5+PySide6 CI |
| NFR-9 | Python 3.9+ |
| NFR-10 | **[v7] 최소 사용성**: 기본값만으로 3줄 이내에 submit+모니터링 동작. 옵션 미지정과 지정 코드가 자연스럽게 연속 (API 교체 불필요) |

---

## 9. 모듈 구조

```
lsfmgr/
├── __init__.py          # LsfJobManager, JobSet, JobSpec, JobState export
├── qt.py                # qtpy re-export 단일 지점
├── options.py           # [v7] Options(frozen), resolve_options(), 검증
├── states.py            # JobState, JobRecord, JobSetRecord (frozen)
├── errors.py            # PersistenceNotSupportedError, JobSetClosedError 등
├── command.py           # LsfCommand 래퍼 (Qt 비의존, chunking, ARG_MAX)
├── store/               # base(ABC) / memory / sqlite
├── submitter.py         # QThreadPool submit + retry + progress/cancel
│                        #   + resubmit_existing (레코드 리셋 재제출, FR-8)
├── monitor.py           # PollingService (QThread+QTimer) + query_once
├── killer.py            # kill 전략 + verify
├── resubmit.py          # [v7.1] ResubmitCoordinator — kill→재제출 조율 (FR-8)
├── handlers.py          # [v7.1] JobSetHandlerService — job별 주기 handler (FR-7)
├── jobset_core.py       # JobSet 도메인 로직 (CRUD/요약/손실/merge)
├── handle.py            # [v7] JobSet 객체(QObject) — 전용 Signal, 위임 메서드
└── manager.py           # LsfJobManager: Facade Signal + JobSet 발급 + 옵션 해석
                         #   + AUTO-1~4 + shutdown
```

Qt 비의존 유지: options/states/command/store/jobset_core (Qt 없이 테스트 가능)

---

## 10. 수용 기준 (Acceptance Criteria)

1. 5,000개 submit — ID 파싱 100% 또는 실패분 정확 분류
2. 5,000개 kill — 부착물 기반 명령 1회, ARG_MAX 에러 없음
3. 부착물 전부 유실 시에도 JobSet만으로 조회/kill 동작
4. 요약 합계 == intended_count (불변식)
5. polling 호출 횟수 ∝ JobSet 수 (job 수 아님)
6. bjobs 소실 → bhist → LOST 누락 없음
7. GUI 응답성 — main 스레드 100ms 이상 정지 없음
8. PyQt5·PySide6 각각 전체 테스트 통과 (`QT_API` 전환만으로)
9. 동시성 — submit+polling+kill 동시 수행 시 양 백엔드 무결성
10. Store 계약 테스트 양 백엔드 통과, InMemory 전용 API 호출 시 예외,
    InMemory 파일 미생성
11. 복원(Sqlite) — kill 후 재시작 → orphan → recover+reconcile 정확 반영
12. shutdown 후 잔여 스레드 없음 (AUTO-3 자동 연결 포함 검증)
13. LSF mock 주입 단위 테스트 가능
14. **[v7] 간소화 API**: `mgr = LsfJobManager(); js = mgr.submit(cmds);
    js.updated.connect(f)` 3줄로 동작 (polling 자동 시작 포함)
15. **[v7] 옵션 계층**: 내장 기본값 < manager kwargs < call kwargs 우선순위
    검증, 오타 키워드 `TypeError`, 범위 위반 `ValueError`
16. **[v7] JobSet Signal**: js.updated가 해당 JobSet 이벤트만 수신
    (타 JobSet 이벤트 미수신), Facade Signal과 이중 발행 일치
17. **[v7.1] handler (FR-7)**: start/end state 구간 준수(시작 전 미발화·종료 시
    final 정확히 1회), 예외 격리(`HandlerResult.error`), 전원 완료 시 휴면,
    resubmit 후 재무장/재가동, end 미포함 terminal은 무발화 종결
18. **[v7.1] resubmit (FR-8)**: 살아있는 job kill 후 재제출·레코드 재사용
    (intended 유지), 원 제출 옵션(queue/resources 등) 보존, merge 혼합
    jobset에서 job별 경로 정확, 중복 key dedupe, 진행 중 재호출 거부,
    kill-phase cancel 시 submit_started/finished Signal 짝 유지

---

## 11. 구현 순서 제안

1. `qt.py`, `states.py`, `errors.py`, `options.py` — 기반 + 옵션 해석 테스트
2. `command.py` — Qt 비의존 LSF 래퍼
3. `store/` — base + memory + 계약 테스트
4. `jobset_core.py` — 도메인 로직 (인메모리 기준)
5. `submitter.py` — sequential → parallel → array → retry → progress/cancel
6. `killer.py`, 7. `monitor.py`
8. `manager.py` + `handle.py` — Facade/JobSet/AUTO-1~4 + pytest-qt 테스트
9. `store/sqlite.py` — 공통 → 계약 테스트 → 전용 API
10. PyQt5 ↔ PySide6 크로스 바인딩 테스트
