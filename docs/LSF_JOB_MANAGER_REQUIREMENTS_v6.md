# LSF Job Manager — Requirements Specification

> **버전**: v6 (2026-07-04)
> **형태**: Qt 전용 Python 라이브러리 — **qtpy** 기반으로
> PyQt5 / PySide2 / PyQt6 / PySide6 전부 호환
> **환경**: Linux, NFS 다중 사용자(~300명), LSF cluster, 폐쇄망

---

## 0. 목적 및 범위

Qt GUI 애플리케이션(SimManager 등)에서 LSF cluster로 대량 시뮬레이션 job을
submit / monitoring / kill 하는 라이브러리를 구현한다.

핵심 문제:
1. 수천 개 job의 submit/kill/조회 시 LSF master 부하와 명령줄 길이(ARG_MAX) 제한
2. submit 실패·ID 파싱 실패로 인한 job 손실 추적
3. 대량 job의 논리적 묶음(JobSet) 단위 관리
4. **GUI freeze 방지** — 모든 LSF 호출은 백그라운드 스레드, 통지는 Signal
5. 저장소 이중 모드: 인메모리(기본) / SQLite(영속, 세션 복원)

### 0.1 Qt 바인딩 호환 (필수)

- **모든 Qt import는 `qtpy`를 통해서만** 수행한다:
  ```python
  from qtpy.QtCore import QObject, QThread, QTimer, Signal, Slot, QThreadPool, QRunnable
  ```
  `PyQt5`/`PySide6` 등 바인딩 직접 import 금지 (테스트 코드 포함).
- qtpy가 Signal/Slot/Property 명명 차이(pyqtSignal ↔ Signal)를 흡수한다.
- 지원 대상: PyQt5, PySide2, PyQt6, PySide6 — 앱이 어떤 바인딩을 쓰든
  `QT_API` 환경변수/이미 import된 바인딩을 qtpy가 자동 감지.
- 바인딩별 API 차이가 있는 부분(exec_ vs exec, enum 접근 방식 등)은
  qtpy 제공 shim 사용, 불가피하면 내부 compat 모듈 한 곳에서만 분기.

---

## 1. 용어 정의 (Terminology) — 혼동 방지 필수

**코드에서 bare "group"이라는 이름 사용 금지.**

| 용어 | 코드 명칭 | 정의 | 관리 주체 |
|---|---|---|---|
| **JobSet** | `JobSet`, `jobset_id` | 본 라이브러리가 자체 관리하는 논리적 job 묶음. LSF와 무관하게 존재. 모든 기능의 기본 단위 | 본 라이브러리 (Store) |
| **LSF Job Group** | `lsf_group_path` | LSF native (`bsub -g /path`). `bjobs/bkill -g` 1회 호출 최적화 **수단** | LSF |
| **LSF Job Name** | `lsf_job_name` | LSF native (`bsub -J name`). 패턴 조회/kill **수단**, fallback 식별자 | LSF |
| **Array Job** | `array_job_id` | LSF native array. Job ID 하나로 N개 element 관리 **수단** | LSF |

### 1.1 관계 규칙

- **JobSet이 유일한 논리 단위.** LSF Job Group / Job Name / Array는
  JobSet을 효율적으로 실행하기 위한 **부착물(attachment)**.
- 하나의 JobSet은 0개 이상의 부착물을 가질 수 있고, 종류가 혼재할 수 있다
  (예: array 2개 + retry로 재submit된 개별 job 다수).
- merge된 JobSet은 여러 부착물을 동시에 보유 → 조회/kill 시 전부 순회.
- 부착물이 전부 유실되어도 JobRecord(job_id 목록)만으로 chunking 방식
  조회/kill 가능 (graceful degradation).

### 1.2 JobSet 자체 관리 기능 (LSF 비의존)

- job_id 목록 기반 상태 추적 (chunked bjobs)
- 내부 상태(SUBMIT_FAILED, LOST 등) 관리
- 메타데이터: label, tag(복수), description, 생성자, 생성 시각
- 계층 구조(옵션): parent_jobset_id
- tag/label 기반 검색

---

## 2. 상태 모델 (State Model)

### 2.1 JobState enum

```python
class JobState(Enum):
    # --- 내부 상태 (LSF 도달 전 / 추적 불가) ---
    CREATED        = "CREATED"
    SUBMITTING     = "SUBMITTING"
    RETRY_WAIT     = "RETRY_WAIT"      # submit 실패 후 재시도 대기 (n/N회)
    SUBMIT_FAILED  = "SUBMIT_FAILED"   # N회 재시도 모두 실패 (최종)
    LOST           = "LOST"            # ID 미확보/조회 불가 (최종)

    # --- LSF native 상태 ---
    PEND  = "PEND"
    RUN   = "RUN"
    DONE  = "DONE"     # exit 0
    EXIT  = "EXIT"     # exit != 0
    PSUSP = "PSUSP"
    USUSP = "USUSP"
    SSUSP = "SSUSP"
    UNKWN = "UNKWN"
    ZOMBI = "ZOMBI"
```

헬퍼: `is_terminal` {DONE, EXIT, SUBMIT_FAILED, LOST} /
`is_failed` {EXIT, SUBMIT_FAILED, LOST} /
`is_on_lsf` (bjobs 조회 대상 여부)

### 2.2 상태 전이

```
CREATED → SUBMITTING → PEND → RUN → DONE / EXIT
              │
              ├─ (실패, n < N) → RETRY_WAIT → SUBMITTING
              └─ (실패, n == N) → SUBMIT_FAILED
PEND/RUN 등 → (bjobs·bhist 모두 조회 불가) → LOST
```

전이는 Store를 통해서만 수행. Sqlite 모드에서는 전이마다 이력 event 기록.

### 2.3 데이터 구조

```python
@dataclass(frozen=True)          # 불변 — 갱신은 replace()
class JobRecord:
    job_id: int | None            # SUBMIT_FAILED이면 None
    array_index: int | None
    jobset_id: str
    lsf_job_name: str             # "<jobset_id>_<idx>"
    state: JobState
    fail_reason: str | None       # "NO_JOBID_PARSED"|"BSUB_TIMEOUT"|...
    retry_count: int
    exit_code: int | None
    submit_time: datetime | None
    command: str                  # retry 재submit용
    updated_at: datetime

@dataclass(frozen=True)
class JobSetRecord:
    jobset_id: str
    intended_count: int           # 손실 감지 기준
    lsf_group_paths: list[str]    # 부착물 (0개 이상, 혼재 가능)
    name_patterns: list[str]
    array_job_ids: list[int]
    label: str
    tags: list[str]
    description: str
    parent_jobset_id: str | None
    created_by: str
    created_at: datetime
    merged_from: list[str]
    session_id: str
    closed: bool
```

주의: frozen dataclass는 Signal 인자로 안전하게 전달 가능 (불변이라
cross-thread 공유 race 없음).

---

## 3. Qt 스레딩 아키텍처 — GUI Freeze 방지 (필수)

### 3.1 원칙

- **QT-1**: GUI(main) 스레드에서 LSF subprocess를 직접 실행하는 public API 금지.
  모든 bsub/bjobs/bkill/bhist는 worker 스레드에서 실행.
- **QT-2**: 결과 통지는 **Qt Signal**로만. worker → main 스레드 간 Signal은
  Qt가 자동으로 QueuedConnection 처리하므로 slot은 main 스레드에서 안전하게
  UI 갱신 가능 (수동 marshalling 불필요).
- **QT-3**: Signal 인자는 불변 객체(frozen dataclass, str, dict 복사본)만 전달.
- **QT-4**: 대량 결과(수천 JobRecord)의 Signal 남발 금지 — job 단위 Signal이
  아니라 **batch Signal** (jobset 단위 요약 + 변경분 리스트)로 UI 이벤트 루프
  폭주 방지. UI 갱신 주기와 polling 주기를 분리 가능하게.
- **QT-5**: 진행률 통지: 대량 submit 중 `progress(done, total)` Signal 제공
  (QProgressBar 연동용). emit 빈도 제한 (예: 1% 단위 또는 100ms throttle).
- **QT-6**: 취소 지원: 대량 submit/kill 진행 중 `cancel()` 요청 시
  안전 지점(job 단위 경계)에서 중단, 이미 submit된 job은 JobSet에 정상 기록.

### 3.2 스레딩 구성

| 컴포넌트 | 방식 | 비고 |
|---|---|---|
| 대량 submit | `QThreadPool` + `QRunnable` worker | worker 수 설정 (기본 16), rate limit |
| polling | 전용 `QThread` + 그 스레드 소속 `QTimer` | 주기 실행, jobset별 스케줄 |
| kill / 단발 조회 | `QThreadPool` 단발 task | |
| retry 대기 | polling 스레드의 QTimer 스케줄 | sleep으로 스레드 점유 금지 |

- QThread 종료 규약: `requestInterruption()` + graceful stop,
  앱 종료 시 `shutdown()`으로 모든 스레드 join (좀비 스레드 금지).
- QObject thread affinity 준수: worker QObject는 해당 스레드로 `moveToThread`,
  timer는 소속 스레드에서 start/stop.

### 3.3 Facade Signal 정의 (public API의 핵심)

```python
class LsfJobManager(QObject):          # 앱이 사용하는 단일 진입점(Facade)
    # --- submit ---
    submit_started   = Signal(str)               # jobset_id
    submit_progress  = Signal(str, int, int)     # jobset_id, done, total
    submit_finished  = Signal(str, object)       # jobset_id, SubmitReport
    # --- 상태 ---
    jobset_updated   = Signal(str, dict)         # jobset_id, summary
    jobs_updated     = Signal(str, list)         # jobset_id, [JobRecord] 변경분
    job_lost         = Signal(str, object)       # jobset_id, JobRecord
    # --- kill ---
    kill_finished    = Signal(str, object)       # jobset_id, KillReport
    # --- 에러 ---
    error_occurred   = Signal(str, str)          # jobset_id, message
```

---

## 4. 저장소 아키텍처 — 이중 백엔드

### 4.1 구조

```
JobSetStore (ABC)
├── InMemoryStore     # 기본. dict + QMutex/RLock, 파일 미생성
└── SqliteStore       # 옵션. 영속화, 세션 간 복원 지원
```

```python
mgr = LsfJobManager(store=InMemoryStore())                        # 기본
mgr = LsfJobManager(store=SqliteStore("~/.lsfmgr/jobsets.db"))    # 영속
```

`store.persistent` (bool)로 모드 판별 — GUI에서 복원 메뉴 활성/비활성 분기.

### 4.2 공통 인터페이스 (두 백엔드 공통)

- JobSet CRUD: `create_jobset / get_jobset / update_jobset / delete_jobset / list_jobsets`
- JobRecord: `add_job / update_job / get_jobs(states filter)`
- `transition(jobset_id, job_key, new_state, **fields)` — 원자적 상태 전이
- `summary(jobset_id)` / `search(tag, label, since)` (세션 범위)

### 4.3 SqliteStore 전용 API — `persistent=True`일 때만

InMemoryStore에서 호출 시 `PersistenceNotSupportedError`.

```python
list_orphan_jobsets() -> list[JobSetRecord]   # 이전 세션 미종결 JobSet
recover_jobset(jobset_id) -> JobSetRecord     # 현재 세션으로 복원
reconcile(jobset_id) -> ReconcileReport       # 저장 상태 vs LSF 실상태 대조 갱신
search_all_sessions(...)                      # 세션 간 검색
get_history(jobset_id)                        # 상태 전이 이력
stats(since, until)                           # submit 성공률, 대기시간 분포 등
archive(older_than_days=30) / vacuum() / export_jobset(id, path)
```

- 복원은 자동 수행하지 않음 — 앱이 orphan 목록을 보고 결정 (GUI 다이얼로그 등).
- reconcile은 worker 스레드에서 실행, 완료 시 `jobset_updated` Signal.

### 4.4 모드별 차이 요약

| 항목 | InMemoryStore | SqliteStore |
|---|---|---|
| 파일 생성 | 없음 | db_path 1개 (+WAL 보조) |
| 프로세스 종료 시 | JobSet 소멸 (LSF job은 잔존) | 보존 |
| 복원/reconcile/이력/통계 | `PersistenceNotSupportedError` | 지원 |
| 다중 프로세스 접근 | 불가 | WAL + busy_timeout |

---

## 5. 기능 요구사항 (FR)

### FR-1. Job Submission

- **FR-1.1** Sequential 대량 submit — 순차 bsub, stdout `Job <id>` 파싱
- **FR-1.2** Parallel 대량 submit — QThreadPool 병렬 (worker 기본 16, 1~32),
  rate limit(token bucket), progress Signal(QT-5), cancel(QT-6)
- **FR-1.3** Array Job — `bsub -J "<jobset_id>[1-N]"`, element별 추적,
  command 상이 시 `$LSB_JOBINDEX` dispatch 스크립트 자동 생성
- **FR-1.4** 식별자 자동 부여 — `-g /lsfmgr/<user>/<jobset_id>`,
  `-J <jobset_id>_<idx>`. 부착물 지정 실패해도 submit 진행
- **FR-1.5** Submit 옵션 템플릿 — queue, `-R`, `-o`/`-e`, env

### FR-2. Submit 실패 처리 (Retry)

- **FR-2.1** 실패 감지 — exit != 0 / ID 파싱 실패 / timeout(기본 30s),
  fail_reason 분류·로깅
- **FR-2.2** Retry — 최대 N회(기본 3), 고정 delay/backoff (QTimer 스케줄),
  `RETRY_WAIT` → 최종 `SUBMIT_FAILED`, 성공 시 동일 JobSet 자동 편입

### FR-3. Job Kill

- **FR-3.1** Kill 전략 우선순위 (자동, ARG_MAX 방지):
  ① `bkill -g <path> 0` ② `bkill <array_id>` / `"id[m-n]"`
  ③ `bkill -J "<jobset_id>_*" 0` ④ chunking(100~500) 병렬.
  부착물 복수면 전부 순회, 없으면 ④ 직행
- **FR-3.2** 부분 kill — 상태 조건(`-stat PEND`)/선택 job
- **FR-3.3** Kill 검증(옵션) — 재조회로 실제 종료 확인, `kill_finished` 리포트

### FR-4. 상태 조회 / 모니터링

- **FR-4.1** 조회 전략 우선순위: group → array → name 패턴 → chunking
  (`bjobs -o "jobid stat exit_code jobname" -noheader`)
- **FR-4.2** `is_on_lsf == True`인 job만 조회
- **FR-4.3** bjobs 없음 → bhist fallback → `LOST` (+ `job_lost` Signal)
- **FR-4.4** 주기적 polling — 전용 QThread + QTimer, 주기 설정(기본 10s),
  jobset 단위 조회로 호출 횟수 최소화, 결과는 batch로 Store 반영 후
  `jobset_updated`/`jobs_updated`(변경분만) Signal(QT-4)

### FR-5. JobSet 관리

- **FR-5.1** 생성 — jobset_id 자동(timestamp+uuid)/수동
- **FR-5.2** 요약 — 상태별 카운트, **불변식: 합계 == intended_count**
- **FR-5.3** 손실 감지 — intended_count vs 확보 ID 수,
  name 패턴 조회로 "ID 미확보지만 실제 submit된 job" 복구
- **FR-5.4** job 추가 — 수동/자동(retry), LSF 동기화 옵션(`bmod -g`)
- **FR-5.5** merge — JobRecord 합집합 + intended_count 합산, 부착물 목록 누적,
  원본 유지/삭제 옵션, `merged_from` 기록
- **FR-5.6** 메타데이터/검색 — label/tag/description, 세션 범위(공통) /
  세션 간(Sqlite 전용)
- **FR-5.7** 종결(close) — 전원 terminal 시 close 가능, `bgdel`,
  InMemory: 해제 대상 / Sqlite: closed 마킹 → archive 대상

### FR-6. 세션 복원 (Sqlite 전용)

- **FR-6.1** 시작 시 orphan 목록 제공 (자동 복원 안 함)
- **FR-6.2** recover + reconcile — 죽어있는 동안의 DONE/EXIT/LOST 반영
- **FR-6.3** 이력/통계 조회

---

## 6. 동시성 안전 요구사항 (CS)

- **CS-1**: Store public 메서드 전부 thread-safe (`QMutex` 또는 `RLock`),
  `transition()`은 read-modify-write 원자성
- **CS-2**: JobRecord/JobSetRecord frozen — Signal 인자/스레드 공유 안전
- **CS-3**: SqliteStore connection 스레드 간 공유 금지 —
  thread-local connection 또는 단일 writer 스레드 + queue (WAL 모드)
- **CS-4**: 동일 JobSet 중복 polling 방지 (진행 중 플래그)
- **CS-5**: worker 내 예외가 스레드를 죽이지 않도록 격리 →
  `error_occurred` Signal + 로깅
- **CS-6**: rate limiter thread-safe
- **CS-7**: QThreadPool worker에서 Store 쓰기는 lock 경유 일원화
  (Signal로 결과만 넘기고 main에서 기록하는 방식도 허용 — 택일 후 일관성 유지)
- **CS-8**: 앱 종료 시 shutdown(): 진행 중 submit의 job_id 유실 방지 —
  bsub 완료 단위 즉시 Store 반영 후 스레드 join
- **CS-9**: SqliteStore db_path는 로컬 디스크 권장, NFS 시 경고 로깅 +
  busy_timeout/재시도 (다중 프로세스: WAL)
- **CS-10**: LSF group 경로 `/lsfmgr/<user>/<jobset_id>` 사용자 격리

(참고: multiprocessing은 사용하지 않는다 — subprocess는 GIL을 해제하므로
QThreadPool로 충분하며, Qt 객체는 fork-safe하지 않음)

---

## 7. 비기능 요구사항 (NFR)

| ID | 요구사항 |
|---|---|
| NFR-1 | **Qt 바인딩 호환**: 모든 Qt import는 qtpy 경유. PyQt5/PySide2/PyQt6/PySide6에서 동일 동작. 바인딩 직접 import 금지 |
| NFR-2 | 의존성: `qtpy` + Qt 바인딩 1종 + stdlib. 그 외 외부 패키지 금지 (폐쇄망) |
| NFR-3 | **GUI freeze 금지**: main 스레드에서 blocking LSF 호출 없음 (§3), 5,000 job polling 중에도 UI 응답성 유지 |
| NFR-4 | LSF master 부하 보호 — 부착물 기반 1회 호출 우선, chunking 최후 수단, rate limit |
| NFR-5 | ARG_MAX 안전 — 인자 총 길이 검사 + chunk 상한 강제 |
| NFR-6 | 이벤트 로깅 — `logging` 계층 (`lsfmgr.submit` 등), SimManager 중앙 로깅 연계 가능 |
| NFR-7 | LSF 명령 경로/db_path 등 configurable |
| NFR-8 | 테스트: LSF subprocess mock 주입, 두 Store 백엔드 동일 계약 테스트, 동시성 테스트, `pytest-qt`(qtbot) 기반 Signal 테스트, 최소 2개 바인딩(PyQt5+PySide6)에서 CI 실행 |
| NFR-9 | Python 3.9+ 호환 |
| NFR-10 | 백엔드 전환이 생성자 인자 하나로 가능 (공통 API 한정) |

---

## 8. 모듈 구조

```
lsfmgr/
├── __init__.py          # LsfJobManager(Facade) export
├── qt.py                # qtpy re-export 단일 지점 (내부는 여기서만 import)
├── states.py            # JobState, JobRecord, JobSetRecord (frozen)
├── errors.py            # PersistenceNotSupportedError 등
├── command.py           # LsfCommand: bsub/bjobs/bkill/bhist/bmod/bgdel 래퍼
│                        #   subprocess(shell 미경유), chunking, ARG_MAX 검사
│                        #   ※ Qt 비의존 (순수 Python) — 단위 테스트 용이
├── store/
│   ├── base.py          # JobSetStore(ABC) + persistent flag
│   ├── memory.py        # InMemoryStore
│   └── sqlite.py        # SqliteStore (WAL, 이력, 전용 API)
├── submitter.py         # QThreadPool 기반 submit + retry + progress/cancel
├── monitor.py           # PollingService(QThread+QTimer) + query_once
├── killer.py            # kill 전략 + verify
├── jobset.py            # JobSetManager: CRUD/요약/손실/merge/close/recover
└── manager.py           # LsfJobManager(QObject Facade): Signal 정의(§3.3),
                         #   컴포넌트 조립, shutdown()
```

원칙: `command.py`, `states.py`, `store/`는 **Qt 비의존 순수 Python**으로 유지
(로직 테스트가 Qt 없이 가능). Qt 의존은 submitter/monitor/killer/manager로 한정.

---

## 9. 수용 기준 (Acceptance Criteria)

1. 5,000개 submit 시 ID 파싱 성공률 100% 또는 실패분이
   `SUBMIT_FAILED`/`RETRY_WAIT`로 정확히 분류
2. 5,000개 kill이 부착물 기반 명령 1회로 완료, ARG_MAX 에러 미발생
3. 부착물 전부 유실 상태에서도 JobSet만으로 조회/kill 동작
4. 요약 상태별 합계 == intended_count (불변식)
5. polling 1회당 LSF 호출 횟수가 JobSet 수에 비례 (job 수 X)
6. bjobs 소실 → bhist fallback → LOST 전이 누락 없음
7. **GUI 응답성**: 5,000 job submit+polling 중 main 스레드 블로킹 없음
   (100ms 이상 이벤트 루프 정지 없음)
8. **바인딩 호환**: PyQt5와 PySide6 각각에서 전체 테스트 통과
   (`QT_API` 전환만으로)
9. 동시성: 병렬 submit + polling + kill 동시 수행 시 양 백엔드 무결성 유지
10. 백엔드 계약: 동일 테스트 스위트를 InMemory/Sqlite 모두 통과
11. InMemory에서 전용 API 호출 시 `PersistenceNotSupportedError`,
    InMemory 모드에서 파일 미생성
12. 복원(Sqlite): 프로세스 kill 후 재시작 → orphan 감지 → recover+reconcile로
    그동안의 DONE/EXIT/LOST 정확 반영
13. shutdown() 후 잔여 스레드 없음 (QThread 전부 join)
14. 모든 LSF 명령 mock 주입 단위 테스트 가능

---

## 10. 구현 순서 제안

1. `qt.py`, `states.py`, `errors.py` — 기반 + 단위 테스트
2. `command.py` — Qt 비의존 LSF 래퍼 (chunking, 파싱, ARG_MAX, mock)
3. `store/base.py` + `store/memory.py` — 인터페이스 + 인메모리
   + **계약 테스트 스위트**
4. `jobset.py` — JobSetManager (인메모리 기준 완성)
5. `submitter.py` — sequential → QThreadPool parallel → array → retry
   → progress/cancel Signal
6. `killer.py` — 전략 선택 + verify
7. `monitor.py` — 조회 전략 + PollingService (QThread+QTimer)
8. `manager.py` — Facade 조립 + shutdown + pytest-qt Signal 테스트
9. `store/sqlite.py` — 공통 API → 계약 테스트 → 전용 API
   (recover/reconcile/이력/통계/archive)
10. PyQt5 ↔ PySide6 크로스 바인딩 테스트
