# MockLSF — IBM LSF 가상 시스템

실제 LSF 서버 없이 `bsub`/`bjobs`/`bkill` 등의 명령을 흉내내는 파이썬 구현.
job 을 수백~수천 개 제출하고 상태(PEND→RUN→DONE/EXIT/SUSPEND)를 모니터링하는
PyQt 앱을 실제 LSF 없이 테스트하기 위한 용도.

## 구조

```
mock-lsf/
├── mocklsf/            # 파이썬 패키지
│   ├── config.py       # 큐·호스트·타이밍·실패율 등 설정 (환경변수로 조정 가능)
│   ├── models.py       # Job 데이터 모델, 상태 상수
│   ├── db.py           # SQLite 공유 상태 저장소
│   ├── submit.py       # bsub 인자 파싱, array 파싱, job 계획값 생성
│   ├── scheduler.py    # 가상 스케줄러 (상태 전이의 핵심)
│   ├── daemon.py       # mocklsfd 데몬 관리
│   ├── formats.py      # bjobs 출력 포맷 (기본/-w/-l/-o/-json)
│   └── cli.py          # 각 명령 구현
├── bin/                # 실제 LSF 명령과 같은 이름의 실행 래퍼
│   ├── bsub  bjobs  bkill  bqueues  bhist  bpeek  bstop  bresume
│   └── mocklsfd
└── tests/
```

## 동작 방식

- **`mocklsfd` 데몬**이 백그라운드에서 스케줄러 루프를 돌리며 시간에 따라 job 상태를 전이시킨다.
- 상태는 **SQLite**(`$MOCKLSF_HOME/state.db`)에 저장되어, 각각 독립 프로세스로 실행되는
  `bsub`/`bjobs` 등이 상태를 공유한다. (앱이 각 명령을 subprocess 로 호출하는 구조와 일치)
- 동시 실행량은 **호스트 슬롯 총합**(기본 4호스트 × 16 = 64)으로 제한되므로,
  수천 개를 던지면 자연스럽게 PEND 가 쌓였다가 순차 실행된다.

## 앱 연동

앱에서 실제 LSF 명령 대신 이 가상 명령을 호출하게 하려면 `bin/` 을 PATH 앞에 두면 된다.

```bash
export PATH="/경로/mock-lsf/bin:$PATH"
mocklsfd start          # 데몬 기동 (bsub 최초 호출 시 자동 기동도 됨)

bsub -q normal -J myjob sleep 30
bjobs
```

앱 코드에서 명령 경로를 지정한다면 `bin/bsub` 등을 절대경로로 지정해도 된다.

## 지원 명령

| 명령 | 설명 |
|------|------|
| `bsub` | job 제출. Job ID 반환, 1~2초 지연, 아주 가끔 실패 재현. `-g <group>` 로 job group 지정 |
| `bjobs` | 상태 조회. `-a -w -l -u -J -q -m -g -r -p -s -d -o -noheader -json` |
| `bkill` | job 종료. id / `123[5]` 개별 element / `0`(전체) / `-J` `-u` `-q` `-g`. `-g`·`-J`·`-u`·`-q` 와 함께 온 `0` 은 그 범위로 한정됨 |
| `bstop` / `bresume` | job suspend / resume (USUSP/PSUSP) |
| `bqueues` | 큐 상태·job 카운트 |
| `bhist` | job 상태 전이 이력 |
| `bpeek` | RUN 중 job 의 출력 미리보기 |
| `bmod` | `-g <group> <ids...>` 로 job 을 job group 으로 편입(이동) |
| `bgdel` | 빈 job group 삭제 (no-op 성공) |
| `mocklsfd` | 데몬 제어: `start` `stop` `restart` `status` `reset` |

**Job group** — `bsub -g <path>` 로 지정한 group 을 job 별로 추적한다. `bjobs -g`
로 group 조회, `bkill -g <path> 0` 로 group 단위 종료가 가능하다. lsfmgr 가
jobset 을 `/lsfmgr/<user>/<jobset_id>` group 으로 격리·kill·모니터링하는 구조를
그대로 지원한다.

### bsub 옵션

- `-q <queue>` 큐 지정, `-m "<host...>"` 실행 호스트 지정, `-J <name>` job 이름
- `-n <cpus>` 프로세서 수, `-P <proj>` 프로젝트
- **array job**: `-J "name[1-10]"`, `-J "name[1-5,8,10-12]"`, `%limit` 동시 실행 제한 `-J "name[1-100]%5"`
- `-Is`/`-I`(인터렉티브)는 **지원 안 함 → 무시**
- 그 외 알 수 없는 옵션은 **무시**

### 커스텀 포맷 (수천 개 파싱에 권장)

```bash
bjobs -a -o "jobid stat queue exec_host" -noheader
bjobs -a -o "jobid:8 stat:6 queue" -delimiter '|'
bjobs -a -o "jobid stat queue delimiter=';'"   # -o 스펙 안에 delimiter 지정 (실제 LSF 방식)
bjobs -a -json -o "jobid stat exec_host"
```

`-o` 스펙 문자열 안에 `delimiter='X'` 키워드를 넣으면 그 문자로 컬럼을
구분한다(패딩 없음). 작은따옴표·큰따옴표·따옴표 없는 형태를 모두 허용하며,
스펙 안 delimiter 가 별도 `-delimiter` 인자보다 우선한다.

## 시뮬레이션 파라미터 (환경변수)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MOCKLSF_HOME` | `~/.mocklsf` | 상태 파일 위치 |
| `MOCKLSF_SLOTS_PER_HOST` | 16 | 호스트당 슬롯 수 |
| `MOCKLSF_SUBMIT_DELAY_MIN/MAX` | 1.0 / 2.0 | bsub 제출 지연(초) |
| `MOCKLSF_PEND_MIN/MAX` | 2.0 / 6.0 | 최소 PEND 유지 시간 |
| `MOCKLSF_RUN_MIN/MAX` | 5.0 / 30.0 | 실행 시간 |
| `MOCKLSF_SUBMIT_FAIL_RATE` | 0.02 | 제출 실패 확률 |
| `MOCKLSF_EXIT_RATE` | 0.08 | 비정상 종료(EXIT) 확률 |
| `MOCKLSF_SUSPEND_RATE` | 0.05 | 실행 중 시스템 suspend 확률 |
| `MOCKLSF_SCHED_INTERVAL` | 0.5 | 스케줄러 tick 주기(초) |
| `MOCKLSF_CLEAN_PERIOD` | 3600 | 완료 job 보존 기간(초). 초과 시 bjobs 에서 purge(→bhist 로만 조회). 작게 주면 bhist fallback 재현 |

### 완료 job purge (MBD_CLEAN_PERIOD 흉내)

실제 LSF 처럼 완료(DONE/EXIT) job 은 `MOCKLSF_CLEAN_PERIOD` 동안만 `bjobs -a` 에
보이고, 그 후엔 **bjobs 에서 purge** 되어 `bjobs <id>` 가 `No matching job found`
(exit 255)를 낸다. `bhist <id>` 는 계속 이력을 조회할 수 있다.

lsfmgr 는 이 상황을 bhist fallback(FR-4.3)으로 처리한다 — bjobs 에서 사라진 job 을
bhist 로 조회해 DONE/EXIT 를 확정한다. `MOCKLSF_CLEAN_PERIOD` 를 작게 주면 실제
LSF 없이 이 경로를 재현·검증할 수 있다.

## 테스트

```bash
python3 -m pytest tests/ -v
```

## 상태 초기화

```bash
mocklsfd reset      # 데몬 정지 + DB 삭제
```
