# lsfmgr 로깅 / 예외 수집 가이드

## 1. Logger 계층

라이브러리의 모든 이벤트는 `lsfmgr.*` logger 계층으로 나갑니다:

| Logger | 내용 |
|---|---|
| `lsfmgr.command` | **모든 LSF subprocess 실행**(DEBUG: 스레드·명령종류·argv·cwd·소요시간·rc·stdout/stderr) — [§1.1](#11-실제-실행subprocess-추적) |
| `lsfmgr.submit` | submit 시작/성공/실패/재시도/취소 |
| `lsfmgr.monitor` | polling 시작/중지, 조회 실패, LOST 확정 |
| `lsfmgr.kill` | kill 전략 선택/실패 |
| `lsfmgr.jobset` | JobSet 생성/merge/close/손실 복구 |
| `lsfmgr.handler` | job별 handler(FR-7) 실행/예외 |
| `lsfmgr.manager` | shutdown 등 수명 이벤트 |

### 1.1 실제 실행(subprocess) 추적

submit(bsub/wrapper)·bjobs·bkill·bhist·bmod·bgdel **모든 LSF subprocess는 단일
funnel `LsfCommand._run`을 지납니다.** 이 한 지점에서 DEBUG 로그를 찍으므로,
"어떤 명령이 어느 스레드에서 어떤 cwd로 실행되고 얼마나 걸려 무슨 결과가
나왔는지"를 빠짐없이 추적할 수 있습니다.

**활성화** — `lsfmgr.command`(또는 상위 `lsfmgr`) 로거 레벨을 DEBUG로:

```python
import logging
logging.getLogger("lsfmgr.command").setLevel(logging.DEBUG)   # 실행 로그만
# 또는 라이브러리 전체
logging.getLogger("lsfmgr").setLevel(logging.DEBUG)
```

**출력 예** (동시 submit worker 2개 → bjobs 조회 → bkill):

```
DEBUG lsfmgr.command: [Dummy-1] exec customwrapper_sub: customwrapper_sub a.sp (cwd=/scratch/run, timeout=30.0s)
DEBUG lsfmgr.command: [Dummy-1] exec customwrapper_sub → rc=0 (0.012s) stdout='Job <1000> is submitted ...' stderr=''
DEBUG lsfmgr.command: [Dummy-2] exec customwrapper_sub: customwrapper_sub b.sp (cwd=/scratch/run, timeout=30.0s)
DEBUG lsfmgr.command: [MainThread] exec bjobs: bjobs -noheader -o ... -g /lsfmgr/u/<jsid> (cwd=None, timeout=120.0s)
DEBUG lsfmgr.command: [MainThread] exec bjobs → rc=0 (0.031s) stdout='1000;RUN;...' stderr=''
DEBUG lsfmgr.command: [Dummy-3] exec bkill: bkill 1000 1001 (cwd=None, timeout=120.0s)
DEBUG lsfmgr.command: [Dummy-3] exec bkill → rc=0 (0.008s) stdout='Job <1000> is being terminated ...' stderr=''
```

각 줄이 담는 것:
- **스레드명**(`[Dummy-N]` submit/kill worker, `[MainThread]` querier 등) — 동시
  실행을 구분한다. 메시지에 직접 넣어 포매터에 `%(threadName)s`가 없어도 보인다.
- **명령 종류**(`argv[0]` basename: `bsub`/`customwrapper_sub`/`bjobs`/`bkill`…),
  **전체 argv**, **cwd**(제출만; 조회/kill은 None=부모 cwd), **timeout**
- 결과 줄: **rc**, **소요시간**(monotonic 초), **stdout/stderr**(각 500자 절단)
- 실패(timeout/OSError 등)도 소요시간과 함께 `exec … 실패 (…s): <예외>`로 찍고
  그대로 전파한다.

**thread safety**: 표준 `logging`은 핸들러 내부 락으로 동시 쓰기를 직렬화하므로
여러 submit/kill worker가 동시에 찍어도 출력이 섞여 깨지지 않습니다. 이 로깅은
지역 변수(스레드명·monotonic 시각)만 쓰고 **공유 가변 상태를 두지 않아** 추가
경합원이 없습니다.

> stdout/stderr 원문까지 남으므로 운영 환경에서는 필요 구간에만 DEBUG를 켜는
> 것을 권장합니다(대량 제출 시 로그량이 큼).

## 2. 레벨 규약 (NFR-6)

| 레벨 | 내용 |
|---|---|
| `DEBUG` | LSF subprocess 실행 추적 — 스레드·명령종류·argv·cwd·timeout·소요시간·rc·stdout/stderr ([§1.1](#11-실제-실행subprocess-추적)) |
| `INFO` | submit/kill 시작·완료, 상태 전이, polling 자동 중지, shutdown |
| `WARNING` | submit 재시도, 부착물(-g/-J) 지정 실패 후 진행, 조회 수단 실패, NFS 경고 |
| `ERROR` | `SUBMIT_FAILED`/`LOST` 최종 확정, worker 예외 (traceback 포함) |

## 3. 앱 연계 (SimManager 중앙 로깅 등)

```python
import logging

logger = logging.getLogger("lsfmgr")
logger.setLevel(logging.INFO)                  # DEBUG면 LSF 명령 원문까지

handler = logging.FileHandler("/local_disk/logs/lsfmgr.log")
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s"))
logger.addHandler(handler)
```

- worker 스레드에서 발생하는 로그가 많으므로 포맷에 `%(threadName)s` 포함을
  권장합니다 (submit worker / `lsfmgr-polling` 스레드 구분).
- 라이브러리는 root logger를 건드리지 않으며 handler도 추가하지 않습니다
  (`NullHandler` 관례). 출력 구성은 전적으로 앱 몫입니다.

## 4. 예외 수집 범위

**라이브러리가 보장하는 것 (CS-5):**

- worker 스레드(submit/polling/kill) 안의 예외는 스레드를 죽이지
  않고 → `logger.exception()`(traceback 포함 ERROR 로그) + `js.error_occurred` /
  `mgr.error_occurred` Signal로 전달됩니다.

**앱에서 추가로 훅킹을 권장하는 것:**

라이브러리 밖(앱 쪽 slot, 다른 스레드)의 예외까지 중앙 수집하려면:

```python
import sys, threading, traceback, logging

app_log = logging.getLogger("app.crash")

def _excepthook(exc_type, exc, tb):
    app_log.error("미처리 예외:\n%s",
                  "".join(traceback.format_exception(exc_type, exc, tb)))

sys.excepthook = _excepthook                       # main 스레드

def _thread_hook(args):
    app_log.error("스레드 %s 미처리 예외:\n%s", args.thread.name,
                  "".join(traceback.format_exception(
                      args.exc_type, args.exc_value, args.exc_traceback)))

threading.excepthook = _thread_hook                # 기타 Python 스레드

# Qt 내부 경고/에러 (connect 실패, QObject 경고 등)
from qtpy.QtCore import qInstallMessageHandler

def _qt_handler(mode, context, message):
    app_log.warning("Qt: %s", message)

qInstallMessageHandler(_qt_handler)
```

주의: PyQt5/PyQt6에서는 slot 안에서 발생한 예외가 기본적으로
`sys.excepthook`으로 전달되며(앱 중단될 수 있음), PySide 계열은 콘솔 출력 후
계속 진행됩니다. 바인딩 간 동작을 통일하려면 위처럼 `sys.excepthook`을
설치하는 것이 안전합니다.
