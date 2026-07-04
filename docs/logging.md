# lsfmgr 로깅 / 예외 수집 가이드

## 1. Logger 계층

라이브러리의 모든 이벤트는 `lsfmgr.*` logger 계층으로 나갑니다:

| Logger | 내용 |
|---|---|
| `lsfmgr.command` | LSF 명령 실행 (DEBUG: argv 원문 / rc / stdout / stderr) |
| `lsfmgr.submit` | submit 시작/성공/실패/재시도/취소 |
| `lsfmgr.monitor` | polling 시작/중지, 조회 실패, LOST 확정 |
| `lsfmgr.kill` | kill 전략 선택/실패 |
| `lsfmgr.jobset` | JobSet 생성/merge/close/손실 복구 |
| `lsfmgr.store` | SqliteStore (NFS 경고 등) |
| `lsfmgr.manager` | shutdown 등 수명 이벤트 |

## 2. 레벨 규약 (NFR-6)

| 레벨 | 내용 |
|---|---|
| `DEBUG` | LSF 명령 원문(argv), stdout/stderr 원문, returncode |
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

- worker 스레드(submit/polling/kill/reconcile) 안의 예외는 스레드를 죽이지
  않고 → `logger.exception()`(traceback 포함 ERROR 로그) + `js.error` /
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
