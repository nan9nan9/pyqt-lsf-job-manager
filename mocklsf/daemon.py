"""mocklsfd 데몬 관리 (start/stop/status).

스케줄러 루프를 백그라운드 프로세스로 띄운다.
PID 파일로 중복 실행을 막고, 다른 CLI 가 상태를 조회할 수 있게 한다.
"""

import fcntl
import os
import signal
import time

from . import config
from .db import Database
from .scheduler import Scheduler


def _read_pid():
    try:
        with open(config.PID_PATH) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def is_running() -> bool:
    """데몬이 살아있는지 PID 로 확인."""
    pid = _read_pid()
    if pid is None:
        return False
    try:
        os.kill(pid, 0)  # 시그널 0 = 존재 확인만.
        return True
    except OSError:
        return False


def _run_scheduler_loop():
    """데몬 프로세스 본체."""
    config.ensure_home()
    db = Database()
    sched = Scheduler(db)

    stopped = {"flag": False}

    def _handle(signum, frame):
        stopped["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    sched.run_forever(stop_flag=lambda: stopped["flag"])

    # 정리.
    try:
        os.remove(config.PID_PATH)
    except OSError:
        pass


def start(foreground: bool = False) -> bool:
    """데몬을 기동한다. 이미 실행 중이면 False.

    여러 프로세스(예: 동시에 실행된 여러 bsub)가 auto-start 를 시도해도
    데몬이 중복 생성되지 않도록 flock 으로 start 임계구역을 직렬화한다.
    is_running() 체크만으로는 원자적이지 않아 경쟁 시 중복 데몬이 뜬다.
    """
    config.ensure_home()
    if is_running():
        return False

    # start 임계구역 진입 (다른 start 는 여기서 대기).
    lock_fd = os.open(config.LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        # 락 획득 후 재확인 (대기 중 다른 프로세스가 이미 띄웠을 수 있음).
        if is_running():
            return False
        # 오래된 PID 파일 정리.
        if os.path.exists(config.PID_PATH):
            try:
                os.remove(config.PID_PATH)
            except OSError:
                pass

        if foreground:
            with open(config.PID_PATH, "w") as f:
                f.write(str(os.getpid()))
            # 포그라운드는 이 프로세스가 곧 데몬이므로 락을 쥔 채 루프.
            _run_scheduler_loop()
            return True

        # 백그라운드로 fork (double fork 로 세션 분리).
        pid = os.fork()
        if pid > 0:
            # 부모: 손자가 PID 파일을 쓸 때까지 락을 유지한 채 대기.
            # (여기서 락을 놓으면 경쟁 프로세스가 중복 데몬을 띄울 수 있다.)
            for _ in range(100):
                if is_running():
                    return True
                time.sleep(0.05)
            return is_running()

        # 자식 (첫 번째). 여기부터는 os._exit 로만 끝나 finally 를 타지 않는다.
        os.setsid()
        pid2 = os.fork()
        if pid2 > 0:
            os._exit(0)

        # 손자: 실제 데몬. 상속받은 락 fd 는 닫는다(부모가 계속 보유).
        try:
            os.close(lock_fd)
        except OSError:
            pass
        with open(config.PID_PATH, "w") as f:
            f.write(str(os.getpid()))
        # 표준 입출력 분리.
        devnull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devnull, 0)
        log = os.open(config.LOG_PATH,
                      os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        os.dup2(log, 1)
        os.dup2(log, 2)
        _run_scheduler_loop()
        os._exit(0)
    finally:
        # 부모/포그라운드 경로만 여기 도달 (fork 자식들은 os._exit).
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(lock_fd)
        except OSError:
            pass


def stop() -> bool:
    """데몬을 종료한다. 실행 중이 아니면 False."""
    pid = _read_pid()
    if pid is None or not is_running():
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return False
    # 종료 대기.
    for _ in range(60):
        if not is_running():
            break
        time.sleep(0.05)
    if os.path.exists(config.PID_PATH):
        try:
            os.remove(config.PID_PATH)
        except OSError:
            pass
    return True


def ensure_running():
    """데몬이 없으면 자동 기동 (bsub 등에서 편의상 호출)."""
    if not is_running():
        start(foreground=False)


def status() -> str:
    pid = _read_pid()
    if is_running():
        return f"mocklsfd is running (pid {pid})"
    return "mocklsfd is not running"
