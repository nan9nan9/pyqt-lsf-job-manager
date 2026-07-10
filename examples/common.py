"""예제 공통 유틸 — manager 생성, 로깅, 자동 종료(스모크 테스트용).

모든 예제는 기본으로 **mocklsf**(저장소 동봉 가상 LSF)를 테스트 환경으로 쓴다.
실제 LSF cluster 명령이 아니라 `bin/`의 가상 명령을 subprocess 로 호출하며,
job 제출은 `customwrapper_sub` 같은 wrapper 를 거쳐 mocklsf 의 bsub 로 이어진다.

- 기본 제출 wrapper: `customwrapper_sub` (실제 환경에선 원하는 wrapper로 교체)
- mocklsf 상태는 프로세스별 임시 `MOCKLSF_HOME`(SQLite)에 격리되고, 종료 시 정리
- 타이밍/실패율은 MOCKLSF_* 환경변수로 조정 (configure_mocklsf 헬퍼)

실제 LSF cluster 에서 돌리려면 환경변수 LSFMGR_REAL=1 을 설정하면 된다.
"""
from __future__ import annotations

import atexit
import logging
import os
import shutil
import subprocess
import sys
import tempfile

from qtpy.QtCore import QTimer

from lsfmgr import LsfJobManager

# --- 경로: 저장소 루트/bin (예제는 examples/ 하위) --------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(_REPO_ROOT, "bin")
# 저장소 루트를 import 경로에 — mocklsf(가상 LSF 패키지, 미설치)를 예제에서
# import 할 수 있게 한다 (cluster_env_path 등).
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

#: 기본 제출 wrapper — 실제 환경의 툴 전용 제출 스크립트를 흉내낸다.
DEFAULT_WRAPPER = "customwrapper_sub"

#: 데모 제출 wrapper 목록 (실제 환경에선 여기에 여러 wrapper를 둘 수 있다).
WRAPPERS = ["customwrapper_sub"]

_REAL = os.environ.get("LSFMGR_REAL") == "1"


def wrapper(tool: str, *args) -> list:
    """create_jobs 에 넘길 wrapper 커맨드(토큰 리스트)를 만든다.

    mocklsf 모드에서는 `bin/<tool>` 절대경로를, 실제 LSF 모드에서는 PATH 의
    `<tool>` 이름을 프로그램으로 쓴다.
    """
    prog = tool if _REAL else os.path.join(BIN, tool)
    return [prog, *[str(a) for a in args]]


# ---------------------------------------------------------------------------
# mocklsf 테스트 환경 셋업
# ---------------------------------------------------------------------------

def _init_mocklsf_home() -> str:
    """프로세스 전용 MOCKLSF_HOME 을 임시 디렉토리로 격리한다.

    각 예제 실행이 깨끗한 상태에서 시작하고, 이전 실행의 데몬/DB 와 섞이지
    않도록 한다. 종료 시 데몬 정지 + 디렉토리 정리를 atexit 로 예약한다.
    """
    home = tempfile.mkdtemp(prefix="lsfmgr_examples_")
    os.environ["MOCKLSF_HOME"] = home

    @atexit.register
    def _cleanup():
        try:
            subprocess.run([os.path.join(BIN, "mocklsfd"), "stop"],
                           timeout=10, capture_output=True)
        except Exception:
            pass
        shutil.rmtree(home, ignore_errors=True)

    return home


def configure_mocklsf(*, pend=None, run=None, submit_delay=None,
                      submit_fail_rate=None, exit_rate=None,
                      suspend_rate=None, slots_per_host=None,
                      forward_clusters=None, forward_rate=None) -> None:
    """mocklsf 타이밍/실패율/MultiCluster를 MOCKLSF_* 환경변수로 설정한다.

    데몬은 첫 submit 시 기동하며 그때의 환경을 읽으므로, 이 함수는 반드시
    submit **이전**(예: 예제 상단)에서 호출해야 반영된다.
    (min, max) 튜플 또는 단일 값을 받는다. 실제 LSF 모드에선 무시된다.
    forward_clusters(리스트)를 주면 MC(job forwarding)를 켠다 — 그 원격
    클러스터들로 forward_rate 확률로 job이 포워딩된 것처럼 동작한다.
    """
    if _REAL:
        return

    def _pair(name, value):
        if value is None:
            return
        lo, hi = value if isinstance(value, (tuple, list)) else (value, value)
        os.environ[f"MOCKLSF_{name}_MIN"] = str(lo)
        os.environ[f"MOCKLSF_{name}_MAX"] = str(hi)

    _pair("SUBMIT_DELAY", submit_delay)
    _pair("PEND", pend)
    _pair("RUN", run)
    if submit_fail_rate is not None:
        os.environ["MOCKLSF_SUBMIT_FAIL_RATE"] = str(submit_fail_rate)
    if exit_rate is not None:
        os.environ["MOCKLSF_EXIT_RATE"] = str(exit_rate)
    if suspend_rate is not None:
        os.environ["MOCKLSF_SUSPEND_RATE"] = str(suspend_rate)
    if slots_per_host is not None:
        os.environ["MOCKLSF_SLOTS_PER_HOST"] = str(slots_per_host)
    if forward_clusters is not None:
        os.environ["MOCKLSF_FORWARD_CLUSTERS"] = ",".join(forward_clusters)
    if forward_rate is not None:
        os.environ["MOCKLSF_FORWARD_RATE"] = str(forward_rate)


def cluster_env_path(cluster: str) -> str:
    """forward 클러스터 <cluster>의 cshrc(env) 경로 — lsfmgr kill의 envpath로
    넘긴다. mocklsf가 첫 DB 접근 시 자동 생성한다(홈 보장 후 반환)."""
    from mocklsf import config as mockcfg
    mockcfg.ensure_home()                 # clusterenv/<cluster>.cshrc 생성 보장
    return mockcfg.cluster_env_path(cluster)


# 프로세스 시작 시 1회: 격리 홈 + 데모용 빠른 기본 타이밍.
if not _REAL:
    _init_mocklsf_home()
    configure_mocklsf(
        submit_delay=0,          # bsub 제출 지연 제거 (대량 submit 이 빠르게)
        pend=(1, 3),
        run=(3, 8),
        submit_fail_rate=0,      # 기본은 실패 주입 없음 (06 에서 켠다)
        exit_rate=0.05,          # 소수는 EXIT 로 (상태 색상 데모용)
        suspend_rate=0,
        slots_per_host=32,       # 총 128 슬롯 — 데모 동시 실행량 확보
    )


# ---------------------------------------------------------------------------
# manager 생성
# ---------------------------------------------------------------------------

def mocklsf_paths(wrapper: str = DEFAULT_WRAPPER) -> dict:
    """LsfJobManager 에 넘길 mocklsf 명령 경로 kwargs.

    실제 LSF 모드(LSFMGR_REAL=1)면 빈 dict — PATH 의 실제 LSF 명령을 쓴다.
    그 외엔 bsub 를 wrapper 로, 나머지 명령을 mocklsf 가상 명령으로 지정한다.
    """
    if _REAL:
        return {}
    return {
        "bsub_path": os.path.join(BIN, wrapper),      # 제출은 wrapper 경유
        "bjobs_path": os.path.join(BIN, "bjobs"),
        "bkill_path": os.path.join(BIN, "bkill"),
        "bhist_path": os.path.join(BIN, "bhist"),
        "bmod_path": os.path.join(BIN, "bmod"),
        "bgdel_path": os.path.join(BIN, "bgdel"),
    }


def make_manager(wrapper: str = DEFAULT_WRAPPER,
                 **mgr_kwargs) -> tuple[LsfJobManager, None]:
    """예제용 manager 생성 — mocklsf 명령 경로를 주입한다.

    반환은 (manager, None) 2-튜플 (과거 시그니처 호환용 자리).
    """
    mgr = LsfJobManager(**mocklsf_paths(wrapper), **mgr_kwargs)
    return mgr, None


def install_logging(level: int = logging.INFO) -> None:
    """lsfmgr.* 로그를 콘솔로 (스레드명 포함 — docs/logging.md 권장 포맷)."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"))
    logger = logging.getLogger("lsfmgr")
    logger.setLevel(level)
    logger.addHandler(handler)


def maybe_autoquit(app) -> None:
    """LSFMGR_DEMO_AUTOQUIT=<초> 설정 시 자동 종료 (CI 스모크 테스트용)."""
    sec = float(os.environ.get("LSFMGR_DEMO_AUTOQUIT", "0"))
    if sec > 0:
        QTimer.singleShot(int(sec * 1000), app.quit)


def format_summary(s: dict) -> str:
    """요약 dict → 한 줄 문자열."""
    keys = [k for k in ("PEND", "RUN", "DONE", "EXIT", "SUBMIT_FAILED",
                        "RETRY_WAIT", "LOST", "CREATED", "SUBMITTING")
            if s.get(k)]
    body = "  ".join(f"{k}={s[k]}" for k in keys)
    return f"total={s.get('total', 0)}  {body}"
