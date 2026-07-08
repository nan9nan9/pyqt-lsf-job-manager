"""MockLSF 전역 설정.

타이밍·큐·호스트·실패율 등 시뮬레이션 파라미터를 한 곳에서 관리한다.
환경 변수로 일부 값을 덮어쓸 수 있어 앱 테스트 시나리오별로 조정 가능하다.
"""

import os


def _env_float(name, default):
    """환경 변수를 float 로 읽되, 없거나 잘못되면 기본값 사용."""
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_int(name, default):
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_list(name):
    """콤마 구분 환경변수를 리스트로. 없으면 빈 리스트."""
    raw = os.environ.get(name, "").strip()
    return [c.strip() for c in raw.split(",") if c.strip()]


# MockLSF 상태 파일(DB, PID, job 출력)이 모이는 홈 디렉토리.
MOCKLSF_HOME = os.environ.get(
    "MOCKLSF_HOME", os.path.join(os.path.expanduser("~"), ".mocklsf")
)

DB_PATH = os.path.join(MOCKLSF_HOME, "state.db")
PID_PATH = os.path.join(MOCKLSF_HOME, "mocklsfd.pid")
LOCK_PATH = os.path.join(MOCKLSF_HOME, "mocklsfd.lock")
LOG_PATH = os.path.join(MOCKLSF_HOME, "mocklsfd.log")
# 각 job 의 가상 stdout 파일(bpeek 용)이 저장되는 디렉토리.
JOB_OUT_DIR = os.path.join(MOCKLSF_HOME, "jobout")

# ---------------------------------------------------------------------------
# 클러스터 구성
# ---------------------------------------------------------------------------

# 클러스터/마스터 호스트 이름. 실제 LSF 출력의 FROM_HOST 등에 쓰인다.
# MOCKLSF_CLUSTER 는 "이 프로세스의 클러스터 컨텍스트"이기도 하다 — forward job
# kill 시 그 클러스터 cshrc 를 source 하면 이 값이 forward_cluster 로 바뀌어
# bkill 이 그 job 에 닿는다(아래 MultiCluster 절 참고).
CLUSTER_NAME = os.environ.get("MOCKLSF_CLUSTER", "mockcluster")
MASTER_HOST = os.environ.get("MOCKLSF_MASTER", "mockmaster")

# ---------------------------------------------------------------------------
# MultiCluster (job forwarding) 시뮬레이션
# ---------------------------------------------------------------------------
# forward 대상 원격 클러스터 이름들 (콤마 구분). 비어 있으면 MC 꺼짐(포워딩 없음).
#   예: MOCKLSF_FORWARD_CLUSTERS="cluster_b,cluster_c"
FORWARD_CLUSTERS = _env_list("MOCKLSF_FORWARD_CLUSTERS")
# job 하나가 forward 될 확률(0~1). MC 켜졌을 때만 의미. 기본 0.5.
FORWARD_RATE = _env_float("MOCKLSF_FORWARD_RATE",
                          0.5 if FORWARD_CLUSTERS else 0.0)
# forward job kill 용 클러스터 env(cshrc) 파일 디렉토리 — source 시 그 프로세스의
# MOCKLSF_CLUSTER 를 해당 클러스터로 바꾼다(bkill 이 forward job 에 닿게).
CLUSTER_ENV_DIR = os.path.join(MOCKLSF_HOME, "clusterenv")


def cluster_env_path(cluster):
    """forward 클러스터 <cluster> 의 cshrc 경로 (lsfmgr envpath 로 넘길 값)."""
    return os.path.join(CLUSTER_ENV_DIR, f"{cluster}.cshrc")

# 실행 호스트와 각 호스트의 슬롯(동시 실행 가능한 job 수).
# 총 슬롯 수가 동시 실행량을 제한하므로, 수천 개를 던지면 자연스럽게 PEND 가 쌓인다.
HOSTS = {
    "hostA": _env_int("MOCKLSF_SLOTS_PER_HOST", 16),
    "hostB": _env_int("MOCKLSF_SLOTS_PER_HOST", 16),
    "hostC": _env_int("MOCKLSF_SLOTS_PER_HOST", 16),
    "hostD": _env_int("MOCKLSF_SLOTS_PER_HOST", 16),
}

# 큐 정의. priority 가 높을수록 먼저 dispatch 된다.
# (name -> dict) 순서가 bqueues 출력 순서.
QUEUES = {
    "priority": {"priority": 43, "nice": 0},
    "normal":   {"priority": 30, "nice": 0},
    "short":    {"priority": 30, "nice": 0},
    "long":     {"priority": 20, "nice": 0},
    "idle":     {"priority": 10, "nice": 20},
}
DEFAULT_QUEUE = "normal"

# ---------------------------------------------------------------------------
# 타이밍 (초 단위)
# ---------------------------------------------------------------------------

# bsub 제출 시 클라이언트가 걸리는 지연 (실제 환경처럼 1~2초 소요).
SUBMIT_DELAY_MIN = _env_float("MOCKLSF_SUBMIT_DELAY_MIN", 1.0)
SUBMIT_DELAY_MAX = _env_float("MOCKLSF_SUBMIT_DELAY_MAX", 2.0)

# 슬롯이 있어도 최소 이만큼은 PEND 상태로 머문다(스케줄링 지연 흉내).
PEND_MIN = _env_float("MOCKLSF_PEND_MIN", 2.0)
PEND_MAX = _env_float("MOCKLSF_PEND_MAX", 6.0)

# job 실행(RUN) 시간 범위.
RUN_MIN = _env_float("MOCKLSF_RUN_MIN", 5.0)
RUN_MAX = _env_float("MOCKLSF_RUN_MAX", 30.0)

# ---------------------------------------------------------------------------
# 확률 (0.0 ~ 1.0)
# ---------------------------------------------------------------------------

# bsub 제출 자체가 실패할 확률 (아주 가끔 재현).
SUBMIT_FAIL_RATE = _env_float("MOCKLSF_SUBMIT_FAIL_RATE", 0.02)

# job 이 정상 종료(DONE) 대신 비정상 종료(EXIT)할 확률.
EXIT_RATE = _env_float("MOCKLSF_EXIT_RATE", 0.08)

# RUN 중 일시적으로 SSUSP(시스템 suspend) 되었다 재개될 확률.
SUSPEND_RATE = _env_float("MOCKLSF_SUSPEND_RATE", 0.05)
SUSPEND_MIN = _env_float("MOCKLSF_SUSPEND_MIN", 3.0)
SUSPEND_MAX = _env_float("MOCKLSF_SUSPEND_MAX", 8.0)

# ---------------------------------------------------------------------------
# 스케줄러
# ---------------------------------------------------------------------------

# 스케줄러 tick 주기(초). 작을수록 상태 전이가 촘촘하다.
SCHED_INTERVAL = _env_float("MOCKLSF_SCHED_INTERVAL", 0.5)

# 한 tick 에서 새로 dispatch 할 수 있는 최대 job 수(폭주 방지, 실제 LSF 유사).
MAX_DISPATCH_PER_TICK = _env_int("MOCKLSF_MAX_DISPATCH_PER_TICK", 200)

# 완료 job 보존 기간(초). 실제 LSF 의 MBD_CLEAN_PERIOD 흉내 — 완료(DONE/EXIT)
# 후 이 시간이 지나면 bjobs 에서 purge 되어 사라진다(그 뒤엔 bhist 로만 조회).
# 기본 3600(1시간). 데모/‏테스트에서 작게 주면 bhist fallback 경로를 태울 수 있다.
CLEAN_PERIOD = _env_float("MOCKLSF_CLEAN_PERIOD", 3600.0)

# job 번호 시작값.
FIRST_JOB_ID = _env_int("MOCKLSF_FIRST_JOB_ID", 1000)


def ensure_home():
    """홈/출력 디렉토리를 만든다 (없으면 생성). MC 켜져 있으면 각 forward
    클러스터의 cshrc(env 파일)도 생성한다 — lsfmgr 가 kill 시 source 한다."""
    os.makedirs(MOCKLSF_HOME, exist_ok=True)
    os.makedirs(JOB_OUT_DIR, exist_ok=True)
    if FORWARD_CLUSTERS:
        os.makedirs(CLUSTER_ENV_DIR, exist_ok=True)
        for cluster in FORWARD_CLUSTERS:
            path = cluster_env_path(cluster)
            if not os.path.exists(path):
                # tcsh 로 source 된다 (lsfmgr: tcsh -c "source <path> && bkill").
                # MOCKLSF_HOME 등은 부모 env 그대로 두고 클러스터 컨텍스트만 바꾼다.
                with open(path, "w", encoding="utf-8") as f:
                    f.write(
                        f"# MockLSF {cluster} 클러스터 env — source 하면 이\n"
                        f"# 프로세스의 bkill 이 {cluster} 로 forward된 job 을\n"
                        f"# 죽일 수 있다(실제 MC 의 cshrc.lsf 흉내).\n"
                        f"setenv MOCKLSF_CLUSTER {cluster}\n")
