"""InternalStatusSource — bjobs 대신 앱 콜백으로 job 상태를 얻는 조회원.

`LsfJobManager(job_status_fetcher=fn)`으로 콜백을 주면 LsfCommand가 bjobs
subprocess 대신 이 객체를 쓴다. 앱은 그 콜백 하나만 주면 되고 나머지(스냅샷
캐시·동시 호출 합치기·JobStatus 매핑)는 여기가 맡는다.

상위 flow는 **한 줄도 바뀌지 않는다** — 이 객체의 반환 계약이
`LsfCommand.bjobs_by_ids`와 같기 때문이다:

    (찾은 JobStatus 목록, 조회에 실패해 판단을 보류할 job_id 집합)

그래서 monitor의 LOST 유예("조회 장애 ≠ job 없음")도 그대로 성립한다 —
콜백이 실패하면 그 사이클의 대상 전원이 실패 집합으로 나가 아무도 LOST로
확정되지 않는다.

**왜 누적인가**: REST는 `updatefrom` 이후 갱신분만 줄 수 있다(증분 조회).
이 경우 "이번 payload에 없다"는 *사라졌다*가 아니라 *안 바뀌었다*는 뜻이므로,
받은 것을 통째로 교체하면 안 되고 job 단위로 **병합**해야 한다. 전량 조회
(`updatefrom=2000-01-01`)도 같은 병합 경로로 자연히 덮인다.

**왜 만료가 필요한가**: 병합만 하면 내부 원장이 영원히 커진다. 그래서 이미
끝난 job(DONE/EXIT) 중 finish_time이 보존 기간(기본 2주)을 넘긴 것은 버린다
— 어차피 그 나이의 종료 job은 조회 대상이 아니다(추적 중인 job은 진작
terminal로 확정돼 monitor가 조회하지 않는다).

**예비 콜백**: `job_status_fetcher_failover`를 주면 주 콜백이 동작하지
않을 때 — 예외·해석 불가 응답·미회수(안 돌아옴) — 같은 조회를 예비 콜백으로
다시 시도한다. 매 조회는 항상 주 콜백부터라 주 콜백이 회복되면 자동으로
되돌아간다. 유일한 예외는 주 콜백이 **미회수로 잡혀 있는 동안**이다: 그때
새 조회를 또 주 콜백부터 걸면 인계 사이클마다 wait_timeout_s를 통째로 다시
날리므로, 처음부터 예비로 간다(미회수 호출이 돌아오면 다시 주부터). 예비까지
실패하면 종전대로 '조회 장애'(판단 보류)다 — flow는 그대로다.

**thread safety**: 호출자가 둘이다 — 폴링 스레드, killer verify 워커.
(detect_lost는 순수 Store 판정이라 여기 안 온다.) 둘이 동시에 들어와도
콜백은 **한 번만** 돈다(single-flight): 하나가 조회를 띄우고 나머지는
Condition에서 기다렸다가 그 결과를 공유한다.

콜백은 **전용 daemon 스레드**에서 돈다. 호출자 스레드에서 직접 돌리면,
timeout 없는 콜백(예: `requests.get(...)`에 timeout 누락) 하나가 폴링
스레드를 영구히 잡아 상태 갱신이 통째로 멈추고 shutdown까지 막는다.
분리해 두면 호출자는 `wait_timeout_s`만 기다리고 빠져나오며, 안 돌아오는
콜백은 daemon 스레드로 남아 프로세스 종료를 막지 않는다. 그 조회가 상한을
넘기면 다음 호출자가 **인계**해 새로 띄운다(그래야 서버가 회복됐을 때
스스로 복구된다). 인계는 동시 미회수 조회 수를 상한으로 묶는다.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Dict, List, Optional, Sequence, Set, Tuple,
)

from .config import JobStatusFetcher  # noqa: F401 (재수출)
from .states import LSF_STAT_MAP, JobState, JobStatus

log = logging.getLogger("lsfmgr.internal_status")


class FetcherState(Enum):
    """상태 조회 콜백의 건강 — 지금 누가 답하고 있나.

    LsfJobManager.status_fetcher_state()가 돌려준다. 판정은 마지막으로
    **끝난** 조회의 결과다 — 진행 중(미회수) 조회는 끝나기 전까지 반영되지
    않고, 늦게 끝난 낡은 조회는 판정을 덮지 못한다(늦은 쓰기 술어).
    """
    IDLE = "IDLE"          # 아직 조회한 적 없음
    PRIMARY = "PRIMARY"    # 주 콜백(job_status_fetcher)이 정상 동작 중
    FAILOVER = "FAILOVER"  # 주는 실패 — 예비 콜백이 대신 동작 중
    DOWN = "DOWN"          # 마지막 조회 실패 (예비까지 실패, 또는 예비 없음)

#: dataId 표기 — "1432342.cluster1" / "1432342[3].cluster1" / "1432342".
_DATA_ID_RE = re.compile(r"^(\d+)(?:\[(\d+)\])?(?:\.(.+))?$")

#: 시각 문자열 끝의 타임존 표기 — "Z" 또는 "+09:00"/"+0900".
#: 날짜의 '-'와 헷갈리지 않는다(부호 뒤 4자리가 붙어 끝나야 매칭).
_TZ_RE = re.compile(r"(Z|[+-]\d{2}:?\d{2})$")

_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")

#: "값 없음"으로 볼 문자열 — REST 구현마다 null 표기가 다르다.
_EMPTY = ("", "-", "null", "none", "nil", "n/a")

#: LSF 표기가 아닌 상태 문자열의 별칭 — 확실한 것만 넣는다.
#: 애매한 값(예: "FINISHED")은 일부러 뺐다: DONE으로 접으면 실제로 실패한
#: job이 성공으로 보고돼 post_process가 잘못 돈다. 모르는 값은 UNKWN으로
#: 두고 **경고를 낸다** — 조용히 넘기면 UNKWN이 non-terminal이라 폴링이
#: 영영 안 멈추고 jobset_finished/post_process가 발화하지 않는다.
_STAT_ALIASES = {
    "RUNNING": JobState.RUN,
    "PENDING": JobState.PEND,
    "EXITED": JobState.EXIT,
    "UNKNOWN": JobState.UNKWN,
}

#: 이미 경고한 미지의 상태 문자열 — 값 하나당 1회만 경고한다(매 폴링 반복 금지).
_warned_stats: Set[str] = set()
_warn_lock = threading.Lock()

#: job dict에서 각 항목을 찾을 키 후보 (사이트마다 표기가 조금씩 다르다).
_KEYS_ID = ("dataId", "dataid", "jobId", "jobid", "id")
_KEYS_STAT = ("stat", "status", "state")
_KEYS_START = ("startTime", "start_time")
_KEYS_FINISH = ("finishTime", "finish_time", "endTime", "end_time")
_KEYS_EXIT = ("exitStatus", "exitCode", "exit_code", "exit_status")
_KEYS_CLUSTER = ("cluster", "clusterName", "cluster_name")


def _pick(job: dict, keys: Sequence[str]):
    """키 후보 중 처음으로 값이 있는 것 — 없으면 None."""
    for key in keys:
        if key in job and job[key] is not None:
            return job[key]
    return None


def _clean(value) -> Optional[str]:
    """문자열 정규화 — 빈 값 표기는 None으로 접는다."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text.lower() in _EMPTY else text


def _split_tz(text: str) -> Tuple[str, Optional[timezone]]:
    """시각 문자열에서 타임존 표기를 떼어 낸다."""
    m = _TZ_RE.search(text)
    if m is None:
        return text, None
    tail = m.group(1)
    body = text[:m.start()].strip()
    if tail == "Z":
        return body, timezone.utc
    digits = tail[1:].replace(":", "")
    delta = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
    return body, timezone(-delta if tail[0] == "-" else delta)


def parse_time(value) -> Optional[datetime]:
    """REST 시각 문자열 → naive datetime (라이브러리 전체가 naive를 쓴다).

    ISO-8601을 기본으로 하되 실환경 표기 흔들림을 흡수한다:
    구분자 T/공백, 소수점 이하 초, 타임존 표기(있으면 로컬로 환산 후
    tzinfo 제거 — aware/naive를 섞으면 뺄셈이 TypeError다), 그리고
    날짜 구분자가 ':'로 오는 사례("2026:08:08T12:00:01")까지.
    파싱 불가면 None — 그 필드만 버린다(행 전체를 버리지 않는다).
    """
    text = _clean(value)
    if text is None:
        return None
    # unix epoch(초/밀리초) 표기 — 숫자로 주는 사이트가 있다. 문자열 포맷
    # 시도에 걸리지 않아 조용히 None이 되던 값이다.
    if text.isdigit() and len(text) in (10, 13):
        ticks = int(text)
        try:
            return datetime.fromtimestamp(
                ticks / 1000.0 if len(text) == 13 else ticks)
        except (OverflowError, OSError, ValueError):
            return None
    body, tz = _split_tz(text)
    date_part, sep, time_part = body.partition("T")
    if not sep:
        date_part, _, time_part = body.partition(" ")
    date_part = date_part.replace(":", "-").replace("/", "-")
    time_part = time_part.split(".", 1)[0].split("+", 1)[0]
    norm = f"{date_part} {time_part}".strip()
    for fmt in _TIME_FORMATS:
        try:
            dt = datetime.strptime(norm, fmt)
        except ValueError:
            continue
        if tz is None:
            return dt
        return dt.replace(tzinfo=tz).astimezone().replace(tzinfo=None)
    log.debug("internal status 시각 파싱 불가: %r", value)
    return None


def _job_state(raw: Optional[str]) -> JobState:
    """상태 문자열 → JobState. 대소문자·별칭을 흡수하고, 모르면 경고 후 UNKWN.

    키 이름은 후보를 여럿 받으면서 값만 대문자 정확일치를 요구하면 비대칭이다
    — `"Run"` 하나에 전 job이 UNKWN이 되고, UNKWN은 terminal이 아니라서
    폴링이 안 멈추고 완료 신호도 영영 안 온다. 그래서 여기서 정규화한다.
    """
    if raw is None:
        return JobState.UNKWN
    key = raw.strip().upper()
    state = LSF_STAT_MAP.get(key) or _STAT_ALIASES.get(key)
    if state is not None:
        return state
    with _warn_lock:
        first = key not in _warned_stats
        _warned_stats.add(key)
    if first:
        log.warning(
            "알 수 없는 job 상태 %r → UNKWN으로 둡니다. UNKWN은 종료 상태가 "
            "아니라 이 job은 폴링이 계속되고 완료 신호(jobset_finished/"
            "post_process)가 발화하지 않습니다 — 상태 표기를 확인하세요", raw)
    return JobState.UNKWN


def _exit_code(job: dict) -> Optional[int]:
    """exit code — payload에 없으면 None(bjobs의 '-'와 같은 취급)."""
    raw = _clean(_pick(job, _KEYS_EXIT))
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def job_status_from_dict(job: dict, now: datetime):
    """job dict 1건 → JobStatus. 해석 불가면 None(그 행만 버린다).

    now는 스냅샷을 받은 시각 — 실행 중 job의 run_time을 여기서 유도한다.
    매 읽기마다 datetime.now()를 쓰면 조회 없이도 값이 흔들려 monitor가
    전 job을 매번 재전이시킨다(jobs_updated 스팸). 스냅샷 시각으로 고정하면
    갱신될 때만 움직인다.
    """
    raw_id = _clean(_pick(job, _KEYS_ID))
    if raw_id is None:
        log.debug("internal status: job id 없음 — 행 무시 %r", job)
        return None
    m = _DATA_ID_RE.match(raw_id)
    if m is None:
        log.debug("internal status: job id 파싱 불가 %r", raw_id)
        return None

    state = _job_state(_clean(_pick(job, _KEYS_STAT)))

    start_time = parse_time(_pick(job, _KEYS_START))
    # bjobs 파서와 같은 규칙 — 실행 중 finishTime은 **예상치**일 수 있어
    # 종료 상태에서만 신뢰한다.
    finish_time = (parse_time(_pick(job, _KEYS_FINISH))
                   if state in (JobState.DONE, JobState.EXIT) else None)
    # run_time은 payload에 없다 — 시각 두 개로 유도한다. 종료 job은 실측
    # 구간, 실행 중 job은 스냅샷 시각까지의 경과(예상 종료시각을 쓰면
    # 아직 돌지도 않은 시간이 run_time으로 찍힌다).
    run_time_s = None
    if start_time is not None:
        end = finish_time if finish_time is not None else now
        if end >= start_time:
            run_time_s = int((end - start_time).total_seconds())

    # cluster는 명시 필드 우선, 없으면 dataId 접미사("...cluster1").
    # forward_cluster는 payload에 개념이 없다 — source만 채우고, kill 분류의
    # 'forward 우선, 없으면 source' 규칙이 그대로 이 값을 쓴다.
    cluster = _clean(_pick(job, _KEYS_CLUSTER)) or _clean(m.group(3))

    return JobStatus(
        job_id=int(m.group(1)),
        array_index=int(m.group(2)) if m.group(2) else None,
        state=state, exit_code=_exit_code(job),
        run_time_s=run_time_s, start_time=start_time,
        finish_time=finish_time,
        source_cluster=cluster, forward_cluster=None)


def parse_internal_jobs(payload, now: Optional[datetime] = None,
                        keep_ids: Optional[Set[int]] = None) -> List:
    """콜백 반환값 → JobStatus 목록.

    payload는 REST 응답 그대로 — {"jobs": [...], "count": N} dict, 또는 job
    dict 목록. **"jobs" 키가 없는 dict는 예외**로 본다: 응답 형식이 깨졌을
    때 '조회 결과 0건'으로 접으면 전 job이 미발견으로 몰려 결국 LOST로
    확정된다. 형식 오류는 조회 장애로 보고해 판단을 보류시켜야 한다.
    (빈 목록 `{"jobs": []}`은 정상 — 진짜로 job이 없다는 뜻이다.)

    keep_ids를 주면 그 job_id만 JobStatus로 만든다 — 유저 전 job이 오는
    payload에서 추적 대상만 남겨, 버릴 객체를 애초에 안 만든다(10만 건
    payload에서 수십 MB 차이). 해석 실패 판정은 필터 **이전** 기준이라
    "전부 남의 job이었다"를 형식 오류로 오해하지 않는다.
    """
    if now is None:
        now = datetime.now()
    if payload is None:
        # 맨 None은 콜백이 return을 빼먹은 것이다 — '0건'으로 접으면 전 job이
        # 미발견으로 몰려 LOST가 된다. ({"jobs": None}과 다르다: 그건 빈 결과를
        # null로 주는 서버 표기라 봉투가 있고, 아래에서 빈 목록으로 접는다.)
        raise ValueError("internal status 응답이 None입니다 — "
                         "콜백이 payload를 반환하는지 확인하세요")
    if isinstance(payload, dict):
        if "jobs" not in payload:
            raise ValueError(
                "internal status 응답에 'jobs' 키가 없습니다 "
                f"(키: {sorted(payload)[:8]})")
        jobs = payload["jobs"]
    else:
        jobs = payload
    if jobs is None:
        jobs = []
    if not isinstance(jobs, (list, tuple)):
        raise ValueError(
            f"internal status의 'jobs'는 목록이어야 합니다 (got {type(jobs).__name__})")
    out = []
    parsed = 0
    for job in jobs:
        if not isinstance(job, dict):
            log.debug("internal status: dict 아닌 항목 무시 %r", job)
            continue
        st = job_status_from_dict(job, now)
        if st is None:
            continue
        parsed += 1
        if keep_ids is None or st.job_id in keep_ids:
            out.append(st)
    dropped = len(jobs) - parsed
    if jobs and not parsed:
        # **전멸**은 형식 불일치다(예: dataId 표기가 다른 사이트). 이걸 빈
        # 결과로 돌려주면 정상 응답 "없음"과 구별되지 않아, 유예가 끝나는
        # 대로 전 job이 LOST(되돌릴 수 없음)로 확정된다. 조회 장애로 올려
        # 판단을 보류시킨다.
        raise ValueError(
            f"internal status 응답 {len(jobs)}건을 한 건도 해석하지 못했습니다 "
            f"— 형식 불일치로 보입니다 (첫 항목 키: {sorted(jobs[0])[:8]})"
            if isinstance(jobs[0], dict) else
            f"internal status 응답 {len(jobs)}건이 모두 dict가 아닙니다")
    if dropped:
        log.warning("internal status: 해석 못 한 행 %d/%d건 무시 "
                    "(DEBUG 로그에 원문)", dropped, len(jobs))
    return out


#: 만료 청소 최소 간격(초) — 원장이 클 때 매 폴링 전수 스캔을 피한다.
#: 보존 기간(주 단위)에 비하면 무시할 만한 지연이라 정확도 손해가 없다.
_PRUNE_MIN_INTERVAL_S = 60.0


@dataclass(frozen=True)
class _Entry:
    """원장 1칸 — 상태 + 이 항목을 마지막으로 받은 시각.

    seen_at은 finish_time이 없는 종료 job의 만료 폴백이다. payload가
    finishTime을 안 주는 사이트에서 DONE/EXIT이 영원히 쌓이는 것을 막는다.
    """
    status: Any
    seen_at: datetime


class InternalStatusSource:
    """앱 콜백 기반 상태 조회원 — bjobs_by_ids와 같은 계약을 제공한다.

    내부 원장은 (job_id, array_index) 키의 누적 dict이고, 콜백이 돌 때마다
    병합된다. 조회는 이 원장에서 id로 뽑아 준다.
    """

    #: 동시에 남아 있을 수 있는 **미회수 조회**의 상한. 콜백이 영영 안
    #: 돌아올 때 인계를 무한히 허용하면 daemon 스레드가 계속 쌓인다.
    MAX_INFLIGHT = 3

    def __init__(self, fetcher: JobStatusFetcher, *,
                 failover: Optional[JobStatusFetcher] = None,
                 refresh_min_s: Optional[float],
                 poll_interval_s: float = 0.0,
                 wait_timeout_s: float,
                 retention_days: float = 14.0,
                 track_runtime: bool = True):
        if not callable(fetcher):
            raise ValueError(
                f"job_status_fetcher는 호출 가능해야 합니다 (got {fetcher!r})")
        if failover is not None and not callable(failover):
            raise ValueError(
                f"job_status_fetcher_failover는 호출 가능해야 합니다 (got {failover!r})")
        self._fetcher = fetcher
        #: 예비 콜백 — 주 콜백이 실패/미회수일 때 같은 조회를 이걸로 재시도.
        self._failover = failover
        #: 시작했지만 아직 안 돌아온 **주 콜백** 호출 수. 예비가 있을 때 이
        #: 값이 0이 아니면 새 조회를 처음부터 예비로 보낸다 — 매번 주부터
        #: 걸면 인계 사이클마다 wait_timeout_s를 통째로 다시 날린다.
        self._primary_unreturned = 0
        #: 건강 판정 — 마지막으로 끝난 조회의 결과 (fetcher_state()).
        self._health = FetcherState.IDLE
        self._health_at = float("-inf")          # 판정의 늦은 쓰기 술어용 시계
        #: 이 간격 안에 다시 들어온 조회는 콜백을 다시 돌리지 않는다.
        #: refresh_min_s=None이면 **자동** — 실제 폴링 주기의 절반을 따라간다
        #: (note_poll_interval). 앱이 값을 명시하면 그 값을 지킨다.
        #: "자동인가"를 따로 받지 않고 여기서 판정하는 이유: 예전엔 호출자가
        #: 같은 config 필드에서 값과 플래그를 **각각** 유도해 넘겨, 둘이
        #: 어긋날 수 있는 구조였다.
        self._explicit = refresh_min_s is not None
        self._refresh_min_s = (float(refresh_min_s) if self._explicit
                               else max(0.0, float(poll_interval_s)) / 2.0)
        #: 지금까지 통지받은 **가장 짧은** 폴링 주기 (자동 모드의 기준)
        self._min_poll_interval_s: Optional[float] = None
        #: 조회 1건을 기다리는 상한 — 넘으면 '조회 장애'로 보고해 호출자가
        #: 영원히 붙잡히지 않게 한다(bjobs 타임아웃과 같은 취급: 판단
        #: 보류이지 LOST가 아니다). 이 시간을 넘긴 조회는 인계 대상이 된다.
        self._wait_timeout_s = max(1.0, float(wait_timeout_s))
        #: 종료 job 보존 기간 — 넘으면 원장에서 버린다.
        self._retention = timedelta(days=max(0.0, float(retention_days)))
        #: 증분 payload에 안 온 진행 중 job의 경과시간을 매 조회마다 갱신할지
        #: (= config.poll_runtime_updates). 꺼져 있으면 monitor가 run_time
        #: 변화를 갱신 대상으로 보지 않으므로 그 갱신은 통째로 버려진다 —
        #: 원장 전수 스캔 + JobStatus/_Entry 재생성을 조회마다 하는 값이
        #: 아무 데도 안 쓰인다(2만 건 기준 조회당 37ms, 그동안 원장 lock을
        #: 쥐고 있어 폴링 읽기가 그만큼 밀린다).
        self._track_runtime = bool(track_runtime)
        # 원장·진행 상태·대기를 한 lock으로 묶는다 — 병합과 조회가 섞이면
        # 반쯤 병합된 원장이 보인다. 콜백은 전용 스레드에서 lock 없이 돈다.
        self._cv = threading.Condition(threading.Lock())
        #: job_id → {array_index: _Entry}. monitor가 array element를 집계할 수
        #: 있게 element를 따로 들고 있는다(bjobs가 element별 행을 주는 것과 동형).
        self._ledger: Dict[int, Dict[Optional[int], _Entry]] = {}
        #: **조회 요청을 받은 적 있는 job_id**. 콜백은 유저의 전 job을 주는데
        #: lsfmgr가 추적하는 건 그중 일부다 — 나머지까지 보관하면 원장이
        #: '이 앱과 무관한 job'으로 부풀어 오른다(10만 건 ≈ 60MB 실측).
        #: 조회 시점에 **병합보다 먼저** 등록하므로 갓 제출된 job도 첫 조회에서
        #: 누락되지 않는다.
        self._interest: Set[int] = set()
        self._last_fetch_at = float("-inf")      # time.monotonic()
        self._last_prune_at = float("-inf")
        self._fetching = False
        self._fetch_deadline = float("inf")      # 진행 중 조회의 인계 시점
        self._fetch_token: object = None         # 현재 조회의 소유 표식
        self._inflight = 0                       # 아직 안 끝난 콜백 스레드 수
        self._gen = 0                            # 콜백 완료 세대(성공/실패 무관)
        self._closed = False

    # ------------------------------------------------------------------
    # 조회 API — LsfCommand가 그대로 위임한다
    # ------------------------------------------------------------------
    def statuses_by_ids(self, job_ids: Sequence[int], *, fresh: bool = False
                        ) -> Tuple[List, Set[int]]:
        """bjobs_by_ids와 동일 계약 — (찾은 것, 판단 보류할 id 집합).

        fresh=True면 캐시를 건너뛰고 이 호출 이후에 시작된 조회 결과만
        받는다 — kill verify처럼 '방금'을 봐야 하는 경로용.
        """
        ids = [int(i) for i in job_ids]
        if not ids:
            return [], set()
        self._register_interest(ids)         # 병합 필터보다 **먼저**
        if not self._ensure_fetched(fresh=fresh):
            # 조회 자체가 실패 — 전원 보류. monitor가 LOST로 확정하지 않는다.
            return [], set(ids)
        out: List = []
        with self._cv:
            for job_id in ids:
                for entry in self._ledger.get(job_id, {}).values():
                    out.append(entry.status)
        return out, set()

    def invalidate(self) -> None:
        """다음 조회가 반드시 콜백을 돌게 한다 — 원장은 그대로 둔다
        (누적 데이터가 아니라 '언제 마지막으로 받았나'만 무효화)."""
        with self._cv:
            self._last_fetch_at = float("-inf")

    def forget(self, job_ids: Sequence[int]) -> None:
        """추적을 그만둔 job을 원장·관심 집합에서 즉시 버린다
        (remove_jobs/clear_jobs/remove_jobset).

        만료(_prune_locked)만으로는 안 빠지는 것들이 있다: 만료는 **종료
        (DONE/EXIT)** 항목만 보고, 원장에 아예 오른 적 없는 job(LOST/
        SUBMIT_FAILED/CANCELLED)은 관심 집합에만 남는다. 레코드가 사라져
        아무도 조회하지 않게 된 뒤에도 그 항목은 매 조회의 전수 스캔
        대상으로 남아, jobset을 만들고 지우기를 반복하는 장수 세션에서
        원장이 계속 커진다."""
        ids = {int(i) for i in job_ids}
        if not ids:
            return
        with self._cv:
            self._interest -= ids
            for job_id in ids:
                self._ledger.pop(job_id, None)

    def note_poll_interval(self, interval_s: float) -> None:
        """실제 폴링 주기를 알려 준다 — 자동 모드면 갱신 간격을 **가장 짧은**
        주기의 절반으로 맞춘다.

        기본값은 LsfConfig.poll_interval_s에서 유도되는데, 그 값은 앱이 실제로
        쓰는 주기가 아닐 수 있다. manager kwarg로 준 poll_interval_s는 config가
        아니라 _defaults로 들어가고, `start_polling(js, 2.0)`처럼 호출마다 다른
        주기를 줄 수도 있다. 그대로 두면 폴링은 2초마다 도는데 캐시가 5초라
        갱신이 밀린다. 최솟값 기준인 이유는 jobset마다 주기가 다를 수 있어서다
        — 가장 빠른 폴러에 맞춰야 아무도 캐시에 막히지 않는다.
        """
        if self._explicit:
            return                           # 앱이 정한 값 — 건드리지 않는다
        interval = max(0.0, float(interval_s))
        with self._cv:
            if (self._min_poll_interval_s is not None
                    and interval >= self._min_poll_interval_s):
                return
            self._min_poll_interval_s = interval
            before, self._refresh_min_s = self._refresh_min_s, interval / 2.0
            if abs(before - self._refresh_min_s) > 1e-9:
                log.info("internal status 갱신 간격 %.1fs → %.1fs "
                         "(폴링 주기 %.1fs에 맞춤)",
                         before, self._refresh_min_s, interval)

    def shutdown(self) -> None:
        """종료 — 대기 중인 호출자를 즉시 풀어 주고 새 조회를 막는다.

        폴링 스레드가 조회를 기다리는 중에 shutdown이 걸리면 이게 없으면
        wait_timeout_s(기본 120초)만큼 종료가 지연된다.
        """
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def fetcher_state(self) -> FetcherState:
        """건강 판정 — 마지막으로 끝난 조회 기준 (FetcherState docstring 참고)."""
        with self._cv:
            return self._health

    def stats(self) -> Dict[str, Any]:
        """원장 현황 — 진단/테스트용."""
        with self._cv:
            jobs = sum(len(v) for v in self._ledger.values())
            return {"job_ids": len(self._ledger), "entries": jobs,
                    "tracked_ids": len(self._interest),
                    "inflight": self._inflight,
                    # 예비 콜백 진단 — 건강 판정과 미회수 주 콜백 수.
                    "fetcher_state": self._health.value,
                    "served_by_failover": self._health is FetcherState.FAILOVER,
                    "primary_unreturned": self._primary_unreturned,
                    # 실제로 적용 중인 갱신 간격 — 자동 모드면 폴링 주기를
                    # 따라 바뀌므로 설정값이 아니라 여기를 봐야 한다.
                    "refresh_min_s": self._refresh_min_s}

    # ------------------------------------------------------------------
    # 콜백 실행 — single-flight + 인계
    # ------------------------------------------------------------------
    def _register_interest(self, ids: Sequence[int]) -> None:
        with self._cv:
            self._interest.update(ids)

    def _ensure_fetched(self, *, fresh: bool) -> bool:
        """원장이 충분히 최신인지 확인하고, 아니면 콜백을 돌린다.

        반환: True=원장을 믿고 읽어도 됨, False=조회 장애(판단 보류).
        min_at 기준으로 판정한다 — 일반 조회는 "간격 안이면 재사용", fresh
        조회는 "이 호출 시각 이후에 받은 것만".

        루프 한 바퀴 = "최신인가? → 아니면 누군가 조회 중이게 만든다 →
        한 번 기다린다 → 결과를 판정한다". 세 관심사를 이름으로 갈라 뒀다
        (예전엔 한 루프에 엉켜 있었다).
        """
        started = time.monotonic()
        min_at = started if fresh else started - self._refresh_min_s
        deadline = started + self._wait_timeout_s
        with self._cv:
            while True:
                if self._closed:
                    return False
                if self._last_fetch_at >= min_at:
                    return True
                if not self._start_or_takeover_locked():
                    return False             # 미회수 조회 상한 — 판단 보류
                before, gen = self._last_fetch_at, self._gen
                done = self._wait_once_locked(deadline, gen)
                if done is None:
                    continue                 # 아직 안 끝남 — 다시 판정
                if not done:
                    return False             # 시간 초과 / 종료
                if self._last_fetch_at <= before:
                    # 조회가 실패했다. 여기서 **내가 다시 띄우지 않는다** —
                    # 콜백이 죽어 있을 때 대기자마다 재시도하면 연타가 된다.
                    return False
                if self._last_fetch_at >= min_at or not fresh:
                    # 기준을 채웠거나, 일반 조회라면 방금 받은 것이 최신이다.
                    return True
                # fresh인데 그 조회는 내 호출보다 **먼저** 시작됐다 —
                # kill verify가 요구하는 '방금'이 아니므로 다시 받는다.

    def _start_or_takeover_locked(self) -> bool:
        """[lock 보유] 조회가 **진행 중인 상태로 만든다**.

        아무도 안 돌고 있으면 시작하고, 진행 중 조회가 상한을 넘겼으면 인계해
        새로 띄운다 — 인계하지 않으면 아무도 리더가 못 돼 서버가 회복돼도
        영원히 '조회 장애'로 남는다. 인계는 동시 미회수 조회 수를 상한으로
        묶는다(안 그러면 안 돌아오는 콜백마다 daemon 스레드가 쌓인다).

        반환: 조회가 진행 중이 되었는가 (False = 미회수 상한이라 포기)."""
        if not self._fetching:
            self._start_fetch_locked()
            return True
        if time.monotonic() < self._fetch_deadline:
            return True                      # 아직 기다릴 만하다
        if self._inflight >= self.MAX_INFLIGHT:
            # 콜백을 특정하지 않는다 — 주가 빨리 실패하고 예비가 갇힌
            # 경우도 이 경로다. 지목이 틀리면 앱이 엉뚱한 콜백을 고친다.
            log.error("미회수 status 조회가 %d건 — 새 조회를 띄우지 않습니다. "
                      "상태 조회 콜백이 반환하지 않고 있습니다"
                      "(콜백에 timeout을 주세요)", self._inflight)
            return False
        log.warning("진행 중 status 조회가 %.0fs를 넘겨 새로 시작합니다 — "
                    "상태 조회 콜백이 반환하지 않고 있습니다"
                    "(콜백에 timeout을 주세요)", self._wait_timeout_s)
        self._start_fetch_locked()
        return True

    def _wait_once_locked(self, deadline: float,
                          gen: int) -> Optional[bool]:
        """[lock 보유] 진행 중 조회를 **한 번** 기다린다.

        반환: True=조회가 하나 끝났다(성공/실패 무관) / False=시간 초과나
        종료(판단 보류) / None=아직 안 끝났으니 caller가 다시 판정하라.
        한 번만 기다리는 이유: 깨어날 때마다 caller가 인계 조건을 다시 보게
        하려는 것이다(콜백이 영영 안 돌아오는 경우의 유일한 탈출구)."""
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not self._cv.wait(timeout=remaining):
            log.warning("internal status 조회 대기 시간 초과(%.0fs) — "
                        "이번 사이클은 판단 보류", self._wait_timeout_s)
            return False
        if self._closed:
            return False
        return True if self._gen != gen else None

    def _start_fetch_locked(self) -> None:
        """콜백을 전용 daemon 스레드로 띄운다 (lock 보유 상태에서 호출)."""
        token = object()
        self._fetching = True
        self._fetch_token = token
        self._fetch_deadline = time.monotonic() + self._wait_timeout_s
        self._inflight += 1
        # 주 콜백이 미회수(안 돌아옴)면 처음부터 예비로 — 인계 경로에서 또
        # 주부터 걸면 그 조회도 같은 이유로 wait_timeout_s를 다 쓴다.
        use_failover = (self._failover is not None
                        and self._primary_unreturned > 0)
        if use_failover:
            # 주 콜백이 오래 갇혀 있으면 매 조회가 이 경로다 — 전환(첫 회)만
            # 알리고 반복은 debug로 내린다 (failover 경로의 warning과 동일 원칙).
            emit = (log.debug if self._health is FetcherState.FAILOVER
                    else log.info)
            emit("이번 status 조회는 예비 콜백으로 갑니다 "
                 "(주 콜백 미회수 %d건)", self._primary_unreturned)
        threading.Thread(target=self._fetch_worker, args=(token, use_failover),
                         name="lsfmgr-status-fetch", daemon=True).start()

    def _attempt(self, fetcher: JobStatusFetcher, now: datetime) -> List:
        """[전용 스레드] 콜백 1회 호출 + 파싱 — 실패는 예외 그대로 올린다."""
        payload = fetcher()
        # 관심 스냅샷은 콜백이 **돌아온 뒤에** 뜬다 — 콜백 앞에서 뜨면
        # 그 왕복(수 초) 동안 새로 제출돼 조회에 들어온 job이 파싱
        # 필터에 걸려 버려진다. 병합은 그 job을 받아들이는데(관심
        # 검사는 lock 아래 최신값) 파싱이 이미 지웠으니, 결과는
        # '조회 장애'가 아니라 **미발견**이라 monitor의 LOST 스트릭이
        # 올라간다(유예를 0으로 둔 앱에서는 곧장 LOST).
        with self._cv:
            keep = set(self._interest)           # 파싱 단계 필터용 스냅샷
        return parse_internal_jobs(payload, now, keep)

    def _attempt_primary(self, now: datetime) -> List:
        """[전용 스레드] 주 콜백 1회 — 미회수 부기를 두른다.

        counter는 try 안에서 올리고 finally에서 내린다. 콜백이 안 돌아오면
        이 스레드가 여기 갇힌 채 counter가 올라 있어, 다음 조회가 처음부터
        예비로 가는 근거가 된다(돌아오는 순간 내려가 주 콜백이 복권된다).
        """
        with self._cv:
            self._primary_unreturned += 1
        try:
            return self._attempt(self._fetcher, now)
        finally:
            with self._cv:
                self._primary_unreturned -= 1

    def _fetch_worker(self, token: object, use_failover: bool) -> None:
        """[전용 스레드] 콜백 1회 실행 + 원장 병합.

        use_failover이면 처음부터 예비 콜백으로 간다(주 콜백 미회수 — 인계
        경로). 아니면 주 콜백을 돌리고, 실패하면 **같은 스레드에서** 예비
        콜백으로 한 번 더 시도한다 — 그래서 호출자는 어느 쪽이 답했는지
        모른 채 같은 계약의 결과만 받는다.
        """
        statuses = None
        served_by_failover = use_failover
        error: Optional[BaseException] = None
        at = time.monotonic()
        now = datetime.now()
        try:
            if use_failover:
                statuses = self._attempt(self._failover, now)
            else:
                try:
                    statuses = self._attempt_primary(now)
                except Exception as primary_err:  # noqa: BLE001
                    if self._failover is None:
                        raise
                    # 전환 순간만 warning — 주 콜백이 오래 죽어 있으면 매
                    # 조회가 이 경로라, 이미 강등 상태(FAILOVER/DOWN)의
                    # 반복분은 debug로 내린다.
                    with self._cv:
                        degraded = self._health in (FetcherState.FAILOVER,
                                                    FetcherState.DOWN)
                    emit = log.debug if degraded else log.warning
                    emit("주 콜백(job_status_fetcher) 실패 — 예비 콜백으로 "
                         "다시 시도합니다: %r", primary_err)
                    try:
                        statuses = self._attempt(self._failover, now)
                        served_by_failover = True
                    except Exception:
                        # 최종 오류는 아래 공통 로그가 맡는다 — 여기서는
                        # 거기 안 실리는 주 콜백 쪽 문맥만 남긴다.
                        log.warning("예비 콜백(job_status_fetcher_failover)도 "
                                    "실패했습니다 — 주 콜백 오류: %r",
                                    primary_err)
                        raise
        except Exception as e:                   # noqa: BLE001 — 조회 장애로 강등
            error = e
        finally:
            # 어떤 예외(BaseException 포함)로 빠져나가도 부기를 마치고
            # 대기자를 깨운다 — 안 그러면 이후 조회가 전부 대기만 하다
            # 타임아웃되고, 폴링이 영영 상태를 못 올린다.
            with self._cv:
                self._inflight -= 1
                try:
                    if statuses is not None:
                        # 병합은 늦은 쓰기 술어 **밖**이다 — 조회가 늦게
                        # 돌아왔어도 응답은 콜백이 돌아온 시점에 서버가 준
                        # 것이라(시작 시각과 무관), 버릴 이유가 없다.
                        self._merge_locked(statuses, now)
                        # 성공 표식은 병합이 끝난 뒤에 찍는다 — 반쯤 병합된
                        # 원장을 '최신'으로 광고하면 안 된다. at 비교(늦은
                        # 쓰기 술어)인 이유: 인계된 뒤 뒤늦게 돌아온 조회가
                        # 시계를 되돌리면 안 된다.
                        if at > self._last_fetch_at:
                            self._last_fetch_at = at
                        self._prune_locked(at, now)
                except Exception as e:           # noqa: BLE001
                    error = e                    # 병합/청소 실패도 조회 장애
                finally:
                    # 건강 판정은 성공·실패 **모두** 기록한다 (병합 실패 포함).
                    self._note_outcome_locked(
                        at, ok=error is None and statuses is not None,
                        served_by_failover=served_by_failover)
                    if self._fetch_token is token:
                        # 인계되지 않았을 때만 진행 상태를 건드린다 — 뒤늦게
                        # 돌아온 조회가 새 조회의 부기를 덮으면 안 된다.
                        self._fetching = False
                        self._fetch_deadline = float("inf")
                        self._gen += 1
                    self._cv.notify_all()
        if error is not None:
            log.warning("internal status 조회 실패 — 이번 사이클은 판단 보류: %r",
                        error)

    # ------------------------------------------------------------------
    # 원장 병합 / 만료 (둘 다 lock 보유 상태에서만 호출)
    # ------------------------------------------------------------------
    def _note_outcome_locked(self, at: float, ok: bool,
                             served_by_failover: bool) -> None:
        """[lock 보유] 이번 조회의 결과로 건강 판정(fetcher_state)을 갱신.

        늦은 쓰기 술어(at 비교) — 인계된 뒤 뒤늦게 끝난 조회가 더 새 조회의
        판정을 덮으면 안 된다. _last_fetch_at과 원칙은 같지만 시계는 따로다:
        건강은 실패도 기록하지만 _last_fetch_at은 성공만 전진시킨다.

        로그는 **좋아지는 전환**만 여기서 낸다 — 회복은 조용해서 여기서
        알려야 보인다. 나빠지는 전환(→DOWN)과 주→예비 전환의 warning은
        실패가 난 지점이 이미 남긴다.
        """
        if at <= self._health_at:
            return
        self._health_at = at
        new = (FetcherState.DOWN if not ok
               else FetcherState.FAILOVER if served_by_failover
               else FetcherState.PRIMARY)
        old, self._health = self._health, new
        if new is old:
            return
        if new is FetcherState.PRIMARY and old in (FetcherState.FAILOVER,
                                                   FetcherState.DOWN):
            log.info("주 콜백(job_status_fetcher) 회복 — 상태 조회 정상화 "
                     "(%s → PRIMARY)", old.value)
        elif old is FetcherState.DOWN and new is FetcherState.FAILOVER:
            log.info("예비 콜백 회복 — 상태 조회가 예비로 재개됩니다 "
                     "(주 콜백은 여전히 실패)")

    def _merge_locked(self, statuses: List, now: datetime) -> None:
        """받은 상태를 원장에 덮어쓴다 — 증분 payload면 나머지는 유지된다.

        추적 대상(_interest)이 아닌 job은 버린다 — 콜백이 주는 '유저의 전
        job'을 다 보관할 이유가 없다.
        """
        fresh_keys = set()
        skipped = 0
        for st in statuses:
            if st.job_id not in self._interest:
                skipped += 1
                continue
            self._ledger.setdefault(st.job_id, {})[st.array_index] = _Entry(
                status=st, seen_at=now)
            fresh_keys.add((st.job_id, st.array_index))
        if skipped:
            log.debug("internal status: 추적 대상 아닌 job %d건 미보관", skipped)
        if self._track_runtime:
            self._refresh_runtimes_locked(now, fresh_keys)

    def _refresh_runtimes_locked(self, now: datetime, fresh_keys: set) -> None:
        """이번 payload에 **안 온** 진행 중 job의 경과시간을 갱신한다.

        증분 조회에서는 상태가 안 바뀐 RUN job이 payload에 안 온다. 그대로
        두면 그 job의 run_time이 옛 값에 멈춰 UI의 경과시간이 정지한다.
        (payload에 온 job은 파싱 단계에서 이미 최신이다.)

        원장 전수 스캔이라 비용이 원장 크기에 비례한다 —
        poll_runtime_updates가 꺼져 있으면(_track_runtime=False) 아예
        호출되지 않는다. 그때는 monitor가 run_time 변화를 갱신 대상에서
        빼므로 여기서 만든 값이 어차피 쓰이지 않는다.
        """
        for job_id, elems in self._ledger.items():
            for idx, entry in elems.items():
                if (job_id, idx) in fresh_keys:
                    continue
                st = entry.status
                if not st.state.is_on_lsf or st.start_time is None:
                    continue
                secs = int((now - st.start_time).total_seconds())
                if secs < 0 or secs == st.run_time_s:
                    continue
                # seen_at은 그대로 둔다 — 만료 기준은 '실제 수신 시각'이다.
                elems[idx] = _Entry(status=replace(st, run_time_s=secs),
                                    seen_at=entry.seen_at)

    def _expired(self, entry: _Entry, now: datetime) -> bool:
        """이 항목을 버려도 되는가.

        기준은 "끝났고, 끝난 지 오래됐다" — 종료(DONE/EXIT)가 아니면 아무리
        오래돼도 남긴다(아직 추적 대상일 수 있다). finish_time이 없는
        종료 job은 마지막으로 받은 시각(seen_at)을 대신 쓴다.
        """
        st = entry.status
        if st.state not in (JobState.DONE, JobState.EXIT):
            return False
        marker = st.finish_time if st.finish_time is not None else entry.seen_at
        return (now - marker) > self._retention

    def _prune_locked(self, at: float, now: datetime) -> None:
        """만료분 청소 — 최소 간격을 둬 매 폴링 전수 스캔을 피한다."""
        if self._retention <= timedelta(0):
            return                               # 보존 0 = 만료 끔
        if at - self._last_prune_at < _PRUNE_MIN_INTERVAL_S:
            return
        self._last_prune_at = at
        dropped = 0
        for job_id in list(self._ledger):
            elems = self._ledger[job_id]
            for idx in [i for i, e in elems.items() if self._expired(e, now)]:
                del elems[idx]
                dropped += 1
            if not elems:
                del self._ledger[job_id]
                self._interest.discard(job_id)   # 관심 집합도 같이 줄인다
        if dropped:
            log.info("internal status 원장 청소: 종료 후 %s 지난 %d건 제거 "
                     "(남은 job %d건)", self._retention, dropped,
                     len(self._ledger))
