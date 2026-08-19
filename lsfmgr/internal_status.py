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
from typing import (
    Any, Callable, Dict, List, Optional, Sequence, Set, Tuple,
)

from .states import LSF_STAT_MAP, JobState

log = logging.getLogger("lsfmgr.internal_status")

#: 콜백 시그니처 — 인자 없이 호출되고, REST 응답 JSON을 그대로 돌려준다.
#: 반환: {"jobs": [...], "count": N, ...} dict 또는 job dict 목록.
#: 예외를 던지면 그 사이클은 '조회 장애'로 취급된다(대상 전원 판단 보류).
JobStatusFetcher = Callable[[], Any]

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


#: JobStatus는 command.py 소유인데 command.py가 이 모듈을 import한다(순환).
#: 매 job마다 import 문을 도는 대신 첫 호출에 한 번만 해석해 캐시한다 —
#: payload가 수만 건일 때 파싱 루프의 군더더기를 없앤다.
_JOB_STATUS_CLS = None


def _job_status_cls():
    global _JOB_STATUS_CLS
    if _JOB_STATUS_CLS is None:
        from .command import JobStatus
        _JOB_STATUS_CLS = JobStatus
    return _JOB_STATUS_CLS


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

    return _job_status_cls()(
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
                 refresh_min_s: float,
                 wait_timeout_s: float,
                 retention_days: float = 14.0,
                 auto_refresh: bool = False):
        if not callable(fetcher):
            raise ValueError(
                f"job_status_fetcher는 호출 가능해야 합니다 (got {fetcher!r})")
        self._fetcher = fetcher
        #: 이 간격 안에 다시 들어온 조회는 콜백을 다시 돌리지 않는다.
        self._refresh_min_s = max(0.0, float(refresh_min_s))
        #: True면 실제 폴링 주기를 알게 될 때 위 값을 자동으로 낮춘다
        #: (앱이 값을 명시했으면 건드리지 않는다).
        self._auto_refresh = bool(auto_refresh)
        #: 지금까지 통지받은 **가장 짧은** 폴링 주기 (자동 모드의 기준)
        self._min_poll_interval_s: Optional[float] = None
        #: 조회 1건을 기다리는 상한 — 넘으면 '조회 장애'로 보고해 호출자가
        #: 영원히 붙잡히지 않게 한다(bjobs 타임아웃과 같은 취급: 판단
        #: 보류이지 LOST가 아니다). 이 시간을 넘긴 조회는 인계 대상이 된다.
        self._wait_timeout_s = max(1.0, float(wait_timeout_s))
        #: 종료 job 보존 기간 — 넘으면 원장에서 버린다.
        self._retention = timedelta(days=max(0.0, float(retention_days)))
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

    def clusters_by_ids(self, job_ids: Sequence[int]) -> Dict[str, str]:
        """bjobs_clusters_by_ids와 동일 계약 — target 문자열 → cluster.

        조회 실패는 빈 dict — 미상은 caller가 기본 env로 처리하므로 kill
        자체는 반드시 나간다(bjobs 경로와 같은 관대 처리).
        """
        ids = [int(i) for i in job_ids]
        if not ids:
            return {}
        self._register_interest(ids)
        if not self._ensure_fetched(fresh=False):
            return {}
        out: Dict[str, str] = {}
        with self._cv:
            for job_id in ids:
                for entry in self._ledger.get(job_id, {}).values():
                    st = entry.status
                    cluster = st.forward_cluster or st.source_cluster
                    if not cluster:
                        continue
                    out[str(job_id)] = cluster            # parent id 표기
                    if st.array_index is not None:
                        out[f"{job_id}[{st.array_index}]"] = cluster
        return out

    def invalidate(self) -> None:
        """다음 조회가 반드시 콜백을 돌게 한다 — 원장은 그대로 둔다
        (누적 데이터가 아니라 '언제 마지막으로 받았나'만 무효화)."""
        with self._cv:
            self._last_fetch_at = float("-inf")

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
        if not self._auto_refresh:
            return
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

    def stats(self) -> Dict[str, Any]:
        """원장 현황 — 진단/테스트용."""
        with self._cv:
            jobs = sum(len(v) for v in self._ledger.values())
            return {"job_ids": len(self._ledger), "entries": jobs,
                    "tracked_ids": len(self._interest),
                    "inflight": self._inflight}

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
                if not self._fetching:
                    self._start_fetch_locked()
                elif time.monotonic() >= self._fetch_deadline:
                    # 진행 중 조회가 상한을 넘겼다 = 콜백이 안 돌아온다.
                    # 인계하지 않으면 아무도 리더가 못 돼 서버가 회복돼도
                    # 영원히 '조회 장애'로 남는다.
                    if self._inflight >= self.MAX_INFLIGHT:
                        log.error(
                            "미회수 status 조회가 %d건 — 새 조회를 띄우지 "
                            "않습니다. job_status_fetcher가 반환하지 않고 "
                            "있습니다(콜백에 timeout을 주세요)", self._inflight)
                        return False
                    log.warning(
                        "진행 중 status 조회가 %.0fs를 넘겨 새로 시작합니다 — "
                        "job_status_fetcher가 반환하지 않고 있습니다"
                        "(콜백에 timeout을 주세요)", self._wait_timeout_s)
                    self._start_fetch_locked()
                before, gen = self._last_fetch_at, self._gen
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cv.wait(timeout=remaining):
                    log.warning("internal status 조회 대기 시간 초과(%.0fs) — "
                                "이번 사이클은 판단 보류", self._wait_timeout_s)
                    return False
                if self._closed:
                    return False
                if self._gen == gen:
                    continue                     # 아직 안 끝남 — 다시 대기
                if self._last_fetch_at <= before:
                    # 조회가 실패했다. 여기서 **내가 다시 띄우지 않는다** —
                    # 콜백이 죽어 있을 때 대기자마다 재시도하면 연타가 된다.
                    return False
                if self._last_fetch_at >= min_at:
                    return True
                if fresh:
                    # 그 조회는 내 호출보다 **먼저** 시작됐다 — kill verify가
                    # 요구하는 '방금'을 만족하지 못하므로 다시 받는다.
                    continue
                # 갱신 간격 기준에는 못 미쳐도 방금 받은 것이 가장 최신이다.
                return True

    def _start_fetch_locked(self) -> None:
        """콜백을 전용 daemon 스레드로 띄운다 (lock 보유 상태에서 호출)."""
        token = object()
        self._fetching = True
        self._fetch_token = token
        self._fetch_deadline = time.monotonic() + self._wait_timeout_s
        self._inflight += 1
        threading.Thread(target=self._fetch_worker, args=(token,),
                         name="lsfmgr-status-fetch", daemon=True).start()

    def _fetch_worker(self, token: object) -> None:
        """[전용 스레드] 콜백 1회 실행 + 원장 병합."""
        statuses = None
        error: Optional[BaseException] = None
        at = time.monotonic()
        now = datetime.now()
        with self._cv:
            keep = set(self._interest)       # 파싱 단계 필터용 스냅샷
        try:
            try:
                statuses = parse_internal_jobs(self._fetcher(), now, keep)
            except Exception as e:               # noqa: BLE001 — 조회 장애로 강등
                error = e
        finally:
            # 어떤 예외(BaseException 포함)로 빠져나가도 부기를 마치고
            # 대기자를 깨운다 — 안 그러면 이후 조회가 전부 대기만 하다
            # 타임아웃되고, 폴링이 영영 상태를 못 올린다.
            with self._cv:
                self._inflight -= 1
                try:
                    if statuses is not None:
                        self._merge_locked(statuses, now)
                        # 성공 표식은 병합이 끝난 뒤에 찍는다 — 반쯤 병합된
                        # 원장을 '최신'으로 광고하면 안 된다. max()인 이유:
                        # 인계된 뒤 뒤늦게 돌아온 조회가 시계를 되돌리면 안 된다.
                        if at > self._last_fetch_at:
                            self._last_fetch_at = at
                        self._prune_locked(at, now)
                except Exception as e:           # noqa: BLE001
                    error = e                    # 병합/청소 실패도 조회 장애
                finally:
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
        self._refresh_runtimes_locked(now, fresh_keys)

    def _refresh_runtimes_locked(self, now: datetime, fresh_keys: set) -> None:
        """이번 payload에 **안 온** 진행 중 job의 경과시간을 갱신한다.

        증분 조회에서는 상태가 안 바뀐 RUN job이 payload에 안 온다. 그대로
        두면 그 job의 run_time이 옛 값에 멈춰 UI의 경과시간이 정지한다.
        (payload에 온 job은 파싱 단계에서 이미 최신이다.)
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
