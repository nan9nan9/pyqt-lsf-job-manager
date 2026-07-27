# 전체 JobSet 상태 카운트 합산 — `mgr.total_summary()`

`LsfJobManager`에 전체 store를 가로질러 상태를 한 번에 집계하는 **내장 메서드**가
있다. 전체 jobset의 RUN/PEND/EXIT/... 카운트를 구할 때는 이걸 쓰면 된다.

## 기본 사용

```python
agg = mgr.total_summary()
# → {"total": 전체_intended 합계, "RUN": .., "PEND": .., "EXIT": .., "DONE": .., ...}
```

반환 dict의 규칙은 jobset 단위 `summary()`와 같다:

- 키: `JobState.value` 문자열(`"RUN"`, `"PEND"`, `"EXIT"`, `"DONE"`, `"CREATED"` 등)과
  `"total"`.
- `"total"` = 각 jobset `intended_count`의 합. `make_summary`가 **상태 합계 ==
  intended_count** 불변식을 유지하므로 전체에서도 상태 합계 == `"total"`이다.
- 어떤 jobset에도 없는 상태 키는 dict에 없다 — `agg.get("RUN", 0)`으로 읽을 것.

## 내부 동작 (경합 안전)

`list_jobsets()` 스냅샷을 순회하며 각 jobset의 `summary()`를 합산한다. 스냅샷을
뜬 시점과 개별 `summary()` 호출 사이에 다른 스레드가 그 jobset을 close/merge로
지우면 `JobSetNotFoundError`가 나는데, **내부에서 그 jobset을 건너뛰고 계속**하므로
호출자가 방어할 필요가 없다 (`find_jobs`와 동일한 경합 스킵 패턴).

## 특정 태그/라벨만 합산

전체가 아니라 일부만 필요하면 `search_jobsets()`로 좁혀 직접 합산한다
(내장 메서드는 전체 전용):

```python
from collections import Counter
from lsfmgr.errors import JobSetNotFoundError

agg = Counter()
for js in mgr.search_jobsets(tag="nightly"):
    try:
        agg.update(mgr.summary(js.jobset_id))
    except JobSetNotFoundError:
        continue                        # 조회 사이에 삭제된 jobset은 스킵
```

## 주의: 스냅샷이라 LSF 최신이 아님

`total_summary()`는 **Store 스냅샷 조회일 뿐 LSF를 호출하지 않는다**
(`[sync, snapshot]`). 반환값이 LSF 최신 상태를 반영하려면 각 jobset이
`PollingService`로 폴링되고 있어야 한다. 폴링이 걸리지 않은 jobset은 마지막
상태 전이 값으로 고정된다.

실시간 갱신이 필요한 화면이라면 매번 합산을 돌리기보다,
`jobset_updated = Signal(jobset_id, summary)`를 받아 jobset별 최신 summary를
캐시에 두고 그 캐시를 합산하는 편이 정확하다.

## 관련 코드 위치

- `LsfJobManager.total_summary()` — `lsfmgr/manager.py`
- `LsfJobManager.summary(jobset_id)` / `search_jobsets(...)` — `lsfmgr/manager.py`
- 카운트 집계 단일 지점 `make_summary()` — `lsfmgr/store/base.py`
- 스레드 경합 스킵 패턴 참고 `find_jobs()` — `lsfmgr/store/base.py`
