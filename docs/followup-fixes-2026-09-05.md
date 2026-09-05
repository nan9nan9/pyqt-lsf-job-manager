# 추가 경합 점검과 수정 — 2026-09-05

기준 커밋은 `085c298`이다. 이전 수정 후 남아 있던 조회 결과 저장,
표시 대기열 배출, MockLSF 자원 정산 경계에서 결함 3건을 재현하고 수정했다.

## 1. 이전 실행의 미발견 횟수를 새 실행이 물려받음

조회가 미발견 횟수를 계산한 뒤 저장하기 전에 재제출·교체·삭제가 진행되면,
`forget`이 지운 횟수를 이전 조회가 다시 저장했다. 같은 key에 새 job을 제출하면
그 횟수를 물려받아 첫 미발견부터 LOST로 확정됐다. 반대로 새 실행의 조회가
재무장 시점의 `forget`보다 먼저 시작해도 이전 횟수를 읽을 수 있었다.

[monitor.py](../lsfmgr/monitor.py)의 횟수 항목에 기존 `_generation`을 함께
저장한다. 읽을 때는 같은 실행의 횟수만 이어 세고, 쓸 때는 현재 Store의 실행과
일치하고 아직 조회 대상인 key만 보존한다. 재확인과 저장은 기존 스트릭 잠금
아래 수행하므로 `forget`과의 순서도 유지한다.

재확인은 `get_jobs_by_keys`로 해당 key만 읽는다. `query_ids`의 범위별 조회와
관심 ID 정리 규칙은 그대로다. 미발견 횟수 항목이 비어 있으면 Store 재확인을
생략해 정상 폴링 사이클의 읽기 호출 수를 유지한다.
실제 LOST 전이 전에 확정을 알리던 중복 경고는
제거했고, Store에 적용된 LOST 레코드의 기존 로그는 유지한다.

기존 테스트 두 곳은 내부 횟수 항목에서 정수 횟수를 꺼내 읽도록 조정했다.
삭제한 key의 정리와 살아 있는 다른 key의 유예를 검사하는 assertion은 유지했다.

## 2. pacer 배출 중 다른 JobSet을 편집하면 옛 상태가 다시 전달됨

pacer는 여러 JobSet의 레코드를 큐에서 모두 꺼낸 뒤 순차 발행했다.
첫 JobSet의 `jobs_updated` 슬롯이 두 번째 JobSet을 교체·삭제해도, 이미 로컬
배출 목록으로 옮긴 레코드는 `forget`으로 제거할 수 없었다. 교체한 CREATED 뒤에
이전 DONE이 오거나, 삭제된 job의 DONE이 전역 신호에 전달됐다.

[pacer.py](../lsfmgr/pacer.py)는 먼저 JobSet별 key를 묶고, 각 JobSet을 발행하기
직전에 살아 있는 큐에서 레코드를 꺼낸다. 앞선 슬롯이 지우거나 교체한 큐는
그대로 반영된다. 종료 시 마지막 상태만 전달하는 처리도 `_drain(flush=True)`로
합쳐 같은 취소 규칙을 사용한다. 취소 여부는 pacer의 기존 큐에서 확인한다.

## 3. MockLSF에서 거부된 종료가 호스트·배열 슬롯을 비움

스케줄러가 RUN을 읽은 뒤 사용자 suspend가 USUSP를 저장하면, 이전 스냅샷의
DONE 전이는 조건부 저장에서 거부된다. 그러나 슬롯 계산은 저장 전의 가상 DONE을
사용하므로 아직 점유 중인 슬롯을 비운 것으로 간주했다. 호스트 슬롯 1개 또는
배열 동시 실행 상한 1개에서 작업 2개가 동시에 active가 되는 것을 재현했다.

[scheduler.py](../mocklsf/scheduler.py)는 실행 중 작업의 종료·suspend 변경을
먼저 저장하고 실제 active 레코드를 다시 읽은 뒤 dispatch를 계산한다.
조건부 저장과 이벤트·출력 발행은 `_apply_transitions` 한 곳에서 수행한다.
각 단계는 배치 저장을 유지하며, 저장이 거부된 전이의 출력도 생성하지 않는다.

## 검증

- [새 회귀 테스트](../tests/test_review_followup.py) **13개**: 실행 교체 4조건,
  새 조회가 forget보다 빠른 경합 1조건, pacer 일반·종료 배출과 편집 6조건,
  MockLSF 호스트·배열 상한 2조건.
- 별도 임시 디렉토리의 `085c298`에 같은 테스트를 적용하면 **13개 모두 실패**했다.
- 최종 PySide6 전체 검사: **1,122개 통과, 246.79초**.
- PySide6 관련 검사: **105개 통과, 7.75초**.
- PyQt5 관련 검사: **105개 통과, 7.57초**.
- 최종 조회 비용 계약과 새 회귀 테스트: **41개 통과, 4.58초**.
- Qt 바인딩은 `QT_API`와 `PYTEST_QT_API`를 함께 지정했고,
  `QT_QPA_PLATFORM=offscreen`으로 실행했다.
- `compileall`과 `git diff --check` 통과.
- 실제 LSF 클러스터는 사용하지 않았다. FakeLsf, Qt 이벤트 루프,
  임시 SQLite DB의 독립 연결로 검증했다.

미발견 500건의 저장·실행 재확인 비용도 측정했다. Store 1,000건에서는 약
**0.28ms**, 100,000건에서는 약 **0.30ms**였다(101회 실행의 중앙값).
이는 `_set_streaks`만 측정한 값이며 외부 조회·GUI 비용은 포함하지 않는다.

```bash
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
  python -m pytest -q --tb=short

QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
  python -m pytest -q tests/test_review_followup.py tests/test_additional_review.py \
  tests/test_state_dwell.py tests/test_review_cycle12.py tests/test_review_cycle19.py \
  tests/test_mocklsf.py tests/test_query_defer.py --tb=short
```
