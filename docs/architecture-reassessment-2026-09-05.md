# 반복 결함의 구조적 원인과 리팩터링 범위 재평가

검토 기준: `34a64e0`, AGENTS.md. `lsfmgr/`·`mocklsf/` 운영 Python 32개
파일(10,866줄)의 모듈 구성과 주요 실행·상태 변경·정리 경로, 요구사항/흐름/GUI
문서, 관련 테스트를 대조했다. 모든 동시 실행 순서나 실제 LSF 환경을 검증했다는
뜻은 아니다. 아래 평가는 수정 전 기준이다. 이후 승인된 리팩터링의 구현 내용은
마지막 절에 기록했다.

## 판단

**핵심 상태·수명 관리에는 구조적 리팩터링이 필요하다.** 개별 함수의 방어 분기를
계속 보충하는 수준으로는 부족하다. 기존 API, Qt, Store, 명령 실행기, 기능별
worker를 유지하면서 **작업 식별 → 결과 반영 → 완료 판정 → 통지·정리**의 공통
규칙을 정리해야 한다. 전면 재작성이나 새로운 범용 프레임워크를 도입할 근거는 없다.

이전 리뷰의 “국소 리팩터링”이라는 판단은 각 경로의 결함 수정 범위를 중심으로
내렸다. 모듈 구분이 타당하다는 판단은 유지하지만, 공통 계약을 함께 고쳐야 하는
범위를 충분히 잡지 못했다. 이번에는 같은 규칙을 사용하는 여러 모듈을 한 작업
범위로 묶어 검증해야 한다.

## 왜 수정 뒤에도 결함이 발견되는가

1. **잠금이 보장하는 단위보다 도메인 동작의 단위가 크다.** Store의 개별 갱신은
   원자적이다. 그러나 kill 검증의 EXIT 반영, `killed` 기록, 완료 통지는 서로
   다른 시점에 일어난다. 한 메서드에 잠금이 있어도 이 전체 동작은 보호되지 않는다.
2. **식별과 오래된 결과의 판정이 호출자마다 다르다.** 어떤 경로는 `job_key`,
   다른 경로는 `job_id + state`, 또 다른 경로는 `_generation`을 사용한다.
   각 표현의 유효 범위를 벗어나는 지점에서 다른 작업이나 과거 결과가 섞인다.
3. **수명 종료의 뒷정리를 여러 경로가 기억해야 한다.** Store 외에도 handler,
   미발견 횟수, callback 조회원의 관심 ID, pacer, 완료 latch에 실행 관련 정보가
   있다. 삭제·교체·재제출이 각각 관련 정리 호출과 순서를 맞춰야 한다.
4. **검증된 사례와 계약 전체는 다르다.** 기존 회귀는 해당 실행 순서를 검증한다.
   다른 worker가 두 단계 사이에 끼어들거나 전역 API로 범위가 바뀌는 경우까지
   자동으로 검증하지는 않는다. 고정 seed도 OS/Qt 스레드의 실행 순서를 고정하지 않는다.

따라서 “수정이 계속 새 결함을 만든다”라고 일괄 결론 내릴 수는 없다. 아래 세
재현은 마지막 수정 전 `085c298`에서도 모두 실패한다. 반면 이런 기존 결함을
늦게 찾는 원인에는 공통 규칙을 통합해서 검토하지 못한 리뷰 방식도 포함된다.

## 현재 코드에서 재현한 결함

### 1. P2 — kill 검증과 마킹 사이에 완료 통지가 먼저 발생

근거: [killer.py](../lsfmgr/killer.py#L273),
[completion.py](../lsfmgr/completion.py#L109),
[manager.py](../lsfmgr/manager.py#L1286).

`kill(js, verify=True)`의 bkill 수락 후 verify가 Store를 EXIT로 바꾼다.
`_mark_killed` 직전 worker를 멈추고 정상 `query_once(js)`를 실행하면,
CompletionTracker가 `EXIT, killed=False`를 읽어 `jobset_finished`를 발행한다.
kill을 계속 진행하면 최종 레코드는 `EXIT, killed=True`가 되지만 이미 나간
완료 신호는 취소할 수 없다. 재현 결과는 `completed=[{'total': 1, 'EXIT': 1}]`이다.

이전에 수정한 “최종 Store에서 killed가 유실됨”과 다른 관찰 지점이다. 최종
필드를 복구하는 것만으로 그 전에 외부로 발행된 잘못된 완료 통지를 막지 못한다.

수정 범위는 kill 결과 반영과 완료 판정의 관계다. 기존 kill 활동의 수명에 맞춰
관련 완료 판정을 보류하고 결과 반영이 끝나면 재평가하는 규칙이 필요하다.
JobSet 없는 원시 ID kill도 추적 레코드의 소속을 포함해야 한다. 단순히
`is_active(jsid)` 검사만 추가하면 `jsid=""`인 전역 경로는 빠질 수 있고,
보류만 하고 kill 종료 시 재평가하지 않으면 완료 자체가 누락될 수 있다.

### 2. P2 — 전역 kill 검증에서 서로 다른 JobSet의 같은 key가 충돌

근거: [killer.py](../lsfmgr/killer.py#L646),
[killer.py의 마킹](../lsfmgr/killer.py#L367),
[manager.py의 원시 ID 경로](../lsfmgr/manager.py#L585).

두 JobSet에 각각 `job_key="a"`인 job을 제출한다. 두 ID를 전역 kill하고,
첫 job만 종료 수락·EXIT, 두 번째는 권한 오류로 PEND인 응답을 준다.
`still_alive=1`은 맞지만 첫 job도 Store에서 `PEND, killed=False`로 남는다.

`_verify_global`이 생존 레코드를 `{job_key}`로 돌려주고 `_mark_killed`가 같은
집합으로 제외하기 때문이다. 두 번째 JobSet의 `a`가 첫 번째 JobSet의 `a`까지
제외한다. `job_key`가 JobSet 안에서만 유일하다는 도메인 계약을 경계에서 잃었다.

전역 레코드 집합은 `(jobset_id, job_key)`로 일관되게 표현해야 한다. 다른 실행의
비동기 결과와 비교할 때에는 기존 `_generation`까지 확인한다. 이 문제에 새
식별자 클래스나 별도 registry를 만들 필요는 없다.

### 3. P2 — MockLSF 상태만 비교하는 조건부 갱신이 중지·재개를 놓침

근거: [scheduler.py](../mocklsf/scheduler.py#L68),
[db.py](../mocklsf/db.py#L166), [cli.py](../mocklsf/cli.py#L559).

스케줄러가 RUN 레코드를 읽은 뒤 다른 DB 연결의 실제 `bstop`·`bresume` 경로를
실행한다. RUN → USUSP → RUN으로 돌아왔지만 사용자 중지 시간만큼 종료 예정
시각은 110에서 120으로 변경됐다. 스케줄러의 오래된 RUN → SSUSP 갱신은
`WHERE stat='RUN'`을 통과하여 종료 시각을 110으로 덮는다. 다음 tick(111)에
실제 재개 후 예정 시각(120)보다 먼저 DONE이 된다.

상태가 원래 값으로 돌아오는 동안 다른 필드가 바뀐 경우다. 최근 수정한 “거부된
전이의 이벤트 발행”이나 “거부된 종료의 슬롯 반환”은 이 경우 갱신 자체가 수락되므로
막지 못한다. MockLSF의 읽기→판정→갱신 경계를 바로잡아야 한다. 짧은 DB 쓰기
트랜잭션 안에서 최신 행을 읽고 판정하는 방식을 우선 검토하고, 파일 출력이나
대기는 그 밖에서 처리한다. 현재 상태마다 예외 조건을 덧붙이는 방식은 피한다.

## 문서와 구현·테스트가 충돌하는 계약

[requirements.md](requirements.md#L273)는 표시 지연을 끈 경우 신호 수신 시
pull 값이 신호 내용과 일치한다고 한다. 그런데
[기존 테스트](../tests/test_additional_review.py#L215)는 Store가 이미 DONE일 때
RUN → SSUSP → RUN → DONE을 전달하도록 요구하며 `dwell=0`도 포함한다.
현재 `_emit_jobs`도 Store 최신 revision과의 일치가 아니라 **이미 수락한 신호보다
오래되었는지** 검사한다. 두 요구는 동시에 성립하지 않는다.

권장 계약은 “신호는 이미 반영된 전이의 스냅샷, pull은 현재 스냅샷, 동일 실행의
전달 순서는 뒤로 가지 않음”이다. 이는 현재 구현과 중간 상태 표시 요구에 맞는다.
모든 신호와 pull의 완전한 일치를 반드시 유지하려면 쓰기·발행 모델까지 바꿔야 한다.
이번 리뷰에서 이 공개 계약을 임의로 변경하지는 않았다.

## 전체 영역별 판단

| 영역 | 판단 | 필요한 범위 |
| --- | --- | --- |
| `manager` / `handle` / 공개 API | 유지하며 내부 책임 정리 | 명령 진입점과 조회 뷰는 적절함. `forget`·`rearm`·완료 판정 호출 순서를 명시적으로 모을 필요가 있음 |
| `states` / `reports` / `errors` | 기존 모델 유지 | 작업 키, 실행 세대, 변경 revision, 명령 결과의 의미를 구별. `_generation`과 `_revision`은 서로 다른 문제를 해결하므로 하나로 합치지 않음 |
| `store/base` / `store/memory` | 기반 유지, 갱신 계약 보강 | 잠금·배치·증분 summary는 유효함. 공통 실행 일치 검증을 호출자 관례로만 남기지 않도록 기존 전이 경계에서 정리 |
| `submitter` / `lifecycle` | Gate 유지, 실행 수명 연결 정리 | 등록과 kill barrier를 한 lock으로 묶은 SubmitGate는 좋은 구조. submit context와 레코드 reset·재무장 사이의 계약을 명확히 함 |
| `killer` / `monitor` / `completion` | 최우선 구조 개선 | 범위를 잃지 않는 결과 표현, 검증과 마킹 중간 상태, 완료 판정 시점 통합 |
| `internal_status` / `query_ids` | 현재 공통 조회 경계 유지 | ID 정리는 삭제 경로 + 늦은 등록 정리의 관계를 유지. timeout takeover·failover·관심 원장까지 없애는 단순화는 요구사항을 훼손함 |
| `handlers` / `pacer` | 역할 유지, 수명 규칙 정렬 | handler 실행 중 표식과 pacer 표시 대기열은 목적이 다름. 교체·재제출 시 무효화와 늦은 결과의 취급을 공통 계약에 맞춤 |
| `command` / `config` / `options` / `qt` / `util` | 광범위한 개편 근거 없음 | 명령 격리, 파싱, 배치, 검증, Qt 호환 경계 유지. 이번 문제를 이유로 설정·파서를 함께 재작성할 필요 없음 |
| `jobset_core` | 편집 수명 작업과 함께 제한적으로 정리 | CRUD 이후 추적 정리의 입력을 실제 교체·삭제된 레코드에 맞춤. Store 외부 호출을 Store에 넣는 방식은 피함 |
| MockLSF `db` / `scheduler` / `cli` | 별도 수정 단위 필요 | 스냅샷 판정의 DB 동시성 계약 정리. 라이브러리와 별개 저장 모델이므로 같은 추상화로 통합하지 않음 |
| MockLSF `daemon` / `submit` / `formats` / `config` / `models` | 이번 근거로 전면 개편하지 않음 | 데몬 수명과 명령 형식을 유지. 위 DB 변경이 실제 stop/resume/kill 동작과 맞는지 검증 |
| 테스트·문서 | 계약 중심 검증 보강 | 기존 사례를 유지하며 범위·세대·결과 반영·신호 소비 경계의 결정적 재현을 추가 |

정리해야 하는 상태가 많다는 이유만으로 모두 중복 상태는 아니다. 예컨대 Store
상태, submit 진행률, handler의 실행 중 여부, 조회원 health, pacer 표시 상태는
각각 다른 질문에 답한다. 이들을 한 상태 머신에 억지로 넣으면 오히려 결합이 커진다.

## AGENTS.md에 맞는 실행 순서와 완료 기준

| 순서 | 변경 단위 | 완료 기준 |
| --- | --- | --- |
| 1 | 전역 대상 식별 통일 | 같은 key를 가진 여러 JobSet의 부분 성공·실패가 서로 영향을 주지 않음. 단일·배열·원시 ID 경로가 같은 범위 규칙 사용 |
| 2 | kill 결과 반영과 완료 판정 정리 | verify 전후에 poll을 끼워도 통지 결과가 같음. 자연 종료는 통지, 전원 kill은 억제, 혼합 종료·actual 정책·전역 kill도 일관됨 |
| 3 | 실행 reset·교체·삭제의 공통 수명 규칙 정리 | 같은 문제의 정리 순서를 여러 API가 복사하지 않음. 늦은 조회·handler·신호가 이전 실행을 되살리지 않음 |
| 4 | MockLSF DB 갱신 경계 수정 | stop/resume로 상태가 돌아와도 변경 시각이 유실되지 않음. 슬롯·배열 제한·이벤트는 적용된 상태와 일치 |
| 5 | 공개 신호 계약·검증 정합성 확정 | 문서와 테스트가 동일한 보장을 설명. 필수 계약은 검증 가능한 실행 순서로 표현 |

각 변경은 기존 클래스와 `transition`/`transition_many` 경로를 우선 사용한다.
새로운 flag, 범용 이벤트 버스, reducer 계층, 별도 DB backend, asyncio 전환은
이번 근거로 도입하지 않는다. 공통 부분을 추출한다면 기존 분기를 실제로 제거해야
하며, 파일을 나누는 것만으로 구조 개선이 끝났다고 판단하지 않는다.

또한 모든 경로에 `_revision` 완전 일치 조건을 기계적으로 추가하지 않는다.
메타데이터의 독립 갱신과 상태 판정에 사용한 필드 변경은 다를 수 있다. 공통 실행
일치 조건과 각 연산에 필요한 충돌 조건을 구분해야 한다. 잠금 안에서 LSF I/O나
사용자 callback을 실행하는 방식도 피한다.

성능 완료 기준에는 선택 key 수에 비례하는 읽기, 배치 갱신, 증분 요약을 포함한다.
최근 개선한 `query_ids`에 다시 Store 전역 스캔을 넣는 방식으로 정리하지 않는다.

## 검증 기록과 한계

이번 기존 관련 테스트 실행:

```bash
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
python -m pytest -q tests/test_killer.py tests/test_additional_review.py \
  tests/test_review_followup.py tests/test_store_contract.py --tb=short
```

결과: **86 passed, 3.11초**. 이전 전체 실행 기록은 `34a64e0`의 1,122개
통과이며 이번 평가에서 전체 테스트를 다시 실행한 것은 아니다.

평가 당시의 새 재현 3건은 별도 파일에 보관했다. 후속 수정에서는
[실행 수명 계약 테스트](../tests/test_execution_lifecycle.py)로 옮겨 기본 테스트에
포함하고, 전역·선택 kill, 실제 상태 정책, 동시 kill, 정리 경계로 확장했다.

```bash
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
python -m pytest -q tests/test_execution_lifecycle.py --tb=short
```

수정 전 `34a64e0`에서 **PySide6 3 failed(0.38초), PyQt5 3 failed(0.27초)**로 세 결함을
재현했다. 독립 archive의 `085c298`에서도 PySide6로 **3 failed(0.53초)**를
확인했다. 경합은 Event 또는 DB 읽기 직후의 삽입 지점으로 실행 순서를 고정했다.

테스트 모듈은 95개이며 그중 review/round 이름의 파일은 28개다. 이 개수 자체가
품질 문제는 아니다. 다만 기능 계약을 찾기 어렵게 만드는 누적 형태이며,
`test_chaos`의 요약·잔재·활동 종료 불변식도 위 완료 통지와 전역 key 충돌까지
검증하지 않는다. 수정한 영역부터 계약별로 모으고, 기존 회귀를 삭제하거나
모두 통과한다는 이유만으로 경계 검증이 완료됐다고 판단하지 않아야 한다.

이번 재현은 상태 일치 검사 부족과 완료 판정 경계 문제의 실제 사례다. 모든
비동기 경계에 추가 결함이 있다고 단정하거나, 이 세 건을 고치면 전체 코드가
무결하다고 보증하는 근거는 아니다.

## 승인 후 구현

- `JobRecord.same_execution`으로 논리 작업과 실행 세대의 비교를 공유한다.
  폴링·kill 마킹·수동 LOST 판정·결과 전달이 이 비교를 사용한다. 전역 kill의
  생존 레코드 집합은 `(jobset_id, job_key)`를 사용한다.
- 기존 Killer 활동 등록에 원시 ID 범위를 함께 보관한다. worker는 결과를
  `_completed`로 보내고, main이 활동 해제와 `finished` 전달을 수행한다.
  별도 완료 보류 flag나 registry는 추가하지 않았다. CompletionTracker는 관련
  활동이 있으면 판정을 보류하고, 결과 전달 때 대상 JobSet을 다시 판정한다.
- `_PendingArm.keys`를 제거하고 `records_reset`에 실제 리셋 레코드를 전달한다.
  handler 재무장에 리셋 실패·barrier 제외 작업이 섞이지 않는다. 이 통지는
  새 SUBMITTING 변경분보다 먼저 전달한다.
- `_forget_tracking`이 handler·미발견 횟수·관심 ID 정리의 공통 호출을 소유한다.
  삭제의 표시 정리는 `_forget_paced`가 이를 호출한다. 교체는 표시 대기열을
  비우고, 재제출은 이미 접수된 EXIT→SUBMITTING 전이를 보존한다. 이 두 동작은
  기존 표시 계약이 달라 같은 정리로 합치지 않았다.
- 미발견 횟수는 저장과 정리 모두 `_current_streaks`로 현재 실행과 대조한다.
  main의 reset 통지보다 먼저 끝난 새 실행의 조회 횟수를 지우지 않는다.
  전달 revision도 실제 key 삭제 때만 비워, 먼저 전달된 새 메타데이터를 보존한다.
- MockLSF는 기존 스냅샷 CAS를 확장했다. 읽기 전체를 잠그거나 DB 세대 컬럼을
  추가하는 대신, 현재 CLI가 변경하는 실행·대기 필드를 갱신 시 함께 비교한다.
  독립적인 job_group 변경은 그대로 허용한다. 실행 시각과 상태가 변한 스냅샷은
  거부되며, 기존 적용 결과 기반 슬롯·이벤트 처리로 이어진다.
- README와 요구사항의 신호/pull 계약 및 전원 kill 통지 조건을 실제 동작과
  일치시켰다. 기존 공개 명령 API와 같은 실행의 중간 상태 표시를 유지한다.

검증에는 새 실행 수명 계약 테스트 21건이 포함된다. 기존 회귀는 유지했고,
내부 시그니처가 바뀐 직접 호출 테스트를 맞췄다. 신호 중계 테스트는 임의의
`object()` 대신 계약에 맞는 `KillReport`를 보내도록 수정했다.

최종 전체 실행:

```bash
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
python -m pytest -q --tb=short

QT_QPA_PLATFORM=offscreen QT_API=pyqt5 PYTEST_QT_API=pyqt5 \
python -m pytest -q --tb=short
```

| Qt 환경 | 결과 | 소요 시간 |
| --- | --- | --- |
| PySide6 | 1,143 passed | 228.64초 |
| PyQt5 | 1,143 passed | 219.87초 |

`git diff --check`도 통과했다. 테스트는 FakeLsf·MockLSF와 로컬 실행 경로를
검증하며, 실제 LSF 클러스터 검증은 포함하지 않는다.

정리 경로 비용은 InMemoryStore에 활성 레코드 500개와 별도 JobSet의 완료
레코드를 넣고, 선택 key 500개의 `_set_streaks`와 `_forget_tracking`을 각각
100회 호출한 중앙값으로 비교했다. LSF I/O를 제외한 로컬 측정이다.

| Store 전체 레코드 | 미발견 횟수 저장 | 공통 추적 정리 |
| --- | --- | --- |
| 1,000개 | 0.281 ms | 0.408 ms |
| 100,000개 | 0.270 ms | 0.385 ms |

이 경로는 선택 key만 다시 읽으며 전체 Store 크기에 따른 전수 스캔 증가가
관찰되지 않았다. 전역 원시 ID kill의 완료 소속 조회에는 기존 `find_jobs`를
사용한다. 폴링 조회 경로에는 이를 추가하지 않았다.
