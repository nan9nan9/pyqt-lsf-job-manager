# 아키텍처 및 결함 리뷰 — 2026-09-05

검토 기준은 커밋 `521db95`다. 운영 Python 35개 파일(11,806줄), `bin` 명령
진입점, 설정·문서와 관련 테스트를 검토했다. 아래 분석·재현 결과는 수정 전
기준이며, 후속 수정 내용은 마지막 절에 기록했다. 같은 날짜의 기존
[리뷰·수정 기록](review-2026-09-05.md) 이후 상태를 대상으로 한다.

핵심 모듈 구분은 타당하지만 kill 처리와 비동기 이벤트 전달에는 구조적
리팩터링이 필요하다. 재현한 결함은 5개이며, P1 두 건을 먼저 수정해야 한다.

| 우선순위 | 결함 | 재현 결과 | 원인과 수정 방향 |
| --- | --- | --- | --- |
| P1 | 배열 일부의 kill 실패를 전체 성공으로 처리 | element 1은 종료 수락, element 2는 실패해도 `unconfirmed=0`, `errors=[]`. 기본 optimistic 정책이 부모를 EXIT로 만들어 살아 있는 element도 폴링에서 제외 | `lsfmgr/command.py:211`: 성공한 element 하나만으로 부모를 성공 집합에 추가. 부모 판정은 element별 결과를 집계해야 함 |
| P1 | 상태로 선택한 kill이 제출 중이던 작업을 놓침 | wrapper 실행 중 `kill(js, only_state=SUBMITTING)` 호출. 제출 완료 후 PEND로 바뀐 작업이 대상에서 빠져 bkill 0회, 작업 생존 | `lsfmgr/manager.py:474`, `lsfmgr/killer.py:365`: 요청 시 선택한 key와 대기 후 상태로 다시 선택한 대상이 다름. 선택한 key를 유지하고 대기 후 ID를 해석해야 함 |
| P2 | 교체 후 오래된 조회 신호가 화면 상태를 되돌림 | 구 작업 DONE 조회 결과가 Qt 큐에 대기하는 동안 교체하면 신호가 새 작업 CREATED → 구 작업 DONE 순서로 전달. Store에는 새 작업이 남음 | `lsfmgr/manager.py:1281`: 전달 시 JobSet 존재만 확인. 현재 실행·교체와 일치하는 결과인지 전달 경계에서 검증해야 함. 표시 지연 옵션을 꺼도 발생 |
| P2 | verify 성공 시 사용자 취소 표식 유실 | `kill(js, verify=True)` 후 EXIT이지만 `killed=False`. optimistic·actual 정책 모두 재현 | `lsfmgr/killer.py:414`, `lsfmgr/killer.py:434`: verify가 먼저 EXIT로 바꾸면 뒤따르는 마킹이 상태 가드에 막힘. 동일 작업인지 확인하면서 상태를 보존하는 부분 갱신 필요 |
| P2 | MockLSF가 종료되지 않았는데 종료 성공 반환 | SIGTERM 후에도 살아 있는 테스트 프로세스에 대해 약 3초 후 True 반환 및 PID 파일 삭제 | `mocklsf/daemon.py:148`: 대기 시간 초과와 실제 종료를 구분하지 않음. 살아 있는 프로세스의 PID 파일을 보존해야 함. 실제 스케줄러의 긴 DB 대기 등에서도 종료 지연 가능 |

마지막 항목은 테스트가 생성한 별도 프로세스로 재현했고, 테스트 종료 시 해당
프로세스를 회수했다. PID 파일이 사라진 상태에서 재시작하면 중복 기동할 수
있다는 것은 코드 흐름에서 도출한 영향이며, 실제 스케줄러 중복 기동을 실행한
것은 아니다.

## 아키텍처 판단

| 영역 | 판단과 권장 방향 |
| --- | --- |
| 제출·조회·kill·Store 경계 | 유지. Qt import 중앙화, 원자적 Store 갱신, 배치 처리는 적절함 |
| kill 대상과 결과 판정 | 우선 통합. 요청 시 선택한 대상부터 실행·검증·마킹까지 동일한 대상 정의 사용 |
| 상태 갱신과 신호 전달 | 우선 개선. Store 원자성만으로 이벤트 순서는 보장되지 않으므로 오래된 결과의 판별 규칙을 전달 경계 한 곳에서 관리 |
| 실행 주기 관리 | Manager·Submitter·CompletionTracker에 분산된 무장·해제·완료 책임 정리. 기존 실행 token 활용을 먼저 검토 |
| 파일 분할·추상화 추가 | 파일 길이만으로 대규모 분할할 근거는 부족함. 재현한 결함을 해결하면서 중복 판단을 줄이는 범위가 적절함 |

## 검증 및 재현 방법

기준 커밋에서 기존 전체 테스트 **1,024개 통과, 242.47초**:

```bash
QT_QPA_PLATFORM=offscreen python -m pytest -q --tb=short -o faulthandler_timeout=60
```

[회귀 테스트](../tests/test_architecture_review.py)는 기대하는 정상 동작을
assertion으로 표현한다. 최초 재현 코드의 **6개 사례 모두 기준 커밋에서 실패**했고,
verify 두 정책이 같은 결함을 각각 검증하므로 결함 수는 5개다. 수정하면서
`docs/repro_architecture_review.py`를 기본 테스트 경로인 `tests/`로 옮기고
관련 경합·옵션 조합을 추가했다. 개별 실행 방법:

```bash
QT_QPA_PLATFORM=offscreen python -m pytest -q tests/test_architecture_review.py --tb=short
```

| 재현 사례 | 기준 커밋의 결과 |
| --- | --- |
| `test_kill_state_snapshot_catches_submitting` | bkill 호출 없이 PEND 작업 생존 |
| `test_verify_preserves_kill_origin[optimistic]` | `killed=False` |
| `test_verify_preserves_kill_origin[actual]` | `killed=False` |
| `test_partial_array_kill_is_not_success` | 일부 실패에도 `unconfirmed=0` |
| `test_stale_poll_cannot_override_replacement_signal` | 마지막 신호의 command가 교체 전 command |
| `test_mock_daemon_stop_does_not_claim_success_while_alive` | 반환 True, 프로세스 생존, PID 파일 삭제 |

실행 환경은 Python 3.12.1·PyQt5 5.15.10·qtpy 2.4.3이며
`QT_QPA_PLATFORM=offscreen`을 사용했다. 실제 LSF 클러스터 및 다른 Qt 바인딩의
런타임 동작은 이번 검증에 포함하지 않았다.

## 후속 수정

- 배열 응답의 성공·미해소 결과를 모은 뒤 부모 결과를 판정한다. 자식 하나라도
  미해소면 부모가 재시도 대상으로 남으며 stdout/stderr 순서에 영향을 받지 않는다.
  재시도 진행률도 자식 응답 수가 아닌 요청 대상 수로 계산해 100%를 넘지 않는다.
- `only_state`는 Manager에서 요청 시 key를 선택하는 데만 사용한다. 이후에는
  기존 선택 key kill 경로로 제출 종료를 기다리고 ID를 해석한다. Killer의
  중복 상태 필터 분기를 제거했다.
- 교체·재제출마다 바뀌고 일반 상태 전이에서는 유지되는 내부 실행 식별자
  `JobRecord._generation`을 추가했다. `job_key`는 재사용되고 `job_id`는 제출
  전에는 없으므로 두 값만으로 늦은 결과를 구분할 수 없었다. CompletionTracker의
  기존 token은 JobSet 단위 후처리 무장용이므로 역할을 유지한다.
  `jobs_updated`와 조회의 `job_lost`는 같은 필터로 삭제·이전 실행의 결과를
  버리고, 요약은 전달 시점의 Store에서 읽는다. 이미 pacer가 접수한 정상
  재제출 전이 순서는 유지하며, 교체 시에는 해당 key의 표시 대기열을 비운다.
- kill 마킹을 한 경로로 모았다. 동일 실행·LSF ID인지 확인하고, verify/폴링이
  먼저 기록한 종료 상태와 exit code를 보존하면서 `killed=True`를 부분 갱신한다.
  verify와 마킹 결과는 job별 최종 레코드 한 건으로 합쳐 중복 발행을 막는다.
- (후속 검토) bkill 응답을 '해소'(재시도 불필요)와 '수락'(kill 신호 접수)으로
  나눴다. `already finished` 계열은 해소일 뿐 수락이 아니므로 EXIT/`killed`로
  마킹하지 않고, 그런 응답이 있으면 verify 설정과 무관하게 한 번 조회해 실제
  종료 상태(DONE/EXIT)를 Store에 반영한다. 기존에는 kill 시점에 Store가 아직
  PEND/RUN이던 정상 완료 job이 EXIT/`KILLED`로 덮이고 terminal이라 폴링에서도
  빠져 영구히 실패로 남았다. timeout 후 조회 확인분은 사라졌거나 EXIT면 이
  kill이 끝낸 것으로(마킹), DONE이면 자연 종료로 본다. 접힌 배열 부모는
  element 하나라도 수락되면 수락이다. 재검토 보완: timeout 조회의 자연 종료
  판정은 선택한 element 행만 보고, element 수락 이력은 형제 실패로 부모가
  미해소여도 재시도까지 유지하며(마킹은 최종 해소된 것만), JobSet 없는 ID
  kill도 추적 레코드의 jobset을 조회해 Store를 갱신한다. 회귀: `test_killer.py`.
- (후속 검토) 조회의 대상 스냅샷과 조회원 관심 등록 사이에 삭제·교체·재제출로
  떨어진 job_id는 그쪽의 forget보다 등록이 늦어 콜백 조회원에 유령으로 남을
  수 있었다(카오스 전체 실행에서 1회 관측, 단독 실행 10회 재현 실패). 조회가
  등록 뒤 현재 레코드와 대조해 되돌아온 id를 다시 버린다. 결정적 재현:
  `test_stale_ids.py`.
- MockLSF 종료 대기 후에도 살아 있으면 False를 반환하고 PID 파일을 보존한다.
  CLI의 stop/restart/reset은 종료 실패 시 오류 코드 1을 반환하며, 재기동이나
  DB 초기화를 진행하지 않는다.

기존 모듈 경계를 유지하고 위 결함에 필요한 대상 선택·마킹·신호 전달만 정리했다.

검증 결과:

- 기본 회귀 테스트 37개 추가: 기존 1,024개를 포함한 전체 **1,061개 통과**
  (238.19초).
- 마지막 배열 재시도 진행률 보완 후 관련 **163개 통과** (15.47초).
- `git diff --check` 통과.
