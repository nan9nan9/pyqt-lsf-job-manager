# 아키텍처 및 결함 리뷰 — 2026-09-05

검토 기준은 커밋 `521db95`다. 운영 Python 35개 파일(11,806줄), `bin` 명령
진입점, 설정·문서와 관련 테스트를 검토했다. 이 기록은 분석 결과이며, 아래
결함의 수정은 포함하지 않는다. 같은 날짜의 기존
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

[별도 재현 코드](repro_architecture_review.py)는 기대하는 정상 동작을 assertion으로
표현한다. 기준 커밋에서 **6개 사례 모두 실패**하며, verify 두 정책이 같은
결함을 각각 검증하므로 결함 수는 5개다. 기본 테스트 경로인 `tests/`와 분리한
리뷰 자료이며, 실행하려면 저장소 루트에서 명시적으로 파일을 지정한다:

```bash
QT_QPA_PLATFORM=offscreen python -m pytest -q docs/repro_architecture_review.py --tb=short
```

| 재현 사례 | 기준 커밋의 결과 |
| --- | --- |
| `test_kill_state_snapshot_catches_submitting` | bkill 호출 없이 PEND 작업 생존 |
| `test_verify_preserves_kill_origin[optimistic]` | `killed=False` |
| `test_verify_preserves_kill_origin[actual]` | `killed=False` |
| `test_partial_array_kill_is_not_success` | 일부 실패에도 `unconfirmed=0` |
| `test_stale_poll_cannot_override_replacement_signal` | 마지막 신호의 command가 교체 전 command |
| `test_mock_daemon_stop_does_not_claim_success_while_alive` | 반환 True, 프로세스 생존, PID 파일 삭제 |

기존 테스트에는 kill 옵션 조합과 실제 Qt 큐를 거치는 교체 경합 검증을 추가할
필요가 있다. 결함 수정 시 재현 사례를 해당 기능의 회귀 테스트로 옮길 수 있다.

실행 환경은 Python 3.12.1·PyQt5 5.15.10·qtpy 2.4.3이며
`QT_QPA_PLATFORM=offscreen`을 사용했다. 실제 LSF 클러스터 및 다른 Qt 바인딩의
런타임 동작은 이번 검증에 포함하지 않았다.
