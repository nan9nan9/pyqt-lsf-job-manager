# 전체 코드 추가 리뷰 — 2026-09-05

기준 커밋은 `01e370c`다. 검토 시작 시 작업 트리는 깨끗했다.
`lsfmgr`, `mocklsf`, `examples`, `tools`의 Python 35개 파일(11,949줄),
`bin`의 진입점 13개, 패키지 설정과 주요 계약·회귀 테스트를 검토했다.
최초 리뷰에서는 운영 코드를 수정하지 않고 재현 테스트를 작성했다.
후속 수정에서 재현을 [정식 회귀 테스트](../tests/test_additional_review.py)로 옮겼다.
아래 결함 설명과 줄 번호는 검토 당시 기준이며, 후속 변경은 마지막 절에 기록한다.

추가 결함 **5건을 재현**했다. 모두 P2로 분류한다. 기존 모듈 경계는 유지할
근거가 충분하지만, 동일 실행의 상태 전달 순서와 완료 재평가, 상태 갱신 이후
부수작용에는 국소적인 리팩터링이 필요하다.

## 확인된 결함

| 우선순위 | 결함 | 확인한 결과 | 핵심 위치 |
|---|---|---|---|
| P2 | 같은 실행의 오래된 제출 신호가 DONE 화면을 PEND로 되돌림 | Store는 DONE이지만 마지막 `jobs_updated`는 PEND. 추가 조회로도 복구되지 않음 | [manager.py:1296](../lsfmgr/manager.py#L1296) |
| P2 | 이전 실행의 최종 handler가 느리면 새 실행의 최종 handler 누락 | job 1000의 final만 실행되고 재제출한 1001의 final은 실행되지 않음 | [handlers.py:332](../lsfmgr/handlers.py#L332) |
| P2 | 재제출의 forget과 ID 초기화 사이에서 이전 관심 ID가 되살아남 | Store에는 새 ID만 남는데 조회원의 관심 집합에는 이전 ID 1000이 잔존 | [submitter.py:367](../lsfmgr/submitter.py#L367) |
| P2 | `already finished` 응답 이후 자연 종료 완료 통지 누락 | DONE·`killed=False`는 정확하나 이후 조회에도 `jobset_finished`가 0회 | [completion.py:185](../lsfmgr/completion.py#L185) |
| P2 | MockLSF가 거부된 상태 전이의 로그·출력을 먼저 기록 | DB는 EXIT/130인데 이벤트는 `kill → done`, 출력은 `Successfully completed` | [scheduler.py:147](../mocklsf/scheduler.py#L147) |

### 1. 동일 실행 안의 늦은 제출 신호

제출 worker가 Store를 PEND로 갱신한 뒤 결과를 배치에 넣기 전에 선점되면,
폴링이 먼저 DONE을 저장하고 발행할 수 있다. 이후 worker가 재개되어 이전
PEND 레코드를 발행한다. 관측 순서는 `SUBMITTING → DONE → PEND`였다.

`_current_records`는 `_generation`만 비교하므로 동일 실행 안의 역순 도착은
그대로 통과한다. 이미 Store는 terminal이므로 뒤의 조회는 변경분을 만들지
않는다. 따라서 표시 지연이 일시적으로 과거 상태를 보여 주는 것과 달리,
마지막 화면 상태가 잘못된 채 남는다. 표시 지연 0초와 0.03초에서 모두 재현했다.
예제의 `Dashboard._apply_jobs`도 전달된 레코드를 그대로 행에 적용한다.

수정 방향은 기존 실행 식별자를 유지하면서 **같은 실행에서 이미 전달한
갱신보다 오래된 결과를 거르는 것**이다. Store의 갱신 순서와 전달 순서를
연결하는 규칙이 필요하다. 상태 이름의 순위를 매기면 RUN·SUSP 전환 등을
표현할 수 없고, 현재 Store 상태와 다른 레코드를 전부 버리면 pacer가 보존해야
하는 정상 중간 전이까지 없어진다. 단순 상태 비교로 해결해서는 안 된다.

재현: `test_delayed_submit_signal_does_not_regress_done`의 두 설정.

### 2. 이전 final handler와 재제출의 경합

첫 실행이 DONE으로 끝나고 최종 handler가 아직 실행 중인 상태에서 재제출한다.
handler 등록과 같은 key는 유지되며 `rearm`이 진행 표식을 초기화한다. 새 job도
DONE으로 끝나 마지막 조회가 실행되지만, 이전 handler의 inflight 표식 때문에
평가를 건너뛴다.

이전 handler가 끝나면 inflight는 해제된다. 그러나 `_run`은 `final=False`였던
실행만 `_recheck`를 발행하므로 새 실행의 final은 보충되지 않는다. 자동 폴링이
종료됐거나 추가 `query_once`가 없으면 이 상태로 결과 수집이 끝난다.

기존 inflight 직렬화는 유지하고, **실행 종료 후 현재 레코드를 재평가하는
책임을 이전 호출의 final 여부와 분리**하는 것이 작은 수정이다. 현재 실행의
`_FINISHED` 표식은 이미 있으므로 같은 final의 중복 실행을 막는 데 재사용할 수 있다.

재현: `test_previous_final_handler_does_not_lose_new_run_final`.

### 3. 이전 ID 정리의 순서

`_reset_records`는 이전 ID 목록을 읽고 `forget_status`를 실행한 다음,
레코드의 `job_id=None` 전이를 수행한다. 두 단계 사이에 전역
`kill_jobs([old_id], verify=True)`가 조회를 끝내면 문제가 생긴다.

그 조회는 관심 ID를 다시 등록한다. `query_ids`의 finally가 Store를 확인할
때는 아직 이전 ID가 있으므로 올바르게 보존한다. 이후 재제출이 ID를 지우지만
forget은 이미 끝났으므로 이전 ID가 남는다. optimistic kill 이후 조회원이
아직 RUN을 반환하는 조건에서 재현했으며, RUN 항목은 terminal 보존 기한으로도
만료되지 않는다.

이는 공통 정리나 범위별 읽기의 문제가 아니다. **Store에서 이전 ID를 제거한
후 그 ID를 forget하는 순서**로 재제출을 맞춰야 한다. 실제로 리셋하지 못한
레코드의 ID까지 지우지 않도록 성공한 리셋을 기준으로 정리해야 한다.
폴링의 전역 검색을 되살리거나 `query_ids`에 재제출 전용 플래그를 추가할 필요는 없다.

재현: `test_resubmit_forget_cannot_be_undone_before_id_reset`.

### 4. 자연 종료인데 완료 통지를 억제

실제 job이 DONE이고 Store는 아직 PEND인 상태에서 kill을 호출한다.
`already finished` 응답을 받은 kill 경로는 현재 코드에서 DONE·exit code 0·
`killed=False`를 정확히 반영한다. 이전의 거짓 EXIT 결함은 해결된 상태다.

그러나 `Manager._emit_updates_after_kill`은 항상 `mute_after_kill`을 호출한다.
이 함수는 전원이 terminal인지 여부만 보고 latch를 세운다. 이후 정상 조회가
반복되어도 `jobset_finished`는 발화하지 않는다. 완료 통지에 연결한 사용자
동작도 실행되지 않는다. 별도 계약인 `post_process`까지 유실됐다는 주장은 아니다.

`maybe_finish`는 `killed`를 보고 억제하고 `mute_after_kill`은 kill 호출 사실만
보고 억제해 판정이 갈라져 있다. **kill 수락·취소 근거가 있는 완료만 억제하는
규칙을 CompletionTracker 안에서 일치**시켜야 한다. 기존 `killed`와 변경 결과를
먼저 활용하고, 단순한 kill 요청 자체를 완료 원인으로 삼지 않아야 한다.

재현: `test_already_finished_kill_preserves_natural_completion`.

### 5. MockLSF의 조건부 저장과 로그·출력 불일치

스케줄러가 RUN 스냅샷을 얻은 뒤 다른 DB 연결에서 bkill과 같은 조건부 갱신으로
EXIT/130을 저장한다. 스케줄러는 낡은 스냅샷의 예정 종료 시각을 보고 `_finish`를
호출한다. 여기서 done 이벤트와 정상 완료 출력이 먼저 기록된다.

마지막 `update_guarded_many`는 Store가 이미 EXIT이므로 DONE 저장을 거부한다.
상태 덮어쓰기 방지는 동작하지만, 앞서 생성한 이벤트와 파일 내용은 취소되지 않는다.
이 때문에 bjobs와 bhist·bpeek가 서로 다른 종료 결과를 보여 줄 수 있다.
상태만 검증하는 테스트가 이런 불일치를 놓칠 수 있다는 점도 문제다.

**조건부 상태 갱신이 적용된 작업만 이벤트·출력을 기록**하도록 순서를 바꿔야 한다.
스케줄러의 `_dispatch`와 suspend/resume 이벤트도 같은 저장 경계를 사용하므로
함께 검토할 대상이다. 이번 재현은 종료 경로에 한정했다.

재현: `test_mock_scheduler_does_not_publish_rejected_transition`의 DONE 종료 설정.

## 아키텍처 판단

| 영역 | 판단 |
|---|---|
| Manager / Submitter / Killer / Polling / Store 경계 | 유지. 실행 주체와 Store 책임이 구분되고, Store 원자적 갱신·배치 API·증분 요약이 마련돼 있음 |
| 모듈 의존성·Qt 호환 | 현재 계층 유지. 의존 순환과 leaf 모듈 계약 검사가 통과함. 이번 비동기 결함은 PyQt5·PySide6 양쪽에서 발생하므로 바인딩별 우회로 해결할 문제가 아님 |
| Store 갱신 → 결과 발행 | 국소 리팩터링 필요. 실행 식별뿐 아니라 동일 실행 내 전달 순서가 연결돼야 함 |
| handler·완료 통지 | 새 서비스보다 기존 재평가·완료 원인 판정을 정리. 기존 상태가 표현할 수 있는 정보를 중복 플래그로 늘리지 않음 |
| ID 수명·외부 부수작용 | 상태 갱신 성공 이후 정리·발행하는 순서로 맞춤. query_ids 공통 경계와 key 기반 읽기는 유지 |
| 파일 분할·새 추상화 | Manager 1,404줄, Submitter 1,100줄이라는 길이만으로 분할할 근거는 부족함. 이번 결함에 필요한 경계를 고치는 것이 우선 |

새 전역 이벤트 버스, Store 전면 교체, 범용 job_id 역인덱스는 이번 결함을
해결하는 데 필요하지 않다. 수정 우선순위는 화면 최종 상태·결과 수집 누락,
ID 수명·자연 종료 통지, MockLSF 검증 정확성 순서가 적절하다.

문서 정합성도 두 곳 확인했다. `docs/gui.md:131` 부근의 전역 kill은 verify를
건너뛴다는 설명은 현재 `_verify_global` 구현과 다르다.
`tools/lsf_selfcheck.py:211`의 payload 파싱 실패가 LOST로 확정된다는 안내도
현재 조회원이 실패를 판단 보류로 처리하는 동작과 다르다.

## 수정 전 검증

- 기존 전체 테스트: **1,088개 통과, 234.90초**.
- 실행 환경: Python 3.12.1, pytest 9.0.2, pytest-qt 4.5.0,
  qtpy 2.4.3, offscreen. 최초 전체 실행은 `QT_API`를 지정하지 않았으며
  pytest 헤더에는 PySide6/Qt 6.10.2가 표시됐다. 후속 검증에서는 `QT_API`와
  `PYTEST_QT_API`를 함께 지정해 라이브러리와 테스트의 바인딩을 일치시켰다.
- 추가 결함은 정상 동작을 assertion으로 표현해 별도 재현 파일에서 검증했다.
  **재현 6개 모두 실패, 0.92초**로 위 결함 5건을 확인했다.
  이후 후속 수정과 함께 기본 테스트 경로 `tests/`로 옮겼다.
- 신호·콜백 관련 재현 5개는 PyQt5 5.15.10 / Qt 5.15.2에서도 같은 결과를
  확인했다. 표시 지연 두 설정이 같은 결함을 검증하므로 전체 결함은 5건이다.
- 실제 LSF 클러스터는 사용하지 않았다. 외부 명령은 FakeLsf와 응답 대역으로,
  MockLSF 저장 경합은 임시 SQLite DB의 두 연결로 재현했다.

```bash
# 기존 전체 테스트
QT_QPA_PLATFORM=offscreen python -m pytest -q

# 후속 수정 후 정식 회귀 테스트 (기존 재현과 인접 계약 검증)
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
  python -m pytest -q tests/test_additional_review.py --tb=short
```

재현의 대기 지점은 Store 갱신 후 선점, handler 실행 중 재제출,
forget 이후 선점, DB 스냅샷 이후 경쟁 갱신을 결정적으로 배치하기 위한 것이다.
실제 운영에서 발생하는 빈도를 측정한 결과는 아니다.

## 후속 수정

- Store가 레코드 갱신마다 `_revision`을 증가시키고 Manager가 key별 마지막
  전달 순서보다 오래된 결과만 버린다. `_generation`은 실행 교체 판정에 그대로
  사용한다. Store가 먼저 DONE이 되어도 순서대로 온 RUN·SSUSP·RUN·DONE은
  pacer에 모두 전달한다. 삭제 시 전달 순서 기록도 정리하며 전역 검색은 추가하지 않았다.
  `local_edit_jobs`도 Store가 반환한 실제 저장 레코드를 사용해 갱신 순서와
  `updated_at`이 신호·반환값에서 빠지지 않게 했다.
- handler 실행 후 재평가를 final 여부와 무관하게 요청한다. 기존 `_FINISHED`
  판정으로 중복 실행을 막고, 이전 final에 막혔던 새 실행의 final을 보충한다.
- 재제출은 성공한 ID 초기화 이후에만 옛 ID를 forget한다. 리셋에 실패한
  레코드의 관심 ID는 보존한다. `query_ids`의 공통 정리와 범위별 읽기는 유지했다.
- `mute_after_kill`을 제거해 완료 통지 억제를 `maybe_finish`의 전원 `killed`
  규칙으로 통일했다. 자연 종료와 부분 kill이 섞인 경우 순서에 관계없이 완료를
  통지하며, 전원 kill인 경우 통지 억제와 후처리 실행 계약은 유지한다.
- MockLSF의 조건부 배치 갱신이 실제 적용한 전이만 반환한다. 스케줄러는 그
  결과에 대해서만 dispatch·종료·suspend·resume 이벤트와 출력을 기록한다.
  DB 갱신은 한 트랜잭션을 유지한다.
- README의 완료 통지 기준, GUI 문서의 전역 kill verify 설명, selfcheck의
  payload 파싱 실패 안내를 현재 동작에 맞췄다.
- PySide6 전체 실행에서 기존 공개 API 문서 검사가 Qt 자동 생성 멤버
  `staticMetaObject`까지 자체 API로 세는 문제가 드러났다. QObject 공통 멤버는
  검사에서 제외하고, selfcheck의 출력 검사도 바뀐 진단 의미에 맞췄다.

회귀 테스트는 기존 결정적 재현에 더해 정상 중간 전이 표시, 메타데이터 신호의
역순 도착, 삭제 후 key 재사용, 부분 kill과 자연 종료의 두 순서, 실패한 리셋의
ID 보존, MockLSF의 다섯 전이 경로와 Store 갱신 순서 계약을 확인한다.

직접 구현한 Store를 주입하는 경우에는 [Store 계약](../lsfmgr/store/base.py)에
따라 갱신 시 현재 `_revision`을 증가시키고 저장된 레코드를 반환해야 한다.
기본 InMemoryStore에는 이를 적용했다.

전달 경계 비용도 확인했다. `_emit_jobs`에 500건을 전달하는 101회 실행의
중앙값은 Store 1,000건에서 **0.597ms**, 100,000건에서 **0.606ms**였다.
PySide6, dwell=0, 사용자 구독자 없는 조건이며 GUI 렌더링 비용은 포함하지 않는다.
이 변경의 정리·순서 판정이 전역 Store 검색을 추가하지 않았다는 범위의 확인이다.

## 수정 후 최종 검증

- 추가 회귀 테스트: **21개** (추가 리뷰 20개 + Store 계약 1개).
- PySide6 전체: **1,109개 통과, 241.12초**. `QT_API=pyside6`,
  `PYTEST_QT_API=pyside6`, `QT_QPA_PLATFORM=offscreen`을 명시했다.
- PyQt5 관련 회귀: **115개 통과, 12.92초**. 추가 리뷰·이전 아키텍처 리뷰·
  handler·완료 통지·관심 ID·제출 신호 순서를 검사했으며 두 바인딩 환경변수를
  모두 `pyqt5`로 지정했다.
- `compileall`과 `git diff --check` 통과.
- 실제 LSF 클러스터는 사용하지 않았다. FakeLsf, MockLSF, 임시 SQLite DB와
  Qt 이벤트 루프에서 검증했다.

```bash
QT_QPA_PLATFORM=offscreen QT_API=pyside6 PYTEST_QT_API=pyside6 \
  python -m pytest -q --tb=short

QT_QPA_PLATFORM=offscreen QT_API=pyqt5 PYTEST_QT_API=pyqt5 \
  python -m pytest -q tests/test_additional_review.py \
  tests/test_architecture_review.py tests/test_handlers.py \
  tests/test_jobset_finished.py tests/test_stale_ids.py \
  tests/test_submit_signal_ordering.py --tb=short
```
