# 명령별 동작 흐름 (submit · merge/재실행 · kill · cancel · polling)

사용자 명령이 내부에서 어떤 스레드를 타고, 상태가 어떻게 전이되며, 어떤
Signal이 언제 발행되는지를 도식으로 정리한다. GUI 연결 규칙은
[README §5](../README.md), Signal 카탈로그는 [lsfmgr.md](lsfmgr.md) 참고.

## 0. 스레드 지형

```
main 스레드                  워커                           통지 (→ main, queued)
──────────────              ─────────────────────────      ─────────────────────
mgr.submit()      ──────▶   submit pool (jobset당 1개,     progress / jobs_updated /
mgr.kill(js)      ──────▶   killer pool (전역 4스레드)      finished / error ...
mgr.start_polling(js)───▶   polling QThread (전역 1개)
```

- 모든 사용자 명령은 **즉시 반환**(비동기)하고 결과는 Signal로 온다.
- 모든 상태 변경은 **store에 먼저** 반영된 뒤 Signal이 나간다(store-first) —
  어느 slot에서든 `js.jobs()` pull이 신호 내용과 일치한다.
- worker→main Signal은 queued connection — slot은 항상 main에서 실행된다.

## 1. submit — `mgr.submit(js)` (유일한 제출 경로, v9)

`create_jobset([...])`(CREATED, 표 즉시 채움) 후 jobset 기준으로
전 job을 (재)제출한다. 가드: 전원 비활성(CREATED/terminal)이어야 한다.

```
main                          submit pool worker (job당 1 task)
────────────────────────      ──────────────────────────────────
mgr.submit(js)
 ├ 가드(전원 비활성) + 레코드 리셋(전원 SUBMITTING)
 │   → jobs_updated([전원])        # 표가 즉시 갱신
 ├ SubmitGate.register(ctx)        # kill barrier 중이면 born-cancelled
 └ pool.start(task × N) → 반환
                              task._run:
                                cancel_event?  ──set──▶ CREATED 복귀(잔재 리셋)
                                │                        → jobs_updated (배치)
                                not set
                                ├ rate limit 대기 (token bucket)
                                ├ bsub 실행 (submit_timeout_s 상한)
                                ├ 성공 → PEND + job_id     ┐ 스로틀 배치
                                ├ 실패(재시도 가능)         │ (0.5s 또는 1%)
                                │   → RETRY_WAIT → QTimer → 재시도(최대 max_retry)
                                └ 실패(최종) → SUBMIT_FAILED┘
                              마지막 task 완료 시:
                                jobs_updated(잔여 배치) → submit_finished(report)
                                → jobset_updated(최종 요약)     # 순서 보장
```

Signal 순서(보장): `(ready_started → ready_finished)`* → `submit_started` →
`jobs_updated[SUBMITTING 전원]` → `submit_progress`+`jobs_updated`(스로틀 배치)
→ `submit_finished` → `jobset_updated`.  (*pre_submit 게이트 지정 시)

상태: `CREATED → SUBMITTING → PEND | RETRY_WAIT(→SUBMITTING 재시도) |
SUBMIT_FAILED(최종)`. cancel/kill 시 `SUBMITTING/RETRY_WAIT → CREATED`
(실패 잔재 fail_reason/retry_count 함께 리셋).

## 2. kill (전체) — kill 우선권 (FR-3)

kill은 진행 중 submit에 **우선권**을 갖는다. 핵심은 SubmitGate barrier —
barrier 확인과 submit 등록이 한 lock 아래 원자적이라, "kill의 취소를
빠져나가는 늦은 제출"이 구조적으로 불가능하다 (`lifecycle.py`).

```
main                              killer pool worker
──────────────────────────       ─────────────────────────────────────
mgr.kill(js)
 ├ cancel_submit()                # 응답성: 미착수 worker 즉시 중단 예약
 ├ abort_retries()                # RETRY_WAIT QTimer 부활 방지
 ├ kill_started 발행(동기) ◀━━ UI 스피너는 여기서 켠다
 └ killer.kill_jobset(scope) → 반환
                                  _KillTask:
                                    scope.acquire()          # barrier ↑
                                    │  ├ 그 시점 submit 활동 전부 취소
                                    │  ├ pool 슬롯 반납(releaseThread)
                                    │  └ 정지 대기 (bsub 완료까지, 상한 있음)
                                    │     · 미제출 → CREATED 복귀(kill 대상 아님)
                                    │     · 그새 제출됨 → PEND+job_id (스냅샷에 포함)
                                    │     · barrier 중 새 submit → 등록 거부(born-cancelled)
                                    ├ 대상 스냅샷 (is_on_lsf)
                                    ├ kill 전략: ①bkill -g ②array ③-J ④id chunk
                                    │     → kill_progress (스로틀)
                                    ├ optimistic(기본): 확인분 즉시 EXIT(KILLED)
                                    ├ kill_finished(KillReport)
                                    │     → jobs_updated([EXIT 전원 배치])
                                    │     → jobset_updated(요약)
                                    └ scope.release()        # barrier ↓ (finally)
```

- `kill_status_policy="actual"`이면 EXIT 전이는 다음 폴링에서(최대
  poll_interval_s 지연) — GUI는 기본(optimistic) 유지 권장.
- 정지 대기 초과는 `KillReport.errors`에 남고 optimistic 표시도 억제된다.
- 부분 kill(`only_state=`)은 우선권 flow 없이 해당 상태만 겨냥한다.

## 3. 재실행 — merge_from + submit (v9, resubmit 제거)

재실행은 별도 파이프라인이 아니라 **데이터 조작 + 일반 submit** 이다:

```
main (전부 앱이 직접 제어)
──────────────────────────────────────────
① (살아있으면) mgr.kill(js) → kill_finished 대기
② fix = mgr.create_jobset([...], merge_ids=[기존과 동일])   # 수정본 바구니
③ mgr.merge(js, fix)     # merge_id 일치분 CREATED 교체(물리 키 유지),
                         # 신규/None 은 추가. fix 소멸. 가드: 전원 비활성
④ mgr.submit(js)         # 전 job 리셋 후 재제출 — §1과 동일 흐름
```

- ③의 replace 는 레코드만 교체한다 — force=True 로 활성 job 을 교체해도
  LSF 의 실제 job 은 그대로다(정리는 앱 책임, 먼저 kill 권장).
- ④의 리셋: job_id/exit_code/실행시간/fail_message/클러스터 소거,
  spec_json(제출 옵션)·merge_id·ud_data 보존. handler 자동 재무장.

## 4. cancel (`mgr.cancel_submit(js)`)

```
cancel_submit(): ctx.cancel_event set → 반환(즉시)
  · 미착수 worker  → 안전 지점에서 SUBMITTING/RETRY_WAIT → CREATED 복귀
  · bsub 진행 중   → 완료까지 진행(PEND 확정) — 강제 중단하지 않는다
  · 대기 중 재시도 → 발화 시 포기 확정
  → 각 취소분은 jobs_updated 배치로, 마지막에 submit_finished(cancelled=k)
```

## 5. polling (자동 상태 갱신, FR-4)

```
polling QThread (jobset당 QTimer, interval마다)
──────────────────────────────────────────────
① probe: bjobs -g(group) / -J(name) / array id       # 부착물 기반
② leftover: 못 찾은 job_id들 → bjobs <id...> chunk    # 종료 상태도 여기서 잡힘
③ 여전히 missing → bhist chunk (이력 fallback)
④ 판정:
     bjobs/bhist에서 발견     → 상태 반영 (guard CAS — 그새 바뀐 레코드 보호)
     미발견 + 조회 전부 성공  → LOST 확정 (NOT_FOUND_IN_LSF)
     미발견 + 조회 실패 섞임  → 판단 보류 (다음 사이클 재시도) ◀ 장애≠부재
⑤ 통지: jobset_updated(요약) + jobs_updated(변경분만) + job_lost
⑥ 전원 terminal 또는 활동 없음 2사이클 → polling 자동 중지 (AUTO-2)
```

- ②③은 **chunk 단위 실패 격리**: 실패 chunk의 job만 보류, 성공 chunk는
  정상 판정. 연속 2회 실패면 회로 차단(남은 chunk 즉시 실패 처리) —
  전면 장애에서 폴링 스레드가 chunk 수 × timeout 블록되지 않는다.
- 보류 경고는 사이클당 1줄로 집계된다.

## 6. 상태 전이도

```
                    ┌──────────── cancel/kill(미제출) ────────────┐
                    ▼                                             │
 CREATED ──▶ SUBMITTING ──▶ PEND ──▶ RUN ──▶ DONE                │
                │   ▲         │        │       (terminal)         │
                │   │재시도    │        ├──▶ EXIT (terminal)       │
                ▼   │         │        │     ▲ kill(optimistic)   │
             RETRY_WAIT ──────┼────────┼─────┘                    │
                │             │        └──▶ PSUSP/USUSP/SSUSP ⇄ RUN
                ▼             ▼
         SUBMIT_FAILED     LOST (조회 전부 성공했는데 미발견)
          (terminal)        (terminal)
```

`is_on_lsf` = PEND/RUN/SUSP*/UNKWN/ZOMBI — 폴링·kill 스냅샷 대상.
`is_terminal` = DONE/EXIT/SUBMIT_FAILED/LOST — 더 이상 전이하지 않음.
