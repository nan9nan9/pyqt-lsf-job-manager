# 명령별 동작 흐름 (submit · resubmit · kill · cancel · polling)

사용자 명령이 내부에서 어떤 스레드를 타고, 상태가 어떻게 전이되며, 어떤
Signal이 언제 발행되는지를 도식으로 정리한다. GUI 연결 규칙은
[README §5](../README.md), Signal 카탈로그는 [lsfmgr.md](lsfmgr.md) 참고.

## 0. 스레드 지형

```
main 스레드                  워커                           통지 (→ main, queued)
──────────────              ─────────────────────────      ─────────────────────
mgr.submit()      ──────▶   submit pool (jobset당 1개,     progress / jobs_updated /
mgr.kill_jobset() ──────▶   killer pool (전역 4스레드)      finished / error ...
js.start_polling()──────▶   polling QThread (전역 1개)
```

- 모든 사용자 명령은 **즉시 반환**(비동기)하고 결과는 Signal로 온다.
- 모든 상태 변경은 **store에 먼저** 반영된 뒤 Signal이 나간다(store-first) —
  어느 slot에서든 `js.jobs()` pull이 신호 내용과 일치한다.
- worker→main Signal은 queued connection — slot은 항상 main에서 실행된다.

## 1. submit (bulk / wrapper / array)

```
main                          submit pool worker (job당 1 task)
────────────────────────      ──────────────────────────────────
submit()
 ├ 레코드 선생성(전원 SUBMITTING)
 │   → jobs_updated([전원])        # 표가 즉시 채워짐
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
kill_jobset()
 ├ cancel_submit()                # 응답성: 미착수 worker 즉시 중단 예약
 ├ resubmit plan 취소             # kill 후 재제출 발화(부활) 방지
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

## 3. resubmit (`js.resubmit_jobs`)

```
main                          killer/worker           main (queued)
──────────────────────       ─────────────────       ─────────────────────────
resubmit_jobs(keys)
 └ plan 등록 → kill-phase ▶  살아있는 대상 bkill
                              (+ verify 재조회)
                              _killed emit ─────▶    plan pop → 레코드 리셋
                                                     (job_id/이력 소거,
                                                      상태 SUBMITTING)
                                                     → submit pool 재제출
                                                       (§1과 동일 흐름)
```

- kill-phase 중 `kill_jobset`이 오면 plan이 취소되고, 리셋 직전에 kill
  barrier가 올라가 있으면 **born-cancelled** — 레코드를 건드리지 않고(원상
  유지) `submit_finished(cancelled=N)`로 끝난다.
- 재실행 job의 내부 kill은 `kill_finished`로 오지 않는다(재제출의 내부 단계).

## 4. cancel (`js.cancel`)

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
