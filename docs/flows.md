# 명령별 동작 흐름 (submit · 재실행 · kill · cancel · polling)

사용자 명령이 내부에서 어떤 스레드를 타고, 상태가 어떻게 전이되며, 어떤 Signal이
언제 발행되는지를 도식으로 정리한다. GUI 연결 규칙은 [README §7](../README.md),
Signal 카탈로그는 [gui.md](gui.md) 참고.

## 0. 스레드 지형

```
main 스레드                  워커                           통지 (→ main, queued)
──────────────              ─────────────────────────      ─────────────────────
mgr.submit(js)    ──────▶   coordinator → 전역 submit pool progress / jobs_updated /
mgr.kill(js)      ──────▶   killer pool (전역 4스레드)      finished / error ...
mgr.start_polling(js)───▶   polling QThread (전역 1개)
```

- submit/kill은 실행을 큐에 넣고 반환하며, 결과는 Signal로 온다.
  create/add/replace/remove 등 로컬 편집과 pull 조회는 동기 API다.
- 상태 변경은 Store에 먼저 반영된 뒤 Signal로 전달된다. Signal은 그 전이의
  스냅샷이고, `js.jobs()`는 현재 스냅샷이므로 다음 갱신이 이미 반영됐을 수 있다.
- worker→main Signal은 queued connection — slot은 항상 main에서 실행된다.

## 1. submit — `mgr.submit(js)` (유일한 제출 경로)

`create_jobset([...])`(CREATED, 표 즉시 채움) 후 jobset의 작업을 (재)제출한다.
`only`를 주면 선택분만 제출한다. 가드: 제출 대상 전원이 비활성(CREATED/terminal)이어야 한다.

```
main                          coordinator                   전역 submit pool
────────────────────────      ──────────────────────────    ─────────────────────
mgr.submit(js)
 ├ 옵션·대상 검사
 ├ SubmitGate.register(ctx)       # kill barrier 중이면 born-cancelled
 └ _LaunchTask 큐잉 → 반환
                              ├ pre_submit (지정 시)
                              │  False/예외 → 원본 유지·정산
                              ├ submit_started
                              ├ 대상 레코드 리셋(SUBMITTING)
                              ├ task 전부 생성
                              ├ records_reset → main에서 완료·handler 무장
                              ├ jobs_updated(실제 리셋분)
                              └ pool.start(task × N) ─────▶ 취소 검사·rate limit 대기
                                                            ├ wrapper 실행
                                                            ├ 성공 → PEND + job_id
                                                            ├ 재시도 → RETRY_WAIT
                                                            │  → main QTimer → task
                                                            └ 최종 실패 → SUBMIT_FAILED

worker 변경분은 스로틀 배치로 전달한다. 마지막 task 완료 시:
jobs_updated(잔여 배치) → submit_finished(report) → jobset_updated(최종 요약)
```

Signal 순서(보장): `(pre_submit_started → pre_submit_finished)`\* → `submit_started` →
`jobs_updated[실제 리셋분]` → `submit_progress`+`jobs_updated`(스로틀 배치)
→ `submit_finished` → `jobset_updated`.  (\*pre_submit 게이트 지정 시)

**완료 통지**: 이후 폴링/`query_once`로 **전원 terminal** 감지 시
`jobset_finished(요약)` 1회 — 등록물과 무관하게 job 상태만 본다. 재제출로 다시
활성이 되면 재무장돼 다음 완료에 또 발화한다. 단, **사용자가 건 kill로** 전원
terminal이 된 완료는 통지하지 않는다(§2) — 스스로 끝낸 것이라 알릴 게 없다.

**완료 후처리(`post_process` 지정 시)**: 같은 감지 지점에서 `jobset_finished` 직후
worker에서 1회 — `post_processing_started → post_processing_finished(result)`.
성공/실패 무관(전원 terminal이면 실행), 한 제출당 1회. 예외는 `error_occurred`
+ `post_processing_finished(None)`.

상태: `CREATED → SUBMITTING → PEND | RETRY_WAIT(→SUBMITTING 재시도) |
SUBMIT_FAILED(최종)`. cancel/kill 시 `SUBMITTING/RETRY_WAIT → CANCELLED`
(실패 잔재 fail_reason/retry_count 함께 리셋).

## 2. kill — 제출에 대한 우선권

kill은 진행 중 submit에 **우선권**을 갖는다. 핵심은 SubmitGate barrier —
barrier 확인과 submit 등록이 한 lock 아래 원자적이라, "kill의 취소를 빠져나가는
늦은 제출"이 구조적으로 불가능하다 (`lifecycle.py`).

```
main                              killer pool worker
──────────────────────────       ─────────────────────────────────────
mgr.kill(js)
 ├ scope = gate.kill_scope(js, keys)   # 범위 = 겨냥한 job (None=전체)
 ├ scope.begin()                  # barrier ↑ + 범위 내 제출 즉시 취소 (논블로킹)
 │                                #   · 미착수 worker 중단 예약
 │                                #   · RETRY_WAIT QTimer 부활 방지
 ├ killer.kill_jobset(scope)      # 등록 + task 큐잉 (동기)
 └ kill_started 발행(동기) ◀━━ UI 스피너는 여기서 켠다
                                  _KillTask:
                                    scope.acquire()          # 정지 대기만 (blocking)
                                    │  ├ pool 슬롯 반납(releaseThread)
                                    │  └ 정지 대기 (제출 완료까지, 상한 있음)
                                    │     · 미제출 → CANCELLED 확정(kill 대상 아님)
                                    │     · 그새 제출됨 → PEND+job_id (스냅샷에 포함)
                                    │     · barrier 중 새 submit → 등록 거부(born-cancelled)
                                    ├ 대상 스냅샷 (is_on_lsf)
                                    ├ bkill id chunk 실행 + 확인 문구 파싱
                                    │     → kill_progress (스로틀)
                                    │     → 미확인분은 kill_max_retry까지 재시도
                                    ├ (verify 또는 수락 없는 해소) 재조회 → 실제 상태·잔존 확인
                                    ├ optimistic(기본): 수락 이력이 있고 해소된 대상 EXIT
                                    ├ scope.release()        # barrier ↓ (_run의 finally)
                                    └ 결과를 main으로 전달 (활동 등록 유지)
                                          → main에서 활동 해제 → kill_finished
                                          → jobs_updated + jobset_updated
                                          → 보류된 완료 판정 재개
```

- kill 경로는 **job_id chunk 단일**이다 — group/array/name 기반 일괄 kill은 쓰지
  않는다(제출이 wrapper 단일 경로라 LSF 부착물이 없다). ARG_MAX 안전은
  `kill_chunk_size`/`arg_max`가 담당한다.
- kill의 진행 등록은 main의 결과 전달까지 유지한다. 그 사이 폴링이 terminal을
  관측해도 완료 판정은 보류한다. 결과 전달 후 관련 JobSet을 다시 판정하므로
  전원 kill의 오통지와 자연 종료의 통지 누락을 함께 막는다.
- chunk 는 `kill_workers`(기본 4) 개까지 **동시에** 돈다 — 실행 풀이 공용이라
  kill 명령이 몇 건이든 bkill 총수가 이 값을 넘지 않는다.
  직렬(=1)이면 소요가 `ceil(N/kill_chunk_size) x bkill 1회`로 늘어선다.
  동시 요청은 `kill_workers x kill_chunk_size`건. 서로 다른 jobset 의 kill 은
  이와 별개로 Killer 풀(4개)까지 병행된다.
- **`kill_timeout_s`는 bkill 호출 1회(= chunk 전체)의 상한이다** — job 1건이
  아니다. 못 끝내면 subprocess timeout이 bkill **클라이언트**를 중간에 죽여
  앞쪽 id만 죽고 뒤쪽은 요청조차 안 나간 채 잘린다. 그래서 timeout난 chunk는
  '안 죽었다'가 아니라 **'모른다'**로 다루고, 다시 bkill을 쏘기 전에 조회로
  생사를 확인한다(이미 죽었으면 재시도하지 않는다). 예산이 target당 100ms
  미만이면 생성 시 경고가 나온다.
- `kill_status_policy="actual"`이면 EXIT 전이는 다음 폴링에서(최대 `poll_interval_s`
  지연) — GUI는 기본(optimistic) 유지 권장.
- 정지 대기 초과는 `KillReport.errors`에 남고 optimistic 표시도 억제된다.
- 제출 우선권은 **kill이 겨냥한 job에만, 항상** 걸린다 — `KillScope(jobset_id, keys)`의
  범위 인자 하나로 표현된다(`keys=None` = jobset 전체 = 전체 kill).
  제출 중인 대상은 job_id가 없어 `bkill` 대상이 될 수 없으므로, 기다리지 않으면
  key→id 해석에서 통째로 빠져 kill을 빠져나간다(→ 나중에 PEND→RUN으로 부활).
- barrier도 같은 범위다 — kill 진행 중 도착한 제출에서 **그 key만** born-cancelled
  되고(레코드는 리셋조차 안 됨), 나머지 job은 정상 제출된다.
- 부분 kill(`only_state=`)의 대상은 이미 on-LSF 상태라 멈출 제출이 없다. "제출 폭주를
  멈추면서 전부 정리"는 `mgr.cancel_submit(js)` + **전체 kill**의 조합이다.
- barrier↑와 취소는 kill 접수 스레드에서 즉시(`scope.begin()`, 논블로킹),
  정지 대기만 killer worker에서(`scope.acquire()`) — 취소가 worker 차례까지 밀리면
  그동안 제출된 job이 전부 '제출됐다가 곧 죽는' 낭비가 된다.

## 3. 재실행 — replace_jobs + submit

재실행은 별도 파이프라인이 아니라 **데이터 조작 + 일반 submit**이다:

```
main (전부 앱이 직접 제어)
──────────────────────────────────────────
① (살아있으면) mgr.kill(js) → kill_finished 대기
② mgr.replace_jobs(js, [...], job_keys=[기존과 동일])
                         # 해당 job 만 CREATED 로 교체(같은 키 자리),
                         # 가드: 교체 대상이 비활성
③ mgr.submit(js)         # 전 job 리셋 후 재제출 — §1과 동일 흐름
```

- ②의 교체는 레코드만 바꾼다 — `force=True`로 활성 job을 교체해도 LSF의 실제
  job은 그대로다(정리는 앱 책임, 먼저 kill 권장).
- ④의 리셋: job_id/exit_code/실행시간/fail_message/클러스터 소거,
  `job_key`/`user_data`/`submit_cwd` 보존. handler 자동 재무장.

## 4. cancel (`mgr.cancel_submit(js)`)

```
cancel_submit(): ctx.cancel_event set → 반환(즉시)
  · 미착수 worker  → 안전 지점에서 SUBMITTING/RETRY_WAIT → CANCELLED 확정
  · 제출 진행 중   → 완료까지 진행(PEND 확정) — 강제 중단하지 않는다
  · 대기 중 재시도 → 발화 시 포기 확정
  → 각 취소분은 jobs_updated 배치로, 마지막에 submit_finished(cancelled=k)
```

## 5. polling (자동 상태 갱신)

```
polling QThread (jobset당 QTimer, interval마다)
──────────────────────────────────────────────
① 대상 스냅샷: is_on_lsf 인 job 의 job_id 수집
② bjobs -noheader -o "jobid stat exit_code … delimiter=';'" <id...>
     — chunk_size(기본 500) 단위로 나눠 조회 (유일한 조회 수단)
③ 판정:
     bjobs에서 발견           → 상태 반영 (guard CAS — 그새 바뀐 레코드 보호)
     미발견 + 조회 전부 성공  → 미발견 streak++ → lost_after_missing_polls 회
                                연속이면 LOST 확정 (NOT_FOUND_IN_LSF)
     미발견 + 조회 실패 섞임  → 판단 보류 (다음 사이클 재시도) ◀ 장애≠부재
④ 통지: jobset_updated(요약) + jobs_updated(변경분만) + job_lost
⑤ handler 실행 (등록돼 있으면 — 이 사이클 상태 기준, worker 스레드)
⑥ 전원 terminal 또는 활동 없음 2사이클 → polling 자동 중지
```

- ②는 **chunk 단위 실패 격리**: 실패 chunk의 job만 보류하고 성공 chunk는 정상
  판정한다. 연속 2회 실패면 회로 차단(남은 chunk 즉시 실패 처리) — 전면 장애에서
  폴링 스레드가 chunk 수 × timeout 만큼 블록되지 않는다.
- 보류·LOST 경고는 사이클당 1줄로 집계된다.
- `collect_clusters=True`면 조회 포맷에 `source_cluster`/`forward_cluster`가 추가되고,
  사이트가 그 필드를 모르면 3단 강등(FULL+MC → FULL → CORE)으로 그 필드만 포기한다.

## 6. 상태 전이도

```
 CREATED ──▶ SUBMITTING ──▶ PEND ──▶ RUN ──▶ DONE
                │   ▲         │        │       (terminal)
                │   │재시도   │        ├──▶ EXIT (terminal)
                ▼   │         │        │     ▲ kill(optimistic)
             RETRY_WAIT ──────┼────────┼─────┘
                │   │         │        └──▶ PSUSP/USUSP/SSUSP ⇄ RUN
                │   │         ▼
                │   │       LOST (조회는 전부 성공했는데 미발견, terminal)
                │   └──▶ SUBMIT_FAILED (재시도 N회 모두 실패, terminal)
                └──────▶ CANCELLED     (제출 도중 kill/취소로 중단, terminal)
```

`is_on_lsf` = PEND/RUN/SUSP\*/UNKWN/ZOMBI — 폴링·kill 스냅샷 대상.
`is_terminal` = DONE/EXIT/SUBMIT_FAILED/CANCELLED/LOST — 더 이상 전이하지 않음.
`is_failed` = EXIT/SUBMIT_FAILED/LOST — CANCELLED는 **실패가 아니다**(의도한 중단).
`is_inactive` = CREATED 또는 terminal — submit/편집/remove 가드의 공통 술어.
