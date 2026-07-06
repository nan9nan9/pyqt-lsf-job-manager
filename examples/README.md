# lsfmgr 기본 예제

lsfmgr 의 주요 기능을 **하나의 GUI 대시보드**(`basic_example.py`)에서 모두 다룹니다.
실제 LSF cluster 없이 실행되도록 저장소에 동봉된 **mocklsf**(가상 LSF)를 테스트
환경으로 씁니다. job 제출은 **`submit_wrapper`** 로 이뤄지며, job 마다
`primesim_sub`/`verilog_sub` 등 서로 다른 wrapper 커맨드(또는 '혼합')를 그대로
실행하고 그 결과의 `Job <id>` 로 job 을 관리합니다(실제 bsub 직접 호출 아님).

```bash
pip install -e .[test]        # 프로젝트 루트에서
python examples/basic_example.py      # 통합 GUI 대시보드
python examples/handler_example.py    # JobSet handler 예제 (콘솔)
```

## 대시보드가 다루는 기능

| 영역 | 데모하는 기능 |
|---|---|
| Submit 옵션 폼 | `submit_wrapper`, wrapper 선택/‏혼합, `count=`, `workers`, `max_retry`, `rate_limit_per_s`, queue |
| 진행률 바 / Cancel | `progress` throttle(QT-5), `cancel()` 안전 중단(QT-6) |
| JobSet 목록 | 다중 JobSet 요약 실시간 갱신, Facade Signal 스트림(README §9) |
| job 모니터링 테이블 | 선택 JobSet 의 job 상태 배치 갱신(QT-4), 상태별 색 |
| Kill 제어 | 전체 kill(job_id chunk, verify), `PEND만 Kill`(`only_state`), `KillReport` |
| 실패 처리 | retry(비정상 종료만), `SUBMIT_FAILED`/`EXIT`, `detect_lost()` |
| 세션 복원 | `persistent=True` + `앱 재시작 시뮬` → orphan → `recover_jobset()` → `reconcile()`(FR-6) |
| 실행 로그 | 실제 실행된 wrapper 커맨드 · 할당 job_id · 상태 전이 |

기본으로 제출 실패율(0.12)·EXIT 확률(0.12)을 주입해 retry/EXIT 상태가 자연스럽게
관찰됩니다.

## handler 예제 (`handler_example.py`)

JobSet 에 **이름 있는 handler** 를 붙여, job 이 RUN 인 동안 **폴링 사이클마다**
worker 스레드에서 job 출력 파일을 파싱하고(중간 수집), DONE/EXIT 시 최종 수집을 한
번 더 수행하는 콘솔 예제입니다. `js.add_handler(name, fn, start_states=,
end_states=)` 등록 → `handler_finished(jsid, name, HandlerResult)` 로 결과 수신 →
모든 job 최종 수집까지의 전체 흐름을 보여줍니다.
`ctx.working_dir`(LSF exec_cwd)/`run_time_s` 같은 LSF 유래 필드 활용도 포함합니다.
자세한 동작 규칙은 [`../docs/lsfmgr.md`](../docs/lsfmgr.md) §2.5 참고.

## 파일

- `basic_example.py` — 통합 GUI 대시보드 (기본 예제).
- `handler_example.py` — JobSet handler 주기 실행/최종 수집 (콘솔 예제).
- `common.py` — mocklsf 테스트 환경 셋업 + manager 생성 헬퍼:
  - `make_manager(wrapper="primesim_sub", **kwargs)` / `mocklsf_paths(...)` —
    mocklsf 명령 경로를 주입한 manager 구성. `wrapper` 로
    `finesim_sub`/`spectrefx_sub` 선택 가능.
  - `configure_mocklsf(pend=, run=, submit_fail_rate=, exit_rate=, ...)` —
    mocklsf 타이밍/실패율을 `MOCKLSF_*` 환경변수로 설정(첫 submit 이전 호출).
  - `install_logging`, `maybe_autoquit`(`LSFMGR_DEMO_AUTOQUIT=<초>`).

mocklsf 자체에 대한 자세한 내용은 [`../docs/mocklsf.md`](../docs/mocklsf.md) 참고.

> 참고: LOST(job이 흔적 없이 소실)는 mocklsf 가 재현하지 않습니다. `detect_lost()`
> 는 호출 가능하지만 mocklsf 환경에서는 보통 0건입니다.

## 실제 LSF 에서 실행

```bash
LSFMGR_REAL=1 python examples/basic_example.py   # mocklsf 대신 PATH 의 bsub/bjobs/bkill
```

## 스모크 테스트 (headless)

```bash
LSFMGR_DEMO_AUTOQUIT=20 QT_QPA_PLATFORM=offscreen python examples/basic_example.py
```
