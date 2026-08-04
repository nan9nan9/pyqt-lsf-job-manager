# lsfmgr 통합 예제

lsfmgr 의 주요 기능을 **하나의 GUI 대시보드**(`gui_demo.py`)에서 모두 다룹니다.
실제 LSF cluster 없이 실행되도록 저장소 동봉 **mocklsf**(가상 LSF)를 테스트
환경으로 쓰며, job 제출은 **`create_jobset([...])` → `submit`** 단일 경로 —
job 마다 `customwrapper_sub` 같은 wrapper 커맨드(혼합 가능)를 그대로 실행하고
그 출력의 `Job <id>` 로 관리합니다.

```bash
pip install -e .[test]        # 프로젝트 루트에서
python examples/gui_demo.py   # 통합 GUI 데모
```

## 다루는 기능

| 영역 | 데모하는 기능 |
|---|---|
| Submit 옵션 폼 | `create_jobset`+`submit`, wrapper 선택/혼합, `workers`/`max_retry`/`rate_limit_per_s`, queue |
| 진행률 바 / Cancel | progress throttle, `cancel_submit` 안전 중단 |
| JobSet 트리 | 다중 JobSet 요약 실시간 갱신, `mgr.*` 전역 Signal 스트림 |
| job 테이블 | 변경분 배치 **증분 upsert**(전체 재그리기 금지), 상태별 색, cluster 열 |
| job 추가 / 재실행 | **merge** 로만 추가, 실패분 같은 `job_key` 교체 후 전체 재제출 |
| Kill 제어 | 전체 kill(verify, **MC-aware** — 생성자 `cluster_envpaths` 로 클러스터별 분류 kill), `PEND만`(제출 우선권 opt-in), 선택 행만(`kill_jobs`) |
| handler | 체크 시 `add_handler` — RUN 중 폴링마다 job 출력 파싱 + 종료 시 최종 1회 → `handler_finished` 로그 |
| post_process | 전원 terminal 도달 시 worker 에서 1회 종합 집계 → `post_processing_finished` |
| job 상세 | 테이블 더블클릭 → 로컬 레코드 상세 (LSF 호출 0) |
| 실패 처리 | retry(비정상 종료만), `SUBMIT_FAILED`/`EXIT`, `detect_lost()` |

기본으로 제출 실패율(0.12)·EXIT 확률(0.12)을 주입해 retry/EXIT 상태가 자연스럽게
관찰됩니다.

### MC(MultiCluster) 시나리오

폼의 "MC forward 흉내"를 켜고 제출하면 mocklsf 가 일부 job 을 원격 클러스터로
forward 합니다(`collect_clusters=True` 폴링이 `forward_cluster` 를 채움 —
테이블 cluster 열에서 확인). 이후 "Kill+verify (MC-aware)"는 forward job 을
클러스터별로 분류해 그 env(cshrc)를 `source` 한 bkill 로, 나머지는
env 지정 없는 일반 bkill 로 죽입니다 — forward job 이 로컬 bkill 로 안 죽는 실제 MC 환경의
해법 시연입니다. 상세는 [`../docs/mocklsf.md`](../docs/mocklsf.md) 참고.

## 파일

- `gui_demo.py` — **통합 GUI 데모** (유일한 예제).
- `common.py` — mocklsf 테스트 환경 셋업 + manager 생성 헬퍼:
  - `mocklsf_paths()` / `make_manager(**kwargs)` — mocklsf 조회/kill 명령 경로 주입.
  - `wrapper(tool, *args)` — 제출 wrapper 커맨드(토큰 리스트) 생성.
  - `configure_mocklsf(pend=, run=, submit_fail_rate=, exit_rate=,
    forward_clusters=, forward_rate=, ...)` — `MOCKLSF_*` 환경변수 설정.
  - `cluster_env_path(cluster)` — forward 클러스터 cshrc 경로(생성자 `cluster_envpaths` 값).
  - `install_logging`, `maybe_autoquit`(`LSFMGR_DEMO_AUTOQUIT=<초>`).

> 참고: LOST(job이 흔적 없이 소실)는 mocklsf 가 재현하지 않습니다. `detect_lost()`
> 는 호출 가능하지만 mocklsf 환경에서는 보통 0건입니다.

## 실제 LSF 에서 실행

```bash
LSFMGR_REAL=1 python examples/gui_demo.py   # mocklsf 대신 PATH 의 bjobs/bkill
```

## 스모크 테스트 (headless)

기동 → 소량 제출(handler 포함) → kill → 자동 종료:

```bash
LSFMGR_DEMO_AUTORUN=1 LSFMGR_DEMO_AUTOQUIT=15 \
    QT_QPA_PLATFORM=offscreen python examples/gui_demo.py
```

## 대량 job 스트레스 테스트

`LSFMGR_DEMO_SUBMIT=<개수>` 를 주면 기동 직후 그 개수로 자동 제출합니다(워커 32).
테이블은 job_key **증분 upsert**라 대량에서도 부드럽습니다(5000행 기준 렌더링
~17s → ~0.3s).

```bash
LSFMGR_DEMO_SUBMIT=5000 python examples/gui_demo.py
```
