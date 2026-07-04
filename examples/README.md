# lsfmgr 예제 모음

실제 LSF cluster 없이 실행할 수 있도록 **시간 진행형 LSF 시뮬레이터**
(`mock_lsf.py`)를 runner로 주입합니다. job이 실제 시간에 따라
PEND → RUN → DONE/EXIT로 진행되므로 polling·kill·복원 동작을 그대로
관찰할 수 있습니다.

```bash
pip install -e .[test]        # 프로젝트 루트에서
python examples/08_dashboard.py
```

실제 LSF에서 돌리려면: `LSFMGR_REAL=1 python examples/08_dashboard.py`
(시뮬레이터 대신 PATH의 bsub/bjobs/bkill 사용)

| 예제 | 내용 | 데모하는 기능 |
|---|---|---|
| `01_minimal_console.py` | GUI 없는 3줄 사용법 | README Quick Start, AUTO-1/2 |
| `02_submit_progress.py` | 진행률 바 + 취소 | `progress` throttle(QT-5), `cancel()`(QT-6), rate limit |
| `03_monitor_table.py` | QTableView 실시간 모니터링 | `jobs_updated` 변경분 batch 반영(QT-4), 상태별 색 |
| `04_array_modes.py` | array/bulk 자동 선택 비교 | AUTO-4, `count=`, dispatch 스크립트, bsub 호출 수 |
| `05_kill_strategies.py` | 전체/부분/verify kill | FR-3 전략(①group 1회), `only_state`, `KillReport` |
| `06_retry_and_lost.py` | 실패 주입 데모 | retry(FR-2), `SUBMIT_FAILED`/`LOST`, `failed` Signal, `detect_lost()` |
| `07_session_restore.py` | SQLite 세션 복원 | `persistent=True`, orphan → `recover_jobset()` → `js.reconcile()` |
| `08_dashboard.py` | 종합 대시보드 | 옵션 3단 계층(②↔③), 다중 JobSet, Low-level Facade Signal |

## 공통 파일

- `mock_lsf.py` — `SimulatedLsf`: bsub/bjobs/bkill/bhist를 흉내내는 runner.
  `pend_s`/`run_s`(구간), `exit_rate`, `submit_fail_rate`(retry 데모),
  `lost_rate`(LOST 데모)를 조절해 시나리오를 만든다.
- `common.py` — manager 생성(`make_manager`), 콘솔 로깅(`install_logging`),
  스모크 테스트용 자동 종료(`LSFMGR_DEMO_AUTOQUIT=<초>`).

## 스모크 테스트 (headless)

```bash
for f in examples/0*.py; do
    LSFMGR_DEMO_AUTOQUIT=3 QT_QPA_PLATFORM=offscreen python "$f" || echo "FAIL: $f"
done
```
