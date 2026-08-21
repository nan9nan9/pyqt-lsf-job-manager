"""internal 원장 보존 기간(internal_retention_days)이 실제로 동작한다.

콜백 조회원(job_status_fetcher)의 원장은 본 job을 계속 들고 있다. 장수 세션
(몇 주 켜 두는 GUI)에서 이게 안 빠지면 종료된 job이 무한 누적된다 — 보존
기간이 그걸 막는 유일한 장치인데 테스트가 하나도 없었다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from lsfmgr.internal_status import InternalStatusSource


def _payload(finish_offset_days: float, n: int = 5):
    """종료된 job n건 — finish_time을 지금으로부터 과거로 민다."""
    fin = (datetime.now()
           - timedelta(days=finish_offset_days)).strftime("%Y-%m-%dT%H:%M:%S")
    return {"jobs": [{"dataId": f"{100 + i}.c1", "stat": "DONE",
                      "endTime": fin} for i in range(n)]}


def _source(**kw):
    kw.setdefault("refresh_min_s", 0.0)
    kw.setdefault("wait_timeout_s", 5.0)
    return InternalStatusSource(**kw)


def test_old_terminal_jobs_are_dropped():
    src = _source(fetcher=lambda: _payload(30), retention_days=14.0)
    try:
        src.statuses_by_ids([100, 101, 102, 103, 104], fresh=True)
        assert src.stats()["entries"] == 0, (
            "보존 기간(14일)이 지난 종료 job이 원장에 남았다")
    finally:
        src.shutdown()


def test_recent_terminal_jobs_are_kept():
    src = _source(fetcher=lambda: _payload(1), retention_days=14.0)
    try:
        src.statuses_by_ids([100, 101, 102, 103, 104], fresh=True)
        assert src.stats()["entries"] == 5
    finally:
        src.shutdown()


def test_retention_zero_disables_expiry():
    """0 = 만료 끔 — 아무리 오래돼도 안 버린다(문서화된 규칙)."""
    src = _source(fetcher=lambda: _payload(3650), retention_days=0.0)
    try:
        src.statuses_by_ids([100, 101, 102, 103, 104], fresh=True)
        assert src.stats()["entries"] == 5
    finally:
        src.shutdown()


def test_running_jobs_survive_regardless_of_age():
    """종료되지 않은 job은 아무리 오래돼도 남긴다 — 아직 추적 대상이다."""
    old = (datetime.now()
           - timedelta(days=99)).strftime("%Y-%m-%dT%H:%M:%S")
    src = _source(fetcher=lambda: {"jobs": [
        {"dataId": "100.c1", "stat": "RUN", "startTime": old}]},
        retention_days=1.0)
    try:
        src.statuses_by_ids([100], fresh=True)
        assert src.stats()["entries"] == 1
    finally:
        src.shutdown()


def test_config_wires_retention_through(qtbot, fake_lsf):
    """LsfConfig → command → source까지 값이 실제로 전달되는지."""
    from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(job_status_fetcher=lambda: {"jobs": []},
                         internal_retention_days=3.0),
        runner=fake_lsf)
    try:
        assert mgr.command.internal_status._retention == timedelta(days=3.0)
    finally:
        mgr.shutdown()


@pytest.mark.parametrize("value", [
    datetime(2026, 8, 20, 10, 0, 0),                      # datetime 객체 그대로
    "2026-08-20T10:00:00Z", "2026-08-20T10:00:00+09:00",  # 오프셋 표기
    1755683000, "1755683000", 1755683000123,              # epoch 초/문자열/ms
    "2026-08-20 10:00:00.123456",
])
def test_parsed_times_are_always_naive(value):
    """만료 비교가 뺄셈이라, aware가 하나라도 섞이면 TypeError로 청소가 죽는다.

    앱의 REST 콜백은 타임존 붙은 문자열이나 datetime 객체를 그대로 주는 일이
    흔하다 — 파서가 어떤 입력을 받든 naive로 통일해야 한다.
    """
    from lsfmgr.internal_status import parse_time

    parsed = parse_time(value)
    assert parsed is not None, f"파싱 실패: {value!r}"
    assert parsed.tzinfo is None, f"aware datetime이 새어 나왔다: {parsed!r}"
    # 실제로 뺄셈이 되는지까지
    assert isinstance(datetime.now() - parsed, timedelta)


def test_aware_payload_does_not_break_pruning():
    """타임존 붙은 종료 시각을 주는 사이트에서도 만료가 정상 동작한다."""
    old = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    src = _source(fetcher=lambda: {"jobs": [
        {"dataId": "100.c1", "stat": "DONE", "endTime": old + "+09:00"}]},
        retention_days=14.0)
    try:
        src.statuses_by_ids([100], fresh=True)
        assert src.stats()["entries"] == 0
    finally:
        src.shutdown()
