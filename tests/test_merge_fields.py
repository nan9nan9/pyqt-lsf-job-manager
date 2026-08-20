"""폴링의 변경 판정 — monitor.merge_fields.

이 함수가 "이번 조회로 바뀐 게 있나"의 유일한 판정 지점이다. 규칙이 셋
얽혀 있고(유효값 보존 / 클러스터 옵션 / run_time 예외) 잘못되면 증상이
조용하다: 매 사이클 전 job이 재전이해 jobs_updated가 폭주하거나, 반대로
바뀐 상태가 영영 반영되지 않는다.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from lsfmgr import JobState
from lsfmgr.command import JobStatus
from lsfmgr.monitor import merge_fields
from lsfmgr.states import JobRecord

T0 = datetime(2026, 8, 21, 10, 0, 0)
T1 = datetime(2026, 8, 21, 11, 0, 0)


def _rec(**kw):
    base = dict(job_id=1000, array_index=None, jobset_id="js", job_key="k",
                state=JobState.RUN, command="x")
    base.update(kw)
    return JobRecord(**base)


def _st(**kw):
    base = dict(job_id=1000, array_index=None, state=JobState.RUN,
                exit_code=None)
    base.update(kw)
    return JobStatus(**base)


def _merge(rec, st, runtime=False, clusters=False):
    return merge_fields(rec, st, runtime_updates=runtime,
                        collect_clusters=clusters)


# --- 변화 없음 → None (재전이 금지) -----------------------------------
def test_identical_is_no_change():
    assert _merge(_rec(), _st()) is None


def test_run_time_alone_is_not_a_change_by_default():
    """poll_runtime_updates=False(기본)면 경과시간만 바뀐 것은 전이가 아니다.
    포함시키면 RUN 전원이 매 폴링 재전이한다(5000건 사이클당 5000 transition)."""
    rec = _rec(run_time_s=100)
    assert _merge(rec, _st(run_time_s=160)) is None


def test_run_time_is_a_change_when_asked():
    rec = _rec(run_time_s=100)
    got = _merge(rec, _st(run_time_s=160), runtime=True)
    assert got is not None and got["run_time_s"] == 160


# --- 유효값 보존 — 관측이 None이면 저장값을 지킨다 ----------------------
def test_none_observation_preserves_the_stored_value():
    """포맷 강등으로 확장 필드가 안 오면 저장값을 지운다 → None != 저장값이
    매 사이클 참이 되어 전 job이 재전이한다(리뷰 M6)."""
    rec = _rec(run_time_s=100, start_time=T0)
    assert _merge(rec, _st(run_time_s=None, start_time=None)) is None


def test_none_observation_does_not_erase_on_a_real_change():
    """다른 이유로 전이할 때도 확장 필드는 보존값이 실려야 한다."""
    rec = _rec(run_time_s=100, start_time=T0)
    got = _merge(rec, _st(state=JobState.DONE, run_time_s=None,
                          start_time=None))
    assert got["run_time_s"] == 100 and got["start_time"] == T0


@pytest.mark.parametrize("field,old,new", [
    ("start_time", T0, T1),
    ("finish_time", None, T1),
])
def test_extension_field_change_is_detected(field, old, new):
    got = _merge(_rec(**{field: old}), _st(**{field: new}))
    assert got is not None and got[field] == new


# --- 상태/exit_code ----------------------------------------------------
def test_state_change():
    got = _merge(_rec(state=JobState.PEND), _st(state=JobState.RUN))
    assert got is not None


def test_exit_code_change():
    got = _merge(_rec(state=JobState.EXIT, exit_code=None),
                 _st(state=JobState.EXIT, exit_code=130))
    assert got is not None and got["exit_code"] == 130


# --- 클러스터 2필드은 옵션일 때만 --------------------------------------
def test_clusters_untouched_when_disabled():
    rec = _rec(source_cluster=None)
    assert _merge(rec, _st(source_cluster="c1")) is None, "꺼짐인데 전이했다"


def test_clusters_detected_when_enabled():
    rec = _rec(source_cluster=None)
    got = _merge(rec, _st(source_cluster="c1"), clusters=True)
    assert got is not None and got["source_cluster"] == "c1"


def test_cluster_fields_absent_from_payload_when_disabled():
    """꺼져 있으면 fields에 클러스터 키가 아예 없어야 한다(무접촉)."""
    got = _merge(_rec(state=JobState.PEND), _st(state=JobState.RUN))
    assert "source_cluster" not in got and "forward_cluster" not in got


# --- job_id는 있으면 유지 ---------------------------------------------
def test_existing_job_id_is_kept():
    """조회가 접힌 array를 대표값으로 줘도 레코드의 id를 덮지 않는다."""
    got = _merge(_rec(job_id=1000, state=JobState.PEND),
                 _st(job_id=9999, state=JobState.RUN))
    assert got["job_id"] == 1000


def test_missing_job_id_is_filled():
    got = _merge(_rec(job_id=None, state=JobState.PEND),
                 _st(job_id=1000, state=JobState.RUN))
    assert got["job_id"] == 1000
