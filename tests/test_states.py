"""JobState / JobRecord / JobSetRecord 단위 테스트."""
from __future__ import annotations

import dataclasses

import pytest

from lsfmgr.states import JobRecord, JobSetRecord, JobState


def test_terminal_states():
    assert JobState.DONE.is_terminal
    assert JobState.EXIT.is_terminal
    assert JobState.SUBMIT_FAILED.is_terminal
    assert JobState.LOST.is_terminal
    assert not JobState.RUN.is_terminal
    assert not JobState.RETRY_WAIT.is_terminal


def test_failed_states():
    assert JobState.EXIT.is_failed
    assert JobState.SUBMIT_FAILED.is_failed
    assert JobState.LOST.is_failed
    assert not JobState.DONE.is_failed


def test_on_lsf_states():
    assert JobState.PEND.is_on_lsf
    assert JobState.RUN.is_on_lsf
    assert JobState.ZOMBI.is_on_lsf
    assert not JobState.DONE.is_on_lsf          # terminal은 재조회 불필요
    assert not JobState.CREATED.is_on_lsf
    assert not JobState.SUBMIT_FAILED.is_on_lsf


def test_job_record_frozen():
    rec = JobRecord(job_id=1, array_index=None, jobset_id="js1",
                    lsf_job_name="js1_0", state=JobState.PEND)
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.state = JobState.RUN                # type: ignore[misc]


def test_jobset_record_frozen():
    js = JobSetRecord(jobset_id="js1", intended_count=10)
    with pytest.raises(dataclasses.FrozenInstanceError):
        js.closed = True                        # type: ignore[misc]


def test_job_key():
    rec = JobRecord(job_id=None, array_index=None, jobset_id="js1",
                    lsf_job_name="js1_3", state=JobState.CREATED)
    assert rec.job_key == "js1_3"
