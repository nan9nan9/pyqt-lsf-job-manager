"""공용 fixture — FakeLsf, 두 Store 백엔드, manager 팩토리."""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


@pytest.fixture
def fake_lsf():
    return FakeLsf()


@pytest.fixture
def config(tmp_path):
    # 테스트는 빠르게: retry delay 최소화
    return LsfConfig(retry_delay_s=0.05, retry_backoff=1.0,
                     kill_retry_delay_s=0.05,
                     script_dir=str(tmp_path / "scripts"))


@pytest.fixture
def store():
    """계약 테스트용 store (InMemory 단일 백엔드)."""
    s = InMemoryStore()
    yield s
    s.store_dispose()


@pytest.fixture
def manager(qtbot, fake_lsf, config):
    """InMemoryStore 기반 manager (기본)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    yield mgr
    mgr.shutdown()




def submit_cmds(mgr, commands, *, wrapper=False, count=None,
                merge_ids=None, **opts):
    """v9 흐름 축약 헬퍼 — create_jobset(commands=...) → submit.

    구 one-shot(mgr.submit(list)/submit_wrapper) 테스트를 단일 제출 경로로
    옮기는 용도. wrapper=False면 bsub 경로(구 submit(list)와 동일 의미),
    True면 wrapper 경로(구 submit_wrapper). 반환: JobSet 핸들."""
    if isinstance(commands, str):
        commands = [commands] * (count or 1)
    label = opts.pop("label", "")
    tags = opts.pop("tags", ())
    js = mgr.create_jobset(list(commands), wrapper=wrapper,
                           merge_ids=merge_ids, label=label, tags=tags)
    mgr.submit(js, **opts)
    return js
