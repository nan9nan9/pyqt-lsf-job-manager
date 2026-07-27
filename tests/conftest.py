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
                     kill_retry_delay_s=0.05)


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




def submit_cmds(mgr, commands, *, wrapper=None, count=None,
                merge_ids=None, **opts):
    """v9 흐름 축약 헬퍼 — create_jobset(commands=...) → submit.

    v10: 제출은 wrapper 단일 경로 — `wrapper` 인자는 하위 호환으로 받기만
    하고 무시한다(구 bsub 경로 테스트들의 호출부를 안 고치기 위함).
    반환: JobSet 핸들."""
    del wrapper                              # v10: 경로 구분 없음
    if isinstance(commands, str):
        commands = [commands] * (count or 1)
    label = opts.pop("label", "")
    tags = opts.pop("tags", ())
    js = mgr.create_jobset(list(commands),
                           merge_ids=merge_ids, label=label, tags=tags)
    mgr.submit(js, **opts)
    return js
