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
    s.close()


@pytest.fixture
def manager(qtbot, fake_lsf, config):
    """InMemoryStore 기반 manager (기본)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    yield mgr
    mgr.shutdown()


