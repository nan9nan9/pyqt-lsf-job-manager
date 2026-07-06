"""공용 fixture — FakeLsf, 두 Store 백엔드, manager 팩토리."""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager, SqliteStore
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


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    """계약 테스트용 — 두 백엔드를 동일 스위트로 검증 (NFR-8)."""
    if request.param == "memory":
        s = InMemoryStore()
    else:
        s = SqliteStore(str(tmp_path / "jobsets.db"))
    yield s
    s.close()


@pytest.fixture
def manager(qtbot, fake_lsf, config):
    """InMemoryStore 기반 manager (기본)."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    yield mgr
    mgr.shutdown()


@pytest.fixture
def sqlite_manager(qtbot, fake_lsf, config, tmp_path):
    mgr = LsfJobManager(store=SqliteStore(str(tmp_path / "db.sqlite")),
                        config=config, runner=fake_lsf)
    yield mgr
    mgr.shutdown()
