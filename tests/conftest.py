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
    return LsfConfig(rate_limit_per_s=None, retry_delay_s=0.05, retry_backoff=1.0,
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




def mk_jobset(mgr, commands=(), *, job_keys=None, **kw):
    """테스트 편의 — job_keys를 안 주면 연번으로 채운다.

    라이브러리는 job_keys를 **필수**로 요구한다(앱이 job을 나중에 가리킬
    수단이 그 키뿐이라 자동 생성하지 않는다). 키 자체가 관심사가 아닌
    테스트까지 매번 적게 하면 신호 대 잡음만 나빠지므로 여기서 채운다."""
    cmds = list(commands)
    if cmds and job_keys is None:
        job_keys = [f"k{i}" for i in range(len(cmds))]
    return mgr.create_jobset(cmds, job_keys=job_keys, **kw)


def submit_cmds(mgr, commands, *, wrapper=None, count=None,
                job_keys=None, **opts):
    """v9 흐름 축약 헬퍼 — create_jobset(commands=...) → submit.

    v10: 제출은 wrapper 단일 경로 — `wrapper` 인자는 하위 호환으로 받기만
    하고 무시한다(구 bsub 경로 테스트들의 호출부를 안 고치기 위함).
    반환: JobSet 핸들."""
    del wrapper                              # v10: 경로 구분 없음
    if isinstance(commands, str):
        commands = [commands] * (count or 1)
    label = opts.pop("label", "")
    tags = opts.pop("tags", ())
    js = mk_jobset(mgr, list(commands),
                   job_keys=job_keys, label=label, tags=tags)
    mgr.submit(js, **opts)
    return js
