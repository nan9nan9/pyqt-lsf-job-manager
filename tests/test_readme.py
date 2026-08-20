"""README 예제 그대로 동작 검증 — 문서와 구현의 계약 테스트."""
from __future__ import annotations

import pytest

from lsfmgr import (
    JobState,
    LsfJobManager,
)
from tests.conftest import submit_cmds


# ----------------------------------------------------------------------
# §1 Quick Start — 3줄 그대로
# ----------------------------------------------------------------------
def test_quickstart_verbatim(qtbot, fake_lsf, config):
    mgr = LsfJobManager(config=config, runner=fake_lsf)
    try:
        lines = []
        js = submit_cmds(mgr, [f"mytool run_{i}.sp" for i in range(50)])
        js.jobset_updated.connect(
            lambda s: lines.append(
                f"RUN={s.get('RUN', 0)} DONE={s.get('DONE', 0)}/{s['total']}"))
        qtbot.waitUntil(lambda: len(lines) >= 1, timeout=10000)
        assert lines[0].endswith("/50")
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# §4.1 SubmitReport — rpt.ok / rpt.total / rpt.failed 표기
# ----------------------------------------------------------------------
def test_report_ok_alias(qtbot, manager, fake_lsf):
    msgs = []
    js = submit_cmds(manager, ["a x", "b y"], auto_poll=False)
    js.submit_finished.connect(lambda rpt: msgs.append(
        f"submitted {rpt.ok}/{rpt.total} (failed {rpt.failed})"))
    qtbot.waitUntil(lambda: bool(msgs), timeout=10000)
    assert msgs == ["submitted 2/2 (failed 0)"]


# ----------------------------------------------------------------------
# §4.2 동일 command 반복 제출 — v9: array 제출 제거, job N건 개별 제출
# ----------------------------------------------------------------------
def test_submit_same_command_repeated(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, "run_sim.sh", count=100, auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    recs = js.jobs()
    assert len(recs) == 100
    assert len({r.job_id for r in recs}) == 100       # 각자 개별 job
    assert js.summary["PEND"] == 100


def test_submit_single_command_without_count(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, "lone.sh", auto_poll=False)   # 단일 job 취급
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    assert len(js.jobs()) == 1


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# §3.3 스냅샷 조회 계약
# ----------------------------------------------------------------------
def test_snapshot_queries_do_not_call_lsf(qtbot, manager, fake_lsf):
    js = submit_cmds(manager, [f"r {i}" for i in range(5)],
                        auto_poll=False)
    with qtbot.waitSignal(js.submit_finished, timeout=10000):
        pass
    fake_lsf.calls.clear()
    _ = js.summary
    _ = js.is_done
    _ = js.failed_jobs
    _ = js.jobs(states={JobState.PEND})
    _ = js.id
    assert fake_lsf.calls == []                       # LSF 호출 없음


# ----------------------------------------------------------------------
# 신호 카탈로그 드리프트 — 문서에 없는 신호 / 신호 없는 문서 금지
# ----------------------------------------------------------------------
def _signal_names(cls):
    from lsfmgr.manager import LsfJobManager as _M
    sig_t = type(vars(_M)["submit_started"])
    return {k for k, v in vars(cls).items()
            if isinstance(v, sig_t) and not k.startswith("_")}


def _doc_text(*names):
    import pathlib
    root = pathlib.Path(__file__).resolve().parent.parent
    return "\n".join((root / n).read_text(encoding="utf-8") for n in names)


def test_every_signal_is_documented():
    """신호를 추가하고 문서를 빼먹으면 앱은 그 신호의 존재를 모른다.
    (반대 방향 — 문서에만 있고 코드엔 없는 신호 — 은 실제로 있었다:
     js.submit_started가 문서에만 있어 구독하면 AttributeError였다.)"""
    from lsfmgr.handle import JobSet
    from lsfmgr.manager import LsfJobManager

    docs = _doc_text("README.md", "docs/gui.md")
    missing = sorted(n for n in _signal_names(LsfJobManager) | _signal_names(JobSet)
                     if f"`{n}`" not in docs)
    assert not missing, f"문서에 없는 신호: {missing}"


def test_documented_signals_exist():
    """README 신호표에 적힌 이름이 실제로 신호로 존재하는가."""
    import re

    from lsfmgr.handle import JobSet
    from lsfmgr.manager import LsfJobManager

    have = _signal_names(LsfJobManager) | _signal_names(JobSet)
    text = _doc_text("README.md")
    # 신호표 행: | `이름` | ... — 표 안의 백틱 이름만 후보로 본다
    rows = re.findall(r"^\|\s*`([a-z_]+)`\s*\|", text, re.M)
    # 신호처럼 생긴 것만(옵션/메서드 표와 섞이지 않게) — 코드에 있는 이름 기준
    suspects = {r for r in rows
                if r.endswith(("_started", "_finished", "_updated",
                               "_progress", "_occurred", "_failed", "_lost"))}
    ghosts = sorted(suspects - have)
    assert not ghosts, f"문서에만 있고 코드엔 없는 신호: {ghosts}"


# ----------------------------------------------------------------------
# 옵션 카탈로그 드리프트 — 신호 카탈로그와 같은 계약
# ----------------------------------------------------------------------
def _option_rows():
    """README §3.1 옵션 표의 이름들. `a` / `b` 처럼 묶인 행도 푼다."""
    import re

    text = _doc_text("README.md")
    start = text.index("### 3.1 옵션 카탈로그")
    end = text.index("## 4. 제출", start)
    names = set()
    for line in text[start:end].splitlines():
        m = re.match(r"^\|\s*(`[a-z_][a-z0-9_]*`(?:\s*/\s*`[a-z_][a-z0-9_]*`)*)\s*\|",
                     line)
        if m:
            names |= set(re.findall(r"`([a-z_][a-z0-9_]*)`", m.group(1)))
    return names


def test_every_option_is_in_the_catalog():
    """옵션을 추가하고 카탈로그를 빼먹으면 앱은 그 노브의 존재를 모른다.
    (kill_timeout_s가 실제로 빠져 있었고, 실환경에서 오설정으로 이어졌다)"""
    from dataclasses import fields

    from lsfmgr.config import LsfConfig
    from lsfmgr.options import MANAGER_ONLY_KEYS, SHARED_KEYS

    documented = _option_rows()
    # 카탈로그에 일부러 안 싣는 것: retry_delay_s는 retry_backoff("fixed:N")로
    # 표현되고, test_submit_wrapper_pattern_cmd는 §4.3에 전용 절이 있다.
    BY_DESIGN = {"retry_delay_s", "retry_backoff_num", "job_status_fetcher",
                 "test_submit_wrapper_pattern_cmd"}
    keys = set(SHARED_KEYS | MANAGER_ONLY_KEYS
               | {f.name for f in fields(LsfConfig)}) - BY_DESIGN
    missing = sorted(k for k in keys if k not in documented)
    assert not missing, f"카탈로그에 없는 옵션: {missing}"


def test_catalog_has_no_ghost_options():
    """제거된 옵션의 행이 표에 남아 있으면 앱이 계속 그 값을 넘긴다
    (rate_limit_per_s 삭제 때 실제로 남을 뻔했다)."""
    from dataclasses import fields

    from lsfmgr.config import LsfConfig
    from lsfmgr.options import MANAGER_ONLY_KEYS, SHARED_KEYS

    known = (SHARED_KEYS | MANAGER_ONLY_KEYS
             | {f.name for f in fields(LsfConfig)})
    ghosts = sorted(n for n in _option_rows() if n not in known)
    assert not ghosts, f"문서에만 있고 코드엔 없는 옵션: {ghosts}"
