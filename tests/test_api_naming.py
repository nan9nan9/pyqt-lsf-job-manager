"""핸들과 manager가 같은 것을 다른 이름으로 부르지 않는다.

두 계층이 같은 개념에 다른 이름을 붙이면(mgr.submit_snapshot ↔ js.submit_state)
앱이 핸들에서 id 기반으로 옮겨갈 때 이름을 새로 찾아야 하고, 문서도 두 벌이
된다. 대부분의 쌍(summary/is_submitting/is_killing…)은 이미 같은 이름을 쓰므로
**어긋난 쪽이 결함**이다. 이름을 눈으로 맞추는 대신, 핸들 멤버가 위임하는
manager 메서드 이름과 자기 이름이 같은지 소스에서 확인한다.
"""
from __future__ import annotations

import inspect
import re

from lsfmgr.handle import JobSet
from lsfmgr.manager import LsfJobManager

# 이름이 달라도 되는 것 — 이유가 있는 경우만, 이유와 함께.
ALIASED = {
    "jobs": "get_jobs",          # 핸들은 jobset이 이미 정해져 있어 '이' jobset의 jobs
    "failed_jobs": "get_jobs",   # get_jobs(states=실패)의 축약
    "is_done": "summary",        # summary에서 파생
    "is_active": "summary",
}


def _members():
    for name, obj in vars(JobSet).items():
        if name.startswith("_"):
            continue
        fn = obj.fget if isinstance(obj, property) else obj
        if not (inspect.isfunction(fn) or inspect.ismethod(fn)):
            continue                         # Signal 등
        try:
            yield name, inspect.getsource(fn)
        except OSError:                      # pragma: no cover
            continue


def test_handle_members_use_the_manager_name():
    mismatched = []
    for name, src in _members():
        delegated = set(re.findall(r"self\._manager\.([a-z_][a-z0-9_]*)", src))
        if len(delegated) != 1:
            continue                         # 위임 없음 or 여러 개(파생) → 대상 아님
        target = delegated.pop()
        if target == name or ALIASED.get(name) == target:
            continue
        mismatched.append(f"js.{name} → mgr.{target} (같은 것의 다른 이름)")
    assert not mismatched, "\n".join(mismatched)


def test_aliases_still_point_at_real_methods():
    """예외 표가 낡으면 가드가 헐거워진다 — 대상이 실재하는지 확인."""
    for name, target in ALIASED.items():
        assert hasattr(JobSet, name), f"핸들에 없는 멤버가 예외 표에: {name}"
        assert hasattr(LsfJobManager, target), f"없는 manager 메서드: {target}"


def test_every_option_is_actually_read():
    """받아들이는 옵션은 반드시 읽는 곳이 있어야 한다.

    verify_kill이 그렇지 않았다: SHARED_KEYS에 있어 submit(verify_kill=True)가
    통과하고 Options 필드로도 남았지만, 정작 읽는 쪽은 manager의 _defaults
    뿐이라 **호출별 지정은 조용히 무시**됐다. 검증만 되고 아무도 안 읽는
    옵션은 앱 입장에서 '설정했는데 안 먹는' 함정이다.
    """
    import pathlib
    import re
    from dataclasses import fields

    from lsfmgr.options import Options, SHARED_KEYS

    src = "\n".join(p.read_text() for p in pathlib.Path("lsfmgr").rglob("*.py"))
    unread = [f.name for f in fields(Options)
              if not re.search(rf"\b(?:opts|options|self)\.{f.name}\b", src)]
    assert not unread, f"Options에 있으나 아무도 읽지 않는 필드: {unread}"

    # 호출별(③) 옵션은 전부 Options로 전달돼야 한다 — 아니면 읽을 방법이 없다
    names = {f.name for f in fields(Options)}
    orphan = sorted(SHARED_KEYS - names)
    assert not orphan, f"SHARED_KEYS에 있으나 Options로 전달되지 않는 키: {orphan}"


def test_every_public_member_is_documented():
    """공개 API가 README에 하나도 빠지지 않는다.

    문서에 없는 공개 멤버는 (a) 문서 누락이거나 (b) 실은 내부용인데 공개로
    노출된 것이다. 실제로 mgr.submit_snapshot/kill_snapshot은 (a)였고
    mgr.resolve_options는 (b)였다 — 둘 다 이 검사로 드러났다.
    """
    import pathlib

    readme = pathlib.Path("README.md").read_text()
    missing = []
    for cls in (LsfJobManager, JobSet):
        for name in vars(cls):
            if name.startswith("_") or name in readme:
                continue
            missing.append(f"{cls.__name__}.{name}")
    assert not missing, ("README에 없는 공개 멤버(문서 누락이거나 "
                         f"내부용인데 공개된 것): {sorted(missing)}")


def test_every_config_field_is_read():
    """LsfConfig 필드는 반드시 읽는 곳이 있어야 한다 — 안 읽히면 '설정했는데
    안 먹는' 함정이다(verify_kill이 정확히 그랬다)."""
    import pathlib
    import re
    from dataclasses import fields

    from lsfmgr.config import LsfConfig

    src = "\n".join(p.read_text() for p in pathlib.Path("lsfmgr").rglob("*.py"))
    unread = [f.name for f in fields(LsfConfig)
              if not re.search(rf"\b(?:config|cfg|self)\.{f.name}\b", src)]
    assert not unread, f"LsfConfig에 있으나 아무도 읽지 않는 필드: {unread}"
