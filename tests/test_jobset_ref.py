"""jobset 인자 규약 — 공개 API는 **핸들이든 jobset_id든** 받는다.

README/docs 예제는 전부 핸들을 넘긴다(`mgr.query_once(js)`). 어느 하나가
_jsid를 빠뜨리면 문서대로 쓴 앱이 그 API에서만 JobSetNotFoundError로
깨진다 — 실제로 mgr.jobset()이 그 상태였다.
"""
from __future__ import annotations

import ast
import pathlib


def test_handle_accepted_everywhere(qtbot, manager):
    js = manager.create_jobset(["mytool a.sp", "mytool b.sp"],
                               job_keys=["a", "b"])
    calls = [
        ("summary", lambda: manager.summary(js)),
        ("get_jobs", lambda: manager.get_jobs(js)),
        ("can_submit", lambda: manager.can_submit(js)),
        ("is_submitting", lambda: manager.is_submitting(js)),
        ("is_killing", lambda: manager.is_killing(js)),
        ("submit_snapshot", lambda: manager.submit_snapshot(js)),
        ("kill_snapshot", lambda: manager.kill_snapshot(js)),
        ("start_polling", lambda: manager.start_polling(js, 5.0)),
        ("query_once", lambda: manager.query_once(js)),
        ("stop_polling", lambda: manager.stop_polling(js)),
        ("cancel_submit", lambda: manager.cancel_submit(js)),
        ("set_user_data", lambda: manager.set_user_data(js, "a", {"x": 1})),
        ("add_handler", lambda: manager.add_handler(js, "h", lambda c: None)),
        ("remove_handler", lambda: manager.remove_handler(js, "h")),
        ("add_jobs", lambda: manager.add_jobs(js, ["mytool c.sp"],
                                              job_keys=["c"])),
        ("upsert_jobs", lambda: manager.upsert_jobs(js, ["mytool c2.sp"],
                                                    job_keys=["c"])),
        ("replace_jobs", lambda: manager.replace_jobs(js, ["mytool c3.sp"],
                                                      job_keys=["c"])),
        ("remove_jobs", lambda: manager.remove_jobs(js, ["c"])),
        ("detect_lost", lambda: manager.detect_lost(js)),
        ("clear_jobs", lambda: manager.clear_jobs(js)),
        ("jobset", lambda: manager.jobset(js)),
    ]
    bad = []
    for name, fn in calls:
        try:
            fn()
        except Exception as e:                # noqa: BLE001
            bad.append(f"{name}: {e!r}")
    print("\n핸들을 못 받는 API:")
    for b in bad:
        print("  " + b)
    assert not bad


def test_every_public_jobset_api_normalizes_its_argument():
    """새 공개 API가 jobset 인자를 받으면서 _jsid를 빠뜨리는 것을 잡는다.
    (위 런타임 테스트는 목록을 손으로 유지하므로 새 API를 놓친다)"""
    src = pathlib.Path("lsfmgr/manager.py").read_text(encoding="utf-8")
    cls = next(n for n in ast.parse(src).body
               if isinstance(n, ast.ClassDef) and n.name == "LsfJobManager")
    # 위임으로 정규화하는 것들 — 몸통에서 _jsid를 부른다
    delegating = {"submit", "add_jobs", "replace_jobs", "upsert_jobs",
                  "kill_jobs"}
    missing = []
    for fn in cls.body:
        if not isinstance(fn, ast.FunctionDef) or fn.name.startswith("_"):
            continue
        if fn.name in delegating:
            continue
        args = [a.arg for a in fn.args.args + fn.args.kwonlyargs]
        if not ({"jobset_id", "js"} & set(args)):
            continue
        if "_jsid" not in ast.dump(fn):
            missing.append(fn.name)
    assert not missing, (
        f"jobset 인자를 받는데 _jsid로 정규화하지 않는 공개 API: {missing}")
