"""모듈 계층 — 런타임 import 순환 금지.

순환이 생기면 그걸 우회하는 코드가 따라 붙는다. 실제로 그랬다:
JobStatus가 command.py에 있어서 config→internal_status→command→config 순환이
생겼고, internal_status는 매 파싱마다 쓰는 클래스를 **함수 안에서 지연
import + 전역 캐시**하는 우회를 달고 있었다. 타입을 제 자리(states)로 옮기니
순환도 우회 코드도 함께 사라졌다.

TYPE_CHECKING 블록의 import는 런타임에 실행되지 않으므로 순환으로 세지 않는다
(handle→manager가 그 경우다).
"""
from __future__ import annotations

import ast
import pathlib


def _runtime_deps():
    """모듈 → 런타임에 실제로 import하는 같은 패키지 모듈."""
    root = pathlib.Path(__file__).resolve().parent.parent / "lsfmgr"
    out = {}
    for f in sorted(root.rglob("*.py")):
        name = (str(f.relative_to(root))[:-3]
                .replace("/", ".").replace(".__init__", "") or "__init__")
        tree = ast.parse(f.read_text(encoding="utf-8"))
        type_only = {
            id(n)
            for blk in ast.walk(tree)
            if isinstance(blk, ast.If) and "TYPE_CHECKING" in ast.dump(blk.test)
            for n in ast.walk(blk)
        }
        out[name] = {
            (n.module or "").split(".")[0]
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom) and n.level
            and id(n) not in type_only and n.module
        } - {name}
    return out


def _find_cycles(graph):
    found, stack, seen = [], [], set()

    def visit(node):
        if node in stack:
            found.append(stack[stack.index(node):] + [node])
            return
        if node in seen:
            return
        seen.add(node)
        stack.append(node)
        for nxt in sorted(graph.get(node, ())):
            if nxt in graph:
                visit(nxt)
        stack.pop()

    for node in sorted(graph):
        visit(node)
    return found


def test_no_runtime_import_cycles():
    cycles = _find_cycles(_runtime_deps())
    assert not cycles, "런타임 import 순환:\n" + "\n".join(
        " → ".join(c) for c in cycles)


def test_leaf_modules_stay_leaves():
    """설정·상태·Qt 어댑터는 아무것도 import하지 않는다 — 여기에 의존이
    붙는 순간 그 아래 계층 전부가 순환 후보가 된다."""
    deps = _runtime_deps()
    for leaf in ("states", "errors", "qt", "reports", "util", "config"):
        assert not deps[leaf], f"{leaf}가 {sorted(deps[leaf])}에 의존한다"


def test_job_status_lives_with_job_record():
    """관측값(JobStatus)과 상태(JobRecord)는 짝이라 같은 모듈에 있어야 한다.
    떨어뜨리면 조회원 두 곳(bjobs/콜백)이 command를 거치게 되어 순환이 난다."""
    from lsfmgr import states
    from lsfmgr.command import JobStatus as via_command

    assert states.JobStatus is via_command      # 재수출 경로 유지
    assert via_command.__module__.endswith("states")
