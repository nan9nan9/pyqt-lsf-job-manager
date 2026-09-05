"""예제도 MockLSF가 정지된 뒤에만 실행 디렉토리를 지운다."""
import importlib.util
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("stop_result", [0, 1, "timeout"])
def test_example_cleanup_requires_daemon_stop(monkeypatch, tmp_path, stop_result):
    # 예제 import의 자동 초기화는 끄고, 검사할 정리 함수를 직접 등록한다.
    monkeypatch.setenv("LSFMGR_REAL", "1")
    monkeypatch.setenv("MOCKLSF_HOME", str(tmp_path))
    path = Path(__file__).resolve().parents[1] / "examples" / "common.py"
    spec = importlib.util.spec_from_file_location("example_common", path)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    callbacks, removed = [], []
    monkeypatch.setattr(common.atexit, "register", callbacks.append)
    monkeypatch.setattr(common.tempfile, "mkdtemp", lambda **kw: str(tmp_path))
    monkeypatch.setattr(common.shutil, "rmtree", lambda path, **kw: removed.append(path))

    def stop(argv, **kw):
        if stop_result == "timeout":
            raise subprocess.TimeoutExpired(argv, kw["timeout"])
        if stop_result and kw.get("check"):
            raise subprocess.CalledProcessError(stop_result, argv)
        return subprocess.CompletedProcess(argv, stop_result)

    monkeypatch.setattr(common.subprocess, "run", stop)
    common._init_mocklsf_home()
    callbacks[0]()
    assert removed == ([str(tmp_path)] if stop_result == 0 else [])
