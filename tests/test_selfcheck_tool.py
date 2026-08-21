"""tools/lsf_selfcheck.py가 실행 가능한 상태로 유지된다.

실환경에서 처음 붙일 때 쓰는 도구인데, 라이브러리가 바뀌어도 이 스크립트는
어떤 테스트도 건드리지 않아 조용히 썩는다(import 하나만 어긋나도 사용자의
첫 시도가 실패한다). 가짜 bjobs/bkill을 PATH에 올려 실제로 돌려 본다.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

TOOL = Path(__file__).resolve().parents[1] / "tools" / "lsf_selfcheck.py"

BJOBS = """#!/usr/bin/env bash
# -o 로 요청한 필드 수만큼 돌려주는 최소한의 흉내
fields=""
next=0
for a in "$@"; do
    if [ $next = 1 ]; then fields="$a"; next=0; fi
    if [ "$a" = "-o" ]; then next=1; fi
done
n=$(echo "$fields" | wc -w)
row=""
for f in $fields; do
    case "$f" in
        jobid) v=1000;; stat) v=RUN;; run_time) v="120 second(s)";;
        start_time) v="Aug 20 10:00:00 2026";; delimiter=*) continue;;
        *) v="";;
    esac
    row="$row$v;"
done
echo "$row"
"""

BKILL = """#!/usr/bin/env bash
echo "Job <1000> is being terminated"
"""


@pytest.fixture
def fake_path(tmp_path):
    b = tmp_path / "bin"
    b.mkdir()
    for name, body in (("bjobs", BJOBS), ("bkill", BKILL)):
        f = b / name
        f.write_text(body)
        f.chmod(0o755)
    env = dict(os.environ, PATH=f"{b}{os.pathsep}{os.environ['PATH']}")
    return env


def test_selfcheck_runs_and_reports(fake_path):
    r = subprocess.run([sys.executable, str(TOOL), "--job", "1000"],
                       capture_output=True, text=True, timeout=120,
                       env=fake_path)
    out = r.stdout + r.stderr
    assert "자가 점검" in out, out[:800]
    assert "결과:" in out, out[-800:]
    # 판정을 실제로 하고 있는지 — OK/FAIL 라인이 나와야 한다
    assert "[  OK ]" in out or "[ FAIL]" in out, out[:800]


def test_selfcheck_help_works():
    r = subprocess.run([sys.executable, str(TOOL), "--help"],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0
    assert "--kill" in r.stdout and "--bench" in r.stdout


@pytest.mark.parametrize("payload,expect_fail,needle", [
    ({"jobs": [{"dataId": "1.c1", "stat": "RUN"},
               {"dataId": "2.c1", "stat": "DONE",
                "endTime": "2026-08-20T11:00:00Z"}]},
     False, "상태 표기 전건 인식"),
    ({"jobs": [{"dataId": "1.c1", "stat": "Bogus"}]},
     True, "상태를 못 알아본"),
    ({"data": [{"jobNumber": 1, "stat": "RUN"}]},
     True, "id 필드를 못 찾음"),
])
def test_payload_mode_diagnoses(tmp_path, payload, expect_fail, needle):
    """콜백 조회원(REST)을 쓰는 앱에는 --payload가 유일한 실환경 점검이다 —
    bjobs 출력 가정은 애초에 타지 않는다."""
    import json

    f = tmp_path / "resp.json"
    f.write_text(json.dumps(payload), encoding="utf-8")
    r = subprocess.run([sys.executable, str(TOOL), "--payload", str(f)],
                       capture_output=True, text=True, timeout=120)
    out = r.stdout + r.stderr
    assert needle in out, out
    assert (r.returncode != 0) is expect_fail, out
