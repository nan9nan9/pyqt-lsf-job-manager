"""희귀 입력 — 일반 경로가 안 닿는 값들.

command shlex 왕복(따옴표/유니코드/개행/빈 인자), ARG_MAX 경계, bjobs 출력
오염(필드 과다·부족·비수치 id), 시각 파싱 경계(윤년/극단 연도/타임존),
같은 job_key의 jobset 간 충돌, 빈 jobset, internal payload 희귀 형태.
"""
import pytest
from datetime import datetime

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand, chunk_args
from lsfmgr.internal_status import parse_internal_jobs, parse_time


# --- ① 커맨드 shlex 왕복 (레코드 저장 → 재제출 시 복원) ---
@pytest.mark.parametrize("argv", [
    ["tool", "-J", "a b"],                       # 공백 포함 인자
    ["tool", "--opt=x'y"],                       # 작은따옴표
    ['tool', '--opt="quoted"'],                  # 큰따옴표
    ["tool", "인자", "한글"],                     # 유니코드
    ["tool", "-c", "echo hi; echo bye"],         # 세미콜론
    ["tool", "-c", "line1\nline2"],              # 개행
    ["tool", "-e", "$HOME/x"],                   # 셸 메타
    ["tool", "-e", "a\\b"],                      # 역슬래시
    ["tool", ""],                                # 빈 인자
])
def test_command_roundtrip(qtbot, manager, monkeypatch, argv):
    js = manager.create_jobset([argv], job_keys=["k"])
    seen = []

    def run_submit(tokens, timeout_s=None, cwd=None):
        seen.append(list(tokens))
        return 1234

    monkeypatch.setattr(manager.command, "run_submit", run_submit)
    with qtbot.waitSignal(manager.submit_finished, timeout=3000):
        manager.submit(js, auto_poll=False)
    assert seen == [argv]


# --- ② ARG_MAX 경계 ---
def test_arg_max_single_item_too_long():
    huge = "x" * 500
    with pytest.raises(Exception) as ei:
        list(chunk_args([huge], 100, arg_max=100, base_len=10))
    assert "ARG_MAX" in str(ei.value)


def test_arg_max_splits_before_overflow():
    items = ["y" * 40 for _ in range(10)]
    chunks = list(chunk_args(items, 100, arg_max=200, base_len=10))
    for c in chunks:
        assert 10 + sum(len(i) + 1 for i in c) <= 200, c
    assert sum(len(c) for c in chunks) == 10


# --- ③ bjobs 출력에 구분자가 섞였을 때 ---
@pytest.mark.parametrize("line,desc", [
    ("1000;RUN;-", "정상 CORE"),
    ("1000;RUN;-;120 second(s);2026-08-21 10:00:00;-", "정상 FULL"),
    ("1000;RUN;-;extra;fields;here;too;many;more", "필드 과다"),
    ("1000;RUN", "필드 부족"),
    (";;", "빈 필드"),
    ("notanid;RUN;-", "비수치 id"),
    ("1000[3];EXIT;130", "array element"),
    ("1000;WEIRDSTATE;-", "알 수 없는 상태"),
])
def test_bjobs_parse_never_raises(line, desc):
    out = LsfCommand._parse_bjobs(line + "\n")             # 예외 없이 행만 버려야 한다
    assert isinstance(out, list), desc


# --- ④ 시각 파싱 경계 ---
@pytest.mark.parametrize("text", [
    "2026-02-29T00:00:00",      # 존재하지 않는 날짜(2026는 평년)
    "2024-02-29T00:00:00",      # 윤년 — 유효
    "2026-12-31T23:59:59",
    "0000-01-01T00:00:00",
    "9999-12-31T23:59:59",
    "2026-08-21T10:00:00.123456789",
    "2026-08-21T10:00:00+14:00",
    "2026-08-21T10:00:00-12:00",
    "1755782400000",            # 밀리초 epoch
    "  2026-08-21 10:00:00  ",  # 공백
])
def test_parse_time_never_raises(text):
    parse_time(text)            # None이어도 되지만 예외는 안 된다


# --- ⑤ 같은 job_key가 여러 jobset에 있을 때 전역 kill ---
def test_same_key_in_two_jobsets(qtbot, manager, fake_lsf):
    from tests.conftest import submit_cmds
    a = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    qtbot.waitSignal(manager.submit_finished, timeout=10000)
    qtbot.wait(200)
    b = manager.create_jobset(["mytool b.sp"], job_keys=[a.jobs()[0].job_key])
    manager.submit(b, auto_poll=False)
    qtbot.wait(400)
    key = a.jobs()[0].job_key
    found = manager.store.find_jobs_by_keys({key})
    print(f"\n같은 key '{key}'를 가진 레코드: {len(found)}건 "
          f"({sorted(r.jobset_id[-4:] for r in found)})")
    assert len(found) == 2, "전역 검색이 하나를 삼켰다"


# --- ⑥ 빈 jobset / 1건 jobset ---
def test_degenerate_sizes(qtbot, manager):
    empty = manager.create_jobset([], job_keys=[])
    assert manager.summary(empty) == {"total": 0}
    assert not manager.can_submit(empty)
    with pytest.raises(Exception):
        manager.submit(empty, auto_poll=False)


# --- ⑦ internal payload 희귀 형태 ---
@pytest.mark.parametrize("payload,desc", [
    ({"jobs": [{"dataId": "1[0].c"}]}, "array index 0 (falsy)"),
    ({"jobs": [{"dataId": "0"}]}, "job_id 0"),
    ({"jobs": [{"dataId": "1", "stat": ""}]}, "빈 stat"),
    ({"jobs": [{"dataId": "1", "exitStatus": "not-a-number"}]}, "비수치 exit"),
    ({"jobs": [{"dataId": "1", "startTime": "쓰레기"}]}, "깨진 시각"),
    ({"jobs": [{"dataId": " 1 "}]}, "공백 낀 id"),
])
def test_internal_payload_edges(payload, desc):
    out = parse_internal_jobs(payload)
    print(f"  {desc:22s} → {len(out)}건 " +
          (f"job_id={out[0].job_id} idx={out[0].array_index}" if out else ""))
