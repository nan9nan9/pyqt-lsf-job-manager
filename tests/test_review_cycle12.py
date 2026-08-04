"""전체 정독 리뷰 사이클 12에서 확정된 회귀 테스트.

미정독 영역(pacer·handlers 서비스·lifecycle·util·store/base·config) 각도.

- C12-1: LsfConfig가 query_timeout_s/kill_timeout_s를 검증하지 않아 0/음수가
  통과했다. 두 값은 manager kwarg가 없어 LsfConfig로만 줄 수 있는데(검증
  계층이 거기뿐), subprocess.run(timeout<=0)은 매번 TimeoutExpired다.
  query 쪽 증상이 특히 조용하다 — 전 chunk가 '조회 실패'로 귀속되고 monitor는
  설계대로 판단을 보류(LOST 확정 안 함)하므로, 폴링이 도는데도 상태가 영영
  안 올라가고 job이 PEND에 고착된다. poll_interval_s/submit_timeout_s는 이미
  같은 이유로 __post_init__에서 막고 있었다(사이클 8) — 나머지 둘도 맞춘다.
"""
from __future__ import annotations

import subprocess

import pytest

from lsfmgr import LsfConfig
from lsfmgr.command import LsfCommand


@pytest.mark.parametrize("name", ["poll_interval_s", "submit_timeout_s",
                                  "query_timeout_s", "kill_timeout_s"])
@pytest.mark.parametrize("bad", [0, -1, 0.0, None])
def test_nonpositive_timeout_rejected(name, bad):
    """주기/타임아웃 4형제는 전부 양수만 받는다 (검증 계층 = 여기 한 곳)."""
    with pytest.raises(ValueError) as ei:
        LsfConfig(**{name: bad})
    assert name in str(ei.value)


def test_positive_timeouts_still_accepted():
    """정당한 저수준 값은 그대로 통과한다 — 범위 정책은 options 계층의 몫."""
    cfg = LsfConfig(poll_interval_s=2, submit_timeout_s=1,
                    query_timeout_s=5, kill_timeout_s=0.5)
    assert (cfg.poll_interval_s, cfg.query_timeout_s) == (2.0, 5.0)
    assert isinstance(cfg.submit_timeout_s, float)      # 정규화도 함께


def test_zero_query_timeout_would_stall_polling_silently():
    """C12-1의 증상 고정 — 검증이 없었다면 전 job이 '조회 실패'로 귀속돼
    monitor가 판단을 보류하고(=LOST도 아님) 상태가 영영 안 올라간다.
    검증이 그 상태에 도달하는 것 자체를 막는지 확인한다."""
    def runner(argv, timeout, cwd=None):
        raise subprocess.TimeoutExpired(argv, timeout)

    # 검증을 우회해 그 상황을 재현하면 실제로 전원 실패로 귀속된다
    cfg = LsfConfig()
    object.__setattr__(cfg, "query_timeout_s", 0)
    out, failed = LsfCommand(cfg, runner).bjobs_by_ids([101, 102])
    assert out == [] and failed == {101, 102}     # 조회 결과 0, 전원 보류

    # 그리고 정상 경로에서는 그 값이 애초에 만들어지지 않는다
    with pytest.raises(ValueError):
        LsfConfig(query_timeout_s=0)


# ----------------------------------------------------------------------
# C12-2: 소멸한 jobset의 LOST 미발견 스트릭이 영구 잔존했다.
#
# JobsetQuerier._missing_streak은 "사이클마다 갈아끼우니 정리 불필요"였는데,
# 그 논리는 **살아있는 jobset 안의 job_key**에만 성립한다. jobset 자체가
# 사라지면(remove_jobset) 그 id로는 다시 query()가 안 불려 _pop_streaks가
# 영영 실행되지 않는다. 장시간 도는 GUI에서 jobset을 지웠다 만들었다 하면
# 누적된다. pacer.forget()·_idle_counts 정리와 같은 격으로 소멸 지점에
# 정리 훅을 건다.
# ----------------------------------------------------------------------
def _streaks(mgr):
    return mgr.querier._missing_streak


def test_removed_jobset_leaves_no_streak(qtbot, manager, fake_lsf):
    """삭제된 jobset의 스트릭 항목이 남지 않는다."""
    from tests.conftest import submit_cmds
    src = submit_cmds(manager, ["echo a"], auto_poll=False)
    tgt = submit_cmds(manager, ["echo b"], auto_poll=False)
    qtbot.waitUntil(lambda: all(j.job_id for j in src.jobs() + tgt.jobs()),
                    timeout=10000)
    for j in list(fake_lsf.jobs.values()):      # bjobs에서 사라지게
        j.vanished = True
    manager.querier.query(src.id)
    manager.querier.query(tgt.id)
    assert src.id in _streaks(manager) and tgt.id in _streaks(manager)

    manager.remove_jobset(src, force=True)      # jobset 소멸
    assert src.id not in _streaks(manager)      # 누수 없음
    assert tgt.id in _streaks(manager)          # 살아있는 쪽은 보존


def test_remove_job_keeps_other_jobs_grace(qtbot, manager, fake_lsf):
    """일부 job만 삭제할 때 **남은 job의 유예는 리셋하지 않는다** —
    통째로 지우면 LOST 확정이 처음부터 다시 세어져 그만큼 늦어진다."""
    from tests.conftest import submit_cmds
    js = submit_cmds(manager, ["echo a", "echo b"], auto_poll=False)
    qtbot.waitUntil(lambda: all(j.job_id for j in js.jobs()), timeout=10000)
    for j in list(fake_lsf.jobs.values()):
        j.vanished = True
    manager.querier.query(js.id)
    gone, kept = sorted(r.job_key for r in js.jobs())
    assert _streaks(manager)[js.id][kept] == 1

    manager.remove_job(js, job_key=gone, force=True)
    assert gone not in _streaks(manager)[js.id]     # 삭제분만 사라지고
    assert _streaks(manager)[js.id][kept] == 1      # 남은 job 유예는 그대로


