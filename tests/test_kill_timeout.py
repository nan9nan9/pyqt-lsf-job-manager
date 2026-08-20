"""bkill이 시간 내 반환하지 않았을 때 — timeout은 '안 죽었다'가 아니다.

실환경 보고: MC 사이트에서 `bkill timeout — 재시도 대상:` 경고가 계속 나오는데
job은 실제로 죽어 있었다. 원인은 둘이 겹친 것:

  ① kill_timeout_s는 **bkill 호출 1회**(= chunk 전체)의 상한인데 chunk가
     조회용 chunk_size(500)를 함께 쓰고 있었다. MC forward job은 원격 왕복이
     있어 한 호출이 상한을 넘고, subprocess timeout이 bkill **클라이언트**를
     중간에 죽여 앞쪽 id만 죽고 뒤쪽은 요청조차 안 나간 채 잘린다.
  ② 그 chunk를 통째로 '미확인'으로 보고 **다시 bkill을 쐈다**. 이미 죽은 job에
     kill을 두 번 더 쏘면서 같은 경고가 라운드마다 반복된다.

여기서는 ②를 고정한다 — timeout 뒤에는 조회로 생사를 먼저 확인한다.
"""
from __future__ import annotations

import subprocess

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.command import LsfCommand
from tests.conftest import submit_cmds


class _TimeoutOnceBkill:
    """첫 bkill 호출만 timeout으로 만든다 — 단, job은 **실제로 죽인다**
    (mbatchd에는 접수됐는데 클라이언트만 잘린 상황 재현)."""

    def __init__(self, inner):
        self.inner = inner
        self.bkill_calls = 0

    def __call__(self, argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] == "bkill":
            self.bkill_calls += 1
            if self.bkill_calls == 1:
                self.inner(argv, timeout, cwd)          # 실제로 죽인다
                raise subprocess.TimeoutExpired(argv, timeout)
        return self.inner(argv, timeout, cwd)


def test_timeout_is_confirmed_by_query_not_by_reissuing_bkill(qtbot, fake_lsf):
    runner = _TimeoutOnceBkill(fake_lsf)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(rate_limit_per_s=None, kill_retry_delay_s=0.01),
        runner=runner)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, [f"mytool {i}.sp" for i in range(5)],
                             auto_poll=False)
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000) as blk:
            mgr.kill(js)
        rpt = blk.args[1]

        # 이미 죽은 job에 bkill을 다시 쏘지 않는다 — 1회로 끝난다
        assert runner.bkill_calls == 1, (
            f"bkill을 {runner.bkill_calls}회 쐈다 — timeout 뒤엔 조회로 "
            f"확인해야 한다")
        assert rpt.unconfirmed == 0, f"미확인 {rpt.unconfirmed}건으로 오보"
        assert not rpt.errors, rpt.errors
        # 죽은 것으로 확인됐으므로 optimistic 마킹도 정상 동작한다
        recs = js.jobs()
        assert all(r.state is JobState.EXIT and r.killed for r in recs), (
            [(r.job_key, r.state.value, r.killed) for r in recs])
    finally:
        mgr.shutdown()


def test_timeout_with_a_still_alive_job_keeps_retrying(qtbot, fake_lsf):
    """반대 방향 — 조회에서 아직 살아있으면 재시도는 그대로 일어나야 한다.
    (timeout을 무조건 '죽었다'로 접으면 진짜 미처리분이 묻힌다)"""
    class _AlwaysTimeout:
        def __init__(self, inner):
            self.inner = inner
            self.bkill_calls = 0

        def __call__(self, argv, timeout, cwd=None):
            if argv[0].rsplit("/", 1)[-1] == "bkill":
                self.bkill_calls += 1
                raise subprocess.TimeoutExpired(argv, timeout)   # 안 죽인다
            return self.inner(argv, timeout, cwd)

    runner = _AlwaysTimeout(fake_lsf)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(rate_limit_per_s=None, kill_retry_delay_s=0.01,
                         kill_max_retry=2),
        runner=runner)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["mytool a.sp"], auto_poll=False)
        with qtbot.waitSignal(mgr.kill_finished, timeout=20000) as blk:
            mgr.kill(js)
        rpt = blk.args[1]
        assert runner.bkill_calls == 3, (
            f"재시도가 {runner.bkill_calls}회 — 살아있으면 kill_max_retry까지 "
            f"다시 쏴야 한다")
        assert rpt.unconfirmed == 1 and rpt.errors
        assert js.jobs()[0].state is JobState.PEND   # 죽지 않았으니 그대로
    finally:
        mgr.shutdown()


def test_bkill_uses_its_own_chunk_size():
    """bkill chunk는 조회 chunk와 따로다 — 쓰기라 건당 비용이 훨씬 크다."""
    cfg = LsfConfig(chunk_size=500, kill_chunk_size=100)
    assert cfg.kill_chunk_size == 100 and cfg.chunk_size == 500


def test_tight_kill_budget_warns_once(caplog):
    """kill_timeout_s를 'job 1건'으로 오해한 설정을 생성 시 짚어 준다.
    (그 값은 chunk **전체**의 상한이라 chunk가 크면 예산이 순식간에 없다)"""
    with caplog.at_level("WARNING", logger="lsfmgr.command"):
        LsfCommand(LsfConfig(kill_timeout_s=8,
                             kill_chunk_size=500))       # 8s / 500건 = 16ms
    hits = [r for r in caplog.records if "호출 1회" in r.message]
    assert len(hits) == 1, [r.message for r in caplog.records]

    for cfg in (LsfConfig(),                             # 기본 120s / 16건
                LsfConfig(kill_timeout_s=8)):            # 8s / 16건 = 500ms
        caplog.clear()
        with caplog.at_level("WARNING", logger="lsfmgr.command"):
            LsfCommand(cfg)
        assert not [r for r in caplog.records if "호출 1회" in r.message], (
            f"멀쩡한 설정에 경고: timeout={cfg.kill_timeout_s} "
            f"chunk={cfg.kill_chunk_size}")


# ----------------------------------------------------------------------
# kill 실패는 상태를 바꾸지 않는다 — 그래서 전용 신호로 알린다
# ----------------------------------------------------------------------
def test_failed_kill_does_not_touch_the_records(qtbot, manager, fake_lsf):
    """kill이 실패했다면 그 job은 LSF에서 여전히 살아 있다 — EXIT로 찍으면
    거짓말이다. 상태·killed·fail_reason 어느 것도 건드리지 않는다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    fake_lsf.fail_next_bkill = 99                 # bkill이 계속 rc=255
    with qtbot.waitSignal(manager.kill_finished, timeout=20000) as blk:
        manager.kill(js)
    assert blk.args[1].unconfirmed == 1
    rec = js.jobs()[0]
    assert (rec.state, rec.killed, rec.fail_reason) == (
        JobState.PEND, False, None)


def test_failed_kill_is_reported_on_kill_failed(qtbot, manager,
                                                        fake_lsf):
    """상태에 흔적이 없으니 신호로 알려야 한다 — 없으면 사용자가 kill을
    눌렀는데 표도 그대로, 알림도 없는 완전 무반응이다."""
    seen, order = [], []
    manager.kill_failed.connect(
        lambda j, m: (seen.append((j, m)), order.append("error")))
    manager.kill_finished.connect(lambda j, r: order.append("finished"))

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    handle_seen = []
    js.kill_failed.connect(handle_seen.append)

    fake_lsf.fail_next_bkill = 99
    with qtbot.waitSignal(manager.kill_finished, timeout=20000):
        manager.kill(js)

    assert len(seen) == 1, seen
    assert seen[0][0] == js.id and "kill 확인 실패" in seen[0][1]
    assert handle_seen == [seen[0][1]], handle_seen      # 핸들로도 중계
    # finished-last — 실패 통지가 완료 통지보다 먼저
    assert order == ["error", "finished"], order


def test_successful_kill_is_silent_on_the_error_signal(qtbot, manager):
    """정상 kill에서는 안 나와야 한다 (신호 노이즈 금지)."""
    seen = []
    manager.kill_failed.connect(lambda j, m: seen.append(m))
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["mytool a.sp"], auto_poll=False)
    with qtbot.waitSignal(manager.kill_finished, timeout=20000):
        manager.kill(js)
    assert seen == []
    assert js.jobs()[0].state is JobState.EXIT


# ----------------------------------------------------------------------
# timeout 후 생사 판정 — 모호하면 '죽었다'로 접지 않는다
# ----------------------------------------------------------------------
def _task(mgr, rows):
    """조회 결과를 고정한 _KillTask — _confirm_by_query 단위 검증용."""
    from lsfmgr.killer import _KillTask
    mgr.command.bjobs_by_ids = lambda ids, fresh=False: (rows, set())
    return _KillTask(mgr.killer, jobset_id="")


@pytest.fixture
def bare_mgr(fake_lsf):
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(rate_limit_per_s=None),
                        runner=fake_lsf)
    yield mgr
    mgr.shutdown()


def _status(job_id, idx, state):
    from lsfmgr.command import JobStatus
    return JobStatus(job_id=job_id, array_index=idx, state=state,
                     exit_code=None)


def test_folded_row_cannot_judge_an_element_target(bare_mgr):
    """조회가 접힌 행(array_index=None)만 주면 element 생사는 알 수 없다 —
    '죽었다'로 접으면 살아있는 job이 재시도에서 빠져 kill을 빠져나간다."""
    task = _task(bare_mgr, [_status(1000, None, JobState.RUN)])
    gone, _calls = task._confirm_by_query({"1000[3]"})
    assert gone == set(), f"판정 불가를 해소로 접었다: {gone}"


def test_absent_id_counts_as_gone(bare_mgr):
    """LSF가 그 id를 아예 모른다 = 이미 끝났다 — 재시도할 이유가 없다."""
    task = _task(bare_mgr, [])
    assert task._confirm_by_query({"1000", "1000[3]"}) [0] == {"1000",
                                                               "1000[3]"}


def test_live_bare_target_keeps_pending(bare_mgr):
    task = _task(bare_mgr, [_status(1000, None, JobState.RUN)])
    assert task._confirm_by_query({"1000"})[0] == set()


def test_terminal_row_counts_as_gone(bare_mgr):
    task = _task(bare_mgr, [_status(1000, None, JobState.EXIT)])
    assert task._confirm_by_query({"1000"})[0] == {"1000"}


def test_element_target_is_judged_per_element(bare_mgr):
    """element 행이 제대로 오면 element 단위로 판정한다 — 형제가 살아 있어도
    내 element가 끝났으면 해소."""
    task = _task(bare_mgr, [_status(1000, 3, JobState.EXIT),
                            _status(1000, 4, JobState.RUN)])
    gone, _c = task._confirm_by_query({"1000[3]", "1000[4]"})
    assert gone == {"1000[3]"}, gone


def test_range_target_uses_the_syntax_owner(bare_mgr):
    """범위 target("id[m-n]")도 첫 element만 보고 접지 않는다."""
    task = _task(bare_mgr, [_status(1000, 2, JobState.EXIT),
                            _status(1000, 5, JobState.RUN)])
    assert task._confirm_by_query({"1000[2-5]"})[0] == set()   # 5가 살아있다
    task2 = _task(bare_mgr, [_status(1000, 2, JobState.EXIT),
                             _status(1000, 5, JobState.DONE)])
    assert task2._confirm_by_query({"1000[2-5]"})[0] == {"1000[2-5]"}


def test_query_failure_keeps_pending(bare_mgr):
    """조회가 터져도 '죽었다'로 접지 않는다 (조회 장애 ≠ job 없음)."""
    def boom(ids, fresh=False):
        raise RuntimeError("mbatchd unreachable")
    bare_mgr.command.bjobs_by_ids = boom
    from lsfmgr.killer import _KillTask
    task = _KillTask(bare_mgr.killer, jobset_id="")
    assert task._confirm_by_query({"1000"}) == (set(), 1)
