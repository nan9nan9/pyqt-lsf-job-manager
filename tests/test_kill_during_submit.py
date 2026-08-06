"""대량 제출 중 kill — 제출 중(SUBMITTING)인 job이 kill을 빠져나가지 않는다.

live 결함(사용자 리포트): 200건 이상을 제출하는 도중 표의 전 행을 선택해
kill했더니 "제출 중"이던 job은 죽지 않고, 그대로 제출을 마쳐 PEND→RUN으로
살아났다 — 다 죽을 때까지 kill을 여러 번 눌러야 했다.

원인: killer의 job_key→id 해석(`_resolve_keys`)은 `job_id is not None`인
레코드만 target으로 잡는다. 제출 중인 job은 아직 id가 없어 **조용히** 대상에서
빠지고, bkill이 나가지 않는다. 전체 kill(mgr.kill)은 SubmitGate barrier로
제출을 먼저 정지시켜(quiesce) 이 창이 없었지만, 선택 kill은 기본적으로 제출을
건드리지 않아 그대로 유출됐다.

수정: 선택 kill도 **대상 job만** 제출을 취소하고 정지를 기다린다
(JobCancelScope) — 미착수분은 CREATED로 되돌리고, 이미 wrapper가 도는 분은
끝나 job_id가 잡힌 뒤 죽인다. 대상 아닌 job의 제출은 계속된다(그걸 통째로
멈추는 것은 여전히 cancel_submit=True의 opt-in).
"""
from __future__ import annotations

import time

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf

N = 200


class SlowSubmitLsf(FakeLsf):
    """제출 wrapper가 느린 클러스터 — '제출 중' 창을 실제 규모로 넓힌다."""

    def __init__(self, delay: float = 0.3):
        super().__init__()
        self.delay = delay

    def __call__(self, argv, timeout, cwd=None):
        cmd = list(argv)[0].rsplit("/", 1)[-1]
        if cmd not in ("bjobs", "bkill", "tcsh", "lsid"):
            time.sleep(self.delay)           # 제출만 느리게 (조회/kill은 즉시)
        return super().__call__(argv, timeout, cwd=cwd)


def _mgr(fake):
    return LsfJobManager(store=InMemoryStore(),
                         config=LsfConfig(retry_delay_s=0.05), runner=fake)


def _jobset(mgr, keys):
    return mgr.create_jobset([f"customwrapper_sub {k}.sp" for k in keys],
                             job_keys=list(keys))


def test_selection_kill_catches_submitting_jobs(qtbot):
    """선택 kill: 제출 중이던 대상도 LSF에 살아남지 않는다."""
    fake = SlowSubmitLsf()
    mgr = _mgr(fake)
    try:
        keys = [f"k{i}" for i in range(N)]
        js = _jobset(mgr, keys)
        done = []
        mgr.submit_finished.connect(lambda j, r: done.append(r))
        mgr.submit(js, auto_poll=False)
        qtbot.waitUntil(                      # 제출이 한창일 때 kill
            lambda: any(r.state is JobState.SUBMITTING
                        for r in mgr.get_jobs(js.id)), timeout=10000)
        submitting = [r for r in mgr.get_jobs(js.id)
                      if r.state is JobState.SUBMITTING]
        assert len(submitting) > 1, "제출 중 상태를 못 잡았다 — 시나리오 무효"

        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill_jobs(js, keys, verify=True)
        qtbot.waitUntil(lambda: bool(done), timeout=60000)   # 사이클 종료까지

        assert not fake.alive_jobs(), (
            f"kill 후 LSF 잔존 {len(fake.alive_jobs())}건 — 제출 중이던 job이 "
            "kill을 빠져나갔다")
        # 레코드도 활성으로 남지 않는다 (제출분 EXIT / 미착수분 CREATED)
        assert not [r for r in mgr.get_jobs(js.id)
                    if r.state.is_on_lsf or r.state is JobState.SUBMITTING]
    finally:
        mgr.shutdown()


def test_selection_kill_leaves_other_jobs_submitting(qtbot):
    """선택 kill은 **대상 아닌** job의 제출까지 멈추지는 않는다.

    (그것까지 멈추는 것은 cancel_submit=True의 역할 — 기본값이 jobset 전체를
    멈추면 행 하나를 kill한 사용자가 제출 전체를 잃는다.)"""
    fake = SlowSubmitLsf(delay=0.1)
    mgr = _mgr(fake)
    try:
        keys = [f"k{i}" for i in range(40)]
        victims, survivors = keys[:20], keys[20:]
        js = _jobset(mgr, keys)
        done = []
        mgr.submit_finished.connect(lambda j, r: done.append(r))
        mgr.submit(js, auto_poll=False)
        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill_jobs(js, victims, verify=True)
        qtbot.waitUntil(lambda: bool(done), timeout=60000)

        by_key = {r.job_key: r for r in mgr.get_jobs(js.id)}
        assert all(by_key[k].state is not JobState.PEND for k in victims)
        # 대상 아닌 job은 제출을 마치고 LSF에 남는다
        assert [k for k in survivors if by_key[k].state is JobState.PEND], \
            "선택 kill이 대상 아닌 job의 제출까지 멈췄다"
    finally:
        mgr.shutdown()


def test_selection_kill_aborts_only_target_retries(qtbot):
    """선택 kill은 대상의 재시도 대기만 포기시킨다 — 남은 job은 재시도로 제출."""
    fake = FakeLsf()
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(retry_delay_s=0.3), runner=fake)
    try:
        keys = ["a", "b"]
        js = _jobset(mgr, keys)
        fake.fail_next_bsub = 2               # 둘 다 1회 실패 → RETRY_WAIT
        done = []
        mgr.submit_finished.connect(lambda j, r: done.append(r))
        mgr.submit(js, auto_poll=False)
        qtbot.waitUntil(
            lambda: all(r.state is JobState.RETRY_WAIT
                        for r in mgr.get_jobs(js.id)), timeout=10000)

        with qtbot.waitSignal(mgr.kill_finished, timeout=30000):
            mgr.kill_jobs(js, ["a"], verify=True)
        qtbot.waitUntil(lambda: bool(done), timeout=30000)

        by_key = {r.job_key: r for r in mgr.get_jobs(js.id)}
        assert by_key["a"].state is JobState.SUBMIT_FAILED  # 재시도 포기
        assert by_key["b"].state is JobState.PEND           # 재시도 성공
    finally:
        mgr.shutdown()


def test_full_kill_catches_submitting_jobs(qtbot):
    """전체 kill(barrier 경로)도 같은 보장 — 회귀 방지용 대칭 케이스."""
    fake = SlowSubmitLsf()
    mgr = _mgr(fake)
    try:
        keys = [f"k{i}" for i in range(N)]
        js = _jobset(mgr, keys)
        done = []
        mgr.submit_finished.connect(lambda j, r: done.append(r))
        mgr.submit(js, auto_poll=False)
        qtbot.waitUntil(
            lambda: any(r.state is JobState.SUBMITTING
                        for r in mgr.get_jobs(js.id)), timeout=10000)
        with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
            mgr.kill(js, verify=True)
        qtbot.waitUntil(lambda: bool(done), timeout=60000)
        assert not fake.alive_jobs()
    finally:
        mgr.shutdown()
