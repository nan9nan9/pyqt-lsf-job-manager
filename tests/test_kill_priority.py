"""kill 우선권 — submit 진행 중 kill이 와도 유출/스킵이 없어야 한다.

정책:
  - kill 시점에 아직 제출 안 된 job(SUBMITTING, worker 미착수) → submit 취소,
    CREATED 복귀 (제출 자체를 안 함)
  - 이미 bsub에 들어간 job → killer가 submit 정지(quiesce)를 기다린 뒤
    job_id로 kill (SUBMITTING이 is_on_lsf 스냅샷에서 스킵돼 몇 초 뒤 PEND로
    살아남던 구멍을 막는다)
"""
from __future__ import annotations

import threading

from lsfmgr import InMemoryStore, LsfJobManager
from lsfmgr.options import Options
from tests.conftest import mk_jobset, submit_cmds
from lsfmgr.states import JobState


_LSF_QUERY_CMDS = {"bjobs", "bkill", "bhist", "bmod", "tcsh", "lsid"}


class GatedBsub:
    """제출(wrapper) 호출만 gate에서 블록하는 runner 래퍼 — 'submit 진행 중'을
    결정적으로 재현한다. FakeLsf lock 밖에서 대기하므로 bjobs/bkill은 병행
    진행된다. (v10: 제출은 임의 wrapper 프로그램 — LSF 조회/kill 명령이 아닌
    호출을 전부 제출로 본다)"""

    def __init__(self, fake):
        self.fake = fake
        self.gate = threading.Event()        # set 전까지 제출 블록
        self.entered = threading.Event()     # 첫 제출 진입 통지

    def __call__(self, argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] not in _LSF_QUERY_CMDS:
            self.entered.set()
            self.gate.wait(10)
        return self.fake(argv, timeout)


def test_kill_during_submit_cancels_unsubmitted_and_kills_submitted(
        qtbot, config, fake_lsf):
    """핵심 시나리오: worker 1개가 bsub 진행 중 + 2개 미착수일 때 kill —
    진행 중이던 1개는 제출 완료 후 kill(EXIT), 미착수 2개는 CREATED 복귀."""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = submit_cmds(mgr, ["echo 1", "echo 2", "echo 3"],
                        workers=1, auto_poll=False)
        assert runner.entered.wait(5)        # worker가 bsub 진입 (제출 중)

        with qtbot.waitSignals([mgr.submit_finished, mgr.kill_finished],
                               timeout=15000):
            mgr.kill(js.id)           # kill 우선권 발동
            runner.gate.set()                # 진행 중이던 bsub 완료 허용

        states = sorted(r.state.name for r in js.jobs())
        assert states == ["CREATED", "CREATED", "EXIT"], states
        killed = [r for r in js.jobs() if r.state is JobState.EXIT]
        assert killed[0].fail_reason == "KILLED"
        assert killed[0].job_id is not None  # 제출돼 버린 그 job이 kill됨
        assert len(fake_lsf.jobs) == 1       # LSF에 실제 제출된 건 1개뿐
        assert fake_lsf.alive_jobs() == []   # 그리고 그것도 죽었다 (유출 0)
    finally:
        mgr.shutdown()


def test_kill_during_submit_invariant_no_survivors(qtbot, manager, fake_lsf):
    """레이스 불변식: submit과 kill이 어떤 순서로 겹치든, 두 작업이 끝난 뒤
    LSF 생존자(on-lsf)는 없고 각 job은 EXIT/CREATED/SUBMIT_FAILED 중 하나다."""
    with qtbot.waitSignals([manager.submit_finished, manager.kill_finished],
                           timeout=15000):
        js = submit_cmds(manager, [f"echo {i}" for i in range(30)],
                            auto_poll=False)
        manager.kill(js.id)

    assert fake_lsf.alive_jobs() == []
    allowed = {JobState.EXIT, JobState.CREATED, JobState.SUBMIT_FAILED}
    got = {r.state for r in js.jobs()}
    assert got <= allowed, got               # SUBMITTING/PEND 잔존 금지


class _StubScope:
    """테스트용 KillScope 대역 — acquire 결과를 주입한다."""

    def __init__(self, quiesced=True, gate=None):
        self.quiesced = quiesced
        self.gate = gate                     # 블로킹 재현용 (threading.Event)
        self.released = threading.Event()

    def acquire(self):
        if self.gate is not None:
            self.gate.wait(30)
        return self.quiesced

    def release(self):
        self.released.set()


def test_overlapping_kills_keep_snapshot_registered(qtbot, manager, fake_lsf):
    """같은 jobset에 kill이 겹칠 때, 먼저 끝난 kill이 진행 중인 다른 kill의
    스냅샷 등록을 지우지 않는다 — kill 1건당 slot 1개 (겹침 안전)."""
    jsid = mk_jobset(manager, intended_count=1).id
    gate = threading.Event()
    try:
        blocked = _StubScope(gate=gate)      # kill A — acquire에서 블록 유지
        manager.killer.kill_jobset(jsid, scope=blocked)
        assert manager.killer.is_active(jsid)      # 호출 스레드 등록 — 즉시

        with qtbot.waitSignal(manager.kill_finished, timeout=10000):
            manager.killer.kill_jobset(jsid)       # kill B — 즉시 완료

        # B가 끝나도 A의 등록은 남아야 한다 (기존 버그: 무조건 pop → False)
        assert manager.killer.is_active(jsid) is True
        assert manager.kill_snapshot(jsid) is not None
    finally:
        gate.set()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        pass                                       # A 완료
    assert manager.killer.is_active(jsid) is False # 전부 끝나면 해제


def test_job_edit_during_active_kill_rejected(qtbot, manager, fake_lsf):
    """kill 진행 중인 jobset은 job 목록 편집을 거부한다 — kill worker가 착수
    시점 스냅샷으로 도는데 그 사이 목록이 바뀌면 결과가 어긋난다
    (submit 중 편집 거부와 대칭)."""
    import pytest

    from lsfmgr.errors import LsfmgrError

    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        a = submit_cmds(manager, ["echo a"], auto_poll=False)

    gate = threading.Event()
    try:
        manager.killer.kill_jobset(a.id, scope=_StubScope(gate=gate))
        assert manager.killer.is_active(a.id)      # kill 진행 중

        with pytest.raises(LsfmgrError):
            manager.add_jobs(a.id, ["echo c"], job_keys=["c"])
    finally:
        gate.set()
    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        pass                                       # kill 완료

    # kill 완료 후에는 편집이 다시 열린다
    manager.add_jobs(a.id, ["echo c"], job_keys=["c"])
    assert manager.summary(a.id)["total"] == 2


def test_kill_started_pull_consistency(qtbot, manager, fake_lsf):
    """kill_started slot에서 pull API(is_killing/kill_state)를 조회하면
    이미 True/값이어야 한다 — 신호와 pull의 착수측 일치 계약."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)

    pulled = {}
    js.kill_started.connect(lambda: pulled.update(
        killing=js.is_killing, snap=js.kill_state is not None))

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js)

    assert pulled == {"killing": True, "snap": True}, pulled


def test_kill_finished_emitted_on_worker_exception(qtbot, manager, fake_lsf):
    """killer worker 예외 시에도 kill_finished가 발행된다(착수/완료 짝 계약)
    — 안 하면 kill_started로 스피너를 켠 UI가 영구 고착된다."""
    errors = []
    manager.error_occurred.connect(lambda _j, m: errors.append(m))

    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.kill("no_such_jobset")      # get_jobset 예외 유발

    _jsid, report = blocker.args
    assert report.errors and "internal" in report.errors[0]
    assert errors                                  # error_occurred도 발행
    assert manager.killer.is_active("no_such_jobset") is False  # 등록 해제


def test_quiesce_timeout_recorded_in_kill_report(qtbot, manager, fake_lsf):
    """barrier 정지 대기 초과는 KillReport.errors에 남고 optimistic EXIT
    표시도 억제된다 — 유출 가능성이 '전부 정리됨'으로 오보되지 않는다.
    barrier는 실패해도 반드시 release된다."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)

    stub = _StubScope(quiesced=False)        # 대기 초과 재현
    with qtbot.waitSignal(manager.kill_finished, timeout=10000) as blocker:
        manager.killer.kill_jobset(js.id, scope=stub)
    _jsid, report = blocker.args

    assert any("quiesce" in e for e in report.errors), report.errors
    # v10.1: per-id 확인(confirm 경로)이 생겨 quiesce 오류가 있어도 **확인된**
    # job은 정확히 EXIT 마킹된다 — 오류는 "그 사이 제출된 job이 kill에서
    # 빠졌을 수 있음"의 경고로 남고, 빠진 job은 애초에 changed에 없다.
    assert all(r.state is JobState.EXIT for r in report.changed)
    assert js.jobs()[0].state is JobState.EXIT   # 확인된 kill은 반영
    assert stub.released.is_set()            # finally에서 barrier 해제 보장


def test_revert_to_created_clears_failure_residue(qtbot, manager, fake_lsf):
    """CREATED 복귀는 이전 시도의 실패 잔재(fail_reason/fail_message/
    retry_count)를 함께 리셋한다 — 안 지우면 '제출된 적 없는' job이
    실패 이력을 달고 UI/persistent store에 남는다."""
    from lsfmgr.options import Options
    from lsfmgr.qt import QThreadPool
    from lsfmgr.states import JobRecord
    from lsfmgr.submitter import _SubmitContext
    from lsfmgr.util import TokenBucketLimiter

    jsid = mk_jobset(manager, intended_count=1).id
    key = f"{jsid}_0"
    manager.store.store_add_jobs([JobRecord(
        job_id=None, array_index=None, jobset_id=jsid, job_key=key,
        state=JobState.RETRY_WAIT, fail_reason="BSUB_TIMEOUT",
        fail_message="bsub: timeout after 30s", retry_count=2,
        command="echo x")])
    ctx = _SubmitContext(jobset_id=jsid, total=1,
                         pool=QThreadPool(), limiter=TokenBucketLimiter(None),
                         options=Options())

    manager.submitter._revert_to_created(ctx, [key])

    rec = manager.store.get_job(jsid, key)
    assert rec.state is JobState.CREATED
    assert rec.fail_reason is None
    assert rec.fail_message is None
    assert rec.retry_count == 0


def test_kill_started_emitted_synchronously(qtbot, manager, fake_lsf):
    """kill_started는 kill 접수 즉시(동기) 발행되고 핸들로도 중계된다 —
    quiesce로 kill_finished가 수십 초 늦어지는 케이스에서도 UI가 착수를
    바로 표시할 수 있다 (submit_started와 대칭인 착수 피드백)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)

    order = []
    manager.kill_started.connect(lambda jsid: order.append(("started", jsid)))
    manager.kill_finished.connect(lambda jsid, _r: order.append(("finished",
                                                                 jsid)))
    js.kill_started.connect(lambda: order.append(("handle_started", js.id)))

    with qtbot.waitSignal(manager.kill_finished, timeout=10000):
        manager.kill(js.id)
        # 동기 발행 — kill_jobset 반환 시점에 이미 도착해 있어야 한다
        assert ("started", js.id) in order
        assert ("handle_started", js.id) in order

    assert order.index(("started", js.id)) \
        < order.index(("finished", js.id))


def test_barrier_wait_releases_killer_pool_slot(qtbot, manager, fake_lsf):
    """barrier 정지 대기는 killer pool(4스레드) 슬롯을 반납한다 — 대기
    4건이 pool을 다 잡아도 다섯 번째 kill이 즉시 착수·완료돼야 한다."""
    gate = threading.Event()
    try:
        # kill 4건을 acquire에서 블록시켜 pool 4슬롯을 점유 상태로 만든다
        blocked = [mk_jobset(manager, intended_count=1).id for _ in range(4)]
        for jsid in blocked:
            manager.killer.kill_jobset(
                jsid, scope=_StubScope(gate=gate))

        target = mk_jobset(manager, intended_count=1).id  # 5번째 — 즉시 처리돼야 함
        with qtbot.waitSignal(manager.kill_finished, timeout=5000) as blocker:
            manager.killer.kill_jobset(target)
        assert blocker.args[0] == target      # 블록 4건보다 먼저 완료
    finally:
        gate.set()                            # 블록 해제 (teardown 청소)


def test_submit_during_kill_barrier_is_born_cancelled(qtbot, manager,
                                                      fake_lsf):
    """kill barrier가 올라간 동안 시작된 재제출은 born-cancelled — 레코드를
    건드리지도, LSF에 제출하지도 않고 전원 취소로 끝난다. 초기 cancel이
    못 잡는 '늦은 사이클'이 구조적으로 막히는지의 핵심 계약 (SubmitGate)."""
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        js = submit_cmds(manager, ["echo a"], auto_poll=False)
    rec = js.jobs()[0]
    fake_lsf.set_all("DONE", 0)
    manager.querier.query(js.id)             # DONE 확정 (원상 기준점)

    scope = manager._gate.kill_scope(js.id)
    assert scope.acquire() is True           # kill 진행 중 상태 재현
    try:
        n_lsf = len(fake_lsf.jobs)
        with qtbot.waitSignal(manager.submit_finished,
                              timeout=10000) as blocker:
            launched = manager.submitter.resubmit_existing(
                js.id, [(rec.job_key, ["echo", "again"])],
                Options())
        _jsid, report = blocker.args
        assert launched is False             # caller가 rearm 등을 생략하는 근거

        assert report.cancelled == 1 and report.succeeded == 0
        assert len(fake_lsf.jobs) == n_lsf   # LSF 제출 0
        after = js.jobs()[0]
        assert after.state is JobState.DONE  # 리셋조차 안 됨 (원상 유지)
        assert after.job_id == rec.job_id
    finally:
        scope.release()


# ---------------------------------------------------------------------------
# 범위 kill의 우선권 — 겨냥한 job에만, 항상 (v10.5: cancel_submit opt-in 삭제)
# ---------------------------------------------------------------------------

def test_cancel_submit_then_full_kill_cleans_everything(qtbot, config,
                                                       fake_lsf):
    """구 `cancel_submit=True` 옵션의 의도(배치 제출을 통째로 멈추면서 kill)는
    이제 `mgr.cancel_submit(js)` + **전체 kill**로 표현한다. 취소는 되돌려지지
    않으므로 순서만 지키면 되고, 전체 kill의 범위가 jobset 전체라 진행 중이던
    제출이 멎기를 기다렸다가(quiesce) 그새 제출된 job까지 정리한다.

    (부분 kill(only_state)로는 이 조합이 성립하지 않는다 — 대상 집합이 "지금
    그 상태인 job"이라, 아직 제출 중이라 PEND가 아닌 job은 애초에 대상이
    아니다. 그래서 '제출 폭주를 멈추면서 전부 정리'는 전체 kill의 일이다.)"""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = submit_cmds(mgr, ["echo 1", "echo 2", "echo 3"],
                         workers=1, auto_poll=False)
        assert runner.entered.wait(5)
        with qtbot.waitSignals([mgr.submit_finished, mgr.kill_finished],
                               timeout=15000):
            mgr.cancel_submit(js.id)              # 배치 전체 중단
            mgr.kill(js.id)                       # 남은 제출분까지 정리
            runner.gate.set()
        states = sorted(r.state.name for r in js.jobs())
        assert states == ["CREATED", "CREATED", "EXIT"], states
        assert fake_lsf.alive_jobs() == []          # 유출 0
    finally:
        mgr.shutdown()


def test_partial_kill_does_not_stop_unrelated_submits(qtbot, config, fake_lsf):
    """부분 kill(only_state)은 '지금 그 상태인 job만' 겨냥하므로, 제출 중인
    (=그 상태가 아닌) job의 제출은 멈추지 않는다."""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = submit_cmds(mgr, ["echo 1", "echo 2", "echo 3"],
                         workers=1, auto_poll=False)
        assert runner.entered.wait(5)
        with qtbot.waitSignal(mgr.kill_finished, timeout=15000):
            mgr.kill(js.id, only_state=JobState.PEND)
        with qtbot.waitSignal(mgr.submit_finished, timeout=15000):
            runner.gate.set()
        # 제출은 계속 진행돼 3건 모두 LSF에 들어갔다 (CREATED 복귀 없음)
        assert len(fake_lsf.jobs) == 3
        assert not any(r.state is JobState.CREATED for r in js.jobs())
    finally:
        mgr.shutdown()


def test_selected_kill_catches_inflight_job_by_default(qtbot, config,
                                                       fake_lsf):
    """선택 kill의 기본 동작: 호출 순간 '제출 중'이라 job_id가 없던 선택
    job도, barrier 뒤 key→id 재해석으로 확실히 죽는다(유출 방지 핵심).
    옵션 없이 항상 — 이게 없으면 그 job이 제출을 마쳐 PEND로 살아난다."""
    runner = GatedBsub(fake_lsf)
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=runner)
    try:
        js = submit_cmds(mgr, ["echo 1", "echo 2"], workers=1,
                         auto_poll=False)
        assert runner.entered.wait(5)
        keys = [r.job_key for r in js.jobs()]
        assert all(r.job_id is None for r in js.jobs())   # 아직 id 없음
        with qtbot.waitSignals([mgr.submit_finished, mgr.kill_finished],
                               timeout=15000):
            mgr.kill_jobs(js, keys)
            runner.gate.set()                # 진행 중이던 제출 완주 허용
        assert fake_lsf.alive_jobs() == []   # 제출된 그 job도 죽었다
        assert any(r.state is JobState.EXIT for r in js.jobs())
    finally:
        mgr.shutdown()


def test_selected_kill_scopes_barrier_to_its_keys(qtbot, config, fake_lsf):
    """선택 kill의 barrier는 **선택한 key에만** 걸린다 — kill 진행 중 도착한
    제출에서 그 key만 born-cancelled 되고, 나머지는 정상 제출된다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo 1", "echo 2"], auto_poll=False)
        recs = sorted(js.jobs(), key=lambda r: r.job_key)
        victim, survivor = recs[0].job_key, recs[1].job_key
        fake_lsf.set_all("DONE", 0)
        mgr.querier.query(js.id)                  # 재제출 가능 상태로

        scope = mgr._gate.kill_scope(js.id, [victim])
        scope.begin()                             # kill 진행 중 상태 재현
        try:
            n_lsf = len(fake_lsf.jobs)
            with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
                mgr.submitter.resubmit_existing(
                    js.id, [(victim, ["echo", "v"]),
                            (survivor, ["echo", "s"])], Options())
            by_key = {r.job_key: r for r in js.jobs()}
            # 대상은 리셋조차 안 됨(원상 유지), 비대상은 새로 제출됨
            assert by_key[victim].state is JobState.DONE
            assert by_key[survivor].state is JobState.PEND
            assert len(fake_lsf.jobs) == n_lsf + 1
        finally:
            scope.release()
    finally:
        mgr.shutdown()


def test_raw_id_kill_does_not_touch_submission(qtbot, config, fake_lsf):
    """원시 id kill은 제출 우선권을 걸지 않는다 — 대상이 이미 job_id를 가진
    (=제출이 끝난) job이라 멈출 제출이 없다. 예외 없이 조용히 통과한다."""
    mgr = LsfJobManager(store=InMemoryStore(), config=config, runner=fake_lsf)
    try:
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            js = submit_cmds(mgr, ["echo 1"], auto_poll=False)
        jid = js.jobs()[0].job_id
        with qtbot.waitSignal(mgr.kill_finished, timeout=10000):
            mgr.kill_jobs([jid], jobset_id=js.id)
        assert fake_lsf.alive_jobs() == []
    finally:
        mgr.shutdown()
