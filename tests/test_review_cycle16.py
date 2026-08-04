"""전체 정독 리뷰 사이클 16 — handler 장부가 job의 정체성과 어긋나는 두 지점.

둘 다 "handler의 진행 장부(status/inflight)가 실제 job이나 handler 수명과
어긋난다"는 같은 계열이다.

- C16-1: job을 **교체**해도 그 job_key의 handler 장부가 남았다.
  교체는 같은 merge_id면 물리 키(job_key)를 유지한 채 내용만 바꾼다(테이블
  행 연속성). 그런데 옛 실행의 _FINISHED가 그대로 남아, 그 키의 **새 job**에
  handler가 영영 침묵한다(_eval_record 첫 줄에서 걸러짐).
  교체분이 CREATED가 되는 통상 경로에서는 재제출의 rearm이 덮어 가려져
  있었지만, force로 이미 RUN인 job을 교체하면 재제출이 없어 바로 드러난다.
  → 편집(_edit_jobs)이 변경분(changed)의 장부를 무효화한다. pacer/querier
    보류분을 _forget_paced로 정리하는 것과 같은 이유.

- C16-2: inflight 표식이 _Handler 객체 안에 있어, remove_handler로 그 객체가
  버려지면 함께 사라졌다. 같은 이름으로 재등록하면 worker에서 아직 도는 job을
  새 handler가 **다시 실행**한다 — 사용자 코드가 같은 job에 동시 2회 진입.
  → 표식을 서비스 레벨((jobset_id, handler_name, job_key))로 올려 handler
    객체 수명과 분리한다. 이름을 키에 포함하므로 서로 다른 handler는 여전히
    독립적으로 돈다.
"""
from __future__ import annotations

import threading

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from tests.fake_lsf import FakeLsf


def _mgr(runner):
    return LsfJobManager(store=InMemoryStore(),
                         config=LsfConfig(retry_delay_s=0.05), runner=runner)


def _poll(qtbot, mgr, js):
    with qtbot.waitSignal(mgr.jobset_updated, timeout=10000):
        mgr.query_once(js)
    qtbot.wait(200)


# ----------------------------------------------------------------------
# C16-1
# ----------------------------------------------------------------------
def test_replace_reruns_handler_without_resubmit(qtbot):
    """교체된 job에 handler가 다시 돈다 — 재제출(rearm) 없이.

    force로 **이미 RUN인** job을 교체하면 새 레코드는 CREATED가 되어 다음
    RUN에서 다시 관찰 대상이 된다. 장부가 안 지워지면 여기서 침묵한다.
    """
    fake = FakeLsf()
    mgr = _mgr(fake)
    seen = []
    mgr.handler_finished.connect(lambda j, n, r: seen.append(r.job_key))
    try:
        # target: merge_id "a"로 완주 → 그 job_key의 장부는 _FINISHED
        tgt = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        mgr.add_handler(tgt.id, "h", lambda ctx: 1)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(tgt, auto_poll=False)
        fake.set_all("RUN")
        _poll(qtbot, mgr, tgt)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, tgt)
        tgt_key = tgt.jobs()[0].job_key
        assert tgt_key in seen

        # 같은 merge_id "a"를 새 커맨드로 교체 → 재제출 후 다시 RUN
        mgr.replace_jobs(tgt, ["customwrapper_sub a2.sp"], merge_ids=["a"])
        rec = tgt.jobs()[0]
        assert rec.job_key == tgt_key            # 물리 키 유지
        assert rec.command == "customwrapper_sub a2.sp"

        seen.clear()
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(tgt, auto_poll=False)
        fake.set_all("RUN")
        _poll(qtbot, mgr, tgt)
        assert tgt_key in seen, "교체된 job에 handler가 안 돌았다(장부 잔류)"
    finally:
        mgr.shutdown()


def test_edit_does_not_touch_untouched_jobs(qtbot):
    """반대 방향 — 편집에 관여하지 않은 기존 job의 장부는 건드리지 않는다.
    (무효화 범위가 changed로 한정돼야 한다. 전체를 리셋하면 완주한 job에
     final이 다시 발화한다.)"""
    fake = FakeLsf()
    mgr = _mgr(fake)
    seen = []
    mgr.handler_finished.connect(lambda j, n, r: seen.append(r.job_key))
    try:
        tgt = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        mgr.add_handler(tgt.id, "h", lambda ctx: 1)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(tgt, auto_poll=False)
        fake.set_all("RUN")
        _poll(qtbot, mgr, tgt)
        fake.set_all("DONE", 0)
        _poll(qtbot, mgr, tgt)
        old_key = tgt.jobs()[0].job_key

        # 무관한 merge_id "b"를 추가 — "a"는 손대지 않는다
        mgr.add_jobs(tgt, ["customwrapper_sub b.sp"], merge_ids=["b"])

        seen.clear()
        _poll(qtbot, mgr, tgt)
        assert old_key not in seen, "관여하지 않은 job에 handler가 재발화했다"
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# C16-2
# ----------------------------------------------------------------------
def test_readd_handler_does_not_double_run(qtbot):
    """실행 중 remove → add 재등록해도 같은 job이 겹쳐 돌지 않는다."""
    fake = FakeLsf()
    mgr = _mgr(fake)
    entered, release = threading.Event(), threading.Event()
    lock = threading.Lock()
    running, peak = {"n": 0}, {"n": 0}

    def slow(ctx):
        with lock:
            running["n"] += 1
            peak["n"] = max(peak["n"], running["n"])
        entered.set()
        release.wait(3)
        with lock:
            running["n"] -= 1
        return 1

    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        mgr.add_handler(js.id, "h", slow)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        mgr.query_once(js)
        # main 스레드를 block하면 queued signal이 안 돌아 handler가 시작조차
        # 못 한다 — 이벤트 루프를 돌리며 기다린다
        qtbot.waitUntil(entered.is_set, timeout=5000)

        mgr.remove_handler(js.id, "h")           # 도는 중에 교체
        mgr.add_handler(js.id, "h", slow)
        _poll(qtbot, mgr, js)                    # 새 handler가 같은 job 평가
        qtbot.wait(300)
        release.set()
        qtbot.wait(500)

        assert peak["n"] == 1, f"같은 job에 handler가 {peak['n']}중 실행됐다"
    finally:
        release.set()
        mgr.shutdown()


def test_distinct_handlers_still_run_concurrently(qtbot):
    """반대 방향 — 이름이 다른 handler는 같은 job에서 여전히 동시에 돈다.
    (inflight 키에 handler 이름이 빠지면 서로를 막아버린다.)"""
    fake = FakeLsf()
    mgr = _mgr(fake)
    lock = threading.Lock()
    running, peak = {"n": 0}, {"n": 0}
    both = threading.Event()

    def slow(ctx):
        with lock:
            running["n"] += 1
            peak["n"] = max(peak["n"], running["n"])
            if running["n"] >= 2:
                both.set()
        both.wait(2)
        with lock:
            running["n"] -= 1
        return 1

    try:
        js = mgr.create_jobset(["customwrapper_sub a.sp"], merge_ids=["a"])
        mgr.add_handler(js.id, "h1", slow)
        mgr.add_handler(js.id, "h2", slow)
        with qtbot.waitSignal(mgr.submit_finished, timeout=10000):
            mgr.submit(js, auto_poll=False)
        fake.set_all("RUN")
        mgr.query_once(js)
        qtbot.waitUntil(both.is_set, timeout=5000)
        assert peak["n"] == 2, "서로 다른 handler가 서로를 막았다"
    finally:
        both.set()
        mgr.shutdown()
