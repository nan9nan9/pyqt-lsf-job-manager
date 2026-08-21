"""여러 jobset을 무작위로 조작해도 불변식이 깨지지 않는다.

순차 시나리오 테스트가 못 잡는 것은 **경합 경로**다 — 실제로 조회 직렬화
lock 누수가 여기서만 걸렸다(삭제 직후 도착한 조회가 lock을 되살려 영영
남았다). 확정 seed로 조작 순서를 뽑아 재현 가능하게 유지한다.

검사하는 불변식:
  ① 상태별 합 == total (요약 불변식)
  ② 정착 후 진행 중인 submit/kill이 없다
  ③ 지운 jobset의 흔적이 살아있는 객체 어디에도 없다
  ④ submit_started 수 ≤ submit_finished 수 (착수/완료 짝)
  ⑤ 문서화된 거부(LsfmgrError/ValueError) 외의 예외가 없다
  ⑥ 콜백 조회원 원장이 store에 없는 job_id를 붙들고 있지 않다
"""
from __future__ import annotations

import collections
import random

import pytest

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager
from lsfmgr.errors import LsfmgrError
from tests.test_jobset_cleanup import _find_traces

STEPS = 500


@pytest.mark.parametrize("seed,rest", [(11, False), (12, False), (13, False),
                                       (54, True), (55, True)])
def test_random_operations_preserve_invariants(seed, rest, qtbot, fake_lsf):
    """rest=True는 조회를 job_status_fetcher(REST 콜백)로 하는 판 — bjobs
    경로와 코드가 갈리므로 따로 흔든다(원장 누수가 여기서만 걸렸다)."""
    rnd = random.Random(seed)

    def fetcher():
        if rnd.random() < 0.08:
            raise RuntimeError("REST 장애")      # 조회 장애도 섞는다
        with fake_lsf.lock:
            return {"jobs": [{"dataId": f"{j.job_id}.c1", "stat": j.stat}
                             for j in fake_lsf.jobs.values()]}

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=5.0, workers=8,
                         job_status_fetcher=fetcher if rest else None,
                         internal_refresh_min_s=0.0 if rest else None,
                         kill_workers=4, kill_chunk_size=4,
                         max_retry=1, retry_delay_s=0.02,
                         min_state_dwell_s=rnd.choice([0.0, 0.15]),
                         chunk_size=rnd.choice([3, 50])),
        runner=fake_lsf)
    live, removed, viol = [], set(), []
    pair = collections.Counter()
    mgr.submit_started.connect(lambda j: pair.update([("s", j)]))
    mgr.submit_finished.connect(lambda j, r: pair.update([("f", j)]))

    def check_summary(*_a):                                    # 불변식 ①
        for js in list(live):
            try:
                s = mgr.summary(js)
            except LsfmgrError:
                continue
            total = s.get("total", 0)
            parts = sum(v for k, v in s.items() if k != "total")
            if parts != total:
                viol.append(f"요약 합 {parts} != total {total} ({js.id})")
    mgr.jobs_updated.connect(check_summary)
    mgr.kill_finished.connect(check_summary)

    def step():
        r = rnd.random()
        try:
            if r < 0.14 or not live:
                n = rnd.randrange(1, 8)
                live.append(mgr.create_jobset(
                    [f"mytool {rnd.randrange(999)}.sp" for _ in range(n)],
                    job_keys=[f"k{i}" for i in range(n)]))
                return
            js = rnd.choice(live)
            if r < 0.40:
                if mgr.can_submit(js):
                    mgr.submit(js, auto_poll=rnd.random() < 0.5,
                               post_process=((lambda rep: None)
                                             if rnd.random() < 0.3 else None))
            elif r < 0.55: mgr.kill(js)
            elif r < 0.65: mgr.query_once(js)
            elif r < 0.72: mgr.start_polling(js, 5.0)
            elif r < 0.78: mgr.stop_polling(js)
            elif r < 0.82: mgr.add_handler(js, f"h{rnd.randrange(3)}",
                                           lambda c: None)
            elif r < 0.86:                       # job 단위 편집
                keys = [x.job_key for x in js.jobs()]
                if keys:
                    pick = rnd.sample(keys, k=min(len(keys), 2))
                    e = rnd.random()
                    if e < 0.3: mgr.remove_jobs(js, pick, force=True)
                    elif e < 0.5: mgr.clear_jobs(js, force=True)
                    elif e < 0.7: mgr.replace_jobs(
                        js, [f"mytool r{i}.sp" for i in range(len(pick))],
                        job_keys=pick)
                    elif e < 0.85: mgr.upsert_jobs(
                        js, ["mytool u0.sp", "mytool u1.sp"],
                        job_keys=["u0", "u1"])
                    else: mgr.set_user_data(js, pick[0], {"n": 1})
            elif r < 0.89: fake_lsf.set_all(
                rnd.choice(["PEND", "RUN", "DONE", "EXIT"]))
            elif r < 0.91:
                mgr.add_jobs(js, [f"mytool a{rnd.randrange(99)}.sp"],
                             job_keys=[f"a{rnd.randrange(999)}"])
            elif r < 0.93:
                ids = [x.job_id for x in js.jobs() if x.job_id]
                if ids:
                    mgr.kill_jobs(ids[:3])
            elif r < 0.945: mgr.detect_lost(js)
            elif r < 0.96:
                mgr.total_summary(); mgr.search_jobsets(tag="x")
                mgr.summary(js); js.jobs(); mgr.is_submitting(js)
            else:
                live.remove(js); removed.add(js.id)
                mgr.remove_jobset(js, force=True)
        except (LsfmgrError, ValueError):
            pass                                # 문서화된 거부는 정상 경로
        except Exception as e:                  # noqa: BLE001  불변식 ⑤
            viol.append(f"예상 밖 예외 {type(e).__name__}: {e}")

    try:
        for i in range(STEPS):
            step()
            if i % 25 == 0:
                qtbot.wait(1)
        qtbot.wait(1500)                        # 정착

        for js in live:                                        # 불변식 ②
            try:
                if mgr.is_submitting(js) or mgr.is_killing(js):
                    viol.append(f"정착 후에도 진행 중: {js.id}")
            except LsfmgrError:
                pass
        traces = _find_traces(mgr, removed)                    # 불변식 ③
        if traces:
            viol.append(f"삭제 흔적: {sorted(traces)[:3]}")
        for (kind, j), c in pair.items():                      # 불변식 ④
            if kind == "s" and pair[("f", j)] < c:
                viol.append(f"started {c} > finished {pair[('f', j)]} ({j})")
        src = mgr.command.internal_status                      # 불변식 ⑥
        if src is not None:
            alive = set()
            for jsr in mgr.store.list_jobsets():
                alive |= {r.job_id for r in mgr.store.get_jobs(jsr.jobset_id)
                          if r.job_id}
            ghosts = set(src._interest) - alive
            if ghosts:
                viol.append(f"원장이 붙든 유령 job_id {len(ghosts)}건 — "
                            f"store 어디에도 없다 (예: {sorted(ghosts)[:5]})")
        assert not viol, "\n  ".join([""] + viol[:10])
    finally:
        mgr.shutdown()
