"""대규모 카오스 — test_chaos의 확장판.

기존 카오스가 흔드는 것: API 조작 순서(500 step, seed 5개).
여기서 더 흔드는 것:
  · **장애 주입** — bsub 실패/id 미파싱/bjobs 전면 장애/부분 장애/bkill 실패/
    job 증발. 순탄한 판에서는 안 도는 복구 경로(재시도 원장·LOST 확정·
    kill verify 재시도)가 경합과 겹친다.
  · **선택 조작** — submit(only=)/kill_jobs(js, keys)/kill(only_state=)처럼
    "일부만" 겨냥하는 경로. 전체 조작만으로는 부분 취소 정산이 안 돌아간다.
  · **불변식 8종 추가** (⑦~⑭) — 특히 ⑬은 증분 카운터가 실제 레코드와
    어긋나는지를 본다(summary는 전수 스캔이 아니라 증분 카운트로 만든다).

규모는 LSFMGR_CHAOS_SCALE로 조절 (기본 1 = CI용).
"""
from __future__ import annotations

import collections
import os
import random
import threading
import time

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.errors import LsfmgrError
from lsfmgr.store.base import summary_from_counts
from tests.test_jobset_cleanup import _find_traces

SCALE = float(os.environ.get("LSFMGR_CHAOS_SCALE", "1"))
STEPS = int(400 * SCALE)
SETTLE_MS = int(os.environ.get("LSFMGR_CHAOS_SETTLE",
                               str(int(25000 * SCALE ** 0.25))))

SEEDS = [int(s) for s in os.environ.get(
    "LSFMGR_CHAOS_SEEDS", "101,102,103,104,105,106,107,108").split(",")]


def _dump_state(mgr):
    """LSFMGR_CHAOS_DUMP=1일 때 고착 상태를 진단 가능한 형태로 찍는다.

    카오스 실패는 "무엇이 안 끝났는가"만으로는 못 고친다 — 진행 원장(누가
    몇 건 남았나)과 전 스레드 스택(어디서 멈췄나)이 같이 있어야 한다.
    실제로 이 조합이 '락은 unlocked인데 워커가 락에서 대기 중'이라는 모순을
    드러내 원인(같은 jobset의 사이클이 계속 새로 돌고 있었다)을 짚어냈다."""
    import faulthandler
    import sys
    print("\n==== 진행 중 submit ctx ====", file=sys.stderr)
    for jsid, c in list(mgr.submitter._contexts.items()):
        print(f"  {jsid}: {c.done}/{c.total} finished={c.finished} "
              f"cancel={c.cancel_event.is_set()} inflight={c.inflight} "
              f"cancelled_keys={c.cancelled_keys} lock={c.lock!r}",
              file=sys.stderr)
    print(f"==== kill slot ====\n  {mgr.killer._active}", file=sys.stderr)
    print(f"==== kill 우선권 게이트 ====\n  activities="
          f"{list(mgr._gate._activities)}\n  barriers="
          f"{list(mgr._gate._barriers)}", file=sys.stderr)
    faulthandler.dump_traceback(file=sys.stderr)


def _settle(mgr, qtbot, deadline_ms):
    """진행 중(submit/kill)이 없어질 때까지 기다린다 — 고정 대기 대신.

    고정 대기는 두 가지를 한꺼번에 망친다: 짧으면 아직 도는 중인 것을
    결함으로 오인하고, 길면 **정말 멎지 않는 것**을 '기다려 주면 통과'로
    숨긴다. 여기서는 조용해지는 즉시 진행하고, deadline까지도 조용해지지
    않으면 그 사실 자체를 위반으로 돌려준다.

    반환: 정착했으면 None, 아니면 아직 진행 중인 jobset을 설명하는 문자열."""
    end = time.monotonic() + deadline_ms / 1000.0
    while True:
        # store에 남은 jobset만 보면 **삭제된 jobset의 진행분**을 통째로
        # 놓친다(remove_jobset은 진행 중 kill을 기다려 주지 않는다). 진행
        # 원장을 직접 본다 — 정착의 정의는 "아무 데도 도는 것이 없다"이지
        # "살아있는 jobset이 조용하다"가 아니다.
        busy = [f"kill:{jsid}" for jsid, slots in list(mgr.killer._active.items())
                if slots]
        busy += [f"submit:{jsid}"
                 for jsid, ctx in list(mgr.submitter._contexts.items())
                 if not ctx.finished]
        for jsr in mgr.store.list_jobsets():
            try:
                if mgr.is_submitting(jsr.jobset_id):
                    busy.append(f"submit:{jsr.jobset_id}")
                if mgr.is_killing(jsr.jobset_id):
                    busy.append(f"kill:{jsr.jobset_id}")
            except LsfmgrError:
                continue                     # 그새 삭제됨
        if not busy:
            qtbot.wait(200)                  # 늦은 신호/타이머 여유
            return None
        if time.monotonic() >= end:
            return (f"{deadline_ms}ms 안에 정착하지 않음 — 진행 중 "
                    f"{len(busy)}건 {busy[:5]}")
        qtbot.wait(50)


def _recount(store, jobset_id):
    """전수 스캔으로 다시 센 요약 — 증분 카운트판과 대조용."""
    js = store.get_jobset(jobset_id)
    counts: collections.Counter = collections.Counter()
    for r in store.get_jobs(jobset_id):
        counts[r.state.value] += 1
    return summary_from_counts(js, dict(counts))


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("rest", [False, True], ids=["bjobs", "rest"])
def test_bigchaos(seed, rest, qtbot, fake_lsf):
    rnd = random.Random(seed * 7 + int(rest))

    def fetcher():
        if rnd.random() < 0.10:
            raise RuntimeError("REST 장애")
        with fake_lsf.lock:
            return {"jobs": [{"dataId": f"{j.job_id}.c1", "stat": j.stat}
                             for j in fake_lsf.jobs.values()
                             if not j.vanished]}

    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=5.0, workers=rnd.choice([2, 8]),
                         job_status_fetcher=fetcher if rest else None,
                         internal_refresh_min_s=0.0 if rest else None,
                         kill_workers=4, kill_chunk_size=rnd.choice([2, 16]),
                         max_retry=rnd.choice([0, 1, 2]), retry_delay_s=0.02,
                         min_state_dwell_s=rnd.choice([0.0, 0.15]),
                         verify_kill=rnd.random() < 0.5,
                         chunk_size=rnd.choice([3, 50])),
        runner=fake_lsf)

    live, removed, viol = [], set(), []
    pair = collections.Counter()
    kpair = collections.Counter()
    mgr.submit_started.connect(lambda j: pair.update([("s", j)]))
    mgr.submit_finished.connect(lambda j, r: pair.update([("f", j)]))
    mgr.kill_started.connect(lambda j: kpair.update([("s", j)]))
    mgr.kill_finished.connect(lambda j, r: kpair.update([("f", j)]))

    def check_live(*_a):                                     # 불변식 ①
        """진행 중에도 성립해야 하는 것만 본다 — summary() **한 번**의
        스냅샷 안에서 닫히는 성질(합계 == total).

        전수 재계산과의 대조(⑬)는 여기서 하면 안 된다: summary()와
        get_jobs()는 lock을 따로 잡으므로 그 사이의 정상 전이가 불일치로
        보인다(실제로 SUBMITTING→PEND가 그렇게 잡혔다). ⑬은 아무것도
        전이하지 않는 정착 뒤에만 뜻이 있다."""
        for js in list(live):
            try:
                s = mgr.summary(js)
            except LsfmgrError:
                continue
            total = s.get("total", 0)
            parts = sum(v for k, v in s.items() if k != "total")
            if parts != total:
                viol.append(f"요약 합 {parts} != total {total} ({js.id})")
    mgr.jobs_updated.connect(check_live)
    mgr.kill_finished.connect(check_live)

    def mk(js_n):
        n = rnd.randrange(1, js_n)
        return mgr.create_jobset(
            [f"mytool {rnd.randrange(999)}.sp" for _ in range(n)],
            job_keys=[f"k{i}" for i in range(n)],
            label=rnd.choice(["", "L"]), tags=rnd.choice([(), ("x",)]))

    def step():
        r = rnd.random()
        try:
            if r < 0.12 or not live:
                live.append(mk(rnd.choice([4, 12])))
                return
            js = rnd.choice(live)
            keys = [x.job_key for x in js.jobs()]
            if r < 0.30:
                only = (rnd.sample(keys, k=min(len(keys), 2))
                        if keys and rnd.random() < 0.35 else None)
                if mgr.can_submit(js, only=only):
                    mgr.submit(js, only=only,
                               auto_poll=rnd.random() < 0.5,
                               post_process=((lambda rep: None)
                                             if rnd.random() < 0.3 else None),
                               pre_submit=((lambda c: rnd.random() < 0.8)
                                           if rnd.random() < 0.15 else None))
            elif r < 0.36:
                mgr.kill(js, only_state=rnd.choice(
                    [None, JobState.PEND, JobState.RUN]))
            elif r < 0.40 and keys:
                mgr.kill_jobs(js, rnd.sample(keys, k=min(len(keys), 3)))
            elif r < 0.42:
                mgr.cancel_submit(js)
            elif r < 0.50: mgr.query_once(js)
            elif r < 0.55: mgr.start_polling(js, rnd.choice([5.0, 0.05]))
            elif r < 0.59: mgr.stop_polling(js)
            elif r < 0.62:
                name = f"h{rnd.randrange(3)}"
                if rnd.random() < 0.5:
                    mgr.add_handler(js, name, lambda c: None)
                else:
                    mgr.remove_handler(js, name)
            elif r < 0.70:                              # job 단위 편집
                if keys:
                    pick = rnd.sample(keys, k=min(len(keys), 2))
                    e = rnd.random()
                    if e < 0.25: mgr.remove_jobs(js, pick, force=True)
                    elif e < 0.40: mgr.clear_jobs(js, force=True)
                    elif e < 0.60: mgr.replace_jobs(
                        js, [f"mytool r{i}.sp" for i in range(len(pick))],
                        job_keys=pick)
                    elif e < 0.80: mgr.upsert_jobs(
                        js, ["mytool u0.sp", "mytool u1.sp"],
                        job_keys=["u0", "u1"])
                    else: mgr.set_user_data(js, pick[0], {"n": 1})
            elif r < 0.74:
                mgr.add_jobs(js, [f"mytool a{rnd.randrange(99)}.sp"],
                             job_keys=[f"a{rnd.randrange(999)}"])
            # --- 장애 주입 ---
            elif r < 0.78:
                fake_lsf.set_all(rnd.choice(["PEND", "RUN", "DONE", "EXIT"]))
            elif r < 0.81:
                fake_lsf.fail_next_bsub += rnd.randrange(1, 4)
            elif r < 0.835:
                fake_lsf.no_jobid_next_bsub += rnd.randrange(1, 3)
            elif r < 0.855:
                fake_lsf.fail_next_bkill += rnd.randrange(1, 3)
            elif r < 0.875:
                fake_lsf.fail_all_queries = not fake_lsf.fail_all_queries
            elif r < 0.895:
                ids = [x.job_id for x in js.jobs() if x.job_id]
                if ids:
                    if rnd.random() < 0.5:
                        fake_lsf.vanish_job(rnd.choice(ids))
                    else:
                        fake_lsf.bjobs_fail_ids ^= {rnd.choice(ids)}
            elif r < 0.915:
                ids = [x.job_id for x in js.jobs() if x.job_id]
                if ids:
                    mgr.kill_jobs(ids[:3], jobset_id=js.id)
            elif r < 0.93: mgr.detect_lost(js)
            elif r < 0.96:                              # 순수 조회
                mgr.total_summary(); mgr.search_jobsets(tag="x")
                mgr.summary(js); js.jobs(); mgr.is_submitting(js)
                mgr.submit_state(js); mgr.kill_state(js); mgr.is_killing(js)
                mgr.get_jobs(js, states={JobState.RUN}); mgr.jobset(js.id)
                mgr.list_jobsets()
            else:
                live.remove(js); removed.add(js.id)
                mgr.remove_jobset(js, force=True)
        except (LsfmgrError, ValueError):
            pass                                # 문서화된 거부는 정상 경로
        except Exception as e:                  # noqa: BLE001  불변식 ⑤
            viol.append(f"예상 밖 예외 {type(e).__name__}: {e!r}")

    threads0 = threading.active_count()
    try:
        for i in range(STEPS):
            step()
            if i % 20 == 0:
                qtbot.wait(1)
        # 정착 — 장애 주입을 걷고 전부 종료 상태로 몰아준다
        fake_lsf.fail_all_queries = False
        fake_lsf.bjobs_fail_ids = set()
        fake_lsf.fail_next_bsub = fake_lsf.no_jobid_next_bsub = 0
        fake_lsf.fail_next_bkill = 0
        fake_lsf.set_all("DONE")
        stuck = _settle(mgr, qtbot, SETTLE_MS)                  # 불변식 ②
        if stuck:
            viol.append(stuck)
        traces = _find_traces(mgr, removed)                    # 불변식 ③
        if traces:
            viol.append(f"삭제 흔적: {sorted(traces)[:3]}")
        for (kind, j), c in pair.items():                      # 불변식 ④
            if kind == "s" and pair[("f", j)] < c:
                viol.append(f"submit started {c} > finished "
                            f"{pair[('f', j)]} ({j})")
        for (kind, j), c in kpair.items():                     # 불변식 ⑫
            if kind == "s" and kpair[("f", j)] < c:
                viol.append(f"kill started {c} > finished "
                            f"{kpair[('f', j)]} ({j})")
        src = mgr.command.internal_status                      # 불변식 ⑥
        alive_ids = set()
        owner = {}
        for jsr in mgr.store.list_jobsets():                   # 불변식 ⑦
            for rec in mgr.store.get_jobs(jsr.jobset_id):
                if rec.job_id is None:
                    continue
                alive_ids.add(rec.job_id)
                prev = owner.get(rec.job_id)
                if prev is not None:
                    viol.append(f"job_id {rec.job_id} 중복 소유: "
                                f"{prev} / {jsr.jobset_id}/{rec.job_key}")
                owner[rec.job_id] = f"{jsr.jobset_id}/{rec.job_key}"
        if src is not None:
            ghosts = set(src._interest) - alive_ids
            if ghosts:
                viol.append(f"원장이 붙든 유령 job_id {len(ghosts)}건 "
                            f"(예: {sorted(ghosts)[:5]})")
        for jsr in mgr.store.list_jobsets():                   # 불변식 ⑧⑬
            s = mgr.store.summary(jsr.jobset_id)
            again = _recount(mgr.store, jsr.jobset_id)
            if s != again:
                viol.append(f"정착 후 증분 요약 {s} != 재계산 {again} "
                            f"({jsr.jobset_id})")
            stuck = [r.job_key for r in mgr.store.get_jobs(jsr.jobset_id)
                     if r.state in (JobState.SUBMITTING, JobState.RETRY_WAIT)]
            if stuck:
                viol.append(f"정착 후 과도 상태로 굳음 {jsr.jobset_id}: "
                            f"{stuck[:5]}")
        if mgr.submitter._contexts:                            # 불변식 ⑨
            viol.append(f"정착 후 submit ctx 잔존: "
                        f"{list(mgr.submitter._contexts)[:3]}")
        if any(mgr.killer._active.values()):
            viol.append(f"정착 후 kill slot 잔존: {mgr.killer._active}")
        grew = threading.active_count() - threads0             # 불변식 ⑪
        if grew > 40:
            viol.append(f"스레드 {grew}개 증가 (풀 상한 밖 누수 의심)")
        if viol and os.environ.get("LSFMGR_CHAOS_DUMP"):
            _dump_state(mgr)
        assert not viol, "\n  ".join([""] + sorted(set(viol))[:12])
    finally:
        t0 = time.monotonic()
        mgr.shutdown()
        took = time.monotonic() - t0                           # 불변식 ⑩
        assert took < 30, f"shutdown이 {took:.1f}s 걸렸다 (join 누수 의심)"


# ======================================================================
# 카오스 ②: **변경 API 재진입** — 신호 slot 안에서 다시 조작한다.
#
# test_slot_reentrancy는 멱등 조회 API만 본다. GUI가 실제로 하는 건 그게
# 아니다 — submit_finished에서 곧바로 kill을 누르고, jobs_updated에서 실패분을
# 지우고, kill_finished에서 jobset을 없앤다. 이 경로는 신호 발화 중(=라이브러리
# 내부 상태가 전이 도중)에 다시 들어오므로 순차 조작과 락 순서가 다르다.
# ======================================================================
MUTATING_SIGNALS = ["submit_started", "submit_progress", "submit_finished",
                    "jobs_updated", "jobset_updated", "kill_started",
                    "kill_finished", "kill_progress", "jobset_finished",
                    "post_processing_finished", "handler_finished",
                    "pre_submit_finished", "error_occurred", "job_lost"]


@pytest.mark.parametrize("seed", SEEDS[:4])
def test_bigchaos_reentrant_mutation(seed, qtbot, fake_lsf):
    rnd = random.Random(seed * 31)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=0.2, workers=4,
                         kill_workers=4, kill_chunk_size=4,
                         max_retry=1, retry_delay_s=0.02,
                         min_state_dwell_s=0.0, chunk_size=8),
        runner=fake_lsf)
    live, viol = [], []
    depth = [0]
    storm = [True]          # 정착 구간에는 내린다 — 아래 주석 참고

    def mutate(*_a):
        """slot 안에서 다시 조작 — 재귀는 2단까지만.

        storm이 내려가면 아무것도 하지 않는다. 폴링 tick이 내는
        jobset_updated에도 반응하게 두면 신호→조작→신호가 자가발전해
        영원히 정착하지 않는다(라이브러리 결함이 아니라 하네스 결함이다 —
        실제 GUI도 사용자가 손을 떼면 조작이 멎는다)."""
        if not storm[0] or depth[0] >= 2 or not live or rnd.random() < 0.55:
            return
        depth[0] += 1
        try:
            js = rnd.choice(live)
            r = rnd.random()
            keys = [x.job_key for x in js.jobs()]
            if r < 0.20 and mgr.can_submit(js):
                mgr.submit(js, auto_poll=rnd.random() < 0.5)
            elif r < 0.35: mgr.kill(js)
            elif r < 0.45 and keys:
                mgr.kill_jobs(js, keys[:2])
            elif r < 0.55: mgr.cancel_submit(js)
            elif r < 0.62 and keys: mgr.remove_jobs(js, keys[:1], force=True)
            elif r < 0.70: mgr.add_jobs(js, ["mytool z.sp"],
                                        job_keys=[f"z{rnd.randrange(999)}"])
            elif r < 0.76 and keys:
                mgr.replace_jobs(js, ["mytool q.sp"], job_keys=keys[:1])
            elif r < 0.82: mgr.upsert_jobs(js, ["mytool w.sp"],
                                           job_keys=["w0"])
            elif r < 0.88: mgr.start_polling(js, 0.05)
            elif r < 0.92: mgr.stop_polling(js)
            elif r < 0.96: mgr.query_once(js)
            elif len(live) > 1:
                live.remove(js)
                mgr.remove_jobset(js, force=True)
        except (LsfmgrError, ValueError):
            pass
        except Exception as e:                        # noqa: BLE001
            viol.append(f"재진입 예외 {type(e).__name__}: {e!r}")
        finally:
            depth[0] -= 1

    for name in MUTATING_SIGNALS:
        getattr(mgr, name).connect(mutate)

    # 살아있는 jobset 상한 — 재진입 판은 jobset 수에 **제곱으로** 무거워진다
    # (jobset마다 폴링 tick → 신호 → mutate → 또 신호). 여기서 안 자르면
    # SCALE을 올렸을 때 라이브러리가 아니라 하네스가 먼저 무릎을 꿇는다.
    MAX_LIVE = 8

    # 폭풍은 **회수가 아니라 벽시계**로 끊는다. 이 판은 조작 1회가 신호를
    # 여러 개 낳고 그 신호가 다시 조작을 부르는 증폭 루프라, 회수를 SCALE에
    # 비례해 늘리면 이벤트 큐가 소비보다 빨리 자라 실행시간이 폭발한다
    # (SCALE=3에서 40분+ 관측). SCALE은 '얼마나 오래 흔드는가'만 정한다.
    storm_until = time.monotonic() + 6.0 * SCALE

    try:
        for _ in range(int(12 * SCALE)):
            if time.monotonic() > storm_until:
                break
            if len(live) >= MAX_LIVE:
                old_js = live.pop(0)
                try:
                    mgr.remove_jobset(old_js, force=True)
                except LsfmgrError:
                    pass
            n = rnd.randrange(2, 10)
            js = mgr.create_jobset(
                [f"mytool {rnd.randrange(999)}.sp" for _ in range(n)],
                job_keys=[f"k{i}" for i in range(n)])
            live.append(js)
            mgr.add_handler(js, "h", lambda c: None)
            try:
                mgr.submit(js, auto_poll=True,
                           post_process=lambda rep: None)
            except LsfmgrError:
                pass
            qtbot.wait(rnd.choice([1, 5, 20]))
            if rnd.random() < 0.4:
                fake_lsf.set_all(rnd.choice(["RUN", "DONE", "EXIT"]))
        storm[0] = False                      # 손을 뗀다 — 이제 정착해야 한다
        fake_lsf.set_all("DONE")
        stuck = _settle(mgr, qtbot, SETTLE_MS)
        if stuck:
            viol.append(stuck)

        for jsr in mgr.store.list_jobsets():
            s = mgr.store.summary(jsr.jobset_id)
            again = _recount(mgr.store, jsr.jobset_id)
            if s != again:
                viol.append(f"증분 요약 {s} != 재계산 {again}")
        if mgr.submitter._contexts:
            viol.append(f"submit ctx 잔존: {list(mgr.submitter._contexts)[:3]}")
        if any(mgr.killer._active.values()):
            viol.append(f"kill slot 잔존: {mgr.killer._active}")
        if viol and os.environ.get("LSFMGR_CHAOS_DUMP"):
            _dump_state(mgr)
        assert not viol, "\n  ".join([""] + sorted(set(viol))[:12])
    finally:
        mgr.shutdown()


# ======================================================================
# 카오스 ③: **폭풍 한가운데서 shutdown** — 진행 중 submit/kill/폴링/후처리를
# 그대로 둔 채 내린다. GUI 종료가 실제로 그렇다(사용자는 조용해지길 기다려
# 주지 않는다). shutdown이 join을 놓치면 인터프리터 종료 중 Qt 객체가 지워진
# 스레드가 살아남아 크래시로 나타난다.
# ======================================================================
@pytest.mark.parametrize("seed", SEEDS[:4])
def test_bigchaos_shutdown_midstorm(seed, qtbot, fake_lsf):
    rnd = random.Random(seed * 17 + 3)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=0.05, workers=8,
                         kill_workers=4, kill_chunk_size=4,
                         max_retry=2, retry_delay_s=0.05,
                         min_state_dwell_s=0.1, chunk_size=4),
        runner=fake_lsf)
    late = []
    mgr.error_occurred.connect(lambda j, m: None)
    try:
        sets = []
        for _ in range(int(8 * SCALE)):
            n = rnd.randrange(5, 40)
            js = mgr.create_jobset(
                [f"mytool {rnd.randrange(999)}.sp" for _ in range(n)],
                job_keys=[f"k{i}" for i in range(n)])
            sets.append(js)
            mgr.add_handler(js, "h", lambda c: None)
            mgr.submit(js, auto_poll=True, post_process=lambda r: None)
            mgr.start_polling(js, 0.05)
        fake_lsf.fail_next_bsub = rnd.randrange(0, 5)
        fake_lsf.fail_next_bkill = rnd.randrange(0, 3)
        for js in sets[:len(sets) // 2]:
            mgr.kill(js)
        qtbot.wait(rnd.choice([0, 3, 15, 60]))        # 폭풍 한가운데

        t0 = time.monotonic()
        mgr.shutdown()
        took = time.monotonic() - t0
        assert took < 30, f"shutdown {took:.1f}s (join 누수)"

        # shutdown 후 조작은 전부 조용한 no-op이어야 한다 (예외 금지)
        for js in sets:
            for call in (lambda: mgr.submit(js), lambda: mgr.kill(js),
                         lambda: mgr.query_once(js),
                         lambda: mgr.start_polling(js, 0.05),
                         lambda: mgr.kill_jobs(js, ["k0"]),
                         lambda: mgr.summary(js), lambda: js.jobs(),
                         lambda: mgr.remove_jobset(js, force=True)):
                try:
                    call()
                except (LsfmgrError, ValueError):
                    pass
                except Exception as e:                # noqa: BLE001
                    late.append(f"shutdown 후 예외 {type(e).__name__}: {e!r}")
        mgr.shutdown()                                # 멱등
        qtbot.wait(300)
        assert not late, "\n  ".join([""] + sorted(set(late))[:10])
    finally:
        mgr.shutdown()


# ======================================================================
# 카오스 ④: **대형 jobset** — job 수를 두 자리에서 세 자리로 올린다.
# chunk 경계·진행 throttle·증분 카운터는 job 수에 비례해 도는 코드라
# 4~12건짜리 jobset으로는 한 번도 안 밟히는 분기가 있다.
# ======================================================================
@pytest.mark.parametrize("seed", SEEDS[:3])
def test_bigchaos_large_jobsets(seed, qtbot, fake_lsf):
    rnd = random.Random(seed * 97)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(poll_interval_s=0.05, workers=8,
                         kill_workers=4, kill_chunk_size=rnd.choice([1, 32]),
                         max_retry=1, retry_delay_s=0.02,
                         min_state_dwell_s=0.0,
                         chunk_size=rnd.choice([7, 200])),
        runner=fake_lsf)
    viol = []
    prog = {}

    def on_prog(jsid, done, total):                   # 진행 단조성
        prev = prog.get(jsid, (-1, total))
        if done < prev[0]:
            viol.append(f"submit 진행 역행 {jsid}: {prev[0]} -> {done}")
        if done > total:
            viol.append(f"submit 진행 초과 {jsid}: {done}/{total}")
        prog[jsid] = (done, total)
    mgr.submit_progress.connect(on_prog)

    try:
        for _ in range(int(4 * SCALE)):
            n = rnd.choice([120, 300, 500])
            js = mgr.create_jobset(
                [f"mytool {i}.sp" for i in range(n)],
                job_keys=[f"k{i}" for i in range(n)])
            with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
                mgr.submit(js, auto_poll=False)
            s = mgr.summary(js)
            again = _recount(mgr.store, js.id)
            if s != again:
                viol.append(f"제출 후 증분 요약 {s} != 재계산 {again}")
            op = rnd.random()
            if op < 0.35:
                with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
                    mgr.kill(js)
            elif op < 0.6:
                keys = [f"k{i}" for i in range(0, n, 3)]
                with qtbot.waitSignal(mgr.kill_finished, timeout=60000):
                    mgr.kill_jobs(js, keys)
            elif op < 0.8:
                mgr.clear_jobs(js, force=True)
            else:
                mgr.remove_jobs(js, [f"k{i}" for i in range(0, n, 2)],
                                force=True)
            qtbot.wait(80)
            s = mgr.summary(js)
            again = _recount(mgr.store, js.id)
            if s != again:
                viol.append(f"조작 후 증분 요약 {s} != 재계산 {again}")
            if sum(v for k, v in s.items() if k != "total") != s["total"]:
                viol.append(f"요약 합 불일치: {s}")
        assert not viol, "\n  ".join([""] + sorted(set(viol))[:12])
    finally:
        mgr.shutdown()
