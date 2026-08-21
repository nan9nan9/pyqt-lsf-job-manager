"""공용 제출 풀은 jobset 사이를 **번갈아** 집행한다 (라운드 로빈).

제출은 jobset마다가 아니라 공용 QThreadPool 하나가 처리한다(전역 workers
상한 — test_global_workers). 그 안의 **분배**는 오래 계약이 없었고, 우선순위를
안 주면 Qt 기본값인 선착순이 그대로 드러났다: 큰 jobset을 통째로 밀어 넣으면
뒤에 온 작은 jobset이 그 뒤에 전부 줄섰다(400건 뒤의 5건 = 1.07s 대기,
단독이면 0.013s. 실환경 bsub 왕복 200ms에 앞이 5000건이면 2분이다).

지금은 task 우선순위를 **jobset 안의 순번**으로 준다 — "모든 jobset의 1번 →
모든 jobset의 2번 → …"이 되어 그 자체가 라운드 로빈이다.

여기서 지키는 계약 셋:
  ① 뒤에 온 작은 jobset이 큰 jobset의 꼬리를 기다리지 않는다
  ② jobset **안**의 순서는 그대로다 (순번대로 우선순위가 내려가므로)
  ③ 재시도는 끼어든다 — 이미 backoff를 기다린 건을 또 줄 뒤로 보내지 않는다

시간이 아니라 **집행 순서**로 검사한다 — 벽시계로 재면 부하에 흔들린다.
"""
from __future__ import annotations

import threading
import time

from lsfmgr import InMemoryStore, LsfConfig, LsfJobManager

BIG, SMALL, WORKERS = 200, 5, 8


class _Recorder:
    """제출 호출 순서를 기록하는 runner 래퍼 — 풀을 계속 물려 두려고
    호출마다 아주 짧게 쉰다(안 그러면 큐가 찰 새 없이 비어 순서가 안 생긴다)."""

    def __init__(self, inner, cost=0.004):
        self.inner = inner
        self.cost = cost
        self.lock = threading.Lock()
        self.order = []                      # 제출된 커맨드 태그, 집행 순

    def __call__(self, argv, timeout, cwd=None):
        if argv[0].rsplit("/", 1)[-1] in ("bjobs", "bkill"):
            return self.inner(argv, timeout, cwd)
        with self.lock:
            self.order.append(argv[-1])
        time.sleep(self.cost)
        return self.inner(argv, timeout, cwd)

    def tags(self, prefix):
        return [i for i, t in enumerate(self.order) if t.startswith(prefix)]


def _mgr(rec):
    return LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(workers=WORKERS, poll_interval_s=100.0,
                         min_state_dwell_s=0.0),
        runner=rec)


def test_small_jobset_does_not_queue_behind_a_big_one(qtbot, fake_lsf):
    """① 나중에 온 5건이 앞선 200건의 꼬리를 기다리지 않는다."""
    rec = _Recorder(fake_lsf)
    mgr = _mgr(rec)
    try:
        big = mgr.create_jobset([f"w b{i}.sp" for i in range(BIG)],
                                job_keys=[f"b{i}" for i in range(BIG)])
        small = mgr.create_jobset([f"w s{i}.sp" for i in range(SMALL)],
                                  job_keys=[f"s{i}" for i in range(SMALL)])
        done = []
        mgr.submit_finished.connect(lambda j, r: done.append(j))
        mgr.submit(big, auto_poll=False)
        qtbot.wait(30)                       # 큰 것이 먼저 큐를 채운 뒤
        mgr.submit(small, auto_poll=False)
        qtbot.waitUntil(lambda: len(done) == 2, timeout=60000)

        pos = rec.tags("s")
        assert len(pos) == SMALL, f"작은 jobset이 다 안 돌았다: {pos}"
        # 선착순이면 마지막 5개(≈195~199)에 몰린다. 라운드 로빈이면 이미
        # 집행 중이던 WORKERS개 뒤에 바로 붙는다 — 넉넉히 잡아도 앞 1/4 안.
        assert max(pos) < BIG // 4, (
            f"작은 jobset이 큰 것 뒤에 줄섰다 — 집행 위치 {pos} "
            f"(전체 {len(rec.order)}건). 우선순위가 안 걸렸는지 확인할 것")
    finally:
        mgr.shutdown()


def test_order_within_a_jobset_is_preserved(qtbot, fake_lsf):
    """② 우선순위를 순번으로 주는 것이 jobset 안의 순서를 뒤집으면 안 된다.

    표의 행 순서와 제출 순서가 어긋나면 '앞에서부터 도는 중'이라는 읽기가
    깨진다. 동시 실행분(WORKERS개)은 서로 앞뒤가 섞일 수 있으므로,
    **그 창을 넘어선** 역전만 잡는다."""
    rec = _Recorder(fake_lsf)
    mgr = _mgr(rec)
    try:
        js = mgr.create_jobset([f"w b{i}.sp" for i in range(BIG)],
                               job_keys=[f"b{i}" for i in range(BIG)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(js, auto_poll=False)

        idx = [int(t[1:-3]) for t in rec.order]        # "b17.sp" → 17
        assert sorted(idx) == list(range(BIG)), "제출된 job 집합이 다르다"
        worst = max(pos - i for i, pos in enumerate(sorted(
            range(BIG), key=lambda n: idx.index(n))))
        assert worst <= WORKERS, (
            f"jobset 안의 순서가 동시 실행 창({WORKERS})을 넘어 뒤집혔다: "
            f"최대 {worst}칸")
    finally:
        mgr.shutdown()


def test_retry_cuts_in_line(qtbot, fake_lsf):
    """③ 재시도는 대기 중인 후속분보다 먼저 나간다.

    이미 RETRY_WAIT로 backoff를 기다린 건이다 — 여기서 또 줄 뒤로 보내면
    지연을 두 번 먹는다. 앞선 몇 건만 실패시켜 재시도를 만들고, 그 재시도가
    아직 안 나간 뒷번호들보다 앞서 집행되는지 본다."""
    rec = _Recorder(fake_lsf)
    mgr = LsfJobManager(
        store=InMemoryStore(),
        config=LsfConfig(workers=2, poll_interval_s=100.0,
                         min_state_dwell_s=0.0,
                         max_retry=1, retry_delay_s=0.01),
        runner=rec)
    try:
        n = 40
        fake_lsf.fail_next_bsub = 2                  # 앞의 2건이 실패 → 재시도
        js = mgr.create_jobset([f"w r{i}.sp" for i in range(n)],
                               job_keys=[f"r{i}" for i in range(n)])
        with qtbot.waitSignal(mgr.submit_finished, timeout=60000):
            mgr.submit(js, auto_poll=False)

        # 재시도분은 같은 태그가 두 번 나온다 — 두 번째 등장 위치가 재시도다
        seen, retry_at = set(), []
        for i, t in enumerate(rec.order):
            if t in seen:
                retry_at.append(i)
            seen.add(t)
        assert retry_at, "재시도가 일어나지 않았다 — 전제가 깨졌다"
        assert max(retry_at) < n, (
            f"재시도가 줄 뒤로 밀렸다 — 위치 {retry_at} / 전체 {len(rec.order)}")
    finally:
        mgr.shutdown()
