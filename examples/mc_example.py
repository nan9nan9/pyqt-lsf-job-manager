#!/usr/bin/env python3
"""MultiCluster(job forwarding) 예제 — forward job kill (콘솔).

실제 LSF MultiCluster 환경에서는 job 이 다른 클러스터로 **forward** 될 수 있고,
그렇게 forward 된 job 은 **로컬 `bkill` 로는 죽지 않는다** — 그 클러스터의 LSF
env(cshrc)를 `source` 한 뒤 bkill 해야 죽는다. 이 예제는 mocklsf 의 MC 흉내
(MOCKLSF_FORWARD_CLUSTERS)로 그 상황을 재현하고, lsfmgr 의 `envpath` kill 로
해결하는 전체 흐름을 보여준다.

흐름:
  1) MC 켠 mocklsf 로 job 여러 개 제출 → 일부가 forward 된다.
  2) `collect_clusters=True` 폴링으로 각 job 의 `forward_cluster` 를 확인.
  3) forward 된 job 하나를 **그냥** `mgr.kill_jobs(js, [key])` — 안 죽는 것을 확인.
  4) 남은 job 을 `forward_cluster` 로 분류해, forward 는 그 클러스터 env 를
     source 한 kill(`envpath=`), 로컬은 일반 kill → 전부 종료.

핵심 API:
  - LsfJobManager(collect_clusters=True)   # forward_cluster 를 폴링으로 채움
  - mgr.kill_jobs(js, keys, envpath="<cshrc 경로>")
        → tcsh -c "source <cshrc> && set noglob && exec bkill <ids>" 로 실행

실행:  python examples/mc_example.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from qtpy.QtCore import QCoreApplication, QTimer

from lsfmgr import JobState
from common import (cluster_env_path, configure_mocklsf, format_summary,
                    install_logging, make_manager, wrapper)

#: mocklsf 가 job 을 forward 할 원격 클러스터들 (실제 MC 의 원격 cluster 흉내).
FORWARD_CLUSTERS = ["cluster_busan", "cluster_daegu"]
N_JOBS = 6


def main():
    app = QCoreApplication(sys.argv)
    install_logging()

    # MC 켜기 — 반드시 submit(=데몬 기동) 이전에 설정해야 반영된다.
    # run 을 길게 줘서 데모 동안 job 이 RUN 으로 살아 있게 한다.
    configure_mocklsf(pend=(0, 1), run=(120, 120), submit_delay=0,
                      submit_fail_rate=0, exit_rate=0, suspend_rate=0,
                      forward_clusters=FORWARD_CLUSTERS, forward_rate=0.7)

    # collect_clusters=True 라야 forward_cluster 가 폴링으로 채워진다.
    # kill 재시도는 짧게(데모: forward job 이 안 죽는 걸 빨리 확인).
    mgr, _ = make_manager(collect_clusters=True,
                          kill_max_retry=1, kill_retry_delay_s=0.5)

    cmds = [wrapper("customwrapper_sub", "-q", "normal", f"run_{i}.sp")
            for i in range(N_JOBS)]
    js = mgr.create_jobset(cmds, label="mc-demo")        # wrapper 커맨드 그대로
    mgr.submit(js, auto_poll=False)
    mgr.start_polling(js, 1)
    print(f"제출: {N_JOBS} jobs → jobset {js.id}  "
          f"(MC forward 대상: {FORWARD_CLUSTERS}, 확률 0.7)\n")

    state = {"phase": "wait_run", "victim": None, "ticks": 0}

    def tick():
        jobs = js.jobs()
        phase = state["phase"]

        if phase == "wait_run":
            on_lsf = [r for r in jobs if r.state.is_on_lsf]
            if jobs and on_lsf and all(r.state is not JobState.PEND
                                       for r in on_lsf):
                print("=== 제출된 job 의 클러스터 ===")
                for r in sorted(jobs, key=lambda r: r.job_key):
                    tag = r.forward_cluster or "(로컬)"
                    print(f"  {r.job_key}: job_id={r.job_id} "
                          f"{r.state.value:4} forward_cluster={tag}")
                fwd = [r for r in jobs if r.forward_cluster
                       and r.state.is_on_lsf]
                if not fwd:
                    print("\n(이번엔 forward 된 job 이 없습니다 — 전부 로컬. "
                          "envpath 데모는 생략)")
                    state["phase"] = "cluster_kill"
                else:
                    state["victim"] = fwd[0].job_key
                    state["phase"] = "plain_kill"

        elif phase == "plain_kill":
            key = state["victim"]
            fc = next(r.forward_cluster for r in jobs if r.job_key == key)
            print(f"\n=== 1) forward job {key}(→{fc}) 를 그냥 kill 시도 "
                  f"(envpath 없이) ===")
            mgr.kill_jobs(js, [key])     # 로컬 bkill — forward job 엔 안 닿는다
            state["ticks"] = 0
            state["phase"] = "check_survived"

        elif phase == "check_survived":
            state["ticks"] += 1
            if state["ticks"] >= 3:      # 재시도가 끝날 시간을 준 뒤 확인
                v = next((r for r in jobs
                          if r.job_key == state["victim"]), None)
                if v and v.state.is_on_lsf:
                    print(f"  → {v.job_key} 는 여전히 {v.state.value} — "
                          f"로컬 bkill 로는 forward job 이 안 죽는다(예상대로).")
                else:
                    print(f"  → {state['victim']} 상태: "
                          f"{v.state.value if v else '사라짐'}")
                state["phase"] = "cluster_kill"

        elif phase == "cluster_kill":
            alive = [r for r in jobs if r.state.is_on_lsf]
            print(f"\n=== 2) 남은 {len(alive)}개를 forward_cluster 로 분류해 "
                  f"각각 kill ===")
            by_cluster = {}
            for r in alive:
                by_cluster.setdefault(r.forward_cluster or None,
                                      []).append(r.job_key)
            for cluster, keys in by_cluster.items():
                if cluster:              # forward 된 job — 그 클러스터 env source
                    envpath = cluster_env_path(cluster)
                    print(f"  {cluster}: {len(keys)}개 → "
                          f"source {os.path.basename(envpath)} && bkill")
                    mgr.kill_jobs(js, keys, envpath=envpath)
                else:                    # 로컬 job — 일반 bkill
                    print(f"  (로컬): {len(keys)}개 → 일반 bkill")
                    mgr.kill_jobs(js, keys)
            state["phase"] = "wait_dead"

        elif phase == "wait_dead":
            if jobs and all(r.state.is_terminal for r in jobs):
                print("\n=== 완료 — 전 job 종료 ===")
                print(" ", format_summary(mgr.summary(js.id)))
                QTimer.singleShot(200, app.quit)

    timer = QTimer()
    timer.timeout.connect(tick)
    timer.start(1000)

    QTimer.singleShot(120_000, app.quit)     # 안전망
    app.exec()
    mgr.shutdown()


if __name__ == "__main__":
    main()
