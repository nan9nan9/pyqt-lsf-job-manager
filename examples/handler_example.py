#!/usr/bin/env python3
"""JobSet handler 예제 — job 별 주기 실행 + 최종 수집 (콘솔).

시나리오: 시뮬레이션이 도는 동안 **각 job 의 출력 파일을 몇 초마다 파싱**해
진행 상황(중간 결과)을 수집하고, job 이 끝나면(DONE/EXIT) **최종 수집을 한 번 더**
수행한다. 이 "주기 파싱" 작업을 lsfmgr 의 JobSet handler 로 붙인다.

핵심 API:
  - mgr.add_handler(js, name, fn, start_states=…, end_states=…)  # 폴링 사이클 구동
      · fn 은 **worker 스레드**에서 실행된다 (GUI/이벤트 루프 안 막음)
      · fn(ctx) 의 ctx 는 job 참조 포인트 — ctx.job_id / ctx.working_dir /
        ctx.record(JobRecord: run_time_s, state, …) / ctx.final
      · fn 의 반환값은 handler_finished Signal 로 전달된다
  - mgr.handler_finished(jsid, name, HandlerResult) — 실행 1회 완료마다 발행
      · res.final=True 는 종료 state 에서의 마지막 실행 (그 후 그 job 은 끝)
      · 모든 job 이 최종 실행까지 끝나면 handler 는 휴면한다 (재실행 시 자동 재가동)
  - mgr.submit(js, post_process=fn)  # 완료 후처리 (대비되는 훅)
      · handler 는 **job 별·폴링 사이클마다**, post_process 는 **jobset 단위·전원
        terminal 시 딱 1회**. 종합 리포트/정리를 한 곳에서 수행할 때 쓴다.
      · 결과는 post_processing_finished(jsid, result) Signal 로 전달된다

여기서는 mocklsf 가 job 마다 남기는 출력 파일($MOCKLSF_HOME/jobout/<id>.out)을
파싱 대상으로 쓴다. 실제 환경이라면 ctx.working_dir(LSF exec_cwd) 아래의
시뮬레이션 로그/결과 파일을 파싱하면 된다.

실행:  python examples/handler_example.py
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from qtpy.QtCore import QCoreApplication, QTimer

from lsfmgr import JobState
from common import format_summary, install_logging, make_manager, wrapper

N_JOBS = 4
HANDLER = "collect"          # handler 이름 — Signal 필터링에 쓴다


def parse_job_output(ctx):
    """handler 본체 — worker 스레드에서 실행된다.

    job 출력 파일을 파싱해 (라인 수, 마지막 줄) 을 돌려준다. 반환값은
    handler_finished Signal 의 HandlerResult.data 로 전달된다.
    blocking I/O 여도 괜찮다 — main 스레드(GUI)를 막지 않는다.
    """
    out = os.path.join(os.environ["MOCKLSF_HOME"], "jobout",
                       f"{ctx.job_id}.out")
    try:
        with open(out, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        lines = []

    # 실행 시간: 종료 job 은 LSF 확정값(run_time_s), 실행 중이면 start_time
    # 기준으로 현재까지 경과를 계산한다 (run_time_s 는 매 폴링 갱신되지 않음).
    rec = ctx.record
    if ctx.final and rec.run_time_s is not None:
        elapsed = rec.run_time_s
    elif rec.start_time is not None:
        elapsed = int((datetime.now() - rec.start_time).total_seconds())
    else:
        elapsed = None
    return {
        "lines": len(lines),
        "last": lines[-1] if lines else "(출력 없음)",
        # LSF 가 채워주는 참조 정보도 함께 — 실제라면 이 디렉토리를 파싱한다.
        "cwd": ctx.working_dir,
        "elapsed_s": elapsed,
    }


def summarize_run(records):
    """완료 후처리 콜백 — **worker 스레드에서 실행, GUI 접근 금지**.

    전원 terminal 도달 시 딱 1회 호출된다. handler 가 job 별로 매 폴링 수집하는
    것과 달리, 여기선 최종 레코드로 **jobset 단위 종합**을 한 번에 만든다 —
    실제 앱이라면 각 job 결과 파일을 수합해 리포트를 쓰는 자리다.
    반환값은 post_processing_finished Signal 로 전달된다.
    """
    from collections import Counter
    counts = Counter(r.state.name for r in records)
    total_run = sum(r.run_time_s or 0 for r in records)
    return {"n": len(records), "states": dict(counts),
            "total_run_s": total_run}


def main():
    app = QCoreApplication(sys.argv)
    install_logging()
    mgr, _ = make_manager()

    # --- handler 결과 구독: 이름으로 필터, 중간/최종을 구분해 출력 (job 별) ---
    def on_handler(jsid, name, res):
        if name != HANDLER:
            return
        if res.error:
            print(f"  [handler 오류] {res.job_key}: {res.error}")
            return
        d = res.data
        kind = "최종" if res.final else "중간"
        print(f"  [{kind}] {res.job_key} (job {res.job_id}): "
              f"{d['lines']}줄, 경과={d['elapsed_s']}s, "
              f"마지막='{d['last'][:60]}'")

    mgr.handler_finished.connect(on_handler)

    # --- 완료 후처리 구독: 전원 terminal 시 1회 (jobset 단위 종합) → 종료 ---
    def on_post_process(jsid, result):
        if result is None:
            print("\n[post_process] 후처리 실패")
        else:
            print(f"\n[post_process] 전원 terminal — 종합 {result['states']}, "
                  f"총 실행시간 {result['total_run_s']}s ({result['n']} jobs)")
            print("최종 요약:", format_summary(mgr.summary(jsid)))
        QTimer.singleShot(200, app.quit)

    mgr.post_processing_finished.connect(on_post_process)

    # --- 제출(post_process 등록) + polling + handler 등록 ---
    cmds = [wrapper("customwrapper_sub", "-q", "normal", f"run_{i}.sp")
            for i in range(N_JOBS)]
    js = mgr.create_jobset(cmds, label="handler-demo")   # wrapper 커맨드 그대로
    mgr.submit(js, auto_poll=False, post_process=summarize_run)
    mgr.start_polling(js, 1)           # 데모: 상태 전이를 촘촘히 관찰
    print(f"제출: {N_JOBS} jobs → jobset {js.id}")

    # RUN 이 되면 폴링 사이클마다 출력 파싱, DONE/EXIT 시 최종 파싱 1회.
    # (별도 주기 없이 poll_interval_s 에 tie — 여기선 polling 1초)
    mgr.add_handler(js, HANDLER, parse_job_output,
                    start_states={JobState.RUN},
                    end_states={JobState.DONE, JobState.EXIT})
    print(f"handler '{HANDLER}' 등록 — 폴링마다(RUN 중), 종료 시 최종 1회\n")

    # 안전망: 최대 90초 후 강제 종료 (mocklsf 지연 대비).
    QTimer.singleShot(90_000, app.quit)
    app.exec()
    mgr.shutdown()


if __name__ == "__main__":
    main()
