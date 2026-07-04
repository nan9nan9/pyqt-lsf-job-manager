#!/usr/bin/env python3
"""예제 01 — 최소 사용법 (GUI 없이 콘솔, README §1 Quick Start).

3줄 핵심:
    mgr = LsfJobManager()
    js = mgr.submit(commands)
    js.updated.connect(...)

실행:  python examples/01_minimal_console.py
"""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from qtpy.QtCore import QCoreApplication

from common import format_summary, install_logging, make_manager, maybe_autoquit

N = 200

app = QCoreApplication(sys.argv)
install_logging()
mgr, sim = make_manager(poll_interval_s=5)      # 시뮬레이터 주입

# ---- 여기부터가 실제 사용 코드 3줄 ------------------------------------
js = mgr.submit([f"hspice run_{i}.sp" for i in range(N)])
js.updated.connect(lambda s: print("  " + format_summary(s)))
js.finished.connect(lambda rpt: print(
    f"submit 완료: ok={rpt.ok}/{rpt.total} failed={rpt.failed} "
    f"({rpt.duration_s:.1f}s)"))
# -----------------------------------------------------------------------

print(f"jobset {js.id} — {N}개 submit (polling 자동 시작, AUTO-1)")
print("전원 terminal이 되면 polling 자동 중지(AUTO-2) 후 종료합니다...")

# 갱신마다 1회 조회를 예약해 데모를 빠르게 (실전에서는 poll_interval로 충분)
def check_done(_summary):
    if js.is_done:
        print("전원 terminal — 종료")
        print("최종:", format_summary(js.summary))
        app.quit()
js.updated.connect(check_done)
js.start_polling(interval_s=1)                  # 데모용 빠른 주기

maybe_autoquit(app)
app.exec()
mgr.shutdown()                                  # AUTO-3도 있지만 명시 호출 예시
