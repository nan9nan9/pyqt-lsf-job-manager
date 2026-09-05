"""전체 정독 리뷰 11차 — 공개 API 인자 경계 훑기에서 나온 4건.

- R11-1: QTimer의 ms 인자는 int32(약 24.8일)다. 넘기면 OverflowError가 나고
  그게 **slot 안에서** 터지면 그 타이머는 영영 안 걸리는데 호출자는 성공한
  줄 안다. 코드베이스가 이미 아는 함정인데(options.MAX_RETRY_DELAY_S) 세 곳
  중 한 곳만 막혀 있었다 — start_polling(interval)과 pacer 재예약이 뚫렸다.
  → qt.timer_ms()로 기계적 clamp를 한 곳에 모으고, 공개 진입점
    (start_polling / min_state_dwell_s)에는 명확한 검증을 따로 둔다.

- R11-2: submit()의 옵션 검증이 **상태 가드보다 뒤**였다. 활성 jobset에
  오타 옵션(workers→worker)을 주면 "활성 job이 있어 submit 불가"가 먼저 나와
  엉뚱한 것을 고치게 된다. 오타는 상태와 무관한 프로그래밍 오류다.

- R11-3: only=/refs=에 문자열을 통째로 넘기면 시퀀스라 **글자 단위로 분해**
  된다. only="ab"가 key "a"와 "b"를 가리켜, 그런 key가 실제로 있으면 엉뚱한
  job이 조용히 제출/삭제된다. kill_jobs는 이미 막고 있던 함정이다.

- R11-4: intended_count가 음수여도 통과했다 — summary의 total이 음수가 되어
  "상태 합계 == total" 불변식이 성립할 수 없다(합계는 0 이상).
"""
from __future__ import annotations

import pytest

from lsfmgr import InMemoryStore, JobState, LsfConfig, LsfJobManager
from lsfmgr.qt import MAX_TIMER_MS, timer_ms


# ----------------------------------------------------------------------
# R11-1 — QTimer int32
# ----------------------------------------------------------------------
def test_timer_ms_clamps_to_int32():
    assert timer_ms(-5) == 0
    assert timer_ms(0.25) == 250
    assert timer_ms(10 ** 9) == MAX_TIMER_MS
    assert timer_ms(float("nan")) == MAX_TIMER_MS
    assert -2147483648 <= timer_ms(10 ** 12) <= MAX_TIMER_MS


def test_start_polling_rejects_an_absurd_interval(qtbot, manager):
    js = manager.create_jobset(["mytool a.sp"], job_keys=["a"])
    for bad in (0, -1, 10 ** 9):
        with pytest.raises(ValueError):
            manager.start_polling(js, bad)
    manager.start_polling(js, 5.0)                # 정상값은 그대로


def test_huge_dwell_is_rejected_not_crashing():
    """상한이 없으면 pacer 재예약 slot에서 OverflowError가 나 전이 표시가 멎는다."""
    with pytest.raises(ValueError):
        LsfConfig(min_state_dwell_s=10 ** 9)
    LsfConfig(min_state_dwell_s=2.0)              # 정상값은 그대로


def test_pacer_survives_a_large_dwell(qtbot, fake_lsf):
    """설정 상한(1시간) 안에서는 재예약이 터지지 않는다."""
    mgr = LsfJobManager(store=InMemoryStore(),
                        config=LsfConfig(
                                         min_state_dwell_s=3600.0),
                        runner=fake_lsf)
    try:
        js = mgr.create_jobset(["mytool a.sp"], job_keys=["a"])
        rec = js.jobs()[0]
        mgr._pacer.push(js.id, [rec])
        mgr._pacer.push(js.id, [rec.__class__(
            **{**rec.__dict__, "state": JobState.PEND})])
    finally:
        mgr.shutdown()


# ----------------------------------------------------------------------
# R11-2 — 인자 오류가 상태 오류보다 먼저
# ----------------------------------------------------------------------
def test_option_typo_is_reported_even_on_a_busy_jobset(qtbot, manager):
    js = manager.create_jobset(["mytool a.sp"], job_keys=["k"])
    with qtbot.waitSignal(manager.submit_finished, timeout=10000):
        manager.submit(js, auto_poll=False)
    assert not manager.can_submit(js)             # 이제 활성(PEND)
    with pytest.raises(TypeError) as ei:
        manager.submit(js, worker=8)              # 오타
    assert "worker" in str(ei.value), str(ei.value)


# ----------------------------------------------------------------------
# R11-3 — 문자열을 목록 자리에 넘기면 글자 단위로 분해된다
# ----------------------------------------------------------------------
def test_string_is_not_accepted_where_a_ref_list_is_expected(manager):
    js = manager.create_jobset(["mytool a.sp", "mytool b.sp", "mytool c.sp"],
                               job_keys=["ab", "a", "b"])
    with pytest.raises(TypeError):
        manager.submit(js, only="ab")             # ['a','b']로 분해될 뻔
    with pytest.raises(TypeError):
        manager.remove_jobs(js, "ab")
    # 목록으로 주면 정상 — 'ab' 하나만 대상
    assert [r.job_key for r in manager.jobsets.submit_targets(js.id, ["ab"])] == ["ab"]


# ----------------------------------------------------------------------
# R11-4 — 음수 intended_count
# ----------------------------------------------------------------------
def test_negative_intended_count_is_rejected(manager):
    with pytest.raises(ValueError):
        manager.create_jobset([], intended_count=-5)
    js = manager.create_jobset([], intended_count=3)   # 0 이상은 그대로
    s = manager.summary(js)
    assert s["total"] == 3 and s["CREATED"] == 3
