"""전체 정독 리뷰 사이클 13에서 확정된 회귀 테스트.

사이클 12에서 부분 확인에 그쳤던 jobset_core·manager 명령 API 정독.
**live 결함은 없었고**, 두 파일이 스스로 세운 규율에서 한 군데씩 벗어나
있던 것만 방어(hardening)로 맞췄다.

- C13-1: local_close_jobset만 _meta_lock 밖에서 JobSetRecord를 읽고-고쳐-썼다.
  JobSetRecord 갱신은 전부 jobset_core 안에 있고 모두 main 스레드 manager
  API에서만 불려 지금은 경합이 없다(= live 결함 아님). 다만 그중 하나가
  off-main으로 옮겨지면 close가 그 사이 바뀐 intended_count를 옛 값으로
  되돌리고, make_summary의 missing이 음수가 되어 무시되므로 summary
  불변식(합계 == intended_count)이 영구 파손된다.
- C13-2: kill_jobs()가 세 형태 중 어느 것도 아닐 때 list(None)의 불투명한
  TypeError("'NoneType' object is not iterable")만 났다 — 바로 위 오용 검사
  3개는 전부 쓸 형태를 짚어 준다. 특히 kill_jobs(jobset_id=...)는 "이 jobset
  전체를 죽이겠다"는 자연스러운 오해라 도달하기 쉽다.
"""
from __future__ import annotations


def test_close_jobset_holds_meta_lock():
    """close 경로가 다른 갱신 경로와 같은 lock 규율을 따른다."""
    import threading
    from lsfmgr import InMemoryStore
    from lsfmgr.jobset_core import JobSetManager

    core = JobSetManager(InMemoryStore())
    js = core.local_create_jobset(0, label="x")
    seen = []
    real = core._meta_lock

    class _Probe:
        def __enter__(self):
            seen.append("acquired")
            return real.__enter__()

        def __exit__(self, *a):
            return real.__exit__(*a)

    core._meta_lock = _Probe()
    core.local_close_jobset(js.jobset_id)
    assert seen == ["acquired"], "close가 _meta_lock 없이 갱신했다"
    assert isinstance(real, type(threading.RLock()))


def test_close_preserves_intended_count(qtbot, manager):
    """close가 intended_count를 건드리지 않는다(불변식 유지)."""
    js = manager.create_jobset(["customwrapper_sub a.sp",
                                "customwrapper_sub b.sp"])
    before = manager.summary(js.id)["total"]
    manager.close(js.id, force=True)
    recs = [r for r in manager.list_jobsets() if r.jobset_id == js.id]
    assert recs and recs[0].intended_count == before == 2


def test_kill_jobs_without_target_raises_actionable_error(qtbot, manager):
    """대상 없는 호출은 쓸 형태를 짚어 주는 TypeError."""
    import pytest as _pytest
    for call in (lambda: manager.kill_jobs(),
                 lambda: manager.kill_jobs(jobset_id="js_x")):
        with _pytest.raises(TypeError) as ei:
            call()
        msg = str(ei.value)
        assert "kill_jobs" in msg and "mgr.kill(js)" in msg
        assert "NoneType" not in msg          # 불투명 에러 재발 방지
