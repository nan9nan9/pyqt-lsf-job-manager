"""store_delete_jobs / get_jobs_by_keys — 배치 API 계약 테스트.

transition_many와 같은 자리의 최적화다(건별 호출이 백엔드 lock을 job 수만큼
잡았다 놓는 것을 없앤다). 그래서 지켜야 할 계약도 같은 모양이다 — 개별
store_delete_job을 반복한 것과 **결과가 구별되지 않아야** 한다.

특히 base의 기본 구현(건별 반복)과 백엔드 오버라이드(lock 1회)를 **같은
스위트로** 대조한다. 기본 구현은 새 백엔드가 오버라이드하지 않았을 때 쓰는
경로인데, 유일한 백엔드가 오버라이드해 버려서 아무도 안 밟는다 — 여기서
직접 태우지 않으면 계약이 갈라져도 아무 테스트도 울지 않는다.
"""
from __future__ import annotations

import pytest

from lsfmgr.errors import JobNotFoundError
from lsfmgr.states import JobRecord, JobSetRecord, JobState
from lsfmgr.store.base import JobSetStore


def _seed(store, n=5):
    store.store_insert_jobset(JobSetRecord(jobset_id="js", intended_count=n))
    store.store_add_jobs([
        JobRecord(job_id=1000 + i, array_index=None, jobset_id="js",
                  job_key=f"k{i}", state=JobState.PEND, command="e")
        for i in range(n)])


#: 백엔드 오버라이드 / base 기본 구현 두 가지로 같은 스위트를 돌린다.
@pytest.fixture(params=["override", "base_default"])
def delete_jobs(request):
    if request.param == "override":
        return lambda store, jsid, keys: store.store_delete_jobs(jsid, keys)
    return lambda store, jsid, keys: JobSetStore.store_delete_jobs(
        store, jsid, keys)


def test_deletes_all_given_keys(store, delete_jobs):
    _seed(store, 5)
    gone = delete_jobs(store, "js", ["k1", "k3"])
    assert [r.job_key for r in gone] == ["k1", "k3"]      # 입력 순서
    assert {r.job_key for r in store.get_jobs("js")} == {"k0", "k2", "k4"}


def test_missing_key_skipped_silently(store, delete_jobs):
    """사이클 도중 다른 스레드가 먼저 지운 키 — 예외 없이 건너뛴다."""
    _seed(store, 3)
    gone = delete_jobs(store, "js", ["k0", "없는키", "k2"])
    assert [r.job_key for r in gone] == ["k0", "k2"]
    assert {r.job_key for r in store.get_jobs("js")} == {"k1"}


def test_empty_keys_is_noop(store, delete_jobs):
    _seed(store, 2)
    assert delete_jobs(store, "js", []) == []
    assert len(store.get_jobs("js")) == 2


def test_missing_jobset_is_not_an_error(store, delete_jobs):
    """사라진 jobset(remove_jobset 경합)도 '지울 것이 없다'로 끝난다 —
    없는 키와 같은 취급이다. 여기서 예외가 나면 정리 경로가 통째로 끊긴다."""
    assert delete_jobs(store, "없는jobset", ["k0"]) == []


def test_matches_individual_deletes(store, delete_jobs):
    """일괄 삭제 == 건별 삭제 반복 (요약/카운터까지 동일)."""
    _seed(store, 6)
    delete_jobs(store, "js", ["k0", "k1", "k2"])
    batch = store.summary("js")

    store.store_delete_jobset("js")
    _seed(store, 6)
    for key in ("k0", "k1", "k2"):
        store.store_delete_job("js", key)
    assert store.summary("js") == batch


def test_summary_counter_stays_exact(store, delete_jobs):
    """summary는 증분 카운트로 만든다 — 일괄 경로가 카운터를 안 줄이면
    지워진 job이 요약에 영원히 남는다(전수 스캔과 대조)."""
    _seed(store, 5)
    delete_jobs(store, "js", ["k0", "k2", "k4"])
    recount = {"total": 5, JobState.PEND.value: 2,
               JobState.CREATED.value: 3}   # 미생성 몫은 CREATED로 계상
    assert store.summary("js") == recount


def test_deleted_job_is_gone(store, delete_jobs):
    _seed(store, 2)
    delete_jobs(store, "js", ["k0"])
    with pytest.raises(JobNotFoundError):
        store.get_job("js", "k0")


# ----------------------------------------------------------------------
# get_jobs_by_keys — 배치 읽기 (같은 이유, 같은 규약)
# ----------------------------------------------------------------------
@pytest.fixture(params=["override", "base_default"])
def jobs_by_keys(request):
    if request.param == "override":
        return lambda store, jsid, keys: store.get_jobs_by_keys(jsid, keys)
    return lambda store, jsid, keys: JobSetStore.get_jobs_by_keys(
        store, jsid, keys)


def test_reads_only_given_keys(store, jobs_by_keys):
    _seed(store, 5)
    got = jobs_by_keys(store, "js", ["k1", "k3"])
    assert set(got) == {"k1", "k3"}
    assert got["k1"].job_id == 1001 and got["k3"].job_id == 1003


def test_read_missing_key_omitted(store, jobs_by_keys):
    _seed(store, 3)
    assert set(jobs_by_keys(store, "js", ["k0", "없는키"])) == {"k0"}


def test_read_empty_keys(store, jobs_by_keys):
    _seed(store, 2)
    assert jobs_by_keys(store, "js", []) == {}


def test_read_missing_jobset_is_empty(store, jobs_by_keys):
    """get_jobs와 달리 예외가 아니다 — 삭제 경합과 '키 없음'이 같은 답이다.
    (base 기본 구현이 get_job의 JobSetNotFoundError를 흘리면 여기서 걸린다)"""
    assert jobs_by_keys(store, "없는jobset", ["k0"]) == {}


def test_read_matches_individual_gets(store, jobs_by_keys):
    _seed(store, 6)
    keys = ["k0", "k2", "k5", "없는키"]
    got = jobs_by_keys(store, "js", keys)
    one = {}
    for k in keys:
        try:
            one[k] = store.get_job("js", k)
        except JobNotFoundError:
            pass
    assert got == one


def test_read_does_not_mutate(store, jobs_by_keys):
    """읽기다 — 요약/카운터가 흔들리면 안 된다."""
    _seed(store, 4)
    before = store.summary("js")
    jobs_by_keys(store, "js", ["k0", "k1", "없는키"])
    assert store.summary("js") == before
