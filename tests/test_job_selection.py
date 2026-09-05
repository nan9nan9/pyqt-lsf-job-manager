"""선택 작업의 해석·편집 비용은 선택 key 수를 따른다."""
import pytest

from lsfmgr import JobState


@pytest.mark.parametrize("operation", ["metadata", "can_submit", "add", "replace", "remove"])
def test_key_operation_does_not_read_unselected_records(manager, monkeypatch, operation):
    js = manager.create_jobset(["tool a", "tool b"], job_keys=["a", "b"])

    def full_read(*args, **kwargs):
        raise AssertionError("선택 key 연산이 전체 레코드를 읽음")

    monkeypatch.setattr(manager.store, "get_jobs", full_read)
    # 편집 후 자동 폴링의 별도 조회는 이 동기 API 비용 검사에서 제외한다.
    monkeypatch.setattr(manager, "start_polling", lambda *a, **kw: None)
    if operation == "metadata":
        assert manager.set_user_data(js, "a", {"v": 1}).user_data == {"v": 1}
    elif operation == "can_submit":
        assert manager.can_submit(js, only=["a"])
    elif operation == "add":
        assert manager.add_jobs(js, ["tool c"], job_keys=["c"])[0].job_key == "c"
        assert js.summary["total"] == 3
    elif operation == "replace":
        assert manager.replace_jobs(js, ["tool new"], job_keys=["a"])[0].command == "tool new"
        assert js.summary["total"] == 2
    else:
        assert manager.remove_jobs(js, ["a"])[0].job_key == "a"
        assert js.summary["total"] == 1


def test_remove_cleans_ids_from_the_records_actually_deleted(manager, monkeypatch):
    js = manager.create_jobset(["tool a"], job_keys=["a"])
    store = manager.store
    store.transition(js.id, "a", JobState.SUBMITTING)
    original = store.store_delete_jobs
    forgotten = []

    def complete_submit_then_delete(jsid, keys):
        store.transition(jsid, "a", JobState.PEND, job_id=1234)
        return original(jsid, keys)

    monkeypatch.setattr(store, "store_delete_jobs", complete_submit_then_delete)
    monkeypatch.setattr(manager.command, "forget_status", forgotten.extend)
    removed = manager.remove_jobs(js, ["a"], force=True)
    assert removed[0].job_id == 1234
    assert forgotten == [1234]


@pytest.mark.parametrize("invalid", ["wrong_owner", "duplicate"])
def test_edit_validates_entire_record_batch_before_writing(manager, invalid):
    from dataclasses import replace

    js = manager.create_jobset(["tool a", "tool b"], job_keys=["a", "b"])
    before = js.jobs()
    valid = replace(before[0], command="new a")
    bad = (replace(before[1], jobset_id="foreign") if invalid == "wrong_owner"
           else replace(valid, command="duplicate"))
    with pytest.raises(ValueError):
        manager.jobsets.local_edit_jobs(js.id, [valid, bad], policy="replace")
    assert js.jobs() == before
