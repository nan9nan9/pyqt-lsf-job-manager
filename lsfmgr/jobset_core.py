"""JobSetManager — JobSet CRUD / 요약 / 손실 감지 / job 편집 / 삭제.

Store만 사용 (Qt 비의존). v10: LSF 호출(부착물 name 역조회·bgdel)이
전부 제거되어 이 계층은 순수 Store 연산이다.

이름 규약(계층이 이름에 드러나게): 공개 API의 두 3형제는 **하려는 일이
이름에 드러난다**.
  삭제: 선택분=remove_jobs, job 전부=clear_jobs, jobset 자체=remove_jobset
  편집: 추가=add_jobs, 교체=replace_jobs, 있으면 교체/없으면 추가=upsert_jobs
각각 도메인에선 한 몸통을 공유한다(_delete_jobs / local_edit_jobs) — 가드와
intended_count 규칙이 갈릴 수 없게.
도메인=local_* (local_create_jobset/local_create_jobs, local_edit_jobs,
local_remove_jobs/local_clear_jobs, local_remove_jobset), 저장소=store_*
(store_insert_jobset/store_add_jobs/store_delete_job/store_dispose).
"""
from __future__ import annotations

import logging
import shlex
import threading
import uuid
from dataclasses import replace
from datetime import datetime
from typing import List, Optional, Sequence

from .errors import (
    JobEditNotAllowedError,
    JobNotFoundError,
    RemoveJobSetNotAllowedError,
    RemoveNotAllowedError,
    SubmitNotAllowedError,
)
from .states import JobRecord, JobSetRecord, JobState
from .store.base import JobSetStore

log = logging.getLogger("lsfmgr.jobset")


def generate_jobset_id() -> str:
    """timestamp + uuid 조합."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"js_{ts}_{uuid.uuid4().hex[:8]}"


class JobSetManager:

    def __init__(self, store: JobSetStore):
        self.store = store
        # JobSetRecord read-modify-write 직렬화 — Store는 개별 연산만
        # 원자적이므로, intended_count 갱신처럼 "읽고-고쳐-쓰는" 경로가 겹치면
        # 한쪽 갱신이 유실된다 (예: local_create_jobs vs local_edit_jobs).
        self._meta_lock = threading.RLock()

    @staticmethod
    def _check_records(jobset_id: str, records: Sequence[JobRecord]) -> None:
        """생성·편집 배치의 소속과 key 유일성을 쓰기 전에 검사한다."""
        keys = set()
        for rec in records:
            if rec.jobset_id != jobset_id:
                raise ValueError(
                    f"레코드 jobset_id 불일치: {rec.jobset_id!r} != "
                    f"{jobset_id!r} ({rec.job_key})")
            if not isinstance(rec.job_key, str) or not rec.job_key.strip():
                raise ValueError(f"job_key는 비어있지 않은 문자열이어야 합니다: {rec.job_key!r}")
            if rec.job_key in keys:
                raise ValueError(f"job_key 중복(한 호출 안에서): {rec.job_key!r}")
            keys.add(rec.job_key)

    # ------------------------------------------------------------------
    # 생성
    # ------------------------------------------------------------------
    @staticmethod
    def build_records(jsid: str, commands: Sequence,
                      job_keys: Optional[Sequence[str]],
                      user_datas: Optional[Sequence[Optional[dict]]],
                      work_dir: Optional[str],
                      work_dirs: Optional[Sequence[Optional[str]]]) -> List[JobRecord]:
        """commands → CREATED JobRecord 목록 (create_jobset / _edit_jobs 공용).
        submit_cwd: work_dir(전체 단일) 또는 work_dirs(job별) — 동시 지정 불가.
        job_keys: job별 키 — **필수**이며 jobset 안에서 유일해야 한다."""
        if isinstance(commands, (str, bytes)):
            raise TypeError("commands는 문자열이 아닌 커맨드 목록이어야 합니다")
        items = list(commands)
        if not items:
            return []
        if work_dir is not None and work_dirs is not None:
            raise ValueError(
                "work_dir와 work_dirs는 동시에 지정할 수 없습니다(둘 중 하나)")
        if job_keys is None:
            raise ValueError(
                "job_keys는 필수입니다 — 각 job의 키를 앱이 정해야 합니다"
                " (commands와 같은 길이)")
        keys_in = list(job_keys)
        uds = list(user_datas) if user_datas is not None else [None] * len(items)
        # work_dir 단일 지정이면 전 job에 적용, 아니면 work_dirs(job별) 사용
        wds = (list(work_dirs) if work_dirs is not None
               else [work_dir] * len(items))
        if (len(keys_in) != len(items) or len(uds) != len(items)
                or len(wds) != len(items)):
            raise ValueError(
                "job_keys/user_datas/work_dirs 길이가 commands와 다릅니다")

        # 기존 키와의 충돌은 add/replace/upsert 정책을 아는 local_edit_jobs가 검사한다.
        records = []
        for item, key, ud, cwd in zip(items, keys_in, uds, wds):
            argv = (shlex.split(item) if isinstance(item, str)
                    else [str(t) for t in item])
            if not argv:
                raise ValueError("빈 커맨드는 job으로 만들 수 없습니다")
            records.append(JobRecord(
                job_id=None, array_index=None, jobset_id=jsid,
                job_key=key, state=JobState.CREATED,
                command=shlex.join(argv),
                user_data=ud, submit_cwd=cwd))
        JobSetManager._check_records(jsid, records)
        return records

    def resolve_refs(self, jobset_id: str,
                      refs: Sequence) -> List[JobRecord]:
        """ref 목록 → 레코드 목록 — remove_jobs·submit(only=) 공용.

        ref는 job_key(str) 또는 job_id(int). 같은 job을 두 형태로 지정하면
        1회로 접히고, 순서는 지정 순서를 따른다. 없는 ref는 JobNotFoundError.

        job_id 색인은 **parent만** 담는다 — array element는 parent와 job_id를
        공유하므로 전부 넣으면 마지막 element가 parent를 덮어, job_id로 지정한
        parent가 엉뚱하게 element로 해석된다. element를 콕 집으려면 그
        element의 job_key를 쓴다."""
        if isinstance(refs, str):
            # 문자열은 시퀀스라 글자 단위로 분해된다 — only="ab"가 key "a"와
            # "b"를 가리키게 되어, 그런 key가 실제로 있으면 **엉뚱한 job이
            # 조용히 제출/삭제된다**. kill_jobs와 같은 규칙으로 막는다.
            raise TypeError(
                f"job 지정은 문자열이 아니라 목록이어야 합니다 — "
                f"하나면 [{refs!r}] (got {refs!r})")
        self.store.get_jobset(jobset_id)
        refs = list(refs)
        if any(isinstance(ref, int) for ref in refs):
            jobs = self.store.get_jobs(jobset_id)
            by_key = {r.job_key: r for r in jobs}
        else:
            jobs = []
            by_key = self.store.get_jobs_by_keys(jobset_id, refs)
        by_jid = {r.job_id: r for r in jobs
                  if r.job_id is not None and r.array_index is None}
        out: List[JobRecord] = []
        seen = set()
        for ref in refs:
            rec = (by_jid.get(ref) if isinstance(ref, int)
                   else by_key.get(ref))
            if rec is None:
                if isinstance(ref, int) and any(r.job_id == ref for r in jobs):
                    # element의 허용 여부는 명령마다 다르므로 여기서는 해당 ID의 형태만 알린다.
                    raise ValueError(
                        f"job_id={ref}에 해당하는 job이 array element뿐입니다"
                        f" (그 id의 parent 레코드가 없음)")
                raise JobNotFoundError(f"{jobset_id}/{ref}")
            if rec.job_key in seen:              # 같은 job을 두 형태로 지정
                continue
            seen.add(rec.job_key)
            out.append(rec)
        return out

    def submit_targets(self, jobset_id: str,
                        only: Optional[Sequence]) -> List[JobRecord]:
        """제출 대상 레코드 — only=None이면 전 job, 아니면 지정분만.

        array element(array_index 지정분)는 개별 제출 대상이 아니다 — parent
        job 하나가 element 전체를 만들므로 element만 따로 제출할 수 없다.
        전체 제출에서는 조용히 걸러지고, only로 콕 집으면 오류로 알린다
        (조용히 무시하면 '선택했는데 안 돌았다'가 된다)."""
        if only is None:
            targets = [r for r in self.store.get_jobs(jobset_id)
                       if r.array_index is None]
        else:
            targets = self.resolve_refs(jobset_id, only)
        for rec in targets:
            if rec.array_index is not None:
                raise ValueError(
                    f"array element는 개별 제출할 수 없습니다:"
                    f" {rec.job_key!r} (parent job을 제출하세요)")
        if not targets:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 제출할 job이 없습니다"
                + (" (only=[] — 빈 선택)" if only is not None else ""),
                jobset_id=jobset_id)
        busy = [r.job_key for r in targets if not r.state.is_inactive]
        if busy:
            raise SubmitNotAllowedError(
                f"{jobset_id}: 활성(진행 중) job이 있어 submit 불가 — "
                f"{busy[:5]} (먼저 kill 하거나 완료를 기다리세요)",
                jobset_id=jobset_id, job_keys=busy)
        return targets

    def local_create_jobset(self, intended_count: int, *, label: str = "",
                            tags: Sequence[str] = (),
                            jobset_id: Optional[str] = None) -> JobSetRecord:
        if intended_count < 0:
            # 음수면 summary의 total이 음수가 되어 "상태 합계 == total"
            # 불변식이 성립할 수 없다(합계는 0 이상이다).
            raise ValueError(
                f"intended_count는 0 이상이어야 합니다 (got {intended_count})")
        jsid = jobset_id or generate_jobset_id()
        record = JobSetRecord(
            jobset_id=jsid, intended_count=intended_count,
            label=label, tags=list(tags),
            created_at=datetime.now())
        return self.store.store_insert_jobset(record)

    # ------------------------------------------------------------------
    # job 추가/편집 — 생성 시 일괄=local_create_jobs, 이후=local_edit_jobs
    # ------------------------------------------------------------------
    def local_create_jobs(self, jobset_id: str,
                          records: Sequence[JobRecord]) -> List[JobRecord]:
        """제출 전(CREATED) 레코드 일괄 생성 — 바구니 누적.

        job_key 중복을 선검사하고 단일 배치로 추가한다 — job_key는 jobset 내
        유일해야 교체/조회가 결정적이다. intended_count는 1회 갱신."""
        records = list(records)
        if not records:
            return []
        with self._meta_lock:
            self.store.get_jobset(jobset_id)
            self._check_records(jobset_id, records)
            existing = self.store.get_jobs_by_keys(
                jobset_id, [r.job_key for r in records])
            for rec in records:
                if rec.job_key in existing:
                    raise ValueError(f"job 이름 중복: {jobset_id}/{rec.job_key}")
            out = self.store.store_add_jobs(records)
            js = self.store.get_jobset(jobset_id)
            count = self.store.count_jobs(jobset_id)
            if count > js.intended_count:
                self.store.update_jobset(
                    replace(js, intended_count=count))
        return out

    def local_edit_jobs(self, jobset_id: str, records: List[JobRecord], *,
                        policy: str, force: bool = False) -> List[JobRecord]:
        """job 목록 편집의 **공용 몸통** — add/replace/upsert의 유일한 차이는
        `policy`로 표현되는 "job_key가 이미 있을 때 어떻게 하느냐"뿐이다.

            add     : 있으면 ValueError (순수 추가)
            replace : 없으면 JobNotFoundError (순수 교체)
            upsert  : 있으면 교체, 없으면 추가

        교체는 같은 job_key 자리에 새 내용을 넣는다 — 키가 곧 행의 정체성
        이므로 GUI 표의 행이 그대로 이어진다. 교체 대상이 활성이면
        JobEditNotAllowedError (force=True면 레코드만 강제 교체 — LSF 정리는
        caller 책임).

        소속·중복 key·활성 상태 검증은 변경 전에 모두 끝낸다.
        잘못된 입력 때문에 배치의 일부만 변경되지 않도록 한다.

        반환: 추가·교체된 레코드 목록 (신호 발행용)."""
        if policy not in ("add", "replace", "upsert"):
            raise ValueError(f"policy는 add/replace/upsert (got {policy!r})")
        with self._meta_lock:
            self.store.get_jobset(jobset_id)
            self._check_records(jobset_id, records)
            by_key = self.store.get_jobs_by_keys(
                jobset_id, [r.job_key for r in records])

            # 정책별 매칭 — (새 레코드, 교체 대상 or None)
            plan: List[tuple] = []
            for rec in records:
                old = by_key.get(rec.job_key)
                if policy == "add":
                    if old is not None:
                        raise ValueError(
                            f"job_key가 이미 있습니다: {rec.job_key!r} "
                            f"(교체하려면 replace_jobs/upsert_jobs)")
                elif policy == "replace":
                    if old is None:
                        raise JobNotFoundError(
                            f"{jobset_id}: 교체 대상 없음 (job_key="
                            f"{rec.job_key!r} — 추가하려면 add_jobs/"
                            f"upsert_jobs)")
                plan.append((rec, old))

            # 교체 대상이 활성이면 거부 (force로 강제 — 고아는 로그로)
            olds = [o for _, o in plan if o is not None]
            busy = [o.job_key for o in olds if not o.state.is_inactive]
            if busy and not force:
                raise JobEditNotAllowedError(
                    f"{policy}_jobs 불가 — 교체 대상이 활성(진행 중)인 job "
                    f"{len(busy)}건: {busy[:5]} "
                    f"(force=True로 레코드만 강제 교체 가능)",
                    jobset_id=jobset_id, job_keys=busy)
            _warn_orphans(jobset_id, olds, f"{policy}_jobs")

            # 교체는 제자리 갱신으로 get_jobs의 행 순서를 보존한다.
            changed: List[JobRecord] = []
            for rec, old in plan:
                if old is not None:
                    rec = self.store.update_job(rec)
                else:
                    rec = self.store.store_add_job(rec)
                changed.append(rec)
            self._sync_intended_count(jobset_id)
        return changed

    def _sync_intended_count(self, jobset_id: str) -> None:
        """[_meta_lock 보유 상태에서 호출] intended_count를 **실제 레코드
        수**로 재동기화 — summary 불변식(상태 합계 == intended_count)의
        단일 유지 지점 (편집/삭제 공용). 생성 경로(local_create_jobs)만
        예외적으로 '늘리기만' 한다 — 빈 jobset의 intended_count 선지정을
        보존하기 위해서다."""
        js = self.store.get_jobset(jobset_id)
        n = self.store.count_jobs(jobset_id)
        if js.intended_count != n:
            self.store.update_jobset(replace(js, intended_count=n))

    def local_remove_jobs(self, jobset_id: str, job_keys: Sequence[str], *,
                          force: bool = False) -> List[JobRecord]:
        """지정 job 삭제 — 대상은 job_key 목록으로 받는다.

        ref(job_key/job_id) 해석은 manager가 하고, 여기서는 **lock 안에서**
        키를 레코드로 다시 푼다 — 밖에서 푼 스냅샷을 그대로 쓰면 그 사이
        worker가 상태를 바꿔(CREATED→SUBMITTING) 활성 가드가 낡은 상태를
        보고 통과할 수 있다.

        비활성(CREATED/terminal)만 삭제 가능 — 활성이면 LsfmgrError,
        force=True면 레코드만 강제 삭제(LSF job 정리는 caller 책임).
        intended_count도 함께 줄여 유령 CREATED가 남지 않는다."""
        keys = list(job_keys)
        if not keys:
            # 빈 선택을 "전부"로 해석하지 않는다 — 조용한 no-op으로 넘기면
            # "선택했는데 안 지워졌다"가 묻힌다. 전량 삭제는 clear_jobs로만.
            raise ValueError("remove_jobs: 지울 job을 지정하세요"
                             " (전부 지우려면 clear_jobs)")
        with self._meta_lock:
            self.store.get_jobset(jobset_id)
            by_key = self.store.get_jobs_by_keys(jobset_id, keys)
            targets = []
            for k in keys:
                rec = by_key.get(k)
                if rec is None:
                    raise JobNotFoundError(f"{jobset_id}/{k}")
                targets.append(rec)
            return self._delete_jobs(jobset_id, targets, force=force,
                                     what="remove_jobs")

    def local_clear_jobs(self, jobset_id: str, *,
                         force: bool = False) -> List[JobRecord]:
        """전 job 삭제 — local_remove_jobs와 **같은 몸통**(_delete_jobs)이라
        가드/intended_count 규칙이 갈릴 수 없다. 빈 jobset이면 정상 no-op."""
        with self._meta_lock:
            jobs = self.store.get_jobs(jobset_id)
            return self._delete_jobs(jobset_id, jobs, force=force,
                                     what="clear_jobs")

    def _delete_jobs(self, jobset_id: str, targets: List[JobRecord], *,
                     force: bool, what: str) -> List[JobRecord]:
        """[_meta_lock 보유 상태에서 호출] job 레코드 삭제의 공용 몸통 —
        remove_jobs(선택분)과 clear_jobs(전부)의 유일한 차이는 targets뿐이다.

        비활성(CREATED/terminal)만 삭제 가능 — 활성이면 RemoveNotAllowedError,
        force=True면 레코드만 강제 삭제(LSF job 정리는 caller 책임).
        intended_count는 **남은 실제 개수**로 재동기화한다 — 유령 CREATED가
        남지 않고, 전량 삭제면 자연히 0이 된다(규칙이 한 벌뿐)."""
        busy = [r.job_key for r in targets if not r.state.is_inactive]
        if busy and not force:
            raise RemoveNotAllowedError(
                f"{what} 불가 — 활성(진행 중) job {len(busy)}건: "
                f"{busy[:5]} (force=True로 레코드만 강제 삭제 가능)",
                jobset_id=jobset_id, job_keys=busy)
        _warn_orphans(jobset_id, targets, what)
        # 건별이 아니라 일괄 — 이유는 store.store_delete_jobs 참고
        removed = self.store.store_delete_jobs(jobset_id, [r.job_key for r in targets])
        self._sync_intended_count(jobset_id)
        return removed

    # ------------------------------------------------------------------
    # 손실 감지
    # ------------------------------------------------------------------
    def detect_lost(self, jobset_id: str) -> List[JobRecord]:
        """intended_count 대비 ID 미확보 job을 감지해 LOST 전이한다.
        (v10: name 패턴 역조회 복구는 bjobs group/name 조회 제거와 함께
        삭제 — ID 미확보 job은 재조회 수단이 없어 바로 LOST 확정한다.)
        반환: 이번 호출로 LOST 확정된 레코드 목록."""
        self.store.get_jobset(jobset_id)     # 존재 검증 (없으면 예외)
        records = self.store.get_jobs(jobset_id)
        # ID 미확보이면서 submit이 시도된 (실패 확정도 아닌) 레코드
        candidates = [r for r in records if r.job_id is None
                      and r.state is JobState.SUBMITTING]

        # 스냅샷 이후 job_id가 채워진 레코드는 CAS로 보호한다.
        # GUI의 잠금 경합을 줄이도록 전이를 일괄 적용한다.
        if not candidates:
            return []
        def _still(rec):
            return lambda cur: (cur.same_execution(rec) and cur.job_id is None
                                and cur.state is rec.state)
        specs = [(rec.job_key, JobState.LOST, _still(rec),
                  {"fail_reason": rec.fail_reason or "NO_JOBID_PARSED"})
                 for rec in candidates]
        return self.store.transition_many(jobset_id, specs)

    # ------------------------------------------------------------------
    # 삭제 (jobset 자체)
    # ------------------------------------------------------------------
    def local_remove_jobset(self, jobset_id: str, *,
                            force: bool = False) -> JobSetRecord:
        """전원 terminal이면 jobset을 **레코드째 삭제**한다. 반환은 삭제 직전 스냅샷 — 삭제 후에는 store에서 조회할
        수 없으므로 caller가 결과를 기록해야 하면 이 값을 쓴다.
        (v10: bgdel group 정리 제거 — 부착물이 생성되지 않으므로 정리할
        group도 없다.)"""
        # 메타데이터 검사와 삭제를 같은 잠금 아래 수행한다.
        with self._meta_lock:
            js = self.store.get_jobset(jobset_id)
            records = self.store.get_jobs(jobset_id)
            not_terminal = [r for r in records if not r.state.is_terminal]
            if not_terminal and not force:
                raise RemoveJobSetNotAllowedError(
                    f"terminal이 아닌 job {len(not_terminal)}개 — "
                    f"remove_jobset 불가 (force=True로 강제 가능)",
                    jobset_id=jobset_id,
                    job_keys=[r.job_key for r in not_terminal])
            _warn_orphans(jobset_id, records, "remove_jobset")
            self.store.store_delete_jobset(jobset_id)
            return js


def _warn_orphans(jobset_id: str, targets: List[JobRecord],
                  what: str) -> None:
    """LSF에 살아있는 job의 레코드가 사라지기 직전 — job_id를 로그에 남긴다.

    삭제(remove_jobs/clear_jobs/remove_jobset)든 교체(replace_jobs/
    upsert_jobs)든, force 계약은 "레코드만 건드린다, LSF job 정리는 caller
    책임"인데 레코드가 사라지면 caller가 그 job_id를 **조회할 방법이 없다**.
    그래서 사라지기 전 마지막 순간의 이 로그가 유일한 흔적이 된다.
    (force가 아니면 on-LSF job은 애초에 가드에 걸려 여기 못 온다 — 즉 이
    경고는 force 경로에서만 나온다.)"""
    alive = [r for r in targets if r.state.is_on_lsf and r.job_id is not None]
    if not alive:
        return
    log.warning(
        "%s: LSF에 살아있는 job %d건의 레코드가 사라집니다 — "
        "job_id=%s%s (정리는 caller 책임, 이후에는 조회 불가)",
        what, len(alive), [r.job_id for r in alive[:20]],
        " …" if len(alive) > 20 else "")
