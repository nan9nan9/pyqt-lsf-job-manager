"""SqliteStore — 영속 저장소. WAL, 세션 복원, 이력, 통계 (§4.3, §4.4).

스레드 안전 전략 (CS-3): connection은 thread-local로 스레드 간 공유 금지,
쓰기는 프로세스 전역 RLock으로 직렬화. WAL + busy_timeout은 다중 프로세스의
"접근"(조회/orphan 복원)을 안전하게 할 뿐, 동시 쓰기의 원자성은 보장하지
않는다 — 같은 jobset을 두 프로세스가 동시에 갱신하면 lost update 가능.
활성 갱신은 프로세스 1개(단일 writer)를 전제로 한다.
db_path는 로컬 디스크 권장 — NFS로 감지되면 경고 로깅 (CS-9).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from ..errors import JobNotFoundError, JobSetNotFoundError
from ..states import JobRecord, JobSetRecord, JobState
from .base import JobSetStore

log = logging.getLogger("lsfmgr.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobsets (
    jobset_id        TEXT PRIMARY KEY,
    intended_count   INTEGER NOT NULL,
    lsf_group_paths  TEXT NOT NULL DEFAULT '[]',
    name_patterns    TEXT NOT NULL DEFAULT '[]',
    array_job_ids    TEXT NOT NULL DEFAULT '[]',
    label            TEXT NOT NULL DEFAULT '',
    tags             TEXT NOT NULL DEFAULT '[]',
    description      TEXT NOT NULL DEFAULT '',
    parent_jobset_id TEXT,
    created_by       TEXT NOT NULL DEFAULT '',
    created_at       TEXT,
    merged_from      TEXT NOT NULL DEFAULT '[]',
    session_id       TEXT NOT NULL DEFAULT '',
    closed           INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS jobs (
    jobset_id    TEXT NOT NULL,
    lsf_job_name TEXT NOT NULL,
    job_id       INTEGER,
    array_index  INTEGER,
    state        TEXT NOT NULL,
    fail_reason  TEXT,
    fail_message TEXT,
    retry_count  INTEGER NOT NULL DEFAULT 0,
    exit_code    INTEGER,
    submit_time  TEXT,
    command      TEXT NOT NULL DEFAULT '',
    updated_at   TEXT,
    run_time_s   INTEGER,
    start_time   TEXT,
    finish_time  TEXT,
    working_dir  TEXT,
    via_wrapper  INTEGER NOT NULL DEFAULT 0,
    spec_json    TEXT,
    PRIMARY KEY (jobset_id, lsf_job_name)
);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs (jobset_id, state);
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    jobset_id TEXT NOT NULL,
    job_key   TEXT NOT NULL,
    old_state TEXT,
    new_state TEXT NOT NULL,
    at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_jobset ON events (jobset_id);
"""


def _dt(s: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(s) if s else None


def _iso(d: Optional[datetime]) -> Optional[str]:
    return d.isoformat() if d else None


def _warn_if_nfs(path: str) -> None:
    """db 디렉토리가 NFS면 경고 (CS-9). 감지 실패는 조용히 무시."""
    try:
        directory = os.path.dirname(os.path.abspath(path)) or "/"
        best, fstype = "", ""
        with open("/proc/mounts") as f:
            for line in f:
                fields = line.split()
                if len(fields) >= 3 and directory.startswith(fields[1]) \
                        and len(fields[1]) > len(best):
                    best, fstype = fields[1], fields[2]
        if fstype.startswith("nfs"):
            log.warning("SqliteStore db가 NFS(%s) 위에 있습니다 — lock 신뢰 불가, "
                        "로컬 디스크 사용을 권장합니다: %s", fstype, path)
    except OSError:
        pass


class SqliteStore(JobSetStore):
    """SQLite 영속 저장소. 세션 간 복원/이력/통계 지원."""

    persistent = True

    def __init__(self, db_path: str, busy_timeout_ms: int = 5000):
        self.db_path = os.path.expanduser(db_path)
        d = os.path.dirname(self.db_path)
        if d:
            os.makedirs(d, exist_ok=True)
        _warn_if_nfs(self.db_path)
        self._busy_timeout_ms = busy_timeout_ms
        self._wlock = threading.RLock()          # 단일 writer 직렬화 (CS-3)
        self._local = threading.local()          # thread-local connection
        self.session_id = uuid.uuid4().hex[:12]  # 이번 세션 식별자
        with self._write() as con:
            con.executescript(_SCHEMA)
            self._migrate(con)

    @staticmethod
    def _migrate(con: sqlite3.Connection) -> None:
        """구버전 DB 호환 — CREATE TABLE IF NOT EXISTS는 컬럼 추가를 안 하므로
        누락된 컬럼을 ALTER로 채운다(멱등). 새 DB엔 이미 있어 no-op."""
        cols = {r[1] for r in con.execute("PRAGMA table_info(jobs)")}
        for name, decl in (("run_time_s", "INTEGER"),
                           ("start_time", "TEXT"), ("finish_time", "TEXT"),
                           ("working_dir", "TEXT"),
                           ("via_wrapper", "INTEGER NOT NULL DEFAULT 0"),
                           ("spec_json", "TEXT"),
                           ("fail_message", "TEXT")):
            if name not in cols:
                try:
                    con.execute(f"ALTER TABLE jobs ADD COLUMN {name} {decl}")
                except sqlite3.OperationalError as e:
                    # 다중 프로세스가 동시에 첫 오픈하면 check-then-ALTER가
                    # 겹칠 수 있다 — 이미 추가됐으면(duplicate column) 무해
                    if "duplicate column" not in str(e).lower():
                        raise
                if name == "via_wrapper":
                    # 구버전 DB 보정: 부착물 없는 jobset == submit_wrapper 로
                    # 만든 것 — 그 소속 job은 wrapper 경로로 복원해야 한다
                    # (기본값 0이면 resubmit이 bsub 경로로 오판해 이중 제출)
                    con.execute(
                        "UPDATE jobs SET via_wrapper=1 WHERE jobset_id IN "
                        "(SELECT jobset_id FROM jobsets "
                        " WHERE lsf_group_paths='[]' AND name_patterns='[]')")

    # ------------------------------------------------------------------
    # connection 관리
    # ------------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.db_path,
                                  timeout=self._busy_timeout_ms / 1000.0)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
            self._local.con = con
        return con

    class _Tx:
        def __init__(self, store: "SqliteStore"):
            self._store = store

        def __enter__(self) -> sqlite3.Connection:
            self._store._wlock.acquire()
            try:
                return self._store._conn()
            except BaseException:
                # connection 생성 실패 시 __exit__이 호출되지 않으므로
                # 여기서 직접 해제 — 누수되면 이후 모든 쓰기가 데드락
                self._store._wlock.release()
                raise

        def __exit__(self, exc_type, exc, tb):
            con = self._store._conn()
            try:
                if exc_type is None:
                    try:
                        con.commit()
                    except BaseException:
                        # commit 실패분을 pending으로 남기면 같은 스레드의
                        # 다음 commit에 유령처럼 함께 반영된다 — 즉시 폐기
                        con.rollback()
                        raise
                else:
                    con.rollback()
            finally:
                self._store._wlock.release()
            return False

    def _write(self) -> "_Tx":
        return SqliteStore._Tx(self)

    def close(self) -> None:
        """현재 스레드의 connection 종료 (다른 스레드 것은 GC에 위임)."""
        con = getattr(self._local, "con", None)
        if con is not None:
            con.close()
            self._local.con = None

    # ------------------------------------------------------------------
    # row ↔ dataclass 변환
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_jobset(r: sqlite3.Row) -> JobSetRecord:
        return JobSetRecord(
            jobset_id=r["jobset_id"],
            intended_count=r["intended_count"],
            lsf_group_paths=json.loads(r["lsf_group_paths"]),
            name_patterns=json.loads(r["name_patterns"]),
            array_job_ids=json.loads(r["array_job_ids"]),
            label=r["label"], tags=json.loads(r["tags"]),
            description=r["description"],
            parent_jobset_id=r["parent_jobset_id"],
            created_by=r["created_by"], created_at=_dt(r["created_at"]),
            merged_from=json.loads(r["merged_from"]),
            session_id=r["session_id"], closed=bool(r["closed"]))

    @staticmethod
    def _row_to_job(r: sqlite3.Row) -> JobRecord:
        return JobRecord(
            job_id=r["job_id"], array_index=r["array_index"],
            jobset_id=r["jobset_id"], lsf_job_name=r["lsf_job_name"],
            state=JobState(r["state"]), fail_reason=r["fail_reason"],
            fail_message=r["fail_message"],
            retry_count=r["retry_count"], exit_code=r["exit_code"],
            submit_time=_dt(r["submit_time"]), command=r["command"],
            updated_at=_dt(r["updated_at"]),
            run_time_s=r["run_time_s"], start_time=_dt(r["start_time"]),
            finish_time=_dt(r["finish_time"]), working_dir=r["working_dir"],
            via_wrapper=bool(r["via_wrapper"]), spec_json=r["spec_json"])

    # INSERT는 반드시 컬럼명을 명시한다 — 위치 바인딩은 마이그레이션(ALTER는
    # 항상 끝에 추가)으로 구/신 DB의 물리 컬럼 순서가 갈라지는 순간, 에러
    # 없이 엉뚱한 컬럼에 값을 쓴다 (SQLite는 타입 불일치도 조용히 허용)
    _JOBSET_COLS = ("jobset_id", "intended_count", "lsf_group_paths",
                    "name_patterns", "array_job_ids", "label", "tags",
                    "description", "parent_jobset_id", "created_by",
                    "created_at", "merged_from", "session_id", "closed")
    _JOB_COLS = ("jobset_id", "lsf_job_name", "job_id", "array_index",
                 "state", "fail_reason", "fail_message", "retry_count",
                 "exit_code", "submit_time", "command", "updated_at",
                 "run_time_s", "start_time", "finish_time", "working_dir",
                 "via_wrapper", "spec_json")

    def _put_jobset(self, con: sqlite3.Connection, js: JobSetRecord) -> None:
        con.execute(
            f"INSERT OR REPLACE INTO jobsets ({','.join(self._JOBSET_COLS)}) "
            f"VALUES ({','.join('?' * len(self._JOBSET_COLS))})",
            (js.jobset_id, js.intended_count,
             json.dumps(js.lsf_group_paths), json.dumps(js.name_patterns),
             json.dumps(js.array_job_ids), js.label, json.dumps(js.tags),
             js.description, js.parent_jobset_id, js.created_by,
             _iso(js.created_at), json.dumps(js.merged_from),
             js.session_id, int(js.closed)))

    def _put_job(self, con: sqlite3.Connection, j: JobRecord) -> None:
        con.execute(
            f"INSERT OR REPLACE INTO jobs ({','.join(self._JOB_COLS)}) "
            f"VALUES ({','.join('?' * len(self._JOB_COLS))})",
            (j.jobset_id, j.lsf_job_name, j.job_id, j.array_index,
             j.state.value, j.fail_reason, j.fail_message, j.retry_count,
             j.exit_code, _iso(j.submit_time), j.command, _iso(j.updated_at),
             j.run_time_s, _iso(j.start_time), _iso(j.finish_time),
             j.working_dir, int(j.via_wrapper), j.spec_json))

    # ------------------------------------------------------------------
    # JobSet CRUD
    # ------------------------------------------------------------------
    def create_jobset(self, record: JobSetRecord) -> JobSetRecord:
        if record.created_at is None:
            record = replace(record, created_at=datetime.now())
        if not record.session_id:
            record = replace(record, session_id=self.session_id)
        with self._write() as con:
            cur = con.execute("SELECT 1 FROM jobsets WHERE jobset_id=?",
                              (record.jobset_id,))
            if cur.fetchone():
                raise ValueError(f"jobset 중복: {record.jobset_id}")
            self._put_jobset(con, record)
        return record

    def get_jobset(self, jobset_id: str) -> JobSetRecord:
        cur = self._conn().execute(
            "SELECT * FROM jobsets WHERE jobset_id=?", (jobset_id,))
        row = cur.fetchone()
        if row is None:
            raise JobSetNotFoundError(jobset_id)
        return self._row_to_jobset(row)

    def update_jobset(self, record: JobSetRecord) -> JobSetRecord:
        with self._write() as con:
            cur = con.execute("SELECT 1 FROM jobsets WHERE jobset_id=?",
                              (record.jobset_id,))
            if not cur.fetchone():
                raise JobSetNotFoundError(record.jobset_id)
            self._put_jobset(con, record)
        return record

    def delete_jobset(self, jobset_id: str) -> None:
        with self._write() as con:
            con.execute("DELETE FROM jobs WHERE jobset_id=?", (jobset_id,))
            con.execute("DELETE FROM events WHERE jobset_id=?", (jobset_id,))
            con.execute("DELETE FROM jobsets WHERE jobset_id=?", (jobset_id,))

    def list_jobsets(self) -> List[JobSetRecord]:
        cur = self._conn().execute(
            "SELECT * FROM jobsets WHERE session_id=?", (self.session_id,))
        return [self._row_to_jobset(r) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # JobRecord
    # ------------------------------------------------------------------
    def add_job(self, record: JobRecord) -> JobRecord:
        if record.updated_at is None:
            record = replace(record, updated_at=datetime.now())
        with self._write() as con:
            cur = con.execute("SELECT 1 FROM jobsets WHERE jobset_id=?",
                              (record.jobset_id,))
            if not cur.fetchone():
                raise JobSetNotFoundError(record.jobset_id)
            self._put_job(con, record)
        return record

    def add_jobs(self, records) -> List[JobRecord]:
        """단일 트랜잭션 일괄 insert — 대량 submit 시 caller 스레드 블로킹 방지
        (건당 트랜잭션이면 5,000건에 수 초 소요, NFR-3 위반)."""
        records = list(records)
        if not records:
            return []
        now = datetime.now()
        out: List[JobRecord] = []
        with self._write() as con:
            for jsid in {r.jobset_id for r in records}:
                cur = con.execute("SELECT 1 FROM jobsets WHERE jobset_id=?",
                                  (jsid,))
                if not cur.fetchone():
                    raise JobSetNotFoundError(jsid)
            for r in records:
                if r.updated_at is None:
                    r = replace(r, updated_at=now)
                self._put_job(con, r)
                out.append(r)
        return out

    def remove_job(self, jobset_id: str, job_key: str) -> JobRecord:
        with self._write() as con:
            cur = con.execute(
                "SELECT * FROM jobs WHERE jobset_id=? AND lsf_job_name=?",
                (jobset_id, job_key))
            row = cur.fetchone()
            if row is None:
                raise JobNotFoundError(f"{jobset_id}/{job_key}")
            rec = self._row_to_job(row)
            con.execute(
                "DELETE FROM jobs WHERE jobset_id=? AND lsf_job_name=?",
                (jobset_id, job_key))
            # 고아 이력 방지 — 삭제 job의 전이 event도 함께 제거 (delete_jobset과 일관)
            con.execute("DELETE FROM events WHERE jobset_id=? AND job_key=?",
                        (jobset_id, job_key))
        return rec

    def update_job(self, record: JobRecord) -> JobRecord:
        record = replace(record, updated_at=datetime.now())
        with self._write() as con:
            cur = con.execute(
                "SELECT 1 FROM jobs WHERE jobset_id=? AND lsf_job_name=?",
                (record.jobset_id, record.lsf_job_name))
            if not cur.fetchone():
                raise JobNotFoundError(
                    f"{record.jobset_id}/{record.lsf_job_name}")
            self._put_job(con, record)
        return record

    def get_job(self, jobset_id: str, job_key: str) -> JobRecord:
        cur = self._conn().execute(
            "SELECT * FROM jobs WHERE jobset_id=? AND lsf_job_name=?",
            (jobset_id, job_key))
        row = cur.fetchone()
        if row is None:
            raise JobNotFoundError(f"{jobset_id}/{job_key}")
        return self._row_to_job(row)

    def get_jobs(self, jobset_id: str,
                 states: Optional[Set[JobState]] = None) -> List[JobRecord]:
        con = self._conn()
        cur = con.execute("SELECT 1 FROM jobsets WHERE jobset_id=?",
                          (jobset_id,))
        if not cur.fetchone():
            raise JobSetNotFoundError(jobset_id)
        if states is not None:                  # 빈 set == 0건 (계약 일치)
            if not states:
                return []
            marks = ",".join("?" * len(states))
            cur = con.execute(
                f"SELECT * FROM jobs WHERE jobset_id=? AND state IN ({marks})",
                (jobset_id, *[s.value for s in states]))
        else:
            cur = con.execute("SELECT * FROM jobs WHERE jobset_id=?",
                              (jobset_id,))
        return [self._row_to_job(r) for r in cur.fetchall()]

    def find_jobs(self, job_ids: Set[int]) -> List[JobRecord]:
        if not job_ids:
            return []
        # 이 세션 소속 jobset의 job만 (list_jobsets와 동일 범위)
        marks = ",".join("?" * len(job_ids))
        cur = self._conn().execute(
            f"SELECT j.* FROM jobs j JOIN jobsets s USING(jobset_id) "
            f"WHERE s.session_id=? AND j.job_id IN ({marks})",
            (self.session_id, *job_ids))
        return [self._row_to_job(r) for r in cur.fetchall()]

    def transition(self, jobset_id: str, job_key: str, new_state: JobState,
                   guard=None, **fields: Any) -> Optional[JobRecord]:
        self._reject_key_fields(fields)
        with self._write() as con:              # 원자적 read-modify-write
            old = self.get_job(jobset_id, job_key)
            if guard is not None and not guard(old):
                return None                     # CAS 불일치 — 전이 건너뜀
            new = replace(old, state=new_state, updated_at=datetime.now(),
                          **fields)
            self._put_job(con, new)
            # events는 '상태 전이' 이력이다 — 상태 불변(같은 state 재설정,
            # 예: worker의 SUBMITTING 재설정, RUN 중 working_dir/exit_code
            # 갱신)일 때 기록하면 이력이 오염되고 stats()의 PEND→RUN 대기시간
            # 이 이중 집계된다. 실제 전이일 때만 남긴다.
            if old.state is not new_state:
                con.execute(
                    "INSERT INTO events (jobset_id, job_key, old_state, "
                    "new_state, at) VALUES (?,?,?,?,?)",
                    (jobset_id, job_key, old.state.value, new_state.value,
                     new.updated_at.isoformat()))
        return new

    # ------------------------------------------------------------------
    # 조회/검색 (공통)
    # ------------------------------------------------------------------
    def summary(self, jobset_id: str) -> Dict[str, Any]:
        js = self.get_jobset(jobset_id)
        cur = self._conn().execute(
            "SELECT state, COUNT(*) AS n FROM jobs WHERE jobset_id=? "
            "GROUP BY state", (jobset_id,))
        counts = {r["state"]: r["n"] for r in cur.fetchall()}
        out: Dict[str, Any] = {"total": js.intended_count}
        missing = js.intended_count - sum(counts.values())
        if missing > 0:
            counts[JobState.CREATED.value] = (
                counts.get(JobState.CREATED.value, 0) + missing)
        out.update(counts)
        return out

    def _search(self, *, tag, label, since, all_sessions: bool
                ) -> List[JobSetRecord]:
        sql = "SELECT * FROM jobsets WHERE 1=1"
        params: List[Any] = []
        if not all_sessions:
            sql += " AND session_id=?"
            params.append(self.session_id)
        if label is not None:
            sql += " AND label=?"
            params.append(label)
        if since is not None:
            sql += " AND created_at>=?"
            params.append(since.isoformat())
        cur = self._conn().execute(sql, params)
        out = [self._row_to_jobset(r) for r in cur.fetchall()]
        if tag is not None:
            out = [js for js in out if tag in js.tags]
        return out

    def search(self, *, tag: Optional[str] = None, label: Optional[str] = None,
               since: Optional[datetime] = None) -> List[JobSetRecord]:
        return self._search(tag=tag, label=label, since=since,
                            all_sessions=False)

    # ------------------------------------------------------------------
    # Sqlite 전용 API (§4.3)
    # ------------------------------------------------------------------
    def list_orphan_jobsets(self) -> List[JobSetRecord]:
        """이전 세션의 미종결(closed=0) JobSet — 복원은 앱이 결정 (FR-6.1)."""
        cur = self._conn().execute(
            "SELECT * FROM jobsets WHERE session_id != ? AND closed=0",
            (self.session_id,))
        return [self._row_to_jobset(r) for r in cur.fetchall()]

    def recover_jobset(self, jobset_id: str) -> JobSetRecord:
        """orphan을 현재 세션으로 편입 (FR-6.2). 상태 대조는 reconcile에서."""
        js = self.get_jobset(jobset_id)
        js = replace(js, session_id=self.session_id)
        return self.update_jobset(js)

    def search_all_sessions(self, *, tag: Optional[str] = None,
                            label: Optional[str] = None,
                            since: Optional[datetime] = None
                            ) -> List[JobSetRecord]:
        return self._search(tag=tag, label=label, since=since,
                            all_sessions=True)

    def get_history(self, jobset_id: str) -> List[Dict[str, Any]]:
        cur = self._conn().execute(
            "SELECT job_key, old_state, new_state, at FROM events "
            "WHERE jobset_id=? ORDER BY id", (jobset_id,))
        return [dict(r) for r in cur.fetchall()]

    def stats(self, since: Optional[datetime] = None,
              until: Optional[datetime] = None) -> Dict[str, Any]:
        """submit 성공률 / PEND→RUN 대기시간 분포 등 (FR-6.3)."""
        sql = "SELECT jobset_id, job_key, old_state, new_state, at FROM events"
        cond, params = [], []
        if since is not None:
            cond.append("at>=?")
            params.append(since.isoformat())
        if until is not None:
            cond.append("at<=?")
            params.append(until.isoformat())
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        rows = self._conn().execute(sql, params).fetchall()

        submit_ok = submit_fail = 0
        pend_at: Dict[str, datetime] = {}
        waits: List[float] = []
        for r in rows:
            key = f"{r['jobset_id']}/{r['job_key']}"
            if r["new_state"] == JobState.PEND.value:
                submit_ok += 1
                pend_at[key] = datetime.fromisoformat(r["at"])
            elif r["new_state"] == JobState.SUBMIT_FAILED.value:
                submit_fail += 1
            elif r["new_state"] == JobState.RUN.value and key in pend_at:
                waits.append(
                    (datetime.fromisoformat(r["at"]) - pend_at[key])
                    .total_seconds())
        attempts = submit_ok + submit_fail
        waits.sort()
        return {
            "submit_success": submit_ok,
            "submit_failed": submit_fail,
            "submit_success_rate": (submit_ok / attempts) if attempts else None,
            "pend_wait_count": len(waits),
            "pend_wait_avg_s": (sum(waits) / len(waits)) if waits else None,
            "pend_wait_p50_s": waits[len(waits) // 2] if waits else None,
            "pend_wait_max_s": waits[-1] if waits else None,
        }

    def archive(self, older_than_days: int = 30) -> int:
        """closed이고 오래된 jobset을 삭제. 삭제된 jobset 수 반환 (FR-5.7)."""
        cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
        with self._write() as con:
            cur = con.execute(
                "SELECT jobset_id FROM jobsets WHERE closed=1 AND "
                "created_at < ?", (cutoff,))
            ids = [r["jobset_id"] for r in cur.fetchall()]
            for jsid in ids:
                con.execute("DELETE FROM jobs WHERE jobset_id=?", (jsid,))
                con.execute("DELETE FROM events WHERE jobset_id=?", (jsid,))
                con.execute("DELETE FROM jobsets WHERE jobset_id=?", (jsid,))
        return len(ids)

    def vacuum(self) -> None:
        with self._wlock:
            self._conn().execute("VACUUM")

    def export_jobset(self, jobset_id: str, path: str) -> None:
        js = self.get_jobset(jobset_id)
        jobs = self.get_jobs(jobset_id)
        data = {
            "jobset": {
                "jobset_id": js.jobset_id,
                "intended_count": js.intended_count,
                "lsf_group_paths": js.lsf_group_paths,
                "name_patterns": js.name_patterns,
                "array_job_ids": js.array_job_ids,
                "label": js.label, "tags": js.tags,
                "description": js.description,
                "parent_jobset_id": js.parent_jobset_id,
                "created_by": js.created_by,
                "created_at": _iso(js.created_at),
                "merged_from": js.merged_from,
                "session_id": js.session_id, "closed": js.closed,
            },
            "jobs": [{
                "job_id": j.job_id, "array_index": j.array_index,
                "lsf_job_name": j.lsf_job_name, "state": j.state.value,
                "fail_reason": j.fail_reason,
                "fail_message": j.fail_message,
                "retry_count": j.retry_count,
                "exit_code": j.exit_code, "submit_time": _iso(j.submit_time),
                "command": j.command, "updated_at": _iso(j.updated_at),
                "run_time_s": j.run_time_s,
                "start_time": _iso(j.start_time),
                "finish_time": _iso(j.finish_time),
                "working_dir": j.working_dir,
            } for j in jobs],
            "history": self.get_history(jobset_id),
        }
        with open(os.path.expanduser(path), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
