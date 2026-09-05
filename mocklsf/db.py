"""SQLite 기반 공유 상태 저장소.

bsub/bjobs/bkill 등은 각각 독립 프로세스로 실행되므로,
상태를 파일 DB 에 두어 프로세스 간에 공유한다.
WAL 모드로 스케줄러의 잦은 쓰기와 CLI 의 읽기가 충돌 없이 공존한다.
"""

import sqlite3
import time
from typing import List, Optional

from . import config
from .models import Job

# ---------------------------------------------------------------------------
# 스키마
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    row_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL,
    array_index     INTEGER,
    array_size      INTEGER NOT NULL DEFAULT 0,
    array_limit     INTEGER NOT NULL DEFAULT 0,
    user            TEXT NOT NULL,
    command         TEXT NOT NULL,
    queue           TEXT NOT NULL,
    from_host       TEXT NOT NULL,
    job_name        TEXT NOT NULL,
    cwd             TEXT NOT NULL DEFAULT '',
    stat            TEXT NOT NULL,
    exec_host       TEXT,
    submit_time     REAL NOT NULL,
    start_time      REAL,
    finish_time     REAL,
    pend_secs       REAL NOT NULL DEFAULT 0,
    run_secs        REAL NOT NULL DEFAULT 0,
    planned_outcome TEXT NOT NULL DEFAULT 'DONE',
    exit_code       INTEGER NOT NULL DEFAULT 0,
    suspend_at      REAL NOT NULL DEFAULT 0,
    suspend_secs    REAL NOT NULL DEFAULT 0,
    susp_since      REAL NOT NULL DEFAULT 0,
    num_cpus        INTEGER NOT NULL DEFAULT 1,
    requested_hosts TEXT NOT NULL DEFAULT '',
    proj            TEXT NOT NULL DEFAULT 'default',
    job_group       TEXT NOT NULL DEFAULT '',
    source_cluster  TEXT NOT NULL DEFAULT '',
    forward_cluster TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_jobs_jobid ON jobs(job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_stat ON jobs(stat);

CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

-- bhist 용 상태 전이 이벤트 로그.
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    INTEGER NOT NULL,
    array_index INTEGER,
    ts        REAL NOT NULL,
    kind      TEXT NOT NULL,   -- submit/dispatch/run/done/exit/suspend/resume/kill
    detail    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_jobid ON events(job_id);
"""

# Job dataclass 필드와 DB 컬럼 매핑 (row_id 제외 후 별도 처리).
_COLUMNS = [
    "job_id", "array_index", "array_size", "array_limit", "user", "command",
    "queue", "from_host", "job_name", "stat", "exec_host", "submit_time",
    "start_time", "finish_time", "pend_secs", "run_secs", "planned_outcome",
    "exit_code", "suspend_at", "suspend_secs", "susp_since", "num_cpus",
    "requested_hosts", "proj", "job_group", "cwd",
    "source_cluster", "forward_cluster",
]


class Database:
    """MockLSF 상태 DB 핸들."""

    def __init__(self, path: str = None):
        config.ensure_home()
        self.path = path or config.DB_PATH
        # timeout 을 넉넉히 주어 동시 접근 시 'database is locked' 를 완화.
        self.conn = sqlite3.connect(self.path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(_SCHEMA)
        # 이전 버전 DB 마이그레이션: 누락 컬럼이 있으면 추가한다.
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(jobs)")}
        for name in ("job_group", "cwd", "source_cluster", "forward_cluster"):
            if name not in cols:
                try:
                    self.conn.execute(
                        f"ALTER TABLE jobs ADD COLUMN {name} "
                        f"TEXT NOT NULL DEFAULT ''"
                    )
                except sqlite3.OperationalError as e:
                    # CLI 다중 프로세스의 동시 첫 오픈 — 이미 추가됐으면 무해
                    if "duplicate column" not in str(e).lower():
                        raise
        self.conn.execute(
            "INSERT OR IGNORE INTO counters(name, value) VALUES('job_id', ?)",
            (config.FIRST_JOB_ID,),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

    # -- job 번호 발급 ------------------------------------------------------

    def next_job_id(self) -> int:
        """다음 LSF job 번호를 원자적으로 발급."""
        cur = self.conn.execute(
            "UPDATE counters SET value = value + 1 WHERE name='job_id' "
            "RETURNING value"
        )
        val = cur.fetchone()[0]
        self.conn.commit()
        return val

    # -- 쓰기 --------------------------------------------------------------

    def insert_jobs(self, jobs: List[Job]):
        """여러 job(주로 array element)을 한 번에 삽입."""
        placeholders = ", ".join("?" for _ in _COLUMNS)
        sql = f"INSERT INTO jobs ({', '.join(_COLUMNS)}) VALUES ({placeholders})"
        rows = [[getattr(j, c) for c in _COLUMNS] for j in jobs]
        self.conn.executemany(sql, rows)
        self.conn.commit()

    def update_job(self, job: Job):
        """단일 job row 전체를 갱신 (row_id 기준)."""
        assigns = ", ".join(f"{c}=?" for c in _COLUMNS)
        vals = [getattr(job, c) for c in _COLUMNS] + [job.row_id]
        self.conn.execute(f"UPDATE jobs SET {assigns} WHERE row_id=?", vals)
        self.conn.commit()

    def update_job_group(self, row_id: int, group: str):
        """bmod는 상태 스냅샷을 되쓰지 않고 group만 변경한다."""
        self.conn.execute("UPDATE jobs SET job_group=? WHERE row_id=?",
                          (group, row_id))
        self.conn.commit()

    def update_many(self, jobs: List[Job]):
        """여러 job 을 한 트랜잭션으로 갱신 (스케줄러용)."""
        if not jobs:
            return
        assigns = ", ".join(f"{c}=?" for c in _COLUMNS)
        sql = f"UPDATE jobs SET {assigns} WHERE row_id=?"
        rows = [[getattr(j, c) for c in _COLUMNS] + [j.row_id] for j in jobs]
        self.conn.executemany(sql, rows)
        self.conn.commit()

    def update_guarded_many(self, pairs):
        """(변경 후 job, 읽었던 job) 쌍을 스냅샷 CAS로 갱신한다.

        CLI가 바꾸는 실행·대기 필드를 함께 비교한다. RUN→USUSP→RUN처럼
        상태가 돌아와도 변경된 시각을 덮지 않는다. 독립 메타데이터(job_group)
        변경은 허용한다. 실제 적용된 쌍만 반환해 이벤트·출력의 근거로 쓴다.
        """
        if not pairs:
            return []
        columns = ("stat", "exec_host", "start_time", "finish_time", "exit_code")
        checked = columns + ("pend_secs", "susp_since")
        assigns = ", ".join(f"{c}=?" for c in columns)
        guard = " AND ".join(f"{c} IS ?" for c in checked)  # NULL도 같은 값으로 비교
        sql = f"UPDATE jobs SET {assigns} WHERE row_id=? AND {guard}"
        applied = []
        with self.conn:
            for j, prev in pairs:
                vals = ([getattr(j, c) for c in columns] + [prev.row_id]
                        + [getattr(prev, c) for c in checked])
                if self.conn.execute(sql, vals).rowcount:
                    applied.append((j, prev))
        return applied

    def update_if_stat_in(self, job: Job, allowed, *, columns) -> bool:
        """DB 의 현재 stat 이 allowed 집합에 있을 때만 job 을 갱신.

        bkill/bstop 같은 사용자 명령이, 읽은 뒤 쓰기 전에 스케줄러가 그 job 을
        종료시켰다면(예: RUN→DONE) 이미 끝난 job 을 되살리지 않도록 막는다.
        호출자가 변경한 columns만 쓴다. 갱신이 실제로 일어났으면 True.
        """
        allowed = list(allowed)
        assigns = ", ".join(f"{c}=?" for c in columns)
        marks = ", ".join("?" for _ in allowed)
        sql = (f"UPDATE jobs SET {assigns} "
               f"WHERE row_id=? AND stat IN ({marks})")
        vals = [getattr(job, c) for c in columns] + [job.row_id] + allowed
        cur = self.conn.execute(sql, vals)
        self.conn.commit()
        return cur.rowcount > 0

    def log_event(self, job_id: int, array_index, kind: str, detail: str = "",
                  ts: float = None):
        self.conn.execute(
            "INSERT INTO events(job_id, array_index, ts, kind, detail) "
            "VALUES(?,?,?,?,?)",
            (job_id, array_index, ts if ts is not None else time.time(), kind,
             detail),
        )
        self.conn.commit()

    # -- 읽기 --------------------------------------------------------------

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(row_id=row["row_id"], **{name: row[name] for name in _COLUMNS})

    def all_jobs(self, order: str = "submit_time, row_id") -> List[Job]:
        cur = self.conn.execute(f"SELECT * FROM jobs ORDER BY {order}")
        return [self._row_to_job(r) for r in cur.fetchall()]

    def jobs_by_id(self, job_id: int) -> List[Job]:
        cur = self.conn.execute(
            "SELECT * FROM jobs WHERE job_id=? ORDER BY array_index", (job_id,)
        )
        return [self._row_to_job(r) for r in cur.fetchall()]

    def one_element(self, job_id: int, array_index: int) -> Optional[Job]:
        cur = self.conn.execute(
            "SELECT * FROM jobs WHERE job_id=? AND array_index IS ?",
            (job_id, array_index),
        )
        row = cur.fetchone()
        return self._row_to_job(row) if row else None

    def jobs_in_states(self, states: List[str]) -> List[Job]:
        marks = ", ".join("?" for _ in states)
        cur = self.conn.execute(
            f"SELECT * FROM jobs WHERE stat IN ({marks}) "
            "ORDER BY submit_time, row_id",
            states,
        )
        return [self._row_to_job(r) for r in cur.fetchall()]

    def events_for(self, job_id: int) -> List[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT * FROM events WHERE job_id=? ORDER BY ts, id", (job_id,)
        )
        return cur.fetchall()
