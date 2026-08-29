"""Durable state. DESIGN D4: one SQLite file (WAL), everything behind ``Store``.

The ``Store`` protocol is what the scheduler talks to. ``SqliteStore`` is the
only implementation today; a ``PostgresStore`` slots in for clustered mode (M5)
without the scheduler noticing.

The one load-bearing operation is :meth:`Store.claim` — the exactly-once
boundary (DESIGN O4). Everything else is bookkeeping.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from punctual.models import Run, RunState

# Bump when SCHEMA changes; add the matching step to _migrate().
SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY,
    job            TEXT NOT NULL,
    scheduled_for  TEXT NOT NULL,          -- ISO8601 UTC; the fire this run is for
    state          TEXT NOT NULL,
    attempt        INTEGER NOT NULL DEFAULT 1,
    claimed_by     TEXT,
    started_at     TEXT,
    finished_at    TEXT,
    exit_code      INTEGER,
    heartbeat_at   TEXT,
    pid            INTEGER,                -- child pid while RUNNING (O2b)
    pid_start_time TEXT,                   -- pid identity check on restart (O2b)
    stdout_tail    TEXT,                   -- O5: last N bytes, decoded
    stderr_tail    TEXT,
    created_at     TEXT NOT NULL,
    UNIQUE (job, scheduled_for, attempt)   -- DESIGN O4: the claim key
);
CREATE INDEX IF NOT EXISTS runs_job_sched ON runs (job, scheduled_for);
CREATE INDEX IF NOT EXISTS runs_open      ON runs (state) WHERE finished_at IS NULL;

-- Last observed fire per job, so catch-up (DESIGN O3) has a baseline even across
-- a config change that renames or removes jobs.
CREATE TABLE IF NOT EXISTS job_clock (
    job        TEXT PRIMARY KEY,
    last_fire  TEXT NOT NULL
);
"""

# Additive column adds for DBs created before a given SCHEMA_VERSION. Guarded by
# PRAGMA table_info so it is safe to run against a fresh DB too (all no-ops).
_ADDED_COLUMNS = {
    "runs": {
        "pid": "INTEGER",
        "pid_start_time": "TEXT",
        "stdout_tail": "TEXT",
        "stderr_tail": "TEXT",
    },
}


def _migrate(db: sqlite3.Connection) -> None:
    (version,) = db.execute("PRAGMA user_version").fetchone()
    if version >= SCHEMA_VERSION:
        return
    for table, columns in _ADDED_COLUMNS.items():
        have = {r["name"] for r in db.execute(f"PRAGMA table_info({table})")}
        for name, decl in columns.items():
            if name not in have:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def _parse(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _parse_req(s: str) -> datetime:
    """For NOT NULL timestamp columns."""
    return datetime.fromisoformat(s)


def default_db_path() -> Path:
    """XDG state dir, overridable via $PUNCTUAL_DB."""
    import os

    if env := os.environ.get("PUNCTUAL_DB"):
        return Path(env)
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "punctual" / "punctual.db"


class Store(Protocol):
    def claim(self, job: str, scheduled_for: datetime, by: str, attempt: int = 1) -> Run | None:
        """Atomically create the run row for a fire. Returns the Run if THIS
        caller won the claim, None if it already existed (someone else has it)."""
        ...

    def mark(self, run: Run) -> None: ...
    def heartbeat(self, run_id: int) -> None: ...
    def open_runs(self) -> list[Run]: ...
    def last_fire(self, job: str) -> datetime | None: ...
    def set_last_fire(self, job: str, when: datetime) -> None: ...
    def history(self, job: str | None = None, limit: int = 50) -> list[Run]: ...


class SqliteStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else default_db_path()
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(SCHEMA)
        _migrate(self._db)

    # --- the exactly-once boundary -----------------------------------------
    def claim(self, job: str, scheduled_for: datetime, by: str, attempt: int = 1) -> Run | None:
        now = _utc(datetime.now(UTC))
        cur = self._db.execute(
            "INSERT OR IGNORE INTO runs (job, scheduled_for, state, attempt, claimed_by, "
            "heartbeat_at, created_at) VALUES (?,?,?,?,?,?,?)",
            (job, _utc(scheduled_for), RunState.CLAIMED.value, attempt, by, now, now),
        )
        if cur.rowcount == 0:
            return None  # already claimed by someone (or a prior attempt row exists)
        return self._row_to_run(
            self._db.execute("SELECT * FROM runs WHERE id = ?", (cur.lastrowid,)).fetchone()
        )

    # --- bookkeeping ------------------------------------------------------
    def mark(self, run: Run) -> None:
        self._db.execute(
            "UPDATE runs SET state=?, started_at=?, finished_at=?, exit_code=?, "
            "heartbeat_at=?, pid=?, pid_start_time=?, stdout_tail=?, stderr_tail=? "
            "WHERE id=?",
            (
                run.state.value,
                _utc(run.started_at) if run.started_at else None,
                _utc(run.finished_at) if run.finished_at else None,
                run.exit_code,
                _utc(run.heartbeat_at) if run.heartbeat_at else None,
                run.pid,
                run.pid_start_time,
                run.stdout_tail,
                run.stderr_tail,
                run.id,
            ),
        )

    def heartbeat(self, run_id: int) -> None:
        self._db.execute(
            "UPDATE runs SET heartbeat_at=? WHERE id=?",
            (_utc(datetime.now(UTC)), run_id),
        )

    def open_runs(self) -> list[Run]:
        rows = self._db.execute("SELECT * FROM runs WHERE finished_at IS NULL").fetchall()
        return [self._row_to_run(r) for r in rows]

    def last_fire(self, job: str) -> datetime | None:
        row = self._db.execute("SELECT last_fire FROM job_clock WHERE job=?", (job,)).fetchone()
        return _parse(row["last_fire"]) if row else None

    def set_last_fire(self, job: str, when: datetime) -> None:
        self._db.execute(
            "INSERT INTO job_clock (job, last_fire) VALUES (?,?) "
            "ON CONFLICT(job) DO UPDATE SET last_fire=excluded.last_fire",
            (job, _utc(when)),
        )

    def history(self, job: str | None = None, limit: int = 50) -> list[Run]:
        if job:
            rows = self._db.execute(
                "SELECT * FROM runs WHERE job=? ORDER BY scheduled_for DESC, attempt DESC LIMIT ?",
                (job, limit),
            ).fetchall()
        else:
            rows = self._db.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_run(r) for r in rows]

    def close(self) -> None:
        self._db.close()

    @staticmethod
    def _row_to_run(r: sqlite3.Row) -> Run:
        return Run(
            id=r["id"],
            job=r["job"],
            scheduled_for=_parse_req(r["scheduled_for"]),
            state=RunState(r["state"]),
            attempt=r["attempt"],
            claimed_by=r["claimed_by"],
            started_at=_parse(r["started_at"]),
            finished_at=_parse(r["finished_at"]),
            exit_code=r["exit_code"],
            heartbeat_at=_parse(r["heartbeat_at"]),
            pid=r["pid"],
            pid_start_time=r["pid_start_time"],
            stdout_tail=r["stdout_tail"],
            stderr_tail=r["stderr_tail"],
        )
