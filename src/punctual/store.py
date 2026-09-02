"""Durable state. DESIGN D4: everything behind the ``Store`` protocol.

The scheduler talks to ``Store`` and never to a driver. Two implementations
share one query/logic core (``_BaseStore``):

* ``SqliteStore`` — one WAL file, zero infra. The default.
* ``PostgresStore`` — a shared store across hosts, so a ``--cluster`` deployment
  (M5) is actually highly available. ``pip install punctual-scheduler[postgres]``.

Both store timestamps as ISO-8601 UTC **text** (fixed ``+00:00`` offset, so
lexical order == chronological), so the SQL is identical bar the parameter token,
the autoincrement declaration, and how the schema version is tracked. The one
load-bearing operation is :meth:`Store.claim` — the exactly-once boundary
(DESIGN O4): ``INSERT … ON CONFLICT DO NOTHING RETURNING *``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from punctual.models import JobState, Run, RunState


class _Cursor(Protocol):
    """The slice of the DB-API cursor both backends' `_exec` returns."""

    @property
    def rowcount(self) -> int: ...
    def fetchone(self) -> Mapping[str, Any] | None: ...
    def fetchall(self) -> Sequence[Mapping[str, Any]]: ...


@dataclass(slots=True)
class MetricsSnapshot:
    """Aggregates for `punctual metrics` / `/metrics` (M3), computed in one pass."""

    run_counts: dict[tuple[str, str], int] = field(default_factory=dict)  # (job, state) -> n
    last_success: dict[str, datetime] = field(default_factory=dict)  # job -> newest success
    durations: list[tuple[str, float]] = field(default_factory=list)  # (job, seconds), terminal
    pending_retries: int = 0


@dataclass(slots=True)
class Lease:
    """A held lease on a named resource (M5). `fence` is a monotonic token bumped
    on every acquisition — stamp it on side-effects so a zombie ex-leader can't
    act after the lease has moved on."""

    resource: str
    holder: str
    fence: int
    expires_at: datetime


# Bump when SCHEMA changes; add the matching step to _BaseStore._migrate().
SCHEMA_VERSION = 7

# `{pk}` is the autoincrement primary-key declaration — the one spot the two
# backends' DDL diverges (see _PK on each store).
SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id             {pk},
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
    not_before     TEXT,                   -- M2: a RETRYING row is due at/after this
    note           TEXT,                   -- M4: SKIPPED / LOST cause, etc.
    created_at     TEXT NOT NULL,
    UNIQUE (job, scheduled_for, attempt)   -- DESIGN O4: the claim key
);
CREATE INDEX IF NOT EXISTS runs_job_sched ON runs (job, scheduled_for);
CREATE INDEX IF NOT EXISTS runs_open      ON runs (state) WHERE finished_at IS NULL;
CREATE INDEX IF NOT EXISTS runs_retry     ON runs (not_before) WHERE state = 'retrying';

-- M5: cluster leases. One row per resource; whoever's `expires_at` is in the
-- future holds it. `fence` bumps on every (re)acquisition.
CREATE TABLE IF NOT EXISTS leases (
    resource   TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    fence      INTEGER NOT NULL DEFAULT 0,
    expires_at TEXT NOT NULL
);

-- M6: durable step checkpoints inside a job body. One row per completed
-- step(name, fn); a retry of the same fire reads the cached JSON result
-- instead of re-running the step.
CREATE TABLE IF NOT EXISTS steps (
    job           TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    name          TEXT NOT NULL,
    result        TEXT NOT NULL,   -- JSON
    created_at    TEXT NOT NULL,
    PRIMARY KEY (job, scheduled_for, name)
);

-- Per-job state that outlives any single run: the catch-up baseline (O3) and
-- the quarantine circuit-breaker (M2 slice 2).
CREATE TABLE IF NOT EXISTS job_clock (
    job                  TEXT PRIMARY KEY,
    last_fire            TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    quarantined_at       TEXT,           -- NULL unless the job is quarantined
    quarantine_reason    TEXT,
    skipped_quarantined  INTEGER NOT NULL DEFAULT 0,  -- fires dropped while out
    resume_requested     INTEGER NOT NULL DEFAULT 0   -- `punctual resume` sets this
);
"""

# Additive column adds for DBs created before a given SCHEMA_VERSION. Guarded by
# a column-existence check so it is safe against a fresh DB too (all no-ops).
_ADDED_COLUMNS = {
    "runs": {
        "pid": "INTEGER",
        "pid_start_time": "TEXT",
        "stdout_tail": "TEXT",
        "stderr_tail": "TEXT",
        "not_before": "TEXT",
        "note": "TEXT",
    },
    "job_clock": {
        "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
        "quarantined_at": "TEXT",
        "quarantine_reason": "TEXT",
        "skipped_quarantined": "INTEGER NOT NULL DEFAULT 0",
        "resume_requested": "INTEGER NOT NULL DEFAULT 0",
    },
}


def _utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def _parse(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _parse_req(s: str) -> datetime:
    """For NOT NULL timestamp columns."""
    return datetime.fromisoformat(s)


def default_db_path() -> Path:
    """XDG state dir, overridable via $PUNCTUAL_DB."""
    if env := os.environ.get("PUNCTUAL_DB"):
        return Path(env)
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "punctual" / "punctual.db"


def store_from_url(url: str | None) -> Store:
    """Build the store for a `[store] url` (or None → the default SQLite file).

    * ``None`` / unset → ``SqliteStore()`` at the XDG path ($PUNCTUAL_DB wins)
    * ``sqlite://<path>`` — the path is taken verbatim after the ``//``:
      ``sqlite:///var/lib/punctual.db`` (absolute), ``sqlite://punctual.db``
      (relative), ``sqlite://:memory:``
    * ``postgresql://user:pw@host:5432/dbname`` (needs the ``[postgres]`` extra)

    $PUNCTUAL_STORE_URL overrides the argument — handy for tests / one-offs.
    """
    url = os.environ.get("PUNCTUAL_STORE_URL") or url
    if not url:
        return SqliteStore()
    scheme = urlsplit(url).scheme
    if scheme == "sqlite":
        return SqliteStore(url[len("sqlite://") :])
    if scheme in ("postgres", "postgresql"):
        return PostgresStore(url)
    raise ValueError(f"unsupported store url scheme {scheme!r}")


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
    def get_run(self, run_id: int) -> Run | None: ...
    def attempts_for(self, job: str, scheduled_for: datetime) -> list[Run]: ...
    def pending_retry(self, job: str) -> Run | None: ...
    def metrics_snapshot(self) -> MetricsSnapshot: ...
    def last_run(self, job: str) -> Run | None: ...
    def last_success_fire(self, job: str) -> datetime | None: ...
    def record_skip(self, job: str, scheduled_for: datetime, by: str, note: str) -> Run | None: ...

    def acquire_lease(self, resource: str, holder: str, ttl: timedelta) -> Lease | None:
        """Take `resource` if it's free or expired, bumping the fence. None if
        someone else holds a live lease."""

    def renew_lease(self, resource: str, holder: str, fence: int, ttl: timedelta) -> bool:
        """Extend our lease. False if we no longer hold it at this fence."""

    def release_lease(self, resource: str, holder: str) -> None: ...
    def lease_holder(self, resource: str) -> str | None: ...

    def job_state(self, job: str) -> JobState:
        """The per-job state row (defaults if the job has no row yet)."""

    def save_job_state(self, state: JobState) -> None:
        """Persist everything on `state` except last_fire (see set_last_fire)."""

    def request_resume(self, job: str) -> None:
        """Flag the job to leave quarantine on the daemon's next tick."""

    def schedule_retry(
        self, job: str, scheduled_for: datetime, attempt: int, not_before: datetime, by: str
    ) -> Run | None:
        """Create the next attempt's row (state RETRYING, due at not_before)."""

    def due_retries(self, now: datetime) -> list[Run]:
        """RETRYING rows whose not_before has passed, oldest first."""

    def next_retry_at(self) -> datetime | None:
        """Earliest not_before across all RETRYING rows."""

    def run_dir(self, run_id: int) -> Path:
        """Per-run scratch dir path (holds the exit sentinel)."""

    def get_step(self, job: str, scheduled_for: datetime, name: str) -> tuple[bool, Any]:
        """`(True, cached_result)` if this step of this fire already completed,
        else `(False, None)`. Result is the JSON-decoded value (M6)."""

    def record_step(self, job: str, scheduled_for: datetime, name: str, result_json: str) -> None:
        """Store a completed step's JSON result (idempotent per fire+name)."""

    def steps_for(self, job: str, scheduled_for: datetime) -> list[tuple[str, Any]]:
        """`(name, result)` for every completed step of a fire, in order."""

    def child_env(self) -> dict[str, str]:
        """Env a spawned job needs to reconnect to this store (for `step()`)."""

    def close(self) -> None: ...


class _BaseStore:
    """Every query, once. Subclasses supply the connection and the four spots
    where the dialects differ (`_exec`, `_executescript`, schema-version and
    column introspection)."""

    _PK = "INTEGER PRIMARY KEY"
    state_dir: Path | None = None

    # --- dialect hooks (overridden per backend) --------------------------
    def _exec(self, sql: str, params: Sequence[Any] = ()) -> _Cursor:
        raise NotImplementedError

    def _executescript(self, sql: str) -> None:
        raise NotImplementedError

    def _schema_version(self) -> int:
        raise NotImplementedError

    def _set_schema_version(self, version: int) -> None:
        raise NotImplementedError

    def _existing_tables(self) -> set[str]:
        raise NotImplementedError

    def _existing_columns(self, table: str) -> set[str]:
        raise NotImplementedError

    # --- one-time setup -------------------------------------------------
    def _init_schema(self) -> None:
        self._migrate()
        self._executescript(SCHEMA.format(pk=self._PK))

    def _migrate(self) -> None:
        """Runs *before* the SCHEMA script, so an index that references a new
        column sees an already-upgraded table. A fresh DB has no tables yet —
        nothing to alter, SCHEMA builds it complete."""
        if self._schema_version() >= SCHEMA_VERSION:
            return
        tables = self._existing_tables()
        for table, columns in _ADDED_COLUMNS.items():
            if table not in tables:
                continue
            have = self._existing_columns(table)
            for name, decl in columns.items():
                if name not in have:
                    self._exec(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
        self._set_schema_version(SCHEMA_VERSION)

    # --- the exactly-once boundary -------------------------------------
    def claim(self, job: str, scheduled_for: datetime, by: str, attempt: int = 1) -> Run | None:
        now = _utc(datetime.now(UTC))
        row = self._exec(
            "INSERT INTO runs (job, scheduled_for, state, attempt, claimed_by, "
            "heartbeat_at, created_at) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT (job, scheduled_for, attempt) DO NOTHING RETURNING *",
            (job, _utc(scheduled_for), RunState.CLAIMED.value, attempt, by, now, now),
        ).fetchone()
        return self._row_to_run(row) if row else None

    # --- bookkeeping --------------------------------------------------
    def mark(self, run: Run) -> None:
        self._exec(
            "UPDATE runs SET state=?, started_at=?, finished_at=?, exit_code=?, "
            "heartbeat_at=?, pid=?, pid_start_time=?, stdout_tail=?, stderr_tail=?, "
            "not_before=?, note=? WHERE id=?",
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
                _utc(run.not_before) if run.not_before else None,
                run.note,
                run.id,
            ),
        )

    def heartbeat(self, run_id: int) -> None:
        self._exec(
            "UPDATE runs SET heartbeat_at=? WHERE id=?",
            (_utc(datetime.now(UTC)), run_id),
        )

    def open_runs(self) -> list[Run]:
        rows = self._exec("SELECT * FROM runs WHERE finished_at IS NULL").fetchall()
        return [self._row_to_run(r) for r in rows]

    def last_fire(self, job: str) -> datetime | None:
        row = self._exec("SELECT last_fire FROM job_clock WHERE job=?", (job,)).fetchone()
        return _parse(row["last_fire"]) if row else None

    def set_last_fire(self, job: str, when: datetime) -> None:
        self._exec(
            "INSERT INTO job_clock (job, last_fire) VALUES (?,?) "
            "ON CONFLICT(job) DO UPDATE SET last_fire=excluded.last_fire",
            (job, _utc(when)),
        )

    # --- per-job state / quarantine (M2 slice 2) --------------------
    def job_state(self, job: str) -> JobState:
        row = self._exec("SELECT * FROM job_clock WHERE job=?", (job,)).fetchone()
        if row is None:
            return JobState(job=job)
        return JobState(
            job=job,
            last_fire=_parse(row["last_fire"]),
            consecutive_failures=row["consecutive_failures"],
            quarantined_at=_parse(row["quarantined_at"]),
            quarantine_reason=row["quarantine_reason"],
            skipped_quarantined=row["skipped_quarantined"],
            resume_requested=bool(row["resume_requested"]),
        )

    def save_job_state(self, s: JobState) -> None:
        self._exec(
            "INSERT INTO job_clock "
            "(job, last_fire, consecutive_failures, quarantined_at, quarantine_reason, "
            " skipped_quarantined, resume_requested) "
            "VALUES (?, COALESCE((SELECT last_fire FROM job_clock WHERE job=?), ?), ?,?,?,?,?) "
            "ON CONFLICT(job) DO UPDATE SET "
            "  consecutive_failures=excluded.consecutive_failures, "
            "  quarantined_at=excluded.quarantined_at, "
            "  quarantine_reason=excluded.quarantine_reason, "
            "  skipped_quarantined=excluded.skipped_quarantined, "
            "  resume_requested=excluded.resume_requested",
            (
                s.job,
                s.job,
                _utc(datetime.now(UTC)),
                s.consecutive_failures,
                _utc(s.quarantined_at) if s.quarantined_at else None,
                s.quarantine_reason,
                s.skipped_quarantined,
                int(s.resume_requested),
            ),
        )

    def request_resume(self, job: str) -> None:
        self._exec(
            "INSERT INTO job_clock (job, last_fire, resume_requested) VALUES (?,?,1) "
            "ON CONFLICT(job) DO UPDATE SET resume_requested=1",
            (job, _utc(datetime.now(UTC))),
        )

    def history(self, job: str | None = None, limit: int = 50) -> list[Run]:
        if job:
            rows = self._exec(
                "SELECT * FROM runs WHERE job=? ORDER BY scheduled_for DESC, attempt DESC LIMIT ?",
                (job, limit),
            ).fetchall()
        else:
            rows = self._exec("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._row_to_run(r) for r in rows]

    def get_run(self, run_id: int) -> Run | None:
        row = self._exec("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return self._row_to_run(row) if row else None

    def attempts_for(self, job: str, scheduled_for: datetime) -> list[Run]:
        """Every attempt row for one fire, oldest attempt first."""
        rows = self._exec(
            "SELECT * FROM runs WHERE job=? AND scheduled_for=? ORDER BY attempt",
            (job, _utc(scheduled_for)),
        ).fetchall()
        return [self._row_to_run(r) for r in rows]

    def pending_retry(self, job: str) -> Run | None:
        row = self._exec(
            "SELECT * FROM runs WHERE job=? AND state='retrying' ORDER BY not_before LIMIT 1",
            (job,),
        ).fetchone()
        return self._row_to_run(row) if row else None

    # --- dependencies (M4) ------------------------------------------
    def last_run(self, job: str) -> Run | None:
        row = self._exec(
            "SELECT * FROM runs WHERE job=? ORDER BY scheduled_for DESC, attempt DESC LIMIT 1",
            (job,),
        ).fetchone()
        return self._row_to_run(row) if row else None

    def last_success_fire(self, job: str) -> datetime | None:
        row = self._exec(
            "SELECT MAX(scheduled_for) t FROM runs WHERE job=? AND state='succeeded'", (job,)
        ).fetchone()
        return _parse(row["t"]) if row and row["t"] else None

    def record_skip(self, job: str, scheduled_for: datetime, by: str, note: str) -> Run | None:
        now = _utc(datetime.now(UTC))
        row = self._exec(
            "INSERT INTO runs (job, scheduled_for, state, claimed_by, finished_at, note, "
            "created_at) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT (job, scheduled_for, attempt) DO NOTHING RETURNING *",
            (job, _utc(scheduled_for), RunState.SKIPPED.value, by, now, note, now),
        ).fetchone()
        return self._row_to_run(row) if row else None

    # --- cluster leases (M5) --------------------------------------
    def acquire_lease(self, resource: str, holder: str, ttl: timedelta) -> Lease | None:
        now = datetime.now(UTC)
        exp = _utc(now + ttl)
        cur = self._exec(
            "INSERT INTO leases (resource, holder, fence, expires_at) VALUES (?,?,1,?) "
            "ON CONFLICT(resource) DO UPDATE SET "
            "  holder=excluded.holder, fence=leases.fence + 1, expires_at=excluded.expires_at "
            "WHERE leases.expires_at < ? OR leases.holder = ?",
            (resource, holder, exp, _utc(now), holder),
        )
        if cur.rowcount == 0:
            return None  # someone else holds a live lease
        row = self._exec(
            "SELECT * FROM leases WHERE resource=? AND holder=?", (resource, holder)
        ).fetchone()
        return Lease(resource, holder, row["fence"], _parse_req(row["expires_at"])) if row else None

    def renew_lease(self, resource: str, holder: str, fence: int, ttl: timedelta) -> bool:
        cur = self._exec(
            "UPDATE leases SET expires_at=? WHERE resource=? AND holder=? AND fence=?",
            (_utc(datetime.now(UTC) + ttl), resource, holder, fence),
        )
        return cur.rowcount > 0

    def release_lease(self, resource: str, holder: str) -> None:
        self._exec("DELETE FROM leases WHERE resource=? AND holder=?", (resource, holder))

    def lease_holder(self, resource: str) -> str | None:
        row = self._exec(
            "SELECT holder FROM leases WHERE resource=? AND expires_at > ?",
            (resource, _utc(datetime.now(UTC))),
        ).fetchone()
        return row["holder"] if row else None

    def metrics_snapshot(self) -> MetricsSnapshot:
        snap = MetricsSnapshot()
        for r in self._exec(
            "SELECT job, state, COUNT(*) n FROM runs WHERE finished_at IS NOT NULL "
            "GROUP BY job, state"
        ).fetchall():
            snap.run_counts[(r["job"], r["state"])] = r["n"]
        for r in self._exec(
            "SELECT job, MAX(finished_at) t FROM runs WHERE state='succeeded' GROUP BY job"
        ).fetchall():
            if r["t"]:
                snap.last_success[r["job"]] = _parse_req(r["t"])
        for r in self._exec(
            "SELECT job, started_at, finished_at FROM runs "
            "WHERE started_at IS NOT NULL AND finished_at IS NOT NULL"
        ).fetchall():
            secs = (_parse_req(r["finished_at"]) - _parse_req(r["started_at"])).total_seconds()
            snap.durations.append((r["job"], max(0.0, secs)))
        row = self._exec("SELECT COUNT(*) n FROM runs WHERE state='retrying'").fetchone()
        snap.pending_retries = row["n"] if row else 0
        return snap

    # --- retries (M2) ---------------------------------------------
    def schedule_retry(
        self, job: str, scheduled_for: datetime, attempt: int, not_before: datetime, by: str
    ) -> Run | None:
        now = _utc(datetime.now(UTC))
        row = self._exec(
            "INSERT INTO runs (job, scheduled_for, state, attempt, claimed_by, not_before, "
            "created_at) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT (job, scheduled_for, attempt) DO NOTHING RETURNING *",
            (job, _utc(scheduled_for), RunState.RETRYING.value, attempt, by, _utc(not_before), now),
        ).fetchone()
        return self._row_to_run(row) if row else None

    def due_retries(self, now: datetime) -> list[Run]:
        rows = self._exec(
            "SELECT * FROM runs WHERE state = 'retrying' AND not_before <= ? ORDER BY not_before",
            (_utc(now),),
        ).fetchall()
        return [self._row_to_run(r) for r in rows]

    def next_retry_at(self) -> datetime | None:
        row = self._exec(
            "SELECT MIN(not_before) AS t FROM runs WHERE state = 'retrying'"
        ).fetchone()
        return _parse(row["t"]) if row and row["t"] else None

    def run_dir(self, run_id: int) -> Path:
        """Per-run scratch dir (holds the exit sentinel). Not created here."""
        base = self.state_dir or Path(tempfile.gettempdir()) / "punctual"
        return base / "runs" / str(run_id)

    # --- durable steps (M6) -------------------------------------
    def get_step(self, job: str, scheduled_for: datetime, name: str) -> tuple[bool, Any]:
        row = self._exec(
            "SELECT result FROM steps WHERE job=? AND scheduled_for=? AND name=?",
            (job, _utc(scheduled_for), name),
        ).fetchone()
        return (True, json.loads(row["result"])) if row else (False, None)

    def record_step(self, job: str, scheduled_for: datetime, name: str, result_json: str) -> None:
        self._exec(
            "INSERT INTO steps (job, scheduled_for, name, result, created_at) VALUES (?,?,?,?,?) "
            "ON CONFLICT (job, scheduled_for, name) DO NOTHING",
            (job, _utc(scheduled_for), name, result_json, _utc(datetime.now(UTC))),
        )

    def steps_for(self, job: str, scheduled_for: datetime) -> list[tuple[str, Any]]:
        rows = self._exec(
            "SELECT name, result FROM steps WHERE job=? AND scheduled_for=? "
            "ORDER BY created_at, name",
            (job, _utc(scheduled_for)),
        ).fetchall()
        return [(r["name"], json.loads(r["result"])) for r in rows]

    def child_env(self) -> dict[str, str]:
        return {}

    def close(self) -> None:
        raise NotImplementedError

    @staticmethod
    def _row_to_run(r: Mapping[str, Any]) -> Run:
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
            not_before=_parse(r["not_before"]),
            created_at=_parse(r["created_at"]),
            note=r["note"],
        )


class SqliteStore(_BaseStore):
    _PK = "INTEGER PRIMARY KEY"

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else default_db_path()
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        self.state_dir = self.path.parent if str(self.path) != ":memory:" else None

    def _exec(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self._db.execute(sql, params)

    def _executescript(self, sql: str) -> None:
        self._db.executescript(sql)

    def _schema_version(self) -> int:
        (version,) = self._db.execute("PRAGMA user_version").fetchone()
        return int(version)

    def _set_schema_version(self, version: int) -> None:
        self._db.execute(f"PRAGMA user_version = {int(version)}")

    def _existing_tables(self) -> set[str]:
        return {
            r["name"] for r in self._db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    def _existing_columns(self, table: str) -> set[str]:
        return {r["name"] for r in self._db.execute(f"PRAGMA table_info({table})")}

    def child_env(self) -> dict[str, str]:
        return {} if str(self.path) == ":memory:" else {"PUNCTUAL_DB": str(self.path)}

    def close(self) -> None:
        self._db.close()


class PostgresStore(_BaseStore):
    """Shared store for a clustered deployment (M5). ``pip install
    punctual-scheduler[postgres]``. Schema and queries are the SQLite ones; only
    the parameter token (``%s``), the identity column, and the version table
    differ."""

    _PK = "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"

    def __init__(self, dsn: str):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ModuleNotFoundError as e:  # pragma: no cover - import guard
            raise RuntimeError(
                "PostgresStore needs psycopg — `pip install punctual-scheduler[postgres]`"
            ) from e
        self.dsn = dsn
        self._conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        self._init_schema()

    @staticmethod
    def _q(sql: str) -> str:
        # No query here contains a literal '?' or '%', so this is unambiguous.
        return sql.replace("?", "%s")

    def _exec(self, sql: str, params: Sequence[Any] = ()) -> Any:
        return self._conn.execute(self._q(sql), tuple(params))

    def _executescript(self, sql: str) -> None:
        self._conn.execute(sql)

    def _schema_version(self) -> int:
        self._conn.execute("CREATE TABLE IF NOT EXISTS schema_meta (version INTEGER NOT NULL)")
        row = self._conn.execute("SELECT version FROM schema_meta LIMIT 1").fetchone()
        return int(row["version"]) if row else 0

    def _set_schema_version(self, version: int) -> None:
        self._conn.execute("DELETE FROM schema_meta")
        self._conn.execute("INSERT INTO schema_meta (version) VALUES (%s)", (version,))

    def _existing_tables(self) -> set[str]:
        rows = self._conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ).fetchall()
        return {r["tablename"] for r in rows}

    def _existing_columns(self, table: str) -> set[str]:
        rows = self._conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,)
        ).fetchall()
        return {r["column_name"] for r in rows}

    def child_env(self) -> dict[str, str]:
        return {"PUNCTUAL_STORE_URL": self.dsn}

    def close(self) -> None:
        self._conn.close()
