# punctual — design & decision log

This is the living record of *why* punctual is built the way it is. Every
non-obvious choice gets an entry. Format borrowed from ADRs but lighter.

---

## Decided

### D1 — Name
`punctual`. PyPI distribution is `punctual-scheduler` (the bare name is a dead
2019 dotfile tool); import package and CLI are `punctual`.

### D2 — Liveness model: **long-running daemon**
`punctual run` is a supervised long-lived process that owns an asyncio loop,
computes next-fire times, and spawns/watches job subprocesses.

- Rejected **tick mode** (OS runs `punctual tick` every minute): weaker pitch
  ("the cron replacement that needs cron"), 1-minute granularity floor.
- The cost of this choice: **restart-recovery and catch-up must be bulletproof
  from day one.** If the daemon is down 02:55–03:05, on restart it must correctly
  decide what to do about the 03:00 fire (per each job's `on_missed`). This is
  the central correctness problem and gets its own design pass (see O3).
- We recommend running it under systemd / launchd / `docker --restart` and will
  ship unit files. `punctual` is not its own supervisor (no daemonize, no
  pidfile dance) — that's the OS's job.

### D3 — Config format: **TOML** (`punctual.toml`)
One `[job.<name>]` table per job. Typed-ish, comments, no indentation traps.
Schema is still being nailed down (O1).

### D4 — State store: **SQLite** (WAL mode), behind a `Store` interface
Zero-infra requirement. All durable state — job definitions snapshot, run
records, leases — in one SQLite file (`~/.local/state/punctual/punctual.db` by
XDG, overridable). The `Store` protocol is defined so a Postgres impl can slot
in for the future clustered mode without touching the scheduler.

### D5 — Execution: **subprocess first**
`command = "..."` runs via `asyncio.create_subprocess_exec` (shell only when the
string needs it). Language-agnostic, matches cron's mental model. In-process
Python callables + `@step` durable execution come later on the same core.

### D6 — Python floor: **3.12+**
`tomllib` in stdlib, mature `asyncio.TaskGroup`, `sys.monitoring` available for
cheap observability hooks later. 3.12 is broadly deployed by 2026.

### D7 — Dependencies: **minimal**
Runtime: `click` (CLI), `croniter` (cron + DST math — explicitly not hand-rolled).
That's it for the MVP. `rich` may come with the TUI. Keeping the tree small
keeps a future single-binary bundle (PyInstaller/Nuitka) viable.

---

## Open — need decisions

### O1 — The `punctual.toml` schema
Proposed v0 fields per `[job.<name>]`:

| field | type | notes |
|---|---|---|
| `schedule` | cron string | required; `@hourly` shorthands allowed |
| `command` | string or list | required |
| `timezone` | IANA name | default: system tz? or force UTC? **← sub-decision** |
| `on_missed` | enum | `skip` \| `run_latest` \| `run_each`; default? |
| `retries` | table | `{ max, backoff, max_delay }` |
| `timeout` | duration | kill + mark failed after this |
| `after` | list[str] | dependency edges (later milestone) |
| `concurrency` | int | max simultaneous runs of THIS job; default 1 |
| `on_fail` / `on_missed_alert` | URI | notification sink after quarantine |
| `enabled` | bool | default true |
| `workdir`, `env`, `user` | | cron-parity knobs |

Questions: default `on_missed`? per-job timezone or global? `command` as string
(shell) vs list (exec) — support both or pick one?

### O2 — Run state machine
Proposed states: `PENDING → CLAIMED → RUNNING → {SUCCEEDED, FAILED, TIMED_OUT, LOST}`
plus `RETRYING`, `QUARANTINED`, `SKIPPED`.

- `CLAIMED` vs `RUNNING`: is the gap worth it? (claim = row written, process not
  yet spawned — matters for exactly-once under a crash in that window)
- `LOST`: a run that was `RUNNING` when the daemon died. How is it detected on
  restart — heartbeat column? pid liveness check? Both?

### O3 — Catch-up / restart semantics (the hard one)
On daemon start, for each job, look at `last_fire` vs `now` and the schedule:
- `skip`: set the clock forward to the next future fire, log the skipped fires.
- `run_latest`: run once immediately, then resume.
- `run_each`: enqueue every missed fire (coalesced? rate-limited? what stops this
  from stampeding a downstream DB — the Airflow footgun?).

Also: what about a job that was mid-run when we crashed? Re-run it, or assume it
finished? (Depends on idempotency — which we can't assume. Probably: mark `LOST`,
re-run only if `on_missed != skip`.)

### O4 — Exactly-once primitive
Claim = `INSERT OR IGNORE INTO runs(job, scheduled_for, ...)` keyed on
`(job, scheduled_for)`. Same pattern proven in the auto_sniper notifier. Open:
does the *command* get an idempotency token in its env (`PUNCTUAL_RUN_ID`) so
the job itself can dedupe side effects? (Yes, probably.)

### O5 — Output capture
stdout/stderr of each run: ring-buffered to a cap (default 64 KiB each?), stored
as a SQLite BLOB vs a file under `state/runs/<id>/`. Full stream to a file always,
tail in the DB for `punctual history`?

### O6 — Observability surface
Prometheus text endpoint (`punctual metrics` / a port), OTel spans per run,
structured JSON logs. Which are in the MVP vs later? A `/healthz` that means
"scheduler loop is live AND not wedged".

### O7 — Time: monotonic vs wall
Scheduling is wall-clock (cron semantics), but drift detection / "is the loop
wedged" needs monotonic. NTP steps, laptop suspend, DST — enumerate the hazards.

---

## Milestones

- **M0 — skeleton** *(now)*: repo, config parse + validate, `Store` + SQLite impl,
  data models, CLI stubs, CI.
- **M1 — it schedules**: daemon loop, cron → next-fire, subprocess exec + capture,
  durable run records, `punctual run` / `list` / `history`.
- **M2 — it's reliable**: retries + backoff + quarantine, timeouts, `LOST`
  detection, catch-up policies, `punctual plan` / `why`.
- **M3 — it's observable**: metrics, traces, structured logs, `punctual tui`.
- **M4 — dependencies**: `after`, topological exec, fan-in, upstream-failure policy.
- **M5 — cluster**: lease-based leader election, fencing tokens, Postgres store.
- **M6 — durable steps**: `@punctual.step` in-process checkpointing.
