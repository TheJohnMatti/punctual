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
| `idempotent` | bool | default `false`; safe to re-run. Drives process-group placement + `on_lost` default (O2b) |
| `on_lost` | enum | `fail` \| `retry`; default derived from `idempotent` (O2b) |
| `concurrency` | int | max simultaneous runs of THIS job; default 1 |
| `quarantine_after` | int | consecutive failed *fires* → `QUARANTINED`; default 5, `0` disables (O9) |
| `quarantine_cooldown` | duration | opt-in: after this, let one probe fire through (O9) |
| `on_fail` | URI | notify: a fire exhausted its retries (O10) |
| `on_quarantine` | URI | notify: the job was quarantined (O10) |
| `enabled` | bool | default true |
| `workdir`, `env`, `user` | | cron-parity knobs |

**Decided:** default `on_missed = run_latest` — differs from cron (so we're
adding value) without the `run_each` stampede footgun.

Still open: per-job timezone or global? `command` as string (shell) vs list
(exec) — support both or pick one? (currently: both; string is shlex-split, not
shell.)

**6-field cron (seconds) — Decided: bless it.** croniter reads a 6th field as
*seconds, appended last*: `* * * * * */3` = every 3s (NOT `*/3 * * * * *`).
Rejecting it would mean *adding* validation; blessing it is free (`schedule.py`
already passes through). Cost: seconds-last differs from Quartz and trips
people — mitigated by a loud README/config note and a regression test
(`test_six_field_cron_is_seconds_last`). Sub-minute schedules are genuinely
useful for health-check-style jobs.

### O2 — Run state machine
States: `PENDING → CLAIMED → RUNNING → {SUCCEEDED, FAILED, TIMED_OUT, LOST}`
plus `RETRYING`, `QUARANTINED`, `SKIPPED`. `CLAIMED` (row written, subprocess not
yet spawned) is kept distinct from `RUNNING` — the crash window between them
needs different handling.

### O2b — Recovering a run whose daemon died mid-flight  *(Decided)*
Daemon spawns job X at 03:00, daemon is `kill -9`'d at 03:02. The subprocess may
have: (A) died with it, (B) kept running (reparented to init), (C) finished
cleanly in the gap and we have no record.

What we record to tell these apart:
- **`pid` + `pid_start_time`** on the RUNNING row → on restart, is that pid alive
  with a matching start time? Alive ⇒ (B). Dead ⇒ (A) or (C).
- **`heartbeat_at`** bumped every N s while supervised → tells us the *daemon*
  died, not whether the *work* did.
- **exit-code sentinel**: a wrapper process (`python -m punctual._runner
  <run_dir> -- <argv>`, **not** a shell — D5) execs the child and writes
  `<run_dir>/exit` with the reaped exit code. Costs nothing, needs no cooperation
  from the job, and makes (C) fully recoverable.

**Every job gets its own session** (`start_new_session=True`), always —
*refined from the original "non-idempotent shares the daemon's group"*. Reason:
per-job group isolation is what lets a `timeout` or `stop --kill` take down one
job's whole process tree without touching the daemon or its siblings (M1
slice 2). The consequence — jobs don't die automatically when the daemon does —
is handled on restart by recovery, not by spawn placement.

**`idempotent` is the one load-bearing flag.** A per-job `idempotent` bool
(default `false`) governs what recovery does with a job it finds still running,
and the `on_lost` default:

| | `idempotent = false` *(default)* | `idempotent = true` |
|---|---|---|
| on restart, still alive | orphan from a daemon that didn't drain. **Kill the orphan**, then `LOST`. Unsupervised non-idempotent work has no timeout enforcement and no output capture — a liability, not an asset. | **adopt**: re-arm timeout from `started_at`, poll `pid`+`pid_start_time`, resolve via sentinel. Counts against `concurrency`. Output has a gap for the daemon-down window (pipes died with the old daemon; can't reattach). |
| `on_lost` default | `fail` | `retry` |

Explicit `on_lost = "fail" | "retry"` overrides the derived default either way.

Recovery on restart, per non-terminal run:
1. **`CLAIMED` at crash** → row written, *nothing spawned*, zero double-execution
   risk → `RETRYING`, always. Never subject to `on_lost` (this is why `CLAIMED`
   is a distinct state).
2. **`RUNNING`, pid dead + sentinel present** → resolve from the sentinel's exit
   code; emit `recovered_from_sentinel`.
3. **`RUNNING`, pid alive** → adopt (idempotent) or kill-orphan-then-`LOST`
   (non-idempotent), per the table above.
4. **`RUNNING`, pid dead + no sentinel** → `LOST` (loud: event + `lost_runs_total`
   metric, never folded into success/failure), then apply `on_lost`.

`LOST` is **conditionally terminal**: `on_lost=fail` → it's the final record
("fate unknown" is a legitimate answer); `on_lost=retry` → `LOST → RETRYING →
CLAIMED`. `RunState.terminal` stays `False` for `LOST`; queries treat "`LOST`
with no scheduled retry" as done rather than expanding the transition table.

Recovery order on daemon start: **recover/adopt → catch-up (O3) → tick loop**.
Adopted runs must be counted before the tick loop starts claiming, or a fresh
fire double-spawns.

**Rejected:** `on_lost = "assume_success"`. It writes a fabricated success into
the record — the exact lie `LOST` exists to avoid (poisons the success-rate
metric, `why`, history). If `LOST` noise from fire-and-forget jobs turns out to
be a real complaint, add `on_lost = "ignore"` later: resolves to `LOST`
truthfully, `lost_runs_total` still increments, but no alert and no hit to the
job-health rollup. Not in M1 (YAGNI).

**Shutdown verbs** (fallout of own-process-group adoption):
- first SIGINT → `drain`: stop claiming, let in-flight finish, exit.
- second SIGINT → escalate to `stop --kill`: SIGTERM → grace → SIGKILL
  everything, idempotent groups included.
- `punctual drain` / `punctual stop --kill` as explicit commands.

### O3 — Catch-up / restart semantics  *(built — M1 slice 3)*
On daemon start, `_plan_catch_up(job)` compares `job_clock.last_fire` (the
baseline, advanced after every terminal run *and* every recovered run) with
`now` and applies `on_missed` to the fires in between:
- `skip`: advance the clock to the newest missed fire, log the count.
- `run_latest` *(default)*: replay only the newest missed fire.
- `run_each`: replay all of them, oldest-first, in a per-job sequential task
  (so `concurrency = 1` holds and shutdown interrupts cleanly).

No history (`last_fire is None`) ⇒ no catch-up: a brand-new job or a fresh DB
starts scheduling from now.

**Decided:** `run_each` takes `catch_up_cap` (int, per-job, **default 25**);
`catch_up_cap = 0` means **uncapped** — some services genuinely require exactly
N executions per N periods. When the cap bites we log at WARNING which fires
were dropped (`catch_up_capped` metric is a M3 TODO at that log site).

The mid-run-when-we-crashed case: see O2b (also built in slice 3).

### O4 — Exactly-once primitive
Claim = `INSERT OR IGNORE INTO runs(job, scheduled_for, ...)` keyed on
`(job, scheduled_for)`. Same pattern proven in the auto_sniper notifier.

**Decided (M1):** the child env carries `PUNCTUAL_RUN_ID`, `PUNCTUAL_JOB`,
`PUNCTUAL_SCHEDULED_FOR`, `PUNCTUAL_ATTEMPT` so the job can dedupe its own side
effects.

### O5 — Output capture
**Decided (M1 slice 1):** keep the last **16 KiB** each of stdout/stderr in a
ring buffer (`executor.TAIL_BYTES`), persisted as `stdout_tail` / `stderr_tail`
TEXT columns on `runs`, decoded UTF-8 with `errors="replace"`. That is what
`punctual history` shows.

Still open: full stream to a file under `state/runs/<id>/` (always? opt-in?
size cap + rotation?). Add when someone needs more than the tail — not before.

### O5b — Schema migrations
**Decided (M1):** `PRAGMA user_version` + a `_migrate()` that runs guarded
additive `ALTER TABLE ... ADD COLUMN` (checked against `PRAGMA table_info`, so
it is a no-op on a fresh DB). No migration framework, no down-migrations. A
column *rename* or *type change* will need a table rebuild — cross that bridge
when we reach it. Pre-alpha DBs with no `user_version` are treated as v0.

### O6 — Observability surface
Prometheus text endpoint (`punctual metrics` / a port), OTel spans per run,
structured JSON logs. Which are in the MVP vs later? A `/healthz` that means
"scheduler loop is live AND not wedged".

### O7 — Time: monotonic vs wall
Scheduling is wall-clock (cron semantics), but drift detection / "is the loop
wedged" needs monotonic. NTP steps, laptop suspend, DST — enumerate the hazards.

### O8 — Retries  *(built — M2 slice 1)*
- **`retries = { max = N }` means N retries *after* the first attempt** — up to
  N+1 runs. `max = 0` (default) = one run, no retry. Matches k8s `backoffLimit`.
- **Retryable outcomes: `FAILED` and `TIMED_OUT`** (`models.RETRYABLE_OUTCOMES`).
  A timeout is usually a transiently slow dependency; retrying often clears it.
- **A pending retry is durable**, not an in-memory timer: `store.schedule_retry`
  writes the next attempt as its own row (state `RETRYING`, `not_before` = when
  the backoff elapses). The tick loop's `_sweep_retries` claims rows whose
  `not_before <= now`. A retry mid-backoff survives a daemon restart — `_recover`
  skips `RETRYING` rows and lets the sweep handle them.
- **One row per attempt**, keyed `(job, scheduled_for, attempt)` — `history`
  shows each attempt's own exit code / output. The failed row stays `FAILED`;
  the retry is a *new* row, not a mutation.
- Backoff math is `RetryPolicy.delay_for_attempt` (fixed / linear / exponential,
  capped at `max_delay`), already in `models.py`.

### O2b addendum — exit-code sentinel  *(built — M2 slice 1)*
Every run is spawned through `python -m punctual._runner <run_dir> -- <argv>`.
The wrapper runs the real command, forwards SIGTERM/SIGINT/SIGHUP to it, and on
exit writes `<run_dir>/exit` (`{code, signaled, at}`, atomic rename) before
returning that code. On restart, `_recover` reads the sentinel for a `RUNNING`
row whose pid is gone and resolves the run to `SUCCEEDED` / `FAILED` /
`TIMED_OUT` (signaled ⇒ treated as `TIMED_OUT`) instead of a blind `LOST`;
`recovered_from_sentinel` is logged. `run_dir` is deleted once a run reaches a
non-retryable terminal state. Cost: one lightweight Python process per run.

### O9 — Quarantine  *(built — M2 slice 2)*
Circuit-breaker per job, state on the `job_clock` row (now the general per-job
state row, `models.JobState`).

- **Counts *fires*, not attempts.** `_update_health` runs once per fire, after
  retries are spent. `consecutive_failures++` on a final outcome in
  `FAILURE_OUTCOMES` (`FAILED` / `TIMED_OUT` / `LOST`); **one success resets it
  to 0.** A flaky-but-recovers job never trips.
- At `quarantine_after` (default 5, `0` disables) → `quarantined_at` is stamped,
  `quarantine_reason` recorded. The tick loop then **skips** the job's fires,
  advancing its clock and incrementing `skipped_quarantined` — no per-fire rows,
  no catch-up storm on resume. `_plan_catch_up` and `_sweep_retries` also skip a
  quarantined job.
- **Out of quarantine:** `punctual resume <job>` sets `resume_requested`; the
  daemon clears the breaker on its next tick (works with the daemon up or down —
  no control socket needed, that's slice 4). Or, opt-in per job,
  `quarantine_cooldown = "1h"`: after it elapses one **probe** fire is allowed
  through — success clears the breaker, failure re-stamps `quarantined_at`
  (restarting the cooldown).
- `punctual status` shows per-job health / quarantine at a glance.

### O11 — Control socket  *(built — M2 slice 4)*
The daemon listens on a Unix domain socket — `$PUNCTUAL_SOCKET`, else
`$XDG_RUNTIME_DIR/punctual-<uid>.sock` (created 0600 via umask, unlinked on
exit). One line of JSON per request, one per reply (`punctual/control.py`).

- `ping` → pid / job count / uptime / in-flight
- `drain` → stop claiming, exit when idle (same as first SIGINT)
- `stop {kill: bool}` → drain, and with `kill` SIGKILL the in-flight groups now
- `reload` → re-read the config path the daemon was started with. **Adds new
  `[job.*]` and drops removed ones only**; a changed field on an existing job is
  *reported* (`changed`, `note: restart to apply`) but the old definition keeps
  running — no half-applied schedule/retry changes. Bad config → `{ok: false}`,
  daemon keeps running the old one.

`punctual drain` / `stop [--kill] [--timeout]` / `reload` / `ping` are the
client; `stop` polls `ping` until the socket goes away. `NotRunning` is raised
when nothing is listening. `why` / `status` stay DB-only (slice 3) — the socket
is additive, and the seam for a future `punctual web`.

### O10 — Notifications  *(started — M2 slice 2; full plugin surface is M3)*
Two hooks, both `str | None` URIs on `Job`: **`on_fail`** fires each time a fire
exhausts its retries; **`on_quarantine`** fires when the breaker opens.

- Built-in sinks (`punctual.notify`): `exec:<argv template>` (shlex-split,
  `{job}` / `{reason}` / `{event}` substituted, full event JSON on stdin and in
  `$PUNCTUAL_EVENT`) and `http(s)://…` (POST the event as JSON). `ntfy://`,
  `slack://`, `on_recovery`, digest mode, and the entry-point plugin surface
  (IDEAS §2) land in M3.
- Fire-and-forget: `notify.fire` schedules a task, held in `notify._inflight`;
  `notify.drain()` at shutdown gives outstanding sends up to 10 s. A sink that
  errors or times out is logged at WARNING and dropped — **notifications never
  affect scheduling.**

---

## Milestones

- **M0 — skeleton** *(now)*: repo, config parse + validate, `Store` + SQLite impl,
  data models, CLI stubs, CI.
- **M1 — it schedules**: daemon loop, cron → next-fire, subprocess exec + capture,
  durable run records, `punctual run` / `history`. Built in slices:
  - *slice 1 ✅* — schedule-from-now, one fire per job per tick (run_latest
    semantics for a slow loop), `execute()` + output tail, `SUCCEEDED / FAILED /
    TIMED_OUT`, `run` + `history`. **Not** yet: cross-restart catch-up,
    `_recover`, process-group kill, retries.
  - *slice 2 ✅* — every job in its own session; timeout / kill act on the whole
    process group; first signal = drain, second signal = `stop --kill`
    (SIGKILL in-flight groups → those runs land FAILED); child pid persisted.
    Not yet: `punctual drain` / `punctual stop` CLI verbs (need a control
    socket — deferred with `reload`/status).
  - *slice 3 ✅* — `_recover` (O2b: resume CLAIMED, kill non-idempotent orphans,
    LOST + `on_lost` for RUNNING) + cross-restart catch-up (O3, honouring
    `on_missed` + `catch_up_cap`), `job_clock` baseline, `logging`-based
    operator logs. Not yet: the exit-code sentinel (would let a lost run resolve
    to its *real* outcome instead of LOST), true adopt-and-supervise.
  ⇒ **M1 done** modulo the sentinel + `plan`/`why` (M2).
- **M2 — it's reliable**. Built in slices:
  - *slice 1 ✅* — retries + backoff (O8), durable `RETRYING` rows swept by the
    tick loop, exit-code sentinel (O2b addendum), `history` shows `#attempt`.
  - *slice 2 ✅* — quarantine circuit-breaker (O9): per-*fire* failure count →
    `QUARANTINED` skips the job; `punctual resume` + opt-in `quarantine_cooldown`
    probe; `punctual status`. Notifications (O10): `on_fail` / `on_quarantine`
    hooks, `exec:` + `http(s):` sinks.
  - *slice 3 ✅* — `punctual/introspect.py`: `why <job>` (health, quarantine,
    last run, pending retry, next fire) and `why <job> <run-id>` (trigger, prior
    attempts, what-happened-next, output tail). `--json` on `why` / `status`.
    `plan` annotates each fire (retry / quarantined-collapsed) + a catch-up
    preview; `-n/--limit` caps output (a per-2s job over 24h is 43k fires).
    Read-only, DB + config, no daemon needed. `Run.created_at` surfaced.
  - *slice 4 ✅* — control socket (O11): UDS + newline-JSON;
    `punctual ping` / `drain` / `stop [--kill]` / `reload` (add/remove jobs
    live, changed jobs need a restart). **⇒ M2 done.**
- **M3 — it's observable**: metrics, traces, structured logs, `punctual tui`.
- **M4 — dependencies**: `after`, topological exec, fan-in, upstream-failure policy.
- **M5 — cluster**: lease-based leader election, fencing tokens, Postgres store.
- **M6 — durable steps**: `@punctual.step` in-process checkpointing.
