# punctual

[![PyPI](https://img.shields.io/pypi/v/punctual-scheduler?include_prereleases)](https://pypi.org/project/punctual-scheduler/)
[![CI](https://github.com/TheJohnMatti/punctual/actions/workflows/ci.yml/badge.svg)](https://github.com/TheJohnMatti/punctual/actions/workflows/ci.yml)

**The reliability layer `cron` never had.** One long-running process, zero
infrastructure, a config file that reads like a crontab — plus the five things
every team ends up bolting onto cron by hand:

| | `cron` | `punctual` |
|---|---|---|
| Retries / backoff | ✗ | ✓ |
| Catch-up after downtime | ✗ (silently skips) | ✓ (per-job policy) |
| Dependencies between jobs | ✗ | ✓ (`after = [...]`, no DAG file) |
| "Did the 3am job run?" | ✗ | ✓ (durable run history, metrics, traces) |
| Exactly-once under overlap/restart | ✗ | ✓ (claim-before-run) |

Not Airflow. Airflow (and Dagster, Temporal, …) assume a database, a scheduler
process, a web server, a worker pool, and that you'll rewrite your jobs as DAGs.
That's right at 500 jobs and 12 engineers. `punctual` is for the machine with
6 scripts on it — which is most machines.

> Status: **alpha.** The full M0–M6 roadmap is built (scheduling, retries,
> dependencies, observability, clustering, Python jobs). See
> [`docs/DESIGN.md`](docs/DESIGN.md) for every decision and why.

## The shape of it

```toml
# punctual.toml
[job.scrape]
schedule  = "*/10 * * * *"
command   = "python -m sniper.scrape"
on_missed = "skip"                       # skip | run_latest | run_each

[job.retrain]
command   = "python -m sniper.retrain"
after     = ["scrape"]                   # runs when scrape succeeds (no clock)
timeout   = "45m"
retries   = { max = 3, backoff = "exponential" }
on_fail   = "ntfy://my-topic"            # page after retries are exhausted
```

```console
$ punctual run                 # start the daemon (put this under systemd/launchd)
$ punctual plan                # what runs next: catch-up + annotated fire list
$ punctual status              # one line per job: health, quarantine
$ punctual history retrain     # every run: when, how long, exit code, output
$ punctual why retrain         # health, last run, pending retry, next fire
$ punctual why retrain 412     # explain one run: trigger, attempts, what happened next
$ punctual graph               # the `after` dependency tree (--format dot for graphviz)
$ punctual trigger backup      # run a job now (ignore schedule / after / quarantine)
$ punctual resume retrain      # take a job out of quarantine
$ punctual reload              # apply added / removed jobs without a restart
$ punctual stop --kill         # drain (or hard-kill) the running daemon
$ punctual metrics             # Prometheus text; also GET /metrics if a port is set
$ punctual tui                 # read-only dashboard (pip install punctual-scheduler[tui])
```

Set `[observability] metrics_port = 9095` in `punctual.toml` and the daemon
serves `GET /metrics` (per-job counters, run-duration histogram,
`punctual_time_since_last_success_seconds` — the SLO gauge) and `GET /healthz`
on `127.0.0.1:9095`.

### Python jobs

A job can be a Python function instead of a command:

```python
# myapp/jobs.py
from punctual import job, step


@job("reindex", schedule="0 * * * *", retries={"max": 2}, timeout="30m")
def reindex():
    docs = step("export", lambda: db.dump())  # runs once per fire; on a retry
    step("index", lambda: search.load(docs))  # a completed step is replayed,
    #                                           not re-run
```

```toml
# punctual.toml
[python]
modules = ["myapp.jobs"]     # the daemon imports these; decorators register the jobs
```

The function runs in its own subprocess (`python -m punctual._inproc …`), so it
gets the same process-group kill, timeout, output capture and crash recovery as
a shell command. TOML `[job.*]` tables and `@job` functions mix freely.
`step(name, fn)` checkpoints work inside a fire (keyed by the fire's timestamp);
results must be JSON-serialisable.

## Quickstart

```console
$ uv tool install --prerelease=allow punctual-scheduler   # or: pipx install --pre punctual-scheduler

$ mkdir -p ~/.config/punctual && cat > ~/.config/punctual/punctual.toml <<'EOF'
[job.heartbeat]
schedule = "* * * * * */30"     # every 30s — 6-field cron: seconds go LAST
command  = "date -u +%FT%TZ"

[job.backup]
schedule = "0 3 * * *"          # 03:00 daily
command  = "restic backup /home/me"
EOF

$ punctual -c ~/.config/punctual/punctual.toml validate
$ punctual -c ~/.config/punctual/punctual.toml run      # Ctrl-C drains, then exits
# ...in another shell:
$ punctual -c ~/.config/punctual/punctual.toml history
  09-01 14:30  heartbeat   succeeded    0.0s  exit   0
  09-01 14:30  backup      succeeded   12.4s  exit   0
```

State lives in `~/.local/state/punctual/punctual.db` (override with `$PUNCTUAL_DB`).
To keep it running, install a service — see [`packaging/`](packaging/).

### High availability

Run two daemons against one shared store and pass `--cluster`:

```console
$ punctual -c punctual.toml run --cluster     # host A — becomes leader
$ punctual -c punctual.toml run --cluster     # host B — hot standby
```

One daemon holds a 30 s lease and does all the scheduling; the other idles until
that lease expires, then takes over (failover ≈ 30–40 s). Every node's control
socket stays live, so `ping` / `healthz` / `metrics` work on both;
`healthz` reports `standby` on the follower.

A cluster on one SQLite file is only as available as that host, so point it at
Postgres:

```console
$ pip install "punctual-scheduler[postgres]"
```

```toml
# punctual.toml
[store]
url = "postgresql://punctual:secret@db.internal:5432/punctual"
```

`[store] url` also takes `sqlite://<path>`; unset means the default XDG file.
`$PUNCTUAL_STORE_URL` overrides the config.

> **What works today (M1–M5):** scheduling, `after` dependencies (trigger-driven,
> fan-in, upstream-failure policy, `wait_timeout`), `punctual trigger` for an
> ad-hoc run, subprocess exec with output capture + timeouts, durable history,
> restart recovery + catch-up, retries with backoff, a quarantine circuit-breaker,
> failure/recovery notifications (`ntfy` / `slack` / `discord` / `exec` / webhook
> / plugins), `why` / `status` / annotated `plan` / `graph`, a control socket
> (`drain` / `stop` / `reload`), Prometheus `/metrics` + `/healthz`, structured
> JSON logs (`--log-format json`), a read-only TUI, lease-based leader election
> (`run --cluster`), a Postgres store, `@punctual.job` Python jobs, and durable
> in-job `step()` checkpoints. That's **M0–M6 — the whole roadmap.** See
> [`docs/DESIGN.md`](docs/DESIGN.md).

## Design principles

1. **A missed run is an incident, not a shrug.** cron's original sin is silence.
2. **Zero infra to start.** Embedded SQLite. No server, no broker, no web app.
3. **Crontab-shaped.** If you can write a crontab you can write a `punctual.toml`.
4. **Exactly-once is at-least-once + idempotency.** We're honest about that and
   build the idempotency in.
5. **Grows, doesn't bloat.** Single node → leader-elected cluster → durable
   in-process steps, same core.

## License

Apache-2.0.
