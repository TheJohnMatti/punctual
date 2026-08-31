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

> Status: **pre-alpha, under active design.** See [`docs/DESIGN.md`](docs/DESIGN.md)
> for the decisions made so far and the ones still open.

## The shape of it

```toml
# punctual.toml
[job.scrape]
schedule  = "*/10 * * * *"
command   = "python -m sniper.scrape"
on_missed = "skip"                       # skip | run_latest | run_each

[job.retrain]
schedule  = "0 8 * * 1"
command   = "python -m sniper.retrain"
after     = ["scrape"]                   # dependency edge
timeout   = "45m"
retries   = { max = 3, backoff = "exponential" }
on_fail   = "ntfy://my-topic"            # page after retries are exhausted
```

```console
$ punctual run                 # start the daemon (put this under systemd/launchd)
$ punctual plan                # next 24h of fires, timezone/DST-aware
$ punctual history retrain     # every run: when, how long, exit code, output
$ punctual why retrain         # explain the last scheduling decision   (coming)
$ punctual tui                 # live dashboard                          (coming)
```

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

> **What works today (M1 slice 1):** scheduling from now, subprocess execution
> with output capture, timeouts, durable history. **Not yet:** catch-up after
> downtime, retries, `why` / `tui`. See [`docs/DESIGN.md`](docs/DESIGN.md).

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
