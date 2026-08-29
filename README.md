# punctual

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
$ punctual why retrain         # explain the last scheduling decision
$ punctual tui                 # live dashboard
```

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
