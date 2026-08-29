# punctual — feature & ecosystem surface

Brain-dump of what can be built around the core. Not a commitment; a map. The
**bold** items are what a compelling v1 actually needs.

## 1. Observability (this is the pitch — cron's sin is silence)

- **`punctual why <job>`** — explain the last scheduling decision: fired / skipped
  / caught-up / quarantined / lost, and the reasoning. cron can never do this.
- **`punctual tui`** — Textual dashboard: job list, next fires, recent runs, live
  output tail, keys for run-now / pause / view-log.
- **Prometheus `/metrics`** — per-job success/fail counters, run-duration
  histogram, `time_since_last_success` gauge (the killer SLO metric),
  `lost_runs_total`, scheduler loop-lag.
- **OpenTelemetry** — one span per run (child spans: claim / spawn / wait);
  inject `TRACEPARENT` into the job env so instrumented jobs link up.
- **Structured JSON logs** — one event per state transition; jq/Loki/vector-shaped.
- **`/healthz`** — "loop ticked within 2× its shortest interval AND isn't wedged".
- **Run manifest** — exit code, duration, peak RSS, exact argv+env+cwd, host,
  config git-sha. A "reproduce this run" primitive.
- `punctual web` — same views as the TUI over HTTP (server-rendered + HTMX, no
  build step). Read-only by default; mutations behind auth.
- `punctual doctor` — is the daemon supervised? clock NTP-synced? DB on a good
  disk? any job with no success in N intervals?

## 2. Notifications (plugin surface #1 — URI-scheme dispatch)

- Sinks: `ntfy://`, `slack://`, `discord://`, `pagerduty://`, `webhook://`,
  `smtp://`, `command://` (hand off to a script). Entry-point plugins.
- Policy: notify on first failure? on quarantine only? on **recovery** ("job X
  healthy again")? on a run exceeding p95 duration (brownout signal)? digest mode.

## 3. Execution backends (plugin surface #2)

- **Local subprocess** (default).
- `docker:` / `podman:` — punctual owns the container lifecycle, logs, timeout.
- `systemd-run` transient units — cgroup limits + journald + oom handling free.
- `ssh:` — run on a remote host (poor-man's distributed exec pre-clustering).
- `k8s:` — a Job per fire, for "I have a cluster, Airflow is too much".
- in-process `@punctual.job` callable — the durable-execution path (M6).

## 4. State backends (plugin surface #3)

- **SQLite** (default). Postgres (clustering, M5). SQLite + Litestream for cheap
  replication/DR without Postgres.

## 5. Config / GitOps

- `include = ["jobs.d/*.toml"]` — drop-in dir like `/etc/cron.d`.
- **`punctual validate` as a pre-commit hook + CI action** — a bad schedule never
  merges.
- **Live reload** on SIGHUP / file-watch: diff old vs new job set, apply
  gracefully (don't kill a running job whose def changed).
- **`punctual plan --diff`** — Terraform-style "what changes if I apply this".
- Secrets: `env_from = "sops://..."` / `"vault://..."` — pluggable providers,
  no secrets in the config file.

## 6. Dependencies & orchestration (M4+)

- `after = [...]` + `on_upstream_failure = skip | run | wait`.
- Fan-in barriers ("C after both A and B this cycle").
- `trigger = "on_success:other_job"` — event-driven, not just time.
- **Sensors** — run when a file appears / an endpoint returns 200 / an S3 key
  exists. (This is where it starts eating Airflow's lunch.)
- Manual gates — `punctual approve` before a deploy step.
- **`punctual backfill <job> --from --to`** — deliberate historical re-run with
  rate limiting. Data teams love this.

## 7. Reliability primitives

- **Jitter** — `jitter = "5m"` so a fleet doesn't thundering-herd an upstream at :00.
- Adaptive / load-aware — skip the heavy job if `load1 > N`; back off if the last
  3 runs took >2× p95.
- Circuit breaker per job (quarantine, have it) and per-dependency ("DB is down,
  pause everything that touches it").
- Deadlines (`useless after 06:00`) distinct from timeouts (`kill after 45m`).
- Overlap policy when `concurrency=1` and a fire lands mid-run: skip | queue |
  kill-and-replace.

## 8. Distribution (M5)

- Lease-based leader election (Postgres advisory locks / lease row + fencing
  tokens). Worker pool with work-stealing. Zone/tag affinity. `punctual drain`
  for rolling deploys.

## 9. Packaging

- Single binary (PyInstaller/Nuitka) — the "ripgrep of cron" promise.
- Homebrew, deb, Docker image, `uv tool install`.
- `punctual install-service` — writes the systemd unit / launchd plist.

## 10. Fun / differentiators

- **`punctual simulate --speed 1000x --days 30`** — run the scheduler against a
  fake clock; catch config bugs (jobs that always collide, a job that never
  fires, a catch-up that would stampede).
- Cost/duration budget tracking — alert when a job's cumulative compute trends up
  (silent regression detection).
