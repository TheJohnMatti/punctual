"""``punctual`` command line. Thin — every command is a few lines that call into
config/store/scheduler. Keeps the surface easy to keep honest.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import socket
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from punctual import __version__, introspect
from punctual.config import ConfigError, load_config
from punctual.models import Config, Job, RunState
from punctual.schedule import fires_between
from punctual.scheduler import serve
from punctual.store import SqliteStore

DEFAULT_CONFIG = "punctual.toml"

_STATE_COLOUR = {
    RunState.SUCCEEDED: "green",
    RunState.FAILED: "red",
    RunState.TIMED_OUT: "yellow",
    RunState.RUNNING: "cyan",
    RunState.LOST: "magenta",
}


def _load_config(ctx: click.Context) -> Config:
    try:
        return load_config(ctx.obj["config_path"])
    except ConfigError as e:
        raise click.ClickException(str(e)) from e


def _load(ctx: click.Context) -> list[Job]:
    return _load_config(ctx).jobs


def _instance_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


@click.group()
@click.version_option(__version__, prog_name="punctual")
@click.option(
    "-c",
    "--config",
    "config_path",
    default=DEFAULT_CONFIG,
    show_default=True,
    type=click.Path(path_type=Path),
)
@click.pass_context
def main(ctx: click.Context, config_path: Path) -> None:
    """The reliability layer cron never had."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.command()
@click.pass_context
def tui(ctx: click.Context) -> None:
    """Live read-only dashboard (needs `pip install punctual-scheduler[tui]`)."""
    try:
        from punctual.tui import run_tui
    except ImportError as e:
        raise click.ClickException(
            f"the TUI needs Textual — `pip install punctual-scheduler[tui]` ({e})"
        ) from e
    _load(ctx)  # fail fast on a broken config
    run_tui(ctx.obj["config_path"])


@main.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Parse and check punctual.toml. Exit non-zero on any problem."""
    jobs = _load(ctx)
    for j in jobs:
        flag = "" if j.enabled else "  (disabled)"
        click.echo(f"  {j.name:20} {j.schedule:16} {' '.join(j.command)}{flag}")
    click.secho(f"ok — {len(jobs)} job(s)", fg="green")


@main.command(name="plan")
@click.option("--hours", default=24, show_default=True)
@click.option("-n", "--limit", default=100, show_default=True, help="max fire lines to print")
@click.pass_context
def plan(ctx: click.Context, hours: int, limit: int) -> None:
    """What the daemon would do next: catch-up on start, then the next N hours of
    fires — each annotated with anything that changes the outcome."""
    jobs = _load(ctx)
    store = SqliteStore()
    now = dt.datetime.now(dt.UTC)
    end = now + dt.timedelta(hours=hours)
    try:
        quarantined = {j.name for j in jobs if store.job_state(j.name).quarantined}

        catch_up = []
        for j in jobs:
            if not j.enabled or j.name in quarantined:
                continue
            base = store.last_fire(j.name)
            if base and (missed := list(fires_between(j.schedule, base, now, j.timezone))):
                catch_up.append((j, missed))
        if catch_up:
            click.secho("on start — catch-up:", bold=True)
            for j, missed in catch_up:
                pol = j.missed.value
                n = 1 if pol == "run_latest" else (0 if pol == "skip" else len(missed))
                verb = {"skip": "skip", "run_latest": "run newest of", "run_each": "replay"}[pol]
                click.echo(f"  {j.name:18}  {verb} {len(missed)} missed fire(s)  →  {n} run(s)")
            click.echo("")

        for name in sorted(quarantined & {j.name for j in jobs if j.enabled}):
            skip = click.style("all fires ⊘ skipped (quarantined)", fg="yellow")
            click.echo(f"  {name:18}  {skip}")

        retries = {r.job: r for r in (store.pending_retry(j.name) for j in jobs) if r}
        rows: list[tuple[dt.datetime, str, str]] = []
        for j in jobs:
            if not j.enabled or j.name in quarantined:
                continue
            rows += [(f, j.name, "") for f in fires_between(j.schedule, now, end, j.timezone)]
        for job_name, r in retries.items():
            if r.not_before and now <= r.not_before <= end:
                rows.append((r.not_before, job_name, f"↻ retry attempt {r.attempt}"))

        rows.sort()
        for when, name, note in rows[:limit]:
            tail = f"   {click.style(note, fg='yellow')}" if note else ""
            click.echo(f"  {when.astimezone().strftime('%a %H:%M')}  {name:18}{tail}")
        if len(rows) > limit:
            click.echo(f"  … {len(rows) - limit} more (raise --limit or lower --hours)")
        click.echo(f"{len(rows)} fire(s) in the next {hours}h")
    finally:
        store.close()


@main.command()
@click.argument("job")
@click.argument("run_id", type=int, required=False)
@click.option("--json", "as_json", is_flag=True, help="machine-readable output")
@click.pass_context
def why(ctx: click.Context, job: str, run_id: int | None, as_json: bool) -> None:
    """Explain a job's current state, or `why <job> <run-id>` for one run."""
    jobs = {j.name: j for j in _load(ctx)}
    if job not in jobs:
        raise click.ClickException(f"no job named {job!r} in the config")
    store = SqliteStore()
    try:
        if run_id is not None:
            run = store.get_run(run_id)
            if run is None or run.job != job:
                raise click.ClickException(f"no run {run_id} for job {job!r}")
            report = introspect.explain_run(run, store)
            _emit(report, as_json, _render_run)
        else:
            report = introspect.explain_job(jobs[job], store)
            _emit(report, as_json, _render_job)
    finally:
        store.close()


def _emit(report: dict[str, Any], as_json: bool, render: Callable[[dict[str, Any]], None]) -> None:
    if as_json:
        click.echo(json.dumps(report, indent=2))
    else:
        render(report)


def _render_job(r: dict[str, Any]) -> None:
    colour = {"ok": "green", "degraded": "yellow", "quarantined": "red", "disabled": "bright_black"}
    click.echo(f"{click.style(r['job'], bold=True)}  ({r['schedule']}, {r['timezone']})")
    click.echo(f"  health     {click.style(r['health'], fg=colour.get(r['health']))}")
    if r["quarantine"]:
        q = r["quarantine"]
        click.echo(f"  quarantine since {q['since']} — {q['reason']}")
        click.echo(f"             {q['fires_skipped']} fires skipped so far")
        if q["probe_at"]:
            click.echo(f"             next cooldown probe at {q['probe_at']}")
        click.echo("             clear it with `punctual resume " + r["job"] + "`")
    elif r["consecutive_failures"]:
        click.echo(f"             {r['consecutive_failures']} consecutive failed fire(s)")
    lr = r["last_run"]
    if lr:
        d = f", {lr['duration_s']}s" if lr["duration_s"] is not None else ""
        code = "" if lr["exit_code"] is None else f", exit {lr['exit_code']}"
        att = f" (attempt {lr['attempt']})" if lr["attempt"] > 1 else ""
        click.echo(f"  last run   {lr['scheduled_for']} — {lr['state']}{code}{d}{att}")
    else:
        click.echo("  last run   never")
    if r["pending_retry"]:
        pr = r["pending_retry"]
        click.echo(f"  retry      attempt {pr['attempt']} pending, not before {pr['not_before']}")
    click.echo(f"  next fire  {r['next_fire']}")


def _render_run(r: dict[str, Any]) -> None:
    head = click.style(f"{r['job']} run {r['run_id']}", bold=True)
    click.echo(f"{head}  (fire {r['scheduled_for']}, attempt {r['attempt']})")
    click.echo(f"  triggered by   {r['trigger']}")
    d = f", {r['duration_s']}s" if r["duration_s"] is not None else ""
    code = "" if r["exit_code"] is None else f", exit {r['exit_code']}"
    click.echo(f"  outcome        {r['state']}{code}{d}")
    for p in r["prior_attempts"]:
        pc = "" if p["exit_code"] is None else f" exit {p['exit_code']}"
        click.echo(f"    attempt {p['attempt']}: {p['state']}{pc}")
    click.echo(f"  what next      {r['what_happened_next']}")
    for label, text in (("stdout", r["stdout_tail"]), ("stderr", r["stderr_tail"])):
        if text:
            snip = text if len(text) <= 500 else text[-500:] + " …(truncated)"
            click.echo(f"  {label}:\n" + "\n".join("    " + ln for ln in snip.splitlines()))


@main.command()
@click.option("-v", "--verbose", is_flag=True, help="DEBUG-level logs")
@click.option(
    "--log-format",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="json = one event object per line",
)
@click.pass_context
def run(ctx: click.Context, verbose: bool, log_format: str) -> None:
    """Start the scheduler daemon (run this under systemd / launchd)."""
    from punctual import logs

    logs.configure(log_format, verbose=verbose)
    path = ctx.obj["config_path"]
    cfg = _load_config(ctx)
    store = SqliteStore()
    n = sum(1 for j in cfg.jobs if j.enabled)
    where = (
        f" · metrics :{cfg.observability.metrics_port}" if cfg.observability.metrics_port else ""
    )
    click.secho(f"punctual: {n} job(s) armed · state at {store.path}{where}", fg="green")
    try:
        asyncio.run(
            serve(
                cfg.jobs,
                store,
                _instance_id(),
                config_reload=lambda: load_config(path).jobs,
                observability=cfg.observability,
            )
        )
    finally:
        store.close()
    click.echo("punctual: stopped")


def _talk(cmd: str, **kw: Any) -> dict[str, Any]:
    from punctual import control

    try:
        return control.request(cmd, **kw)
    except control.NotRunning as e:
        raise click.ClickException(str(e)) from e


@main.command()
def ping() -> None:
    """Check the running daemon is alive."""
    r = _talk("ping")
    click.echo(
        f"pid {r['pid']}, {r['jobs']} job(s), {r['in_flight']} in flight, up {r['uptime_s']}s"
    )


@main.command()
def metrics() -> None:
    """Print the running daemon's Prometheus metrics."""
    click.echo(_talk("metrics")["text"], nl=False)


@main.command()
def healthz() -> None:
    """Exit 0 if the scheduler loop is live, non-zero otherwise."""
    r = _talk("healthz")
    click.echo(r["reason"])
    if not r["ok"]:
        raise SystemExit(1)


@main.command()
def drain() -> None:
    """Tell the daemon to stop claiming and exit once in-flight runs finish."""
    r = _talk("drain")
    click.secho(
        f"draining — {r['in_flight']} run(s) in flight; the daemon exits when idle", fg="green"
    )


@main.command()
@click.option("--kill", is_flag=True, help="SIGKILL in-flight jobs instead of waiting")
@click.option("--timeout", default=60.0, show_default=True, help="seconds to wait for exit")
def stop(kill: bool, timeout: float) -> None:
    """Drain (or --kill) the daemon and wait for it to exit."""
    from punctual import control

    _talk("stop", kill=kill)
    click.echo("killing in-flight jobs…" if kill else "draining…")
    if control.wait_until_gone(timeout=timeout):
        click.secho("daemon stopped", fg="green")
    else:
        raise click.ClickException(f"daemon still running after {timeout:g}s")


@main.command()
def reload() -> None:
    """Re-read the config: add new jobs, drop removed ones (changed jobs need a restart)."""
    r = _talk("reload")
    if not r.get("ok"):
        raise click.ClickException(str(r.get("error")))
    for label, names in (
        ("added", r["added"]),
        ("removed", r["removed"]),
        ("changed", r["changed"]),
    ):
        if names:
            click.echo(f"  {label}: {', '.join(names)}")
    if r["note"]:
        click.secho(f"  note: {r['note']}", fg="yellow")
    if not (r["added"] or r["removed"] or r["changed"]):
        click.echo("  no changes")


@main.command()
@click.argument("job", required=False)
@click.option("-n", "--limit", default=20, show_default=True)
@click.pass_context
def history(ctx: click.Context, job: str | None, limit: int) -> None:
    """Recent runs: when, duration, state, exit code."""
    store = SqliteStore()
    try:
        runs = store.history(job, limit)
    finally:
        store.close()
    if not runs:
        click.echo("no runs recorded yet")
        return
    for r in reversed(runs):  # oldest first, like a log
        when = r.scheduled_for.astimezone().strftime("%m-%d %H:%M")
        dur = f"{r.duration.total_seconds():6.1f}s" if r.duration is not None else "       -"
        code = "  -" if r.exit_code is None else f"{r.exit_code:3d}"
        state = click.style(f"{r.state.value:10}", fg=_STATE_COLOUR.get(r.state))
        att = f"#{r.attempt}" if r.attempt > 1 else "  "
        click.echo(f"  {when}  {r.job:18} {att}  {state}  {dur}  exit {code}")


@main.command()
@click.option("--json", "as_json", is_flag=True, help="machine-readable output")
@click.pass_context
def status(ctx: click.Context, as_json: bool) -> None:
    """One line per job: health and quarantine state."""
    jobs = _load(ctx)
    store = SqliteStore()
    try:
        if as_json:
            click.echo(
                json.dumps(
                    [introspect.explain_job(j, store) for j in sorted(jobs, key=lambda x: x.name)],
                    indent=2,
                )
            )
            return
        for j in sorted(jobs, key=lambda x: x.name):
            st = store.job_state(j.name)
            if st.quarantined:
                since = _ago(st.quarantined_at)
                note = click.style(
                    f"QUARANTINED  {st.quarantine_reason}; "
                    f"{st.skipped_quarantined} fires skipped, since {since}",
                    fg="red",
                )
            elif st.consecutive_failures:
                note = click.style(
                    f"degraded  {st.consecutive_failures} consecutive failures", fg="yellow"
                )
            elif not j.enabled:
                note = click.style("disabled", fg="bright_black")
            else:
                last = f"last fire {_ago(st.last_fire)}" if st.last_fire else "no runs yet"
                note = click.style(f"ok  {last}", fg="green")
            click.echo(f"  {j.name:20}  {note}")
    finally:
        store.close()


@main.command()
@click.argument("job")
@click.pass_context
def resume(ctx: click.Context, job: str) -> None:
    """Take a job out of quarantine (effective on the daemon's next tick)."""
    names = {j.name for j in _load(ctx)}
    if job not in names:
        raise click.ClickException(f"no job named {job!r} in the config")
    store = SqliteStore()
    try:
        st = store.job_state(job)
        if not st.quarantined:
            click.echo(f"{job} is not quarantined")
            return
        store.request_resume(job)
    finally:
        store.close()
    click.secho(
        f"{job}: resume requested — the daemon will clear quarantine on its next tick", fg="green"
    )


def _ago(when: dt.datetime | None) -> str:
    if when is None:
        return "never"
    secs = (dt.datetime.now(dt.UTC) - when).total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs >= size:
            return f"{int(secs // size)}{unit} ago"
    return "just now"


if __name__ == "__main__":
    main()
