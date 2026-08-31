"""``punctual`` command line. Thin — every command is a few lines that call into
config/store/scheduler. Keeps the surface easy to keep honest.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import socket
from pathlib import Path

import click

from punctual import __version__
from punctual.config import ConfigError, load_config
from punctual.models import Job, RunState
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


def _load(ctx: click.Context) -> list[Job]:
    try:
        return load_config(ctx.obj["config_path"])
    except ConfigError as e:
        raise click.ClickException(str(e)) from e


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
def validate(ctx: click.Context) -> None:
    """Parse and check punctual.toml. Exit non-zero on any problem."""
    jobs = _load(ctx)
    for j in jobs:
        flag = "" if j.enabled else "  (disabled)"
        click.echo(f"  {j.name:20} {j.schedule:16} {' '.join(j.command)}{flag}")
    click.secho(f"ok — {len(jobs)} job(s)", fg="green")


@main.command(name="plan")
@click.option("--hours", default=24, show_default=True)
@click.pass_context
def plan(ctx: click.Context, hours: int) -> None:
    """Show every fire in the next N hours, in order (timezone/DST-aware)."""
    jobs = _load(ctx)

    now = dt.datetime.now(dt.UTC)
    end = now + dt.timedelta(hours=hours)
    fires = sorted(
        (f, j.name)
        for j in jobs
        if j.enabled
        for f in fires_between(j.schedule, now, end, j.timezone)
    )
    for when, name in fires:
        click.echo(f"  {when.astimezone().strftime('%a %H:%M')}  {name}")
    click.echo(f"{len(fires)} fire(s) in the next {hours}h")


@main.command()
@click.option("-v", "--verbose", is_flag=True, help="DEBUG-level logs")
@click.pass_context
def run(ctx: click.Context, verbose: bool) -> None:
    """Start the scheduler daemon (run this under systemd / launchd)."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    jobs = _load(ctx)
    store = SqliteStore()
    n = sum(1 for j in jobs if j.enabled)
    click.secho(f"punctual: {n} job(s) armed · state at {store.path}", fg="green")
    try:
        asyncio.run(serve(jobs, store, _instance_id()))
    finally:
        store.close()
    click.echo("punctual: stopped")


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


if __name__ == "__main__":
    main()
