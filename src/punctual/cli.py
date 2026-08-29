"""``punctual`` command line. Thin — every command is a few lines that call into
config/store/scheduler. Keeps the surface easy to keep honest.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import click

from punctual import __version__
from punctual.config import ConfigError, load_config
from punctual.schedule import fires_between

DEFAULT_CONFIG = "punctual.toml"


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
    try:
        jobs = load_config(ctx.obj["config_path"])
    except ConfigError as e:
        raise click.ClickException(str(e)) from e
    for j in jobs:
        flag = "" if j.enabled else "  (disabled)"
        click.echo(f"  {j.name:20} {j.schedule:16} {' '.join(j.command)}{flag}")
    click.secho(f"ok — {len(jobs)} job(s)", fg="green")


@main.command(name="plan")
@click.option("--hours", default=24, show_default=True)
@click.pass_context
def plan(ctx: click.Context, hours: int) -> None:
    """Show every fire in the next N hours, in order (timezone/DST-aware)."""
    try:
        jobs = load_config(ctx.obj["config_path"])
    except ConfigError as e:
        raise click.ClickException(str(e)) from e

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
@click.pass_context
def run(ctx: click.Context) -> None:
    """Start the scheduler daemon (run this under systemd / launchd)."""
    raise click.ClickException("`punctual run` lands in M1 — see docs/DESIGN.md")


@main.command()
@click.argument("job", required=False)
@click.option("-n", "--limit", default=20, show_default=True)
@click.pass_context
def history(ctx: click.Context, job: str | None, limit: int) -> None:
    """Recent runs: when, duration, exit code."""
    raise click.ClickException("`punctual history` lands in M1")


if __name__ == "__main__":
    main()
