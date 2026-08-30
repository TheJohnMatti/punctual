"""Load and validate ``punctual.toml`` into :class:`~punctual.models.Job` objects.

v0 schema — DESIGN O1. Deliberately strict: unknown keys are an error, not a
warning, so typos in a scheduling config fail loudly at load instead of silently
at 3am.
"""

from __future__ import annotations

import tomllib
from datetime import timedelta
from pathlib import Path
from typing import Any

from croniter import croniter

from punctual.models import Backoff, Job, MissedPolicy, OnLost, RetryPolicy

_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

_JOB_KEYS = {
    "schedule",
    "command",
    "timezone",
    "on_missed",
    "retries",
    "timeout",
    "after",
    "idempotent",
    "on_lost",
    "catch_up_cap",
    "concurrency",
    "quarantine_after",
    "on_fail",
    "workdir",
    "env",
    "enabled",
}
_RETRY_KEYS = {"max", "backoff", "base_delay", "max_delay"}


class ConfigError(ValueError):
    pass


def parse_duration(text: str | int | float) -> timedelta:
    """'45m' -> timedelta(minutes=45). Bare numbers are seconds."""
    if isinstance(text, (int, float)):
        return timedelta(seconds=float(text))
    text = text.strip().lower()
    if text and text[-1] in _DURATION_UNITS:
        return timedelta(seconds=float(text[:-1]) * _DURATION_UNITS[text[-1]])
    try:
        return timedelta(seconds=float(text))
    except ValueError as e:
        raise ConfigError(f"bad duration {text!r} (try '30s', '45m', '2h')") from e


def _as_argv(command: str | list[str], job: str) -> list[str]:
    if isinstance(command, list):
        if not all(isinstance(c, str) for c in command):
            raise ConfigError(f"job {job!r}: command list must be all strings")
        return command
    if isinstance(command, str):
        # DESIGN O1: string form is exec-split, NOT shell. Users who want a shell
        # write `command = ["bash", "-lc", "..."]` explicitly. Revisit.
        import shlex

        return shlex.split(command)
    raise ConfigError(f"job {job!r}: command must be a string or list of strings")


def _retry_policy(raw: dict[str, Any], job: str) -> RetryPolicy:
    unknown = set(raw) - _RETRY_KEYS
    if unknown:
        raise ConfigError(f"job {job!r}: unknown retries keys {sorted(unknown)}")
    p = RetryPolicy()
    if "max" in raw:
        p.max = int(raw["max"])
    if "backoff" in raw:
        try:
            p.backoff = Backoff(raw["backoff"])
        except ValueError as e:
            raise ConfigError(
                f"job {job!r}: backoff must be one of {[b.value for b in Backoff]}"
            ) from e
    if "base_delay" in raw:
        p.base_delay = parse_duration(raw["base_delay"])
    if "max_delay" in raw:
        p.max_delay = parse_duration(raw["max_delay"])
    return p


def _build_job(name: str, raw: dict[str, Any]) -> Job:
    if not isinstance(raw, dict):
        raise ConfigError(f"job {name!r}: expected a table")
    unknown = set(raw) - _JOB_KEYS
    if unknown:
        raise ConfigError(f"job {name!r}: unknown keys {sorted(unknown)}")
    for required in ("schedule", "command"):
        if required not in raw:
            raise ConfigError(f"job {name!r}: missing required key {required!r}")

    schedule = raw["schedule"]
    if not croniter.is_valid(schedule):
        raise ConfigError(f"job {name!r}: invalid cron schedule {schedule!r}")

    job = Job(
        name=name,
        schedule=schedule,
        command=_as_argv(raw["command"], name),
        timezone=raw.get("timezone", "UTC"),
        retries=_retry_policy(raw.get("retries", {}), name),
        after=list(raw.get("after", [])),
        idempotent=bool(raw.get("idempotent", False)),
        catch_up_cap=int(raw.get("catch_up_cap", 25)),
        concurrency=int(raw.get("concurrency", 1)),
        quarantine_after=int(raw.get("quarantine_after", 5)),
        on_fail=raw.get("on_fail"),
        workdir=raw.get("workdir"),
        env={str(k): str(v) for k, v in raw.get("env", {}).items()},
        enabled=bool(raw.get("enabled", True)),
    )
    if "on_missed" in raw:
        try:
            job.missed = MissedPolicy(raw["on_missed"])
        except ValueError as e:
            raise ConfigError(
                f"job {name!r}: on_missed must be one of {[m.value for m in MissedPolicy]}"
            ) from e
    if "on_lost" in raw:
        try:
            job.on_lost = OnLost(raw["on_lost"])
        except ValueError as e:
            raise ConfigError(
                f"job {name!r}: on_lost must be one of {[o.value for o in OnLost]}"
            ) from e
    if "timeout" in raw:
        job.timeout = parse_duration(raw["timeout"])
    return job


def load_config(path: str | Path) -> list[Job]:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"{path} not found")
    try:
        doc = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{path}: {e}") from e

    jobs_table = doc.get("job", {})
    if not jobs_table:
        raise ConfigError(f"{path}: no [job.*] tables defined")

    jobs = [_build_job(name, raw) for name, raw in jobs_table.items()]

    names = {j.name for j in jobs}
    for j in jobs:
        for dep in j.after:
            if dep not in names:
                raise ConfigError(f"job {j.name!r}: after references unknown job {dep!r}")
    # DESIGN O1: also reject dependency cycles here once M4 lands.
    return jobs
