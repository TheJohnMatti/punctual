"""Load and validate ``punctual.toml`` into :class:`~punctual.models.Job` objects.

v0 schema — DESIGN O1. Deliberately strict: unknown keys are an error, not a
warning, so typos in a scheduling config fail loudly at load instead of silently
at 3am.
"""

from __future__ import annotations

import importlib
import sys
import tomllib
from datetime import timedelta
from pathlib import Path
from typing import Any

from croniter import croniter

from punctual import registry
from punctual.models import (
    Backoff,
    Config,
    Job,
    MissedPolicy,
    ObservabilityConfig,
    OnLost,
    RetryPolicy,
    StoreConfig,
    UpstreamFailure,
)

_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

_JOB_KEYS = {
    "schedule",
    "command",
    "timezone",
    "on_missed",
    "retries",
    "timeout",
    "after",
    "on_upstream_failure",
    "wait_timeout",
    "idempotent",
    "on_lost",
    "catch_up_cap",
    "concurrency",
    "quarantine_after",
    "quarantine_cooldown",
    "on_fail",
    "on_quarantine",
    "on_recovery",
    "workdir",
    "env",
    "enabled",
}
_RETRY_KEYS = {"max", "backoff", "base_delay", "max_delay"}


class ConfigError(ValueError):
    pass


def parse_duration(text: str | int | float | timedelta) -> timedelta:
    """'45m' -> timedelta(minutes=45). Bare numbers are seconds."""
    if isinstance(text, timedelta):  # an @punctual.job passed one directly
        return text
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


def _retry_policy(raw: dict[str, Any] | RetryPolicy, job: str) -> RetryPolicy:
    if isinstance(raw, RetryPolicy):  # an @punctual.job passed one directly
        return raw
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
    if "command" not in raw:
        raise ConfigError(f"job {name!r}: missing required key 'command'")

    after = list(raw.get("after", []))
    schedule = raw.get("schedule")
    if bool(schedule) == bool(after):
        raise ConfigError(
            f"job {name!r}: set exactly one of 'schedule' (clock-driven) or "
            f"'after' (runs when its upstreams complete)"
        )
    if schedule is not None and not croniter.is_valid(schedule):
        raise ConfigError(f"job {name!r}: invalid cron schedule {schedule!r}")

    job = Job(
        name=name,
        schedule=schedule,
        command=_as_argv(raw["command"], name),
        timezone=raw.get("timezone", "UTC"),
        retries=_retry_policy(raw.get("retries", {}), name),
        after=after,
        idempotent=bool(raw.get("idempotent", False)),
        catch_up_cap=int(raw.get("catch_up_cap", 25)),
        concurrency=int(raw.get("concurrency", 1)),
        quarantine_after=int(raw.get("quarantine_after", 5)),
        on_fail=raw.get("on_fail"),
        on_quarantine=raw.get("on_quarantine"),
        on_recovery=raw.get("on_recovery"),
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
    if "on_upstream_failure" in raw:
        try:
            job.on_upstream_failure = UpstreamFailure(raw["on_upstream_failure"])
        except ValueError as e:
            raise ConfigError(
                f"job {name!r}: on_upstream_failure must be one of "
                f"{[u.value for u in UpstreamFailure]}"
            ) from e
    if "timeout" in raw:
        job.timeout = parse_duration(raw["timeout"])
    if "quarantine_cooldown" in raw:
        job.quarantine_cooldown = parse_duration(raw["quarantine_cooldown"])
    if "wait_timeout" in raw:
        job.wait_timeout = parse_duration(raw["wait_timeout"])
    return job


_OBSERVABILITY_KEYS = {"metrics_addr", "metrics_port"}
_STORE_KEYS = {"url"}
_PYTHON_KEYS = {"modules"}


def _python_modules(raw: dict[str, Any]) -> list[str]:
    unknown = set(raw) - _PYTHON_KEYS
    if unknown:
        raise ConfigError(f"[python]: unknown keys {sorted(unknown)}")
    mods = raw.get("modules", [])
    if not isinstance(mods, list) or not all(isinstance(m, str) for m in mods):
        raise ConfigError("[python]: 'modules' must be a list of import paths")
    return list(mods)


def _load_python_jobs(modules: list[str]) -> list[Job]:
    """Import each module (runs the ``@punctual.job`` decorators) and turn every
    registration into a :class:`Job` that re-execs ``python -m punctual._inproc``.

    Not cleared between calls: an already-imported module won't re-run its
    decorators, so the registry has to persist. Dropping a module from the config
    without a daemon restart leaves its jobs registered — matches ``reload``'s
    "changed jobs need a restart" rule.
    """
    if modules and "" not in sys.path and str(Path.cwd()) not in sys.path:
        sys.path.insert(0, "")  # resolve a job module living in the daemon's cwd
    for mod in modules:
        try:
            importlib.import_module(mod)
        except ImportError as e:
            raise ConfigError(f"[python]: cannot import {mod!r}: {e}") from e

    wanted = set(modules)
    jobs = []
    for name, (fn, options) in registry.registered().items():
        mod = getattr(fn, "__module__", "")
        if not any(mod == w or mod.startswith(f"{w}.") for w in wanted):
            continue  # a registration from a different config in this process
        ref = f"{fn.__module__}:{fn.__qualname__}"
        raw = {**options, "command": [sys.executable, "-m", "punctual._inproc", ref]}
        try:
            job = _build_job(name, raw)
        except ConfigError as e:
            raise ConfigError(f"@punctual.job({name!r}): {e}") from e
        job.python_ref = ref
        jobs.append(job)
    return jobs


def _store(raw: dict[str, Any]) -> StoreConfig:
    unknown = set(raw) - _STORE_KEYS
    if unknown:
        raise ConfigError(f"[store]: unknown keys {sorted(unknown)}")
    cfg = StoreConfig()
    if "url" in raw:
        url = str(raw["url"])
        if not url.startswith(("sqlite:", "postgres:", "postgresql:")):
            raise ConfigError(f"[store]: unsupported url scheme in {url!r}")
        cfg.url = url
    return cfg


def _observability(raw: dict[str, Any]) -> ObservabilityConfig:
    unknown = set(raw) - _OBSERVABILITY_KEYS
    if unknown:
        raise ConfigError(f"[observability]: unknown keys {sorted(unknown)}")
    cfg = ObservabilityConfig()
    if "metrics_addr" in raw:
        cfg.metrics_addr = str(raw["metrics_addr"])
    if "metrics_port" in raw:
        port = int(raw["metrics_port"])
        if not 1 <= port <= 65535:
            raise ConfigError(f"[observability]: metrics_port {port} out of range")
        cfg.metrics_port = port
    return cfg


def load_config(path: str | Path) -> Config:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"{path} not found")
    try:
        doc = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{path}: {e}") from e

    unknown_top = set(doc) - {"job", "observability", "store", "python"}
    if unknown_top:
        raise ConfigError(f"{path}: unknown top-level table(s) {sorted(unknown_top)}")

    python_modules = _python_modules(doc.get("python", {}))
    py_jobs = _load_python_jobs(python_modules)

    jobs_table = doc.get("job", {})
    if not jobs_table and not py_jobs:
        raise ConfigError(f"{path}: no [job.*] tables or [python] modules defined")

    jobs = py_jobs + [_build_job(name, raw) for name, raw in jobs_table.items()]

    names: set[str] = set()
    for j in jobs:
        if j.name in names:
            raise ConfigError(f"job {j.name!r} is defined twice (TOML and @punctual.job?)")
        names.add(j.name)
    for j in jobs:
        for dep in j.after:
            if dep not in names:
                raise ConfigError(f"job {j.name!r}: after references unknown job {dep!r}")
    _reject_cycles(jobs)
    return Config(
        jobs=jobs,
        observability=_observability(doc.get("observability", {})),
        store=_store(doc.get("store", {})),
        python_modules=python_modules,
    )


def _reject_cycles(jobs: list[Job]) -> None:
    """DFS over the `after` graph; raise on the first cycle found (O12)."""
    graph = {j.name: j.after for j in jobs}
    WHITE, GREY, BLACK = 0, 1, 2
    colour = dict.fromkeys(graph, WHITE)

    def visit(node: str, path: list[str]) -> None:
        colour[node] = GREY
        for up in graph[node]:
            if colour[up] == GREY:
                cycle = (
                    " -> ".join([*path[path.index(up) :], up]) if up in path else f"{node} -> {up}"
                )
                raise ConfigError(f"dependency cycle: {cycle}")
            if colour[up] == WHITE:
                visit(up, [*path, up])
        colour[node] = BLACK

    for name in graph:
        if colour[name] == WHITE:
            visit(name, [name])
