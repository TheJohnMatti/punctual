"""``@punctual.job`` — declare a scheduled job as a Python function (M6 slice 1).

The function's module goes in ``[python] modules`` in punctual.toml; the daemon
imports it, which runs the decorator, which registers the job here. At config
load the registered jobs are turned into ordinary :class:`~punctual.models.Job`s
whose ``command`` re-execs ``python -m punctual._inproc <module>:<func>`` — so a
Python job gets every guarantee a shell job gets (process-group kill, timeout,
output capture, exit sentinel, LOST recovery).

This module imports nothing from ``punctual`` — it must be safe to import very
early.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# name -> (function, the Job-config kwargs passed to the decorator)
_REGISTERED: dict[str, tuple[Callable[[], Any], dict[str, Any]]] = {}


def job(name: str, **options: Any) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Register ``name`` as a job that calls the decorated zero-arg function.

    ``options`` are the same keys a ``[job.<name>]`` table takes — ``schedule``
    **or** ``after``, plus ``retries`` / ``timeout`` / ``on_fail`` / … — and are
    validated identically at config load. ``command`` is not allowed: it's
    synthesised.
    """
    if "command" in options:
        raise ValueError(f"@punctual.job({name!r}): 'command' is synthesised, don't pass it")

    def register(fn: Callable[[], Any]) -> Callable[[], Any]:
        if name in _REGISTERED and _REGISTERED[name][0] is not fn:
            raise ValueError(f"@punctual.job({name!r}) is already registered by another function")
        _REGISTERED[name] = (fn, options)
        fn.__punctual_job__ = name  # type: ignore[attr-defined]
        return fn

    return register


def registered() -> dict[str, tuple[Callable[[], Any], dict[str, Any]]]:
    """Everything declared so far — {name: (fn, options)}."""
    return dict(_REGISTERED)


def clear() -> None:
    """Drop all registrations. For tests — `load_config` deliberately does *not*
    clear (an already-imported module won't re-run its decorators)."""
    _REGISTERED.clear()
