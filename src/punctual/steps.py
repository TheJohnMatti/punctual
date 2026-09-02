"""``step(name, fn)`` — a durable checkpoint inside a job body (M6 slice 2).

    from punctual import job, step

    @job("sync", schedule="0 * * * *", retries={"max": 3})
    def sync():
        rows = step("fetch", lambda: api.export())     # runs once
        step("load", lambda: warehouse.upsert(rows))   # if this fails and the
                                                       # job retries, "fetch" is
                                                       # NOT re-run — its result
                                                       # is replayed from the store

A step runs its function once per fire, keyed by ``(job, scheduled_for, name)``.
On a retry, a completed step returns its cached result without calling ``fn``.
The result must be JSON-serialisable and is round-tripped through JSON on the
first call too, so a step returns the same thing whether it ran or replayed
(a tuple comes back a list, etc.). Control flow *between* steps must be
deterministic — a step that doesn't run on the retry can't have its result
replayed.

Only meaningful inside a function launched by punctual (it reads ``PUNCTUAL_JOB``
/ ``PUNCTUAL_SCHEDULED_FOR`` from the environment the daemon set).
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

from punctual.store import Store, store_from_url

_store: Store | None = None


def _ctx() -> tuple[Store, str, datetime]:
    job = os.environ.get("PUNCTUAL_JOB")
    fire = os.environ.get("PUNCTUAL_SCHEDULED_FOR")
    if not job or not fire:
        raise RuntimeError(
            "step() only works inside a job run by punctual "
            "(PUNCTUAL_JOB / PUNCTUAL_SCHEDULED_FOR are unset)"
        )
    global _store
    if _store is None:
        _store = store_from_url(os.environ.get("PUNCTUAL_STORE_URL"))
    return _store, job, datetime.fromisoformat(fire)


def step(name: str, fn: Callable[[], Any]) -> Any:
    """Run ``fn`` once for this fire and cache its (JSON) result under ``name``;
    on a replay return the cached result without calling ``fn`` again. The result
    is JSON round-tripped either way, so it's identical on run and on replay."""
    store, job, fire = _ctx()
    hit, cached = store.get_step(job, fire, name)
    if hit:
        return cached

    result = fn()
    try:
        payload = json.dumps(result)
    except TypeError as e:
        raise TypeError(f"step({name!r}) result is not JSON-serialisable: {e}") from e
    store.record_step(job, fire, name, payload)
    return json.loads(payload)
