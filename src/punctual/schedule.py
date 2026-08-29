"""Cron-expression math. Thin wrapper over croniter so the rest of the code
never imports it directly and timezone handling lives in exactly one place.

DESIGN O7: schedules are wall-clock in the job's timezone. croniter handles the
DST folds/gaps; we just feed it tz-aware datetimes.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

_SHORTHANDS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def normalize(expr: str) -> str:
    return _SHORTHANDS.get(expr.strip(), expr)


def next_fire(expr: str, after: datetime, tz: str = "UTC") -> datetime:
    """First fire strictly after ``after`` (which may be naive or tz-aware)."""
    zone = ZoneInfo(tz)
    base = after.astimezone(zone) if after.tzinfo else after.replace(tzinfo=zone)
    nxt: datetime = croniter(normalize(expr), base).get_next(datetime)
    return nxt


def fires_between(expr: str, start: datetime, end: datetime, tz: str = "UTC") -> Iterator[datetime]:
    """All fires in (start, end]. Used by catch-up (DESIGN O3) and `punctual plan`.

    DESIGN O7: on a DST fall-back, a wall-clock time (e.g. 01:30) happens twice.
    cron fires such a job once; we match that by de-duplicating consecutive fires
    that land on the same wall-clock slot. Spring-forward gaps are left to
    croniter (it advances to the next valid instant).
    """
    zone = ZoneInfo(tz)
    start = start.astimezone(zone) if start.tzinfo else start.replace(tzinfo=zone)
    end = end.astimezone(zone) if end.tzinfo else end.replace(tzinfo=zone)
    it = croniter(normalize(expr), start)
    last_wall: tuple[int, ...] | None = None
    while True:
        nxt = it.get_next(datetime)
        if nxt > end:
            return
        wall = nxt.timetuple()[:6]
        if wall == last_wall:
            continue
        last_wall = wall
        yield nxt
