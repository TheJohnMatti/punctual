from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from punctual.schedule import fires_between, next_fire, normalize


def test_normalize_shorthands():
    assert normalize("@daily") == "0 0 * * *"
    assert normalize("*/5 * * * *") == "*/5 * * * *"


def test_next_fire_basic():
    after = datetime(2026, 1, 1, 10, 3, tzinfo=ZoneInfo("UTC"))
    assert next_fire("*/10 * * * *", after, "UTC") == datetime(
        2026, 1, 1, 10, 10, tzinfo=ZoneInfo("UTC")
    )


def test_fires_between_count():
    start = datetime(2026, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC"))
    end = start + timedelta(hours=1)
    assert len(list(fires_between("*/10 * * * *", start, end, "UTC"))) == 6


def test_dst_spring_forward_toronto():
    # 2026-03-08: Toronto clocks jump 02:00 -> 03:00. A 02:30 daily job must not
    # fire that day, and the day before/after it fires normally.
    tz = "America/Toronto"
    zone = ZoneInfo(tz)
    before = datetime(2026, 3, 7, 12, 0, tzinfo=zone)
    fires = list(fires_between("30 2 * * *", before, before + timedelta(days=3), tz))
    hours = {(f.month, f.day): f.hour for f in fires}
    assert hours[(3, 8)] != 2  # the 02:30 slot doesn't exist on the 8th
    assert hours[(3, 9)] == 2  # back to normal


def test_dst_fall_back_no_double_fire():
    # 2026-11-01: Toronto 02:00 -> 01:00. A 01:30 job should fire once, not twice.
    tz = "America/Toronto"
    zone = ZoneInfo(tz)
    start = datetime(2026, 11, 1, 0, 0, tzinfo=zone)
    fires = [f for f in fires_between("30 1 * * *", start, start + timedelta(days=1), tz)]
    assert len(fires) == 1
