import textwrap
from datetime import timedelta

import pytest

from punctual.config import ConfigError, load_config, parse_duration
from punctual.models import Backoff, MissedPolicy, OnLost


def write(tmp_path, body: str):
    p = tmp_path / "punctual.toml"
    p.write_text(textwrap.dedent(body))
    return p


def test_minimal_job(tmp_path):
    cfg = write(
        tmp_path,
        """
        [job.backup]
        schedule = "0 3 * * *"
        command = "restic backup /data"
    """,
    )
    (job,) = load_config(cfg).jobs
    assert job.name == "backup"
    assert job.command == ["restic", "backup", "/data"]
    assert job.missed is MissedPolicy.RUN_LATEST
    assert job.enabled


def test_full_job(tmp_path):
    cfg = write(
        tmp_path,
        """
        [job.retrain]
        schedule = "0 8 * * 1"
        command = ["python", "-m", "sniper.retrain"]
        timezone = "America/Toronto"
        on_missed = "run_latest"
        timeout = "45m"
        after = ["scrape"]
        retries = { max = 3, backoff = "exponential", base_delay = "30s" }

        [job.scrape]
        schedule = "*/10 * * * *"
        command = "python -m sniper.scrape"
        on_missed = "skip"
    """,
    )
    jobs = {j.name: j for j in load_config(cfg).jobs}
    r = jobs["retrain"]
    assert r.timezone == "America/Toronto"
    assert r.timeout == timedelta(minutes=45)
    assert r.after == ["scrape"]
    assert r.retries.max == 3
    assert r.retries.backoff is Backoff.EXPONENTIAL
    assert r.retries.base_delay == timedelta(seconds=30)
    assert jobs["scrape"].missed is MissedPolicy.SKIP


def test_on_lost_defaults_derive_from_idempotent(tmp_path):
    cfg = write(
        tmp_path,
        """
        [job.plain]
        schedule = "0 * * * *"
        command = "true"

        [job.safe]
        schedule = "0 * * * *"
        command = "true"
        idempotent = true

        [job.override]
        schedule = "0 * * * *"
        command = "true"
        idempotent = true
        on_lost = "fail"
    """,
    )
    jobs = {j.name: j for j in load_config(cfg).jobs}
    assert jobs["plain"].idempotent is False
    assert jobs["plain"].on_lost is None
    assert jobs["plain"].effective_on_lost is OnLost.FAIL
    assert jobs["safe"].effective_on_lost is OnLost.RETRY
    assert jobs["override"].on_lost is OnLost.FAIL
    assert jobs["override"].effective_on_lost is OnLost.FAIL


@pytest.mark.parametrize(
    "body,msg",
    [
        ("[job.x]\nschedule='0 3 * * *'", "missing required key 'command'"),
        ("[job.x]\nschedule='not a cron'\ncommand='true'", "invalid cron"),
        ("[job.x]\nschedule='0 3 * * *'\ncommand='true'\nnonsense=1", "unknown keys"),
        (
            "[job.x]\nschedule='0 3 * * *'\ncommand='true'\non_missed='sometimes'",
            "on_missed must be",
        ),
        ("[job.a]\nschedule='@daily'\ncommand='true'\nafter=['ghost']", "unknown job 'ghost'"),
        (
            "[job.x]\nschedule='0 3 * * *'\ncommand='true'\non_lost='maybe'",
            "on_lost must be",
        ),
        ("", r"no \[job"),
    ],
)
def test_rejections(tmp_path, body, msg):
    with pytest.raises(ConfigError, match=msg):
        load_config(write(tmp_path, body))


def test_shorthand_schedule(tmp_path):
    cfg = write(tmp_path, "[job.x]\nschedule='@hourly'\ncommand='true'")
    (job,) = load_config(cfg).jobs
    assert job.schedule == "@hourly"  # normalized at schedule-computation time, not load


def test_observability_section(tmp_path):
    cfg = write(
        tmp_path,
        """
        [observability]
        metrics_port = 9095

        [job.x]
        schedule = "@hourly"
        command = "true"
        """,
    )
    obs = load_config(cfg).observability
    assert obs.metrics_port == 9095 and obs.metrics_addr == "127.0.0.1"


def test_observability_rejects_junk(tmp_path):
    cfg = write(tmp_path, "[observability]\nnope = 1\n[job.x]\nschedule='@hourly'\ncommand='true'")
    with pytest.raises(ConfigError, match="unknown keys"):
        load_config(cfg)


def test_unknown_top_level_table_is_rejected(tmp_path):
    cfg = write(tmp_path, "[nonsense]\nx = 1\n[job.x]\nschedule='@hourly'\ncommand='true'")
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_config(cfg)


@pytest.mark.parametrize(
    "text,secs",
    [("30s", 30), ("45m", 2700), ("2h", 7200), ("1d", 86400), (90, 90)],
)
def test_parse_duration(text, secs):
    assert parse_duration(text) == timedelta(seconds=secs)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.toml")
