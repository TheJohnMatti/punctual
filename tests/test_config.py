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
        command = ["python", "-m", "sniper.retrain"]
        timezone = "America/Toronto"
        timeout = "45m"
        after = ["scrape"]
        on_upstream_failure = "run"
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
    assert r.after == ["scrape"] and r.schedule is None and r.triggered
    assert r.on_upstream_failure.value == "run"
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
        ("[job.a]\ncommand='true'\nafter=['ghost']", "unknown job 'ghost'"),
        ("[job.a]\nschedule='@daily'\ncommand='true'\nafter=['a']", "exactly one of"),
        ("[job.a]\ncommand='true'", "exactly one of"),
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


def test_store_section(tmp_path):
    cfg = write(
        tmp_path,
        """
        [store]
        url = "postgresql://u:p@h:5432/punctual"

        [job.x]
        schedule = "@hourly"
        command = "true"
        """,
    )
    assert load_config(cfg).store.url == "postgresql://u:p@h:5432/punctual"


def test_store_defaults_to_none(tmp_path):
    cfg = write(tmp_path, "[job.x]\nschedule='@hourly'\ncommand='true'")
    assert load_config(cfg).store.url is None


def test_store_rejects_bad_scheme(tmp_path):
    cfg = write(tmp_path, "[store]\nurl='mysql://h/d'\n[job.x]\nschedule='@hourly'\ncommand='true'")
    with pytest.raises(ConfigError, match="unsupported url scheme"):
        load_config(cfg)


def test_unknown_top_level_table_is_rejected(tmp_path):
    cfg = write(tmp_path, "[nonsense]\nx = 1\n[job.x]\nschedule='@hourly'\ncommand='true'")
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_config(cfg)


def test_dependency_cycle_is_rejected(tmp_path):
    cfg = write(
        tmp_path,
        """
        [job.a]
        command = "true"
        after = ["c"]
        [job.b]
        command = "true"
        after = ["a"]
        [job.c]
        command = "true"
        after = ["b"]
        """,
    )
    with pytest.raises(ConfigError, match="dependency cycle"):
        load_config(cfg)


def test_triggered_job_needs_no_schedule(tmp_path):
    cfg = write(
        tmp_path,
        """
        [job.up]
        schedule = "0 * * * *"
        command = "true"
        [job.down]
        command = "true"
        after = ["up"]
        """,
    )
    down = {j.name: j for j in load_config(cfg).jobs}["down"]
    assert down.triggered and down.schedule is None


@pytest.mark.parametrize(
    "text,secs",
    [("30s", 30), ("45m", 2700), ("2h", 7200), ("1d", 86400), (90, 90)],
)
def test_parse_duration(text, secs):
    assert parse_duration(text) == timedelta(seconds=secs)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.toml")
