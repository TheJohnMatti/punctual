import textwrap
from datetime import UTC, datetime

from click.testing import CliRunner

from punctual.cli import main
from punctual.models import JobState, RunState
from punctual.store import SqliteStore

DEMO = """
[job.hello]
schedule = "* * * * * *"
command  = "echo hi"
"""


def _cfg(tmp_path) -> str:
    p = tmp_path / "punctual.toml"
    p.write_text(textwrap.dedent(DEMO))
    return str(p)


def test_validate_ok(tmp_path):
    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "validate"])
    assert r.exit_code == 0
    assert "1 job(s)" in r.output


def test_validate_reports_bad_config(tmp_path):
    p = tmp_path / "punctual.toml"
    p.write_text("[job.x]\nschedule='nope'\ncommand='true'\n")
    r = CliRunner().invoke(main, ["-c", str(p), "validate"])
    assert r.exit_code != 0
    assert "invalid cron" in r.output


def test_plan_lists_upcoming_fires(tmp_path):
    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "plan", "--hours", "1"])
    assert r.exit_code == 0
    assert "fire(s) in the next 1h" in r.output


def test_history_empty(tmp_path, monkeypatch):
    monkeypatch.setenv("PUNCTUAL_DB", str(tmp_path / "p.db"))
    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "history"])
    assert r.exit_code == 0
    assert "no runs recorded yet" in r.output


def test_history_shows_recorded_runs(tmp_path, monkeypatch):
    db = tmp_path / "p.db"
    monkeypatch.setenv("PUNCTUAL_DB", str(db))
    store = SqliteStore(db)
    run = store.claim("hello", datetime.now(UTC).replace(microsecond=0), "test")
    assert run is not None
    run.transition_to(RunState.RUNNING)
    run.transition_to(RunState.SUCCEEDED)
    run.exit_code = 0
    run.stdout_tail = "hi"
    store.mark(run)
    store.close()

    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "history", "hello"])
    assert r.exit_code == 0
    assert "hello" in r.output
    assert "succeeded" in r.output


def test_status_and_resume_a_quarantined_job(tmp_path, monkeypatch):
    db = tmp_path / "p.db"
    monkeypatch.setenv("PUNCTUAL_DB", str(db))
    store = SqliteStore(db)
    store.save_job_state(
        JobState(
            job="hello",
            consecutive_failures=5,
            quarantined_at=datetime.now(UTC),
            quarantine_reason="5 consecutive failed fires",
            skipped_quarantined=12,
        )
    )
    store.close()

    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "status"])
    assert r.exit_code == 0
    assert "QUARANTINED" in r.output and "12 fires skipped" in r.output

    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "resume", "hello"])
    assert r.exit_code == 0 and "resume requested" in r.output
    assert SqliteStore(db).job_state("hello").resume_requested is True

    r = CliRunner().invoke(main, ["-c", _cfg(tmp_path), "resume", "nope"])
    assert r.exit_code != 0 and "no job named" in r.output
