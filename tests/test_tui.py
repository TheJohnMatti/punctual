import textwrap
from datetime import UTC, datetime

from textual.widgets import DataTable

from punctual.models import JobState, RunState
from punctual.store import SqliteStore
from punctual.tui import PunctualTUI

CFG = """
[job.alpha]
schedule = "0 * * * *"
command  = "true"

[job.beta]
schedule = "*/5 * * * *"
command  = "false"
"""


def _cfg(tmp_path):
    p = tmp_path / "punctual.toml"
    p.write_text(textwrap.dedent(CFG))
    return p


async def test_tui_lists_jobs_and_shows_detail(tmp_path, monkeypatch):
    db = tmp_path / "p.db"
    monkeypatch.setenv("PUNCTUAL_DB", str(db))
    store = SqliteStore(db)
    run = store.claim("beta", datetime.now(UTC).replace(microsecond=0), "t")
    run.transition_to(RunState.RUNNING)
    run.transition_to(RunState.FAILED)
    run.exit_code = 1
    run.stdout_tail = "some output"
    store.mark(run)
    store.save_job_state(
        JobState(
            job="beta",
            consecutive_failures=3,
            quarantined_at=datetime.now(UTC),
            quarantine_reason="broken",
        )
    )
    store.close()

    app = PunctualTUI(_cfg(tmp_path))
    async with app.run_test() as pilot:
        await pilot.pause()
        jobs = app.query_one("#jobs", DataTable)
        assert jobs.row_count == 2
        names = {str(jobs.get_row_at(i)[0]) for i in range(jobs.row_count)}
        assert names == {"alpha", "beta"}

        assert "alpha" in app.detail_text  # first row selected by default

        # move to beta, its detail should show the quarantine + the run
        await pilot.press("down")
        await pilot.pause()
        assert "quarantined" in app.detail_text and "broken" in app.detail_text
        assert app.query_one("#runs", DataTable).row_count == 1
