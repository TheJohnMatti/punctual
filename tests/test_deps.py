"""M4 slice 1 — trigger-driven jobs (`after`)."""

import asyncio
import sys
from datetime import UTC, datetime

from punctual.models import Job, RunState, UpstreamFailure
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

OK = [sys.executable, "-c", "pass"]
FAIL = [sys.executable, "-c", "import sys; sys.exit(1)"]


def _upstream(name: str, command=OK, schedule: str = "* * * * * *") -> Job:
    return Job(name=name, schedule=schedule, command=command)


def _triggered(name: str, after: list[str], command=OK, **kw) -> Job:
    return Job(name=name, schedule=None, command=command, after=after, **kw)


async def _run_briefly(sched: Scheduler, seconds: float) -> None:
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(seconds)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)


async def test_downstream_fires_when_upstream_succeeds(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler(
        [_upstream("scrape"), _triggered("process", ["scrape"])], store, "t", handle_signals=False
    )
    await _run_briefly(sched, 3.0)

    scrapes = [r for r in store.history("scrape", 50) if r.state is RunState.SUCCEEDED]
    procs = [r for r in store.history("process", 50) if r.state is RunState.SUCCEEDED]
    assert scrapes and procs
    # each process run's scheduled_for matches a scrape fire (the triggering one)
    scrape_fires = {r.scheduled_for for r in scrapes}
    assert all(p.scheduled_for in scrape_fires for p in procs)
    store.close()


async def test_fan_in_waits_for_all_upstreams(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    slow = ["bash", "-lc", "sleep 2"]
    sched = Scheduler(
        [_upstream("a"), _upstream("b", slow), _triggered("report", ["a", "b"])],
        store,
        "t",
        handle_signals=False,
    )
    await _run_briefly(sched, 3.5)

    # report only ran after b (the slow one) had at least one success
    reports = store.history("report", 50)
    b_ok = [r for r in store.history("b", 50) if r.state is RunState.SUCCEEDED]
    assert reports and b_ok
    assert min(r.scheduled_for for r in reports) >= min(r.scheduled_for for r in b_ok)
    store.close()


async def test_upstream_failure_skips_downstream_by_default(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler(
        [_upstream("scrape", FAIL), _triggered("process", ["scrape"])],
        store,
        "t",
        handle_signals=False,
    )
    await _run_briefly(sched, 3.0)

    proc_rows = store.history("process", 50)
    assert proc_rows
    assert all(r.state is RunState.SKIPPED for r in proc_rows)
    assert "upstream scrape failed" in proc_rows[0].note
    store.close()


async def test_on_upstream_failure_run(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler(
        [
            _upstream("scrape", FAIL),
            _triggered("process", ["scrape"], on_upstream_failure=UpstreamFailure.RUN),
        ],
        store,
        "t",
        handle_signals=False,
    )
    await _run_briefly(sched, 3.0)

    ran = [r for r in store.history("process", 50) if r.state is RunState.SUCCEEDED]
    assert ran and "despite upstream" in ran[0].note
    store.close()


async def test_downstream_does_not_rerun_without_a_fresh_upstream_success(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    # upstream fires once then never again (yearly), succeeds via catch-up? no —
    # seed a single success manually, run briefly, expect exactly one downstream run
    fire = datetime.now(UTC).replace(microsecond=0)
    up = store.claim("scrape", fire, "seed")
    up.transition_to(RunState.RUNNING)
    up.transition_to(RunState.SUCCEEDED)
    up.exit_code = 0
    store.mark(up)

    sched = Scheduler(
        [Job(name="scrape", schedule="0 0 1 1 *", command=OK), _triggered("process", ["scrape"])],
        store,
        "t",
        handle_signals=False,
    )
    await _run_briefly(sched, 2.5)

    procs = store.history("process", 50)
    assert len(procs) == 1 and procs[0].scheduled_for == fire
    store.close()
