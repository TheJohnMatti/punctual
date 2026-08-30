import asyncio
import sys
from datetime import UTC, datetime, timedelta

from punctual.models import Job, RunState
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore


def _echo_job(name: str, msg: str = "tick", **kw) -> Job:
    # 6-field cron: fire every second, so tests don't wait on wall-clock minutes.
    return Job(
        name=name,
        schedule="* * * * * *",
        command=[sys.executable, "-c", f"print({msg!r})"],
        **kw,
    )


async def _run_briefly(sched: Scheduler, seconds: float) -> None:
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(seconds)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)


async def test_fires_on_schedule_and_records_a_run(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler([_echo_job("tick")], store, "test", handle_signals=False)

    await _run_briefly(sched, 2.5)

    runs = store.history("tick")
    assert runs, "expected at least one run"
    latest = runs[0]
    assert latest.state is RunState.SUCCEEDED
    assert latest.exit_code == 0
    assert (latest.stdout_tail or "").strip() == "tick"
    assert latest.duration is not None
    store.close()


async def test_failing_job_is_recorded_as_failed(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    job = Job(
        name="bad",
        schedule="* * * * * *",
        command=[sys.executable, "-c", "import sys; sys.exit(2)"],
    )
    sched = Scheduler([job], store, "test", handle_signals=False)

    await _run_briefly(sched, 2.5)

    runs = store.history("bad")
    assert runs
    assert runs[0].state is RunState.FAILED
    assert runs[0].exit_code == 2
    store.close()


async def test_no_double_dispatch_for_one_fire(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler([_echo_job("tick")], store, "test", handle_signals=False)

    await _run_briefly(sched, 3.0)

    runs = store.history("tick", limit=100)
    fires = [r.scheduled_for for r in runs]
    assert len(fires) == len(set(fires)), "each fire should produce exactly one run"
    store.close()


async def test_disabled_job_never_fires(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler([_echo_job("off", enabled=False)], store, "test", handle_signals=False)

    await _run_briefly(sched, 2.0)

    assert store.history("off") == []
    store.close()


async def test_skips_fires_from_before_startup(tmp_path):
    # job_clock says this job last fired an hour ago; slice 1 does NOT catch up,
    # so nothing from that gap should run.
    store = SqliteStore(tmp_path / "s.db")
    store.set_last_fire("tick", datetime.now(UTC) - timedelta(hours=1))
    sched = Scheduler([_echo_job("tick")], store, "test", handle_signals=False)

    await _run_briefly(sched, 2.5)

    runs = store.history("tick", limit=100)
    # a handful of per-second fires from the short window, nowhere near an hour's worth
    assert 0 < len(runs) < 20
    store.close()
