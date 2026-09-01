"""M2 slice 2 — quarantine circuit-breaker + on_fail / on_quarantine notify."""

import asyncio
import json
import sys
from datetime import timedelta

from punctual.models import Backoff, Job, RetryPolicy, RunState
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

FAIL = [sys.executable, "-c", "import sys; sys.exit(1)"]

# exit 0 iff argv[1] exists — an external "is the problem fixed yet" gate
_GATED_SRC = "import os,sys; sys.exit(0 if os.path.exists(sys.argv[1]) else 1)"
# fail once, then succeed forever (drops its own marker)
_FLAKE_SRC = (
    "import os,sys; p=sys.argv[1]\n"
    "sys.exit(0) if os.path.exists(p) else (open(p,'w').close(), sys.exit(1))"
)


def _gated(marker) -> list[str]:
    return [sys.executable, "-c", _GATED_SRC, str(marker)]


def _job(name: str, command=FAIL, **kw) -> Job:
    return Job(name=name, schedule="* * * * * *", command=command, **kw)


async def _run_briefly(sched: Scheduler, seconds: float) -> None:
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(seconds)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)


async def test_consecutive_failures_trip_quarantine(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    sched = Scheduler([_job("j", quarantine_after=3)], store, "t", handle_signals=False)

    await _run_briefly(sched, 6.0)

    st = store.job_state("j")
    assert st.quarantined
    assert st.consecutive_failures >= 3
    assert st.skipped_quarantined >= 1  # fires kept coming due, and were dropped
    # only a handful of runs actually happened before the breaker opened
    assert len(store.history("j", 50)) <= 5
    store.close()


async def test_a_success_resets_the_counter(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    marker = tmp_path / "ok"
    sched = Scheduler(
        [_job("j", _gated(marker), quarantine_after=10)], store, "t", handle_signals=False
    )

    task = asyncio.create_task(sched.run())
    await asyncio.sleep(2.5)
    assert store.job_state("j").consecutive_failures >= 1  # failing so far
    marker.write_text("")  # from now on the job succeeds
    await asyncio.sleep(2.5)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)

    st = store.job_state("j")
    assert not st.quarantined
    assert st.consecutive_failures == 0  # a success reset it
    store.close()


async def test_a_fire_that_succeeds_on_retry_does_not_count(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    cmd = [sys.executable, "-c", _FLAKE_SRC, str(tmp_path / "ok")]
    job = _job(
        "j",
        cmd,
        quarantine_after=1,  # would trip on a single failed *fire*
        retries=RetryPolicy(max=2, backoff=Backoff.FIXED, base_delay=timedelta(seconds=0.2)),
    )
    sched = Scheduler([job], store, "t", handle_signals=False)

    await _run_briefly(sched, 3.0)

    st = store.job_state("j")
    assert not st.quarantined  # attempt 1 failed, attempt 2 succeeded => fire OK
    assert st.consecutive_failures == 0
    store.close()


async def test_resume_clears_quarantine(tmp_path):
    db = tmp_path / "s.db"
    await _run_briefly(
        Scheduler([_job("j", quarantine_after=2)], SqliteStore(db), "d1", handle_signals=False), 5.0
    )
    store = SqliteStore(db)
    assert store.job_state("j").quarantined
    store.request_resume("j")

    # daemon back up: it should clear quarantine and start running the job again
    await _run_briefly(Scheduler([_job("j", ["true"])], store, "d2", handle_signals=False), 3.0)
    st = store.job_state("j")
    assert not st.quarantined
    assert any(r.state is RunState.SUCCEEDED for r in store.history("j", 50))
    store.close()


async def test_cooldown_probe_recovers_a_healthy_job(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    marker = tmp_path / "ok"
    job = _job("j", _gated(marker), quarantine_after=2, quarantine_cooldown=timedelta(seconds=2))
    sched = Scheduler([job], store, "t", handle_signals=False)

    task = asyncio.create_task(sched.run())
    await asyncio.sleep(3.0)  # trips quarantine (marker absent)
    assert store.job_state("j").quarantined
    marker.write_text("")  # the underlying problem is "fixed"
    await asyncio.sleep(4.0)  # cooldown elapses, probe fires, succeeds
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)

    assert not store.job_state("j").quarantined
    store.close()


async def test_on_quarantine_notification_fires(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    hook = tmp_path / "paged.json"
    script = tmp_path / "sink.py"
    script.write_text("import sys, pathlib\npathlib.Path(sys.argv[1]).write_text(sys.stdin.read())")
    job = _job(
        "j",
        quarantine_after=2,
        on_quarantine=f"exec:{sys.executable} {script} {hook}",
    )

    await _run_briefly(Scheduler([job], store, "t", handle_signals=False), 5.0)

    assert hook.exists()
    event = json.loads(hook.read_text())
    assert event["event"] == "quarantine" and event["job"] == "j"
    store.close()
