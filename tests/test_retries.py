"""M2 slice 1 — retries + backoff + the exit-code sentinel."""

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from punctual.executor import read_sentinel
from punctual.models import Backoff, Job, RetryPolicy, RunState
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

FAIL = [sys.executable, "-c", "import sys; sys.exit(1)"]
OK = [sys.executable, "-c", "pass"]


def _retry(max_: int, delay: float = 0.3) -> RetryPolicy:
    return RetryPolicy(max=max_, backoff=Backoff.FIXED, base_delay=timedelta(seconds=delay))


def _hourly_job(name: str, command: list[str], **kw) -> Job:
    # won't fire during a test; the single run comes from catch-up (below)
    return Job(name=name, schedule="0 * * * *", command=command, **kw)


def _seed_one_missed_fire(store: SqliteStore, job: str) -> None:
    store.set_last_fire(job, datetime.now(UTC) - timedelta(hours=1, minutes=30))


async def _run_briefly(sched: Scheduler, seconds: float) -> None:
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(seconds)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)


_FLAKE = """
import os, sys
p = sys.argv[1]
if os.path.exists(p):
    sys.exit(0)
open(p, "w").close()
sys.exit(1)
"""


async def test_failing_run_is_retried_then_succeeds(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    _seed_one_missed_fire(store, "j")
    # first attempt drops a marker and exits 1; the retry sees it and exits 0
    script = tmp_path / "flake.py"
    script.write_text(_FLAKE)
    cmd = [sys.executable, str(script), str(tmp_path / "done")]
    sched = Scheduler([_hourly_job("j", cmd, retries=_retry(3))], store, "t", handle_signals=False)

    await _run_briefly(sched, 3.0)

    rows = {r.attempt: r for r in store.history("j", 50)}
    assert rows[1].state is RunState.FAILED
    assert rows[2].state is RunState.SUCCEEDED
    store.close()


async def test_retries_stop_at_max(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    _seed_one_missed_fire(store, "j")
    sched = Scheduler([_hourly_job("j", FAIL, retries=_retry(2))], store, "t", handle_signals=False)

    await _run_briefly(sched, 3.0)

    attempts = sorted(r.attempt for r in store.history("j", 50))
    assert attempts == [1, 2, 3]  # first try + 2 retries, then it stops
    assert all(r.state is RunState.FAILED for r in store.history("j", 50))
    store.close()


async def test_timed_out_run_is_retried(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    _seed_one_missed_fire(store, "j")
    slow = _hourly_job(
        "j",
        ["bash", "-lc", "sleep 30"],
        timeout=timedelta(seconds=0.4),
        retries=_retry(1),
    )
    sched = Scheduler([slow], store, "t", handle_signals=False)

    await _run_briefly(sched, 4.0)

    rows = {r.attempt: r for r in store.history("j", 50)}
    assert rows[1].state is RunState.TIMED_OUT
    assert 2 in rows  # a retry was attempted
    store.close()


async def test_pending_retry_survives_a_restart(tmp_path):
    db = tmp_path / "s.db"
    _seed_one_missed_fire(SqliteStore(db), "j")
    # long backoff so the retry is still pending when the first daemon stops
    job = _hourly_job("j", FAIL, retries=_retry(1, delay=2.0))

    await _run_briefly(Scheduler([job], SqliteStore(db), "daemon-1", handle_signals=False), 1.0)

    store = SqliteStore(db)
    pending = [r for r in store.history("j", 50) if r.state is RunState.RETRYING]
    assert len(pending) == 1 and pending[0].attempt == 2

    # a fresh daemon picks it up
    await _run_briefly(Scheduler([job], store, "daemon-2", handle_signals=False), 4.0)
    rows = {r.attempt: r for r in store.history("j", 50)}
    assert rows[2].state is RunState.FAILED  # the retry ran (and failed again)
    store.close()


async def test_sentinel_resolves_a_lost_run(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    r = store.claim("j", datetime.now(UTC).replace(microsecond=0), "dead-daemon")
    assert r is not None
    r.transition_to(RunState.RUNNING)
    r.started_at = datetime.now(UTC)
    r.pid = 999_999_999  # dead
    store.mark(r)
    # the wrapper had written the real outcome before the daemon died
    rd = store.run_dir(r.id)
    rd.mkdir(parents=True)
    (rd / "exit").write_text(json.dumps({"code": 0, "signaled": False, "at": "x"}))

    sched = Scheduler([_hourly_job("j", OK)], store, "new", handle_signals=False)
    await _run_briefly(sched, 0.5)

    (row,) = [x for x in store.history("j", 50) if x.attempt == 1]
    assert row.state is RunState.SUCCEEDED  # not LOST
    assert row.exit_code == 0
    store.close()


def test_runner_writes_the_sentinel_and_passes_the_code_through(tmp_path):
    rc = subprocess.call(
        [
            sys.executable,
            "-m",
            "punctual._runner",
            str(tmp_path),
            "--",
            sys.executable,
            "-c",
            "import sys; sys.exit(7)",
        ]
    )
    assert rc == 7
    sentinel = read_sentinel(tmp_path)
    assert sentinel is not None and sentinel.exit_code == 7 and sentinel.signaled is False
