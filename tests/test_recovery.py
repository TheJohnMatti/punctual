"""M1 slice 3 — restart recovery (O2b) and catch-up (O3)."""

import asyncio
import sys
from datetime import UTC, datetime, timedelta

from punctual.models import Job, MissedPolicy, RunState
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

CMD = [sys.executable, "-c", "pass"]
DEAD_PID = 999_999_999  # nothing runs here


def _job(name: str, schedule: str = "0 0 1 1 *", **kw) -> Job:
    # default schedule = "yearly" so only recovery/catch-up act during a test
    return Job(name=name, schedule=schedule, command=CMD, **kw)


def _fire(minutes_ago: int) -> datetime:
    return (datetime.now(UTC) - timedelta(minutes=minutes_ago)).replace(microsecond=0)


async def _run_briefly(sched: Scheduler, seconds: float) -> None:
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(seconds)
    sched._request_stop()
    await asyncio.wait_for(task, timeout=10)


# --- catch-up (O3): _plan_catch_up is sync, test it directly ----------------


def _plan(store, job) -> list:
    return Scheduler([job], store, "t", handle_signals=False)._plan_catch_up(job)


def test_no_catch_up_without_history(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    assert _plan(store, _job("j", "* * * * * *")) == []


def test_catch_up_skip_advances_the_clock_and_runs_nothing(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.set_last_fire("j", _fire(5))
    job = _job("j", "* * * * * *", missed=MissedPolicy.SKIP)
    assert _plan(store, job) == []
    assert store.last_fire("j") > _fire(1)  # clock moved forward


def test_catch_up_run_latest_returns_one(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.set_last_fire("j", _fire(5))
    job = _job("j", "* * * * * *", missed=MissedPolicy.RUN_LATEST)
    assert len(_plan(store, job)) == 1


def test_catch_up_run_each_returns_all_missed(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.set_last_fire("j", _fire(1))  # ~60 per-second fires missed
    job = _job("j", "* * * * * *", missed=MissedPolicy.RUN_EACH, catch_up_cap=0)
    assert 50 <= len(_plan(store, job)) <= 62


def test_catch_up_run_each_respects_the_cap(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.set_last_fire("j", _fire(1))
    job = _job("j", "* * * * * *", missed=MissedPolicy.RUN_EACH, catch_up_cap=5)
    plan = _plan(store, job)
    assert len(plan) == 5
    assert plan == sorted(plan)  # newest 5, in order


# --- recovery (O2b) --------------------------------------------------------


async def test_resumes_a_claimed_run(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    stale = store.claim("j", _fire(1), "dead-daemon")
    assert stale is not None and stale.state is RunState.CLAIMED

    await _run_briefly(Scheduler([_job("j")], store, "new", handle_signals=False), 1.0)

    (row,) = [r for r in store.history("j", 50) if r.scheduled_for == stale.scheduled_for]
    assert row.state is RunState.SUCCEEDED
    store.close()


async def test_running_orphan_becomes_lost_and_fails_when_not_idempotent(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    r = store.claim("j", _fire(1), "dead-daemon")
    assert r is not None
    r.transition_to(RunState.RUNNING)
    r.started_at = datetime.now(UTC)
    r.pid = DEAD_PID
    store.mark(r)

    await _run_briefly(Scheduler([_job("j")], store, "new", handle_signals=False), 0.5)

    rows = store.history("j", 50)
    assert len(rows) == 1  # no retry
    assert rows[0].state is RunState.LOST


async def test_running_orphan_is_retried_when_idempotent(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    r = store.claim("j", _fire(1), "dead-daemon")
    assert r is not None
    r.transition_to(RunState.RUNNING)
    r.started_at = datetime.now(UTC)
    r.pid = DEAD_PID
    store.mark(r)

    job = _job("j", idempotent=True)  # -> on_lost = retry
    await _run_briefly(Scheduler([job], store, "new", handle_signals=False), 1.0)

    rows = {x.attempt: x for x in store.history("j", 50)}
    assert rows[1].state is RunState.LOST
    assert 2 in rows and rows[2].state is RunState.SUCCEEDED
    store.close()
