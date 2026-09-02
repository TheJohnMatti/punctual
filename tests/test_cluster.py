"""M5 slice 1 — lease-based leader election + hot standby."""

import asyncio
from datetime import timedelta

import pytest

from punctual import scheduler as sched_mod
from punctual.models import Job
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

R = "scheduler"


def _job(name: str, schedule: str = "0 0 1 1 *") -> Job:
    return Job(name=name, schedule=schedule, command=["true"])


# --- store-level lease mechanics ---------------------------------------


def test_acquire_is_exclusive_while_live(tmp_path):
    s = SqliteStore(tmp_path / "d.db")
    a = s.acquire_lease(R, "a", timedelta(seconds=30))
    assert a is not None and a.holder == "a" and a.fence == 1
    assert s.acquire_lease(R, "b", timedelta(seconds=30)) is None
    assert s.lease_holder(R) == "a"
    s.close()


def test_holder_can_reacquire_and_renew(tmp_path):
    s = SqliteStore(tmp_path / "d.db")
    a = s.acquire_lease(R, "a", timedelta(seconds=30))
    assert s.renew_lease(R, "a", a.fence, timedelta(seconds=30)) is True
    # wrong fence — a zombie ex-leader — is rejected
    assert s.renew_lease(R, "a", a.fence + 5, timedelta(seconds=30)) is False
    # wrong holder is rejected
    assert s.renew_lease(R, "b", a.fence, timedelta(seconds=30)) is False
    s.close()


def test_expired_lease_is_taken_over_with_bumped_fence(tmp_path):
    s = SqliteStore(tmp_path / "d.db")
    a = s.acquire_lease(R, "a", timedelta(seconds=-1))  # already expired
    assert a is not None
    assert s.lease_holder(R) is None  # nobody holds a live lease
    b = s.acquire_lease(R, "b", timedelta(seconds=30))
    assert b is not None and b.holder == "b" and b.fence == a.fence + 1
    # the old leader's renew now fails — fence moved
    assert s.renew_lease(R, "a", a.fence, timedelta(seconds=30)) is False
    s.close()


def test_release_frees_the_lease(tmp_path):
    s = SqliteStore(tmp_path / "d.db")
    s.acquire_lease(R, "a", timedelta(seconds=30))
    s.release_lease(R, "a")
    assert s.lease_holder(R) is None
    assert s.acquire_lease(R, "b", timedelta(seconds=30)) is not None
    s.close()


# --- scheduler leader/standby behaviour --------------------------------


async def test_solo_daemon_is_always_leader(tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    sc = Scheduler([_job("a")], store, "solo", handle_signals=False)
    task = asyncio.create_task(sc.run())
    for _ in range(40):
        await asyncio.sleep(0.05)
        if sc._leader:
            break
    assert sc._leader and store.lease_holder(R) is None  # no lease taken in solo mode
    assert sc.control_status()["role"] == "solo"
    sc.request_drain()
    await asyncio.wait_for(task, timeout=10)
    store.close()


async def test_leader_steps_down_when_a_renew_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(sched_mod, "_LEASE_TTL", timedelta(seconds=1))
    monkeypatch.setattr(sched_mod, "_LEASE_RENEW", timedelta(seconds=0.2))
    store = SqliteStore(tmp_path / "d.db")
    sc = Scheduler([_job("a")], store, "one", handle_signals=False, cluster=True)

    real_renew = store.renew_lease
    calls = {"n": 0}

    def flaky_renew(*a, **k):
        calls["n"] += 1
        return False if calls["n"] == 3 else real_renew(*a, **k)

    monkeypatch.setattr(store, "renew_lease", flaky_renew)
    task = asyncio.create_task(sc.run())

    for _ in range(50):
        await asyncio.sleep(0.1)
        if sc._leader and sc._fence >= 2:  # stepped down, then re-acquired
            break
    assert sc._fence >= 2

    sc.request_drain()
    await asyncio.wait_for(task, timeout=10)
    store.close()


async def test_second_daemon_stands_by_then_takes_over(tmp_path, monkeypatch):
    monkeypatch.setattr(sched_mod, "_LEASE_TTL", timedelta(seconds=1))
    monkeypatch.setattr(sched_mod, "_LEASE_RENEW", timedelta(seconds=0.3))
    db = tmp_path / "d.db"
    store1, store2 = SqliteStore(db), SqliteStore(db)

    leader = Scheduler([_job("a")], store1, "one", handle_signals=False, cluster=True)
    standby = Scheduler([_job("a")], store2, "two", handle_signals=False, cluster=True)
    t1 = asyncio.create_task(leader.run())
    t2 = asyncio.create_task(standby.run())

    for _ in range(40):
        await asyncio.sleep(0.1)
        if leader._leader and not standby._leader:
            break
    assert leader._leader and not standby._leader
    assert store1.lease_holder(R) == "one"
    assert standby.healthz() == (True, "standby")

    # leader crashes: it never gets to release the lease — standby must wait
    # for the lease to expire before taking over.
    monkeypatch.setattr(store1, "release_lease", lambda *a, **k: None)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    store1.close()

    for _ in range(60):
        await asyncio.sleep(0.1)
        if standby._leader:
            break
    assert standby._leader and store2.lease_holder(R) == "two"

    standby.request_drain()
    await asyncio.wait_for(t2, timeout=10)
    store2.close()
