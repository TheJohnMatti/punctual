"""M2 slice 4 — the control socket."""

import asyncio
import os
import tempfile

import pytest

from punctual import control
from punctual.models import Job
from punctual.scheduler import Scheduler
from punctual.store import SqliteStore

SLEEP = ["bash", "-lc", "sleep 30"]


def _job(name: str, command=("true",), schedule: str = "0 0 1 1 *") -> Job:
    return Job(name=name, schedule=schedule, command=list(command))


@pytest.fixture
def sock(monkeypatch):
    # short path — macOS caps AF_UNIX at ~104 chars, pytest's tmp_path is longer
    fd, p = tempfile.mkstemp(prefix="pnc-", suffix=".sock", dir="/tmp")
    os.close(fd)
    os.unlink(p)
    monkeypatch.setenv("PUNCTUAL_SOCKET", p)
    yield p
    if os.path.exists(p):
        os.unlink(p)


async def _serve(jobs, store, **kw):
    sched = Scheduler(jobs, store, "t", handle_signals=False, control=True, **kw)
    task = asyncio.create_task(sched.run())
    for _ in range(60):
        await asyncio.sleep(0.05)
        try:
            await control._request({"cmd": "ping"}, control.socket_path(), timeout=1)
            return sched, task
        except control.NotRunning:
            continue
    raise AssertionError("control socket never came up")


async def _call(cmd, **kw):
    return await control._request({"cmd": cmd, **kw}, control.socket_path(), timeout=5)


async def _shutdown(sched, task):
    sched.request_drain()
    await asyncio.wait_for(task, timeout=10)


async def test_ping(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    sched, task = await _serve([_job("a"), _job("b")], store)
    r = await _call("ping")
    assert r["ok"] and r["jobs"] == 2 and r["in_flight"] == 0 and "pid" in r
    await _shutdown(sched, task)
    store.close()


async def test_drain_makes_the_daemon_exit(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    _sched, task = await _serve([_job("a")], store)
    assert (await _call("drain"))["ok"]
    await asyncio.wait_for(task, timeout=10)  # it exited on its own
    store.close()


async def test_stop_kill_takes_down_in_flight(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    _sched, task = await _serve([_job("hang", SLEEP, "* * * * * *")], store)
    await asyncio.sleep(2)
    assert (await _call("ping"))["in_flight"] == 1
    r = await _call("stop", kill=True)
    assert r["ok"] and r["killed"]
    await asyncio.wait_for(task, timeout=10)
    store.close()


async def test_reload_adds_and_removes_jobs(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    current = [_job("keep"), _job("drop")]
    sched, task = await _serve(list(current), store, config_reload=lambda: current[:])

    current[:] = [_job("keep"), _job("added")]
    r = await _call("reload")
    assert r["added"] == ["added"] and r["removed"] == ["drop"] and r["changed"] == []
    assert {j.name for j in sched.jobs} == {"keep", "added"}

    current[:] = [Job(name="keep", schedule="*/5 * * * *", command=["true"]), _job("added")]
    r = await _call("reload")
    assert r["changed"] == ["keep"] and "restart" in r["note"]
    assert next(j for j in sched.jobs if j.name == "keep").schedule == "0 0 1 1 *"

    await _shutdown(sched, task)
    store.close()


async def test_trigger_over_the_socket(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    sched, task = await _serve([_job("a")], store)  # yearly schedule
    r = await _call("trigger", job="a")
    assert r["ok"] and r["job"] == "a"
    await asyncio.sleep(1.0)
    await _shutdown(sched, task)
    assert store.history("a") and store.history("a")[0].state.value == "succeeded"
    store.close()


async def test_trigger_unknown_job(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    sched, task = await _serve([_job("a")], store)
    r = await _call("trigger", job="ghost")
    assert not r["ok"] and "no job" in r["error"]
    await _shutdown(sched, task)
    store.close()


async def test_metrics_and_healthz_over_the_socket(sock, tmp_path):
    store = SqliteStore(tmp_path / "d.db")
    sched, task = await _serve([_job("a")], store)
    m = await _call("metrics")
    assert m["ok"] and "punctual_up 1" in m["text"]
    h = await _call("healthz")
    assert h["ok"] and h["reason"] == "ok"
    await _shutdown(sched, task)
    store.close()


def test_client_raises_when_no_daemon(sock):
    with pytest.raises(control.NotRunning):
        control.request("ping", timeout=1)
