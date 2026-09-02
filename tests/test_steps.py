"""M6 slice 2 — step(name, fn) durable checkpoints."""

import os
import subprocess
import sys
import textwrap
import uuid
from datetime import UTC, datetime

import pytest

import punctual.steps as steps_mod
from punctual.steps import step

FIRE = datetime(2026, 1, 1, tzinfo=UTC)


# --- store-level (both backends via the `store` fixture) --------------


def test_step_rows_roundtrip(store):
    assert store.get_step("j", FIRE, "a") == (False, None)
    store.record_step("j", FIRE, "a", "[1, 2, 3]")
    assert store.get_step("j", FIRE, "a") == (True, [1, 2, 3])
    # a null result is "found", not "missing"
    store.record_step("j", FIRE, "b", "null")
    assert store.get_step("j", FIRE, "b") == (True, None)
    store.record_step("j", FIRE, "a", '"changed"')  # ON CONFLICT DO NOTHING
    assert store.get_step("j", FIRE, "a") == (True, [1, 2, 3])
    assert [n for n, _ in store.steps_for("j", FIRE)] == ["a", "b"]


# --- the step() helper ----------------------------------------------


@pytest.fixture
def _job_env(tmp_path, monkeypatch):
    monkeypatch.setenv("PUNCTUAL_DB", str(tmp_path / "s.db"))
    monkeypatch.setenv("PUNCTUAL_JOB", "sync")
    monkeypatch.setenv("PUNCTUAL_SCHEDULED_FOR", FIRE.isoformat())
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    steps_mod._store = None
    yield
    steps_mod._store = None


def test_step_runs_once_then_replays(_job_env):
    calls = []
    assert step("fetch", lambda: calls.append(1) or "done") == "done"
    assert step("fetch", lambda: calls.append(1) or "other") == "done"  # cached
    assert calls == [1]


def test_step_outside_a_job_is_an_error(monkeypatch):
    monkeypatch.delenv("PUNCTUAL_JOB", raising=False)
    steps_mod._store = None
    with pytest.raises(RuntimeError, match="inside a job run by punctual"):
        step("x", lambda: 1)


def test_step_rejects_non_json_result(_job_env):
    with pytest.raises(TypeError, match="not JSON-serialisable"):
        step("bad", lambda: object())


def test_completed_steps_survive_a_retry(tmp_path, monkeypatch):
    """The real contract: a job that dies after step 1, re-run, step 1 not redone."""
    mod = f"pnc_steps_{uuid.uuid4().hex[:12]}"
    marker = tmp_path / "fetch.calls"
    (tmp_path / f"{mod}.py").write_text(
        textwrap.dedent(f"""
        import os, pathlib
        from punctual import job, step

        M = pathlib.Path({str(marker)!r})

        @job("sync", schedule="@daily")
        def sync():
            def fetch():
                M.write_text(str(int(M.read_text() or 0) + 1) if M.exists() else "1")
                return [1, 2]
            rows = step("fetch", fetch)
            if not os.environ.get("PNC_LET_IT_PASS"):
                raise SystemExit("load failed")
            step("load", lambda: len(rows))
        """)
    )
    env = {
        **os.environ,
        "PYTHONPATH": str(tmp_path),
        "PUNCTUAL_DB": str(tmp_path / "s.db"),
        "PUNCTUAL_JOB": "sync",
        "PUNCTUAL_SCHEDULED_FOR": FIRE.isoformat(),
    }

    argv = [sys.executable, "-m", "punctual._inproc", f"{mod}:sync"]
    r1 = subprocess.run(argv, capture_output=True, text=True, env=env, cwd=tmp_path)
    assert r1.returncode != 0 and marker.read_text() == "1"

    env["PNC_LET_IT_PASS"] = "1"
    r2 = subprocess.run(argv, capture_output=True, text=True, env=env, cwd=tmp_path)
    assert r2.returncode == 0
    assert marker.read_text() == "1"  # fetch() was NOT called again
