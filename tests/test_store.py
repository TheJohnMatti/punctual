from datetime import UTC, datetime, timedelta

import pytest

from punctual.models import RunState
from punctual.store import PostgresStore, SqliteStore, store_from_url

# `store` is the parametrized (sqlite + postgres) fixture from conftest.py.


def _fire():
    return datetime(2026, 1, 1, 3, 0, tzinfo=UTC)


def test_claim_is_exactly_once(store):
    run = store.claim("backup", _fire(), by="node-a")
    assert run is not None and run.state is RunState.CLAIMED

    # a second daemon racing the same fire gets nothing
    assert store.claim("backup", _fire(), by="node-b") is None

    # ...but a different fire is fine
    other = datetime(2026, 1, 2, 3, 0, tzinfo=UTC)
    assert store.claim("backup", other, by="node-b") is not None


def test_retry_attempt_is_a_new_claim(store):
    store.claim("backup", _fire(), by="n", attempt=1)
    assert store.claim("backup", _fire(), by="n", attempt=1) is None
    assert store.claim("backup", _fire(), by="n", attempt=2) is not None


def test_mark_and_history(store):
    run = store.claim("backup", _fire(), by="n")
    run.transition_to(RunState.RUNNING)
    run.started_at = datetime.now(UTC)
    store.mark(run)
    run.transition_to(RunState.SUCCEEDED)
    run.finished_at = datetime.now(UTC)
    run.exit_code = 0
    store.mark(run)

    (h,) = store.history("backup")
    assert h.state is RunState.SUCCEEDED and h.exit_code == 0
    assert store.open_runs() == []


def test_job_clock(store):
    assert store.last_fire("backup") is None
    store.set_last_fire("backup", _fire())
    assert store.last_fire("backup") == _fire()


def test_record_skip_is_idempotent_per_fire(store):
    r = store.record_skip("j", _fire(), by="n", note="upstream x failed")
    assert r is not None and r.state is RunState.SKIPPED and r.note == "upstream x failed"
    assert store.record_skip("j", _fire(), by="n", note="again") is None


def test_schedule_retry_then_due(store):
    store.claim("j", _fire(), by="n", attempt=1)
    nb = datetime(2026, 1, 1, 3, 5, tzinfo=UTC)
    r = store.schedule_retry("j", _fire(), attempt=2, not_before=nb, by="n")
    assert r is not None and r.state is RunState.RETRYING
    assert store.due_retries(nb - timedelta(minutes=1)) == []
    assert [x.attempt for x in store.due_retries(nb)] == [2]
    assert store.next_retry_at() == nb


def test_lease_roundtrip(store):
    a = store.acquire_lease("res", "a", timedelta(seconds=30))
    assert a is not None and a.fence == 1
    assert store.acquire_lease("res", "b", timedelta(seconds=30)) is None
    assert store.renew_lease("res", "a", 1, timedelta(seconds=30)) is True
    store.release_lease("res", "a")
    assert store.lease_holder("res") is None


# --- store_from_url routing (no server needed) ------------------------


def test_store_from_url_none_is_default_sqlite(tmp_path, monkeypatch):
    monkeypatch.setenv("PUNCTUAL_DB", str(tmp_path / "x.db"))
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    s = store_from_url(None)
    assert isinstance(s, SqliteStore) and s.path == tmp_path / "x.db"
    s.close()


def test_store_from_url_sqlite_path(tmp_path, monkeypatch):
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    s = store_from_url(f"sqlite://{tmp_path / 'y.db'}")
    assert isinstance(s, SqliteStore) and s.path == tmp_path / "y.db"
    s.close()


def test_store_from_url_postgres_defers_to_postgresstore(monkeypatch):
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    seen = {}

    def fake_init(self, dsn):
        seen["dsn"] = dsn

    monkeypatch.setattr(PostgresStore, "__init__", fake_init)
    s = store_from_url("postgresql://u@h/db")
    assert isinstance(s, PostgresStore) and seen["dsn"] == "postgresql://u@h/db"


def test_store_from_url_sqlite_memory(monkeypatch):
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    s = store_from_url("sqlite://:memory:")
    assert isinstance(s, SqliteStore) and str(s.path) == ":memory:"
    assert s.child_env() == {}  # nothing to hand a subprocess
    s.close()


def test_store_from_url_env_overrides_arg(tmp_path, monkeypatch):
    monkeypatch.setenv("PUNCTUAL_STORE_URL", f"sqlite://{tmp_path / 'env.db'}")
    s = store_from_url("sqlite:///wherever/ignored.db")
    assert isinstance(s, SqliteStore) and s.path == tmp_path / "env.db"
    s.close()


def test_store_from_url_rejects_unknown_scheme(monkeypatch):
    monkeypatch.delenv("PUNCTUAL_STORE_URL", raising=False)
    with pytest.raises(ValueError, match="scheme"):
        store_from_url("mysql://nope")


def test_sqlite_child_env_is_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s = store_from_url("sqlite://rel.db")  # relative in the URL
    env = s.child_env()
    assert env["PUNCTUAL_DB"] == str((tmp_path / "rel.db").resolve())
    s.close()
