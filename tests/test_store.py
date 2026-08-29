from datetime import UTC, datetime

import pytest

from punctual.models import RunState
from punctual.store import SqliteStore


@pytest.fixture
def store(tmp_path):
    s = SqliteStore(tmp_path / "t.db")
    yield s
    s.close()


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
