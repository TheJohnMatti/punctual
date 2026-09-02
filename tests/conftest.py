"""Shared fixtures.

The `store` fixture is parametrized over both backends. The Postgres leg needs a
reachable server via ``$PUNCTUAL_TEST_PG_DSN`` (an admin DSN — it creates and
drops a throwaway database per test, so xdist workers don't collide); it `skip`s
when that's unset. CI sets it; locally the SQLite leg still runs.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

from punctual import registry
from punctual.store import PostgresStore, SqliteStore, Store

_PG_ADMIN_DSN = os.environ.get("PUNCTUAL_TEST_PG_DSN")


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    """@punctual.job registrations are process-global — reset around every test."""
    registry.clear()
    yield
    registry.clear()


@pytest.fixture(params=["sqlite", "postgres"])
def store(request: pytest.FixtureRequest, tmp_path) -> Iterator[Store]:
    if request.param == "sqlite":
        s: Store = SqliteStore(tmp_path / "t.db")
        yield s
        s.close()
        return

    if not _PG_ADMIN_DSN:
        pytest.skip("no $PUNCTUAL_TEST_PG_DSN — Postgres backend runs in CI")

    import psycopg
    from psycopg.conninfo import make_conninfo

    name = f"pnc_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(_PG_ADMIN_DSN, autocommit=True) as c:
        c.execute(f'CREATE DATABASE "{name}"')
    try:
        pg = PostgresStore(make_conninfo(_PG_ADMIN_DSN, dbname=name))
        try:
            yield pg
        finally:
            pg.close()
    finally:
        with psycopg.connect(_PG_ADMIN_DSN, autocommit=True) as c:
            c.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
