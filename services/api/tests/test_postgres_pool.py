from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
import sys

import pytest

from radar.storage import PostgresRepository


def test_postgres_repositories_share_a_bounded_connection_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[object] = []
    connection = object()

    class FakePool:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.opened = False
            created.append(self)

        def open(self) -> None:
            self.opened = True

        @contextmanager
        def connection(self):
            yield connection

    monkeypatch.setattr(PostgresRepository, "_pools", {})
    monkeypatch.setitem(sys.modules, "psycopg_pool", SimpleNamespace(ConnectionPool=FakePool))
    monkeypatch.setenv("POSTGRES_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("POSTGRES_POOL_MAX_SIZE", "4")
    monkeypatch.setenv("POSTGRES_POOL_TIMEOUT_SECONDS", "12")

    first = PostgresRepository("postgresql://shared")
    second = PostgresRepository("postgresql://shared")
    with first.connection() as first_connection, second.connection() as second_connection:
        assert first_connection is connection
        assert second_connection is connection

    assert len(created) == 1
    assert created[0].opened is True
    assert created[0].kwargs["min_size"] == 1
    assert created[0].kwargs["max_size"] == 4
    assert created[0].kwargs["timeout"] == 12


def test_postgres_pool_rejects_invalid_size_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PostgresRepository, "_pools", {})
    monkeypatch.setenv("POSTGRES_POOL_MIN_SIZE", "5")
    monkeypatch.setenv("POSTGRES_POOL_MAX_SIZE", "4")

    with pytest.raises(RuntimeError, match="pool sizes"):
        PostgresRepository("postgresql://invalid")._pool_for("postgresql://invalid")
