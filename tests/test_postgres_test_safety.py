from __future__ import annotations

from pathlib import Path

import pytest
from conftest import (
    connect_disposable_postgres,
    connect_loopback_postgres,
    loopback_postgres_connector,
)


def _write_env(path: Path, **overrides: str) -> None:
    values = {
        "KNOWLEDGE_HOST": "127.0.0.1",
        "KNOWLEDGE_PORT": "55432",
        "KNOWLEDGE_DB": "knowledge",
        "KNOWLEDGE_USER": "knowledge_app",
        "KNOWLEDGE_PASSWORD": "fake-test-password",
        **overrides,
    }
    path.write_text(
        "\n".join(
            f"{key}={value}" for key, value in values.items()
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)


@pytest.mark.parametrize(
    ("override", "value", "message"),
    [
        ("KNOWLEDGE_HOST", "192.0.2.10", "isolated loopback"),
        ("KNOWLEDGE_DB", "production", "isolated knowledge database"),
        ("KNOWLEDGE_USER", "knowledge_owner", "knowledge_app test role"),
        ("KNOWLEDGE_PORT", "5433", "separate disposable port"),
    ],
)
def test_unsafe_database_target_is_rejected_before_driver_connect(
    tmp_path: Path,
    override: str,
    value: str,
    message: str,
) -> None:
    env_path = tmp_path / "postgres-test.env"
    _write_env(env_path, **{override: value})
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> object:
        connect_calls.append(kwargs)
        return object()

    with pytest.raises(AssertionError, match=message):
        connect_disposable_postgres(
            env_path,
            connector,
            expected_path=env_path,
        )

    assert connect_calls == []


def test_validated_test_credentials_are_the_only_values_passed_to_driver(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "postgres-test.env"
    _write_env(env_path, KNOWLEDGE_PORT="55432")
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> object:
        connect_calls.append(kwargs)
        return "connected"

    result = connect_disposable_postgres(
        env_path,
        connector,
        expected_path=env_path,
        autocommit=True,
    )

    assert result == "connected"
    assert connect_calls == [
        {
            "host": "127.0.0.1",
            "hostaddr": "127.0.0.1",
            "port": 55432,
            "dbname": "knowledge",
            "user": "knowledge_app",
            "password": "fake-test-password",
            "autocommit": True,
        }
    ]


def test_libpq_environment_hostaddr_cannot_override_loopback_driver_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("psycopg")
    monkeypatch.setenv("PGHOSTADDR", "192.0.2.45")
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> str:
        connect_calls.append(kwargs)
        return "connected"

    result = connect_loopback_postgres(
        connector,
        "host=127.0.0.1 port=55432 dbname=knowledge",
        user="knowledge_app",
    )

    assert result == "connected"
    assert connect_calls[0]["host"] == "127.0.0.1"
    assert connect_calls[0]["hostaddr"] == "127.0.0.1"
    assert connect_calls[0]["port"] == "55432"


def test_nonloopback_conninfo_is_rejected_before_driver_connect() -> None:
    pytest.importorskip("psycopg")
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> object:
        connect_calls.append(kwargs)
        return object()

    with pytest.raises(AssertionError, match="non-loopback hosts"):
        connect_loopback_postgres(
            connector,
            "host=192.0.2.45 port=55432 dbname=knowledge",
        )

    assert connect_calls == []


def test_telegram_loader_connection_forces_hostaddr_before_driver_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from kiber_analyst import load

    monkeypatch.setenv("PGHOSTADDR", "192.0.2.45")
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> object:
        connect_calls.append(kwargs)
        return object()

    monkeypatch.setattr(psycopg, "connect", loopback_postgres_connector(connector))
    result = load._connect(
        {
            "KNOWLEDGE_HOST": "127.0.0.1",
            "KNOWLEDGE_PORT": "55432",
            "KNOWLEDGE_DB": "knowledge",
            "KNOWLEDGE_USER": "knowledge_app",
            "KNOWLEDGE_PASSWORD": "fake-test-password",
        }
    )

    assert result is not None
    assert connect_calls[0]["host"] == "127.0.0.1"
    assert connect_calls[0]["hostaddr"] == "127.0.0.1"
