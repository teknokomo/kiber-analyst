from __future__ import annotations

from pathlib import Path

import pytest
from conftest import connect_disposable_postgres


def _write_env(path: Path, port: int) -> None:
    path.write_text(
        "\n".join(
            (
                "KNOWLEDGE_HOST=127.0.0.1",
                f"KNOWLEDGE_PORT={port}",
                "KNOWLEDGE_DB=knowledge",
                "KNOWLEDGE_USER=knowledge_app",
                "KNOWLEDGE_PASSWORD=fake-test-password",
                "",
            )
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_production_port_is_rejected_before_driver_connect(tmp_path: Path) -> None:
    env_path = tmp_path / "postgres-test.env"
    _write_env(env_path, 5433)
    connect_calls: list[dict[str, object]] = []

    def connector(**kwargs: object) -> object:
        connect_calls.append(kwargs)
        return object()

    with pytest.raises(AssertionError, match="separate disposable port"):
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
    _write_env(env_path, 55432)
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
            "port": 55432,
            "dbname": "knowledge",
            "user": "knowledge_app",
            "password": "fake-test-password",
            "autocommit": True,
        }
    ]
