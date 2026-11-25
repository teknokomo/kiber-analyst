from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _allow_explicit_disposable_postgres_port(monkeypatch: pytest.MonkeyPatch) -> None:
    env_name = "KIBER_ANALYST_INGEST_TEST_ENV"
    configured_path = os.environ.get(env_name)
    if not configured_path:
        return

    from dotenv import dotenv_values

    expected_path = Path(configured_path).resolve()
    if stat.S_IMODE(expected_path.stat().st_mode) & 0o077:
        raise AssertionError("Disposable PostgreSQL test env must be private")

    def test_env(path: Path) -> dict[str, str]:
        if Path(path).resolve() != expected_path:
            raise AssertionError("PostgreSQL integration tests must use their declared test env file")
        values = {key: str(value) for key, value in dotenv_values(path).items() if value}
        if values.get("KNOWLEDGE_HOST") != "127.0.0.1" or values.get("KNOWLEDGE_DB") != "knowledge":
            raise AssertionError("PostgreSQL tests require the isolated loopback knowledge database")
        try:
            port = int(values["KNOWLEDGE_PORT"])
        except (KeyError, ValueError):
            raise AssertionError("PostgreSQL test port is invalid") from None
        if not 1 <= port <= 65535 or port == 5433:
            raise AssertionError("PostgreSQL integration tests require a separate disposable port")
        required = (
            "KNOWLEDGE_HOST", "KNOWLEDGE_PORT", "KNOWLEDGE_DB",
            "KNOWLEDGE_USER", "KNOWLEDGE_PASSWORD",
        )
        missing = [key for key in required if not values.get(key)]
        if missing:
            raise AssertionError(f"PostgreSQL test env missing: {', '.join(missing)}")
        return {key: values[key] for key in required}

    # Production keeps its fixed loopback-port guard. Only an explicitly
    # supplied private test env can use a dynamically mapped scratch port.
    from kiber_analyst import external_embed, external_load, load

    for module in (load, external_embed, external_load):
        monkeypatch.setattr(module, "_required_env", test_env)
