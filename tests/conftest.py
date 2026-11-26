from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Callable

import pytest

TEST_ENV_NAME = "KIBER_ANALYST_INGEST_TEST_ENV"
_REQUIRED_TEST_KEYS = (
    "KNOWLEDGE_HOST",
    "KNOWLEDGE_PORT",
    "KNOWLEDGE_DB",
    "KNOWLEDGE_USER",
    "KNOWLEDGE_PASSWORD",
)


def validate_disposable_postgres_credentials(
    values: dict[str, str],
) -> dict[str, str]:
    """Return only validated connection fields for the isolated PG test target."""
    missing = [key for key in _REQUIRED_TEST_KEYS if not values.get(key)]
    if missing:
        raise AssertionError(
            f"PostgreSQL test env missing: {', '.join(missing)}"
        )

    credentials = {key: values[key] for key in _REQUIRED_TEST_KEYS}
    if credentials["KNOWLEDGE_HOST"] != "127.0.0.1":
        raise AssertionError("PostgreSQL tests require the isolated loopback database")
    if credentials["KNOWLEDGE_DB"] != "knowledge":
        raise AssertionError("PostgreSQL tests require the isolated knowledge database")
    if credentials["KNOWLEDGE_USER"] != "knowledge_app":
        raise AssertionError("PostgreSQL tests require the knowledge_app test role")
    try:
        port = int(credentials["KNOWLEDGE_PORT"])
    except ValueError:
        raise AssertionError("PostgreSQL test port is invalid") from None
    if not 1 <= port <= 65535 or port == 5433:
        raise AssertionError("PostgreSQL tests require a separate disposable port")
    return credentials


def load_disposable_postgres_env(
    configured_path: str | Path,
    *,
    expected_path: str | Path | None = None,
) -> dict[str, str]:
    """Validate the private test env and its destination before any DB connection."""
    raw_path = Path(configured_path)
    if raw_path.is_symlink():
        raise AssertionError("Disposable PostgreSQL test env must not be a symlink")
    try:
        resolved_path = raw_path.resolve(strict=True)
    except OSError:
        raise AssertionError("Disposable PostgreSQL test env file is unavailable") from None
    if not resolved_path.is_file():
        raise AssertionError("Disposable PostgreSQL test env must be a regular file")
    if expected_path is not None:
        try:
            expected = Path(expected_path).resolve(strict=True)
        except OSError:
            raise AssertionError("Declared PostgreSQL test env file is unavailable") from None
        if resolved_path != expected:
            raise AssertionError(
                "PostgreSQL integration tests must use their declared test env file"
            )
    if stat.S_IMODE(resolved_path.stat().st_mode) & 0o077:
        raise AssertionError("Disposable PostgreSQL test env must be private")

    from dotenv import dotenv_values

    values = {
        key: str(value)
        for key, value in dotenv_values(resolved_path).items()
        if value is not None
    }
    return validate_disposable_postgres_credentials(values)


def connect_disposable_postgres(
    configured_path: str | Path,
    connector: Callable[..., Any],
    *,
    expected_path: str | Path | None = None,
    autocommit: bool = False,
) -> Any:
    """Validate the test target before invoking the DB driver's connect call."""
    credentials = load_disposable_postgres_env(
        configured_path,
        expected_path=expected_path,
    )
    return connect_validated_postgres(
        credentials,
        connector,
        autocommit=autocommit,
    )


def connect_validated_postgres(
    values: dict[str, str],
    connector: Callable[..., Any],
    *,
    autocommit: bool = False,
) -> Any:
    """Connect only with credentials that pass the disposable-target checks."""
    credentials = validate_disposable_postgres_credentials(values)
    return connector(
        host=credentials["KNOWLEDGE_HOST"],
        hostaddr=credentials["KNOWLEDGE_HOST"],
        port=int(credentials["KNOWLEDGE_PORT"]),
        dbname=credentials["KNOWLEDGE_DB"],
        user=credentials["KNOWLEDGE_USER"],
        password=credentials["KNOWLEDGE_PASSWORD"],
        autocommit=autocommit,
    )


def connect_loopback_postgres(
    connector: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Force an explicit loopback hostaddr for any psycopg connection form."""
    if len(args) > 1:
        raise AssertionError("PostgreSQL tests require a single connection string")
    conninfo = args[0] if args else kwargs.pop("conninfo", None)
    if args and "conninfo" in kwargs:
        raise AssertionError("PostgreSQL connection string was supplied more than once")

    if conninfo is None:
        params: dict[str, Any] = {}
    elif isinstance(conninfo, Mapping):
        params = dict(conninfo)
    else:
        from psycopg.conninfo import conninfo_to_dict

        params = conninfo_to_dict(conninfo)
    params.update(kwargs)

    host = params.get("host")
    if not isinstance(host, str) or not host or "," in host:
        raise AssertionError("PostgreSQL tests require one explicit loopback host")
    if host.lower() == "localhost":
        hostaddr = "127.0.0.1"
    else:
        try:
            parsed_host = ip_address(host)
        except ValueError:
            raise AssertionError("PostgreSQL tests require a numeric loopback host") from None
        if not parsed_host.is_loopback:
            raise AssertionError("PostgreSQL tests reject non-loopback hosts")
        hostaddr = parsed_host.compressed

    supplied_hostaddr = params.get("hostaddr")
    if supplied_hostaddr is not None:
        try:
            parsed_hostaddr = ip_address(str(supplied_hostaddr))
        except ValueError:
            raise AssertionError("PostgreSQL tests reject non-numeric hostaddr") from None
        if not parsed_hostaddr.is_loopback:
            raise AssertionError("PostgreSQL tests reject non-loopback hostaddr")

    # Passing hostaddr explicitly makes libpq ignore PGHOSTADDR from the
    # environment; the fixture also removes that variable for test clarity.
    params["hostaddr"] = hostaddr
    return connector(**params)


def loopback_postgres_connector(connector: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a psycopg-compatible connector with the loopback-only policy."""
    def guarded_connect(*args: Any, **kwargs: Any) -> Any:
        return connect_loopback_postgres(connector, *args, **kwargs)

    return guarded_connect


@pytest.fixture
def loopback_postgres_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGHOSTADDR", raising=False)
    try:
        import psycopg
    except ImportError:
        return

    original_connect = psycopg.connect
    monkeypatch.setattr(psycopg, "connect", loopback_postgres_connector(original_connect))


@pytest.fixture
def disposable_postgres_credentials(
    monkeypatch: pytest.MonkeyPatch,
    loopback_postgres_driver: None,
) -> dict[str, str]:
    configured_path = os.environ.get(TEST_ENV_NAME)
    if not configured_path:
        pytest.skip(f"set {TEST_ENV_NAME} to an isolated disposable test database env file")

    credentials = load_disposable_postgres_env(configured_path)

    def test_env(path: Path) -> dict[str, str]:
        return load_disposable_postgres_env(path, expected_path=configured_path)

    # Production keeps its fixed loopback-port guard. Only this explicitly
    # supplied private test env can use a separate scratch port.
    from kiber_analyst import external_embed, external_load, load

    for module in (load, external_embed, external_load):
        monkeypatch.setattr(module, "_required_env", test_env)
    return credentials


@pytest.fixture
def optional_disposable_postgres_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str] | None:
    """Enable full load checks when a guarded scratch PostgreSQL is configured."""
    configured_path = os.environ.get(TEST_ENV_NAME)
    if not configured_path:
        return None
    credentials = load_disposable_postgres_env(configured_path)

    def test_env(path: Path) -> dict[str, str]:
        return load_disposable_postgres_env(path, expected_path=configured_path)

    from kiber_analyst import external_embed, external_load, load

    for module in (load, external_embed, external_load):
        monkeypatch.setattr(module, "_required_env", test_env)
    return credentials
