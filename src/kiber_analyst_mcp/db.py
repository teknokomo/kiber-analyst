from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from kiber_analyst_mcp.config import MCPConfig


def connect(config: MCPConfig) -> Any:
    try:
        import psycopg
    except ImportError:
        raise RuntimeError("PostgreSQL driver is unavailable") from None
    return psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
        connect_timeout=3,
        application_name="kiber-analyst-mcp",
        autocommit=True,
    )


@contextmanager
def readonly_connection(config: MCPConfig) -> Iterator[Any]:
    conn = connect(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET statement_timeout = '15s'")
            cursor.execute("SET idle_in_transaction_session_timeout = '30s'")
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
            yield conn
    finally:
        conn.close()
