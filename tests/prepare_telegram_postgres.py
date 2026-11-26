"""Create a guarded disposable PostgreSQL target for Telegram CI tests."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from kiber_analyst.migrate import run_migrations

BASELINE = Path(__file__).parent / "fixtures" / "migration_runner_baseline.sql"


def main() -> None:
    admin_dsn = os.environ.get("KIBER_ANALYST_TELEGRAM_TEST_ADMIN_DSN")
    output = os.environ.get("KIBER_ANALYST_TELEGRAM_TEST_ENV_OUTPUT")
    if not admin_dsn or not output:
        raise SystemExit("disposable Telegram PostgreSQL test settings are required")
    info = conninfo_to_dict(admin_dsn)
    if (info.get("host") != "127.0.0.1" or info.get("dbname", "postgres") != "postgres"
            or info.get("port") != "55433"):
        raise SystemExit("Telegram PostgreSQL setup only permits the isolated loopback service")

    safe_admin_dsn = make_conninfo(admin_dsn, hostaddr="127.0.0.1")
    admin = psycopg.connect(safe_admin_dsn, autocommit=True)
    try:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname='knowledge'").fetchone():
            raise SystemExit("refusing to reuse an existing knowledge database")
        roles = admin.execute(
            "SELECT rolname FROM pg_roles WHERE rolname IN "
            "('knowledge_owner','knowledge_app','knowledge_reader')"
        ).fetchall()
        if roles:
            raise SystemExit("refusing to reuse pre-existing knowledge roles")
        admin.execute("CREATE DATABASE knowledge")
    finally:
        admin.close()

    dsn = make_conninfo(safe_admin_dsn, dbname="knowledge")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(BASELINE.read_text(encoding="utf-8"))
    result = run_migrations(dsn)
    if result.get("status") not in {"ready", "current", "applied"} or result.get("pending") != []:
        raise SystemExit("disposable Telegram PostgreSQL migrations did not complete")

    password = secrets.token_urlsafe(32)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("ALTER ROLE knowledge_app WITH LOGIN PASSWORD {}").format(sql.Literal(password))
        )
    path = Path(output)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(
                "KNOWLEDGE_HOST=127.0.0.1\nKNOWLEDGE_PORT=55433\n"
                "KNOWLEDGE_DB=knowledge\nKNOWLEDGE_USER=knowledge_app\n"
                f"KNOWLEDGE_PASSWORD={password}\n"
            )
        os.chmod(path, 0o600)
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


if __name__ == "__main__":
    main()
