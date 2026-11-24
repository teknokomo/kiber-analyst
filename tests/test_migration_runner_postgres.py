from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest

from kiber_analyst.migrate import (
    BASELINE_VERSION,
    LOCK_KEY,
    MigrationError,
    load_catalog,
    run_migrations,
)

ROOT = Path(__file__).parents[1]
BASELINE_SQL = ROOT / "tests/fixtures/migration_runner_baseline.sql"
MIGRATIONS = ROOT / "migrations"
ADMIN_DSN = os.environ.get("KIBER_ANALYST_MIGRATION_TEST_ADMIN_DSN")
pytestmark = pytest.mark.skipif(
    not ADMIN_DSN,
    reason="set KIBER_ANALYST_MIGRATION_TEST_ADMIN_DSN for an isolated disposable PostgreSQL service",
)


def test_runner_dry_run_target_full_lock_and_atomic_rollback(tmp_path: Path) -> None:
    psycopg = pytest.importorskip("psycopg")
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    assert ADMIN_DSN is not None
    admin_info = conninfo_to_dict(ADMIN_DSN)
    assert admin_info.get("host") in {"127.0.0.1", "localhost", "::1"}, (
        "migration integration tests only accept a loopback PostgreSQL host"
    )
    assert admin_info.get("dbname", "postgres") == "postgres"

    admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    created_database = False
    created_roles = False
    try:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname='knowledge'").fetchone():
            pytest.fail("refusing to use an existing database named knowledge")
        existing_roles = admin.execute(
            "SELECT rolname FROM pg_roles WHERE rolname IN "
            "('knowledge_owner','knowledge_app','knowledge_reader')"
        ).fetchall()
        if existing_roles:
            pytest.fail("refusing to use pre-existing knowledge roles")

        admin.execute("CREATE DATABASE knowledge")
        created_database = True
        created_roles = True
        dsn = make_conninfo(ADMIN_DSN, dbname="knowledge")
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(BASELINE_SQL.read_text(encoding="utf-8"))
            assert conn.execute("SELECT version FROM schema_migrations").fetchall() == [
                (BASELINE_VERSION,)
            ]

        dry = run_migrations(dsn, dry_run=True)
        assert dry["status"] == "dry_run"
        assert dry["pending"] == list(load_catalog().versions[1:])
        assert dry["checksums_verified"]["0006_external_discussions"] == (
            "a6150d72bd154b95119ba2c4ab175bf0ab6a392a9898c8cbaf206b5ff616befa"
        )
        with psycopg.connect(dsn) as conn:
            assert conn.execute(
                "SELECT to_regclass('public.snapshots'), to_regclass('public.asr_runs')"
            ).fetchone() == (None, None)
            assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 1

        targeted = run_migrations(dsn, target="0003_asr")
        assert targeted["newly_applied"] == ["0002_telegram", "0003_asr"]
        with pytest.raises(MigrationError, match="TARGET_BEHIND_LEDGER"):
            run_migrations(dsn, target="0002_telegram")

        through_services = run_migrations(dsn, target="0005_telegram_services")
        assert through_services["newly_applied"] == [
            "0004_mcp_reader",
            "0005_telegram_services",
        ]
        completed = run_migrations(dsn)
        assert completed["newly_applied"] == [
            "0006_external_discussions",
            "0007_segments_tsv_generated",
        ]
        with psycopg.connect(dsn) as conn:
            ledger = [
                row[0]
                for row in conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
            ]
            assert ledger == list(load_catalog().versions)
            assert (
                conn.execute(
                    "SELECT attgenerated::text FROM pg_attribute "
                    "WHERE attrelid='segments'::regclass AND attname='tsv'"
                ).fetchone()[0]
                == "s"
            )
            expression = conn.execute(
                "SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef "
                "WHERE adrelid='segments'::regclass AND adnum=("
                "SELECT attnum FROM pg_attribute WHERE attrelid='segments'::regclass "
                "AND attname='tsv')"
            ).fetchone()[0]
            assert "coalesce" in expression.lower()
            assert (
                conn.execute("SELECT indexdef FROM pg_indexes WHERE indexname='idx_segments_tsv'")
                .fetchone()[0]
                .lower()
                .find("using gin (tsv)")
                >= 0
            )

        assert run_migrations(dsn)["status"] == "current"

        held_lock = psycopg.connect(dsn, autocommit=True)
        assert held_lock.execute("SELECT pg_try_advisory_lock(%s, %s)", LOCK_KEY).fetchone()[0]
        try:
            with pytest.raises(MigrationError, match="MIGRATION_LOCK_BUSY"):
                run_migrations(dsn, dry_run=True)
        finally:
            held_lock.execute("SELECT pg_advisory_unlock(%s, %s)", LOCK_KEY)
            held_lock.close()

        # A temporary forward migration fails after DDL but before registration;
        # its own transaction must remove both the DDL and any ledger update.
        rollback_catalog = tmp_path / "rollback-migrations"
        shutil.copytree(MIGRATIONS, rollback_catalog)
        rollback_sql = (
            "CREATE TABLE migration_rollback_probe (id integer);\n"
            "SELECT 1 / 0;\n"
            "INSERT INTO schema_migrations(version) VALUES ('0008_rollback_probe');\n"
        )
        (rollback_catalog / "0008_rollback_probe.sql").write_text(rollback_sql, encoding="utf-8")
        manifest_path = rollback_catalog / "checksums.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["migrations"].append(
            {
                "version": "0008_rollback_probe",
                "file": "0008_rollback_probe.sql",
                "sha256": hashlib.sha256(rollback_sql.encode()).hexdigest(),
            }
        )
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        with pytest.raises(MigrationError, match="MIGRATION_FAILED") as error:
            run_migrations(dsn, target="0008_rollback_probe", migrations_dir=rollback_catalog)
        assert error.value.sqlstate == "22012"
        with psycopg.connect(dsn) as conn:
            assert (
                conn.execute("SELECT to_regclass('public.migration_rollback_probe')").fetchone()[0]
                is None
            )
            assert (
                conn.execute(
                    "SELECT count(*) FROM schema_migrations WHERE version='0008_rollback_probe'"
                ).fetchone()[0]
                == 0
            )
    finally:
        if created_database:
            admin.execute("DROP DATABASE IF EXISTS knowledge WITH (FORCE)")
        if created_roles:
            for role in ("knowledge_reader", "knowledge_app", "knowledge_owner"):
                admin.execute(f'DROP ROLE IF EXISTS "{role}"')
        admin.close()
