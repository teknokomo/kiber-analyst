from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from kiber_analyst.migrate import (
    BASELINE_VERSION,
    MigrationError,
    _planned_versions,
    load_catalog,
)

ROOT = Path(__file__).parents[1]
MIGRATIONS = ROOT / "migrations"


def test_catalog_checksums_and_self_registration() -> None:
    catalog = load_catalog(MIGRATIONS)
    assert catalog.versions == (
        BASELINE_VERSION,
        "0002_telegram",
        "0003_asr",
        "0004_mcp_reader",
        "0005_telegram_services",
        "0006_external_discussions",
        "0007_segments_tsv_generated",
        "0008_external_check_constraints",
    )
    assert all(migration.sql for migration in catalog.migrations)


def test_catalog_rejects_changed_sql(tmp_path: Path) -> None:
    copied = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS, copied)
    migration = copied / "0002_telegram.sql"
    migration.write_text(migration.read_text(encoding="utf-8") + "\n-- changed\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="CHECKSUM_MISMATCH"):
        load_catalog(copied)


def test_catalog_rejects_migration_without_self_registration(tmp_path: Path) -> None:
    copied = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS, copied)
    migration = copied / "0002_telegram.sql"
    sql = migration.read_text(encoding="utf-8")
    sql = sql.replace(
        "INSERT INTO schema_migrations(version) VALUES ('0002_telegram')\nON CONFLICT DO NOTHING;",
        "-- registration removed for test",
    )
    migration.write_text(sql, encoding="utf-8")
    manifest_path = copied / "checksums.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["migrations"][0]["sha256"] = hashlib.sha256(migration.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(MigrationError, match="SELF_REGISTRATION_INVALID"):
        load_catalog(copied)


def test_plan_requires_a_contiguous_ledger_and_forward_target() -> None:
    catalog = load_catalog(MIGRATIONS)
    assert _planned_versions(catalog, (BASELINE_VERSION,), "0003_asr") == (
        "0002_telegram",
        "0003_asr",
    )
    with pytest.raises(MigrationError, match="LEDGER_GAP_OR_ORDER"):
        _planned_versions(catalog, (BASELINE_VERSION, "0003_asr"), None)
    with pytest.raises(MigrationError, match="TARGET_BEHIND_LEDGER"):
        _planned_versions(catalog, catalog.versions[:4], "0003_asr")


def test_runner_source_never_writes_the_migration_ledger() -> None:
    source = (ROOT / "src/kiber_analyst/migrate.py").read_text(encoding="utf-8")
    assert "INSERT INTO schema_migrations" not in source
    assert "pg_try_advisory_lock" in source
