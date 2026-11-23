from __future__ import annotations

from pathlib import Path


def test_external_discussions_migration_records_its_version_idempotently() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"
    sql = migration.read_text(encoding="utf-8")
    assert "ALTER TABLE documents" in sql
    assert "ADD COLUMN IF NOT EXISTS external_ref text" in sql
    assert "uq_documents_source_external_ref" in sql
    assert "ADD COLUMN IF NOT EXISTS thread_ref text" in sql
    assert "ADD COLUMN IF NOT EXISTS parent_external_ref text" in sql
    assert "ADD COLUMN IF NOT EXISTS item_kind text" in sql
    assert (
        "INSERT INTO schema_migrations(version) VALUES ('0006_external_discussions')"
        in sql
    )
    assert "ON CONFLICT DO NOTHING;" in sql


def test_external_discussions_migration_does_not_touch_telegram_or_asr() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"
    sql = migration.read_text(encoding="utf-8")
    assert "telegram_service_events" not in sql
    assert "asr_runs" not in sql
    assert "asr_segments" not in sql


def test_external_discussions_migration_grants_are_read_write_scoped() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"
    sql = migration.read_text(encoding="utf-8")
    assert "GRANT SELECT, INSERT, UPDATE ON documents, segments TO knowledge_app;" in sql
    assert "GRANT SELECT, INSERT, UPDATE ON snapshots TO knowledge_app;" in sql
    assert "GRANT SELECT ON documents, segments TO knowledge_reader;" in sql


def test_base_schema_fixture_includes_external_columns() -> None:
    fixture = Path(__file__).parents[1] / "tests" / "fixtures" / "knowledge_base_test_schema.sql"
    sql = fixture.read_text(encoding="utf-8")
    assert "external_ref text" in sql
    assert "thread_ref text" in sql
    assert "parent_external_ref text" in sql
    assert "item_kind text" in sql
    assert "uq_documents_source_external_ref" in sql
    assert "'0006_external_discussions'" in sql
