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


def test_external_item_uniqueness_is_scoped_per_source() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"
    sql = migration.read_text(encoding="utf-8")
    # The same repository may live in two projects: external refs are unique
    # per document, while Telegram rows keep their global uniqueness.
    assert "DROP INDEX IF EXISTS uq_segments_external_ref;" in sql
    assert "uq_segments_external_ref_telegram" in sql
    assert "AND item_kind IS NULL" in sql
    assert "uq_segments_document_external_ref" in sql


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


def test_tsv_migration_makes_it_generated_with_explicit_config() -> None:
    migration = (
        Path(__file__).parents[1] / "migrations" / "0007_segments_tsv_generated.sql"
    )
    sql = migration.read_text(encoding="utf-8")
    assert "GENERATED ALWAYS AS (" in sql
    assert "to_tsvector('russian'::regconfig, coalesce(text, ''))" in sql
    assert ") STORED;" in sql
    # The one-argument to_tsvector depends on default_text_search_config and is
    # not immutable, so it must not appear anywhere in the migration.
    assert "to_tsvector(text" not in sql
    assert "INSERT INTO schema_migrations(version)"
    assert "VALUES ('0007_segments_tsv_generated')" in sql
    assert "ON CONFLICT DO NOTHING;" in sql


def test_tsv_migration_checks_the_existing_mechanism_first() -> None:
    migration = (
        Path(__file__).parents[1] / "migrations" / "0007_segments_tsv_generated.sql"
    )
    sql = migration.read_text(encoding="utf-8")
    assert "pg_attribute" in sql and "attgenerated" in sql
    assert "pg_trigger" in sql
    # A foreign trigger must abort the migration (version stays unrecorded)
    # instead of being silently bypassed.
    assert "RAISE EXCEPTION 'segments has % custom trigger(s)" in sql
    # Plain columns (attgenerated IS NULL or '') must be converted, and any
    # other generation kind must abort with an exception.
    assert "IF generated_kind IS NOT NULL AND generated_kind <> ''" in sql
    assert "RAISE EXCEPTION 'segments.tsv generation kind % is not supported'" in sql


def test_base_schema_fixture_declares_generated_tsv() -> None:
    fixture = Path(__file__).parents[1] / "tests" / "fixtures" / "knowledge_base_test_schema.sql"
    normalized = " ".join(fixture.read_text(encoding="utf-8").split()).lower()
    assert (
        "tsv tsvector generated always as ( "
        "to_tsvector('russian'::regconfig, coalesce(text, '')) ) stored,"
        in normalized
    )


def test_external_loader_does_not_fill_tsv_manually() -> None:
    loader = (
        Path(__file__).parents[1] / "src" / "kiber_analyst" / "external_load.py"
    )
    assert "tsv" not in loader.read_text(encoding="utf-8")
