from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
ADMIN_DSN = os.environ.get("KIBER_ANALYST_MIGRATION_TEST_ADMIN_DSN")
pytestmark = [
    pytest.mark.skipif(
        not ADMIN_DSN,
        reason="set KIBER_ANALYST_MIGRATION_TEST_ADMIN_DSN for disposable PostgreSQL",
    ),
    pytest.mark.usefixtures("loopback_postgres_driver"),
]

ROOT = Path(__file__).parents[1]
BASELINE_SQL = ROOT / "tests/fixtures/migration_runner_baseline.sql"


def _gitlab_bundle(job_dir: Path) -> None:
    source_id = "gitlab:gitlab.com/teknokomo/universo"
    thread_ref = f"{source_id}:issue:1"
    threads = [
        {
            "thread_ref": thread_ref,
            "provider": "gitlab",
            "kind": "issue",
            "number": 1,
            "title": "Synthetic smoke issue",
            "state": "open",
            "author": "synthetic-author",
            "created_at": "2026-11-01T10:00:00Z",
            "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://gitlab.com/teknokomo/universo/-/issues/1",
            "labels": [],
            "meta": {"events": [], "milestone": None, "assignees": []},
        }
    ]
    text = "Synthetic GitLab thread body for migration acceptance."
    items = [
        {
            "external_ref": f"{thread_ref}:body",
            "thread_ref": thread_ref,
            "item_kind": "body",
            "parent_external_ref": None,
            "author": "synthetic-author",
            "text": text,
            "posted_at": "2026-11-01T10:00:00Z",
            "url": "https://gitlab.com/teknokomo/universo/-/issues/1",
            "content_hash": hashlib.sha256(text.encode()).hexdigest(),
            "meta": {"provider": "gitlab", "iid": 1},
        }
    ]
    normalized = job_dir / "normalized"
    normalized.mkdir(parents=True)
    (normalized / "threads.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in threads),
        encoding="utf-8",
    )
    (normalized / "items.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in items),
        encoding="utf-8",
    )
    snapshot_sha256 = hashlib.sha256(
        json.dumps({"threads": threads, "items": items}, sort_keys=True).encode()
    ).hexdigest()
    import_key = hashlib.sha256(
        json.dumps(
            {"source_id": source_id, "snapshot_sha256": snapshot_sha256},
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "source_id": source_id,
        "provider": "gitlab",
        "api_version": "test",
        "snapshot_sha256": snapshot_sha256,
        "external_import_key": import_key,
        "archive_uri": f"external://gitlab/{snapshot_sha256}",
        "archive_size_bytes": 1,
        "thread_count": len(threads),
        "item_count": len(items),
        "nonempty_count": 1,
        "skipped_empty_count": 0,
        "fetched_at": "2026-11-02T10:00:00Z",
        "checkpoint_before": {"since": "2026-10-01T00:00:00Z"},
        "checkpoint_after": {"since": "2026-11-02T10:00:00Z", "threads_fetched": 1},
        "threads_complete": {thread_ref: True},
        "limit_applied": 1,
        "errors": [],
        "partial_reasons": [],
        "project_slug": "kiber-gitlab-smoke",
        "pipeline_version": "test",
    }
    (normalized / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _table_counts(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
            return {
                table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("projects", "sources", "snapshots", "documents", "segments", "runs")
            }


def test_0008_upgrades_production_checks_and_loads_gitlab_bundle(
    tmp_path: Path,
) -> None:
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from kiber_analyst.errors import LoadError
    from kiber_analyst.external_load import load_external
    from kiber_analyst.migrate import run_migrations

    assert ADMIN_DSN is not None
    admin_info = conninfo_to_dict(ADMIN_DSN)
    assert admin_info.get("host") in {"127.0.0.1", "localhost", "::1"}
    assert admin_info.get("dbname", "postgres") == "postgres"
    assert admin_info.get("port", "5432") == "5433", (
        "the disposable service must be mapped to loopback port 5433"
    )

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
        created_roles = True

        admin.execute("CREATE DATABASE knowledge")
        created_database = True
        dsn = make_conninfo(ADMIN_DSN, dbname="knowledge")
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(BASELINE_SQL.read_text(encoding="utf-8"))

        through_0007 = run_migrations(dsn, target="0007_segments_tsv_generated")
        assert through_0007["newly_applied"] == [
            "0002_telegram",
            "0003_asr",
            "0004_mcp_reader",
            "0005_telegram_services",
            "0006_external_discussions",
            "0007_segments_tsv_generated",
        ]
        pending = run_migrations(dsn, dry_run=True)
        assert pending["pending"] == ["0008_external_check_constraints"]

        password = secrets.token_urlsafe(24)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER ROLE knowledge_app WITH LOGIN PASSWORD {}").format(
                    sql.Literal(password)
                )
            )

        config_path = tmp_path / "knowledge.conf"
        config_path.write_text(
            "\n".join(
                (
                    "KNOWLEDGE_HOST=127.0.0.1",
                    "KNOWLEDGE_PORT=5433",
                    "KNOWLEDGE_DB=knowledge",
                    "KNOWLEDGE_USER=knowledge_app",
                    f"KNOWLEDGE_PASSWORD={password}",
                    "",
                )
            ),
            encoding="utf-8",
        )
        config_path.chmod(0o600)
        job_dir = tmp_path / "gitlab-bundle"
        _gitlab_bundle(job_dir)
        project_slug = f"kiber-gitlab-smoke-{uuid.uuid4().hex[:8]}"

        # The production-like pre-0008 CHECK rejects the provider at sources.
        before_failed_dry_run = _table_counts(dsn)
        with pytest.raises(
            LoadError, match=r"CheckViolation \(SQLSTATE 23514\)"
        ) as failure:
            load_external(
                job_dir,
                env_path=config_path,
                project_slug=project_slug,
                dry_run=True,
            )
        assert isinstance(failure.value.__cause__, psycopg.errors.CheckViolation)
        assert failure.value.__cause__.diag.constraint_name == "sources_kind_check"
        assert _table_counts(dsn) == before_failed_dry_run

        applied = run_migrations(dsn, target="0008_external_check_constraints")
        assert applied["newly_applied"] == ["0008_external_check_constraints"]
        with psycopg.connect(dsn) as conn:
            checks = dict(
                conn.execute(
                    "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint"
                    " WHERE contype='c' AND conrelid IN ('sources'::regclass,'documents'::regclass,'runs'::regclass)"
                ).fetchall()
            )
            source_check = checks["sources_kind_check"].lower()
            document_check = checks["documents_doc_type_check"].lower()
            assert all(value in source_check for value in ("github", "gitlab", "gitverse"))
            assert "thread" in document_check
            assert "external_ingest" not in source_check + document_check
            assert checks["runs_status_check"].lower() == (
                "check ((status = any (array['queued'::text, 'running'::text, "
                "'done'::text, 'failed'::text, 'partial'::text])))"
            )

        before_successful_dry_run = _table_counts(dsn)
        report = load_external(
            job_dir,
            env_path=config_path,
            project_slug=project_slug,
            dry_run=True,
        )
        assert report["status"] == "dry_run"
        assert report["rollback_confirmed"] is True
        assert report["threads"] == 1
        assert report["items"] == 1
        assert report["inserted"] == 1
        assert _table_counts(dsn) == before_successful_dry_run
        assert run_migrations(dsn)["status"] == "current"
    finally:
        if created_database:
            admin.execute("DROP DATABASE IF EXISTS knowledge WITH (FORCE)")
        if created_roles:
            for role in ("knowledge_reader", "knowledge_app", "knowledge_owner"):
                admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        admin.close()
