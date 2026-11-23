"""Database tests for external discussion ingest (ТЗ §13.3).

Runs only against an isolated disposable database configured through
KIBER_ANALYST_INGEST_TEST_ENV; the schema must exist there (base fixture plus
migrations). Never point these tests at production data.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
TEST_ENV = os.environ.get("KIBER_ANALYST_INGEST_TEST_ENV")
pytestmark = pytest.mark.skipif(
    not TEST_ENV or not Path(TEST_ENV).is_file(),
    reason="set KIBER_ANALYST_INGEST_TEST_ENV to an isolated disposable test database env file",
)

MIGRATION = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"


def _connect(credentials: dict[str, str], *, autocommit: bool = True):
    return psycopg.connect(
        host=credentials["KNOWLEDGE_HOST"],
        port=int(credentials["KNOWLEDGE_PORT"]),
        dbname=credentials["KNOWLEDGE_DB"],
        user=credentials["KNOWLEDGE_USER"],
        password=credentials["KNOWLEDGE_PASSWORD"],
        autocommit=autocommit,
    )


def _thread_ref(source_id: str, kind: str, number: int) -> str:
    return f"{source_id}:{kind}:{number}"


def _bundle(job_dir: Path, source_id: str, provider: str, threads: list[dict], items: list[dict]) -> None:
    normalized = job_dir / "normalized"
    normalized.mkdir(parents=True, exist_ok=True)
    with (normalized / "threads.jsonl").open("w", encoding="utf-8") as stream:
        for thread in threads:
            stream.write(json.dumps(thread, ensure_ascii=False, sort_keys=True) + "\n")
    with (normalized / "items.jsonl").open("w", encoding="utf-8") as stream:
        for item in items:
            stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
    snapshot = hashlib.sha256(
        json.dumps({"threads": threads, "items": items}, sort_keys=True).encode()
    ).hexdigest()
    manifest = {
        "source_id": source_id,
        "provider": provider,
        "api_version": "test",
        "snapshot_sha256": snapshot,
        "external_import_key": hashlib.sha256(
            json.dumps({"source_id": source_id, "snapshot_sha256": snapshot},
                       sort_keys=True).encode()
        ).hexdigest(),
        "archive_uri": f"external://{source_id}/{snapshot}",
        "archive_size_bytes": 1,
        "thread_count": len(threads),
        "item_count": len(items),
        "nonempty_count": sum(1 for item in items if item["text"].strip()),
        "skipped_empty_count": 0,
        "fetched_at": "2026-11-23T00:00:00Z",
        "checkpoint_before": {"since": "2026-08-25T00:00:00Z"},
        "checkpoint_after": {"since": "2026-11-22T23:55:00Z", "threads_fetched": len(threads)},
        "threads_complete": {thread["thread_ref"]: True for thread in threads},
        "limit_applied": None,
        "errors": [],
        "partial_reasons": [],
        "project_slug": "unused",
        "pipeline_version": "test",
    }
    (normalized / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _sample_bundle(source_id: str, provider: str) -> tuple[list[dict], list[dict]]:
    thread = _thread_ref(source_id, "issue", 1)
    threads = [{
        "thread_ref": thread, "provider": provider, "kind": "issue", "number": 1,
        "title": "Sample thread", "state": "open", "author": "alice",
        "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
        "url": f"https://example.com/{provider}/issues/1", "labels": ["bug"],
        "meta": {"events": [], "milestone": None, "assignees": []},
    }]
    items = [
        {
            "external_ref": f"{thread}:body", "thread_ref": thread,
            "item_kind": "body", "parent_external_ref": None, "author": "alice",
            "text": "Please fix the connector", "posted_at": "2026-11-01T10:00:00Z",
            "url": f"https://example.com/{provider}/issues/1",
            "content_hash": hashlib.sha256(b"Please fix the connector").hexdigest(),
            "meta": {"provider": provider, "message_id": "1"},
        },
        {
            "external_ref": f"{thread}:comment:101", "thread_ref": thread,
            "item_kind": "comment", "parent_external_ref": None, "author": "bob",
            "text": "On it", "posted_at": "2026-11-02T11:00:00Z",
            "url": f"https://example.com/{provider}/issues/1#note_101",
            "content_hash": hashlib.sha256(b"On it").hexdigest(),
            "meta": {"provider": provider, "message_id": "101"},
        },
    ]
    return threads, items


def test_external_load_embed_and_idempotency(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    project_slug = f"kiber-ext-{uuid.uuid4().hex[:12]}"

    with _connect(credentials) as conn:
        # Migration 0006 is idempotent; applying it here may lack owner rights
        # on an operator-built database, which is acceptable when the objects
        # already exist.
        applied = True
        try:
            conn.execute(MIGRATION.read_text(encoding="utf-8"))
            conn.execute(MIGRATION.read_text(encoding="utf-8"))
        except psycopg.errors.InsufficientPrivilege:
            applied = False
        versions = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version='0006_external_discussions'"
        ).fetchone()[0]
        assert versions == 1
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name='segments'"
            ).fetchall()
        }
        assert {"thread_ref", "parent_external_ref", "item_kind"} <= columns
        doc_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name='documents'"
            ).fetchall()
        }
        assert "external_ref" in doc_columns

        # A Telegram row that must remain untouched by everything below.
        telegram_project = conn.execute(
            "INSERT INTO projects (slug, title) VALUES (%s, %s) RETURNING id",
            (f"kiber-tg-{uuid.uuid4().hex[:12]}", "Telegram Guard"),
        ).fetchone()[0]
        telegram_source = conn.execute(
            "INSERT INTO sources (project_id, kind, external_id, meta)"
            " VALUES (%s, 'telegram', '-100999', '{}') RETURNING id",
            (telegram_project,),
        ).fetchone()[0]
        telegram_doc = conn.execute(
            "INSERT INTO documents (source_id, doc_type, meta)"
            " VALUES (%s, 'export', '{\"topic_id\": \"7\"}') RETURNING id",
            (telegram_source,),
        ).fetchone()[0]
        guard_ref = f"tg:-100999:{uuid.uuid4().hex[:12]}"
        telegram_segment = conn.execute(
            "INSERT INTO segments (document_id, seq, text, external_ref, meta)"
            " VALUES (%s, 1, 'telegram text', %s, '{}') RETURNING id",
            (telegram_doc, guard_ref),
        ).fetchone()[0]

    _ = applied  # documented above
    # Random repository suffix keeps external_refs globally unique, so the test
    # can be re-run against the same disposable database.
    token = uuid.uuid4().hex[:10]
    sources = {
        "github": f"github:owner/repo-{token}",
        "gitlab": f"gitlab:gitlab.com/group/project-{token}",
        "gitverse": f"gitverse:owner/repo-{token}",
    }
    results = {}
    for provider, source_id in sources.items():
        threads, items = _sample_bundle(source_id, provider)
        job_dir = tmp_path / provider
        _bundle(job_dir, source_id, provider, threads, items)
        first = load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)
        second = load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)
        results[provider] = (first, second)

    def fake_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        vectors = []
        for text in texts:
            # 1536 deterministic finite components (32-byte digest repeated).
            digest = hashlib.sha256(text.encode()).digest() * 48
            vectors.append([float(byte % 7) for byte in digest[:1536]])
        return vectors, None

    embed_first = embed_external(
        sources["github"], project=project_slug,
        env_path=Path(TEST_ENV), embedding_call=fake_embedding,
    )
    embed_second = embed_external(
        sources["github"], project=project_slug,
        env_path=Path(TEST_ENV), embedding_call=fake_embedding,
    )

    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        for provider, (first, second) in results.items():
            assert first["status"] == "done" and first["inserted"] == 2
            assert second["status"] == "done"
            assert second["inserted"] == 0 and second["skipped"] == 2
            source_id_value = conn.execute(
                "SELECT id FROM sources WHERE project_id=%s AND kind=%s AND external_id=%s",
                (project_id, provider, sources[provider].split(":", 1)[1]),
            ).fetchone()[0]
            assert conn.execute(
                "SELECT count(*) FROM documents WHERE source_id=%s AND doc_type='thread'",
                (source_id_value,),
            ).fetchone()[0] == 1
            assert conn.execute(
                """SELECT count(*) FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                   WHERE doc.source_id=%s""",
                (source_id_value,),
            ).fetchone()[0] == 2
            assert conn.execute(
                """SELECT count(*) FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                   WHERE doc.source_id=%s AND seg.item_kind='comment' AND seg.thread_ref IS NOT NULL""",
                (source_id_value,),
            ).fetchone()[0] == 1
        assert embed_first["inserted"] == 2 and embed_first["skipped_empty"] == 0
        assert embed_second["inserted"] == 0 and embed_second["skipped_existing"] == 2
        assert conn.execute(
            """SELECT count(*) FROM embeddings WHERE project_id=%s AND owner_table='segments'
               AND model='gemini-embedding-2:1536'""",
            (project_id,),
        ).fetchone()[0] == 2
        # Runs are recorded for both pipeline kinds.
        assert conn.execute(
            "SELECT count(*) FROM runs WHERE project_id=%s AND kind='external_ingest' AND status='done'",
            (project_id,),
        ).fetchone()[0] == 3
        # Each embed invocation records its own completed run (a done run is
        # never resumed); the second run re-embeds nothing.
        assert conn.execute(
            "SELECT count(*) FROM runs WHERE project_id=%s AND kind='external_embed' AND status='done'",
            (project_id,),
        ).fetchone()[0] == 2
        # The Telegram row is byte-identical to what we inserted.
        row = conn.execute(
            """SELECT seg.text, seg.external_ref, seg.item_kind, seg.thread_ref
               FROM segments seg WHERE seg.id=%s""",
            (telegram_segment,),
        ).fetchone()
        assert row == ("telegram text", guard_ref, None, None)
        assert conn.execute(
            "SELECT count(*) FROM documents WHERE source_id=%s AND meta->>'topic_id'='7'",
            (telegram_source,),
        ).fetchone()[0] == 1
