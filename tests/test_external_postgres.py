"""Database tests for external discussion ingest (ТЗ §13.3).

Runs only against an isolated disposable database configured through
KIBER_ANALYST_INGEST_TEST_ENV; the schema must exist there (base fixture plus
migrations). Never point these tests at production data.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
TEST_ENV = os.environ.get("KIBER_ANALYST_INGEST_TEST_ENV")
pytestmark = [
    pytest.mark.skipif(
        not TEST_ENV or not Path(TEST_ENV).is_file(),
        reason="set KIBER_ANALYST_INGEST_TEST_ENV to an isolated disposable test database env file",
    ),
    pytest.mark.usefixtures("disposable_postgres_credentials"),
]

MIGRATION = Path(__file__).parents[1] / "migrations" / "0006_external_discussions.sql"


def _connect(credentials: dict[str, str], *, autocommit: bool = True):
    from conftest import connect_validated_postgres

    return connect_validated_postgres(
        credentials,
        psycopg.connect,
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


def test_same_repository_in_two_projects_stays_isolated(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_load import load_external
    from kiber_analyst.search_core import execute_search

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/shared-{token}"
    project_a = f"kiber-a-{uuid.uuid4().hex[:10]}"
    project_b = f"kiber-b-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")

    job_dir = tmp_path / "shared"
    _bundle(job_dir, source_id, "github", threads, items)
    first = load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_a)
    second = load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_b)
    assert (first["inserted"], second["inserted"]) == (2, 2)

    with _connect(credentials) as conn:
        rows = conn.execute(
            """SELECT p.slug, count(*) FROM segments s
               JOIN documents d ON d.id=s.document_id
               JOIN sources so ON so.id=d.source_id
               JOIN projects p ON p.id=so.project_id
               WHERE s.external_ref LIKE %s GROUP BY p.slug ORDER BY p.slug""",
            (f"{source_id}:%",),
        ).fetchall()
        assert rows == [(project_a, 2), (project_b, 2)]
        # Every segment resolves to exactly one project-owned document.
        cross = conn.execute(
            """SELECT count(*) FROM segments s
               JOIN documents d ON d.id=s.document_id
               JOIN sources so ON so.id=d.source_id
               JOIN projects p ON p.id=so.project_id
               WHERE s.external_ref LIKE %s AND p.slug NOT IN (%s, %s)""",
            (f"{source_id}:%", project_a, project_b),
        ).fetchone()[0]
        assert cross == 0

        # Search isolation: both projects must answer their own rows only.
        # tsv is a stored generated column, so plain load_external already
        # fills it: no manual updates here.
        null_tsv = conn.execute(
            """SELECT count(*) FROM segments s
               WHERE s.external_ref LIKE %s AND s.tsv IS NULL""",
            (f"{source_id}:%",),
        ).fetchone()[0]
        assert null_tsv == 0
        for slug in (project_a, project_b):
            result = execute_search(
                conn, "connector", project=slug, vector=None,
                source_id=source_id, limit=10,
            )
            assert result["results"], "each project must find its own segments"
            assert all(row["project_slug"] == slug for row in result["results"])
            assert all(row["source_id"] == source_id for row in result["results"])


def test_seq_reuse_deleted_reappear_and_prior_run_reconciliation(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"gitverse:owner/seq-{token}"
    project_slug = f"kiber-seq-{uuid.uuid4().hex[:10]}"
    thread_ref = f"{source_id}:issue:1"

    def bundle(job_dir: Path, comments: list[tuple[str, str]]) -> None:
        threads = [{
            "thread_ref": thread_ref, "provider": "gitverse", "kind": "issue", "number": 1,
            "title": "Seq", "state": "open", "author": "alice",
            "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://gitverse.ru/owner/repo/issues/1", "labels": [],
            "meta": {"events": []},
        }]
        items = [{
            "external_ref": f"{thread_ref}:body", "thread_ref": thread_ref,
            "item_kind": "body", "parent_external_ref": None, "author": "alice",
            "text": "Body", "posted_at": "2026-11-01T10:00:00Z",
            "url": "https://gitverse.ru/owner/repo/issues/1",
            "content_hash": hashlib.sha256(b"Body").hexdigest(), "meta": {},
        }]
        for ref, text in comments:
            items.append({
                "external_ref": f"{thread_ref}:comment:{ref}", "thread_ref": thread_ref,
                "item_kind": "comment", "parent_external_ref": None, "author": "bob",
                "text": text, "posted_at": "2026-11-02T11:00:00Z",
                "url": f"https://gitverse.ru/owner/repo/issues/1#issuecomment-{ref}",
                "content_hash": hashlib.sha256(text.encode()).hexdigest(), "meta": {},
            })
        _bundle(job_dir, source_id, "gitverse", threads, items)

    # Bundle 1: body + c1 + c2. Bundle 2 drops c1 and adds c3, which must reuse
    # no stale sequence value of a skipped segment.
    bundle(tmp_path / "one", [("c1", "First"), ("c2", "Second")])
    load_external(tmp_path / "one", env_path=Path(TEST_ENV), project_slug=project_slug)
    bundle(tmp_path / "two", [("c2", "Second"), ("c3", "Third")])
    second = load_external(tmp_path / "two", env_path=Path(TEST_ENV), project_slug=project_slug)
    assert second["deleted"] == 1  # c1 is gone from a complete fetch

    # Bundle 3 brings c1 back unchanged: the deletion marker must clear.
    bundle(tmp_path / "three", [("c1", "First"), ("c2", "Second"), ("c3", "Third")])
    third = load_external(tmp_path / "three", env_path=Path(TEST_ENV), project_slug=project_slug)
    assert third["updated"] == 1

    # A complete re-fetch with an identical bundle still reconciles deletions:
    # plant a segment that is absent from the bundle, then reload bundle 3.
    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        document_id = conn.execute(
            """SELECT d.id FROM documents d JOIN sources so ON so.id=d.source_id
               WHERE so.project_id=%s AND d.external_ref=%s""",
            (project_id, thread_ref),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO segments (document_id, seq, text, external_ref, content_hash,
                                     meta, thread_ref, item_kind)
               VALUES (%s, 99, 'planted', %s, %s, '{}', %s, 'comment')""",
            (document_id, f"{thread_ref}:comment:phantom",
             hashlib.sha256(b"planted").hexdigest(), thread_ref),
        )
        conn.execute(
            "UPDATE segments SET meta='{}'::jsonb WHERE external_ref=%s",
            (f"{thread_ref}:comment:phantom",),
        )
    fourth = load_external(tmp_path / "three", env_path=Path(TEST_ENV), project_slug=project_slug)
    assert fourth["deleted"] >= 1
    with _connect(credentials) as conn:
        meta = conn.execute(
            "SELECT meta FROM segments WHERE external_ref=%s",
            (f"{thread_ref}:comment:phantom",),
        ).fetchone()[0]
        assert meta.get("deleted") is True
        seqs = conn.execute(
            """SELECT s.seq, s.external_ref FROM segments s
               JOIN documents d ON d.id=s.document_id
               WHERE d.external_ref=%s ORDER BY s.seq""",
            (thread_ref,),
        ).fetchall()
        assert len({row[0] for row in seqs}) == len(seqs), "sequence values must stay unique"


def test_metadata_update_keeps_edit_provenance(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/prov-{token}"
    project_slug = f"kiber-prov-{uuid.uuid4().hex[:10]}"
    thread_ref = f"{source_id}:issue:1"

    def bundle(job_dir: Path, text: str, author: str) -> None:
        threads = [{
            "thread_ref": thread_ref, "provider": "github", "kind": "issue", "number": 1,
            "title": "Prov", "state": "open", "author": "alice",
            "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://github.com/owner/repo/issues/1", "labels": [], "meta": {},
        }]
        items = [
            {
                "external_ref": f"{thread_ref}:body", "thread_ref": thread_ref,
                "item_kind": "body", "parent_external_ref": None, "author": "alice",
                "text": "Body", "posted_at": "2026-11-01T10:00:00Z",
                "url": "https://github.com/owner/repo/issues/1",
                "content_hash": hashlib.sha256(b"Body").hexdigest(), "meta": {},
            },
            {
                "external_ref": f"{thread_ref}:comment:1", "thread_ref": thread_ref,
                "item_kind": "comment", "parent_external_ref": None, "author": author,
                "text": text, "posted_at": "2026-11-02T11:00:00Z",
                "url": "https://github.com/owner/repo/issues/1#issuecomment-1",
                "content_hash": hashlib.sha256(text.encode()).hexdigest(), "meta": {},
            },
        ]
        _bundle(job_dir, source_id, "github", threads, items)

    bundle(tmp_path / "v1", "Original", "bob")
    load_external(tmp_path / "v1", env_path=Path(TEST_ENV), project_slug=project_slug)
    bundle(tmp_path / "v2", "Edited", "bob")
    load_external(tmp_path / "v2", env_path=Path(TEST_ENV), project_slug=project_slug)
    bundle(tmp_path / "v3", "Edited", "carol")
    load_external(tmp_path / "v3", env_path=Path(TEST_ENV), project_slug=project_slug)

    with _connect(credentials) as conn:
        meta = conn.execute(
            "SELECT meta, content_hash FROM segments WHERE external_ref=%s",
            (f"{thread_ref}:comment:1",),
        ).fetchone()[0:2]
        meta_value = meta[0]
        assert meta_value.get("revision") == 1
        assert meta_value.get("previous_content_hash") == hashlib.sha256(b"Original").hexdigest()
        assert "edited_at" in meta_value
        assert meta[1] == hashlib.sha256(b"Edited").hexdigest()


def test_source_urls_point_at_the_provider_repository(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    project_slug = f"kiber-url-{uuid.uuid4().hex[:10]}"
    cases = {
        "github": (f"github:owner/repo-{token}", f"https://github.com/owner/repo-{token}"),
        "gitverse": (f"gitverse:owner/repo-{token}", f"https://gitverse.ru/owner/repo-{token}"),
        "gitlab": (
            f"gitlab:gitlab.com/group/project-{token}",
            f"https://gitlab.com/group/project-{token}",
        ),
    }
    for provider, (source_id, expected_url) in cases.items():
        threads, items = _sample_bundle(source_id, provider)
        job_dir = tmp_path / f"url-{provider}"
        _bundle(job_dir, source_id, provider, threads, items)
        load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)
        with _connect(credentials) as conn:
            url = conn.execute(
                """SELECT so.url FROM sources so JOIN projects p ON p.id=so.project_id
                   WHERE p.slug=%s AND so.kind=%s AND so.external_id=%s""",
                (project_slug, provider, source_id.split(":", 1)[1]),
            ).fetchone()[0]
        assert url == expected_url


def test_embed_limit_applies_after_the_resume_cursor(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/limit-{token}"
    project_slug = f"kiber-limit-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")
    job_dir = tmp_path / "limit"
    _bundle(job_dir, source_id, "github", threads, items)
    load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)

    def fake_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        return [[0.01] * 1536 for _ in texts], None

    first = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding, limit=1,
    )
    second = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding, limit=1,
    )
    assert (first["inserted"], second["inserted"]) == (1, 1)
    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        assert conn.execute(
            "SELECT count(*) FROM embeddings WHERE project_id=%s", (project_id,)
        ).fetchone()[0] == 2


def test_external_embedding_lock_covers_provider_call_and_prevents_duplicate_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dotenv import dotenv_values

    import kiber_analyst.external_embed as embed_module
    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/lock-{token}"
    project_slug = f"kiber-lock-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")
    job_dir = tmp_path / "locked-embed"
    _bundle(job_dir, source_id, "github", threads, items)
    load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)

    first_entered = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_passed_lock = threading.Event()
    second_provider_calls = []
    errors: list[BaseException] = []

    original_get_or_create_run = embed_module._get_or_create_run

    def observe_lock(cursor, project_id, source_id):
        if threading.current_thread().name == "second-embedding":
            second_passed_lock.set()
        return original_get_or_create_run(cursor, project_id, source_id)

    monkeypatch.setattr(embed_module, "_get_or_create_run", observe_lock)

    def vectors(texts: list[str]) -> tuple[list[list[float]], int | None]:
        return [[0.04] * 1536 for _ in texts], None

    def first_embedding(texts: list[str]):
        first_entered.set()
        if not release_first.wait(10):
            raise TimeoutError("test did not release first embedding")
        return vectors(texts)

    def second_embedding(texts: list[str]):
        second_provider_calls.append(len(texts))
        return vectors(texts)

    def run_embedding(callback) -> None:
        if threading.current_thread().name == "second-embedding":
            second_started.set()
        try:
            embed_external(
                source_id, project=project_slug, env_path=Path(TEST_ENV),
                embedding_call=callback,
            )
        except BaseException as exc:  # captured for assertion in the test thread
            errors.append(exc)

    first = threading.Thread(target=run_embedding, args=(first_embedding,))
    second = threading.Thread(
        target=run_embedding, args=(second_embedding,), name="second-embedding"
    )
    first.start()
    try:
        assert first_entered.wait(10)
        second.start()
        assert second_started.wait(10)
        assert not second_passed_lock.wait(0.3)
    finally:
        release_first.set()
    first.join(10)
    second.join(10)
    assert not first.is_alive() and not second.is_alive()
    assert second_passed_lock.is_set()
    assert second_provider_calls == []
    assert errors == []

    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        assert conn.execute(
            "SELECT count(*) FROM embeddings WHERE project_id=%s", (project_id,)
        ).fetchone()[0] == len(items)


def test_embed_skips_tab_only_text_and_changed_segments(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/guard-{token}"
    project_slug = f"kiber-guard-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")
    job_dir = tmp_path / "guard"
    _bundle(job_dir, source_id, "github", threads, items)
    load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)

    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        document_id = conn.execute(
            """SELECT d.id FROM documents d JOIN sources so ON so.id=d.source_id
               WHERE so.project_id=%s AND d.external_ref LIKE %s""",
            (project_id, f"{source_id}:%"),
        ).fetchone()[0]
        thread_ref = conn.execute(
            "SELECT external_ref FROM documents WHERE id=%s", (document_id,)
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO segments (document_id, seq, text, external_ref, content_hash,
                                     meta, thread_ref, item_kind)
               VALUES (%s, 10, E'\\t', %s, %s, '{}', %s, 'comment')""",
            (document_id, f"{thread_ref}:comment:tab",
             hashlib.sha256(b"\t").hexdigest(), thread_ref),
        )

    def racing_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        # Simulate a discussion refresh that changes the text during the call.
        with _connect(credentials) as conn:
            conn.execute(
                "UPDATE segments SET text='changed', content_hash=%s"
                " WHERE external_ref LIKE %s AND text='Please fix the connector'",
                (hashlib.sha256(b"changed").hexdigest(), f"{source_id}:%"),
            )
        return [[0.02] * 1536 for _ in texts], None

    report = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=racing_embedding,
    )
    assert report["inserted"] == 1  # only the unchanged comment is embedded
    assert report["skipped_changed"] == 1
    with _connect(credentials) as conn:
        tab = conn.execute(
            "SELECT id FROM segments WHERE external_ref LIKE %s AND text=E'\\t'",
            (f"{source_id}:%",),
        ).fetchone()
        assert tab is not None
        assert conn.execute(
            """SELECT count(*) FROM embeddings WHERE project_id=%s AND owner_id=%s""",
            (project_id, str(tab[0])),
        ).fetchone()[0] == 0


def test_partial_ingest_loads_good_threads_with_the_real_loader(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.connectors.base import (
        NormalizedThread,
        RawItem,
        RawThread,
        ThreadRef,
    )
    from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
    from kiber_analyst.connectors.normalize import item_record, thread_record
    from kiber_analyst.external_ingest import run_external_ingest

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/partial-{token}"
    project_slug = f"kiber-partial-{uuid.uuid4().hex[:10]}"

    class PartialConnector:
        provider = "github"
        discovery_errors: list[dict[str, str]] = []

        def validate_source(self, source_id: str) -> None:
            return None

        def discover(self, source_id: str, checkpoint: dict | None = None):
            for number in (1, 2):
                yield ThreadRef(
                    provider="github", source_id=source_id,
                    thread_ref=f"{source_id}:issue:{number}", kind="issue",
                    number=number, updated_at="2026-11-10T10:00:00Z",
                    state="open", title=f"Issue {number}",
                )

        def fetch_thread(self, ref: ThreadRef) -> RawThread:
            if ref.number == 2:
                raise ConnectorError(
                    ConnectorCodes.THREAD_NOT_FOUND, "thread disappeared", status=404
                )
            body = {
                "number": 1, "title": "Issue 1", "state": "open",
                "user": {"login": "alice"},
                "created_at": "2026-11-01T10:00:00Z",
                "updated_at": "2026-11-10T10:00:00Z",
                "html_url": "https://github.com/owner/repo/issues/1",
                "body": "Good body", "labels": [],
            }
            return RawThread(
                ref=ref, body=body,
                items=[RawItem(item_kind="body", external_id=None, data=body)],
            )

        def normalize(self, raw: RawThread) -> NormalizedThread:
            body = raw.body or {}
            thread = thread_record(
                thread_ref_value=raw.ref.thread_ref,
                title=body.get("title") or "",
                state=body.get("state"),
                author="alice",
                created_at=body.get("created_at"),
                updated_at=body.get("updated_at"),
                url=body.get("html_url"),
                labels=[],
                meta={},
            )
            item = item_record(
                thread_ref_value=raw.ref.thread_ref,
                item_kind="body",
                external_id=None,
                author="alice",
                text=body.get("body") or "",
                posted_at=body.get("created_at"),
                url=body.get("html_url"),
                meta={},
            )
            return NormalizedThread(thread=thread, items=[item])

    result = run_external_ingest(
        provider="github", source_id=source_id, project_slug=project_slug,
        since="2026-11-01", artifact_dir=tmp_path / "partial",
        env_path=Path(TEST_ENV), connector=PartialConnector(),
    )
    assert result["status"] == "partial"
    assert result["threads"] == 1
    assert result["load"]["inserted"] == 1
    assert result["checkpoint_after"]["since"] == "2026-11-01T00:00:00Z"
    assert result["checkpoint_after"]["watermark_suppressed"] == "partial"

    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        assert conn.execute(
            """SELECT count(*) FROM segments s JOIN documents d ON d.id=s.document_id
               JOIN sources so ON so.id=d.source_id
               WHERE so.project_id=%s AND s.thread_ref LIKE %s""",
            (project_id, f"{source_id}:%"),
        ).fetchone()[0] == 1
        run = conn.execute(
            """SELECT status, checkpoint->>'since' FROM runs
               WHERE project_id=%s AND kind='external_ingest'
               ORDER BY started_at DESC LIMIT 1""",
            (project_id,),
        ).fetchone()
        assert run[0] == "done"
        assert run[1] == "2026-11-01T00:00:00Z"
    from kiber_analyst.external_load import latest_checkpoint_since

    # A partial run must not move the checkpoint read by the next import.
    assert latest_checkpoint_since(
        project_slug, source_id, env_path=Path(TEST_ENV)
    ) == "2026-11-01T00:00:00Z"


def test_embedding_refreshes_only_when_the_text_changes(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/embed-{token}"
    project_slug = f"kiber-embed-{uuid.uuid4().hex[:10]}"
    thread_ref = f"{source_id}:issue:1"

    def bundle(job_dir: Path, text: str, author: str) -> None:
        threads = [{
            "thread_ref": thread_ref, "provider": "github", "kind": "issue", "number": 1,
            "title": "Embed", "state": "open", "author": "alice",
            "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://github.com/owner/repo/issues/1", "labels": [], "meta": {},
        }]
        items = [
            {
                "external_ref": f"{thread_ref}:body", "thread_ref": thread_ref,
                "item_kind": "body", "parent_external_ref": None, "author": "alice",
                "text": "Body", "posted_at": "2026-11-01T10:00:00Z",
                "url": "https://github.com/owner/repo/issues/1",
                "content_hash": hashlib.sha256(b"Body").hexdigest(), "meta": {},
            },
            {
                "external_ref": f"{thread_ref}:comment:1", "thread_ref": thread_ref,
                "item_kind": "comment", "parent_external_ref": None, "author": author,
                "text": text, "posted_at": "2026-11-02T11:00:00Z",
                "url": "https://github.com/owner/repo/issues/1#issuecomment-1",
                "content_hash": hashlib.sha256(text.encode()).hexdigest(), "meta": {},
            },
        ]
        _bundle(job_dir, source_id, "github", threads, items)

    def fake_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        vectors = []
        for text in texts:
            digest = hashlib.sha256(text.encode()).digest() * 48
            vectors.append([float(byte % 7) for byte in digest[:1536]])
        return vectors, None

    bundle(tmp_path / "e1", "Original", "bob")
    load_external(tmp_path / "e1", env_path=Path(TEST_ENV), project_slug=project_slug)
    first = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    assert (first["inserted"], first["updated_vectors"]) == (2, 0)

    bundle(tmp_path / "e2", "Edited", "bob")
    load_external(tmp_path / "e2", env_path=Path(TEST_ENV), project_slug=project_slug)
    edited = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    assert (edited["inserted"], edited["updated_vectors"], edited["skipped_existing"]) == (0, 1, 1)

    bundle(tmp_path / "e3", "Edited", "carol")  # metadata-only change
    load_external(tmp_path / "e3", env_path=Path(TEST_ENV), project_slug=project_slug)
    metadata_only = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    assert (
        metadata_only["inserted"],
        metadata_only["updated_vectors"],
        metadata_only["skipped_existing"],
    ) == (0, 0, 2)
    with _connect(credentials) as conn:
        stored = conn.execute(
            """SELECT e.content_hash FROM embeddings e
               JOIN segments s ON s.id::text=e.owner_id
               WHERE s.external_ref=%s""",
            (f"{thread_ref}:comment:1",),
        ).fetchone()[0]
    assert stored == hashlib.sha256(b"Edited").hexdigest()


def test_reimporting_an_older_snapshot_restores_its_text(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external
    from kiber_analyst.search_core import execute_search

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/aba-{token}"
    project_slug = f"kiber-aba-{uuid.uuid4().hex[:10]}"
    thread_ref = f"{source_id}:issue:1"

    def bundle(job_dir: Path, text: str) -> None:
        threads = [{
            "thread_ref": thread_ref, "provider": "github", "kind": "issue", "number": 1,
            "title": "ABA", "state": "open", "author": "alice",
            "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://github.com/owner/repo/issues/1", "labels": [], "meta": {},
        }]
        items = [{
            "external_ref": f"{thread_ref}:comment:1", "thread_ref": thread_ref,
            "item_kind": "comment", "parent_external_ref": None, "author": "bob",
            "text": text, "posted_at": "2026-11-02T11:00:00Z",
            "url": "https://github.com/owner/repo/issues/1#issuecomment-1",
            "content_hash": hashlib.sha256(text.encode()).hexdigest(), "meta": {},
        }]
        _bundle(job_dir, source_id, "github", threads, items)

    def fake_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        return [[0.03] * 1536 for _ in texts], None

    bundle(tmp_path / "a1", "Version A")
    load_external(tmp_path / "a1", env_path=Path(TEST_ENV), project_slug=project_slug)
    embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    bundle(tmp_path / "b1", "Version B")
    load_external(tmp_path / "b1", env_path=Path(TEST_ENV), project_slug=project_slug)
    embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    # Re-import the previously seen snapshot: the database must return to A,
    # and the embedding must be refreshed for the restored text.
    restored = load_external(
        tmp_path / "a1", env_path=Path(TEST_ENV), project_slug=project_slug
    )
    assert restored["updated"] >= 1
    refreshed = embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    assert refreshed["updated_vectors"] == 1
    with _connect(credentials) as conn:
        row = conn.execute(
            "SELECT text, content_hash FROM segments WHERE external_ref=%s",
            (f"{thread_ref}:comment:1",),
        ).fetchone()
        assert row[0] == "Version A"
        assert row[1] == hashlib.sha256(b"Version A").hexdigest()
        # The refreshed vector must now rank the restored text semantically.
        vector_a, _ = fake_embedding(["Version A"])
        semantic = execute_search(
            conn, "zzzznomatch", project=project_slug, vector=vector_a[0],
            source_id=source_id, limit=10,
        )
        assert any(
            item["item_ref"] == f"{thread_ref}:comment:1"
            for item in semantic["results"]
        )


def test_external_import_order_uses_timestamp_assigned_after_source_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dotenv import dotenv_values

    import kiber_analyst.external_load as load_module
    from kiber_analyst.external_load import load_external

    _credentials = {
        key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value
    }
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/order-{token}"
    project_slug = f"kiber-order-{uuid.uuid4().hex[:10]}"

    def make_bundle(path: Path, text: str) -> int:
        threads, items = _sample_bundle(source_id, "github")
        items[0]["text"] = text
        items[0]["content_hash"] = hashlib.sha256(text.encode()).hexdigest()
        _bundle(path, source_id, "github", threads, items)
        return len(items)

    bundle_a = tmp_path / "order-a"
    bundle_b = tmp_path / "order-b"
    item_count = make_bundle(bundle_a, "Snapshot A")
    make_bundle(bundle_b, "Snapshot B")
    load_external(bundle_a, env_path=Path(TEST_ENV), project_slug=project_slug)

    original_project_id = load_module._project_id
    transaction_started = threading.Event()
    allow_source_lock = threading.Event()
    thread_errors: list[BaseException] = []

    def controlled_project_id(cursor, slug):
        project_id = original_project_id(cursor, slug)
        if threading.current_thread().name == "older-transaction":
            transaction_started.set()
            if not allow_source_lock.wait(10):
                raise TimeoutError("test did not release older transaction")
        return project_id

    monkeypatch.setattr(load_module, "_project_id", controlled_project_id)

    def load_older_snapshot() -> None:
        try:
            load_external(bundle_a, env_path=Path(TEST_ENV), project_slug=project_slug)
        except BaseException as exc:  # captured for assertion in the test thread
            thread_errors.append(exc)

    first = threading.Thread(target=load_older_snapshot, name="older-transaction")
    first.start()
    try:
        assert transaction_started.wait(10)
        load_external(bundle_b, env_path=Path(TEST_ENV), project_slug=project_slug)
    finally:
        allow_source_lock.set()
    first.join(10)
    assert not first.is_alive()
    assert thread_errors == []

    runs_before_repeat = None
    repeated = load_external(bundle_a, env_path=Path(TEST_ENV), project_slug=project_slug)
    with _connect(_credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        runs_before_repeat = conn.execute(
            "SELECT count(*) FROM runs WHERE project_id=%s AND kind='external_ingest'",
            (project_id,),
        ).fetchone()[0]
        latest_key = conn.execute(
            """SELECT checkpoint->>'import_key' FROM runs
               WHERE project_id=%s AND kind='external_ingest' AND status='done'
               ORDER BY started_at DESC, id DESC LIMIT 1""",
            (project_id,),
        ).fetchone()[0]
        assert conn.execute(
            """SELECT text FROM segments WHERE external_ref LIKE %s ORDER BY id LIMIT 1""",
            (f"{source_id}:%",),
        ).fetchone()[0] == "Snapshot A"
        assert latest_key == json.loads(
            (bundle_a / "normalized" / "manifest.json").read_text()
        )["external_import_key"]
    assert repeated["skipped"] == item_count
    assert runs_before_repeat == 3


def test_saved_checkpoint_is_read_for_the_next_import(tmp_path: Path) -> None:
    from kiber_analyst.external_load import latest_checkpoint_since, load_external

    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/cp-{token}"
    project_slug = f"kiber-cp-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")
    job_dir = tmp_path / "cp"
    _bundle(job_dir, source_id, "github", threads, items)
    assert latest_checkpoint_since(project_slug, source_id, env_path=Path(TEST_ENV)) is None

    load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)
    saved = latest_checkpoint_since(project_slug, source_id, env_path=Path(TEST_ENV))
    # _bundle stores this checkpoint in the manifest and the loader persists it.
    assert saved == "2026-11-22T23:55:00Z"

    # Another source in the same project must not borrow this checkpoint.
    other = f"github:owner/cp-other-{token}"
    assert latest_checkpoint_since(project_slug, other, env_path=Path(TEST_ENV)) is None


def test_stale_vectors_are_excluded_from_semantic_ranking(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.external_embed import embed_external
    from kiber_analyst.external_load import load_external
    from kiber_analyst.search_core import execute_search

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/stale-{token}"
    project_slug = f"kiber-stale-{uuid.uuid4().hex[:10]}"
    thread_ref = f"{source_id}:issue:1"

    def bundle(job_dir: Path, edited_text: str) -> None:
        threads = [{
            "thread_ref": thread_ref, "provider": "github", "kind": "issue", "number": 1,
            "title": "Stale", "state": "open", "author": "alice",
            "created_at": "2026-11-01T10:00:00Z", "updated_at": "2026-11-02T10:00:00Z",
            "url": "https://github.com/owner/repo/issues/1", "labels": [], "meta": {},
        }]
        items = [
            {
                "external_ref": f"{thread_ref}:comment:1", "thread_ref": thread_ref,
                "item_kind": "comment", "parent_external_ref": None, "author": "bob",
                "text": edited_text, "posted_at": "2026-11-02T11:00:00Z",
                "url": "https://github.com/owner/repo/issues/1#issuecomment-1",
                "content_hash": hashlib.sha256(edited_text.encode()).hexdigest(),
                "meta": {},
            },
            {
                "external_ref": f"{thread_ref}:comment:2", "thread_ref": thread_ref,
                "item_kind": "comment", "parent_external_ref": None, "author": "carol",
                "text": "Unchanged comment", "posted_at": "2026-11-02T12:00:00Z",
                "url": "https://github.com/owner/repo/issues/1#issuecomment-2",
                "content_hash": hashlib.sha256(b"Unchanged comment").hexdigest(),
                "meta": {},
            },
        ]
        _bundle(job_dir, source_id, "github", threads, items)

    def fake_embedding(texts: list[str]) -> tuple[list[list[float]], int | None]:
        vectors = []
        for text in texts:
            digest = hashlib.sha256(text.encode()).digest() * 48
            vectors.append([float(byte % 7) for byte in digest[:1536]])
        return vectors, None

    bundle(tmp_path / "old", "Version A")
    load_external(tmp_path / "old", env_path=Path(TEST_ENV), project_slug=project_slug)
    embed_external(
        source_id, project=project_slug, env_path=Path(TEST_ENV),
        embedding_call=fake_embedding,
    )
    # Edit the text and do NOT re-embed: the stored vector is now stale.
    bundle(tmp_path / "new", "Version B")
    load_external(tmp_path / "new", env_path=Path(TEST_ENV), project_slug=project_slug)

    with _connect(credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,)
        ).fetchone()[0]
        null_tsv = conn.execute(
            """SELECT count(*) FROM segments s
               JOIN documents d ON d.id=s.document_id
               JOIN sources so ON so.id=d.source_id
               WHERE so.project_id=%s AND s.tsv IS NULL""",
            (project_id,),
        ).fetchone()[0]
        assert null_tsv == 0

    query_vector, _ = fake_embedding(["Version B"])
    with _connect(credentials) as conn:
        semantic_only = execute_search(
            conn, "zzzznomatch", project=project_slug, vector=query_vector[0],
            source_id=source_id, limit=10,
        )
        refs = {row["item_ref"] for row in semantic_only["results"]}
        assert f"{thread_ref}:comment:1" not in refs, "stale vector must not rank"
        assert f"{thread_ref}:comment:2" in refs

        # Full-text search still finds the new text without any re-embed:
        # tsv is generated from the stored text by the database itself.
        fts = execute_search(
            conn, "Version", project=project_slug, vector=None,
            source_id=source_id, limit=10,
        )
        assert any(
            row["item_ref"] == f"{thread_ref}:comment:1" for row in fts["results"]
        )


def test_telegram_semantic_ranking_is_unchanged(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.search_core import execute_search

    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    project_slug = f"kiber-tgsem-{uuid.uuid4().hex[:10]}"
    with _connect(credentials) as conn:
        project_id = conn.execute(
            "INSERT INTO projects (slug,title) VALUES (%s,'TgSem') RETURNING id",
            (project_slug,),
        ).fetchone()[0]
        source_id = conn.execute(
            "INSERT INTO sources (project_id,kind,external_id,meta)"
            " VALUES (%s,'telegram','-100555','{}') RETURNING id",
            (project_id,),
        ).fetchone()[0]
        document_id = conn.execute(
            "INSERT INTO documents (source_id,doc_type,meta)"
            " VALUES (%s,'export','{\"topic_id\":\"7\",\"topic_title\":\"Topic\"}')"
            " RETURNING id",
            (source_id,),
        ).fetchone()[0]
        # A per-run message id keeps the test repeatable: Telegram external
        # refs stay globally unique.
        message_ref = f"tg:-100555:{uuid.uuid4().hex[:10]}"
        segment_id = conn.execute(
            """INSERT INTO segments (document_id,seq,text,external_ref,content_hash,
                                     meta,posted_at)
               VALUES (%s,1,'telegram semantic text',%s,%s,'{}',
                       '2026-11-01T10:00:00Z') RETURNING id""",
            (document_id, message_ref,
             hashlib.sha256(b"telegram semantic text").hexdigest()),
        ).fetchone()[0]
        # Telegram embeddings carry no content_hash; semantic ranking must keep
        # working exactly as before the external-item freshness check.
        conn.execute(
            """INSERT INTO embeddings
                 (project_id,owner_table,owner_id,model,embedding,content_hash)
               VALUES (%s,'segments',%s,'gemini-embedding-2:1536',%s::vector,NULL)""",
            (project_id, str(segment_id), json.dumps([0.05] * 1536)),
        )
        result = execute_search(
            conn, "zzzznomatch", project=project_slug, vector=[0.05] * 1536,
            limit=10,
        )
    assert result["results"], "Telegram semantic ranking must still work"
    row = result["results"][0]
    assert row["source_kind"] == "telegram"
    assert row["message_id"] == message_ref.split(":")[2]


def test_tsv_migration_converts_plain_column_and_is_idempotent(tmp_path: Path) -> None:
    """Apply 0007 for real to an old-schema database (plain segments.tsv).

    Requires the disposable database to be reachable and the file pointed to by
    KIBER_ANALYST_INGEST_TEST_ENV to also contain TEST_OWNER_PASSWORD for the
    database owner (the migration rewrites the table, so the application role
    cannot run it).
    """
    from conftest import validate_disposable_postgres_credentials
    from dotenv import dotenv_values

    from kiber_analyst.external_load import load_external

    values = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    credentials = validate_disposable_postgres_credentials(values)
    owner_password = values.get("TEST_OWNER_PASSWORD")
    if not owner_password:
        pytest.skip("TEST_OWNER_PASSWORD is required to apply migration 0007")

    def owner_connect(autocommit: bool = True):
        return psycopg.connect(
            host=credentials["KNOWLEDGE_HOST"],
            port=int(credentials["KNOWLEDGE_PORT"]),
            dbname=credentials["KNOWLEDGE_DB"],
            user="postgres",
            password=owner_password,
            autocommit=autocommit,
        )

    migration_sql = (
        Path(__file__).parents[1] / "migrations" / "0007_segments_tsv_generated.sql"
    ).read_text(encoding="utf-8")

    def tsv_state(conn) -> tuple[bool, bool]:
        row = conn.execute(
            """SELECT attgenerated = 's' AS generated,
                      attgenerated IS NULL OR attgenerated = '' AS plain
               FROM pg_attribute
               WHERE attrelid='segments'::regclass AND attname='tsv'
                 AND NOT attisdropped"""
        ).fetchone()
        assert row is not None, "segments.tsv must exist"
        return bool(row[0]), bool(row[1])

    with owner_connect() as conn:
        # Force the old mechanism: a plain, manually filled column.
        conn.execute("DROP INDEX IF EXISTS idx_segments_tsv")
        conn.execute("ALTER TABLE segments DROP COLUMN IF EXISTS tsv")
        conn.execute("ALTER TABLE segments ADD COLUMN tsv tsvector")
        conn.execute("DROP TRIGGER IF EXISTS kiber_migration_guard ON segments")
        conn.execute("DROP FUNCTION IF EXISTS kiber_migration_guard_fn()")
        generated, plain = tsv_state(conn)
        assert (generated, plain) == (False, True)

        # An existing row that predates the migration must get its tsv back.
        # Own project/source/document keeps this test repeatable.
        token = uuid.uuid4().hex[:10]
        project_id = conn.execute(
            "INSERT INTO projects (slug, title) VALUES (%s, 'TsvOld') RETURNING id",
            (f"kiber-tsvold-{token}",),
        ).fetchone()[0]
        source_id = conn.execute(
            "INSERT INTO sources (project_id, kind, external_id, meta)"
            " VALUES (%s, 'telegram', '-100777', '{}') RETURNING id",
            (project_id,),
        ).fetchone()[0]
        document_id = conn.execute(
            'INSERT INTO documents (source_id, doc_type, meta)'
            " VALUES (%s, 'export', '{\"topic_id\": \"9\"}') RETURNING id",
            (source_id,),
        ).fetchone()[0]
        existing = conn.execute(
            """INSERT INTO segments (document_id, seq, text, external_ref)
               VALUES (%s, 1, 'pre-migration text', %s) RETURNING id""",
            (document_id, f"tg:-100777:{token}"),
        ).fetchone()

    # Apply 0007 for real, then again: idempotent both times.
    with owner_connect() as conn:
        conn.execute(migration_sql)
        conn.execute(migration_sql)
        generated, plain = tsv_state(conn)
        assert (generated, plain) == (True, False)
        generated_expression = conn.execute(
            """SELECT pg_get_expr(adbin, adrelid)
               FROM pg_attrdef
               WHERE adrelid='segments'::regclass
                 AND adnum=(SELECT attnum FROM pg_attribute
                            WHERE attrelid='segments'::regclass
                              AND attname='tsv' AND NOT attisdropped)"""
        ).fetchone()[0]
        normalized_expression = "".join(generated_expression.lower().split())
        normalized_expression = normalized_expression.replace("''::text", "''")
        assert normalized_expression == (
            "to_tsvector('russian'::regconfig,coalesce(text,''))"
        )
    with owner_connect() as conn:
        versions = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version='0007_segments_tsv_generated'"
        ).fetchone()[0]
        assert versions == 1
        assert conn.execute(
            "SELECT tsv IS NOT NULL FROM segments WHERE id=%s", (existing[0],)
        ).fetchone()[0] is True

    # The standard load path fills tsv through the generated column: no manual
    # UPDATE anywhere in this test.
    token = uuid.uuid4().hex[:10]
    source_id = f"github:owner/tsv-{token}"
    project_slug = f"kiber-tsv-{uuid.uuid4().hex[:10]}"
    threads, items = _sample_bundle(source_id, "github")
    job_dir = tmp_path / "tsv"
    _bundle(job_dir, source_id, "github", threads, items)
    load_external(job_dir, env_path=Path(TEST_ENV), project_slug=project_slug)

    with _connect(credentials) as conn:
        null_tsv = conn.execute(
            """SELECT count(*) FROM segments s
               JOIN documents d ON d.id=s.document_id
               JOIN sources so ON so.id=d.source_id
               WHERE so.project_id=%s AND s.tsv IS NULL""",
            (project_id_of(conn, project_slug),),
        ).fetchone()[0]
        assert null_tsv == 0
        fts = conn.execute(
            """SELECT count(*) FROM segments s
               WHERE s.tsv @@ to_tsquery('russian', 'good')""",
        ).fetchone()[0]
        assert fts >= 1

    # Unsupported state: a foreign trigger must abort the migration and keep
    # the version unrecorded.
    with owner_connect() as conn:
        conn.execute("DROP INDEX IF EXISTS idx_segments_tsv")
        conn.execute("ALTER TABLE segments DROP COLUMN IF EXISTS tsv")
        conn.execute("ALTER TABLE segments ADD COLUMN tsv tsvector")
        conn.execute(
            "CREATE OR REPLACE FUNCTION kiber_migration_guard_fn() RETURNS trigger"
            " LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
        )
        conn.execute(
            "CREATE TRIGGER kiber_migration_guard BEFORE INSERT ON segments"
            " FOR EACH STATEMENT EXECUTE FUNCTION kiber_migration_guard_fn()"
        )
    with owner_connect() as conn:
        with pytest.raises(Exception, match="custom trigger"):
            conn.execute(migration_sql)
        versions = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version='0007_segments_tsv_generated'"
        ).fetchone()[0]
        assert versions == 1, "the unsupported state must not record the version"
    # Restore the supported state for any later tests.
    with owner_connect() as conn:
        conn.execute("DROP TRIGGER IF EXISTS kiber_migration_guard ON segments")
        conn.execute("DROP FUNCTION IF EXISTS kiber_migration_guard_fn()")
        conn.execute(migration_sql)


def project_id_of(conn, project_slug: str) -> str:
    return conn.execute(
        "SELECT id FROM projects WHERE slug=%s", (project_slug,)
    ).fetchone()[0]
