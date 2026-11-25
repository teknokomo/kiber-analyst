from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
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


def _write_private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _connect(
    credentials: dict[str, str],
    *,
    autocommit: bool = True,
):
    from conftest import connect_validated_postgres

    return connect_validated_postgres(
        credentials,
        psycopg.connect,
        autocommit=autocommit,
    )


def _fixture(root: Path) -> Path:
    chat_id = f"-100{uuid.uuid4().int % 10**15 + 1}"
    topic_id = 1
    source_id = f"telegram:{chat_id}:topic:{topic_id}"
    topic = root / chat_id / str(topic_id)
    topic.mkdir(mode=0o700, parents=True)
    topic.chmod(0o700)
    message = {
        "chat_id": chat_id,
        "date": "2024-02-03T04:05:06+03:00",
        "message_id": 101,
        "media": None,
        "reply_to_message_id": None,
        "sender_id": 7,
        "sender_name": "Fixture Author",
        "text": "Изолированная проверка импорта",
        "topic_id": topic_id,
    }
    event = {
        "actor_id": "7",
        "actor_name": "Fixture Author",
        "date": "2024-02-03T01:05:06Z",
        "event_id": 100,
        "message_id": None,
        "new_value": "Тема создана",
        "old_value": None,
        "raw_action": "message_action_topic_create",
        "target_id": None,
        "target_name": None,
        "topic_id": topic_id,
        "type": "topic_created",
    }
    messages = _json_bytes(message)
    services = _json_bytes(event)
    _write_private(topic / "messages.jsonl", messages)
    _write_private(topic / "services.jsonl", services)
    _write_private(
        topic / "manifest.json",
        _json_bytes({
            "chat_id": chat_id,
            "complete": True,
            "export_version": 1,
            "messages_sha256": hashlib.sha256(messages).hexdigest(),
            "message_count": 1,
            "scope_before": None,
            "source_id": source_id,
            "topic_id": topic_id,
            "topic_title": "Fixture topic",
        }),
    )
    _write_private(
        topic / "services_manifest.json",
        _json_bytes({
            "chat_id": int(chat_id),
            "complete": True,
            "errors": [],
            "event_count": 1,
            "range_complete": True,
            "scope_before": None,
            "services_sha256": hashlib.sha256(services).hexdigest(),
            "source_id": source_id,
            "topic_id": topic_id,
        }),
    )
    return topic


def test_telegram_import_load_and_repeat_are_idempotent_in_test_database(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_slug = f"kiber-test-{uuid.uuid4().hex[:12]}"
    with _connect(disposable_postgres_credentials, autocommit=True) as conn:
        assert conn.execute(
            """SELECT count(*) FROM sources
               WHERE external_id = %s AND project_id IN
                 (SELECT id FROM projects WHERE slug = %s)""",
            ("-10012345", project_slug),
        ).fetchone()[0] == 0

    export_root = tmp_path / "telegram"
    source = _fixture(export_root)
    chat_id = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["chat_id"]
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source.iterdir()
        if path.is_file()
    }
    artifacts = tmp_path / "artifacts" / "job-1"
    first = load_telegram_export(
        source,
        artifacts,
        env_path=Path(TEST_ENV),
        project_slug=project_slug,
        root=export_root,
    )
    second = load_telegram_export(
        source,
        artifacts,
        env_path=Path(TEST_ENV),
        project_slug=project_slug,
        root=export_root,
    )

    assert (first["inserted"], first["service_events_inserted"], first["errors"]) == (1, 1, 0)
    assert (second["inserted"], second["skipped"], second["service_events_skipped"]) == (0, 1, 1)
    assert {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source.iterdir()
        if path.is_file()
    } == before
    with _connect(disposable_postgres_credentials, autocommit=False) as conn:
        project_id = conn.execute("SELECT id FROM projects WHERE slug=%s", (project_slug,)).fetchone()[0]
        source_id = conn.execute(
            "SELECT id FROM sources WHERE project_id=%s AND external_id=%s",
            (project_id, chat_id),
        ).fetchone()[0]
        assert conn.execute(
            """SELECT count(*) FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               WHERE doc.source_id=%s""",
            (source_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM telegram_service_events WHERE source_id=%s",
            (source_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM runs WHERE project_id=%s AND kind='telegram_ingest' AND status='done'",
            (project_id,),
        ).fetchone()[0] == 1


def test_telegram_topic_cannot_move_between_projects_and_dry_runs_rollback(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    from kiber_analyst.errors import LoadError, SearchError
    from kiber_analyst.search_core import execute_search
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_a = f"kiber-iso-a-{uuid.uuid4().hex[:10]}"
    project_b = f"kiber-iso-b-{uuid.uuid4().hex[:10]}"
    with _connect(disposable_postgres_credentials) as conn:
        conn.execute(
            "INSERT INTO projects (slug, title) VALUES (%s, %s), (%s, %s)",
            (project_a, "Isolation A", project_b, "Isolation B"),
        )

    export_root = tmp_path / "telegram"
    source = _fixture(export_root)
    chat_id = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["chat_id"]
    artifacts = tmp_path / "artifacts" / "shared-job"
    first = load_telegram_export(
        source, artifacts, env_path=Path(TEST_ENV), project_slug=project_a, root=export_root,
    )
    repeated = load_telegram_export(
        source, artifacts, env_path=Path(TEST_ENV), project_slug=project_a, root=export_root,
    )
    assert (first["inserted"], first["service_events_inserted"]) == (1, 1)
    assert (repeated["inserted"], repeated["skipped"], repeated["service_events_skipped"]) == (0, 1, 1)

    def project_counts(conn, slug: str) -> tuple[int, ...]:
        return conn.execute(
            """SELECT
                 (SELECT count(*) FROM sources so JOIN projects p ON p.id=so.project_id WHERE p.slug=%s),
                 (SELECT count(*) FROM documents d JOIN sources so ON so.id=d.source_id
                    JOIN projects p ON p.id=so.project_id WHERE p.slug=%s),
                 (SELECT count(*) FROM segments s JOIN documents d ON d.id=s.document_id
                    JOIN sources so ON so.id=d.source_id JOIN projects p ON p.id=so.project_id
                    WHERE p.slug=%s),
                 (SELECT count(*) FROM snapshots sn JOIN sources so ON so.id=sn.source_id
                    JOIN projects p ON p.id=so.project_id WHERE p.slug=%s),
                 (SELECT count(*) FROM runs r JOIN projects p ON p.id=r.project_id WHERE p.slug=%s),
                 (SELECT count(*) FROM telegram_service_events ev JOIN sources so ON so.id=ev.source_id
                    JOIN projects p ON p.id=so.project_id WHERE p.slug=%s)""",
            (slug, slug, slug, slug, slug, slug),
        ).fetchone()

    with _connect(disposable_postgres_credentials) as conn:
        project_a_before_dry = project_counts(conn, project_a)
    dry = load_telegram_export(
        source, artifacts, env_path=Path(TEST_ENV), project_slug=project_a,
        dry_run=True, root=export_root,
    )
    assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
    with _connect(disposable_postgres_credentials) as conn:
        assert project_counts(conn, project_a) == project_a_before_dry
        project_a_before_cross_project = project_counts(conn, project_a)
        project_b_before = project_counts(conn, project_b)
        project_id = conn.execute("SELECT id FROM projects WHERE slug=%s", (project_a,)).fetchone()[0]
        message_before = conn.execute(
            """SELECT seg.id, seg.document_id, seg.text, seg.content_hash, seg.meta
               FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               JOIN sources so ON so.id=doc.source_id
               WHERE so.project_id=%s AND seg.external_ref=%s""",
            (project_id, f"tg:{chat_id}:101"),
        ).fetchone()
        assert message_before is not None
        assert message_before[2] == "Изолированная проверка импорта"
        search_before = execute_search(
            conn, "изолированная проверка", project=project_a, vector=None,
            limit=5, source_id=f"telegram:{chat_id}:topic:1",
        )
        assert len(search_before["results"]) == 1

    with pytest.raises(LoadError, match="TELEGRAM_PROJECT_CONFLICT"):
        load_telegram_export(
            source, artifacts, env_path=Path(TEST_ENV), project_slug=project_b, root=export_root,
        )
    with pytest.raises(LoadError, match="TELEGRAM_PROJECT_CONFLICT"):
        load_telegram_export(
            source, artifacts, env_path=Path(TEST_ENV), project_slug=project_b,
            dry_run=True, root=export_root,
        )

    with _connect(disposable_postgres_credentials) as conn:
        assert project_counts(conn, project_b) == project_b_before == (0, 0, 0, 0, 0, 0)
        assert project_counts(conn, project_a) == project_a_before_cross_project
        message_after = conn.execute(
            """SELECT seg.id, seg.document_id, seg.text, seg.content_hash, seg.meta
               FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               JOIN sources so ON so.id=doc.source_id
               WHERE so.project_id=%s AND seg.external_ref=%s""",
            (project_id, f"tg:{chat_id}:101"),
        ).fetchone()
        assert message_after == message_before
        assert conn.execute(
            "SELECT count(*) FROM segments WHERE external_ref=%s",
            (f"tg:{chat_id}:101",),
        ).fetchone()[0] == 1
        a_search = execute_search(
            conn, "изолированная проверка", project=project_a, vector=None,
            limit=5, source_id=f"telegram:{chat_id}:topic:1",
        )
        assert a_search == search_before
        b_search = execute_search(
            conn, "изолированная проверка", project=project_b, vector=None, limit=5,
        )
        assert len(a_search["results"]) == 1
        assert a_search["results"][0]["text"] == message_before[2]
        assert b_search["results"] == []
        with pytest.raises(SearchError, match="Telegram source or topic does not exist"):
            execute_search(
                conn, "изолированная проверка", project=project_b, vector=None,
                limit=5, source_id=f"telegram:{chat_id}:topic:1",
            )


def test_concurrent_same_chat_import_keeps_project_a_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    import kiber_analyst.load as load_module
    from kiber_analyst.errors import LoadError
    from kiber_analyst.search_core import execute_search
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_a = f"kiber-race-a-{uuid.uuid4().hex[:10]}"
    project_b = f"kiber-race-b-{uuid.uuid4().hex[:10]}"
    with _connect(disposable_postgres_credentials) as conn:
        conn.execute(
            "INSERT INTO projects (slug, title) VALUES (%s, %s), (%s, %s)",
            (project_a, "Race A", project_b, "Race B"),
        )

    export_root = tmp_path / "telegram-race"
    source = _fixture(export_root)
    chat_id = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["chat_id"]
    load_telegram_export(
        source, tmp_path / "artifacts" / "initial", env_path=Path(TEST_ENV),
        project_slug=project_a, root=export_root,
    )

    def project_snapshot(conn, slug: str) -> tuple[tuple[str, ...], ...]:
        queries = (
            ("projects", "SELECT to_jsonb(p)::text FROM projects p WHERE p.slug=%s ORDER BY p.id"),
            ("sources", "SELECT to_jsonb(s)::text FROM sources s JOIN projects p ON p.id=s.project_id WHERE p.slug=%s ORDER BY s.id"),
            ("snapshots", "SELECT to_jsonb(sn)::text FROM snapshots sn JOIN sources s ON s.id=sn.source_id JOIN projects p ON p.id=s.project_id WHERE p.slug=%s ORDER BY sn.id"),
            ("documents", "SELECT to_jsonb(d)::text FROM documents d JOIN sources s ON s.id=d.source_id JOIN projects p ON p.id=s.project_id WHERE p.slug=%s ORDER BY d.id"),
            ("segments", "SELECT to_jsonb(seg)::text FROM segments seg JOIN documents d ON d.id=seg.document_id JOIN sources s ON s.id=d.source_id JOIN projects p ON p.id=s.project_id WHERE p.slug=%s ORDER BY seg.id"),
            ("runs", "SELECT to_jsonb(r)::text FROM runs r JOIN projects p ON p.id=r.project_id WHERE p.slug=%s ORDER BY r.id"),
            ("service_events", "SELECT to_jsonb(ev)::text FROM telegram_service_events ev JOIN sources s ON s.id=ev.source_id JOIN projects p ON p.id=s.project_id WHERE p.slug=%s ORDER BY ev.id"),
        )
        return tuple(
            tuple(row[0] for row in conn.execute(query, (slug,)).fetchall())
            for _name, query in queries
        )

    with _connect(disposable_postgres_credentials) as conn:
        project_a_before = project_snapshot(conn, project_a)
        project_b_before = project_snapshot(conn, project_b)
        search_before = execute_search(
            conn, "изолированная проверка", project=project_a, vector=None,
            limit=5, source_id=f"telegram:{chat_id}:topic:1",
        )
    assert len(search_before["results"]) == 1

    real_connect = load_module._connect
    connection_barrier = threading.Barrier(3)
    backend_pids: list[int] = []
    pid_lock = threading.Lock()

    def synchronized_connect(config):
        conn = real_connect(config)
        with pid_lock:
            backend_pids.append(conn.info.backend_pid)
        connection_barrier.wait(timeout=20)
        return conn

    monkeypatch.setattr(load_module, "_connect", synchronized_connect)

    def import_for(slug: str):
        try:
            return load_telegram_export(
                source, tmp_path / "artifacts" / slug, env_path=Path(TEST_ENV),
                project_slug=slug, root=export_root,
            )
        except Exception as exc:  # Return exceptions to the coordinator thread.
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        retry_a = pool.submit(import_for, project_a)
        conflict_b = pool.submit(import_for, project_b)
        connection_barrier.wait(timeout=20)
        result_a = retry_a.result(timeout=30)
        result_b = conflict_b.result(timeout=30)

    assert len(set(backend_pids)) == 2
    assert not isinstance(result_a, Exception), repr(result_a)
    assert (result_a["inserted"], result_a["skipped"]) == (0, 1)
    assert isinstance(result_b, LoadError)
    assert "TELEGRAM_PROJECT_CONFLICT" in str(result_b)

    with _connect(disposable_postgres_credentials) as conn:
        assert project_snapshot(conn, project_a) == project_a_before
        assert project_snapshot(conn, project_b) == project_b_before
        search_after = execute_search(
            conn, "изолированная проверка", project=project_a, vector=None,
            limit=5, source_id=f"telegram:{chat_id}:topic:1",
        )
        assert search_after == search_before
