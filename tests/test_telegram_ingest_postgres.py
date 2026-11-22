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


def _write_private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _fixture(root: Path) -> Path:
    chat_id = "-10012345"
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


def test_telegram_import_load_and_repeat_are_idempotent_in_test_database(tmp_path: Path) -> None:
    from dotenv import dotenv_values

    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_slug = f"kiber-test-{uuid.uuid4().hex[:12]}"
    credentials = {key: str(value) for key, value in dotenv_values(TEST_ENV).items() if value}
    with psycopg.connect(
        host=credentials["KNOWLEDGE_HOST"],
        port=int(credentials["KNOWLEDGE_PORT"]),
        dbname=credentials["KNOWLEDGE_DB"],
        user=credentials["KNOWLEDGE_USER"],
        password=credentials["KNOWLEDGE_PASSWORD"],
        autocommit=True,
    ) as conn:
        assert conn.execute(
            """SELECT count(*) FROM sources
               WHERE external_id = %s AND project_id IN
                 (SELECT id FROM projects WHERE slug = %s)""",
            ("-10012345", project_slug),
        ).fetchone()[0] == 0

    export_root = tmp_path / "telegram"
    source = _fixture(export_root)
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
    with psycopg.connect(
        host=credentials["KNOWLEDGE_HOST"],
        port=int(credentials["KNOWLEDGE_PORT"]),
        dbname=credentials["KNOWLEDGE_DB"],
        user=credentials["KNOWLEDGE_USER"],
        password=credentials["KNOWLEDGE_PASSWORD"],
    ) as conn:
        project_id = conn.execute("SELECT id FROM projects WHERE slug=%s", (project_slug,)).fetchone()[0]
        source_id = conn.execute(
            "SELECT id FROM sources WHERE project_id=%s AND external_id=%s",
            (project_id, "-10012345"),
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
