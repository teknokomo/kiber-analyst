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


def _tree_snapshot(root: Path) -> dict[str, tuple[str, int, str | None]]:
    snapshot = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode & 0o777
        if path.is_symlink():
            snapshot[relative] = ("symlink", mode, os.readlink(path))
        elif path.is_dir():
            snapshot[relative] = ("directory", mode, None)
        else:
            snapshot[relative] = (
                "file", mode, hashlib.sha256(path.read_bytes()).hexdigest(),
            )
    return snapshot


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


def _scope_fixture(root: Path, *, chat_id: str, scope: str, topic_id: int | None) -> Path:
    scope_dir = "group" if scope == "chat" else str(topic_id)
    source_id = f"telegram:{chat_id}:group" if scope == "chat" else f"telegram:{chat_id}:topic:{topic_id}"
    directory = root / chat_id / scope_dir
    directory.mkdir(mode=0o700, parents=True)
    directory.chmod(0o700)
    actual_topics = [None] if scope == "topic" else ([None] if topic_id is None else [17, 18])
    messages = [
        {
            "chat_id": chat_id,
            "date": "2026-01-02T03:04:05Z",
            "message_id": 500 + index,
            "media": None,
            "reply_to_message_id": None,
            "sender_id": 77,
            "sender_name": "Scope fixture",
            "text": f"scope fixture {scope} {actual_topic}",
            "topic_id": topic_id if scope == "topic" else actual_topic,
        }
        for index, actual_topic in enumerate(actual_topics)
    ]
    events = [
        {
            "actor_id": "77", "actor_name": "Scope fixture",
            "date": "2026-01-02T03:04:05Z", "event_id": 700 + index,
            "message_id": None, "new_value": "synthetic service event", "old_value": None,
            "raw_action": "synthetic", "target_id": None, "target_name": None,
            "topic_id": topic_id if scope == "topic" else actual_topic, "type": "unknown",
        }
        for index, actual_topic in enumerate(actual_topics)
    ]
    message_bytes = b"".join(_json_bytes(row) for row in messages)
    service_bytes = b"".join(_json_bytes(row) for row in events)
    _write_private(directory / "messages.jsonl", message_bytes)
    _write_private(directory / "services.jsonl", service_bytes)
    _write_private(directory / "manifest.json", _json_bytes({
        "chat_id": chat_id, "chat_title": "Synthetic group", "complete": True,
        "export_version": 1, "max_file_bytes": 104857600,
        "messages_sha256": hashlib.sha256(message_bytes).hexdigest(),
        "message_count": len(messages), "scope": scope, "scope_before": None,
        "source_id": source_id, "topic_id": topic_id if scope == "topic" else None,
        "topic_title": "Synthetic topic" if topic_id is not None else None,
    }))
    _write_private(directory / "services_manifest.json", _json_bytes({
        "chat_id": int(chat_id), "complete": True, "errors": [],
        "event_count": len(events), "range_complete": True, "scope": scope,
        "scope_before": None, "services_sha256": hashlib.sha256(service_bytes).hexdigest(),
        "source_id": source_id, "topic_id": topic_id if scope == "topic" else None,
    }))
    return directory


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


@pytest.mark.parametrize("media_root", ["media", "media-resume"])
def test_repeated_ingest_attaches_verified_recovered_media_from_each_root(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
    media_root: str,
) -> None:
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_slug = f"kiber-recovered-media-{uuid.uuid4().hex[:10]}"
    export_root = tmp_path / "telegram"
    source = _fixture(export_root)
    message_path = source / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"] = {
        "type": "photo", "path": None, "size": None, "size_bytes": None,
        "sha256": None, "max_file_bytes": 104857600,
        "export_status": "skipped", "reason": "unknown_size",
    }
    messages_bytes = _json_bytes(message)
    _write_private(message_path, messages_bytes)
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages_bytes).hexdigest()
    _write_private(manifest_path, _json_bytes(manifest))

    initial = load_telegram_export(
        source, tmp_path / "artifacts" / f"initial-{media_root}",
        env_path=Path(TEST_ENV), project_slug=project_slug, root=export_root,
    )
    assert (initial["inserted"], initial["media_missing"],
            initial["service_events_inserted"]) == (1, 1, 1)

    media_bytes = b"verified resumed telegram attachment"
    media_sha = hashlib.sha256(media_bytes).hexdigest()
    relative_media = f"{media_root}/101/recovered.bin"
    _write_private(source / relative_media, media_bytes)
    (source / media_root).chmod(0o700)
    (source / media_root / "101").chmod(0o700)
    control_hashes = {
        "messages.jsonl": hashlib.sha256(messages_bytes).hexdigest(),
        "services.jsonl": hashlib.sha256((source / "services.jsonl").read_bytes()).hexdigest(),
        "manifest.json": hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest(),
        "services_manifest.json": hashlib.sha256(
            (source / "services_manifest.json").read_bytes()
        ).hexdigest(),
    }
    checkpoint = {
        "version": 2,
        "source_id": manifest["source_id"],
        "inputs": control_hashes,
        "requested_max_file_bytes": 104857600,
        "over_limit_message_ids": [],
        "records": {"101": {
            "path": relative_media, "size": len(media_bytes), "sha256": media_sha,
            "type": "photo", "max_file_bytes": 104857600,
            "export_status": "downloaded",
        }},
    }
    _write_private(
        source / "media-resume" / "status.json",
        _json_bytes(checkpoint),
    )
    (source / "media-resume").chmod(0o700)
    files_before = _tree_snapshot(source)

    with _connect(disposable_postgres_credentials) as conn:
        project_id = conn.execute(
            "SELECT id FROM projects WHERE slug=%s", (project_slug,),
        ).fetchone()[0]
        source_db_id = conn.execute(
            "SELECT id FROM sources WHERE project_id=%s", (project_id,),
        ).fetchone()[0]
        segment = conn.execute(
            """SELECT seg.id, seg.external_ref, seg.text, seg.meta::text
               FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               WHERE doc.source_id=%s""", (source_db_id,),
        ).fetchone()
        segment_before = segment[:3]
        segment_id = segment[0]
        vector = "[" + ",".join(["0"] * 1536) + "]"
        conn.execute(
            """INSERT INTO embeddings
               (project_id, owner_table, owner_id, model, embedding)
               VALUES (%s::uuid, 'segments', %s, 'gemini-embedding-2:1536', %s::vector)""",
            (project_id, str(segment_id), vector),
        )
        embedding_before = conn.execute(
            """SELECT id, owner_id, model, embedding::text FROM embeddings
               WHERE owner_table='segments' AND owner_id=%s""", (str(segment_id),),
        ).fetchone()
        events_before = conn.execute(
            """SELECT external_ref, event_id, topic_id, event_type, payload::text,
                      provenance::text, event_sha256
               FROM telegram_service_events WHERE source_id=%s ORDER BY event_id""",
            (source_db_id,),
        ).fetchall()

    resumed = load_telegram_export(
        source, tmp_path / "artifacts" / f"resumed-{media_root}",
        env_path=Path(TEST_ENV), project_slug=project_slug, root=export_root,
    )
    assert (resumed["inserted"], resumed["updated"], resumed["media_missing"],
            resumed["service_events_skipped"]) == (0, 1, 0, 1)
    normalized = json.loads(
        (tmp_path / "artifacts" / f"resumed-{media_root}" / "normalized" / "messages.jsonl")
        .read_text(encoding="utf-8")
    )
    assert normalized["media_path"] == relative_media
    assert hashlib.sha256((source / normalized["media_path"]).read_bytes()).hexdigest() == media_sha

    with _connect(disposable_postgres_credentials) as conn:
        attached = conn.execute(
            "SELECT meta->>'media_path', meta->'media_export'->>'sha256' FROM segments WHERE id=%s",
            (segment_id,),
        ).fetchone()
        assert attached == (relative_media, media_sha)

    repeated = load_telegram_export(
        source, tmp_path / "artifacts" / f"repeat-{media_root}",
        env_path=Path(TEST_ENV), project_slug=project_slug, root=export_root,
    )
    assert repeated["inserted"] == 0 and repeated["updated"] == 0
    assert repeated["service_events_skipped"] == 1
    assert _tree_snapshot(source) == files_before
    with _connect(disposable_postgres_credentials) as conn:
        assert conn.execute(
            "SELECT id, external_ref, text FROM segments WHERE id=%s",
            (segment_id,),
        ).fetchone() == segment_before
        assert conn.execute(
            "SELECT meta->>'media_path', meta->'media_export'->>'sha256' FROM segments WHERE id=%s",
            (segment_id,),
        ).fetchone() == (relative_media, media_sha)
        assert conn.execute(
            "SELECT count(*) FROM segments WHERE id=%s", (segment_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            """SELECT external_ref, event_id, topic_id, event_type, payload::text,
                      provenance::text, event_sha256
               FROM telegram_service_events WHERE source_id=%s ORDER BY event_id""",
            (source_db_id,),
        ).fetchall() == events_before
        assert conn.execute(
            """SELECT id, owner_id, model, embedding::text FROM embeddings
               WHERE owner_table='segments' AND owner_id=%s""", (str(segment_id),),
        ).fetchone() == embedding_before


def test_group_forum_group_and_topic_scopes_load_with_provenance(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    cases = (
        ("chat", None, f"-100{uuid.uuid4().int % 10**15 + 1}", 1),
        ("chat", 17, f"-100{uuid.uuid4().int % 10**15 + 1}", 2),
        ("topic", 17, f"-100{uuid.uuid4().int % 10**15 + 1}", 1),
    )
    for scope, topic_id, chat_id, expected_count in cases:
        project = f"kiber-scope-{uuid.uuid4().hex[:12]}"
        root = tmp_path / project
        source = _scope_fixture(root, chat_id=chat_id, scope=scope, topic_id=topic_id)
        dry = load_telegram_export(
            source, tmp_path / "artifacts" / f"{project}-dry",
            env_path=Path(TEST_ENV), project_slug=project, dry_run=True, root=root,
        )
        assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
        first = load_telegram_export(
            source, tmp_path / "artifacts" / f"{project}-real",
            env_path=Path(TEST_ENV), project_slug=project, root=root,
        )
        repeated = load_telegram_export(
            source, tmp_path / "artifacts" / f"{project}-repeat",
            env_path=Path(TEST_ENV), project_slug=project, root=root,
        )
        assert first["inserted"] == expected_count
        assert first["service_events_inserted"] == expected_count
        assert repeated["inserted"] == 0 and repeated["service_events_skipped"] == expected_count
        with _connect(disposable_postgres_credentials) as conn:
            rows = conn.execute(
                """SELECT seg.external_ref, seg.meta->>'topic_id', seg.meta->>'export_scope'
                   FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                   JOIN sources src ON src.id=doc.source_id JOIN projects p ON p.id=src.project_id
                   WHERE p.slug=%s ORDER BY seg.external_ref""",
                (project,),
            ).fetchall()
            assert len(rows) == expected_count
            assert all(row[2] == scope for row in rows)
            assert conn.execute(
                """SELECT count(*) FROM telegram_service_events ev
                   JOIN sources src ON src.id=ev.source_id JOIN projects p ON p.id=src.project_id
                   WHERE p.slug=%s""",
                (project,),
            ).fetchone()[0] == expected_count
            if scope == "chat" and topic_id is not None:
                assert {row[1] for row in rows} == {"17", "18"}
            elif scope == "topic":
                assert {row[1] for row in rows} == {str(topic_id)}
            else:
                assert rows[0][1] is None


def test_group_export_refuses_overlapping_topic_in_another_project_without_mutation(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    from kiber_analyst.errors import LoadError
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    chat_id = f"-100{uuid.uuid4().int % 10**15 + 1}"
    project_a = f"kiber-group-owner-{uuid.uuid4().hex[:8]}"
    project_b = f"kiber-topic-target-{uuid.uuid4().hex[:8]}"
    root = tmp_path / "shared-source"
    group_export = _scope_fixture(root, chat_id=chat_id, scope="chat", topic_id=17)
    topic_export = _scope_fixture(root, chat_id=chat_id, scope="topic", topic_id=17)

    first = load_telegram_export(
        group_export, tmp_path / "artifacts" / "group-owner", env_path=Path(TEST_ENV),
        project_slug=project_a, root=root,
    )
    assert first["inserted"] == 2
    with _connect(disposable_postgres_credentials) as conn:
        before = conn.execute(
            """SELECT seg.external_ref, seg.document_id, seg.text, seg.meta::text
               FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               JOIN sources src ON src.id=doc.source_id JOIN projects p ON p.id=src.project_id
               WHERE p.slug=%s ORDER BY seg.external_ref""",
            (project_a,),
        ).fetchall()
    with pytest.raises(LoadError, match="TELEGRAM_PROJECT_CONFLICT"):
        load_telegram_export(
            topic_export, tmp_path / "artifacts" / "topic-conflict", env_path=Path(TEST_ENV),
            project_slug=project_b, root=root,
        )
    with _connect(disposable_postgres_credentials) as conn:
        after = conn.execute(
            """SELECT seg.external_ref, seg.document_id, seg.text, seg.meta::text
               FROM segments seg JOIN documents doc ON doc.id=seg.document_id
               JOIN sources src ON src.id=doc.source_id JOIN projects p ON p.id=src.project_id
               WHERE p.slug=%s ORDER BY seg.external_ref""",
            (project_a,),
        ).fetchall()
        assert after == before
        assert conn.execute(
            "SELECT count(*) FROM projects WHERE slug=%s", (project_b,),
        ).fetchone()[0] == 0


@pytest.mark.parametrize("media_root", ["media", "media-resume"])
@pytest.mark.parametrize("checkpoint_version", [1, 2])
def test_broker_stages_each_checkpoint_media_root_and_repeat_ingest_is_idempotent(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    media_root: str,
    checkpoint_version: int,
) -> None:
    from kiber_analyst import broker
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    monkeypatch.setattr(broker, "RESERVE", 0)
    project_slug = f"kiber-resume-root-{uuid.uuid4().hex[:10]}"
    owner = "synthetic-resume-root-owner"
    app_root = tmp_path / "application"
    imports_root = app_root / "imports"
    app_root.mkdir(mode=0o700)
    (app_root / "broker").mkdir(mode=0o700)
    imports_root.mkdir(mode=0o700)
    (imports_root / ".broker").mkdir(mode=0o700)
    source = _fixture(imports_root)
    source.parent.chmod(0o700)
    chat_id = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["chat_id"]
    source_id = f"telegram:{chat_id}:topic:1"

    message_path = source / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    media_bytes = b"recovered media root fixture"
    media_sha = hashlib.sha256(media_bytes).hexdigest()
    message["media"] = {
        "type": "photo", "path": None, "size": len(media_bytes),
        "sha256": None, "size_source": "synthetic",
        "max_file_bytes": 4096, "export_status": "skipped",
        "reason": "unknown_size",
    }
    message_bytes = _json_bytes(message)
    _write_private(message_path, message_bytes)
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        messages_sha256=hashlib.sha256(message_bytes).hexdigest(),
        max_file_bytes=4096,
    )
    manifest_bytes = _json_bytes(manifest)
    _write_private(manifest_path, manifest_bytes)
    services_bytes = (source / "services.jsonl").read_bytes()
    services_manifest_bytes = (source / "services_manifest.json").read_bytes()
    checkpoint_inputs = {
        "messages.jsonl": hashlib.sha256(message_bytes).hexdigest(),
        "services.jsonl": hashlib.sha256(services_bytes).hexdigest(),
        "manifest.json": hashlib.sha256(manifest_bytes).hexdigest(),
        "services_manifest.json": hashlib.sha256(services_manifest_bytes).hexdigest(),
    }
    if checkpoint_version == 1:
        checkpoint_inputs.pop("services_manifest.json")
    relative_media = f"{media_root}/101/recovered.bin"
    _write_private(source / relative_media, media_bytes)
    (source / media_root).chmod(0o700)
    (source / media_root / "101").chmod(0o700)
    checkpoint = _json_bytes({
        "version": checkpoint_version,
        "source_id": source_id,
        "inputs": checkpoint_inputs,
        "requested_max_file_bytes": 4096,
        "records": {"101": {
            "path": relative_media,
            "size": len(media_bytes),
            "sha256": media_sha,
            "type": "photo",
            "max_file_bytes": 4096,
            "export_status": "downloaded",
        }},
    })
    _write_private(source / "media-resume" / "status.json", checkpoint)
    (source / "media-resume").chmod(0o700)

    submitted: list[list[str]] = []

    def worker_api(action: str, **kwargs):
        if action == "project-catalog":
            return {"projects": [{"slug": project_slug, "title": "Resume root test"}]}
        if action == "source-overview":
            return {
                "documents": 1, "messages": 1, "unique_messages": 1,
                "service_events": 1, "messages_with_media": 1,
            }
        if action == "status":
            return {"state": "done"}
        if action == "result":
            return {"inserted": 1, "service_events_inserted": 1}
        raise AssertionError(f"unexpected worker API operation: {action}")

    def submit(argv: list[str], *, idempotency_key: str):
        submitted.append(list(argv))
        return {"job_id": str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key))}

    app = broker.Broker(
        app_root, {}, worker_api=worker_api, submit=submit,
        authorized_projects={project_slug},
    )
    app.sources[source_id] = {
        "project": project_slug,
        "hashes": {
            name: checkpoint_inputs[name]
            for name in ("manifest.json", "messages.jsonl", "services.jsonl")
        },
    }
    try:
        assert app.handle(owner, "source-add", source_id, project_slug)["registered"]
        request = app.handle(owner, "ingest", source_id, project_slug)
        staged = Path(submitted[-1][submitted[-1].index("--job") + 1])
        assert request["job_id"]
        staged_file = staged / relative_media
        assert staged_file.read_bytes() == media_bytes
        assert hashlib.sha256(staged_file.read_bytes()).hexdigest() == media_sha

        first = load_telegram_export(
            staged, tmp_path / "artifacts" / f"first-{media_root}-{checkpoint_version}",
            env_path=Path(TEST_ENV), project_slug=project_slug, root=imports_root,
        )
        assert (first["inserted"], first["media_missing"], first["service_events_inserted"]) == (1, 0, 1)

        with _connect(disposable_postgres_credentials) as conn:
            project_id = conn.execute(
                "SELECT id FROM projects WHERE slug=%s", (project_slug,),
            ).fetchone()[0]
            source_db_id = conn.execute(
                "SELECT id FROM sources WHERE project_id=%s AND external_id=%s",
                (project_id, chat_id),
            ).fetchone()[0]
            segment_id = conn.execute(
                "SELECT id FROM segments WHERE external_ref=%s", (f"tg:{chat_id}:101",),
            ).fetchone()[0]
            vector = "[" + ",".join(["0"] * 1536) + "]"
            conn.execute(
                """INSERT INTO embeddings
                   (project_id, owner_table, owner_id, model, embedding)
                   VALUES (%s::uuid, 'segments', %s, 'gemini-embedding-2:1536', %s::vector)""",
                (project_id, str(segment_id), vector),
            )
            embedding_before = conn.execute(
                "SELECT id, owner_id, model, embedding::text FROM embeddings "
                "WHERE owner_table='segments' AND owner_id=%s", (str(segment_id),),
            ).fetchone()

        entry, fingerprint, parsed = app._managed_source_entry(owner, source_id, project_slug)
        assert parsed == (chat_id, "1")
        revision = hashlib.sha256(checkpoint).hexdigest()
        repeated_stage = app.stage(source_id, entry, fingerprint, chat_id, "1", revision)
        assert repeated_stage == staged
        repeated = load_telegram_export(
            repeated_stage,
            tmp_path / "artifacts" / f"repeat-{media_root}-{checkpoint_version}",
            env_path=Path(TEST_ENV), project_slug=project_slug, root=imports_root,
        )
        assert (repeated["inserted"], repeated["media_missing"],
                repeated["service_events_skipped"]) == (0, 0, 1)
        assert (repeated_stage / relative_media).read_bytes() == media_bytes

        with _connect(disposable_postgres_credentials) as conn:
            assert conn.execute(
                "SELECT count(*) FROM segments WHERE external_ref=%s", (f"tg:{chat_id}:101",),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM telegram_service_events WHERE source_id=%s",
                (source_db_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT id, owner_id, model, embedding::text FROM embeddings "
                "WHERE owner_table='segments' AND owner_id=%s", (str(segment_id),),
            ).fetchone() == embedding_before

            segment_before = conn.execute(
                "SELECT id, external_ref, text, meta::text FROM segments WHERE id=%s",
                (segment_id,),
            ).fetchone()
            events_before = conn.execute(
                """SELECT external_ref, event_id, topic_id, event_type, payload::text,
                          provenance::text, event_sha256
                   FROM telegram_service_events WHERE source_id=%s ORDER BY event_id""",
                (source_db_id,),
            ).fetchall()
        files_before = _tree_snapshot(source)

        reindex_first = app.handle(owner, "reindex", source_id)
        reindex_repeat = app.handle(owner, "reindex", source_id)
        assert reindex_first == reindex_repeat
        assert reindex_first["integrity"] == {
            "media_sha256_verified": 1, "messages_match": True,
            "service_events_match": True, "ingested_media_match": True,
        }
        assert _tree_snapshot(source) == files_before

        with _connect(disposable_postgres_credentials) as conn:
            assert conn.execute(
                "SELECT id, external_ref, text, meta::text FROM segments WHERE id=%s",
                (segment_id,),
            ).fetchone() == segment_before
            assert conn.execute(
                """SELECT external_ref, event_id, topic_id, event_type, payload::text,
                          provenance::text, event_sha256
                   FROM telegram_service_events WHERE source_id=%s ORDER BY event_id""",
                (source_db_id,),
            ).fetchall() == events_before
            assert conn.execute(
                "SELECT id, owner_id, model, embedding::text FROM embeddings "
                "WHERE owner_table='segments' AND owner_id=%s", (str(segment_id),),
            ).fetchone() == embedding_before
    finally:
        app.db.close()


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


def test_service_event_identity_cannot_move_between_projects(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
) -> None:
    from kiber_analyst.errors import LoadError
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    project_a = f"service-iso-a-{uuid.uuid4().hex[:10]}"
    project_b = f"service-iso-b-{uuid.uuid4().hex[:10]}"
    with _connect(disposable_postgres_credentials) as conn:
        conn.execute(
            "INSERT INTO projects (slug, title) VALUES (%s, %s), (%s, %s)",
            (project_a, "Service isolation A", project_b, "Service isolation B"),
        )

    export_root = tmp_path / "telegram"
    chat_id = f"-100{uuid.uuid4().int % 10**15 + 1}"
    source = _scope_fixture(export_root, chat_id=chat_id, scope="chat", topic_id=None)
    _write_private(source / "messages.jsonl", b"")
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(message_count=0, messages_sha256=hashlib.sha256(b"").hexdigest())
    _write_private(manifest_path, _json_bytes(manifest))
    artifacts = tmp_path / "artifacts"

    loaded = load_telegram_export(
        source, artifacts / "a", env_path=Path(TEST_ENV), project_slug=project_a,
        root=export_root,
    )
    assert loaded["service_events_inserted"] == 1

    def event_snapshot(conn, slug):
        return conn.execute(
            """SELECT event.external_ref, event.topic_id, event.event_type,
                      event.payload, event.provenance, event.event_sha256
                 FROM telegram_service_events event
                 JOIN sources source ON source.id=event.source_id
                 JOIN projects project ON project.id=source.project_id
                 WHERE project.slug=%s ORDER BY event.external_ref""",
            (slug,),
        ).fetchall()

    with _connect(disposable_postgres_credentials) as conn:
        before_a = event_snapshot(conn, project_a)
        assert len(before_a) == 1
        assert event_snapshot(conn, project_b) == []

    with pytest.raises(LoadError, match="TELEGRAM_PROJECT_CONFLICT"):
        load_telegram_export(
            source, artifacts / "b", env_path=Path(TEST_ENV), project_slug=project_b,
            dry_run=True, root=export_root,
        )

    with _connect(disposable_postgres_credentials) as conn:
        assert event_snapshot(conn, project_a) == before_a
        assert event_snapshot(conn, project_b) == []
        assert conn.execute(
            "SELECT count(*) FROM telegram_service_events WHERE external_ref=%s",
            (f"tg:{chat_id}:service:700",),
        ).fetchone()[0] == 1


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


def test_sequential_export_revisions_keep_both_media_and_loaded_data(
    tmp_path: Path,
    disposable_postgres_credentials: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from telegram_pipeline_support import build_partial_media_revisions

    from kiber_analyst import broker
    from kiber_analyst.telegram_ingest import load_telegram_export

    assert TEST_ENV is not None
    # Synthetic integration data is not subject to the production shared-storage reserve.
    monkeypatch.setattr(broker, "RESERVE", 0)
    project_slug = f"kiber-media-revision-{uuid.uuid4().hex[:10]}"
    chat_id = f"-100{uuid.uuid4().int % 10**15 + 1}"
    package: dict[str, object] = {}
    jobs: dict[str, dict] = {}
    submitted: list[tuple[str, list[str]]] = []
    owner = "synthetic-pipeline-owner"

    def worker_api(action: str, **kwargs):
        if action == "project-catalog":
            return {"projects": [{"slug": project_slug, "title": "Synthetic media revisions"}]}
        if action == "status":
            return jobs[kwargs["job_id"]]
        if action == "result":
            return jobs[kwargs["job_id"]].get("result", {})
        if action == "source-overview":
            return {"messages": 2, "service_events": 1}
        raise AssertionError(f"unexpected worker operation: {action}")

    def submit(argv: list[str], *, idempotency_key: str):
        job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key))
        jobs.setdefault(job_id, {"state": "queued", "progress": {}})
        submitted.append((job_id, list(argv)))
        return {"job_id": job_id}

    def stage_from_last_submission() -> Path:
        argv = submitted[-1][1]
        assert argv[:2] == ["telegram", "ingest"]
        return Path(argv[argv.index("--job") + 1])

    def complete_job(request: dict, result: dict) -> None:
        jobs[request["job_id"]] = {
            "state": "done", "progress": {}, "result": result,
        }

    def process_first_published_revision(stage: Path) -> None:
        application_root = tmp_path / "pipeline-data" / "application"
        (application_root / "broker").mkdir(mode=0o700, parents=True, exist_ok=True)
        (application_root / "imports" / ".broker").mkdir(mode=0o700, parents=True, exist_ok=True)
        app = broker.Broker(
            application_root, {}, worker_api=worker_api, submit=submit,
            authorized_projects={project_slug},
        )
        package["broker"] = app
        package["application_root"] = application_root
        package["source_id"] = f"telegram:{chat_id}:topic:17"

        source_id = package["source_id"]
        registration = app.handle(owner, "source-add", source_id, project_slug)
        assert registration["registered"] is True
        dry_request = app.handle(owner, "ingest-dry-run", source_id, project_slug)
        dry_stage = stage_from_last_submission()
        dry = load_telegram_export(
            dry_stage,
            tmp_path / "artifacts" / "first-dry-run",
            env_path=Path(TEST_ENV), project_slug=project_slug, dry_run=True,
            root=application_root / "imports",
        )
        assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
        assert dry["inserted"] == 2 and dry["service_events_inserted"] == 1
        assert dry["media_missing"] == 1
        with _connect(disposable_postgres_credentials) as conn:
            assert conn.execute(
                "SELECT count(*) FROM projects WHERE slug=%s", (project_slug,),
            ).fetchone()[0] == 0
        complete_job(dry_request, {"status": "dry_run", "counts": {"inserted": 2}})

        real_request = app.handle(owner, "ingest", source_id, project_slug)
        first_real_stage = stage_from_last_submission()
        first = load_telegram_export(
            first_real_stage,
            tmp_path / "artifacts" / "first-real-load",
            env_path=Path(TEST_ENV), project_slug=project_slug,
            root=application_root / "imports",
        )
        assert first["status"] == "done" and first["media_missing"] == 1
        assert (first["inserted"], first["service_events_inserted"]) == (2, 1)
        complete_job(real_request, {"status": "done", "counts": {"inserted": 2}})
        package["first_stage"] = first_real_stage
        package["first_request"] = real_request

        repeated_request = app.handle(owner, "ingest", source_id, project_slug)
        assert repeated_request == real_request
        repeated = load_telegram_export(
            first_real_stage,
            tmp_path / "artifacts" / "first-repeat",
            env_path=Path(TEST_ENV), project_slug=project_slug,
            root=application_root / "imports",
        )
        assert (repeated["inserted"], repeated["skipped"]) == (0, 2)
        assert repeated["service_events_skipped"] == 1

        with _connect(disposable_postgres_credentials) as conn:
            project_id = conn.execute(
                "SELECT id FROM projects WHERE slug=%s", (project_slug,),
            ).fetchone()[0]
            segment_rows = conn.execute(
                """SELECT seg.id, seg.external_ref, seg.text, seg.content_hash,
                          seg.meta->>'media_path',
                          (seg.meta->'media_export'->>'size')::bigint,
                          seg.meta->'media_export'->>'sha256'
                   FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                   JOIN sources src ON src.id=doc.source_id
                   WHERE src.project_id=%s ORDER BY seg.external_ref""",
                (project_id,),
            ).fetchall()
            assert [row[1] for row in segment_rows] == [
                f"tg:{chat_id}:501", f"tg:{chat_id}:502",
            ]
            assert segment_rows[0][4] == "media/501/item-501.bin"
            assert segment_rows[1][4] is None
            embedded_segment_id = segment_rows[0][0]
            vector = "[" + ",".join(["0"] * 1536) + "]"
            conn.execute(
                """INSERT INTO embeddings
                   (project_id, owner_table, owner_id, model, embedding)
                   VALUES (%s::uuid, 'segments', %s, 'gemini-embedding-2:1536', %s::vector)""",
                (project_id, str(embedded_segment_id), vector),
            )
            package["baseline_segments"] = segment_rows
            package["project_id"] = str(project_id)
            package["embedding_id"] = conn.execute(
                "SELECT id FROM embeddings WHERE owner_table='segments' AND owner_id=%s",
                (str(embedded_segment_id),),
            ).fetchone()[0]
            package["baseline_event"] = conn.execute(
                """SELECT event_id, event_type, payload::text
                   FROM telegram_service_events ev JOIN sources src ON src.id=ev.source_id
                   WHERE src.project_id=%s""",
                (project_id,),
            ).fetchone()

    try:
        package.update(build_partial_media_revisions(
            tmp_path, after_first_stage=process_first_published_revision, chat_id=chat_id,
        ))
        app = package["broker"]
        application_root = package["application_root"]
        source_id = package["source_id"]
        project_id = package["project_id"]
        assert (package["final_stage"] / "media/501/item-501.bin").is_file()
        assert (package["final_stage"] / "media/502/item-502.bin").is_file()
        first_media = package["final_stage"] / "media/501/item-501.bin"
        with first_media.open("rb") as stream:
            first_media_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
        large_media = package["final_stage"] / "media/502/item-502.bin"
        assert large_media.stat().st_size == 100 * 1024**2 + 1
        with large_media.open("rb") as stream:
            large_media_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()

        app.handle(owner, "source-add", source_id, project_slug)
        dry_request = app.handle(owner, "ingest-dry-run", source_id, project_slug)
        dry_stage = stage_from_last_submission()
        before_dry = load_telegram_export(
            dry_stage, tmp_path / "artifacts" / "second-dry-run",
            env_path=Path(TEST_ENV), project_slug=project_slug, dry_run=True,
            root=application_root / "imports",
        )
        assert before_dry["status"] == "dry_run"
        assert before_dry["rollback_confirmed"] is True and before_dry["media_missing"] == 0
        with _connect(disposable_postgres_credentials) as conn:
            assert conn.execute(
                "SELECT count(*) FROM segments seg JOIN documents d ON d.id=seg.document_id "
                "JOIN sources s ON s.id=d.source_id WHERE s.project_id=%s", (project_id,),
            ).fetchone()[0] == 2
            assert conn.execute(
                "SELECT count(*) FROM telegram_service_events ev JOIN sources s ON s.id=ev.source_id "
                "WHERE s.project_id=%s", (project_id,),
            ).fetchone()[0] == 1
        complete_job(dry_request, {"status": "dry_run", "counts": {"inserted": 0}})

        second_request = app.handle(owner, "ingest", source_id, project_slug)
        second_stage = stage_from_last_submission()
        second = load_telegram_export(
            second_stage, tmp_path / "artifacts" / "second-real-load",
            env_path=Path(TEST_ENV), project_slug=project_slug,
            root=application_root / "imports",
        )
        assert second["status"] == "done" and second["media_missing"] == 0
        assert second["inserted"] == 0
        assert (second["service_events_inserted"] + second["service_events_updated"]
                + second["service_events_skipped"]) == 1
        complete_job(second_request, {"status": "done", "counts": {"updated": second["updated"]}})
        assert app.handle(owner, "ingest", source_id, project_slug) == second_request

        repeated = load_telegram_export(
            second_stage, tmp_path / "artifacts" / "second-repeat",
            env_path=Path(TEST_ENV), project_slug=project_slug,
            root=application_root / "imports",
        )
        assert repeated["inserted"] == 0 and repeated["service_events_skipped"] == 1

        with _connect(disposable_postgres_credentials) as conn:
            final_rows = conn.execute(
                """SELECT seg.id, seg.external_ref, seg.text, seg.content_hash,
                          seg.meta->>'media_path',
                          (seg.meta->'media_export'->>'size')::bigint,
                          seg.meta->'media_export'->>'sha256'
                   FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                   JOIN sources src ON src.id=doc.source_id
                   WHERE src.project_id=%s ORDER BY seg.external_ref""",
                (project_id,),
            ).fetchall()
            assert len(final_rows) == 2
            assert [row[2:4] for row in final_rows] == [
                row[2:4] for row in package["baseline_segments"]
            ]
            assert final_rows[0][4] == "media/501/item-501.bin"
            assert final_rows[1][4] == "media/502/item-502.bin"
            assert final_rows[0][5] == 2
            assert final_rows[0][6] == first_media_sha256
            assert final_rows[1][5] == 100 * 1024**2 + 1
            assert final_rows[1][6] == large_media_sha256
            event = conn.execute(
                """SELECT event_id, event_type, payload::text
                   FROM telegram_service_events ev JOIN sources src ON src.id=ev.source_id
                   WHERE src.project_id=%s""",
                (project_id,),
            ).fetchone()
            assert event == package["baseline_event"]
            assert conn.execute(
                "SELECT count(*) FROM telegram_service_events ev JOIN sources src ON src.id=ev.source_id "
                "WHERE src.project_id=%s", (project_id,),
            ).fetchone()[0] == 1
            embedding = conn.execute(
                "SELECT id, owner_id, model, embedding::text FROM embeddings "
                "WHERE id=%s", (package["embedding_id"],),
            ).fetchone()
            assert embedding is not None
            assert embedding[1:3] == (str(final_rows[0][0]), "gemini-embedding-2:1536")
            assert embedding[0] == package["embedding_id"]
    finally:
        if package.get("broker") is not None:
            package["broker"].db.close()
