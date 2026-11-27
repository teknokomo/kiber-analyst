from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import tracemalloc
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker, telegram_exporter, telegram_publisher
from kiber_analyst.telegram_exporter import (
    Budget,
    Dir,
    _publish_services,
    _ServiceRecordStore,
    belongs,
    export,
    export_services,
    service_belongs,
)
from kiber_analyst.telegram_ingest import inspect_export, prepare_export

PROJECT = "telegram-scope-fixture"
CHAT = "-1007654321"


class _ScopeTelegram:
    def __init__(self, *, chat: str, scope: str, forum: bool, count: int = 1,
                 topic_provenance: bool = False):
        self.chat = chat
        self.scope = scope
        self.forum = forum
        self.count = count
        self.topic_provenance = topic_provenance
        self.page_calls = 0
        self.service_calls = 0

    def info(self, _chat):
        return {
            "id": int(self.chat), "forum": self.forum,
            "type": "supergroup" if self.forum else "group",
            "title": "Synthetic group",
        }

    def title(self, _chat, topic):
        return f"Synthetic topic {topic}" if topic is not None else None

    def page(self, _chat, topic, limit, before):
        self.page_calls += 1
        if self.page_calls > 1:
            return []
        if self.topic_provenance:
            cases = [
                # Telegram encodes forumTopic as an optional true flag. If it
                # is absent, a topMsgId alone does not make this a topic reply.
                (1006, {"topMsgId": 17}),
                (1005, {"forumTopic": False, "topMsgId": 17}),
                (1004, {"forumTopic": False}),
                (1003, {"forumTopic": True, "topMsgId": 17}),
                (1002, {"forumTopic": True, "replyToMsgId": 1003}),
                (1001, {"forumTopic": True, "replyToMsgId": 9999}),
                (1000, {"forumTopic": True, "topMsgId": 17, "replyToMsgId": 1003}),
            ]
            return [{
                "id": message_id, "date": "2026-01-01T00:00:00Z",
                "sender": "Synthetic author", "senderId": 42,
                "text": f"forum provenance {message_id}",
                "topicId": 17 if message_id == 1002 else None,
                "full": {"replyTo": reply}, "media": None,
            } for message_id, reply in cases[:limit]]
        rows = []
        for index in range(self.count):
            message_id = 1000 - index
            actual_topic = 17 + index if self.forum and self.scope == "group" else 17
            full = {"replyTo": {"forumTopic": True, "topMsgId": actual_topic}} if self.forum else {}
            if topic is not None and actual_topic != topic:
                continue
            rows.append({
                "id": message_id,
                "date": "2026-01-01T00:00:00Z",
                "sender": "Synthetic author",
                "senderId": 42,
                "text": f"scope message {message_id}",
                "full": full,
                "media": {
                    "kind": "document", "size": len(f"media-{message_id}".encode()),
                    "file_name": f"{message_id}.bin",
                },
            })
        return rows[:limit]

    def media_metadata(self, _chat, _message_ids, peer_type='supergroup'):
        return {}

    def service_page(self, _chat, topic, limit, before):
        self.service_calls += 1
        if self.service_calls > 1:
            return []
        if topic is None and self.forum:
            topic_ids = [17 + index for index in range(self.count)]
        elif topic is not None:
            topic_ids = [topic]
        else:
            topic_ids = [None]
        events = []
        for index, topic_id in enumerate(topic_ids):
            event_id = 1700 + index
            action = (
                {"_": "messageActionTopicCreate", "title": f"Topic {topic_id}"}
                if topic_id is not None
                else {"_": "messageActionChatEditTitle", "title": "Renamed synthetic group"}
            )
            event = {
                "id": event_id if topic_id is None else topic_id,
                "date": "2026-01-01T00:00:00Z",
                "fromId": {"userId": 42},
                "action": action,
            }
            if topic_id is not None:
                event["replyTo"] = {"forumTopic": True, "topMsgId": topic_id}
            events.append(event)
        return events[:limit]

    def download(self, _chat, message_id, directory, _limit):
        fd = os.open(
            f"{message_id}.bin", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600, dir_fd=directory.fd,
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(f"media-{message_id}".encode())


def _write_config(path: Path, value: dict, mode: int):
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(mode)


def _run_scope_pipeline(
    tmp_path: Path, *, scope: str, forum: bool, count: int,
    monkeypatch: pytest.MonkeyPatch,
    topic_provenance: bool = False, project_slug: str | None = None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    chat = f"-100{uuid.uuid4().int % 10**12 + 1}"
    topic = 17 if scope == "topic" else None
    source_id = f"telegram:{chat}:topic:{topic}" if topic is not None else f"telegram:{chat}:group"
    data_root = tmp_path / "data"
    export_root = data_root / "exports"
    app_root = data_root / "application"
    imports_root = app_root / "imports"
    lock_root = data_root / "run"
    config_root = tmp_path / "config"
    for directory in (data_root, export_root, app_root, imports_root, lock_root, config_root):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
    (imports_root / ".broker").mkdir(mode=0o700)
    (app_root / "broker").mkdir(mode=0o700)
    storage_limit = 256 * 1024**2
    export_config = {"storage_limit_bytes": storage_limit, "min_free_bytes": 0}
    export_config_path = config_root / "export.json"
    publisher_config_path = config_root / "publisher.json"
    _write_config(export_config_path, export_config, 0o640)
    _write_config(publisher_config_path, {
        "version": 1, "data_root": str(data_root), "source_root": str(export_root),
        "destination_root": str(imports_root), "export_config": str(export_config_path),
        "lock_path": str(lock_root / "publisher.lock"), "expected_data_uuid": None,
        "require_mount": False, "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0, "source_uid": os.geteuid(), "source_gid": os.getegid(),
        "destination_uid": os.geteuid(), "destination_gid": os.getegid(),
    }, 0o600)

    project_slug = project_slug or PROJECT
    client = _ScopeTelegram(chat=chat, scope=scope, forum=forum, count=count,
                            topic_provenance=topic_provenance)
    root = Dir.root(str(export_root))
    try:
        export_manifest = export(
            root, export_config,
            ["export", chat, "GROUP" if topic is None else str(topic), "MEDIA", "100", "100MiB"],
            client=client,
        )
        service_manifest = export_services(
            root, export_config, chat, topic, 100, None, client=client,
        )
    finally:
        root.close()

    export_dir = export_root / chat / ("group" if topic is None else str(topic))
    inspected = inspect_export(export_dir, root=export_root)
    assert export_manifest["complete"] is True and service_manifest["complete"] is True
    assert inspected.scope == ("chat" if scope == "group" else "topic")
    assert inspected.message_count == count
    published = telegram_publisher.handle("copy", source_id, config_path=str(publisher_config_path))
    assert published["status"] == "ready"
    stage = imports_root / chat / ("group" if topic is None else str(topic))
    staged = inspect_export(stage, root=imports_root)
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    assert transfer["source_id"] == source_id and staged.message_count == count
    assert published["copied_bytes"] == transfer["copied_bytes"]
    assert staged.scope == ("chat" if scope == "group" else "topic")
    message_rows = [json.loads(line) for line in (stage / "messages.jsonl").read_text().splitlines()]
    event_rows = [json.loads(line) for line in (stage / "services.jsonl").read_text().splitlines()]
    assert published["copied_bytes"] == sum(
        row["media"]["size_bytes"] for row in message_rows
        if row.get("media") and row["media"].get("export_status") == "downloaded"
    )
    if scope == "group" and not topic_provenance:
        assert all((row["topic_id"] is None) == (not forum) for row in message_rows)
    elif scope == "topic":
        assert all(row["topic_id"] == topic for row in message_rows)
    if topic_provenance:
        assert [(row["message_id"], row["topic_id"], row["topic_id_status"])
                for row in message_rows] == [
                    (1006, 1, "known"),
                    (1005, 1, "known"), (1004, 1, "known"),
                    (1003, 17, "known"), (1002, None, "unknown"),
                    (1001, None, "unknown"), (1000, 17, "known"),
                ]
        assert export_manifest["topic_assignment_complete"] is False
        assert export_manifest["topic_assignment_unknown_count"] == 2
    assert len(event_rows) == inspected.service_event_count
    assert all(
        row.get("media") is None or (stage / row["media"]["path"]).is_file()
        for row in message_rows
    )

    submissions = []
    monkeypatch.setattr(broker, "RESERVE", 0)

    def worker_api(action, **_kwargs):
        if action == "project-catalog":
            return {"projects": [{"slug": project_slug, "title": "Synthetic project"}]}
        if action == "source-overview":
            return {"messages": count, "unique_messages": count, "service_events": len(event_rows)}
        raise AssertionError(action)

    def submit(argv, *, idempotency_key):
        submissions.append((argv, idempotency_key))
        return {"job_id": str(uuid.uuid4())}

    app = broker.Broker(
        app_root, {}, worker_api=worker_api, submit=submit, authorized_projects={project_slug},
    )
    try:
        registration = app.handle("synthetic-owner", "source-add", source_id, project_slug)
        request = app.handle("synthetic-owner", "ingest-dry-run", source_id, project_slug)
        worker_stage = Path(submissions[-1][0][submissions[-1][0].index("--job") + 1])
        normalized_dir, _metadata, media_missing = prepare_export(
            worker_stage, tmp_path / "normalized", root=imports_root,
        )
        normalized = [json.loads(line) for line in (
            normalized_dir / "normalized" / "messages.jsonl"
        ).read_text().splitlines()]
        assert registration["registered"] is True and request["request_id"]
        assert media_missing == 0
        assert all(row["meta"]["export_scope"] == inspected.scope for row in normalized)
        test_env = os.environ.get("KIBER_ANALYST_INGEST_TEST_ENV")
        if test_env and Path(test_env).is_file():
            from kiber_analyst.telegram_ingest import load_telegram_export

            dry = load_telegram_export(
                worker_stage, tmp_path / "artifacts" / "scope-dry",
                env_path=Path(test_env), project_slug=project_slug, dry_run=True,
                root=imports_root,
            )
            assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
            loaded = load_telegram_export(
                worker_stage, tmp_path / "artifacts" / "scope-load",
                env_path=Path(test_env), project_slug=project_slug, root=imports_root,
            )
            repeated = load_telegram_export(
                worker_stage, tmp_path / "artifacts" / "scope-repeat",
                env_path=Path(test_env), project_slug=project_slug, root=imports_root,
            )
            assert loaded["inserted"] == count
            assert loaded["service_events_inserted"] == len(event_rows)
            assert repeated["inserted"] == 0
            assert repeated["service_events_skipped"] == len(event_rows)
        return {
            "source_id": source_id, "messages": message_rows, "events": event_rows,
            "stage": stage, "export_dir": export_dir, "publisher_config": publisher_config_path,
            "imports_root": imports_root, "chat": chat,
            "project_slug": project_slug, "normalized": normalized,
            "sha256": hashlib.sha256((stage / "messages.jsonl").read_bytes()).hexdigest(),
        }
    finally:
        app.db.close()


@pytest.mark.parametrize(
    ("scope", "forum", "count"),
    [("group", False, 1), ("group", True, 2), ("topic", True, 1)],
)
def test_group_forum_group_and_topic_export_transfer_register_and_stage(
    tmp_path: Path, scope: str, forum: bool, count: int,
    monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    result = _run_scope_pipeline(
        tmp_path, scope=scope, forum=forum, count=count, monkeypatch=monkeypatch,
    )
    assert result["source_id"].endswith(":group") if scope == "group" else ":topic:17" in result["source_id"]
    assert len(result["messages"]) == count
    assert all("media" in row and row["media"]["path"].startswith("media/") for row in result["messages"])


def test_forum_topic_provenance_is_explicit_and_survives_ingest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    project = f"forum-provenance-{uuid.uuid4().hex[:10]}"
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=True, count=7, monkeypatch=monkeypatch,
        topic_provenance=True, project_slug=project,
    )
    normalized = result["normalized"]
    assert [(item["topic_id"], item["meta"]["topic_id_status"]) for item in normalized] == [
        ("1", "known"), ("1", "known"), ("1", "known"), ("17", "known"),
        (None, "unknown"), (None, "unknown"), ("17", "known"),
    ]
    if optional_disposable_postgres_credentials is None:
        return
    import psycopg
    from conftest import connect_validated_postgres

    with connect_validated_postgres(
        optional_disposable_postgres_credentials, psycopg.connect,
    ) as conn:
        rows = conn.execute(
            """SELECT seg.meta->>'topic_id', seg.meta->>'topic_id_status',
                      seg.meta->>'export_scope'
                 FROM segments seg JOIN documents doc ON doc.id=seg.document_id
                 JOIN sources src ON src.id=doc.source_id
                 JOIN projects p ON p.id=src.project_id
                 WHERE p.slug=%s ORDER BY seg.external_ref DESC""",
            (project,),
        ).fetchall()
    assert rows == [
        ("1", "known", "chat"), ("1", "known", "chat"), ("1", "known", "chat"),
        ("17", "known", "chat"), (None, "unknown", "chat"),
        (None, "unknown", "chat"), ("17", "known", "chat"),
    ]


def test_forum_general_provenance_precedes_reply_root_for_messages_and_services():
    general_reply = {"full": {"replyTo": {"forumTopic": False, "topMsgId": 420}}}
    general_reply_without_flag = {"full": {"replyTo": {"topMsgId": 420}}}
    topic_reply = {"full": {"replyTo": {"forumTopic": True, "topMsgId": 420}}}
    topic_root_without_reply_header = {}
    unresolved_topic_reply = {
        "full": {"replyTo": {"forumTopic": True, "replyToMsgId": 421}},
        "topicId": 420,
    }

    assert belongs(general_reply, 1, forum=True)
    assert not belongs(general_reply, 420, forum=True)
    assert belongs(general_reply_without_flag, 1, forum=True)
    assert not belongs(general_reply_without_flag, 420, forum=True)
    assert belongs(topic_reply, 420, forum=True)
    assert not belongs(topic_reply, 1, forum=True)
    assert belongs(topic_root_without_reply_header, 420, forum=True)
    assert not belongs(unresolved_topic_reply, 1, forum=True)

    general_event = {"id": 900, "replyTo": {"forumTopic": False, "topMsgId": 420}}
    topic_event = {"id": 901, "replyTo": {"forumTopic": True, "topMsgId": 420}}
    assert service_belongs(general_event, 1, forum=True)
    assert not service_belongs(general_event, 420, forum=True)
    general_event_without_flag = {"id": 902, "replyTo": {"topMsgId": 420}}
    assert service_belongs(general_event_without_flag, 1, forum=True)
    assert not service_belongs(general_event_without_flag, 420, forum=True)
    assert service_belongs(topic_event, 420, forum=True)
    assert not service_belongs(topic_event, 1, forum=True)


def test_service_topic_create_never_falls_into_general_and_respects_reply_top_id():
    create = {
        "id": 420,
        "action": {"_": "messageActionTopicCreate", "title": "New topic"},
    }
    normalized, _ = telegram_exporter.service_event(create, None, forum=True)
    assert normalized["topic_id"] is None
    assert normalized["topic_id_status"] == "unknown"
    assert not service_belongs(create, 1, forum=True)
    assert service_belongs(create, 420, forum=True)
    scoped, _ = telegram_exporter.service_event(create, 420, forum=True)
    assert scoped["topic_id"] == 420
    assert scoped["topic_id_status"] == "known"

    matching_root = {
        **create,
        "replyTo": {"forumTopic": True, "replyToTopId": 420},
    }
    normalized, _ = telegram_exporter.service_event(matching_root, None, forum=True)
    assert normalized["topic_id"] == 420
    assert normalized["topic_id_status"] == "known"
    assert service_belongs(matching_root, 420, forum=True)

    conflicting = {
        **create,
        "replyTo": {"forumTopic": True, "replyToTopId": 900},
    }
    normalized, limitations = telegram_exporter.service_event(
        conflicting, None, forum=True,
    )
    assert normalized["topic_id"] is None
    assert normalized["topic_id_status"] == "unknown"
    assert "topic_assignment_conflict" in limitations
    assert not service_belongs(conflicting, 1, forum=True)
    assert not service_belongs(conflicting, 420, forum=True)
    assert not service_belongs(conflicting, 900, forum=True)

    # A conflicting reply root stays unknown even when a caller passes that
    # root as the requested topic.
    scoped, _ = telegram_exporter.service_event(conflicting, 900, forum=True)
    assert scoped["topic_id"] is None
    assert scoped["topic_id_status"] == "unknown"


def test_service_snapshot_budget_replaces_prior_revision_reservation(tmp_path: Path):
    root_path = tmp_path / "export"
    root_path.mkdir(mode=0o700)
    root_path.chmod(0o700)
    root = Dir.root(str(root_path))
    try:
        budget = Budget(root, {
            "storage_limit_bytes": 128 * 1024**2,
            "min_free_bytes": 0,
            "aggregate_available_bytes": 30 * 1024**2,
        })
        first = root.private_stream_temp(
            (b"a" * (256 * 1024) for _ in range(16)), budget=budget,
        )
        root.publish_private_temp(first[0], "services.jsonl")
        first_reserved = budget.reserved_bytes
        second = root.private_stream_temp(
            (b"b" * (256 * 1024) for _ in range(32)), budget=budget,
        )
        root.publish_private_temp(second[0], "services.jsonl")
        assert first[1] == 4 * 1024**2
        assert second[1] == 8 * 1024**2
        assert budget.reserved_bytes == budget.named_reservations["services-snapshot"]
        assert budget.reserved_bytes > first_reserved
        assert budget.reserved_bytes < budget.cfg["aggregate_available_bytes"]
        assert first_reserved + budget.reserved_bytes > budget.cfg["aggregate_available_bytes"]
    finally:
        root.close()


def test_service_snapshot_failure_restores_previous_budget_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    root_path = tmp_path / "export"
    root_path.mkdir(mode=0o700)
    root_path.chmod(0o700)
    root = Dir.root(str(root_path))
    records = _ServiceRecordStore()
    replacement = _ServiceRecordStore()
    chat_dir = None
    destination = None
    try:
        chat_dir = root.child(CHAT, True)
        destination = chat_dir.child("group", True)
        budget = Budget(root, {
            "storage_limit_bytes": 128 * 1024**2,
            "min_free_bytes": 0,
            "aggregate_available_bytes": 64 * 1024**2,
        })
        records[1] = {"event_id": 1, "type": "title_changed", "new_value": "old"}
        _publish_services(
            destination, {"source_id": f"telegram:{CHAT}:group", "scope_before": None},
            records, None, True, budget,
        )
        previous_bytes = destination.read("services.jsonl", 1024**2)
        previous_reservation = budget.reserved_bytes
        replacement[1] = {
            "event_id": 1, "type": "title_changed", "new_value": "new" * 1000,
        }

        def fail_manifest(*_args, **_kwargs):
            raise OSError("synthetic manifest publication failure")

        monkeypatch.setattr(destination, "atomic_private", fail_manifest)
        with pytest.raises(OSError, match="synthetic manifest publication failure"):
            _publish_services(
                destination, {"source_id": f"telegram:{CHAT}:group", "scope_before": None},
                replacement, None, True, budget,
            )

        assert destination.read("services.jsonl", 1024**2) == previous_bytes
        assert budget.reserved_bytes == previous_reservation
        assert budget.named_reservations["services-snapshot"] == previous_reservation
        assert not [name for name in destination.names() if name.startswith(".tmp-services-")]
    finally:
        records.close()
        replacement.close()
        if destination is not None:
            destination.close()
        if chat_dir is not None:
            chat_dir.close()
        root.close()


def test_broker_stage_checks_control_space_before_copying_controls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    # This case exercises the broker's preflight boundary only; keep the
    # shared export fixture from loading data into the optional PG database.
    monkeypatch.delenv("KIBER_ANALYST_INGEST_TEST_ENV", raising=False)
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    source = result["stage"]
    inspected = inspect_export(source, root=result["imports_root"])
    entry = {"hashes": {
        "messages.jsonl": inspected.message_sha256,
        "services.jsonl": inspected.service_sha256,
        "manifest.json": inspected.manifest_sha256,
    }}
    app_root = result["imports_root"].parent
    broker_stage_root = result["imports_root"] / ".broker"
    existing_stages = set(broker_stage_root.iterdir())
    broker_app = broker.Broker(app_root, {})
    copies = []
    original_copy = broker._copy_input
    monkeypatch.setattr(
        broker, "_copy_input",
        lambda *args, **kwargs: (copies.append(args[1]), original_copy(*args, **kwargs))[1],
    )
    real_os = broker.os

    class NoFreeSpace:
        def __getattr__(self, name):
            return getattr(real_os, name)

        @staticmethod
        def fstatvfs(_fd):
            return SimpleNamespace(f_bavail=0, f_frsize=1)

    monkeypatch.setattr(broker, "os", NoFreeSpace())
    monkeypatch.setattr(broker, "RESERVE", 0)
    try:
        with pytest.raises(broker.Refused, match="INSUFFICIENT_SPACE"):
            broker_app.stage(
                result["source_id"], entry, "c" * 64, result["chat"], "group",
            )
        assert copies == []
        assert set(broker_stage_root.iterdir()) == existing_stages
    finally:
        broker_app.db.close()


def test_publisher_refuses_messages_changed_between_validation_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    source = result["export_dir"]
    stage = result["stage"]
    previous_stage = tmp_path / "preserved-previous-stage"
    os.rename(stage, previous_stage)

    messages = source / "messages.jsonl"
    raw = messages.read_bytes()
    decoded = json.loads(raw)
    decoded["text"] = "updated source before second publication"
    changed = (json.dumps(decoded, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
    messages.write_bytes(changed)
    messages.chmod(0o600)
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["messages_sha256"] = hashlib.sha256(changed).hexdigest()
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n").encode()
    manifest_path.write_bytes(manifest_bytes)
    manifest_path.chmod(0o600)

    original_iter = telegram_publisher._validated_message_rows
    validations = 0
    mutation = b"updated source"
    replacement = b"Updated source"

    def change_source_after_sizing_pass(*args, **kwargs):
        nonlocal validations
        validations += 1
        yield from original_iter(*args, **kwargs)
        if validations == 3:
            current = messages.read_bytes()
            assert mutation in current
            altered = current.replace(mutation, replacement, 1)
            assert len(altered) == len(current)
            with messages.open("wb") as stream:
                stream.write(altered)
                stream.flush()
                os.fsync(stream.fileno())

    monkeypatch.setattr(
        telegram_publisher, "_validated_message_rows", change_source_after_sizing_pass,
    )
    with pytest.raises(telegram_publisher.TransferError, match="MESSAGES_HASH_MISMATCH"):
        telegram_publisher.handle(
            "copy", result["source_id"], config_path=str(result["publisher_config"]),
        )

    assert validations >= 4
    assert previous_stage.is_dir()
    assert not stage.exists()
    chat_root = result["imports_root"] / result["chat"]
    assert not list(chat_root.glob(".transfer-*"))
    assert not list(chat_root.glob(".publisher-init-*"))
    assert not list(chat_root.glob(".publisher-init-*.json"))


def test_publisher_fsyncs_chat_directory_after_final_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    previous_stage = tmp_path / "preserved-ready-revision"
    os.rename(stage, previous_stage)
    parent_info = os.stat(stage.parent)
    events: list[str] = []
    final_renamed = False
    original_rename = telegram_publisher._rename_noreplace
    original_fsync = os.fsync

    def record_rename(source_parent, source_name, destination_parent, destination_name):
        nonlocal final_renamed
        result_value = original_rename(
            source_parent, source_name, destination_parent, destination_name,
        )
        parent = os.fstat(destination_parent)
        if ((parent.st_dev, parent.st_ino) == (parent_info.st_dev, parent_info.st_ino)
                and destination_name == stage.name):
            events.append("final_rename")
            final_renamed = True
        return result_value

    fail_final_parent_sync = True

    def record_fsync(fd):
        nonlocal fail_final_parent_sync
        info = os.fstat(fd)
        if (final_renamed
                and (info.st_dev, info.st_ino) == (parent_info.st_dev, parent_info.st_ino)):
            events.append("chat_directory_fsync_after_final")
            if fail_final_parent_sync:
                fail_final_parent_sync = False
                raise OSError("simulated parent fsync failure")
        return original_fsync(fd)

    monkeypatch.setattr(telegram_publisher, "_rename_noreplace", record_rename)
    monkeypatch.setattr(telegram_publisher.os, "fsync", record_fsync)
    with pytest.raises(OSError, match="simulated parent fsync failure"):
        telegram_publisher.handle(
            "copy", result["source_id"], config_path=str(result["publisher_config"]),
        )
    assert stage.is_dir() and previous_stage.is_dir()
    published = telegram_publisher.handle(
        "copy", result["source_id"], config_path=str(result["publisher_config"]),
    )
    assert published["status"] == "already_ready"
    assert events.count("chat_directory_fsync_after_final") >= 2
    assert events.index("final_rename") < events.index("chat_directory_fsync_after_final")
    assert stage.is_dir() and previous_stage.is_dir()


def test_publisher_recovers_marked_partial_temps_and_refuses_unknown_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    media_sha = transfer["media_manifest_sha256"]
    temp_name = ".transfer-" + hashlib.sha256(
        (source_id + json.dumps(source_hashes, sort_keys=True) + (media_sha or "none")).encode()
    ).hexdigest()[:32]
    marker = {
        "source_id": source_id,
        "source_hashes": source_hashes,
        "media_manifest_sha256": media_sha,
    }

    def snapshot():
        return {
            str(path.relative_to(stage)): hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
            for path in stage.rglob("*") if path.is_file()
        }

    before = snapshot()
    source_token = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:16]
    unmarked_write_temp = f".publisher-init-write-{source_token}-{uuid.uuid4().hex}.tmp"
    (parent / unmarked_write_temp).write_bytes(b'{"partial":')
    (parent / unmarked_write_temp).chmod(0o600)
    other_source = source_id + ":other-scope"
    other_token = hashlib.sha256(other_source.encode("utf-8")).hexdigest()[:16]
    other_source_temp = f".publisher-init-write-{other_token}-{uuid.uuid4().hex}.tmp"
    (parent / other_source_temp).write_bytes(b'{"other":')
    (parent / other_source_temp).chmod(0o600)
    legacy_unscoped_temp = ".publisher-init-write-" + uuid.uuid4().hex + ".tmp"
    (parent / legacy_unscoped_temp).write_bytes(b'{"legacy":')
    (parent / legacy_unscoped_temp).chmod(0o600)
    parent_info = os.stat(parent)
    original_fsync_for_temp = os.fsync
    temp_recovery_syncs = []

    def record_temp_recovery_sync(fd):
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode) and info.st_ino == parent_info.st_ino:
            temp_recovery_syncs.append(not (parent / unmarked_write_temp).exists())
        return original_fsync_for_temp(fd)

    monkeypatch.setattr(telegram_publisher.os, "fsync", record_temp_recovery_sync)
    recovered_temp = telegram_publisher.handle(
        "copy", result["source_id"], config_path=str(result["publisher_config"]),
    )
    assert recovered_temp["status"] == "already_ready"
    assert not (parent / unmarked_write_temp).exists()
    assert (parent / other_source_temp).exists()
    assert (parent / legacy_unscoped_temp).exists()
    assert recovered_temp["recovery_cleanup"]["reclaimed_bytes"] >= len(b'{"partial":')
    assert recovered_temp["recovery_cleanup"]["preserved"] == {
        "other_source_intent_write": 1,
        "unscoped_legacy_intent_write": 1,
    }
    assert any(temp_recovery_syncs)
    monkeypatch.setattr(telegram_publisher.os, "fsync", original_fsync_for_temp)

    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        init_name = ".publisher-init-" + uuid.uuid4().hex
        telegram_publisher._write_init_intent(
            parent_fd, init_name, uid=os.geteuid(), gid=os.getegid(),
            source_id=source_id, source_hashes=source_hashes,
            media_manifest_sha=media_sha,
        )
        os.mkdir(init_name, 0o700, dir_fd=parent_fd)
        init_fd = os.open(init_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            # Simulate a crash after mkdir but before the internal marker.
            (parent / init_name / "partial-metadata").write_bytes(b"partial")
            (parent / init_name / "partial-metadata").chmod(0o600)
            os.fsync(init_fd)
        finally:
            os.close(init_fd)

        os.mkdir(temp_name, 0o700, dir_fd=parent_fd)
        temp_fd = os.open(temp_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                          dir_fd=parent_fd)
        try:
            fd = os.open(".publisher-temp.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                         0o600, dir_fd=temp_fd)
            with os.fdopen(fd, "wb") as stream:
                stream.write((json.dumps(marker, sort_keys=True) + "\n").encode())
            fd = os.open("messages.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                         0o600, dir_fd=temp_fd)
            os.write(fd, b"partial package")
            os.close(fd)
            os.fsync(temp_fd)
        finally:
            os.close(temp_fd)
    finally:
        os.close(parent_fd)

    # The durable sidecar proves ownership of an init tree even before its
    # in-directory marker exists; the completed revision remains unchanged.
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(tmp_path / "config" / "publisher.json"),
    )
    assert recovered["status"] == "already_ready"
    assert (parent / init_name).with_suffix(".json").exists() is False
    assert not (parent / init_name).exists() and not (parent / temp_name).exists()
    assert snapshot() == before

    # Removal of a partial .transfer entry must be durable in its parent.
    temp_dir = parent / temp_name
    temp_dir.mkdir(mode=0o700)
    (temp_dir / ".publisher-temp.json").write_text(json.dumps(marker, sort_keys=True) + "\n")
    (temp_dir / ".publisher-temp.json").chmod(0o600)
    (temp_dir / "messages.jsonl").write_bytes(b"partial")
    (temp_dir / "messages.jsonl").chmod(0o600)
    os.chmod(temp_dir, 0o700)
    parent_info = os.stat(parent)
    real_fsync = os.fsync
    synced_parent_after_removal = []

    def record_directory_fsync(fd):
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode) and info.st_ino == parent_info.st_ino:
            synced_parent_after_removal.append(not temp_dir.exists())
        return real_fsync(fd)

    monkeypatch.setattr(telegram_publisher.os, "fsync", record_directory_fsync)
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(tmp_path / "config" / "publisher.json"),
    )
    assert recovered["status"] == "already_ready"
    assert not temp_dir.exists()
    assert any(synced_parent_after_removal), "parent directory must be fsynced after temp removal"
    assert snapshot() == before
    monkeypatch.setattr(telegram_publisher.os, "fsync", real_fsync)

    unknown_name = ".publisher-init-" + uuid.uuid4().hex
    unknown = parent / unknown_name
    unknown.mkdir(mode=0o700)
    (unknown / ".publisher-temp.json").write_text("{}\n")
    (unknown / ".publisher-temp.json").chmod(0o600)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(tmp_path / "config" / "publisher.json"),
        )
    assert snapshot() == before


def _add_empty_media_resume_revision(export_dir: Path, source_id: str) -> str:
    names = ("messages.jsonl", "services.jsonl", "manifest.json", "services_manifest.json")
    hashes = {name: hashlib.sha256((export_dir / name).read_bytes()).hexdigest()
              for name in names}
    resume = export_dir / "media-resume"
    resume.mkdir(mode=0o700)
    checkpoint = {
        "version": 2, "source_id": source_id, "inputs": hashes, "records": {},
        "requested_max_file_bytes": telegram_publisher.MAX_FILE_BYTES,
        "over_limit_message_ids": [],
    }
    checkpoint_bytes = (json.dumps(checkpoint, sort_keys=True) + "\n").encode()
    (resume / "status.json").write_bytes(checkpoint_bytes)
    (resume / "status.json").chmod(0o600)
    media_manifest = {
        "version": 2, "source_id": source_id, "inputs": hashes,
        "checkpoint_sha256": hashlib.sha256(checkpoint_bytes).hexdigest(),
        "requested_max_file_bytes": telegram_publisher.MAX_FILE_BYTES,
        "media_count": 0, "remaining": 0, "complete": True,
        "download_errors": 0,
    }
    (resume / "manifest.json").write_text(
        json.dumps(media_manifest, sort_keys=True) + "\n", encoding="utf-8",
    )
    (resume / "manifest.json").chmod(0o600)
    return telegram_publisher._media_manifest_sha(media_manifest)


def _add_interrupted_revision(parent: Path, *, kind: str, source_id: str,
                              source_hashes: dict, stage: Path) -> tuple[str, str | None]:
    old_media_sha = None
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, old_media_sha,
    )
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if kind in {"before_directory", "partial_init_directory", "complete_candidate"}:
            init_name = ".publisher-init-" + uuid.uuid4().hex
            telegram_publisher._write_init_intent(
                parent_fd, init_name, uid=os.geteuid(), gid=os.getegid(),
                source_id=source_id, source_hashes=source_hashes,
                media_manifest_sha=old_media_sha,
            )
            if kind == "partial_init_directory":
                os.mkdir(init_name, 0o700, dir_fd=parent_fd)
                init_fd = os.open(init_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                  dir_fd=parent_fd)
                try:
                    fd = os.open("partial-metadata", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                 0o600, dir_fd=init_fd)
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(b"partial-init-data")
                    os.fsync(init_fd)
                finally:
                    os.close(init_fd)
            elif kind == "complete_candidate":
                shutil.copytree(stage, parent / candidate_name)
                marker = {
                    "source_id": source_id, "source_hashes": source_hashes,
                    "media_manifest_sha256": old_media_sha,
                }
                marker_path = parent / candidate_name / ".publisher-temp.json"
                marker_path.write_text(json.dumps(marker, sort_keys=True) + "\n")
                marker_path.chmod(0o600)
        elif kind == "complete_candidate_marked":
            shutil.copytree(stage, parent / candidate_name)
            marker = {
                "source_id": source_id, "source_hashes": source_hashes,
                "media_manifest_sha256": old_media_sha,
            }
            marker_path = parent / candidate_name / ".publisher-temp.json"
            marker_path.write_text(json.dumps(marker, sort_keys=True) + "\n")
            marker_path.chmod(0o600)
        elif kind == "partial_candidate":
            marker = {
                "source_id": source_id, "source_hashes": source_hashes,
                "media_manifest_sha256": old_media_sha,
            }
            (parent / candidate_name).mkdir(mode=0o700)
            (parent / candidate_name / ".publisher-temp.json").write_text(
                json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8",
            )
            (parent / candidate_name / ".publisher-temp.json").chmod(0o600)
            (parent / candidate_name / "messages.jsonl").write_bytes(b"partial-candidate")
            (parent / candidate_name / "messages.jsonl").chmod(0o600)
        else:
            raise AssertionError(kind)
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    return candidate_name, old_media_sha


@pytest.mark.parametrize(
    "kind", ["before_directory", "partial_init_directory", "partial_candidate",
             "complete_candidate", "complete_candidate_marked"],
)
def test_publisher_recovers_old_revision_before_publishing_new_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    old_transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    candidate_name, old_media_sha = _add_interrupted_revision(
        parent, kind=kind, source_id=source_id,
        source_hashes=old_transfer["source_hashes"], stage=stage,
    )
    new_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    assert new_media_sha != old_media_sha
    previous_ready_sha = hashlib.sha256(
        (stage / "telegram_transfer_manifest.json").read_bytes()
    ).hexdigest()

    published = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )
    current = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    assert published["status"] == "ready"
    assert current["media_manifest_sha256"] == new_media_sha
    assert (parent / ".revisions" / f"revision-{previous_ready_sha}").is_dir()
    if kind in {"complete_candidate", "complete_candidate_marked"}:
        # The fully verified unpublished candidate is retained separately;
        # the prior ready package is retained in the normal revision archive.
        recovery_archives = list((parent / ".revisions").glob("recovery-*"))
        assert len(recovery_archives) == 1
        revisions_fd = os.open(parent / ".revisions", os.O_RDONLY | os.O_DIRECTORY)
        try:
            recovered_record, recovered_sha = telegram_publisher._transfer_record(
                revisions_fd, recovery_archives[0].name, uid=os.geteuid(),
            )
        finally:
            os.close(revisions_fd)
        assert recovered_record == old_transfer
        assert recovered_sha == previous_ready_sha
        assert published["recovery_cleanup"]["archived_candidates"] == 1
    else:
        assert not (parent / candidate_name).exists()
        assert published["recovery_cleanup"]["reclaimed_bytes"] > 0

    before_revisions = sorted(path.name for path in (parent / ".revisions").iterdir())
    repeated = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )
    assert repeated["status"] == "already_ready"
    assert sorted(path.name for path in (parent / ".revisions").iterdir()) == before_revisions
    assert repeated["recovery_cleanup"]["removed_artifacts"] == 0


def test_publisher_recovers_empty_candidate_after_cleanup_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    old_media_sha = hashlib.sha256(b"interrupted-cleanup-revision").hexdigest()
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, old_media_sha,
    )
    candidate = parent / candidate_name
    candidate.mkdir(mode=0o700)
    marker = {
        "source_id": source_id, "source_hashes": source_hashes,
        "media_manifest_sha256": old_media_sha,
    }
    (candidate / ".publisher-temp.json").write_text(
        json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8",
    )
    (candidate / ".publisher-temp.json").chmod(0o600)
    (candidate / "messages.jsonl").write_bytes(b"partial-candidate")
    (candidate / "messages.jsonl").chmod(0o600)

    candidate_info = candidate.stat()
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    parent_info = os.fstat(parent_fd)
    real_rmdir = os.rmdir
    interrupted = False

    def interrupt_final_rmdir(name, *, dir_fd=None):
        nonlocal interrupted
        if (not interrupted and dir_fd is not None and name == candidate_name
                and (os.fstat(dir_fd).st_dev, os.fstat(dir_fd).st_ino)
                == (parent_info.st_dev, parent_info.st_ino)):
            info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
            if (info.st_dev, info.st_ino) == (candidate_info.st_dev, candidate_info.st_ino):
                interrupted = True
                raise OSError(errno.EIO, "synthetic crash after candidate contents removed")
        return real_rmdir(name, dir_fd=dir_fd)

    monkeypatch.setattr(telegram_publisher.os, "rmdir", interrupt_final_rmdir)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(result["publisher_config"]),
        )
    assert interrupted
    assert candidate.is_dir()
    assert list(candidate.iterdir()) == []
    assert stage.is_dir()

    monkeypatch.setattr(telegram_publisher.os, "rmdir", real_rmdir)
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )
    os.close(parent_fd)
    assert recovered["status"] == "already_ready"
    assert not candidate.exists()
    assert stage.is_dir()


def test_publisher_recovers_current_candidate_after_cleanup_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    desired_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, desired_media_sha,
    )
    candidate = parent / candidate_name
    candidate.mkdir(mode=0o700)
    marker = {
        "source_id": source_id, "source_hashes": source_hashes,
        "media_manifest_sha256": desired_media_sha,
    }
    (candidate / ".publisher-temp.json").write_text(
        json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8",
    )
    (candidate / ".publisher-temp.json").chmod(0o600)
    (candidate / "messages.jsonl").write_bytes(b"partial-current-candidate")
    (candidate / "messages.jsonl").chmod(0o600)

    candidate_info = candidate.stat()
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    parent_info = os.fstat(parent_fd)
    real_rmdir = os.rmdir
    real_fsync = os.fsync
    real_unlink = os.unlink
    candidate_events: list[str] = []
    interrupted = False

    def capture_candidate_fsync(fd):
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode) and (info.st_dev, info.st_ino) == (
                candidate_info.st_dev, candidate_info.st_ino):
            candidate_events.append("fsync")
        return real_fsync(fd)

    def capture_marker_unlink(name, *, dir_fd=None):
        if name == ".publisher-temp.json" and dir_fd is not None:
            info = os.fstat(dir_fd)
            if (info.st_dev, info.st_ino) == (candidate_info.st_dev, candidate_info.st_ino):
                candidate_events.append("marker_unlink")
        return real_unlink(name, dir_fd=dir_fd)

    def interrupt_final_rmdir(name, *, dir_fd=None):
        nonlocal interrupted
        if (not interrupted and dir_fd is not None and name == candidate_name
                and (os.fstat(dir_fd).st_dev, os.fstat(dir_fd).st_ino)
                == (parent_info.st_dev, parent_info.st_ino)):
            info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
            if (info.st_dev, info.st_ino) == (candidate_info.st_dev, candidate_info.st_ino):
                interrupted = True
                raise OSError(errno.EIO, "synthetic crash after current marker removal")
        return real_rmdir(name, dir_fd=dir_fd)

    monkeypatch.setattr(telegram_publisher.os, "rmdir", interrupt_final_rmdir)
    monkeypatch.setattr(telegram_publisher.os, "fsync", capture_candidate_fsync)
    monkeypatch.setattr(telegram_publisher.os, "unlink", capture_marker_unlink)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(result["publisher_config"]),
        )
    assert interrupted
    marker_index = candidate_events.index("marker_unlink")
    assert "fsync" in candidate_events[:marker_index]
    assert "fsync" in candidate_events[marker_index + 1:]
    assert candidate.is_dir() and list(candidate.iterdir()) == []
    assert stage.is_dir()
    ready_before = hashlib.sha256(
        (stage / "telegram_transfer_manifest.json").read_bytes()
    ).hexdigest()

    monkeypatch.setattr(telegram_publisher.os, "rmdir", real_rmdir)
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )
    os.close(parent_fd)
    assert recovered["status"] == "ready"
    assert not candidate.exists()
    current = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    assert current["media_manifest_sha256"] == desired_media_sha
    assert hashlib.sha256((stage / "telegram_transfer_manifest.json").read_bytes()).hexdigest() != ready_before
    assert (parent / ".revisions" / f"revision-{ready_before}").is_dir()
    assert telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )["status"] == "already_ready"


def test_publisher_preserves_nonempty_markerless_current_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    desired_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, desired_media_sha,
    )
    candidate = parent / candidate_name
    candidate.mkdir(mode=0o700)
    payload = candidate / "uncertain.payload"
    payload.write_bytes(b"preserve data without verifiable provenance")
    payload.chmod(0o600)
    before = payload.read_bytes()
    ready_before = (stage / "telegram_transfer_manifest.json").read_bytes()

    for _ in range(2):
        with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
            telegram_publisher.handle(
                "copy", source_id, config_path=str(result["publisher_config"]),
            )
        assert candidate.is_dir()
        assert payload.read_bytes() == before
        assert (stage / "telegram_transfer_manifest.json").read_bytes() == ready_before


def test_publisher_preserves_nonempty_markerless_candidate_and_other_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    unknown_sha = hashlib.sha256(b"unknown markerless candidate").hexdigest()
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, unknown_sha,
    )
    candidate = parent / candidate_name
    candidate.mkdir(mode=0o700)
    payload = candidate / "unverified.payload"
    payload.write_bytes(b"keep: provenance cannot be established")
    payload.chmod(0o600)
    payload_before = payload.read_bytes()

    other_source_id = f"telegram:{result['chat']}:topic:999"
    other_media_sha = hashlib.sha256(b"other-source candidate").hexdigest()
    other_name = telegram_publisher._transfer_candidate_name(
        other_source_id, source_hashes, other_media_sha,
    )
    other_candidate = parent / other_name
    other_candidate.mkdir(mode=0o700)
    other_marker = {
        "source_id": other_source_id,
        "source_hashes": source_hashes,
        "media_manifest_sha256": other_media_sha,
    }
    marker_path = other_candidate / ".publisher-temp.json"
    marker_path.write_text(json.dumps(other_marker, sort_keys=True) + "\n", encoding="utf-8")
    marker_path.chmod(0o600)
    other_payload = other_candidate / "other-source.payload"
    other_payload.write_bytes(b"preserve another source")
    other_payload.chmod(0o600)
    other_payload_before = other_payload.read_bytes()

    ready_before = (stage / "telegram_transfer_manifest.json").read_bytes()
    messages_before = (stage / "messages.jsonl").read_bytes()
    services_before = (stage / "services.jsonl").read_bytes()
    for _attempt in range(2):
        with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
            telegram_publisher.handle(
                "copy", source_id, config_path=str(result["publisher_config"]),
            )
        assert candidate.is_dir()
        assert payload.read_bytes() == payload_before
        assert not (candidate / ".publisher-temp.json").exists()
        assert other_candidate.is_dir()
        assert other_payload.read_bytes() == other_payload_before
        assert marker_path.is_file()
        assert (stage / "telegram_transfer_manifest.json").read_bytes() == ready_before
        assert (stage / "messages.jsonl").read_bytes() == messages_before
        assert (stage / "services.jsonl").read_bytes() == services_before


def test_publisher_preserves_incomplete_candidate_with_marker_intent_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = transfer["source_hashes"]
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, None,
    )
    init_name = ".publisher-init-" + uuid.uuid4().hex
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    telegram_publisher._write_init_intent(
        parent_fd, init_name, uid=os.geteuid(), gid=os.getegid(),
        source_id=source_id, source_hashes=source_hashes,
        media_manifest_sha=None,
    )
    os.close(parent_fd)

    candidate = parent / candidate_name
    candidate.mkdir(mode=0o700)
    conflicting_hashes = dict(source_hashes)
    conflicting_hashes["messages.jsonl"] = "0" * 64
    conflicting_marker = {
        "source_id": source_id, "source_hashes": conflicting_hashes,
        "media_manifest_sha256": None,
    }
    marker_path = candidate / ".publisher-temp.json"
    marker_path.write_text(json.dumps(conflicting_marker, sort_keys=True) + "\n")
    marker_path.chmod(0o600)
    payload_path = candidate / "messages.jsonl"
    payload_path.write_bytes(b"do-not-delete-uncertain-candidate")
    payload_path.chmod(0o600)
    marker_before = marker_path.read_bytes()
    payload_before = payload_path.read_bytes()
    ready_before = (stage / "telegram_transfer_manifest.json").read_bytes()

    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(result["publisher_config"]),
        )

    assert marker_path.read_bytes() == marker_before
    assert payload_path.read_bytes() == payload_before
    assert (parent / f"{init_name}.json").is_file()
    assert (stage / "telegram_transfer_manifest.json").read_bytes() == ready_before


def test_publisher_other_scope_candidate_does_not_block_current_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    source_id = result["source_id"]
    source_hashes = json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "source_hashes"
    ]
    other_source = source_id.rsplit(":", 1)[0] + ":topic:987"
    other_media_sha = hashlib.sha256(b"other-scope-revision").hexdigest()
    other_name = telegram_publisher._transfer_candidate_name(
        other_source, source_hashes, other_media_sha,
    )
    other_dir = stage.parent / other_name
    other_dir.mkdir(mode=0o700)
    other_marker = {
        "source_id": other_source, "source_hashes": source_hashes,
        "media_manifest_sha256": other_media_sha,
    }
    (other_dir / ".publisher-temp.json").write_text(
        json.dumps(other_marker, sort_keys=True) + "\n",
    )
    (other_dir / ".publisher-temp.json").chmod(0o600)
    other_payload = other_dir / "messages.jsonl"
    other_payload.write_bytes(b"keep-candidate-from-another-scope")
    other_payload.chmod(0o600)
    other_bytes_before = other_payload.read_bytes()

    new_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    published = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )

    assert published["status"] == "ready"
    assert json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "media_manifest_sha256"
    ] == new_media_sha
    assert other_payload.read_bytes() == other_bytes_before
    assert other_dir.is_dir()
    assert published["recovery_cleanup"]["preserved"]["other_source_candidate"] == 1


def test_publisher_complete_candidate_with_marker_for_current_revision_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_id = result["source_id"]
    candidate_name = telegram_publisher._transfer_candidate_name(
        source_id, transfer["source_hashes"], transfer["media_manifest_sha256"],
    )
    candidate = stage.parent / candidate_name
    shutil.copytree(stage, candidate)
    marker = {
        "source_id": source_id, "source_hashes": transfer["source_hashes"],
        "media_manifest_sha256": transfer["media_manifest_sha256"],
    }
    marker_path = candidate / ".publisher-temp.json"
    marker_path.write_text(json.dumps(marker, sort_keys=True) + "\n")
    marker_path.chmod(0o600)
    ready_before = (stage / "telegram_transfer_manifest.json").read_bytes()
    candidate_snapshot = {
        str(path.relative_to(candidate)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in candidate.rglob("*")
        if path.is_file() and path.name != ".publisher-temp.json"
    }

    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )

    assert recovered["status"] == "already_ready"
    assert not candidate.exists()
    assert (stage / "telegram_transfer_manifest.json").is_file()
    assert not (stage / ".publisher-temp.json").exists()
    assert (stage / "telegram_transfer_manifest.json").read_bytes() == ready_before
    assert {
        str(path.relative_to(stage)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in stage.rglob("*") if path.is_file()
    } == candidate_snapshot
    recovery_archives = list((stage.parent / ".revisions").glob("recovery-*"))
    assert len(recovery_archives) == 1
    revisions_fd = os.open(
        stage.parent / ".revisions", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    try:
        archived_record, archived_sha = telegram_publisher._transfer_record(
            revisions_fd, recovery_archives[0].name, uid=os.geteuid(),
        )
    finally:
        os.close(revisions_fd)
    assert archived_record == transfer
    assert archived_sha == hashlib.sha256(ready_before).hexdigest()


def test_publisher_valid_unmarked_candidate_from_other_topic_does_not_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    source_id = result["source_id"]
    other_source = source_id.rsplit(":", 1)[0] + ":topic:17"
    topic_id = 17
    export_root = result["export_dir"].parent.parent
    export_config = json.loads((result["publisher_config"].parent / "export.json").read_text())
    client = _ScopeTelegram(chat=result["chat"], scope="topic", forum=True, count=1)
    export_root_fd = Dir.root(str(export_root))
    try:
        topic_manifest = export(
            export_root_fd, export_config,
            ["export", result["chat"], str(topic_id), "MEDIA", "100", "100MiB"],
            client=client,
        )
        services_manifest = export_services(
            export_root_fd, export_config, result["chat"], topic_id, 100, None,
            client=client,
        )
        assert topic_manifest["complete"] and services_manifest["complete"]
    finally:
        export_root_fd.close()
    topic_stage = result["imports_root"] / result["chat"] / str(topic_id)
    topic_publish = telegram_publisher.handle(
        "copy", other_source, config_path=str(result["publisher_config"]),
    )
    assert topic_publish["status"] == "ready"
    topic_transfer = json.loads((topic_stage / "telegram_transfer_manifest.json").read_text())
    other_candidate_name = telegram_publisher._transfer_candidate_name(
        other_source, topic_transfer["source_hashes"], topic_transfer["media_manifest_sha256"],
    )
    other_candidate = stage.parent / other_candidate_name
    os.rename(topic_stage, other_candidate)
    assert not (other_candidate / ".publisher-temp.json").exists()

    parent_fd = os.open(stage.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        other_status = telegram_publisher._staged_status(
            parent_fd, other_candidate_name, other_source, dest_uid=os.geteuid(),
        )
    finally:
        os.close(parent_fd)
    assert other_status and other_status["status"] == "ready"
    transfer_path = other_candidate / "telegram_transfer_manifest.json"
    message_path = other_candidate / "messages.jsonl"
    other_transfer_before = transfer_path.read_bytes()
    other_messages_before = message_path.read_bytes()

    new_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    published = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )

    assert published["status"] == "ready"
    assert json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "media_manifest_sha256"
    ] == new_media_sha
    assert transfer_path.read_bytes() == other_transfer_before
    assert message_path.read_bytes() == other_messages_before
    assert other_candidate.is_dir()
    assert published["recovery_cleanup"]["preserved"]["other_source_candidate"] == 1


def test_publisher_fsyncs_media_parent_after_creating_message_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source_media_item = source / "media" / "101"
    source_media_item.mkdir(mode=0o700, parents=True)
    source.chmod(0o700)
    (source / "media").chmod(0o700)
    source_media_item.chmod(0o700)
    payload = b"media-parent-durability"
    media_file = source_media_item / "attachment.bin"
    media_file.write_bytes(payload)
    media_file.chmod(0o600)
    receipt = {
        "name": "attachment.bin", "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (source_media_item / "receipt.json").write_text(json.dumps(receipt) + "\n")
    (source_media_item / "receipt.json").chmod(0o600)
    destination = tmp_path / "candidate"
    destination.mkdir(mode=0o700)
    destination.chmod(0o700)
    source_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    destination_fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    media_info = None
    synced_media_parent = False
    original_open_child = telegram_publisher._open_child
    original_fsync = os.fsync

    def capture_media_parent(parent, name, **kwargs):
        nonlocal media_info
        fd = original_open_child(parent, name, **kwargs)
        if name == "media":
            info = os.fstat(fd)
            media_info = (info.st_dev, info.st_ino)
        return fd

    def record_media_parent_fsync(fd):
        nonlocal synced_media_parent
        info = os.fstat(fd)
        if media_info == (info.st_dev, info.st_ino):
            synced_media_parent = True
        return original_fsync(fd)

    monkeypatch.setattr(telegram_publisher, "_open_child", capture_media_parent)
    monkeypatch.setattr(telegram_publisher.os, "fsync", record_media_parent_fsync)
    try:
        staged_path, size, digest = telegram_publisher._copy_media_reference(
            source_fd, destination_fd,
            {
                "path": "media/101/attachment.bin", "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(), "max_file_bytes": 1024,
            },
            101, source_uid=os.geteuid(), dest_uid=os.geteuid(),
            dest_gid=os.getegid(), max_file_bytes=1024,
        )
        assert staged_path == "media/101/attachment.bin"
        assert (size, digest) == (len(payload), hashlib.sha256(payload).hexdigest())
        assert (destination / "media" / "101" / "attachment.bin").read_bytes() == payload
        assert synced_media_parent, "media directory entry for the message must be fsynced"
    finally:
        os.close(source_fd)
        os.close(destination_fd)


def test_publisher_resyncs_revision_archive_before_already_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    old_bytes = (stage / "telegram_transfer_manifest.json").read_bytes()
    old_sha = hashlib.sha256(old_bytes).hexdigest()
    old_revision = parent / ".revisions" / f"revision-{old_sha}"
    new_media_sha = _add_empty_media_resume_revision(result["export_dir"], result["source_id"])
    real_fsync = os.fsync
    fail_revision_sync = True

    def fail_archive_sync_once(fd):
        nonlocal fail_revision_sync
        if os.path.realpath(f"/proc/self/fd/{fd}") == str(parent / ".revisions"):
            if fail_revision_sync:
                fail_revision_sync = False
                raise OSError(errno.EIO, "synthetic revision directory fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(telegram_publisher.os, "fsync", fail_archive_sync_once)
    with pytest.raises((OSError, telegram_publisher.TransferError)):
        telegram_publisher.handle(
            "copy", result["source_id"], config_path=str(result["publisher_config"]),
        )
    assert not fail_revision_sync
    assert (stage / "telegram_transfer_manifest.json").is_file()
    assert old_revision.is_dir()
    assert json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "media_manifest_sha256"
    ] == new_media_sha

    revision_syncs = []

    def record_archive_sync(fd):
        if os.path.realpath(f"/proc/self/fd/{fd}") == str(parent / ".revisions"):
            revision_syncs.append(True)
        return real_fsync(fd)

    monkeypatch.setattr(telegram_publisher.os, "fsync", record_archive_sync)
    retry = telegram_publisher.handle(
        "copy", result["source_id"], config_path=str(result["publisher_config"]),
    )
    assert retry["status"] == "already_ready"
    assert revision_syncs
    assert old_revision.is_dir()


@pytest.mark.parametrize("scenario", ["orphan_marker", "init_intent", "current_temp"])
@pytest.mark.parametrize("failure", ["eio", "access"])
@pytest.mark.parametrize("failure_point", ["transfer_manifest", "temp_marker"])
def test_publisher_preserves_candidates_when_recovery_read_is_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    failure: str,
    failure_point: str,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    parent = stage.parent
    source_id = result["source_id"]
    old_transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    source_hashes = old_transfer["source_hashes"]

    if scenario in {"orphan_marker", "init_intent"}:
        kind = "partial_candidate" if scenario == "orphan_marker" else "complete_candidate"
        candidate_name, _ = _add_interrupted_revision(
            parent, kind=kind, source_id=source_id,
            source_hashes=source_hashes, stage=stage,
        )
        expected_media_sha = None
    else:
        expected_media_sha = _add_empty_media_resume_revision(
            result["export_dir"], source_id,
        )
        candidate_name = telegram_publisher._transfer_candidate_name(
            source_id, source_hashes, expected_media_sha,
        )
        candidate = parent / candidate_name
        candidate.mkdir(mode=0o700)
        marker = {
            "source_id": source_id,
            "source_hashes": source_hashes,
            "media_manifest_sha256": expected_media_sha,
        }
        (candidate / ".publisher-temp.json").write_text(
            json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8",
        )
        (candidate / ".publisher-temp.json").chmod(0o600)
        (candidate / "messages.jsonl").write_bytes(b"partial-current-candidate")
        (candidate / "messages.jsonl").chmod(0o600)
    candidate = parent / candidate_name
    candidate_info = candidate.stat()
    candidate_message_before = (candidate / "messages.jsonl").read_bytes()
    previous_manifest = (stage / "telegram_transfer_manifest.json").read_bytes()
    original_read_file = telegram_publisher._read_file

    target_file = (
        "telegram_transfer_manifest.json"
        if failure_point == "transfer_manifest" else ".publisher-temp.json"
    )

    def fail_recovery_read(directory_fd, name, **kwargs):
        info = os.fstat(directory_fd)
        if (name == target_file
                and (info.st_dev, info.st_ino)
                == (candidate_info.st_dev, candidate_info.st_ino)):
            if failure == "eio":
                raise OSError(errno.EIO, "synthetic read failure")
            raise PermissionError(errno.EACCES, "synthetic access failure")
        return original_read_file(directory_fd, name, **kwargs)

    monkeypatch.setattr(telegram_publisher, "_read_file", fail_recovery_read)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_RECOVERY_REQUIRED"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(result["publisher_config"]),
        )

    assert candidate.is_dir()
    assert (candidate / "messages.jsonl").read_bytes() == candidate_message_before
    assert (stage / "telegram_transfer_manifest.json").read_bytes() == previous_manifest
    if scenario == "init_intent":
        assert list(parent.glob(".publisher-init-*.json"))


def test_publisher_keeps_previous_ready_revision_if_new_candidate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    source_id = result["source_id"]
    old_transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    _add_interrupted_revision(
        stage.parent, kind="before_directory", source_id=source_id,
        source_hashes=old_transfer["source_hashes"], stage=stage,
    )
    new_media_sha = _add_empty_media_resume_revision(result["export_dir"], source_id)
    old_bytes = (stage / "telegram_transfer_manifest.json").read_bytes()
    write_file = telegram_publisher._write_file

    def fail_candidate(parent_fd, name, data, **kwargs):
        if name == "telegram_transfer_manifest.json":
            raise OSError("simulated crash before candidate completion")
        return write_file(parent_fd, name, data, **kwargs)

    monkeypatch.setattr(telegram_publisher, "_write_file", fail_candidate)
    with pytest.raises(OSError, match="simulated crash before candidate completion"):
        telegram_publisher.handle(
            "copy", source_id, config_path=str(result["publisher_config"]),
        )
    assert (stage / "telegram_transfer_manifest.json").read_bytes() == old_bytes
    assert json.loads(old_bytes)["media_manifest_sha256"] is None

    monkeypatch.setattr(telegram_publisher, "_write_file", write_file)
    retried = telegram_publisher.handle(
        "copy", source_id, config_path=str(result["publisher_config"]),
    )
    assert retried["status"] == "ready"
    assert json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "media_manifest_sha256"
    ] == new_media_sha


def test_publisher_cleanup_is_source_scoped_and_restores_storage_quota(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    result = _run_scope_pipeline(
        tmp_path, scope="group", forum=False, count=1, monkeypatch=monkeypatch,
    )
    stage = result["stage"]
    source_id = result["source_id"]
    source_hashes = json.loads((stage / "telegram_transfer_manifest.json").read_text())[
        "source_hashes"
    ]
    parent = stage.parent
    old_media_sha = hashlib.sha256(b"old-media-revision").hexdigest()
    stale_name = telegram_publisher._transfer_candidate_name(
        source_id, source_hashes, old_media_sha,
    )
    marker = {
        "source_id": source_id, "source_hashes": source_hashes,
        "media_manifest_sha256": old_media_sha,
    }
    stale = parent / stale_name
    stale.mkdir(mode=0o700)
    (stale / ".publisher-temp.json").write_text(json.dumps(marker, sort_keys=True) + "\n")
    (stale / ".publisher-temp.json").chmod(0o600)
    stale_marker_size = (stale / ".publisher-temp.json").stat().st_size
    payload = b"stale-private-candidate" * 4096
    (stale / "messages.jsonl").write_bytes(payload)
    (stale / "messages.jsonl").chmod(0o600)

    other_source = source_id.rsplit(":", 1)[0] + ":topic:987"
    other_hashes = dict(source_hashes)
    other_media_sha = hashlib.sha256(b"another-source-revision").hexdigest()
    other_name = telegram_publisher._transfer_candidate_name(
        other_source, other_hashes, other_media_sha,
    )
    other_dir = parent / other_name
    other_dir.mkdir(mode=0o700)
    other_marker = {
        "source_id": other_source, "source_hashes": other_hashes,
        "media_manifest_sha256": other_media_sha,
    }
    (other_dir / ".publisher-temp.json").write_text(
        json.dumps(other_marker, sort_keys=True) + "\n",
    )
    (other_dir / ".publisher-temp.json").chmod(0o600)
    (other_dir / "messages.jsonl").write_bytes(b"keep-other-source")
    (other_dir / "messages.jsonl").chmod(0o600)

    config = telegram_publisher._safe_config(str(result["publisher_config"]))
    current_temp_name = telegram_publisher._transfer_candidate_name(source_id, source_hashes, None)
    with telegram_publisher._exclusive_lock(config):
        before_available = telegram_publisher._available(config)[0]
        destination_root = telegram_publisher._open_root(config["destination_root"])
        try:
            chat_fd = telegram_publisher._open_child(
                destination_root, result["chat"], uid=config["destination_uid"],
            )
            try:
                recovery = telegram_publisher._recover_initialization_directories(
                    chat_fd, uid=config["destination_uid"], gid=config["destination_gid"],
                    source_id=source_id, source_hashes=source_hashes,
                    media_manifest_sha=None, temp_name=current_temp_name,
                )
            finally:
                os.close(chat_fd)
        finally:
            os.close(destination_root)
        after_available = telegram_publisher._available(config)[0]

    assert recovery["removed_artifacts"] == 1
    assert recovery["reclaimed_bytes"] == len(payload) + stale_marker_size
    assert after_available - before_available == recovery["reclaimed_bytes"]
    assert not stale.exists()
    assert (other_dir / "messages.jsonl").read_bytes() == b"keep-other-source"
    assert recovery["preserved"]["other_source_candidate"] == 1


class _LargeHistoryTelegram(_ScopeTelegram):
    def __init__(self, chat: str, total: int, text_bytes: int):
        super().__init__(chat=chat, scope="group", forum=False, count=total)
        self.total = total
        self.text = "x" * text_bytes
        self.page_calls = 0

    def page(self, _chat, _topic, limit, before):
        self.page_calls += 1
        high = self.total if before is None else min(before - 1, self.total)
        if high <= 0:
            return []
        low = max(1, high - limit + 1)
        return [{
            "id": message_id, "date": "2026-01-01T00:00:00Z",
            "sender": "Synthetic author", "senderId": 42, "text": self.text,
            "media": None,
        } for message_id in range(high, low - 1, -1)]


def test_history_over_32_mib_stays_streamed_through_publisher_and_broker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    optional_disposable_postgres_credentials: dict[str, str] | None,
):
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
    chat = f"-100{uuid.uuid4().int % 10**12 + 1}"
    total = 9000
    text_size = 8192
    data_root = tmp_path / "data"
    export_root = data_root / "exports"
    app_root = data_root / "application"
    imports_root = app_root / "imports"
    config_root = tmp_path / "config"
    lock_root = data_root / "run"
    for directory in (data_root, export_root, app_root, imports_root, config_root, lock_root):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
    (imports_root / ".broker").mkdir(mode=0o700)
    (app_root / "broker").mkdir(mode=0o700)
    storage_limit = 1024**3
    export_config = {"storage_limit_bytes": storage_limit, "min_free_bytes": 0}
    export_config_path = config_root / "export.json"
    publisher_config_path = config_root / "publisher.json"
    _write_config(export_config_path, export_config, 0o640)
    _write_config(publisher_config_path, {
        "version": 1, "data_root": str(data_root), "source_root": str(export_root),
        "destination_root": str(imports_root), "export_config": str(export_config_path),
        "lock_path": str(lock_root / "publisher.lock"), "expected_data_uuid": None,
        "require_mount": False, "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0, "source_uid": os.geteuid(), "source_gid": os.getegid(),
        "destination_uid": os.geteuid(), "destination_gid": os.getegid(),
    }, 0o600)
    client = _LargeHistoryTelegram(chat, total, text_size)
    root = Dir.root(str(export_root))
    tracemalloc.start()
    try:
        manifest = export(
            root, export_config, ["export", chat, "GROUP", "1000"], client=client,
        )
        services = export_services(root, export_config, chat, None, 1000, None, client=client)
        _current, export_peak = tracemalloc.get_traced_memory()
        assert export_peak < 64 * 1024**2
    finally:
        root.close()
    try:
        export_dir = export_root / chat / "group"
        message_path = export_dir / "messages.jsonl"
        size = message_path.stat().st_size
        assert size > 70 * 1024**2
        # Each stage must stay below the complete input size; 64 MiB leaves
        # headroom for bounded 1,000-message exporter pages but catches a
        # whole-file allocation of this >70 MiB history.
        expected_sha = hashlib.file_digest(message_path.open("rb"), "sha256").hexdigest()
        assert manifest["message_count"] == total
        assert services["complete"] is True
        tracemalloc.reset_peak()
        assert inspect_export(export_dir, root=export_root).message_count == total
        _current, inspect_peak = tracemalloc.get_traced_memory()
        assert inspect_peak < 64 * 1024**2

        tracemalloc.reset_peak()
        published = telegram_publisher.handle("copy", f"telegram:{chat}:group", config_path=str(publisher_config_path))
        assert published["status"] == "ready"
        stage = imports_root / chat / "group"
        transfer = json.loads(stage.joinpath("telegram_transfer_manifest.json").read_text())
        assert transfer["source_hashes"]["messages.jsonl"] == expected_sha
        staged_sha = hashlib.file_digest(stage.joinpath("messages.jsonl").open("rb"), "sha256").hexdigest()
        assert transfer["staged_hashes"]["messages.jsonl"] == staged_sha
        assert stage.joinpath("messages.jsonl").stat().st_size > 32 * 1024**2
        _current, publisher_peak = tracemalloc.get_traced_memory()
        assert publisher_peak < 64 * 1024**2

        submissions = []

        def worker_api(action, **_kwargs):
            if action == "project-catalog":
                return {"projects": [{"slug": PROJECT, "title": "Synthetic project"}]}
            if action == "source-overview":
                return {"documents": total}
            raise AssertionError(action)

        def submit(argv, *, idempotency_key):
            submissions.append(argv)
            return {"job_id": str(uuid.uuid4())}

        # This synthetic run verifies streaming behavior, not the deployment's
        # shared-storage reserve. Production retains the 5 GiB reserve.
        monkeypatch.setattr(broker, "RESERVE", 0)
        tracemalloc.reset_peak()
        app = broker.Broker(app_root, {}, worker_api=worker_api, submit=submit,
                            authorized_projects={PROJECT})
        try:
            app.handle("large-history-owner", "source-add", f"telegram:{chat}:group", PROJECT)
            request = app.handle("large-history-owner", "ingest-dry-run", f"telegram:{chat}:group", PROJECT)
            staged = Path(submissions[-1][submissions[-1].index("--job") + 1])
            staged_path = staged / "messages.jsonl"
            assert request["request_id"] and staged_path.stat().st_size > 32 * 1024**2
            assert hashlib.file_digest(staged_path.open("rb"), "sha256").hexdigest() == staged_sha
            with staged_path.open("rb") as stream:
                count = sum(1 for _ in stream)
            assert count == total
        finally:
            app.db.close()
        _current, peak = tracemalloc.get_traced_memory()
        # The history itself exceeds 70 MiB. Measure this stage independently
        # so either publisher or broker buffering the whole JSONL is caught.
        assert peak < 64 * 1024**2
    finally:
        tracemalloc.stop()
    test_env = os.environ.get("KIBER_ANALYST_INGEST_TEST_ENV")
    if test_env and Path(test_env).is_file():
        from kiber_analyst.telegram_ingest import load_telegram_export

        tracemalloc.start()
        try:
            dry = load_telegram_export(
                staged, tmp_path / "large-history-dry", env_path=Path(test_env),
                project_slug=PROJECT, dry_run=True, root=imports_root,
            )
            assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
            loaded = load_telegram_export(
                staged, tmp_path / "large-history-load", env_path=Path(test_env),
                project_slug=PROJECT, root=imports_root,
            )
            repeated = load_telegram_export(
                staged, tmp_path / "large-history-repeat", env_path=Path(test_env),
                project_slug=PROJECT, root=imports_root,
            )
            assert loaded["inserted"] == total and loaded["errors"] == 0
            assert repeated["inserted"] == 0 and repeated["skipped"] == total
            _current, ingest_peak = tracemalloc.get_traced_memory()
            assert ingest_peak < 48 * 1024**2
        finally:
            tracemalloc.stop()
