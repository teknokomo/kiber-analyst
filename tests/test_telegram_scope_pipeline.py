from __future__ import annotations

import hashlib
import json
import os
import tracemalloc
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker, telegram_publisher
from kiber_analyst.telegram_exporter import Dir, export, export_services
from kiber_analyst.telegram_ingest import inspect_export, prepare_export

PROJECT = "telegram-scope-fixture"
CHAT = "-1007654321"


class _ScopeTelegram:
    def __init__(self, *, chat: str, scope: str, forum: bool, count: int = 1):
        self.chat = chat
        self.scope = scope
        self.forum = forum
        self.count = count
        self.page_calls = 0
        self.service_calls = 0

    def info(self, _chat):
        return {"id": int(self.chat), "forum": self.forum, "title": "Synthetic group"}

    def title(self, _chat, topic):
        return f"Synthetic topic {topic}" if topic is not None else None

    def page(self, _chat, topic, limit, before):
        self.page_calls += 1
        if self.page_calls > 1:
            return []
        rows = []
        for index in range(self.count):
            message_id = 1000 - index
            actual_topic = 17 + index if self.forum and self.scope == "group" else 17
            full = {"replyTo": {"topMsgId": actual_topic}} if self.forum else {}
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

    def media_metadata(self, _chat, _message_ids):
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
                event["replyTo"] = {"topMsgId": topic_id}
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

    client = _ScopeTelegram(chat=chat, scope=scope, forum=forum, count=count)
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
    assert staged.scope == ("chat" if scope == "group" else "topic")
    message_rows = [json.loads(line) for line in (stage / "messages.jsonl").read_text().splitlines()]
    event_rows = [json.loads(line) for line in (stage / "services.jsonl").read_text().splitlines()]
    assert all((row["topic_id"] is None) == (not forum) for row in message_rows) if scope == "group" else all(
        row["topic_id"] == topic for row in message_rows
    )
    assert len(event_rows) == inspected.service_event_count
    assert all((stage / row["media"]["path"]).is_file() for row in message_rows)

    submissions = []
    monkeypatch.setattr(broker, "RESERVE", 0)

    def worker_api(action, **_kwargs):
        if action == "project-catalog":
            return {"projects": [{"slug": PROJECT, "title": "Synthetic project"}]}
        if action == "source-overview":
            return {"messages": count, "unique_messages": count, "service_events": len(event_rows)}
        raise AssertionError(action)

    def submit(argv, *, idempotency_key):
        submissions.append((argv, idempotency_key))
        return {"job_id": str(uuid.uuid4())}

    app = broker.Broker(
        app_root, {}, worker_api=worker_api, submit=submit, authorized_projects={PROJECT},
    )
    try:
        registration = app.handle("synthetic-owner", "source-add", source_id, PROJECT)
        request = app.handle("synthetic-owner", "ingest-dry-run", source_id, PROJECT)
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
                env_path=Path(test_env), project_slug=PROJECT, dry_run=True,
                root=imports_root,
            )
            assert dry["status"] == "dry_run" and dry["rollback_confirmed"] is True
            loaded = load_telegram_export(
                worker_stage, tmp_path / "artifacts" / "scope-load",
                env_path=Path(test_env), project_slug=PROJECT, root=imports_root,
            )
            repeated = load_telegram_export(
                worker_stage, tmp_path / "artifacts" / "scope-repeat",
                env_path=Path(test_env), project_slug=PROJECT, root=imports_root,
            )
            assert loaded["inserted"] == count
            assert loaded["service_events_inserted"] == len(event_rows)
            assert repeated["inserted"] == 0
            assert repeated["service_events_skipped"] == len(event_rows)
        return {
            "source_id": source_id, "messages": message_rows, "events": event_rows,
            "stage": stage, "sha256": hashlib.sha256((stage / "messages.jsonl").read_bytes()).hexdigest(),
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
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        init_name = ".publisher-init-" + uuid.uuid4().hex
        os.mkdir(init_name, 0o700, dir_fd=parent_fd)
        init_fd = os.open(init_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            fd = os.open(".publisher-temp.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                         0o600, dir_fd=init_fd)
            with os.fdopen(fd, "wb") as stream:
                stream.write((json.dumps(marker, sort_keys=True) + "\n").encode())
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

    # A valid marker lets the publisher discard both pre-manifest crash states;
    # the completed revision remains byte-for-byte unchanged.
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(tmp_path / "config" / "publisher.json"),
    )
    assert recovered["status"] == "already_ready"
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
    synced_parent = []

    def record_directory_fsync(fd):
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode) and info.st_ino == parent_info.st_ino:
            synced_parent.append(info.st_ino)
        return real_fsync(fd)

    import stat

    monkeypatch.setattr(telegram_publisher.os, "fsync", record_directory_fsync)
    recovered = telegram_publisher.handle(
        "copy", source_id, config_path=str(tmp_path / "config" / "publisher.json"),
    )
    assert recovered["status"] == "already_ready"
    assert synced_parent and not temp_dir.exists()
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
    finally:
        root.close()
    try:
        export_dir = export_root / chat / "group"
        message_path = export_dir / "messages.jsonl"
        size = message_path.stat().st_size
        assert size > 32 * 1024**2
        expected_sha = hashlib.file_digest(message_path.open("rb"), "sha256").hexdigest()
        assert manifest["message_count"] == total
        assert services["complete"] is True
        assert inspect_export(export_dir, root=export_root).message_count == total
        published = telegram_publisher.handle("copy", f"telegram:{chat}:group", config_path=str(publisher_config_path))
        assert published["status"] == "ready"
        stage = imports_root / chat / "group"
        transfer = json.loads(stage.joinpath("telegram_transfer_manifest.json").read_text())
        assert transfer["source_hashes"]["messages.jsonl"] == expected_sha
        staged_sha = hashlib.file_digest(stage.joinpath("messages.jsonl").open("rb"), "sha256").hexdigest()
        assert transfer["staged_hashes"]["messages.jsonl"] == staged_sha
        assert stage.joinpath("messages.jsonl").stat().st_size > 32 * 1024**2

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
        assert peak < 96 * 1024**2
    finally:
        tracemalloc.stop()
    test_env = os.environ.get("KIBER_ANALYST_INGEST_TEST_ENV")
    if test_env and Path(test_env).is_file():
        from kiber_analyst.telegram_ingest import load_telegram_export

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
