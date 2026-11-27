from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from kiber_analyst.telegram_exporter import (
    DEFAULT_FILE_BYTES,
    MAX_FILE_BYTES,
    Dir,
    export,
    export_resume,
    export_services,
)
from kiber_analyst.telegram_publisher import handle as publish


class _PartialMediaClient:
    topic = 17
    sizes = {501: 2, 502: 100 * 1024**2 + 1}

    def __init__(self, chat: str):
        self.chat = chat
        self.pages = 0
        self.service_pages = 0

    def info(self, _chat):
        return {"id": int(self.chat), "forum": True}

    def title(self, _chat, _topic):
        return "Synthetic partial-resume topic"

    def page(self, _chat, _topic, _limit, _before):
        self.pages += 1
        if self.pages > 1:
            return []
        return [
            {
                "id": mid,
                "date": "2026-01-01T00:00:00Z",
                "sender": "Synthetic author",
                "senderId": 9,
                "text": f"Synthetic item {mid}",
                "full": {"replyTo": {"forumTopic": True, "topMsgId": self.topic}},
                "media": {
                    "kind": "document", "size": size,
                    "size_source": "document.size", "file_name": f"item-{mid}.bin",
                },
            }
            for mid, size in self.sizes.items()
        ]

    def media_metadata(self, _chat, message_ids, peer_type='supergroup'):
        return {
            mid: {"document": {"_": "document", "size": self.sizes[mid]}}
            for mid in message_ids
        }

    def service_page(self, _chat, _topic, _limit, _before):
        self.service_pages += 1
        if self.service_pages > 1:
            return []
        return [{
            "id": self.topic,
            "date": "2026-01-01T00:00:00Z",
            "sender": "Synthetic moderator",
            "senderId": 10,
            "text": "",
            "action": {"_": "messageActionTopicCreate", "title": "Synthetic topic"},
        }]

    def download(self, _chat, message_id, directory, _file_limit):
        name = f"item-{message_id}.bin"
        fd = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory.fd,
        )
        try:
            if message_id == 501:
                os.write(fd, bytes([message_id % 251]) * self.sizes[message_id])
            else:
                # A sparse fixture exercises large-file streaming without allocating
                # the payload in Python memory or contacting Telegram.
                os.ftruncate(fd, self.sizes[message_id])
        finally:
            os.close(fd)


def _private_write(path: Path, payload: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)
    path.parent.chmod(0o700)


def _transfer_media_records(stage: Path) -> list[dict]:
    transfer = json.loads((stage / "telegram_transfer_manifest.json").read_text())
    if transfer.get("version") == 1:
        return transfer["media_files"]
    return [json.loads(line) for line in (stage / "media_files.jsonl").read_text().splitlines()]


def build_partial_media_revisions(
    tmp_path: Path,
    *,
    after_first_stage: Callable[[Path], None] | None = None,
    chat_id: str | None = None,
) -> dict[str, Path | str]:
    """Build initial, A-only, and cumulative A+B immutable publisher revisions."""
    if os.geteuid() == 0:
        pytest.skip("publisher integration tests require a non-root user")
    chat = chat_id or f"-100{uuid.uuid4().int % 10**15 + 1}"
    topic = _PartialMediaClient.topic
    source_id = f"telegram:{chat}:topic:{topic}"
    data_root = tmp_path / "pipeline-data"
    export_root = data_root / "exports"
    application_root = data_root / "application"
    import_root = application_root / "imports"
    config_root = tmp_path / "pipeline-config"
    lock_root = data_root / "locks"
    for path in (data_root, export_root, application_root, import_root, config_root, lock_root):
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)

    export_config_path = config_root / "export.json"
    publisher_config_path = config_root / "publisher.json"
    storage_limit = 1024**3
    exporter_config = {
        "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0,
    }
    _private_write(export_config_path, json.dumps(exporter_config).encode(), 0o640)
    gid = os.getegid()
    publisher_config = {
        "version": 1,
        "data_root": str(data_root),
        "source_root": str(export_root),
        "destination_root": str(import_root),
        "export_config": str(export_config_path),
        "lock_path": str(lock_root / "publisher.lock"),
        "expected_data_uuid": None,
        "require_mount": False,
        "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0,
        "source_uid": os.geteuid(),
        "source_gid": gid,
        "destination_uid": os.geteuid(),
        "destination_gid": gid,
    }
    _private_write(publisher_config_path, json.dumps(publisher_config).encode())

    client = _PartialMediaClient(chat)
    root = Dir.root(str(export_root))
    try:
        initial_manifest = export(
            root, exporter_config,
            ["export", chat, str(topic), "MEDIA", "100", "1B"],
            client=client,
        )
        services = export_services(root, exporter_config, chat, topic, 100, None, client=client)
        assert initial_manifest["complete"] is True
        assert services["complete"] is True
        original_controls = {
            name: (export_root / chat / str(topic) / name).read_bytes()
            for name in ("messages.jsonl", "services.jsonl", "manifest.json", "services_manifest.json")
        }
        first_resume = export_resume(
            root, exporter_config, chat, topic, 100, DEFAULT_FILE_BYTES, client,
        )
        assert first_resume["status"] == "complete"
        first_publish = publish(
            "copy", source_id, config_path=str(publisher_config_path),
        )
        first_stage = import_root / chat / str(topic)
        assert first_publish["status"] == "ready"
        assert [item["message_id"] for item in _transfer_media_records(first_stage)] == [501]
        if after_first_stage is not None:
            after_first_stage(first_stage)

        second_resume = export_resume(
            root, exporter_config, chat, topic, 100, MAX_FILE_BYTES, client,
        )
        assert second_resume["status"] == "complete"
        second_publish = publish(
            "copy", source_id, config_path=str(publisher_config_path),
        )
        final_stage = import_root / chat / str(topic)
        assert second_publish["status"] == "ready"
        final_files = _transfer_media_records(final_stage)
        assert sorted(item["message_id"] for item in final_files) == [501, 502]
        assert (final_stage / "media/501/item-501.bin").is_file()
        assert (final_stage / "media/502/item-502.bin").is_file()
        revisions = import_root / chat / ".revisions"
        assert revisions.is_dir()
        archived_stage = next(revisions.iterdir())
        assert (archived_stage / "media/501/item-501.bin").is_file()
        assert {
            name: (export_root / chat / str(topic) / name).read_bytes()
            for name in original_controls
        } == original_controls
    finally:
        root.close()

    return {
        "source_id": source_id,
        "chat": chat,
        "topic": str(topic),
        "export_root": export_root,
        "import_root": import_root,
        "application_root": application_root,
        "first_stage": archived_stage,
        "final_stage": import_root / chat / str(topic),
        "publisher_config": publisher_config_path,
    }
