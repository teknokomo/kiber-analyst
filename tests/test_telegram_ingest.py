from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from kiber_analyst.config_paths import data_path
from kiber_analyst.errors import LoadError
from kiber_analyst.telegram_ingest import TELEGRAM_ROOT, inspect_export, prepare_export

CHAT_ID = "-10012345"
TOPIC_ID = 1
SOURCE_ID = f"telegram:{CHAT_ID}:topic:{TOPIC_ID}"


def test_default_import_root_uses_kiber_managed_staging_area() -> None:
    assert TELEGRAM_ROOT == data_path("imports")


def _write_private(path: Path, data: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o600)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _make_export(root: Path, *, media: bool = False) -> Path:
    topic = root / CHAT_ID / str(TOPIC_ID)
    topic.mkdir(mode=0o700, parents=True)
    topic.chmod(0o700)
    if media:
        media_path = "media/101/audio.ogg"
        _write_private(topic / media_path, b"fixture audio")
        message_media = {
            "type": "voice_message",
            "path": media_path,
            "export_status": "downloaded",
            "sha256": hashlib.sha256(b"fixture audio").hexdigest(),
            "size": len(b"fixture audio"),
        }
    else:
        message_media = None
    message = {
        "chat_id": CHAT_ID,
        "date": "2024-02-03T04:05:06+03:00",
        "message_id": 101,
        "media": message_media,
        "reply_to_message_id": None,
        "sender_id": 7,
        "sender_name": "Fixture Author",
        "text": "Проверочное сообщение",
        "topic_id": TOPIC_ID,
    }
    messages = _json_bytes(message)
    service_event = {
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
        "topic_id": TOPIC_ID,
        "type": "topic_created",
    }
    services = _json_bytes(service_event)
    _write_private(topic / "messages.jsonl", messages)
    _write_private(topic / "services.jsonl", services)
    (topic / ".pages").mkdir(mode=0o700)
    (topic / ".pages" / "page.json").write_bytes(b"untouched page checkpoint")
    (topic / ".pages" / "page.json").chmod(0o600)
    manifest = {
        "chat_id": CHAT_ID,
        "complete": True,
        "export_version": 1,
        "messages_sha256": hashlib.sha256(messages).hexdigest(),
        "message_count": 1,
        "scope_before": None,
        "source_id": SOURCE_ID,
        "topic_id": TOPIC_ID,
        "topic_title": "Fixture topic",
    }
    service_manifest = {
        "chat_id": int(CHAT_ID),
        "complete": True,
        "errors": [],
        "event_count": 1,
        "range_complete": True,
        "scope_before": None,
        "services_sha256": hashlib.sha256(services).hexdigest(),
        "source_id": SOURCE_ID,
        "topic_id": TOPIC_ID,
    }
    _write_private(topic / "manifest.json", _json_bytes(manifest))
    _write_private(topic / "services_manifest.json", _json_bytes(service_manifest))
    return topic


def _source_hashes(topic: Path) -> dict[str, str]:
    return {
        str(path.relative_to(topic)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(topic.rglob("*"))
        if path.is_file()
    }


def test_prepare_export_normalizes_data_without_mutating_source(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    before = _source_hashes(topic)

    artifact_dir, export, media_missing = prepare_export(
        topic, tmp_path / "artifacts" / "job-1", root=root
    )

    assert export.message_count == 1
    assert export.service_event_count == 1
    assert media_missing == 0
    normalized = artifact_dir / "normalized" / "messages.jsonl"
    record = json.loads(normalized.read_text(encoding="utf-8"))
    assert record["chat_id"] == CHAT_ID
    assert record["message_id"] == "101"
    assert record["topic_id"] == "1"
    assert record["attribution"] == "scope"
    assert record["url"].endswith("/1/101")
    assert _source_hashes(topic) == before


def test_sender_id_survives_when_export_has_no_sender_name(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message.pop("sender_name")
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages).hexdigest()
    _write_private(manifest_path, _json_bytes(manifest))

    artifact_dir, _, _ = prepare_export(
        topic, tmp_path / "artifacts" / "job-no-name", root=root
    )
    record = json.loads(
        (artifact_dir / "normalized" / "messages.jsonl").read_text(encoding="utf-8")
    )

    assert record["author"] is None
    assert record["author_id"] == "7"


def test_import_key_ignores_manifest_export_timestamp(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    first = inspect_export(topic, root=root)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["exported_at"] = "later"
    _write_private(manifest_path, _json_bytes(manifest))
    second = inspect_export(topic, root=root)

    assert first.manifest_sha256 != second.manifest_sha256
    assert first.import_key == second.import_key


def test_media_metadata_is_checked_and_missing_media_counted(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    before = _source_hashes(topic)
    _, _, media_missing = prepare_export(topic, tmp_path / "artifacts" / "job-media", root=root)
    record = json.loads(
        (tmp_path / "artifacts" / "job-media" / "normalized" / "messages.jsonl").read_text()
    )

    assert media_missing == 0
    assert record["media_type"] == "voice_message"
    assert record["media_path"] == "media/101/audio.ogg"
    assert _source_hashes(topic) == before


def test_exporter_media_kind_is_normalized_to_media_type(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"]["kind"] = message["media"].pop("type")
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages).hexdigest()
    _write_private(manifest_path, _json_bytes(manifest))

    artifact_dir, _, media_missing = prepare_export(
        topic, tmp_path / "artifacts" / "job-media-kind", root=root
    )
    record = json.loads(
        (artifact_dir / "normalized" / "messages.jsonl").read_text(encoding="utf-8")
    )

    assert media_missing == 0
    assert record["media_type"] == "voice_message"
    assert record["media_path"] == "media/101/audio.ogg"


def test_incomplete_service_export_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    manifest_path = topic / "services_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["complete"] = False
    _write_private(manifest_path, _json_bytes(manifest))

    with pytest.raises(LoadError, match="service export is incomplete"):
        inspect_export(topic, root=root)


def test_export_rejects_traversal_and_symlinked_topic(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)

    with pytest.raises(LoadError, match="traversal-free"):
        inspect_export(root / CHAT_ID / ".." / "1", root=root)
    symlink = root / CHAT_ID / "2"
    symlink.symlink_to(topic, target_is_directory=True)
    with pytest.raises(LoadError, match="symbolic link"):
        inspect_export(symlink, root=root)


def test_media_path_traversal_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"] = {"type": "document", "path": "media/101/../../escape"}
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages).hexdigest()
    _write_private(manifest_path, _json_bytes(manifest))

    with pytest.raises(LoadError, match="traversal"):
        prepare_export(topic, tmp_path / "artifacts" / "bad", root=root)
