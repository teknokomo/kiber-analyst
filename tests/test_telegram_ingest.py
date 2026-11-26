from __future__ import annotations

import hashlib
import json
import tracemalloc
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
        (topic / "media").chmod(0o700)
        (topic / "media/101").chmod(0o700)
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


def test_inspect_export_checks_jsonl_size_before_scanning_and_accepts_exact_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.telegram_ingest as telegram_ingest

    root = tmp_path / "telegram"
    topic = _make_export(root)
    messages = topic / "messages.jsonl"
    services = topic / "services.jsonl"
    candidates = (messages, services)
    largest = max(candidates, key=lambda path: path.stat().st_size)
    limit = largest.stat().st_size

    inspected = inspect_export(topic, root=root, max_jsonl_bytes=limit)
    assert inspected.messages_size <= limit
    assert inspected.services_size <= limit

    _write_private(largest, largest.read_bytes() + b"x")
    original_scan = telegram_ingest._iter_jsonl_stream
    scanned_streams = []

    def count_scanned(stream):
        scanned_streams.append(stream)
        yield from original_scan(stream)

    monkeypatch.setattr(telegram_ingest, "_iter_jsonl_stream", count_scanned)
    with pytest.raises(LoadError, match="inspection byte limit"):
        inspect_export(topic, root=root, max_jsonl_bytes=limit)

    expected_scanned_files = 1 if largest == services else 0
    assert len(scanned_streams) == expected_scanned_files


def test_jsonl_reader_bounds_a_newline_free_record_before_allocation() -> None:
    import kiber_analyst.telegram_ingest as telegram_ingest

    class UnboundedInput:
        def __init__(self):
            self.requested = []

        def readline(self, size: int) -> bytes:
            self.requested.append(size)
            return b"x" * size

    stream = UnboundedInput()
    tracemalloc.start()
    try:
        with pytest.raises(LoadError, match="record 1 is oversized"):
            next(telegram_ingest._iter_jsonl_stream(stream))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert stream.requested == [telegram_ingest._MAX_LINE_BYTES + 1]
    assert peak < 8 * 1024**2


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
    assert record["meta"]["media_export"]["size"] == len(b"fixture audio")
    assert record["meta"]["media_export"]["sha256"] == hashlib.sha256(b"fixture audio").hexdigest()
    assert _source_hashes(topic) == before


@pytest.mark.parametrize("replacement", [b"fixture audix", b"fixture audio!"])
def test_regular_media_actual_bytes_must_match_export_size_and_hash(
    tmp_path: Path, replacement: bytes,
) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    media_file = topic / "media/101/audio.ogg"
    _write_private(media_file, replacement)

    with pytest.raises(LoadError, match="size|SHA-256"):
        prepare_export(topic, tmp_path / "artifacts" / "modified-media", root=root)


def test_regular_media_rejects_a_false_declared_sha256(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    message = json.loads((topic / "messages.jsonl").read_text(encoding="utf-8"))
    message["media"]["sha256"] = "a" * 64
    messages = _json_bytes(message)
    _write_private(topic / "messages.jsonl", messages)
    manifest = json.loads((topic / "manifest.json").read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages).hexdigest()
    _write_private(topic / "manifest.json", _json_bytes(manifest))

    with pytest.raises(LoadError, match="SHA-256"):
        prepare_export(topic, tmp_path / "artifacts" / "bad-media-hash", root=root)


def test_legacy_regular_media_without_declared_hash_remains_supported(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"].pop("sha256")
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["messages_sha256"] = hashlib.sha256(messages).hexdigest()
    _write_private(manifest_path, _json_bytes(manifest))

    artifact_dir, _, missing = prepare_export(
        topic, tmp_path / "artifacts" / "legacy-media", root=root,
    )
    normalized = json.loads(
        (artifact_dir / "normalized" / "messages.jsonl").read_text(encoding="utf-8")
    )

    assert missing == 0
    assert normalized["meta"]["media_export"]["sha256"] == hashlib.sha256(
        b"fixture audio"
    ).hexdigest()


def test_media_changed_after_open_is_rejected_before_normalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.telegram_ingest as ingest

    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    verify = ingest._verify_media_stream
    media_file = topic / "media/101/audio.ogg"

    def mutate_open_file(stream, **kwargs):
        with media_file.open("r+b") as writable:
            writable.write(b"X")
            writable.flush()
        return verify(stream, **kwargs)

    monkeypatch.setattr(ingest, "_verify_media_stream", mutate_open_file)
    with pytest.raises(LoadError, match="SHA-256"):
        prepare_export(topic, tmp_path / "artifacts" / "changed-after-open", root=root)


def test_media_record_cannot_exceed_manifest_selected_limit(tmp_path: Path) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root, media=True)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"]["size"] = 101
    message["media"]["max_file_bytes"] = 101
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(messages_sha256=hashlib.sha256(messages).hexdigest(), max_file_bytes=100)
    _write_private(manifest_path, _json_bytes(manifest))

    with pytest.raises(LoadError, match="exceeds its manifest"):
        prepare_export(topic, tmp_path / "artifacts" / "too-large-media-limit", root=root)


@pytest.mark.parametrize("media_root", ["media-resume", "media"])
@pytest.mark.parametrize("version", [1, 2])
def test_resumed_media_uses_checkpoint_limit_and_preserves_voice_type(
    tmp_path: Path, media_root: str, version: int,
) -> None:
    root = tmp_path / "telegram"
    topic = _make_export(root)
    message_path = topic / "messages.jsonl"
    message = json.loads(message_path.read_text(encoding="utf-8"))
    message["media"] = {
        "type": "voice_message", "size": 101, "max_file_bytes": 100,
        "export_status": "skipped", "reason": "over_limit",
    }
    messages = _json_bytes(message)
    _write_private(message_path, messages)
    manifest_path = topic / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        messages_sha256=hashlib.sha256(messages).hexdigest(),
        max_file_bytes=100,
    )
    manifest_bytes = _json_bytes(manifest)
    _write_private(manifest_path, manifest_bytes)

    resume_dir = topic / media_root
    resume_dir.mkdir(mode=0o700)
    media_dir = resume_dir / "101"
    media_dir.mkdir(mode=0o700)
    payload = b"x" * 101
    _write_private(media_dir / "audio.ogg", payload)
    checkpoint_inputs = {
        "messages.jsonl": hashlib.sha256(messages).hexdigest(),
        "services.jsonl": hashlib.sha256((topic / "services.jsonl").read_bytes()).hexdigest(),
        "manifest.json": hashlib.sha256(manifest_bytes).hexdigest(),
        "services_manifest.json": hashlib.sha256(
            (topic / "services_manifest.json").read_bytes()
        ).hexdigest(),
    }
    if version == 1:
        checkpoint_inputs.pop("services_manifest.json")
    checkpoint = {
        "version": version,
        "source_id": SOURCE_ID,
        "inputs": checkpoint_inputs,
        "requested_max_file_bytes": 101,
        "records": {"101": {
            "export_status": "downloaded", "path": f"{media_root}/101/audio.ogg",
            "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
            "max_file_bytes": 101,
        }},
    }
    _write_private(resume_dir / "status.json", _json_bytes(checkpoint))
    (resume_dir / "status.json").chmod(0o600)

    artifact_dir, _, media_missing = prepare_export(
        topic, tmp_path / "artifacts" / "resumed-large-voice", root=root,
    )
    normalized = json.loads(
        (artifact_dir / "normalized" / "messages.jsonl").read_text(encoding="utf-8")
    )

    assert media_missing == 0
    assert normalized["media_type"] == "voice_message"
    assert normalized["media_path"] == f"{media_root}/101/audio.ogg"
    assert normalized["meta"]["media_export"]["max_file_bytes"] == 101


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
