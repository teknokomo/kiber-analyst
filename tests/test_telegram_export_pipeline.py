from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker, telegram_publisher
from kiber_analyst.telegram_exporter import (
    MAX_FILE_BYTES,
    Dir,
    ExportError,
    _file_limit_bytes,
    export,
    export_services,
)

CHAT = "-10012345"
TOPIC = 17
SOURCE = f"telegram:{CHAT}:topic:{TOPIC}"
PROJECT = "synthetic-project"


class _FakeTelegram:
    def __init__(self, size: int):
        self.size = size
        self.pages = 0
        self.download_limits: list[int] = []

    def info(self, _chat: str) -> dict:
        return {"id": int(CHAT), "forum": True}

    def title(self, _chat: str, _topic: int) -> str:
        return "Synthetic forum topic"

    def page(self, _chat: str, _topic: int, _limit: int, _before: int | None) -> list[dict]:
        self.pages += 1
        if self.pages > 1:
            return []
        return [{
            "id": 501,
            "date": "2026-01-01T00:00:00Z",
            "sender": "Synthetic author",
            "senderId": 9,
            "text": "A synthetic video larger than the ASR local limit",
            "full": {"replyTo": {"topMsgId": TOPIC}},
            "media": {
                "kind": "video", "size": self.size, "size_source": "document.size",
                "file_name": "synthetic-video.bin",
            },
        }]

    def media_metadata(self, _chat: str, _ids: list[int]) -> dict:
        return {}

    def service_page(self, _chat: str, _topic: int, _limit: int, _before: int | None) -> list:
        return []

    def download(self, _chat: str, _message: int, directory: Dir, file_limit: int) -> None:
        self.download_limits.append(file_limit)
        fd = os.open(
            "synthetic-video.bin",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory.fd,
        )
        try:
            os.ftruncate(fd, self.size)
        finally:
            os.close(fd)


class _UnknownSizeTelegram(_FakeTelegram):
    def __init__(self):
        super().__init__(None)

    def download(self, _chat: str, _message: int, directory: Dir, file_limit: int) -> None:
        self.download_limits.append(file_limit)
        fd = os.open(
            "synthetic-video.bin",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory.fd,
        )
        try:
            # Sparse oversized output verifies the selected byte ceiling without
            # allocating the synthetic payload or contacting Telegram.
            os.ftruncate(fd, file_limit + 1)
        finally:
            os.close(fd)


class _InterruptedTelegram:
    def __init__(self):
        self.chat = CHAT
        self.page_calls = 0
        self.fail_second_once = True
        self.payloads = {501: b"first-complete-file", 502: b"second-complete-file"}

    def info(self, _chat: str) -> dict:
        return {"id": int(CHAT), "forum": True}

    def title(self, _chat: str, _topic: int) -> str:
        return "Synthetic interrupted topic"

    def page(self, _chat: str, _topic: int, _limit: int, _before: int | None) -> list[dict]:
        self.page_calls += 1
        if self.page_calls > 1:
            return []
        return [{
            "id": mid,
            "date": "2026-01-01T00:00:00Z",
            "sender": "Synthetic author",
            "senderId": 9,
            "text": f"Synthetic item {mid}",
            "full": {"replyTo": {"topMsgId": TOPIC}},
            "media": {"kind": "document", "size": len(payload), "file_name": f"{mid}.bin"},
        } for mid, payload in self.payloads.items()]

    def media_metadata(self, _chat: str, _ids: list[int]) -> dict:
        return {}

    def service_page(self, _chat: str, _topic: int, _limit: int, _before: int | None) -> list:
        return []

    def download(self, _chat: str, message_id: int, directory: Dir, _file_limit: int) -> None:
        filename = f"{message_id}.bin"
        fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory.fd)
        try:
            if message_id == 501 and self.fail_second_once:
                os.write(fd, b"interrupted partial")
                self.fail_second_once = False
                raise OSError("synthetic interrupted stream")
            os.write(fd, self.payloads[message_id])
        finally:
            os.close(fd)


def _write_config(path: Path, config: dict, mode: int) -> None:
    path.write_text(json.dumps(config, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(mode)


def test_telegram_file_limit_uses_decimal_mb_and_default_mib() -> None:
    assert _file_limit_bytes() == 104_857_600
    assert _file_limit_bytes("100MiB") == 104_857_600
    assert _file_limit_bytes("4000MB") == 4_000_000_000
    assert _file_limit_bytes("4000000000B") == MAX_FILE_BYTES
    with pytest.raises(ExportError):
        _file_limit_bytes("4001MB")


@pytest.mark.parametrize("media_kind", [
    "photo", "video", "audio", "document", "voice", "video_note", "sticker",
    "gif", "animation", "file", "round_video",
])
def test_each_telegram_attachment_kind_uses_selected_limit(media_kind: str) -> None:
    from kiber_analyst.telegram_exporter import media_size_metadata

    within, within_reason = media_size_metadata(
        {"kind": media_kind, "size": 99}, None, 100,
    )
    above, above_reason = media_size_metadata(
        {"kind": media_kind, "size": 101}, None, 100,
    )

    assert within["size_bytes"] == 99 and within_reason is None
    assert above["size_bytes"] == 101 and above_reason == "over_limit"


def test_export_publisher_registration_and_worker_stage_keep_media_over_100_mib(
    tmp_path: Path,
) -> None:
    media_size = 100 * 1024**2 + 1
    data_root = tmp_path / "data"
    export_root = data_root / "exports"
    app_root = data_root / "application"
    import_root = app_root / "imports"
    lock_root = data_root / "run"
    config_root = tmp_path / "config"
    for directory in (data_root, export_root, app_root, import_root, lock_root, config_root):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
    (app_root / "broker").mkdir(mode=0o700)
    (import_root / ".broker").mkdir(mode=0o700)

    storage_limit = 512 * 1024**2
    group = os.getegid()
    exporter_config_path = config_root / "telegram-export.json"
    publisher_config_path = config_root / "telegram-publisher.json"
    _write_config(exporter_config_path, {
        "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0,
    }, 0o640)
    publisher_config = {
        "version": 1,
        "data_root": str(data_root),
        "source_root": str(export_root),
        "destination_root": str(import_root),
        "export_config": str(exporter_config_path),
        "lock_path": str(lock_root / "publisher.lock"),
        "expected_data_uuid": None,
        "require_mount": False,
        "storage_limit_bytes": storage_limit,
        "min_free_bytes": 0,
        "source_uid": os.geteuid(),
        "source_gid": group,
        "destination_uid": os.geteuid(),
        "destination_gid": group,
    }
    _write_config(publisher_config_path, publisher_config, 0o600)

    client = _FakeTelegram(media_size)
    root = Dir.root(str(export_root))
    exporter_config = {"storage_limit_bytes": storage_limit, "min_free_bytes": 0}
    try:
        message_manifest = export(
            root, exporter_config,
            ["export", CHAT, str(TOPIC), "MEDIA", "100", "4000MB"],
            client=client,
        )
        service_manifest = export_services(
            root, exporter_config, CHAT, TOPIC, 100, None, client=client,
        )
        assert message_manifest["complete"] is True
        assert message_manifest["max_file_bytes"] == MAX_FILE_BYTES
        assert service_manifest["complete"] is True
    finally:
        root.close()

    source_path = export_root / CHAT / str(TOPIC)
    exported_message = json.loads((source_path / "messages.jsonl").read_text().splitlines()[0])
    assert client.download_limits == [media_size]
    assert exported_message["media"]["size"] == media_size
    assert exported_message["media"]["requested_max_file_bytes"] == MAX_FILE_BYTES
    assert media_size <= exported_message["media"]["max_file_bytes"] <= MAX_FILE_BYTES
    assert json.loads((source_path / "manifest.json").read_text())["max_file_bytes"] == MAX_FILE_BYTES

    published = telegram_publisher.handle(
        "copy", SOURCE, config_path=str(publisher_config_path),
    )
    assert published["status"] == "ready"
    assert published["downloaded_media"] == 1
    staged_topic = import_root / CHAT / str(TOPIC)
    staged_media = staged_topic / "media" / "501" / "synthetic-video.bin"
    assert staged_media.stat().st_size == media_size
    with staged_media.open("rb") as stream:
        expected_digest = hashlib.file_digest(stream, "sha256").hexdigest()
    staged_message = json.loads((staged_topic / "messages.jsonl").read_text().splitlines()[0])
    assert staged_message["media"]["sha256"] == expected_digest

    projects = {PROJECT: {"slug": PROJECT, "title": "Synthetic project"}}
    submissions: list[tuple[list[str], str]] = []

    def worker_api(action, **_kwargs):
        if action == "project-catalog":
            return {"projects": list(projects.values())}
        if action == "source-overview":
            return {"documents": 0}
        raise AssertionError(f"unexpected worker operation: {action}")

    def submit(argv, *, idempotency_key):
        submissions.append((list(argv), idempotency_key))
        return {"job_id": str(uuid.uuid4())}

    app = broker.Broker(
        app_root, {}, worker_api=worker_api, submit=submit,
        authorized_projects={PROJECT},
    )
    try:
        registration = app.handle("synthetic-owner", "source-add", SOURCE, PROJECT)
        dry_run = app.handle("synthetic-owner", "ingest-dry-run", SOURCE, PROJECT)
        argv = submissions[-1][0]
        worker_stage = Path(argv[argv.index("--job") + 1])
        assert registration["registered"] is True
        assert argv[:2] == ["telegram", "ingest"]
        assert "--dry-run" in argv
        assert worker_stage.is_dir()
        assert (worker_stage / "media/501/synthetic-video.bin").stat().st_size == media_size

        from kiber_analyst.telegram_ingest import prepare_export

        normalized_dir, _metadata, media_missing = prepare_export(
            worker_stage, tmp_path / "normalized", root=import_root,
        )
        normalized = json.loads(
            (normalized_dir / "normalized" / "messages.jsonl").read_text().splitlines()[0]
        )
        assert dry_run["request_id"]
        assert media_missing == 0
        assert normalized["media_path"] == "media/501/synthetic-video.bin"
        assert normalized["meta"]["media_export"]["size"] == media_size
    finally:
        app.db.close()


def test_export_retry_removes_partial_file_and_keeps_completed_media(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    export_root = data_root / "exports"
    export_root.mkdir(parents=True, mode=0o700)
    export_root.chmod(0o700)
    client = _InterruptedTelegram()
    exporter_config = {"storage_limit_bytes": 1024**3, "min_free_bytes": 0}
    command = ["export", CHAT, str(TOPIC), "MEDIA", "100", "100MiB"]
    root = Dir.root(str(export_root))
    try:
        with pytest.raises(OSError, match="synthetic interrupted stream"):
            export(root, exporter_config, command, client=client)

        topic_dir = export_root / CHAT / str(TOPIC)
        assert (topic_dir / "media/502/502.bin").read_bytes() == client.payloads[502]
        assert not (topic_dir / "media/.part-501").exists()
        assert not (topic_dir / "media/501").exists()

        client.page_calls = 0
        resumed = export(root, exporter_config, command, client=client)
        assert resumed["complete"] is True
        assert resumed["message_count"] == 2
        for message_id, payload in client.payloads.items():
            saved = topic_dir / "media" / str(message_id) / f"{message_id}.bin"
            assert saved.read_bytes() == payload
        assert not any(path.name.startswith(".part-") for path in (topic_dir / "media").iterdir())
    finally:
        root.close()


def test_export_marks_media_skipped_when_storage_budget_is_too_small(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    client = _FakeTelegram(8_000)
    monkeypatch.setattr(exporter.Budget, "available_bytes", lambda _self: 69_536)
    root = Dir.root(str(export_root))
    try:
        manifest = export(
            root,
            {"storage_limit_bytes": 1024**3, "min_free_bytes": 0},
            ["export", CHAT, str(TOPIC), "MEDIA", "100", "100MiB"],
            client=client,
        )
    finally:
        root.close()

    skipped = manifest["skipped_media"]
    assert len(skipped) == 1
    assert skipped[0]["metadata"]["reason"] == "storage_budget_exhausted"
    assert client.download_limits == []


def test_unknown_size_download_stops_at_selected_limit_and_removes_partial_file(
    tmp_path: Path,
) -> None:
    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    client = _UnknownSizeTelegram()
    root = Dir.root(str(export_root))
    try:
        result = export(
            root,
            {"storage_limit_bytes": 1024**3, "min_free_bytes": 0},
            ["export", CHAT, str(TOPIC), "MEDIA", "100", "100MiB"],
            client=client,
        )
    finally:
        root.close()

    topic_dir = export_root / CHAT / str(TOPIC)
    message = json.loads((topic_dir / "messages.jsonl").read_text(encoding="utf-8"))
    assert result["complete"] is True
    assert client.download_limits == [100 * 1024**2]
    assert message["media"]["reason"] == "over_100_MiB"
    assert not list((topic_dir / "media").iterdir())


def test_publisher_configuration_rejects_outside_and_overlapping_storage_roots(
    tmp_path: Path,
) -> None:
    base = tmp_path / "data"
    base.mkdir(mode=0o700)
    uid, gid = os.geteuid(), os.getegid()
    config_path = tmp_path / "publisher.json"
    config = {
        "version": 1,
        "data_root": str(base),
        "source_root": str(base / "exports"),
        "destination_root": str(base / "imports"),
        "export_config": str(tmp_path / "export.json"),
        "lock_path": str(tmp_path / "publisher.lock"),
        "expected_data_uuid": None,
        "require_mount": False,
        "storage_limit_bytes": 10 * 1024**2,
        "min_free_bytes": 0,
        "source_uid": uid,
        "source_gid": gid,
        "destination_uid": uid,
        "destination_gid": gid,
    }
    _write_config(config_path, config, 0o600)
    assert telegram_publisher._safe_config(str(config_path))["destination_root"].endswith("imports")

    config["destination_root"] = str(tmp_path / "outside")
    _write_config(config_path, config, 0o600)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_CONFIG_INVALID"):
        telegram_publisher._safe_config(str(config_path))

    config["destination_root"] = str(base / "exports" / "nested")
    _write_config(config_path, config, 0o600)
    with pytest.raises(telegram_publisher.TransferError, match="TRANSFER_CONFIG_INVALID"):
        telegram_publisher._safe_config(str(config_path))


def test_sequential_resume_revisions_keep_both_media_files(tmp_path: Path) -> None:
    from telegram_pipeline_support import build_partial_media_revisions

    package = build_partial_media_revisions(tmp_path)
    final_stage = package["final_stage"]
    first_stage = package["first_stage"]
    assert (first_stage / "media/501/item-501.bin").read_bytes() == bytes([501 % 251]) * 2
    assert sorted(path.name for path in (final_stage / "media").iterdir()) == ["501", "502"]
    assert (final_stage / "media/501/item-501.bin").read_bytes() == bytes([501 % 251]) * 2
    large_file = final_stage / "media/502/item-502.bin"
    assert large_file.stat().st_size == 100 * 1024**2 + 1
    with large_file.open("rb") as stream:
        large_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    transfer = json.loads(
        (final_stage / "telegram_transfer_manifest.json").read_text(encoding="utf-8")
    )
    large_record = next(item for item in transfer["media_files"] if item["message_id"] == 502)
    assert large_record["size"] == 100 * 1024**2 + 1
    assert large_record["sha256"] == large_sha256
