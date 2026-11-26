from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from kiber_analyst import broker, telegram_publisher
from kiber_analyst.telegram_exporter import (
    MAX_FILE_BYTES,
    Budget,
    Client,
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

    def media_metadata(self, _chat: str, _ids: list[int], peer_type: str = 'supergroup') -> dict:
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

    def media_metadata(self, _chat: str, _ids: list[int], peer_type: str = 'supergroup') -> dict:
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


class _BasicGroupPhotoTelegram:
    def __init__(self, chat: str):
        self.chat = chat
        self.pages = 0
        self.metadata_calls = []
        self.download_calls = 0

    def info(self, _chat: str) -> dict:
        return {"id": int(self.chat), "forum": False, "type": "group", "title": "Synthetic group"}

    def title(self, _chat: str, _topic: int | None) -> None:
        return None

    def page(self, _chat: str, _topic: int | None, _limit: int, _before: int | None) -> list[dict]:
        self.pages += 1
        if self.pages > 1:
            return []
        return [{
            "id": 42, "date": "2026-01-01T00:00:00Z", "sender": "Synthetic",
            "senderId": 7, "text": "photo from a basic group", "media": {"kind": "photo"},
        }]

    def media_metadata(self, chat: str, ids: list[int], peer_type: str = "supergroup") -> dict:
        self.metadata_calls.append((chat, ids, peer_type))
        return {42: {
            "_": "messageMediaPhoto",
            "photo": {
                "_": "photo",
                "sizes": [
                    {"_": "photoSize", "size": 17},
                    {"_": "photoSize", "size": 51},
                ],
                "videoSizes": None,
            },
        }}

    def service_page(self, _chat: str, _topic: int | None, _limit: int, _before: int | None) -> list:
        return []

    def download(self, _chat: str, _message: int, directory: Dir, _file_limit: int) -> None:
        self.download_calls += 1
        fd = os.open("photo.jpg", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory.fd)
        with os.fdopen(fd, "wb") as stream:
            stream.write(b"p" * 51)


@pytest.mark.parametrize("peer_type,expected_method", [
    ("group", "messages.GetMessages"),
    ("supergroup", "channels.GetMessages"),
    ("forum", "channels.GetMessages"),
])
def test_media_metadata_uses_the_telegram_method_for_each_peer_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, peer_type: str, expected_method: str,
) -> None:
    home = tmp_path / "telegram-home"
    home.mkdir(mode=0o700)
    home.chmod(0o700)
    client = Client({"telegram_cli": "/unused", "telegram_home": str(home)})
    calls = []

    def fake_call(argv):
        calls.append(argv)
        return {"messages": [{
            "id": 42,
            "peerId": {"_": "peerChat", "chatId": 12345},
            "media": {"_": "messageMediaPhoto", "photo": {"_": "photo", "sizes": []}},
        }]}

    monkeypatch.setattr(client, "call", fake_call)
    result = client.media_metadata("-12345", [42], peer_type=peer_type)

    assert result[42]["_"] == "messageMediaPhoto"
    assert calls[0][1] == expected_method
    params = json.loads(calls[0][calls[0].index("--params") + 1])
    if peer_type == "group":
        assert params == {"id": [{"_": "inputMessageID", "id": 42}]}
    else:
        assert params["channel"] == {"_chat": "-12345"}
    if peer_type == "group":
        monkeypatch.setattr(client, "call", lambda _argv: {
            "messages": [{"id": 42, "media": {"_": "messageMediaPhoto"}}],
        })
        assert client.media_metadata("-12345", [42], peer_type="group") == {}


@pytest.mark.parametrize("download_media", [False, True])
def test_basic_group_photo_metadata_is_preserved_with_or_without_download(
    tmp_path: Path, download_media: bool,
) -> None:
    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    config = {"storage_limit_bytes": 1024**2, "min_free_bytes": 0}
    client = _BasicGroupPhotoTelegram("-12345")
    root = Dir.root(str(export_root))
    try:
        command = ["export", "-12345", "GROUP"]
        if download_media:
            command.append("MEDIA")
        command.extend(["10", "100MiB"])
        manifest = export(root, config, command, client=client)
    finally:
        root.close()

    topic = export_root / "-12345" / "group"
    message = json.loads((topic / "messages.jsonl").read_text(encoding="utf-8"))
    assert manifest["complete"] is True
    assert manifest["media_metadata_unavailable_count"] == 0
    assert client.metadata_calls == [("-12345", [42], "group")]
    assert message["media"]["size_bytes"] == 51
    assert message["media"]["size_source"] == "photo.sizes"
    if download_media:
        assert client.download_calls == 1
        assert message["media"]["export_status"] == "downloaded"
        assert (topic / message["media"]["path"]).read_bytes() == b"p" * 51
    else:
        assert client.download_calls == 0
        assert message["media"]["export_status"] == "not_requested"
        assert "path" not in message["media"]


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


def test_four_gigabyte_attachment_boundary_uses_decimal_bytes_without_allocating_file() -> None:
    from kiber_analyst.telegram_exporter import media_size_metadata

    maximum = 4_000_000_000
    accepted, accepted_reason = media_size_metadata(
        {"kind": "document", "size": maximum}, None, maximum,
    )
    rejected, rejected_reason = media_size_metadata(
        {"kind": "document", "size": maximum + 1}, None, maximum,
    )
    assert accepted["size_bytes"] == maximum and accepted_reason is None
    assert rejected["size_bytes"] == maximum + 1 and rejected_reason == "over_limit"


def test_export_publisher_registration_and_worker_stage_keep_media_over_100_mib(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("the restricted publisher intentionally refuses root execution")
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

    # This fixture verifies that the configured attachment limit is
    # independent of the deployment's 5 GiB free-space reserve.
    monkeypatch.setattr(broker, "RESERVE", 0)
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
    from telegram_pipeline_support import _transfer_media_records, build_partial_media_revisions

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
    large_record = next(item for item in _transfer_media_records(final_stage)
                        if item["message_id"] == 502)
    assert large_record["size"] == 100 * 1024**2 + 1
    assert large_record["sha256"] == large_sha256

    # Simulate a process stop after the candidate is complete and its transfer
    # manifest is durable, but before the final directory rename.
    source_hashes = transfer["source_hashes"]
    media_sha = transfer["media_manifest_sha256"]
    temp_name = ".transfer-" + hashlib.sha256(
        (package["source_id"] + json.dumps(source_hashes, sort_keys=True)
         + (media_sha or "none")).encode()
    ).hexdigest()[:32]
    os.rename(final_stage, final_stage.parent / temp_name)
    recovered = telegram_publisher.handle(
        "copy", package["source_id"], config_path=str(package["publisher_config"]),
    )
    final_stage = package["import_root"] / package["chat"] / package["topic"]
    assert recovered["status"] == "ready"
    assert sorted(path.name for path in (final_stage / "media").iterdir()) == ["501", "502"]
    assert (first_stage / "media/501/item-501.bin").is_file()


@pytest.mark.parametrize("overrun", ["storage", "filesystem", "aggregate"])
def test_budget_check_zero_detects_signed_budget_overrun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrun: str,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    root_path = tmp_path / "budget-root"
    root_path.mkdir(mode=0o700)
    root_path.chmod(0o700)
    root = Dir.root(str(root_path))
    try:
        config = {"storage_limit_bytes": 100, "min_free_bytes": 0}
        if overrun == "storage":
            monkeypatch.setattr(exporter, "tree_size", lambda _root: 101)
            monkeypatch.setattr(
                exporter.os, "fstatvfs",
                lambda _fd: SimpleNamespace(f_bavail=1000, f_frsize=1),
            )
        elif overrun == "filesystem":
            config["min_free_bytes"] = 1
            monkeypatch.setattr(
                exporter.os, "fstatvfs",
                lambda _fd: SimpleNamespace(f_bavail=0, f_frsize=1),
            )
        else:
            config.update(storage_limit_bytes=1000, aggregate_available_bytes=0)
            monkeypatch.setattr(
                exporter.os, "fstatvfs",
                lambda _fd: SimpleNamespace(f_bavail=1000, f_frsize=1),
            )
        budget = Budget(root, config)
        if overrun == "aggregate":
            budget.consume(1)
        assert budget.available_bytes() == 0
        with pytest.raises(ExportError, match="budget exhausted"):
            budget.check(0)
    finally:
        root.close()


def test_export_resume_keeps_completed_download_receipt_when_free_space_shrinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    class ShrinkingClient(_FakeTelegram):
        space_shrunk = False

        def download(self, chat, message_id, directory, file_limit):
            super().download(chat, message_id, directory, file_limit)
            self.space_shrunk = True

    client = ShrinkingClient(32_768)
    config = {"storage_limit_bytes": 1024**3, "min_free_bytes": 1}
    root = Dir.root(str(export_root))
    try:
        export(root, config, ["export", CHAT, str(TOPIC), "100"], client=client)
        export_services(root, config, CHAT, TOPIC, 100, None, client=client)

        def shrinking_free_space(_fd):
            available = 0 if client.space_shrunk else 2_000_000
            return SimpleNamespace(f_bavail=available, f_frsize=1)

        monkeypatch.setattr(exporter.os, "fstatvfs", shrinking_free_space)
        first = export(root, config, ["export-resume", CHAT, str(TOPIC)], client=client)
        checkpoint = export_root / CHAT / str(TOPIC) / "media-resume"
        saved_checkpoint = json.loads((checkpoint / "status.json").read_text())
        assert first["status"] == "partial"
        assert saved_checkpoint["records"]["501"]["reason"] == "download_error"
        assert (checkpoint / ".part-501" / "receipt.json").is_file()
        assert client.download_limits == [32_768]

        client.space_shrunk = False
        monkeypatch.setattr(
            exporter.os, "fstatvfs",
            lambda _fd: SimpleNamespace(f_bavail=2_000_000, f_frsize=1),
        )
        second = export(root, config, ["export-resume", CHAT, str(TOPIC)], client=client)
        assert second["status"] == "complete"
        assert second["downloaded"] == 1
        assert client.download_limits == [32_768]
        media = checkpoint / "501" / "synthetic-video.bin"
        assert media.stat().st_size == 32_768
        assert not (checkpoint / ".part-501").exists()
    finally:
        root.close()


def test_export_resume_complete_noop_does_not_require_new_storage_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    client = _FakeTelegram(32_768)
    config = {"storage_limit_bytes": 1024**3, "min_free_bytes": 0}
    root = Dir.root(str(export_root))
    try:
        export(root, config, ["export", CHAT, str(TOPIC), "100"], client=client)
        export_services(root, config, CHAT, TOPIC, 100, None, client=client)
        first = exporter.export_resume(root, config, CHAT, TOPIC, 100, None, client)
        assert first["status"] == "complete"

        resume_dir = export_root / CHAT / str(TOPIC) / "media-resume"
        checkpoint_before = (resume_dir / "status.json").read_bytes()
        manifest_before = (resume_dir / "manifest.json").read_bytes()

        def no_headroom(_self, _required):
            raise exporter.ExportError("STORAGE_BUDGET_EXHAUSTED")

        monkeypatch.setattr(exporter.Budget, "check", no_headroom)
        repeated = exporter.export_resume(root, config, CHAT, TOPIC, 100, None, client)

        assert repeated["status"] == "complete"
        assert (resume_dir / "status.json").read_bytes() == checkpoint_before
        assert (resume_dir / "manifest.json").read_bytes() == manifest_before
    finally:
        root.close()


@pytest.mark.parametrize("channel", ["stdout", "stderr"])
def test_telegram_cli_bounds_output_while_reading_and_kills_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, channel: str,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    home = tmp_path / "telegram-home"
    home.mkdir(mode=0o700)
    home.chmod(0o700)
    marker = tmp_path / "child-survived"
    cli = tmp_path / "fake-telegram"
    cap_name = "MAX_CLI_STDOUT_BYTES" if channel == "stdout" else "MAX_CLI_STDERR_BYTES"
    monkeypatch.setattr(exporter, cap_name, 1024)
    cli.write_text(
        f"#!{sys.executable}\n"
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, '-c', \"import time,pathlib; time.sleep(0.5); pathlib.Path({str(marker)!r}).touch()\"])\n"
        f"sys.{channel}.write('x' * 4096)\n"
        f"sys.{channel}.flush()\n"
        "time.sleep(2)\n",
        encoding="utf-8",
    )
    cli.chmod(0o700)
    client = Client({"telegram_cli": str(cli), "telegram_home": str(home)})
    started = time.monotonic()
    with pytest.raises(ExportError, match="output limit exceeded"):
        client.call(["synthetic"])
    assert time.monotonic() - started < 1.5
    time.sleep(0.65)
    assert not marker.exists()


def test_pinned_better_tg_cli_mock_download_uses_only_local_write_exception(
    tmp_path: Path,
) -> None:
    cli = os.environ.get("KIBER_BETTER_TG_CLI_BIN")
    if not cli:
        pytest.skip("CI or operator must provide the pinned Better-TG-CLI executable")
    import shutil
    import subprocess

    home = tmp_path / "telegram-home"
    home.mkdir(mode=0o700)
    home.chmod(0o700)
    client = Client({"telegram_cli": cli, "telegram_home": str(home)})
    assert client.env["PATH"] == "/usr/bin:/bin"
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required to run the pinned Better-TG-CLI")
    # GitHub setup-node exposes Node from its tool cache, while the service
    # intentionally fixes PATH to /usr/bin:/bin. Keep the production default
    # assertion above and add the CI-managed Node directory only for this
    # isolated CLI integration test.
    client.env["PATH"] = f"{Path(node).resolve().parent}:/usr/bin:/bin"
    client.env["TG_MOCK"] = "1"
    client.env["TG_NO_UPDATE_CHECK"] = "1"
    with pytest.raises(ExportError, match="Unsafe Telegram CLI permission override"):
        client.call(["api", "messages.SendMessage"], read_only=False)
    assert client.env["TG_READ_ONLY"] == "1"
    version = subprocess.run(
        [cli, "--version"], cwd=home, env=client.env,
        capture_output=True, timeout=15, check=True,
    )
    assert version.stdout.decode("utf-8").strip() == "0.41.2"

    output = tmp_path / "media-output"
    output.mkdir(mode=0o700)
    output.chmod(0o700)
    directory = Dir.root(str(output))
    try:
        response = client.download("-1004002", 1, directory, file_limit=4 * 1024**2)
        names = directory.names()
        assert response["messageId"] == 1
        assert len(names) == 1
        size, digest = directory.digest(names[0], 4 * 1024**2)
        assert size > 0 and len(digest) == 64
    finally:
        directory.close()


def test_export_manifest_bounds_skipped_media_sample_and_keeps_exact_counts(
    tmp_path: Path,
) -> None:
    import kiber_analyst.telegram_exporter as exporter

    export_root = tmp_path / "exports"
    export_root.mkdir(mode=0o700)
    export_root.chmod(0o700)
    topic_dir = export_root / CHAT / str(TOPIC)
    pages_dir = topic_dir / ".pages"
    topic_dir.mkdir(mode=0o700, parents=True)
    pages_dir.mkdir(mode=0o700)
    destination = Dir.root(str(topic_dir))
    pages = Dir.root(str(pages_dir))
    count = 500
    records = [{
        "message_id": count - index,
        "date": "2026-01-01T00:00:00Z",
        "text": "synthetic",
        "media": {
            "kind": "document", "size_bytes": 101, "size_source": "document.size",
            "export_status": "skipped", "reason": "over_limit",
            "private_unbounded_field": "z" * 512,
        },
    } for index in range(count)]
    pages.write_json("00000000.json", {
        "observed_newest_id": count,
        "previous_before": None,
        "next_before": None,
        "exhausted": True,
        "records": records,
    })
    meta = {
        "source_id": SOURCE, "chat_id": CHAT, "topic_id": TOPIC,
        "scope": "topic", "scope_before": None, "snapshot_newest_id": None,
    }

    class NoopBudget:
        @staticmethod
        def check(_extra=0):
            return None

    try:
        manifest, message_count = exporter.publish(
            destination, pages, meta, NoopBudget(),
        )
        assert message_count == count
        assert manifest["skipped_media_count"] == count
        assert manifest["skipped_media_by_reason"] == {"over_limit": count}
        assert manifest["skipped_media_sample_truncated"] is True
        assert len(manifest["skipped_media"]) == exporter.MAX_SKIPPED_MEDIA_SAMPLE
        assert all("private_unbounded_field" not in row["metadata"]
                   for row in manifest["skipped_media"])
        assert manifest["skipped_media"][0]["metadata"]["reason"] == "over_limit"
    finally:
        pages.close()
        destination.close()
