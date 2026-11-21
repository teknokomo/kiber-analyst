from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kiber_analyst.backup import (
    BackupError,
    RcloneDestination,
    _validate_remote_root,
    publish_backups,
)


class RecordingDestination:
    name = "test"

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def publish_and_verify(self, source: Path) -> None:
        self.files = {
            path.relative_to(source).as_posix(): path.read_bytes()
            for path in source.rglob("*")
            if path.is_file()
        }


def _write(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


def test_publish_requires_both_queue_and_knowledge(tmp_path: Path) -> None:
    root = tmp_path / "backups"
    root.mkdir()
    (root / "knowledge").mkdir()
    _write(root / "daily-test.sqlite3", b"queue")
    destination = RecordingDestination()

    with pytest.raises(BackupError, match="Knowledge dump is missing"):
        publish_backups(root, destination)

    assert destination.files == {}


def test_publish_writes_checksums_and_one_verified_manifest_tree(tmp_path: Path) -> None:
    root = tmp_path / "backups"
    _write(root / "daily-test.sqlite3", b"queue snapshot")
    _write(root / "knowledge" / "knowledge-test.dump", b"knowledge snapshot")
    destination = RecordingDestination()

    report = publish_backups(
        root,
        destination,
        now=datetime(2026, 11, 7, 12, 34, 56, tzinfo=timezone.utc),
    )

    assert report == {
        "manifest": "manifest-20261107T123456.000000Z.json",
        "queue_files": 1,
        "knowledge_files": 1,
        "verified": True,
    }
    assert destination.files["queue/daily-test.sqlite3"] == b"queue snapshot"
    assert destination.files["knowledge/knowledge-test.dump"] == b"knowledge snapshot"
    assert "queue/SHA256SUMS" in destination.files
    assert "knowledge/SHA256SUMS" in destination.files
    manifest = json.loads(destination.files[f"manifests/{report['manifest']}"])
    assert manifest["destination_backend"] == "test"
    objects = {entry["remote_path"]: entry for entry in manifest["files"]}
    assert objects["queue/daily-test.sqlite3"]["sha256"] == hashlib.sha256(
        b"queue snapshot"
    ).hexdigest()
    assert objects["knowledge/knowledge-test.dump"]["size_bytes"] == len(
        b"knowledge snapshot"
    )

    for checksum_path, file_path in (
        (root / "SHA256SUMS", root / "daily-test.sqlite3"),
        (root / "knowledge" / "SHA256SUMS", root / "knowledge" / "knowledge-test.dump"),
    ):
        expected, name = checksum_path.read_text(encoding="utf-8").strip().split("  ", 1)
        assert name == file_path.name
        assert expected == hashlib.sha256(file_path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "value",
    [
        "",
        "gdrive:",
        "gdrive:/absolute",
        "gdrive:backup/../escape",
        "s3:bucket\n--config=/tmp/secret",
    ],
)
def test_remote_root_rejects_unsafe_or_incomplete_values(value: str) -> None:
    with pytest.raises(BackupError):
        _validate_remote_root(value)


def test_rclone_transport_supports_configured_remote_name_without_echoing_failure(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[list[str]] = []

    def fake_run(args, **_kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 7, stdout="", stderr="socks5://user:secret@proxy")

    monkeypatch.setattr("kiber_analyst.backup.subprocess.run", fake_run)
    destination = RcloneDestination(
        "s3:bucket/kiber-analyst",
        config=tmp_path / "rclone.conf",
        binary="rclone-test-double",
    )

    with pytest.raises(BackupError) as error:
        destination.publish_and_verify(tmp_path)

    assert "secret" not in str(error.value)
    assert len(calls) == 1
    assert calls[0][0:4] == [
        "rclone-test-double",
        "copy",
        str(tmp_path),
        "s3:bucket/kiber-analyst",
    ]
