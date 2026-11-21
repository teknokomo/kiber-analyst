from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Protocol

from kiber_analyst.config_paths import data_path, runtime_path
from kiber_analyst.jobapi import api_call

DEFAULT_BACKUP_ROOT = data_path("backups")
DEFAULT_RCLONE_CONFIG = runtime_path("rclone.conf")


class BackupError(RuntimeError):
    """A safe, user-facing backup failure without command output or credentials."""


class Destination(Protocol):
    """Publish a staged backup tree and verify every staged object."""

    name: str

    def publish_and_verify(self, source: Path) -> None: ...


class RcloneDestination:
    """Transport adapter for any configured rclone remote, including future S3."""

    name = "rclone"

    def __init__(
        self,
        remote_root: str,
        *,
        config: Path = DEFAULT_RCLONE_CONFIG,
        binary: str = "rclone",
    ) -> None:
        self.remote_root = _validate_remote_root(remote_root)
        self.config = config
        self.binary = binary

    def _run(self, operation: str, args: list[str]) -> None:
        try:
            result = subprocess.run(
                [self.binary, *args, "--config", str(self.config), "--log-level=ERROR"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            raise BackupError(f"rclone {operation} could not start") from None
        if result.returncode:
            raise BackupError(f"rclone {operation} failed (exit {result.returncode})")

    def publish_and_verify(self, source: Path) -> None:
        self._run(
            "copy",
            ["copy", str(source), self.remote_root, "--checksum"],
        )
        self._run(
            "check",
            ["check", str(source), self.remote_root, "--one-way"],
        )


def _validate_remote_root(value: str) -> str:
    if not isinstance(value, str) or "\n" in value or "\x00" in value:
        raise BackupError("Backup destination is invalid")
    match = re.fullmatch(r"([A-Za-z0-9_.-]+):(.+)", value)
    if not match:
        raise BackupError("Backup destination must name a configured rclone remote")
    path = PurePosixPath(match.group(2))
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise BackupError("Backup destination path is invalid")
    return f"{match.group(1)}:{path.as_posix()}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _backup_files(backup_root: Path) -> tuple[list[Path], list[Path]]:
    root = backup_root.resolve(strict=True)
    knowledge_dir = root / "knowledge"
    if knowledge_dir.is_symlink() or not knowledge_dir.is_dir():
        raise BackupError("Knowledge backup directory is missing or unsafe")
    queue_files = sorted(root.glob("*.sqlite3"))
    knowledge_files = sorted(knowledge_dir.glob("knowledge-*.dump"))
    for path in [*queue_files, *knowledge_files]:
        if path.is_symlink() or not path.is_file() or root not in path.resolve().parents:
            raise BackupError("Backup input is not a regular file inside the backup directory")
    if not queue_files:
        raise BackupError("Queue backup is missing; refusing to publish an incomplete backup")
    if not knowledge_files:
        raise BackupError("Knowledge dump is missing; refusing to publish an incomplete backup")
    return queue_files, knowledge_files


def _write_sha256sums(directory: Path, files: list[Path]) -> Path:
    lines = [f"{_sha256(path)}  {path.name}" for path in files]
    sums_path = directory / "SHA256SUMS"
    _write_atomic(sums_path, ("\n".join(lines) + "\n").encode("utf-8"))
    return sums_path


def _stage_link(source: Path, target: Path) -> None:
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)
        os.chmod(target, 0o600)


def publish_backups(
    backup_root: Path,
    destination: Destination,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Create checksum manifests and publish queue plus knowledge as one tree."""
    backup_root = backup_root.resolve(strict=True)
    queue_files, knowledge_files = _backup_files(backup_root)
    knowledge_dir = backup_root / "knowledge"
    queue_sums = _write_sha256sums(backup_root, queue_files)
    knowledge_sums = _write_sha256sums(knowledge_dir, knowledge_files)
    created = now or datetime.now(timezone.utc)
    if created.tzinfo is None:
        raise BackupError("Manifest timestamp must include a timezone")
    stamp = created.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    manifest_path = backup_root / f"manifest-{stamp}.json"
    files: list[dict[str, object]] = []
    for category, paths, sums in (
        ("queue", queue_files, queue_sums),
        ("knowledge", knowledge_files, knowledge_sums),
    ):
        for path in [*paths, sums]:
            remote_path = f"{category}/{path.name}"
            files.append(
                {
                    "category": category,
                    "remote_path": remote_path,
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    manifest = {
        "format_version": 1,
        "created_at": created.astimezone(timezone.utc).isoformat(),
        "destination_backend": destination.name,
        "files": files,
    }
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    _write_atomic(manifest_path, manifest_bytes)

    with tempfile.TemporaryDirectory(prefix=".publish-", dir=backup_root) as stage_name:
        stage = Path(stage_name)
        for path in queue_files:
            _stage_link(path, stage / "queue" / path.name)
        _stage_link(queue_sums, stage / "queue" / "SHA256SUMS")
        for path in knowledge_files:
            _stage_link(path, stage / "knowledge" / path.name)
        _stage_link(knowledge_sums, stage / "knowledge" / "SHA256SUMS")
        _stage_link(manifest_path, stage / "manifests" / manifest_path.name)
        destination.publish_and_verify(stage)

    return {
        "manifest": manifest_path.name,
        "queue_files": len(queue_files),
        "knowledge_files": len(knowledge_files),
        "verified": True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Back up the kiber-analyst queue and knowledge database")
    configured_destination = os.environ.get("KIBER_ANALYST_BACKUP_DESTINATION")
    parser.add_argument("--destination", default=configured_destination, required=not configured_destination)
    parser.add_argument(
        "--backup-root",
        type=Path,
        default=Path(os.environ.get("KIBER_ANALYST_BACKUP_ROOT", DEFAULT_BACKUP_ROOT)),
    )
    parser.add_argument(
        "--rclone-config",
        type=Path,
        default=Path(os.environ.get("RCLONE_CONFIG", DEFAULT_RCLONE_CONFIG)),
    )
    parser.add_argument(
        "--rclone-bin",
        default=os.environ.get("RCLONE_BIN", "rclone"),
    )
    args = parser.parse_args(argv)

    try:
        queue_result = api_call("backup")
        backup_path = Path(queue_result["backup_path"]).resolve(strict=True)
        backup_root = args.backup_root.resolve(strict=True)
        if backup_path.parent != backup_root:
            raise BackupError("Worker backup path is outside the configured backup directory")
        destination = RcloneDestination(
            args.destination,
            config=args.rclone_config,
            binary=args.rclone_bin,
        )
        report = publish_backups(backup_root, destination)
    except BackupError as exc:
        parser.exit(1, f"kiber-analyst backup failed: {exc}\n")
    except Exception as exc:  # noqa: BLE001 - do not print provider, config, or command data.
        parser.exit(1, f"kiber-analyst backup failed ({type(exc).__name__})\n")
    print(json.dumps({"queue_backup": backup_path.name, **report}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
