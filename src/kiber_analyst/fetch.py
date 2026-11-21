from __future__ import annotations

import hashlib
import re
import shutil
import stat
import subprocess
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from kiber_analyst.config_paths import data_path
from kiber_analyst.errors import FetchError
from kiber_analyst.paths import job_paths, write_json_atomic

MIN_FREE_BYTES = 10 * 1024**3


def _digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _rclone(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["rclone", *args],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except OSError as exc:
        raise FetchError(f"Cannot start rclone: {exc}") from exc
    if result.returncode:
        raise FetchError(f"rclone exited with status {result.returncode}")
    return result.stdout.strip()


def _validate_zip(archive: Path, unpacked: Path) -> str:
    try:
        with zipfile.ZipFile(archive) as source:
            infos = source.infolist()
            result_members: list[str] = []
            total_uncompressed = 0
            for info in infos:
                name = info.filename.replace("\\", "/")
                member_path = PurePosixPath(name)
                mode = (info.external_attr >> 16) & 0o170000
                if (
                    not name
                    or member_path.is_absolute()
                    or ".." in member_path.parts
                    or "\x00" in name
                    or mode == stat.S_IFLNK
                ):
                    raise FetchError(f"Unsafe ZIP member path: {info.filename!r}")
                if info.is_dir():
                    continue
                total_uncompressed += info.file_size
                if member_path.name == "result.json":
                    result_members.append(name)
            if len(result_members) != 1:
                raise FetchError(
                    "ZIP must contain exactly one result.json; "
                    f"found {len(result_members)}"
                )
            free = shutil.disk_usage(unpacked.parent).free
            if free < MIN_FREE_BYTES + total_uncompressed:
                raise FetchError(
                    "Insufficient free space for safe extraction: "
                    f"need at least {MIN_FREE_BYTES + total_uncompressed} bytes, have {free}"
                )
            source.extractall(unpacked)
            result_path = unpacked.joinpath(*PurePosixPath(result_members[0]).parts)
            if not result_path.is_file():
                raise FetchError("result.json was not present after extraction")
            return result_path.relative_to(unpacked).as_posix()
    except zipfile.BadZipFile as exc:
        raise FetchError(f"Invalid ZIP archive: {exc}") from exc
    except OSError as exc:
        raise FetchError(f"Cannot inspect or extract ZIP archive: {exc}") from exc


def fetch_archive(
    archive_uri: str | None = None,
    work_root: Path | None = None,
    *,
    scope: str = "topic",
    topic_id: str | None = None,
    topic_title: str | None = None,
    chat_id: str | None = None,
    expected_md5: str | None = None,
    expected_size: int | None = None,
) -> Path:
    if not isinstance(archive_uri, str) or not archive_uri.strip():
        raise FetchError("archive_uri is required")
    if not isinstance(chat_id, str) or not chat_id.strip():
        raise FetchError("chat_id is required")
    if work_root is None:
        work_root = data_path("telegram")
    if scope not in {"topic", "chat"}:
        raise FetchError("scope must be 'topic' or 'chat'")
    if scope == "topic" and not topic_id:
        raise FetchError("topic scope requires topic_id")
    archive_name = Path(archive_uri.rsplit("/", 1)[-1]).stem
    slug = re.sub(r"[^a-z0-9-]+", "-", archive_name.lower()).strip("-")
    job_name = f"{datetime.now(UTC):%Y%m%d}-{slug or 'telegram'}"
    job_dir = work_root / job_name
    if job_dir.exists():
        raise FetchError(
            f"Job directory already exists; refusing to overwrite: {job_dir}"
        )

    job_dir.mkdir(parents=True, mode=0o700)
    paths = job_paths(job_dir)
    paths["raw"].mkdir(mode=0o700)
    paths["unpacked"].mkdir(mode=0o700)
    paths["normalized"].mkdir(mode=0o700)
    archive_path = paths["raw"] / Path(archive_uri).name
    started = datetime.now(UTC)

    _rclone(["copy", archive_uri, str(paths["raw"])])
    if not archive_path.is_file():
        raise FetchError(f"rclone did not create the expected archive: {archive_path}")

    remote_sum = _rclone(["md5sum", archive_uri]).split()
    local_md5 = _digest(archive_path, "md5")
    remote_md5 = remote_sum[0].lower() if remote_sum else None
    if remote_md5 is None or local_md5.lower() != remote_md5:
        raise FetchError("Downloaded archive checksum does not match the remote checksum")
    if expected_md5 is not None and local_md5.lower() != expected_md5.lower():
        raise FetchError("Downloaded archive checksum does not match the expected checksum")
    size_bytes = archive_path.stat().st_size
    if expected_size is not None and size_bytes != expected_size:
        raise FetchError("Downloaded archive size does not match the expected size")

    result_member = _validate_zip(archive_path, paths["unpacked"])
    manifest = {
        "archive_uri": archive_uri,
        "archive_name": archive_path.name,
        "archive_size_bytes": size_bytes,
        "archive_md5": local_md5,
        "snapshot_sha256": _digest(archive_path, "sha256"),
        "scope": scope,
        "scope_topic_id": str(topic_id) if topic_id is not None else None,
        "scope_topic_title": topic_title,
        "chat_id": str(chat_id),
        "result_json": result_member,
        "steps": {
            "fetch": {
                "started_at": started.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
            }
        },
        "load": {
            "status": "not_run",
            "inserted": None,
            "updated": None,
            "skipped": None,
            "absent": None,
        },
    }
    write_json_atomic(paths["manifest"], manifest)
    return job_dir
