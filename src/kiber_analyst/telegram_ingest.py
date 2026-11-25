from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from kiber_analyst.config_paths import data_path
from kiber_analyst.errors import LoadError
from kiber_analyst.media_resume import resumed_photos
from kiber_analyst.parse import _make_url, normalized_content_hash
from kiber_analyst.paths import job_paths, write_json_atomic

TELEGRAM_ROOT = Path(
    os.environ.get("KIBER_ANALYST_TELEGRAM_ROOT", str(data_path("imports")))
)
_MAX_LINE_BYTES = 4 * 1024 * 1024
_EVENT_TYPES = {
    "member_added", "member_joined", "member_left", "member_removed",
    "message_pinned", "message_unpinned", "topic_created", "topic_renamed",
    "topic_closed", "topic_reopened", "title_changed", "photo_changed",
    "description_changed", "permissions_changed", "video_chat_started",
    "video_chat_ended", "message_deleted", "unknown", "unsupported",
}


@dataclass(frozen=True)
class TelegramExport:
    path: Path
    chat_id: str
    topic_id: int
    topic_title: str | None
    message_count: int
    service_event_count: int
    message_sha256: str
    service_sha256: str
    manifest_sha256: str
    messages_size: int
    services_size: int
    manifest_size: int
    services_manifest_size: int
    chat_title: str | None
    services_manifest: dict[str, Any]

    @property
    def import_key(self) -> str:
        payload = {
            "chat_id": self.chat_id,
            "topic_id": self.topic_id,
            "messages_sha256": self.message_sha256,
            "services_sha256": self.service_sha256,
        }
        return hashlib.sha256(_canonical(payload)).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _safe_directory(path: Path, root: Path = TELEGRAM_ROOT) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise LoadError("Telegram export path must be absolute and traversal-free")
    candidate = Path(os.path.abspath(path))
    allowed = Path(os.path.abspath(root))
    if candidate == allowed or allowed not in candidate.parents:
        raise LoadError("Telegram export must be below the configured export root")
    current = Path("/")
    for part in candidate.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except OSError:
            raise LoadError("Telegram export path is unavailable") from None
        if stat.S_ISLNK(info.st_mode):
            raise LoadError("Telegram export path contains a symbolic link")
        if current != candidate and not stat.S_ISDIR(info.st_mode):
            raise LoadError("Telegram export path contains a non-directory component")
    info = candidate.lstat()
    if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
        raise LoadError("Telegram topic export directory must be private mode 0700")
    return candidate


def _open_private(path: Path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    except OSError:
        raise LoadError("Telegram export file is unavailable or unsafe") from None
    info = os.fstat(fd)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        os.close(fd)
        raise LoadError("Telegram export files must be regular, singly linked, mode 0600")
    return os.fdopen(fd, "rb")


def _read_json(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        with _open_private(path) as stream:
            raw = stream.read(1024 * 1024 + 1)
    except OSError:
        raise LoadError("Cannot read Telegram export metadata") from None
    if len(raw) > 1024 * 1024:
        raise LoadError("Telegram export metadata is oversized")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise LoadError("Telegram export metadata is invalid JSON") from None
    if not isinstance(value, dict):
        raise LoadError("Telegram export metadata must be an object")
    return value, raw


def _iter_jsonl_stream(stream) -> Iterator[tuple[bytes, dict[str, Any]]]:
    for number, raw in enumerate(stream, 1):
        if len(raw) > _MAX_LINE_BYTES:
            raise LoadError(f"Telegram JSONL record {number} is oversized")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LoadError(f"Telegram JSONL record {number} is invalid") from None
        if not isinstance(value, dict):
            raise LoadError(f"Telegram JSONL record {number} is not an object")
        yield raw, value


def _iter_jsonl(path: Path) -> Iterator[tuple[bytes, dict[str, Any]]]:
    try:
        stream = _open_private(path)
    except LoadError:
        raise
    with stream:
        yield from _iter_jsonl_stream(stream)


def _chat_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise LoadError("Telegram export has an invalid chat ID")
    result = str(value)
    if not re.fullmatch(r"-?[1-9][0-9]{0,18}", result):
        raise LoadError("Telegram export has an invalid chat ID")
    return result


def _topic_id(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise LoadError("Telegram export has an invalid topic ID")
    return value


def _validate_message(value: dict[str, Any], chat_id: str, topic_id: int) -> None:
    message_id = value.get("message_id")
    if type(message_id) is not int or message_id <= 0:
        raise LoadError("Telegram export contains an invalid message ID")
    if value.get("topic_id") != topic_id:
        raise LoadError("Telegram message is outside the exported topic")
    if not isinstance(value.get("date"), str) or not isinstance(value.get("text"), str):
        raise LoadError("Telegram message is missing its date or text")
    if "sender_name" in value and value["sender_name"] is not None and not isinstance(value["sender_name"], str):
        raise LoadError("Telegram message sender is invalid")
    if "sender_id" in value and value["sender_id"] is not None and not isinstance(value["sender_id"], (int, str)):
        raise LoadError("Telegram message sender ID is invalid")
    if "chat_id" in value and _chat_id(value["chat_id"]) != chat_id:
        raise LoadError("Telegram message belongs to another chat")


def _scan_file(
    path: Path,
    validate: Any,
    *,
    max_bytes: int | None = None,
) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    count = 0
    identifiers: set[int] = set()
    try:
        stream = _open_private(path)
    except LoadError:
        raise
    with stream:
        size = os.fstat(stream.fileno()).st_size
        if max_bytes is not None and size > max_bytes:
            raise LoadError("Telegram export file exceeds its inspection byte limit")
        scanned = 0
        for raw, value in _iter_jsonl_stream(stream):
            scanned += len(raw)
            if scanned > size or (max_bytes is not None and scanned > max_bytes):
                raise LoadError("Telegram export file changed or exceeded its inspection byte limit")
            validate(value)
            identifier = value.get("message_id", value.get("event_id"))
            if type(identifier) is int:
                if identifier in identifiers:
                    raise LoadError("Telegram export contains duplicate IDs")
                identifiers.add(identifier)
            digest.update(raw)
            count += 1
        if scanned != size:
            raise LoadError("Telegram export file changed during validation")
    return digest.hexdigest(), count, size


def inspect_export(
    path: Path,
    *,
    root: Path = TELEGRAM_ROOT,
    max_jsonl_bytes: int | None = None,
) -> TelegramExport:
    if max_jsonl_bytes is not None and (
        type(max_jsonl_bytes) is not int or max_jsonl_bytes <= 0
    ):
        raise LoadError("Telegram export inspection byte limit is invalid")
    export_dir = _safe_directory(path, root)
    manifest, manifest_bytes = _read_json(export_dir / "manifest.json")
    services, service_manifest_bytes = _read_json(export_dir / "services_manifest.json")
    if manifest.get("export_version") != 1 or manifest.get("complete") is not True:
        raise LoadError("Telegram message export is incomplete or unsupported")
    if manifest.get("scope_before") is not None:
        raise LoadError("Telegram ingestion requires a complete, unbounded topic export")
    chat_id = _chat_id(manifest.get("chat_id"))
    topic_id = _topic_id(manifest.get("topic_id"))
    expected_source = f"telegram:{chat_id}:topic:{topic_id}"
    if manifest.get("source_id") != expected_source:
        raise LoadError("Telegram export source identity does not match its directory")
    if str(export_dir.parent.name) != chat_id or export_dir.name != str(topic_id):
        raise LoadError("Telegram export directory does not match its manifest")
    expected_messages_sha = manifest.get("messages_sha256")
    if not isinstance(expected_messages_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_messages_sha):
        raise LoadError("Telegram message manifest has no valid SHA-256")
    message_sha, message_count, messages_size = _scan_file(
        export_dir / "messages.jsonl",
        lambda item: _validate_message(item, chat_id, topic_id),
        max_bytes=max_jsonl_bytes,
    )
    if message_sha != expected_messages_sha:
        raise LoadError("Telegram messages.jsonl SHA-256 does not match its manifest")
    if type(manifest.get("message_count")) is not int or manifest["message_count"] != message_count:
        raise LoadError("Telegram message count does not match its manifest")
    if (
        services.get("source_id") != expected_source
        or services.get("chat_id") != int(chat_id)
        or services.get("topic_id") != topic_id
        or services.get("complete") is not True
        or services.get("range_complete") is not True
        or services.get("scope_before") is not None
        or services.get("errors") not in ([], None)
    ):
        raise LoadError("Telegram service export is incomplete or does not match the topic")
    expected_services_sha = services.get("services_sha256")
    if not isinstance(expected_services_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_services_sha):
        raise LoadError("Telegram service manifest has no valid SHA-256")

    def validate_event(item: dict[str, Any]) -> None:
        event_id = item.get("event_id")
        if type(event_id) is not int or event_id <= 0 or item.get("topic_id") != topic_id:
            raise LoadError("Telegram service export contains an invalid event identity")
        if item.get("type") not in _EVENT_TYPES:
            raise LoadError("Telegram service export contains an unsupported event type")

    service_sha, service_count, service_size = _scan_file(
        export_dir / "services.jsonl", validate_event, max_bytes=max_jsonl_bytes,
    )
    if service_sha != expected_services_sha:
        raise LoadError("Telegram services.jsonl SHA-256 does not match its manifest")
    if type(services.get("event_count")) is not int or services["event_count"] != service_count:
        raise LoadError("Telegram service event count does not match its manifest")
    return TelegramExport(
        path=export_dir,
        chat_id=chat_id,
        topic_id=topic_id,
        topic_title=manifest.get("topic_title") if isinstance(manifest.get("topic_title"), str) else None,
        message_count=message_count,
        service_event_count=service_count,
        message_sha256=message_sha,
        service_sha256=service_sha,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        messages_size=messages_size,
        services_size=service_size,
        manifest_size=len(manifest_bytes),
        services_manifest_size=len(service_manifest_bytes),
        chat_title=manifest.get("chat_title") if isinstance(manifest.get("chat_title"), str) else None,
        services_manifest=services,
    )


def _media_info(export: TelegramExport, message_id: int, value: Any, *, resumed: bool = False) -> tuple[str | None, str | None, bool, dict[str, Any]]:
    if value is None:
        return None, None, False, {}
    if not isinstance(value, dict):
        raise LoadError("Telegram message media metadata is invalid")
    media_type = value.get("type")
    if media_type is None:
        media_type = value.get("media_type")
    if media_type is None:
        media_type = value.get("kind")
    if media_type is not None and (
        not isinstance(media_type, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", media_type)
    ):
        raise LoadError("Telegram message media type is invalid")
    relative = value.get("path")
    safe_meta = {
        key: value[key]
        for key in ("export_status", "reason", "sha256", "size")
        if key in value and isinstance(value[key], (str, int, bool, type(None)))
    }
    if relative is None:
        return media_type, None, True, safe_meta
    if not isinstance(relative, str):
        raise LoadError("Telegram message media path is invalid")
    parsed = PurePosixPath(relative)
    if parsed.is_absolute() or not parsed.parts or parsed.parts[0] != ("media-resume" if resumed else "media") or ".." in parsed.parts:
        raise LoadError("Telegram media path contains traversal")
    if len(parsed.parts) < 3 or parsed.parts[1] != str(message_id):
        raise LoadError("Telegram media path does not match its message")
    candidate = export.path.joinpath(*parsed.parts)
    current = export.path
    for part in parsed.parts[:-1]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return media_type, None, True, safe_meta
        except OSError:
            raise LoadError("Telegram media path is unavailable") from None
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise LoadError("Telegram media path contains a symbolic link or non-directory")
    try:
        with _open_private(candidate) as stream:
            if resumed and (os.fstat(stream.fileno()).st_size != value["size"]
                            or hashlib.file_digest(stream, "sha256").hexdigest() != value["sha256"]):
                raise LoadError("Media resume file does not match its checksum")
    except LoadError as exc:
        if not candidate.exists() and not candidate.is_symlink():
            return media_type, None, True, safe_meta
        raise exc
    return media_type, relative, False, safe_meta


def _normalize_message(export: TelegramExport, value: dict[str, Any], resumed_media: dict | None = None) -> tuple[dict[str, Any], bool]:
    message_id = value["message_id"]
    media_type, media_path, media_missing, media_meta = _media_info(
        export, message_id, resumed_media if resumed_media is not None else value.get("media"),
        resumed=resumed_media is not None,
    )
    if resumed_media is not None and media_missing:
        raise LoadError("Media resume file is missing")
    text = value["text"]
    topic_id = str(export.topic_id)
    return {
        "chat_id": export.chat_id,
        "message_id": str(message_id),
        "topic_id": topic_id,
        "topic_title": export.topic_title,
        "attribution": "scope",
        "date": value["date"],
        "date_raw": value["date"],
        "author": value.get("sender_name"),
        "author_id": str(value["sender_id"]) if value.get("sender_id") is not None else None,
        "text": text,
        "reply_to": value.get("reply_to_message_id"),
        "forwarded_from": None,
        "media_type": media_type,
        "media_path": media_path,
        "edited_at": None,
        "content_hash": normalized_content_hash(text),
        "url": _make_url(export.chat_id, topic_id, str(message_id)),
        "meta": {"media_export": media_meta} if media_meta else {},
        "_media_missing": media_missing,
    }, media_missing


def prepare_export(source: Path, artifact_dir: Path, *, root: Path = TELEGRAM_ROOT) -> tuple[Path, TelegramExport, int]:
    export = inspect_export(source, root=root)
    recovered = {}
    resume = export.path / "media-resume"
    if resume.exists() or resume.is_symlink():
        info = resume.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
            raise LoadError("Media resume directory is unsafe")
        with _open_private(resume / "status.json") as stream:
            checkpoint = stream.read(4 * 1024**2 + 1)
        if len(checkpoint) > 4 * 1024**2:
            raise LoadError("Media resume checkpoint is too large")
        recovered = resumed_photos(checkpoint, {
            "messages.jsonl": export.message_sha256,
            "services.jsonl": export.service_sha256,
            "manifest.json": export.manifest_sha256,
        }, (row for _, row in _iter_jsonl(export.path / "messages.jsonl")))
    paths = job_paths(artifact_dir)
    paths["normalized"].mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(paths["normalized"], 0o700)
    temporary = paths["messages"].with_name("messages.jsonl.tmp")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        info = temporary.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise LoadError("Unsafe stale Telegram normalization temporary") from None
        temporary.unlink()
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    messages_digest = hashlib.sha256()
    normalized_count = 0
    media_missing = 0
    try:
        with os.fdopen(fd, "wb") as output:
            for raw, item in _iter_jsonl(export.path / "messages.jsonl"):
                messages_digest.update(raw)
                normalized, missing = _normalize_message(export, item, recovered.get(item["message_id"]))
                encoded = (json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
                output.write(encoded)
                normalized_count += 1
                media_missing += int(missing)
            output.flush()
            os.fsync(output.fileno())
        if messages_digest.hexdigest() != export.message_sha256 or normalized_count != export.message_count:
            raise LoadError("Telegram messages changed while preparing the ingest job")
        os.replace(temporary, paths["messages"])
        os.chmod(paths["messages"], 0o600)
        uri = f"telegram-export://{export.chat_id}/{export.topic_id}/{export.message_sha256}"
        normalized_manifest = {
            "source_id": f"telegram:{export.chat_id}:topic:{export.topic_id}",
            "snapshot_sha256": export.message_sha256,
            "archive_size_bytes": export.messages_size,
            "archive_uri": uri,
            "archive_name": f"telegram-{export.chat_id}-{export.topic_id}-{export.message_sha256[:12]}",
            "scope": "topic",
            "scope_topic_id": str(export.topic_id),
            "chat_id": export.chat_id,
            "telegram_import_key": export.import_key,
            "service_events_sha256": export.service_sha256,
            "media_missing_count": media_missing,
            "media_resume_count": len(recovered),
            "parse": {
                "chat_title": export.chat_title,
                "topics": [{"topic_id": str(export.topic_id), "topic_title": export.topic_title}],
            },
        }
        write_json_atomic(paths["manifest"], normalized_manifest)
        os.chmod(paths["manifest"], 0o600)
        return artifact_dir, export, media_missing
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def load_telegram_export(
    source: Path,
    artifact_dir: Path,
    *,
    env_path: Path,
    project_slug: str = "default",
    dry_run: bool = False,
    root: Path = TELEGRAM_ROOT,
) -> dict[str, Any]:
    artifact_dir, export, media_missing = prepare_export(source, artifact_dir, root=root)
    from kiber_analyst.load import load_job

    result = load_job(
        artifact_dir,
        env_path=env_path,
        dry_run=dry_run,
        service_events_path=export.path / "services.jsonl",
        service_manifest=export.services_manifest,
        project_slug=project_slug,
    )
    result["media_missing"] = media_missing
    result["errors"] = 0
    result["messages_sha256"] = export.message_sha256
    result["services_sha256"] = export.service_sha256
    return result
