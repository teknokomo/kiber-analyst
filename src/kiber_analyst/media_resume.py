"""Validate a completed Telegram media repair checkpoint and its file references."""
from __future__ import annotations

import json
import re

from kiber_analyst.errors import LoadError
from kiber_analyst.media_limits import (
    DEFAULT_TELEGRAM_MEDIA_BYTES,
    MAX_TELEGRAM_MEDIA_BYTES,
    validate_telegram_media_limit,
)

_SKIP_REASONS = {
    "over_100_MiB", "over_limit", "unknown_size", "unsupported_media",
    "photo_variant_selection_unsupported", "download_error",
    "metadata_unavailable", "storage_budget_exhausted",
}


def resumed_media(checkpoint: bytes, hashes: dict[str, str], messages) -> dict[int, dict]:
    try:
        data = json.loads(checkpoint)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise LoadError("Media resume checkpoint is invalid JSON") from None
    if not isinstance(data, dict):
        raise LoadError("Media resume checkpoint does not match the export")
    checkpoint_version = data.get("version", 1)
    expected_inputs = hashes
    if checkpoint_version == 1:
        # Legacy checkpoints predate services_manifest.json in the input set.
        expected_inputs = {key: value for key, value in hashes.items()
                           if key != "services_manifest.json"}
    elif checkpoint_version != 2:
        raise LoadError("Media resume checkpoint version is unsupported")
    if data.get("inputs") != expected_inputs:
        raise LoadError("Media resume checkpoint does not match the export")
    requested_limit = data.get("requested_max_file_bytes", DEFAULT_TELEGRAM_MEDIA_BYTES)
    try:
        validate_telegram_media_limit(requested_limit)
    except ValueError:
        raise LoadError("Media resume checkpoint has an invalid size limit") from None
    records = data.get("records")
    if not isinstance(records, dict):
        raise LoadError("Media resume checkpoint has invalid records")
    missing = {}
    over_limit = []
    for row in messages:
        if not isinstance(row, dict):
            raise LoadError("Media resume source message is invalid")
        message_id = row.get("message_id")
        if type(message_id) is not int or message_id <= 0:
            raise LoadError("Media resume source message ID is invalid")
        media = row.get("media")
        if isinstance(media, dict) and media.get("path") is None:
            missing[str(message_id)] = media
        if isinstance(media, dict) and media.get("reason") in {"over_100_MiB", "over_limit"}:
            over_limit.append(message_id)
    recorded_over_limit = data.get("over_limit_message_ids")
    if (not records.keys() <= missing.keys()
            or (recorded_over_limit is not None
                and recorded_over_limit != sorted(over_limit))):
        raise LoadError("Media resume checkpoint is incomplete or outside the export")
    result = {}
    for mid, receipt in records.items():
        if not re.fullmatch(r"[1-9][0-9]{0,18}", mid) or not isinstance(receipt, dict):
            raise LoadError("Media resume receipt is invalid")
        source_media = missing[mid]
        if not source_media:
            raise LoadError("Media resume source attachment is empty")
        source_type = _media_type(source_media)
        source_status = source_media.get("export_status")
        source_reason = source_media.get("reason")
        if (source_type is None
                or source_status not in {None, "skipped", "not_requested"}
                or (source_status == "skipped" and source_reason not in _SKIP_REASONS)
                or (source_reason is not None and source_reason not in _SKIP_REASONS)):
            raise LoadError("Media resume source attachment is not a valid missing export")
        status = receipt.get("export_status")
        if status in {"skipped", "already_present"}:
            if receipt.get("reason") not in _SKIP_REASONS or receipt.get("path") is not None:
                raise LoadError("Media resume skip record is invalid")
            continue
        filename = receipt.get("path")
        size, digest = receipt.get("size"), receipt.get("sha256")
        limit = receipt.get("max_file_bytes", requested_limit)
        # A receipt from a later/higher-limit pass may be reused only within
        # the limit recorded for this checkpoint.  The actual file size must
        # still fit both limits; expose the effective (lower) limit downstream.
        if type(limit) is int and type(requested_limit) is int:
            limit = min(limit, requested_limit)
        receipt_type = _media_type(receipt, allow_missing=True)
        media_type = receipt_type or source_type
        if (
            status != "downloaded"
            or not isinstance(filename, str)
            or not re.fullmatch(rf"(?:media|media-resume)/{re.escape(mid)}/[^/\\\x00]+", filename)
            or filename.rsplit("/", 1)[1] in {".", ".."}
            or type(size) is not int or not 0 < size <= MAX_TELEGRAM_MEDIA_BYTES
            or type(limit) is not int or not 0 < limit <= MAX_TELEGRAM_MEDIA_BYTES
            or size > limit
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or (media_type is not None and (not isinstance(media_type, str)
                or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", media_type)))
            or media_type != source_type
        ):
            raise LoadError("Media resume receipt has an unsafe path or checksum")
        result[int(mid)] = {
            "path": filename, "size": size, "sha256": digest,
            "type": media_type, "max_file_bytes": validate_telegram_media_limit(limit),
            "export_status": "downloaded",
        }
    return result


def _media_type(value: dict, *, allow_missing: bool = False) -> str | None:
    """Read legacy media type aliases while rejecting conflicting metadata."""
    kinds = [value[key] for key in ("type", "media_type", "kind")
             if value.get(key) is not None]
    if not kinds and allow_missing:
        return None
    if (not kinds or any(not isinstance(kind, str)
                         or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", kind)
                         for kind in kinds)
            or len(set(kinds)) != 1):
        raise LoadError("Media resume source and receipt types are invalid")
    return kinds[0]


def resumed_photos(checkpoint: bytes, hashes: dict[str, str], messages) -> dict[int, dict]:
    """Compatibility wrapper for callers that used the original photo-only name."""
    return resumed_media(checkpoint, hashes, messages)
