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
    if not isinstance(data, dict) or data.get("inputs") != hashes:
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
        media = row.get("media") or {}
        if isinstance(media, dict) and media.get("path") is None:
            missing[str(row["message_id"])] = media
        if isinstance(media, dict) and media.get("reason") in {"over_100_MiB", "over_limit"}:
            over_limit.append(row["message_id"])
    recorded_over_limit = data.get("over_limit_message_ids")
    if (not records.keys() <= missing.keys()
            or (recorded_over_limit is not None
                and recorded_over_limit != sorted(over_limit))):
        raise LoadError("Media resume checkpoint is incomplete or outside the export")
    result = {}
    for mid, receipt in records.items():
        if not re.fullmatch(r"[1-9][0-9]{0,18}", mid) or not isinstance(receipt, dict):
            raise LoadError("Media resume receipt is invalid")
        status = receipt.get("export_status")
        if status in {"skipped", "already_present"}:
            if receipt.get("reason") not in _SKIP_REASONS or receipt.get("path") is not None:
                raise LoadError("Media resume skip record is invalid")
            continue
        filename = receipt.get("path")
        size, digest = receipt.get("size"), receipt.get("sha256")
        limit = receipt.get("max_file_bytes", requested_limit)
        media_type = receipt.get("type", receipt.get("kind", missing[mid].get("kind")))
        if (
            status != "downloaded"
            or not isinstance(filename, str)
            or not re.fullmatch(rf"media-resume/{re.escape(mid)}/[^/\\\x00]+", filename)
            or filename.rsplit("/", 1)[1] in {".", ".."}
            or type(size) is not int or not 0 < size <= MAX_TELEGRAM_MEDIA_BYTES
            or type(limit) is not int or not 0 < limit <= MAX_TELEGRAM_MEDIA_BYTES
            or limit > requested_limit or size > limit
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or (media_type is not None and (not isinstance(media_type, str)
                or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", media_type)))
            or (missing[mid].get("kind") is not None
                and media_type is not None
                and missing[mid]["kind"] != media_type)
        ):
            raise LoadError("Media resume receipt has an unsafe path or checksum")
        result[int(mid)] = {
            "path": filename, "size": size, "sha256": digest,
            "type": media_type, "max_file_bytes": validate_telegram_media_limit(limit),
            "export_status": "downloaded",
        }
    return result


def resumed_photos(checkpoint: bytes, hashes: dict[str, str], messages) -> dict[int, dict]:
    """Compatibility wrapper for callers that used the original photo-only name."""
    return resumed_media(checkpoint, hashes, messages)
