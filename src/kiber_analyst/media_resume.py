"""Validate a completed Telegram media repair checkpoint and its file references."""
from __future__ import annotations

import json
import re

from kiber_analyst.errors import LoadError


def resumed_photos(checkpoint: bytes, hashes: dict[str, str], messages) -> dict[int, dict]:
    data = json.loads(checkpoint)
    if not isinstance(data, dict) or data.get("inputs") != hashes:
        raise LoadError("Media resume checkpoint does not match the export")
    records = data.get("records")
    if not isinstance(records, dict):
        raise LoadError("Media resume checkpoint has invalid records")
    missing = {}
    over_limit = []
    for row in messages:
        media = row.get("media") or {}
        if media.get("reason") == "unknown_size" and media.get("path") is None:
            missing[str(row["message_id"])] = media
        elif media.get("reason") == "over_100_MiB":
            over_limit.append(row["message_id"])
    if records.keys() != missing.keys() or data.get("over_limit_message_ids") != sorted(over_limit):
        raise LoadError("Media resume checkpoint is incomplete or outside the export")
    result = {}
    for mid, receipt in records.items():
        if not isinstance(receipt, dict):
            raise LoadError("Media resume receipt is invalid")
        if receipt.get("export_status") in {"skipped", "already_present"}:
            continue
        filename = receipt.get("path")
        size, digest = receipt.get("size"), receipt.get("sha256")
        if (
            receipt.get("export_status") != "downloaded"
            or missing[mid].get("kind") != "photo"
            or not isinstance(filename, str)
            or not re.fullmatch(rf"media-resume/{re.escape(mid)}/[^/\\\x00]+", filename)
            or filename.rsplit("/", 1)[1] in {".", ".."}
            or type(size) is not int or not 0 < size <= 100 * 1024**2
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise LoadError("Media resume receipt has an unsafe path or checksum")
        result[int(mid)] = {"path": filename, "size": size, "sha256": digest,
                            "type": "photo", "export_status": "downloaded"}
    return result
