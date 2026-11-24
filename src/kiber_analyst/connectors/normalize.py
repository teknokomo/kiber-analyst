"""Deterministic normalization into threads.jsonl / items.jsonl records (ТЗ §5.4, §7.2).

Same raw input always yields byte-identical JSONL: fields are validated, values
are canonicalized, ordering is caller-provided and stable, and text hashing uses
the same normalized-content rule as the Telegram pipeline.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.source_ids import (
    item_ref as build_item_ref,
)
from kiber_analyst.connectors.source_ids import (
    parse_thread_ref,
)
from kiber_analyst.parse import normalized_content_hash

_STATE = re.compile(r"[a-z_]{1,24}\Z")
_NAME = re.compile(r".{0,200}\Z", re.DOTALL)


def content_hash(text: str) -> str:
    return normalized_content_hash(text)


def parse_posted_at(value: Any, *, what: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} date is not a string"
        )
    candidate = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} date is not ISO 8601"
        ) from None
    if parsed.tzinfo is None:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} date has no timezone offset"
        )
    return value


def clean_text(value: Any, *, what: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} text is not a string"
        )
    if len(value) > 1_000_000:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} text is oversized"
        )
    return value


def clean_author(value: Any, *, what: str = "author") -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _NAME.fullmatch(value) is None:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} name is invalid"
        )
    return value


def clean_state(value: Any, *, what: str = "state") -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _STATE.fullmatch(value) is None:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, f"{what} value is invalid"
        )
    return value


def clean_labels(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, "labels must be a list of strings"
        )
    return sorted(value)


def clean_url(value: Any, *, what: str = "url") -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        return None
    if len(value) > 2000:
        return None
    return value


def thread_record(
    *,
    thread_ref_value: str,
    title: str,
    state: str | None,
    author: str | None,
    created_at: str | None,
    updated_at: str | None,
    url: str | None,
    labels: list[str],
    meta: dict[str, Any],
) -> dict[str, Any]:
    parsed = parse_thread_ref(thread_ref_value)
    if parsed is None:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, "thread_ref format is invalid"
        )
    record = {
        "thread_ref": thread_ref_value,
        "provider": parsed[0].provider,
        "kind": parsed[1],
        "number": parsed[2],
        "title": clean_text(title, what="thread title")[:2000],
        "state": clean_state(state),
        "author": clean_author(author, what="thread author"),
        "created_at": parse_posted_at(created_at, what="thread created_at"),
        "updated_at": parse_posted_at(updated_at, what="thread updated_at"),
        "url": clean_url(url, what="thread url"),
        "labels": clean_labels(labels),
        "meta": meta,
    }
    _require_jsonable(record)
    return record


def item_record(
    *,
    thread_ref_value: str,
    item_kind: str,
    external_id: str | None,
    parent_external_ref: str | None = None,
    author: str | None,
    text: str,
    posted_at: str | None,
    url: str | None,
    meta: dict[str, Any],
) -> dict[str, Any]:
    external_ref_value = build_item_ref(thread_ref_value, item_kind, external_id)
    if parent_external_ref is not None and not isinstance(parent_external_ref, str):
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED, "parent_external_ref must be a string"
        )
    record = {
        "external_ref": external_ref_value,
        "thread_ref": thread_ref_value,
        "item_kind": item_kind,
        "parent_external_ref": parent_external_ref,
        "author": clean_author(author),
        "text": clean_text(text, what="item"),
        "posted_at": parse_posted_at(posted_at, what="item posted_at"),
        "url": clean_url(url, what="item url"),
        "content_hash": normalized_content_hash(clean_text(text, what="item")),
        "meta": meta,
    }
    _require_jsonable(record)
    return record


def is_nonempty(record: dict[str, Any]) -> bool:
    return bool(record.get("text", "").strip())


def _require_jsonable(record: dict[str, Any]) -> None:
    import json

    try:
        json.dumps(record, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ConnectorError(
            ConnectorCodes.NORMALIZE_FAILED,
            "normalized record contains a non-serializable value",
        ) from exc
