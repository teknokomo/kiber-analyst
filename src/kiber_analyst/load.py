from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from kiber_analyst.config_paths import config_path
from kiber_analyst.errors import LoadError
from kiber_analyst.parse import NO_TOPIC_TITLE, iter_messages
from kiber_analyst.paths import job_paths, read_json, write_json_atomic

ENV_PATH = config_path("knowledge.conf")
PROJECT_SLUG = os.environ.get("KIBER_ANALYST_PROJECT", "default")
PROJECT_TITLE = "Default Project"


class _DryRunRollback(Exception):
    """Internal signal to roll back a fully executed dry-run transaction."""


class TelegramProjectConflict(LoadError):
    """A Telegram message reference is already owned by another project."""


class TelegramScopeConflict(LoadError):
    """A Telegram group/topic scope would overlap existing imported data."""


def document_title_for(topic_id: str | None, topic_title: str | None) -> str | None:
    return topic_title if topic_id is not None else NO_TOPIC_TITLE


def _required_env(path: Path) -> dict[str, str]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            values = dotenv_values(stream=stream)
    except OSError as exc:
        raise LoadError(
            f"Cannot read database environment file as current user: {exc}"
        ) from exc
    required = (
        "KNOWLEDGE_HOST",
        "KNOWLEDGE_PORT",
        "KNOWLEDGE_DB",
        "KNOWLEDGE_USER",
        "KNOWLEDGE_PASSWORD",
    )
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise LoadError(
            "Database environment is missing required keys: " + ", ".join(missing)
        )
    config = {key: str(values[key]) for key in required}
    if config["KNOWLEDGE_HOST"] != "127.0.0.1":
        raise LoadError("Knowledge database host must be 127.0.0.1")
    if config["KNOWLEDGE_PORT"] != "5433" or config["KNOWLEDGE_DB"] != "knowledge":
        raise LoadError("Knowledge database must be knowledge on loopback port 5433")
    if config["KNOWLEDGE_USER"] != "knowledge_app":
        raise LoadError("Load must connect as knowledge_app")
    return config


def _connect(config: dict[str, str]) -> Any:
    # psycopg is an optional dependency and deliberately stays out of imports
    # needed by fetch, parse, report, and fixture tests.
    try:
        import psycopg
    except ImportError as exc:
        raise LoadError(
            "Database support is unavailable; install kiber-analyst[db]"
        ) from exc
    return psycopg.connect(
        host=config["KNOWLEDGE_HOST"],
        port=int(config["KNOWLEDGE_PORT"]),
        dbname=config["KNOWLEDGE_DB"],
        user=config["KNOWLEDGE_USER"],
        password=config["KNOWLEDGE_PASSWORD"],
        connect_timeout=10,
    )


def _safe_error(exc: Exception, config: dict[str, str] | None) -> str:
    if isinstance(exc, TelegramProjectConflict):
        return "TELEGRAM_PROJECT_CONFLICT"
    if isinstance(exc, TelegramScopeConflict):
        return "TELEGRAM_SCOPE_CONFLICT"
    state = getattr(exc, "sqlstate", None)
    if not isinstance(state, str) or not re.fullmatch(r"[0-9A-Z]{5}", state):
        state = None
    return f"{type(exc).__name__}" + (f" (SQLSTATE {state})" if state else "")


def _snapshot_info(manifest: dict[str, Any]) -> tuple[str, int, str, str, str | None]:
    sha256 = manifest.get("snapshot_sha256")
    if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", sha256):
        raise LoadError(
            "manifest.json is missing a valid snapshot_sha256; fetch must complete first"
        )
    size = manifest.get("archive_size_bytes")
    if not isinstance(size, int) or size < 0:
        raise LoadError("manifest.json is missing a valid archive_size_bytes")
    uri = manifest.get("archive_uri")
    if not isinstance(uri, str) or not uri:
        raise LoadError("manifest.json is missing archive_uri")
    scope = manifest.get("scope")
    if scope not in {"topic", "chat"}:
        raise LoadError("manifest.json has unsupported scope")
    topic_id = manifest.get("scope_topic_id")
    return (
        sha256.lower(),
        size,
        uri,
        scope,
        str(topic_id) if topic_id is not None else None,
    )


def _group_key(record: dict[str, Any]) -> str | None:
    topic_id = record.get("topic_id")
    return str(topic_id) if topic_id is not None else None


def _precount_messages(path: Path) -> tuple[Counter[str | None], int]:
    counts: Counter[str | None] = Counter()
    for record in iter_messages(path):
        counts[_group_key(record)] += 1
    return counts, sum(counts.values())


def _assert_telegram_refs_project(
    cursor: Any, path: Path, project_slug: str, chat_id: str,
    scope: str, scope_topic_id: str | None,
) -> None:
    """Reject mixed overlapping scopes and exact refs owned by another project."""
    cursor.execute(
        """SELECT doc.meta->>'export_scope', doc.meta->>'topic_id'
           FROM documents doc
           JOIN sources src ON src.id = doc.source_id
           JOIN projects project ON project.id = src.project_id
           WHERE src.kind = 'telegram' AND src.external_id = %s
             AND project.slug = %s AND doc.doc_type = 'export'""",
        (chat_id, project_slug),
    )
    for prior_scope, prior_topic in cursor.fetchall():
        # Legacy topic documents have a topic_id but no explicit scope. A null
        # topic on a legacy row is ambiguous, so refuse a potentially mixed set.
        if prior_scope not in {"topic", "chat"}:
            prior_scope = "topic" if prior_topic is not None else "unknown"
        if prior_scope != scope or (scope == "topic" and prior_topic is None):
            raise TelegramScopeConflict(
                "Telegram group and topic scopes cannot share one project source"
            )

    references: list[str] = []

    def check_batch() -> None:
        if not references:
            return
        cursor.execute(
            """SELECT 1
               FROM segments seg
               JOIN documents doc ON doc.id = seg.document_id
               JOIN sources src ON src.id = doc.source_id
               JOIN projects project ON project.id = src.project_id
               WHERE seg.external_ref = ANY(%s)
                 AND project.slug IS DISTINCT FROM %s
               LIMIT 1""",
            (references, project_slug),
        )
        if cursor.fetchone() is not None:
            raise TelegramProjectConflict("Telegram source is already assigned to another project")
        references.clear()

    for record in iter_messages(path):
        record_chat_id = record.get("chat_id")
        message_id = record.get("message_id")
        message_id_text = str(message_id) if isinstance(message_id, (str, int)) else ""
        if (
            isinstance(record_chat_id, bool)
            or not isinstance(record_chat_id, (str, int))
            or str(record_chat_id) != chat_id
            or isinstance(message_id, bool)
            or re.fullmatch(r"[1-9][0-9]{0,18}", message_id_text) is None
        ):
            raise LoadError("Normalized Telegram message identity is invalid")
        references.append(f"tg:{chat_id}:{message_id_text}")
        if len(references) == 500:
            check_batch()
    check_batch()


def _assert_snapshot_scope(
    cursor: Any, source_id: str, sha256: str, scope: str, topic_id: str | None,
) -> None:
    cursor.execute(
        "SELECT scope, meta->>'topic_id' FROM snapshots WHERE source_id=%s::uuid AND sha256=%s",
        (source_id, sha256),
    )
    row = cursor.fetchone()
    if row is not None and (row[0] != scope or row[1] != topic_id):
        raise TelegramScopeConflict("Telegram snapshot hash is already recorded for another scope")


def _project_id(cursor: Any, project_slug: str = PROJECT_SLUG) -> str:
    project_title = PROJECT_TITLE if project_slug == PROJECT_SLUG else project_slug.replace("-", " ").title()
    cursor.execute(
        "INSERT INTO projects (slug, title) VALUES (%s, %s) "
        "ON CONFLICT (slug) DO NOTHING",
        (project_slug, project_title),
    )
    cursor.execute("SELECT id FROM projects WHERE slug = %s", (project_slug,))
    row = cursor.fetchone()
    if not row:
        raise LoadError("Could not create or find the configured project")
    return str(row[0])


def _source_id(cursor: Any, project_id: str, manifest: dict[str, Any]) -> str:
    parse = manifest.get("parse", {})
    chat_title = parse.get("chat_title") if isinstance(parse, dict) else None
    archive_name = str(manifest.get("archive_name") or "")
    archive_date_match = re.search(r"\d{4}-\d{2}-\d{2}", archive_name)
    chat_value = manifest.get("chat_id")
    if chat_value is None or not str(chat_value):
        raise LoadError("manifest.json is missing chat_id")
    chat_id = str(chat_value)
    internal_chat_id = chat_id.removeprefix("-100").removeprefix("-")
    # A source row represents the group; per-topic provenance belongs to documents.
    url = f"https://t.me/c/{internal_chat_id}"
    source_meta = {
        "snapshot_date": archive_date_match.group(0) if archive_date_match else None,
        "archive_md5": manifest.get("archive_md5"),
    }
    cursor.execute(
        """INSERT INTO sources (project_id, kind, external_id, url, title, meta)
           VALUES (%s::uuid, 'telegram', %s, %s, %s, %s::jsonb)
           ON CONFLICT (project_id, kind, external_id) DO UPDATE
             SET url = EXCLUDED.url, title = COALESCE(EXCLUDED.title, sources.title),
                 meta = sources.meta || EXCLUDED.meta
           RETURNING id""",
        (
            project_id,
            chat_id,
            url,
            chat_title,
            json.dumps(source_meta, ensure_ascii=False),
        ),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError("Could not create or find Telegram source")
    return str(row[0])


def _snapshot_id(
    cursor: Any,
    source_id: str,
    sha256: str,
    size: int,
    uri: str,
    scope: str,
    topic_id: str | None,
) -> str:
    meta = {"topic_id": topic_id}
    cursor.execute(
        """INSERT INTO snapshots (source_id, sha256, size_bytes, uri, scope, meta)
           VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb)
           ON CONFLICT (source_id, sha256) DO UPDATE
             SET size_bytes = EXCLUDED.size_bytes, uri = EXCLUDED.uri,
                 scope = EXCLUDED.scope, meta = snapshots.meta || EXCLUDED.meta
           RETURNING id""",
        (source_id, sha256, size, uri, scope, json.dumps(meta)),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError("Could not create or find archive snapshot")
    return str(row[0])


def _document_id(
    cursor: Any,
    source_id: str,
    topic_id: str | None,
    topic_title: str | None,
    snapshot_id: str,
    message_count: int,
    uri: str,
    scope: str,
) -> str:
    title = document_title_for(topic_id, topic_title)
    cursor.execute(
        """SELECT id FROM documents
           WHERE source_id = %s::uuid AND doc_type = 'export'
             AND meta->>'topic_id' IS NOT DISTINCT FROM %s
           ORDER BY created_at, id""",
        (source_id, topic_id),
    )
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise LoadError(f"Multiple export documents exist for topic {topic_id!r}")
    meta = {
        "topic_id": topic_id,
        "topic_title": title,
        "snapshot_id": snapshot_id,
        "message_count": message_count,
        "export_scope": scope,
    }
    meta_json = json.dumps(meta, ensure_ascii=False)
    if rows:
        document_id = str(rows[0][0])
        cursor.execute(
            "UPDATE documents SET raw_path = %s, meta = %s::jsonb WHERE id = %s::uuid",
            (uri, meta_json, document_id),
        )
        return document_id
    cursor.execute(
        """INSERT INTO documents (source_id, doc_type, raw_path, meta)
           VALUES (%s::uuid, 'export', %s, %s::jsonb) RETURNING id""",
        (source_id, uri, meta_json),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError(f"Could not create export document for topic {topic_id!r}")
    return str(row[0])


def _message_meta(record: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "topic_id": record.get("topic_id"),
        "topic_title": record.get("topic_title"),
        "attribution": record.get("attribution"),
        "author_id": record.get("author_id"),
        "reply_to": record.get("reply_to"),
        "forwarded_from": record.get("forwarded_from"),
        "media_type": record.get("media_type"),
        "media_path": record.get("media_path"),
        "edited_at": record.get("edited_at"),
        "date_raw": record.get("date_raw"),
        "url": record.get("url"),
    }
    extra = record.get("meta")
    if isinstance(extra, dict):
        meta.update(extra)
    return meta


def _posted_at(record: dict[str, Any]) -> datetime | None:
    value = record.get("date")
    if value is None:
        return None
    if not isinstance(value, str):
        raise LoadError("Normalized message date is not a string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise LoadError(f"Invalid normalized message date: {exc}") from exc
    if parsed.tzinfo is None:
        raise LoadError("Normalized message date has no timezone offset")
    return parsed


def _db_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _upsert_service_event(
    cursor: Any,
    event: dict[str, Any],
    *,
    source_id: str,
    snapshot_id: str,
    chat_id: str,
    topic_id: str,
    provenance: dict[str, Any],
) -> str:
    event_id = event.get("event_id")
    event_type = event.get("type")
    if type(event_id) is not int or event_id <= 0 or not isinstance(event_type, str):
        raise LoadError("Telegram service event identity is invalid")
    payload = {
        key: event.get(key)
        for key in (
            "actor_id", "actor_name", "target_id", "target_name", "message_id",
            "old_value", "new_value", "raw_action", "error_code",
        )
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    event_sha256 = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    occurred_at: datetime | None = None
    date_value = event.get("date")
    if date_value is not None:
        if not isinstance(date_value, str):
            raise LoadError("Telegram service event date is invalid")
        try:
            occurred_at = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
        except ValueError:
            raise LoadError("Telegram service event date is invalid") from None
        if occurred_at.tzinfo is None:
            raise LoadError("Telegram service event date has no timezone")
    external_ref = f"tg:{chat_id}:service:{event_id}"
    cursor.execute(
        """SELECT id, snapshot_id, event_sha256, provenance FROM telegram_service_events
           WHERE source_id = %s::uuid AND external_ref = %s FOR UPDATE""",
        (source_id, external_ref),
    )
    existing = cursor.fetchone()
    if existing:
        old_id, old_snapshot, old_sha, old_provenance_value = existing
        old_provenance = _db_json(old_provenance_value)
        cursor.execute(
            "SELECT topic_id FROM telegram_service_events WHERE id=%s FOR UPDATE",
            (old_id,),
        )
        old_topic = cursor.fetchone()
        if old_topic is not None and str(old_topic[0]) != topic_id:
            raise TelegramScopeConflict("Telegram service event identity changed topic scope")
        if str(old_snapshot) == snapshot_id and old_sha == event_sha256 and old_provenance == provenance:
            return "skipped"
        cursor.execute(
            """UPDATE telegram_service_events
               SET snapshot_id = %s::uuid, event_type = %s, topic_id = %s,
                   occurred_at = %s, payload = %s::jsonb, provenance = %s::jsonb,
                   event_sha256 = %s, updated_at = now()
               WHERE id = %s""",
            (
                snapshot_id, event_type, topic_id, occurred_at, encoded,
                json.dumps(provenance, sort_keys=True), event_sha256, old_id,
            ),
        )
        return "updated"
    cursor.execute(
        """INSERT INTO telegram_service_events
           (source_id, snapshot_id, external_ref, event_id, topic_id, event_type,
            occurred_at, payload, provenance, event_sha256)
           VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)""",
        (
            source_id, snapshot_id, external_ref, event_id, topic_id, event_type,
            occurred_at, encoded, json.dumps(provenance, sort_keys=True), event_sha256,
        ),
    )
    return "inserted"


def _service_file_info(path: Path, service_manifest: dict[str, Any]) -> tuple[str, int]:
    from kiber_analyst.telegram_ingest import _EVENT_TYPES, _iter_jsonl

    scope = service_manifest.get("scope", "topic")
    if scope not in {"topic", "chat"}:
        raise LoadError("Telegram service event scope is invalid")
    digest = hashlib.sha256()
    count = 0
    seen: set[int] = set()
    for raw, event in _iter_jsonl(path):
        event_id = event.get("event_id")
        topic_id = event.get("topic_id")
        if type(event_id) is not int or event_id <= 0 or event_id in seen:
            raise LoadError("Telegram service event IDs are invalid or duplicated")
        valid_topic = (
            type(topic_id) is int and topic_id > 0
            if scope == "topic"
            else topic_id is None or (type(topic_id) is int and topic_id > 0)
        )
        if event.get("type") not in _EVENT_TYPES or not valid_topic:
            raise LoadError("Telegram service event fields are invalid")
        seen.add(event_id)
        digest.update(raw)
        count += 1
    expected_sha = service_manifest.get("services_sha256")
    if digest.hexdigest() != expected_sha or service_manifest.get("event_count") != count:
        raise LoadError("Telegram service data changed after validation")
    return digest.hexdigest(), count


def _upsert_segment(
    cursor: Any,
    record: dict[str, Any],
    document_id: str,
    seq: int,
    project_id: str,
) -> str:
    message_id = record.get("message_id")
    chat_id = record.get("chat_id")
    if message_id is None or chat_id is None:
        raise LoadError("Normalized message is missing chat_id or message_id")
    external_ref = f"tg:{chat_id}:{message_id}"
    content_hash = record.get("content_hash")
    text = record.get("text")
    if text is None:
        text = ""
    if not isinstance(text, str):
        raise LoadError("Normalized message text is not a string")
    incoming_meta = _message_meta(record)
    posted_at = _posted_at(record)
    speaker = record.get("author") if isinstance(record.get("author"), str) else None
    cursor.execute(
        """SELECT seg.id, seg.document_id, seg.seq, seg.content_hash,
                  seg.speaker, seg.posted_at, seg.meta, src.project_id
           FROM segments seg
           JOIN documents doc ON doc.id = seg.document_id
           JOIN sources src ON src.id = doc.source_id
           WHERE seg.external_ref = %s""",
        (external_ref,),
    )
    existing = cursor.fetchone()
    if existing:
        (
            segment_id,
            old_doc,
            old_seq,
            old_hash,
            old_speaker,
            old_posted,
            old_meta_value,
            old_project_id,
        ) = existing
        if str(old_project_id) != project_id:
            raise TelegramProjectConflict(
                "Telegram source is already assigned to another project"
            )
        old_meta = _db_json(old_meta_value)
        if old_meta.get("topic_id") != incoming_meta.get("topic_id"):
            raise TelegramScopeConflict("Telegram message identity changed topic scope")
        text_changed = old_hash != content_hash
        was_absent = old_meta.get("presence") == "absent_in_snapshot"
        placement_changed = str(old_doc) != document_id or int(old_seq) != seq
        metadata_changed = {
            k: v
            for k, v in old_meta.items()
            if k not in {"revision", "presence", "snapshot_id"}
        } != incoming_meta
        timestamp_changed = old_posted != posted_at
        speaker_changed = old_speaker != speaker
        if not any(
            (
                text_changed,
                was_absent,
                placement_changed,
                metadata_changed,
                timestamp_changed,
                speaker_changed,
            )
        ):
            return "skipped"
        new_meta = dict(incoming_meta)
        if text_changed:
            new_meta["revision"] = int(old_meta.get("revision", 0)) + 1
        elif "revision" in old_meta:
            new_meta["revision"] = old_meta["revision"]
        cursor.execute(
            """UPDATE segments SET document_id = %s::uuid, seq = %s, speaker = %s,
                      text = %s, content_hash = %s, meta = %s::jsonb,
                      posted_at = %s, start_ms = NULL, end_ms = NULL
                   WHERE id = %s""",
            (
                document_id,
                seq,
                speaker,
                text,
                content_hash,
                json.dumps(new_meta, ensure_ascii=False),
                posted_at,
                segment_id,
            ),
        )
        return "updated"

    cursor.execute(
        """INSERT INTO segments
             (document_id, seq, start_ms, end_ms, speaker, text,
              external_ref, content_hash, meta, posted_at)
           VALUES (%s::uuid, %s, NULL, NULL, %s, %s, %s, %s, %s::jsonb, %s)""",
        (
            document_id,
            seq,
            speaker,
            text,
            external_ref,
            content_hash,
            json.dumps(incoming_meta, ensure_ascii=False),
            posted_at,
        ),
    )
    return "inserted"


def _mark_absent(
    cursor: Any,
    document_id: str,
    present_refs: set[str],
    snapshot_id: str,
) -> int:
    cursor.execute(
        "SELECT id, external_ref, meta FROM segments WHERE document_id = %s::uuid",
        (document_id,),
    )
    changed = 0
    for segment_id, external_ref, meta_value in cursor.fetchall():
        if external_ref in present_refs:
            continue
        meta = _db_json(meta_value)
        if (
            meta.get("presence") == "absent_in_snapshot"
            and meta.get("snapshot_id") == snapshot_id
        ):
            continue
        meta["presence"] = "absent_in_snapshot"
        meta["snapshot_id"] = snapshot_id
        cursor.execute(
            "UPDATE segments SET meta = %s::jsonb WHERE id = %s",
            (json.dumps(meta, ensure_ascii=False), segment_id),
        )
        changed += 1
    return changed


def _mark_absent_in_scope(
    cursor: Any,
    source_id: str,
    scope: str,
    scope_topic_id: str | None,
    refs_by_doc: dict[str, set[str]],
    snapshot_id: str,
) -> int:
    cursor.execute(
        """SELECT id, meta->>'topic_id' FROM documents
           WHERE source_id = %s::uuid AND doc_type = 'export'
           ORDER BY id""",
        (source_id,),
    )
    documents = cursor.fetchall()
    if scope == "chat":
        document_ids = [str(document_id) for document_id, _ in documents]
    elif scope == "topic" and scope_topic_id is not None:
        document_ids = [
            str(document_id)
            for document_id, topic_id in documents
            if topic_id == scope_topic_id
        ]
    else:
        document_ids = []

    return sum(
        _mark_absent(
            cursor,
            document_id,
            refs_by_doc.get(document_id, set()),
            snapshot_id,
        )
        for document_id in document_ids
    )


def _write_failed_run(
    config: dict[str, str],
    manifest: dict[str, Any],
    job_dir: Path,
    error: str,
    project_slug: str = PROJECT_SLUG,
) -> None:
    conn = _connect(config)
    try:
        with conn.transaction(), conn.cursor() as cursor:
            project_id = _project_id(cursor, project_slug)
            checkpoint = {
                "job_dir": str(job_dir.resolve()),
                "snapshot_sha256": manifest.get("snapshot_sha256"),
                "error": error,
            }
            cursor.execute(
                """INSERT INTO runs (project_id, kind, status, checkpoint, finished_at, error)
                   VALUES (%s::uuid, 'telegram_ingest', 'failed', %s::jsonb, now(), %s)""",
                (project_id, json.dumps(checkpoint), error),
            )
    finally:
        conn.close()


def load_job(
    job_dir: Path,
    *,
    env_path: Path = ENV_PATH,
    dry_run: bool = False,
    service_events_path: Path | None = None,
    service_manifest: dict[str, Any] | None = None,
    project_slug: str = PROJECT_SLUG,
) -> dict[str, Any]:
    paths = job_paths(job_dir)
    try:
        manifest = read_json(paths["manifest"])
    except (OSError, ValueError, TypeError) as exc:
        raise LoadError(f"Cannot read job manifest: {exc}") from exc
    if not paths["messages"].is_file():
        raise LoadError(f"Normalized messages file not found: {paths['messages']}")
    config = _required_env(env_path)
    sha256, size, uri, scope, scope_topic_id = _snapshot_info(manifest)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", project_slug):
        raise LoadError("Project slug is invalid")
    if (service_events_path is None) != (service_manifest is None):
        raise LoadError("Telegram service data and manifest must be provided together")
    service_sha256: str | None = None
    service_event_count = 0
    if service_events_path is not None and service_manifest is not None:
        if service_manifest.get("complete") is not True or service_manifest.get("range_complete") is not True:
            raise LoadError("Telegram service export is incomplete")
        if service_manifest.get("scope", "topic") != manifest.get("scope", "topic"):
            raise LoadError("Telegram service export scope does not match the message snapshot")
        service_sha256, service_event_count = _service_file_info(service_events_path, service_manifest)
        if manifest.get("service_events_sha256") != service_sha256:
            raise LoadError("Telegram service snapshot does not match the normalized manifest")
    message_counts, messages_total = _precount_messages(paths["messages"])
    media_missing_value = manifest.get("media_missing_count", 0)
    if type(media_missing_value) is not int or media_missing_value < 0:
        raise LoadError("Normalized Telegram media-missing count is invalid")
    media_missing = media_missing_value
    if scope == "topic" and scope_topic_id is not None:
        message_counts.setdefault(scope_topic_id, 0)
    started_at = datetime.now(UTC)
    counters: Counter[str] = Counter()
    service_counters: Counter[str] = Counter()
    absent_count = 0
    project_id: str | None = None
    snapshot_id: str | None = None
    run_id: str | None = None
    rolled_back = False

    try:
        conn = _connect(config)
        try:
            with conn.transaction(), conn.cursor() as cursor:
                chat_value = manifest.get("chat_id")
                if (
                    isinstance(chat_value, bool)
                    or not isinstance(chat_value, (str, int))
                    or re.fullmatch(r"-?[1-9][0-9]{0,18}", str(chat_value)) is None
                ):
                    raise LoadError("manifest.json is missing a valid chat_id")
                chat_id = str(chat_value)
                # Lock by Telegram chat identity, not project-specific source UUID,
                # so concurrent imports into different projects cannot race.
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (f"telegram-project-import:{chat_id}",),
                )
                _assert_telegram_refs_project(
                    cursor, paths["messages"], project_slug, chat_id, scope, scope_topic_id,
                )
                project_id = _project_id(cursor, project_slug)
                source_id = _source_id(cursor, project_id, manifest)
                _assert_snapshot_scope(cursor, source_id, sha256, scope, scope_topic_id)
                snapshot_id = _snapshot_id(
                    cursor, source_id, sha256, size, uri, scope, scope_topic_id
                )
                import_key = manifest.get("telegram_import_key")
                topic_data = {
                    str(item.get("topic_id")): item.get("topic_title")
                    for item in manifest.get("parse", {}).get("topics", [])
                    if isinstance(item, dict) and item.get("topic_id") is not None
                }
                doc_ids: dict[str | None, str] = {}
                refs_by_doc: dict[str, set[str]] = defaultdict(set)
                seq_by_doc: Counter[str] = Counter()
                for topic_id, message_count in message_counts.items():
                    doc_id = _document_id(
                        cursor,
                        source_id,
                        topic_id,
                        topic_data.get(topic_id)
                        if topic_id is not None
                        else NO_TOPIC_TITLE,
                        snapshot_id,
                        message_count,
                        uri,
                        scope,
                    )
                    doc_ids[topic_id] = doc_id
                prior_run = None
                if isinstance(import_key, str):
                    cursor.execute(
                        """SELECT id FROM runs WHERE project_id = %s::uuid
                           AND kind = 'telegram_ingest' AND status = 'done'
                           AND checkpoint->>'import_key' = %s
                           ORDER BY started_at DESC LIMIT 1""",
                        (project_id, import_key),
                    )
                    prior_run = cursor.fetchone()
                if prior_run:
                    run_id = str(prior_run[0])
                    counters["skipped"] = messages_total
                    service_counters["skipped"] = service_event_count
                    if manifest.get("media_resume_count", 0):
                        for record in iter_messages(paths["messages"]):
                            relative = record.get("media_path")
                            if not isinstance(relative, str) or not relative.startswith("media-resume/"):
                                continue
                            media_meta = record["meta"]["media_export"]
                            identity = (doc_ids[_group_key(record)],
                                        f"tg:{record['chat_id']}:{record['message_id']}",
                                        record["content_hash"])
                            cursor.execute(
                                """UPDATE segments
                                   SET meta = meta || %s::jsonb
                                   WHERE document_id = %s::uuid AND external_ref = %s
                                     AND content_hash = %s AND meta->>'media_path' IS NULL""",
                                (json.dumps({"media_path": relative, "media_type": record["media_type"],
                                             "media_export": media_meta}),
                                 *identity),
                            )
                            if cursor.rowcount:
                                counters["updated"] += cursor.rowcount
                                counters["skipped"] -= cursor.rowcount
                            else:
                                cursor.execute(
                                    """SELECT 1 FROM segments WHERE document_id = %s::uuid
                                       AND external_ref = %s AND content_hash = %s
                                       AND meta->>'media_path' IS NOT NULL""", identity,
                                )
                                if cursor.fetchone() is None:
                                    raise LoadError("Media resume segment does not match the loaded snapshot")
                else:
                    cursor.execute(
                        """INSERT INTO runs (project_id, kind, status, checkpoint)
                           VALUES (%s::uuid, 'telegram_ingest', 'running', %s::jsonb)
                           RETURNING id""",
                        (
                            project_id,
                            json.dumps({
                                "job_dir": str(job_dir.resolve()),
                                "snapshot_sha256": sha256,
                                "messages_total": messages_total,
                                "import_key": import_key,
                            }),
                        ),
                    )
                    run_row = cursor.fetchone()
                    run_id = str(run_row[0]) if run_row else None
                    if run_id is None:
                        raise LoadError("Could not create ingestion run row")

                    for record in iter_messages(paths["messages"]):
                        topic_id = _group_key(record)
                        document_id = doc_ids.get(topic_id)
                        if document_id is None:
                            raise LoadError(f"No export document prepared for topic {topic_id!r}")
                        seq_by_doc[document_id] += 1
                        outcome = _upsert_segment(
                            cursor, record, document_id, seq_by_doc[document_id], project_id,
                        )
                        counters[outcome] += 1
                        message_ref = f"tg:{record.get('chat_id')}:{record.get('message_id')}"
                        refs_by_doc[document_id].add(message_ref)

                    if service_events_path is not None and service_manifest is not None:
                        from kiber_analyst.telegram_ingest import _iter_jsonl

                        provenance = {
                            "import_key": import_key,
                            "snapshot_sha256": sha256,
                            "services_sha256": service_sha256,
                            "source_id": manifest.get("source_id"),
                        }
                        chat_value = manifest.get("chat_id")
                        if chat_value is None or not str(chat_value):
                            raise LoadError("manifest.json is missing chat_id")
                        chat_id = str(chat_value)
                        for _raw, event in _iter_jsonl(service_events_path):
                            event_topic = event.get("topic_id")
                            if (scope == "topic" and str(event_topic) != str(scope_topic_id)):
                                raise LoadError("Telegram service event is outside the exported topic")
                            if (scope == "chat" and event_topic is not None
                                    and (type(event_topic) is not int or event_topic <= 0)):
                                raise LoadError("Telegram service event has an invalid topic ID")
                            service_counters[_upsert_service_event(
                                cursor, event, source_id=source_id, snapshot_id=snapshot_id,
                                chat_id=chat_id,
                                topic_id=str(event_topic) if event_topic is not None else "0",
                                provenance=provenance,
                            )] += 1

                    absent_count = _mark_absent_in_scope(
                        cursor, source_id, scope, scope_topic_id, refs_by_doc, snapshot_id
                    )
                    checkpoint = {
                        "job_dir": str(job_dir.resolve()),
                        "snapshot_sha256": sha256,
                        "import_key": import_key,
                        "inserted": counters["inserted"],
                        "updated": counters["updated"],
                        "skipped": counters["skipped"],
                        "media_missing": media_missing,
                        "service_events": sum(service_counters.values()),
                        "service_events_inserted": service_counters["inserted"],
                        "service_events_updated": service_counters["updated"],
                        "service_events_skipped": service_counters["skipped"],
                        "absent": absent_count,
                    }
                    cursor.execute(
                        """UPDATE runs SET status = 'done', checkpoint = %s::jsonb,
                                  finished_at = now() WHERE id = %s::uuid""",
                        (json.dumps(checkpoint), run_id),
                    )
                if dry_run:
                    raise _DryRunRollback
        finally:
            conn.close()
    except _DryRunRollback:
        rolled_back = True
    except Exception as exc:
        safe_error = _safe_error(exc, config)
        failed_run_recorded = False
        if not dry_run and not isinstance(exc, TelegramProjectConflict):
            try:
                _write_failed_run(config, manifest, job_dir, safe_error, project_slug)
                failed_run_recorded = True
            except Exception as failed_run_exc:  # noqa: BLE001 - failed-run write is best effort
                safe_error += f"; failed-run record could not be written ({type(failed_run_exc).__name__})"
        manifest["load"] = {
            "status": "dry_run_failed" if dry_run else "failed",
            "dry_run": dry_run,
            "transaction": "rolled_back",
            "rollback_confirmed": dry_run,
            "inserted": counters["inserted"],
            "updated": counters["updated"],
            "skipped": counters["skipped"],
            "absent": absent_count,
            "media_missing": media_missing,
            "service_events": sum(service_counters.values()),
            "service_events_inserted": service_counters["inserted"],
            "service_events_updated": service_counters["updated"],
            "service_events_skipped": service_counters["skipped"],
            "errors": 1,
            "project_slug": project_slug,
            "project_id": None,
            "failed_run_recorded": failed_run_recorded,
            "error": safe_error,
        }
        write_json_atomic(paths["manifest"], manifest)
        raise LoadError(safe_error) from exc

    result = {
        "status": "dry_run" if rolled_back else "done",
        "dry_run": dry_run,
        "transaction": "rolled_back" if rolled_back else "committed",
        "rollback_confirmed": rolled_back,
        "inserted": counters["inserted"],
        "updated": counters["updated"],
        "skipped": counters["skipped"],
        "absent": absent_count,
        "media_missing": media_missing,
        "service_events": sum(service_counters.values()),
        "service_events_inserted": service_counters["inserted"],
        "service_events_updated": service_counters["updated"],
        "service_events_skipped": service_counters["skipped"],
        "errors": 0,
        "project_slug": project_slug,
        "project_id": None if rolled_back else project_id,
        "snapshot_id": None if rolled_back else snapshot_id,
        "run_id": None if rolled_back else run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
    }
    manifest["load"] = result
    write_json_atomic(paths["manifest"], manifest)
    return result
