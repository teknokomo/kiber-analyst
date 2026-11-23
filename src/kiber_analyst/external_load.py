"""PostgreSQL loader for normalized external discussion bundles (ТЗ 0.2.0 §7).

Input is the bundle written by the external_ingest orchestration:
- normalized/threads.jsonl  (one record per discussion)
- normalized/items.jsonl    (one record per body/comment/review/reply/event)
- normalized/manifest.json  (identity, counts, checkpoints, errors)

Everything runs in a single transaction with an advisory lock per source:
sources/documents/segments are upserted by stable external references, edits
bump meta.revision and keep meta.previous_content_hash, and items are marked
deleted only when their thread was completely re-fetched (an explicit upstream
signal). Nothing is ever deleted physically.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kiber_analyst.connectors.errors import ConnectorError
from kiber_analyst.connectors.source_ids import parse_source_id, parse_thread_ref
from kiber_analyst.errors import LoadError
from kiber_analyst.load import (
    ENV_PATH,
    PROJECT_SLUG,
    _connect,
    _db_json,
    _project_id,
    _required_env,
    _safe_error,
)
from kiber_analyst.paths import read_json, write_json_atomic

_SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
_THREAD_ITEMS = ("body", "comment", "review", "review_comment", "reply", "event")
_TEXT_ITEM_KINDS = frozenset({"body", "comment", "review", "review_comment", "reply"})


class _DryRunRollback(Exception):
    """Internal signal to roll back a fully executed dry-run transaction."""


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise LoadError(f"Normalized JSONL record {number} is invalid") from None
            if not isinstance(value, dict):
                raise LoadError(f"Normalized JSONL record {number} is not an object")
            yield value


def _bundle_paths(job_dir: Path) -> dict[str, Path]:
    return {
        "root": job_dir,
        "normalized": job_dir / "normalized",
        "threads": job_dir / "normalized" / "threads.jsonl",
        "items": job_dir / "normalized" / "items.jsonl",
        "manifest": job_dir / "normalized" / "manifest.json",
    }


def _validate_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    sha256 = manifest.get("snapshot_sha256")
    if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise LoadError("External manifest is missing a valid snapshot_sha256")
    source_id = manifest.get("source_id")
    parsed = parse_source_id(source_id) if isinstance(source_id, str) else None
    if parsed is None:
        raise LoadError("External manifest has an invalid source_id")
    provider = manifest.get("provider")
    if provider != parsed.provider:
        raise LoadError("External manifest provider does not match its source_id")
    for key in ("thread_count", "item_count"):
        if type(manifest.get(key)) is not int or manifest[key] < 0:
            raise LoadError(f"External manifest has an invalid {key}")
    fetched_at = manifest.get("fetched_at")
    if not isinstance(fetched_at, str) or not fetched_at:
        raise LoadError("External manifest is missing fetched_at")
    if not isinstance(manifest.get("threads_complete"), dict):
        raise LoadError("External manifest is missing threads_complete")
    import_key = manifest.get("external_import_key")
    if not isinstance(import_key, str) or not re.fullmatch(r"[0-9a-f]{64}", import_key):
        raise LoadError("External manifest is missing external_import_key")
    return {
        "sha256": sha256,
        "source_id": source_id,
        "provider": provider,
        "import_key": import_key,
        "fetched_at": fetched_at,
    }


def _validate_thread(record: dict[str, Any], manifest: dict[str, Any]) -> None:
    thread_ref_value = record.get("thread_ref")
    parsed = parse_thread_ref(thread_ref_value) if isinstance(thread_ref_value, str) else None
    if parsed is None:
        raise LoadError("Normalized thread record has an invalid thread_ref")
    if parsed[0].external_id != manifest["source_id"].split(":", 1)[1]:
        raise LoadError("Normalized thread belongs to another source")
    if record.get("provider") != manifest["provider"]:
        raise LoadError("Normalized thread provider does not match the manifest")
    if not isinstance(record.get("title"), str):
        raise LoadError("Normalized thread record is missing its title")


def _validate_item(record: dict[str, Any], thread_refs: set[str]) -> None:
    external_ref = record.get("external_ref")
    thread_ref_value = record.get("thread_ref")
    if not isinstance(external_ref, str) or not isinstance(thread_ref_value, str):
        raise LoadError("Normalized item record is missing its references")
    if thread_ref_value not in thread_refs:
        raise LoadError("Normalized item references an unknown thread")
    if not external_ref.startswith(thread_ref_value + ":"):
        raise LoadError("Normalized item external_ref does not extend its thread")
    if record.get("item_kind") not in _THREAD_ITEMS:
        raise LoadError("Normalized item has an unknown item_kind")
    if not isinstance(record.get("text"), str):
        raise LoadError("Normalized item text is not a string")
    if not isinstance(record.get("content_hash"), str):
        raise LoadError("Normalized item is missing content_hash")
    if not isinstance(record.get("meta"), dict):
        raise LoadError("Normalized item meta is not an object")


def _posted_at(record: dict[str, Any]) -> datetime | None:
    value = record.get("posted_at")
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise LoadError("Normalized item date is invalid") from None
    if parsed.tzinfo is None:
        raise LoadError("Normalized item date has no timezone offset")
    return parsed


def _source_row(
    cursor: Any, project_id: str, manifest: dict[str, Any], identity: dict[str, Any]
) -> str:
    source_id = identity["source_id"]
    external_id = source_id.split(":", 1)[1]
    provider = identity["provider"]
    parsed = parse_source_id(source_id)
    if parsed is not None and parsed.host is not None:
        # GitLab always carries the host; GitHub Enterprise carries it too.
        url = f"https://{external_id}"
    elif provider == "github":
        url = f"https://github.com/{external_id}"
    elif provider == "gitverse":
        url = f"https://gitverse.ru/{external_id}"
    else:
        url = None
    title = external_id
    meta = {"provider": provider}
    cursor.execute(
        """INSERT INTO sources (project_id, kind, external_id, url, title, meta)
           VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb)
           ON CONFLICT (project_id, kind, external_id) DO UPDATE
             SET url = COALESCE(EXCLUDED.url, sources.url),
                 title = COALESCE(EXCLUDED.title, sources.title),
                 meta = sources.meta || EXCLUDED.meta
           RETURNING id""",
            (project_id, provider, external_id, url, title, json.dumps(meta)),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError("Could not create or find the external source")
    return str(row[0])


def _snapshot_row(
    cursor: Any, source_id: str, manifest: dict[str, Any], identity: dict[str, Any]
) -> str:
    size = manifest.get("archive_size_bytes")
    if not isinstance(size, int) or size < 0:
        size = 0
    uri = manifest.get("archive_uri")
    if not isinstance(uri, str) or not uri:
        uri = f"external://{identity['provider']}/{identity['fetched_at']}"
    cursor.execute(
        """INSERT INTO snapshots (source_id, sha256, size_bytes, uri, scope, meta)
           VALUES (%s::uuid, %s, %s, %s, 'external', %s::jsonb)
           ON CONFLICT (source_id, sha256) DO UPDATE
             SET size_bytes = EXCLUDED.size_bytes, uri = EXCLUDED.uri,
                 meta = snapshots.meta || EXCLUDED.meta
           RETURNING id""",
        (
            source_id,
            identity["sha256"],
            size,
            uri,
            json.dumps({"fetched_at": identity["fetched_at"]}),
        ),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError("Could not create or find the external snapshot")
    return str(row[0])


def _thread_document(
    cursor: Any,
    source_id: str,
    thread: dict[str, Any],
    snapshot_id: str,
    item_count: int,
) -> str:
    thread_ref_value = thread["thread_ref"]
    cursor.execute(
        """SELECT id FROM documents
           WHERE source_id = %s::uuid AND doc_type = 'thread' AND external_ref = %s""",
        (source_id, thread_ref_value),
    )
    rows = cursor.fetchall()
    if len(rows) > 1:
        raise LoadError(f"Multiple thread documents exist for {thread_ref_value}")
    meta = {
        "topic_title": thread.get("title"),
        "provider": thread.get("provider"),
        "kind": thread.get("kind"),
        "number": thread.get("number"),
        "state": thread.get("state"),
        "author": thread.get("author"),
        "labels": thread.get("labels", []),
        "created_at": thread.get("created_at"),
        "updated_at": thread.get("updated_at"),
        "url": thread.get("url"),
        "events": thread.get("meta", {}).get("events", []),
        "item_count": item_count,
        "snapshot_id": snapshot_id,
    }
    meta_json = json.dumps(meta, ensure_ascii=False)
    if rows:
        document_id = str(rows[0][0])
        cursor.execute(
            """UPDATE documents SET meta = %s::jsonb, external_ref = %s
               WHERE id = %s::uuid""",
            (meta_json, thread_ref_value, document_id),
        )
        return document_id
    cursor.execute(
        """INSERT INTO documents (source_id, doc_type, external_ref, meta)
           VALUES (%s::uuid, 'thread', %s, %s::jsonb) RETURNING id""",
        (source_id, thread_ref_value, meta_json),
    )
    row = cursor.fetchone()
    if not row:
        raise LoadError(f"Could not create thread document for {thread_ref_value}")
    return str(row[0])


def _item_meta(item: dict[str, Any], thread: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "provider": thread.get("provider"),
        "author": item.get("author"),
        "url": item.get("url"),
        "message_id": item.get("meta", {}).get("message_id"),
    }
    meta.update(item.get("meta", {}))
    return {key: value for key, value in meta.items() if value is not None}


def _upsert_external_segment(
    cursor: Any,
    item: dict[str, Any],
    thread: dict[str, Any],
    document_id: str,
    seq_candidate: int,
    source_id: str,
) -> tuple[str, bool]:
    """Upsert one item; return (outcome, seq_candidate_consumed).

    Existing rows keep their sequence value (deleted rows keep occupying their
    slot), so a new item never reuses a number held by another row.
    """
    external_ref = item["external_ref"]
    incoming_meta = _item_meta(item, thread)
    content_hash = item["content_hash"]
    text = item["text"]
    speaker = item.get("author") if isinstance(item.get("author"), str) else None
    posted_at = _posted_at(item)
    # Scope the lookup to the owning source: the same repository may be loaded
    # into another project with identical external_ref values.
    cursor.execute(
        """SELECT s.id, s.content_hash, s.speaker, s.posted_at, s.meta,
                  s.document_id, s.seq
           FROM segments s
           JOIN documents d ON d.id = s.document_id
           WHERE s.external_ref = %s AND d.source_id = %s::uuid""",
        (external_ref, source_id),
    )
    existing = cursor.fetchone()
    if existing:
        (
            segment_id,
            old_hash,
            old_speaker,
            old_posted,
            old_meta_value,
            old_document,
            old_seq,
        ) = existing
        old_meta = _db_json(old_meta_value)
        text_changed = old_hash != content_hash
        was_deleted = old_meta.get("deleted") is True
        document_moved = str(old_document) != document_id
        metadata_changed = {
            key: value
            for key, value in old_meta.items()
            if key not in {"revision", "presence", "snapshot_id", "deleted", "deleted_at",
                           "previous_content_hash", "edited_at"}
        } != incoming_meta
        timestamp_changed = old_posted != posted_at
        speaker_changed = old_speaker != speaker
        if not any(
            (
                text_changed,
                was_deleted,
                document_moved,
                metadata_changed,
                timestamp_changed,
                speaker_changed,
            )
        ):
            return "skipped", False
        new_seq = seq_candidate if document_moved else int(old_seq)
        new_meta = dict(incoming_meta)
        if "revision" in old_meta:
            new_meta["revision"] = old_meta["revision"]
        # Metadata-only updates must not erase edit provenance.
        for key in ("previous_content_hash", "edited_at", "deleted_at"):
            if key in old_meta and key not in incoming_meta:
                new_meta[key] = old_meta[key]
        if text_changed:
            new_meta["revision"] = int(old_meta.get("revision", 0)) + 1
            new_meta["previous_content_hash"] = old_hash
            new_meta["edited_at"] = (
                incoming_meta.get("edited_at")
                or datetime.now(UTC).isoformat()
            )
        # An item that reappears upstream clears the deletion marker.
        if was_deleted:
            new_meta["deleted"] = False
        cursor.execute(
            """UPDATE segments SET document_id = %s::uuid, seq = %s, speaker = %s,
                      text = %s, content_hash = %s, meta = %s::jsonb, posted_at = %s,
                      thread_ref = %s, parent_external_ref = %s, item_kind = %s
                   WHERE id = %s""",
            (
                document_id,
                new_seq,
                speaker,
                text,
                content_hash,
                json.dumps(new_meta, ensure_ascii=False),
                posted_at,
                item["thread_ref"],
                item.get("parent_external_ref"),
                item["item_kind"],
                segment_id,
            ),
        )
        return "updated", document_moved
    cursor.execute(
        """INSERT INTO segments
             (document_id, seq, speaker, text, external_ref, content_hash, meta,
              posted_at, thread_ref, parent_external_ref, item_kind)
           VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)""",
        (
            document_id,
            seq_candidate,
            speaker,
            text,
            external_ref,
            content_hash,
            json.dumps(incoming_meta, ensure_ascii=False),
            posted_at,
            item["thread_ref"],
            item.get("parent_external_ref"),
            item["item_kind"],
        ),
    )
    return "inserted", True


def _mark_deleted_items(
    cursor: Any,
    source_id: str,
    thread_ref_value: str,
    present_refs: set[str],
) -> int:
    """Mark items deleted only when their thread was completely re-fetched."""
    cursor.execute(
        """SELECT s.id, s.external_ref, s.meta FROM segments s
           JOIN documents d ON d.id = s.document_id
           WHERE s.thread_ref = %s AND d.source_id = %s::uuid
             AND s.item_kind = ANY(%s)""",
        (thread_ref_value, source_id, list(_TEXT_ITEM_KINDS | {"event"})),
    )
    changed = 0
    now_iso = datetime.now(UTC).isoformat()
    for segment_id, external_ref, meta_value in cursor.fetchall():
        meta = _db_json(meta_value)
        if external_ref in present_refs:
            # An item that reappears (even unchanged, i.e. skipped by the
            # upsert) must clear a stale deletion marker from an earlier run.
            if meta.get("deleted") is True:
                cursor.execute(
                    "UPDATE segments SET meta = %s::jsonb WHERE id = %s",
                    (json.dumps({**meta, "deleted": False}, ensure_ascii=False), segment_id),
                )
                changed += 1
            continue
        if meta.get("deleted") is True:
            continue
        meta["deleted"] = True
        meta["deleted_at"] = meta.get("deleted_at") or now_iso
        cursor.execute(
            "UPDATE segments SET meta = %s::jsonb WHERE id = %s",
            (json.dumps(meta, ensure_ascii=False), segment_id),
        )
        changed += 1
    return changed


def _write_failed_run(
    config: dict[str, str],
    manifest: dict[str, Any],
    job_dir: Path,
    error: str,
    project_slug: str,
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
                   VALUES (%s::uuid, 'external_ingest', 'failed', %s::jsonb, now(), %s)""",
                (project_id, json.dumps(checkpoint), error),
            )
    finally:
        conn.close()


def load_external(
    job_dir: Path,
    *,
    env_path: Path = ENV_PATH,
    project_slug: str = PROJECT_SLUG,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Load a normalized external discussion bundle in one transaction."""
    paths = _bundle_paths(job_dir)
    try:
        manifest = read_json(paths["manifest"])
    except (OSError, ValueError, TypeError) as exc:
        raise LoadError(f"Cannot read external job manifest: {exc}") from exc
    if not paths["threads"].is_file() or not paths["items"].is_file():
        raise LoadError("External normalized bundle is incomplete")
    config = _required_env(env_path)
    identity = _validate_manifest(manifest)
    if not _SLUG.fullmatch(project_slug or ""):
        raise LoadError("Project slug is invalid")
    threads = list(_iter_jsonl(paths["threads"]))
    items = list(_iter_jsonl(paths["items"]))
    thread_count = len(threads)
    if manifest["thread_count"] != thread_count:
        raise LoadError("External manifest thread_count does not match threads.jsonl")
    if manifest["item_count"] != len(items):
        raise LoadError("External manifest item_count does not match items.jsonl")
    thread_refs: set[str] = set()
    threads_by_ref: dict[str, dict[str, Any]] = {}
    for thread in threads:
        _validate_thread(thread, manifest)
        thread_refs.add(thread["thread_ref"])
        threads_by_ref[thread["thread_ref"]] = thread
    for item in items:
        _validate_item(item, thread_refs)
    items_complete = manifest["threads_complete"]
    for thread_ref_value in items_complete:
        if thread_ref_value not in thread_refs:
            raise LoadError("External manifest threads_complete has unknown threads")
    started_at = datetime.now(UTC)
    counters: Counter[str] = Counter()
    deleted_count = 0
    rolled_back = False
    source_id_value: str | None = None

    try:
        conn = _connect(config)
        try:
            with conn.transaction(), conn.cursor() as cursor:
                project_id = _project_id(cursor, project_slug)
                source_id_value = _source_row(cursor, project_id, manifest, identity)
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (f"external-ingest:{identity['source_id']}",),
                )
                snapshot_id = _snapshot_row(cursor, source_id_value, manifest, identity)
                prior_run = None
                cursor.execute(
                    """SELECT id FROM runs WHERE project_id = %s::uuid
                       AND kind = 'external_ingest' AND status = 'done'
                       AND checkpoint->>'import_key' = %s
                       ORDER BY started_at DESC LIMIT 1""",
                    (project_id, identity["import_key"]),
                )
                prior_run = cursor.fetchone()
                items_by_thread: dict[str, list[dict[str, Any]]] = {}
                for item in items:
                    items_by_thread.setdefault(item["thread_ref"], []).append(item)
                if prior_run:
                    counters["skipped"] = len(items)
                    # An identical bundle must still reconcile deletions for
                    # threads that were fetched completely in this run.
                    for thread in threads:
                        if items_complete.get(thread["thread_ref"]) is not True:
                            continue
                        present = {
                            item["external_ref"]
                            for item in items_by_thread.get(thread["thread_ref"], [])
                        }
                        deleted_count += _mark_deleted_items(
                            cursor, source_id_value, thread["thread_ref"], present
                        )
                else:
                    cursor.execute(
                        """INSERT INTO runs (project_id, kind, status, checkpoint)
                           VALUES (%s::uuid, 'external_ingest', 'running', %s::jsonb)
                           RETURNING id""",
                        (
                            project_id,
                            json.dumps({
                                "job_dir": str(job_dir.resolve()),
                                "snapshot_sha256": identity["sha256"],
                                "import_key": identity["import_key"],
                                "threads_total": thread_count,
                                "items_total": len(items),
                            }),
                        ),
                    )
                    run_row = cursor.fetchone()
                    run_id = str(run_row[0]) if run_row else None
                    if run_id is None:
                        raise LoadError("Could not create the external ingestion run row")
                    doc_by_ref: dict[str, str] = {}
                    for thread in threads:
                        thread_items = items_by_thread.get(thread["thread_ref"], [])
                        document_id = _thread_document(
                            cursor,
                            source_id_value,
                            thread,
                            snapshot_id,
                            len(thread_items),
                        )
                        doc_by_ref[thread["thread_ref"]] = document_id
                        cursor.execute(
                            "SELECT coalesce(max(seq), 0) FROM segments WHERE document_id = %s::uuid",
                            (document_id,),
                        )
                        max_seq_row = cursor.fetchone()
                        next_seq = int(max_seq_row[0]) if max_seq_row else 0
                        present_refs: set[str] = set()
                        for item in thread_items:
                            outcome, consumed = _upsert_external_segment(
                                cursor,
                                item,
                                thread,
                                document_id,
                                next_seq + 1,
                                source_id_value,
                            )
                            if consumed:
                                next_seq += 1
                            counters[outcome] += 1
                            present_refs.add(item["external_ref"])
                        if items_complete.get(thread["thread_ref"]) is True:
                            deleted_count += _mark_deleted_items(
                                cursor, source_id_value, thread["thread_ref"], present_refs
                            )
                    checkpoint_data = {
                        "job_dir": str(job_dir.resolve()),
                        "snapshot_sha256": identity["sha256"],
                        "import_key": identity["import_key"],
                        "since": manifest.get("checkpoint_after", {}).get("since"),
                        "threads_total": thread_count,
                        "items_total": len(items),
                        "inserted": counters["inserted"],
                        "updated": counters["updated"],
                        "skipped": counters["skipped"],
                        "deleted": deleted_count,
                    }
                    cursor.execute(
                        """UPDATE runs SET status = 'done', checkpoint = %s::jsonb,
                                  finished_at = now() WHERE id = %s::uuid""",
                        (json.dumps(checkpoint_data), run_id),
                    )
                if dry_run:
                    raise _DryRunRollback
        finally:
            conn.close()
    except _DryRunRollback:
        rolled_back = True
    except Exception as exc:
        safe_error = _safe_error(exc, config)
        if isinstance(exc, ConnectorError):
            safe_error = f"{type(exc).__name__} ({exc.code})"
        if not dry_run:
            try:
                _write_failed_run(config, manifest, job_dir, safe_error, project_slug)
            except Exception as failed_run_exc:  # noqa: BLE001 - best effort record
                safe_error += (
                    f"; failed-run record could not be written"
                    f" ({type(failed_run_exc).__name__})"
                )
        manifest["load"] = {
            "status": "dry_run_failed" if dry_run else "failed",
            "dry_run": dry_run,
            "transaction": "rolled_back",
            "rollback_confirmed": dry_run,
            "inserted": counters["inserted"],
            "updated": counters["updated"],
            "skipped": counters["skipped"],
            "deleted": deleted_count,
            "errors": 1,
            "project_slug": project_slug,
            "project_id": None,
            "failed_run_recorded": not dry_run,
            "error": safe_error,
        }
        write_json_atomic(paths["manifest"], manifest)
        raise LoadError(safe_error) from exc

    result = {
        "status": "dry_run" if rolled_back else "done",
        "dry_run": dry_run,
        "transaction": "rolled_back" if rolled_back else "committed",
        "rollback_confirmed": rolled_back,
        "source_id": None if rolled_back else identity["source_id"],
        "provider": identity["provider"],
        "threads": thread_count,
        "items": len(items),
        "inserted": counters["inserted"],
        "updated": counters["updated"],
        "skipped": counters["skipped"],
        "deleted": deleted_count,
        "nonempty": manifest.get("nonempty_count", 0),
        "skipped_empty": manifest.get("skipped_empty_count", 0),
        "errors": 0,
        "project_slug": project_slug,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
    }
    manifest["load"] = result
    write_json_atomic(paths["manifest"], manifest)
    return result
