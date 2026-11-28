"""Read-only ASR counts for one registered Telegram group or topic."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from kiber_analyst.jobqueue import JobsError

_AUDIO_TYPES = ("audio", "audio_file", "voice", "voice_message")
_SOURCE_ID = re.compile(
    r"telegram:(?P<chat>-[1-9][0-9]{0,18}):"
    r"(?:(?:topic:(?P<topic>[1-9][0-9]{0,9}))|group)\Z"
)
_SAFE_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,39}\Z")
_PROVIDER_MAX_BYTES = 25_000_000
_EXPORT_MAX_BYTES = 100 * 1024**2

_ROWS_SQL = """
WITH source AS (
    SELECT so.id, so.project_id
    FROM sources so JOIN projects p ON p.id = so.project_id
    WHERE p.slug = %s AND so.kind = 'telegram' AND so.external_id = %s
), documents AS (
    SELECT d.id FROM documents d JOIN source s ON s.id = d.source_id
    WHERE d.doc_type = 'export'
      AND CASE
            WHEN d.meta->>'export_scope' = 'chat'
              OR (d.meta->>'export_scope' IS NULL AND d.meta->>'topic_id' IS NULL)
              THEN 'group'
            WHEN d.meta->>'topic_id' IS NOT NULL
              THEN ('topic:' || (d.meta->>'topic_id'))
          END = %s
), expected AS (
    SELECT DISTINCT ON (g.external_ref)
           g.external_ref,
           g.meta->>'media_type' AS media_type,
           g.meta->>'media_path' AS media_path,
           CASE
             WHEN g.meta->'media_export'->>'size' ~ '^[0-9]+$'
             THEN (g.meta->'media_export'->>'size')::bigint
             ELSE NULL
           END AS size_bytes
    FROM segments g JOIN documents d ON d.id = g.document_id
    WHERE g.external_ref IS NOT NULL
      AND g.meta->>'media_type' = ANY(%s)
    ORDER BY g.external_ref, g.id
), ranked AS (
    SELECT DISTINCT ON (r.external_ref)
           r.external_ref, r.status, r.http_code,
           r.error->>'class' AS error_class, ar.run_key, ar.id AS run_id,
           ar.finished_at, ar.created_at
    FROM asr_results r
    JOIN asr_runs ar ON ar.id = r.run_id
    JOIN source s ON s.project_id = ar.project_id
    JOIN expected e ON e.external_ref = r.external_ref
    WHERE ar.model = 'whisper-large-v3-turbo'
    ORDER BY r.external_ref, (r.status = 'complete') DESC,
             ar.finished_at DESC NULLS LAST, ar.created_at DESC, ar.run_key
)
SELECT e.external_ref, e.media_type, e.media_path, e.size_bytes,
       r.status, r.http_code, r.error_class, r.run_key,
       COALESCE(seg.total, 0), COALESCE(seg.empty, 0),
       r.finished_at, r.created_at
FROM expected e
LEFT JOIN ranked r ON r.external_ref = e.external_ref
LEFT JOIN LATERAL (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE btrim(a.text) = '') AS empty
    FROM asr_segments a
    WHERE a.run_id = r.run_id AND a.external_ref = e.external_ref
) seg ON true
ORDER BY e.external_ref
"""

_EMBEDDINGS_SQL = """
WITH source AS (
    SELECT so.id, so.project_id
    FROM sources so JOIN projects p ON p.id = so.project_id
    WHERE p.slug = %s AND so.kind = 'telegram' AND so.external_id = %s
), expected AS (
    SELECT DISTINCT g.external_ref
    FROM segments g
    JOIN documents d ON d.id = g.document_id
    JOIN source s ON s.id = d.source_id
    WHERE g.external_ref IS NOT NULL
      AND d.doc_type = 'export'
      AND CASE
            WHEN d.meta->>'export_scope' = 'chat'
              OR (d.meta->>'export_scope' IS NULL AND d.meta->>'topic_id' IS NULL)
              THEN 'group'
            WHEN d.meta->>'topic_id' IS NOT NULL
              THEN ('topic:' || (d.meta->>'topic_id'))
          END = %s
      AND g.meta->>'media_type' = ANY(%s)
)
SELECT count(*) FILTER (WHERE btrim(a.text) <> '') AS nonempty_segments,
       count(*) FILTER (WHERE btrim(a.text) <> '' AND e.id IS NOT NULL) AS embedded_segments
FROM source s
JOIN asr_runs ar ON ar.project_id = s.project_id
                 AND ar.model = 'whisper-large-v3-turbo'
                 AND ar.status IN ('done', 'partial')
JOIN asr_results r ON r.run_id = ar.id AND r.status = 'complete'
JOIN asr_segments a ON a.run_id = ar.id AND a.external_ref = r.external_ref
JOIN expected x ON x.external_ref = a.external_ref
LEFT JOIN embeddings e ON e.project_id = ar.project_id
                      AND e.owner_table = 'asr_segments'
                      AND e.owner_id = a.id::text
                      AND e.model = %s
"""


def asr_source_overview(source_id: Any, project: Any) -> dict[str, Any]:
    if not isinstance(source_id, str) or not isinstance(project, str):
        raise JobsError("Invalid ASR source scope")
    match = _SOURCE_ID.fullmatch(source_id)
    if match is None or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", project):
        raise JobsError("Invalid ASR source scope")
    chat_id = match.group("chat")
    topic_id = match.group("topic")
    scope_key = "group" if topic_id is None else f"topic:{topic_id}"

    from kiber_analyst.embedding_api import MODEL_KEY
    from kiber_analyst.load import ENV_PATH, _connect, _required_env

    try:
        with _connect(_required_env(ENV_PATH)) as conn, conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
            conn.execute("SET LOCAL statement_timeout = '10s'")
            documents = conn.execute(
                """SELECT count(*) FROM documents d
                   JOIN sources so ON so.id = d.source_id
                   JOIN projects p ON p.id = so.project_id
                   WHERE p.slug = %s AND so.kind = 'telegram'
                     AND so.external_id = %s AND d.doc_type = 'export'
                     AND CASE
                           WHEN d.meta->>'export_scope' = 'chat'
                             OR (d.meta->>'export_scope' IS NULL AND d.meta->>'topic_id' IS NULL)
                             THEN 'group'
                           WHEN d.meta->>'topic_id' IS NOT NULL
                             THEN ('topic:' || (d.meta->>'topic_id'))
                         END = %s""",
                (project, chat_id, scope_key),
            ).fetchone()[0]
            rows = conn.execute(
                _ROWS_SQL,
                (project, chat_id, scope_key, list(_AUDIO_TYPES)),
            ).fetchall()
            embedding_row = conn.execute(
                _EMBEDDINGS_SQL,
                (project, chat_id, scope_key, list(_AUDIO_TYPES), MODEL_KEY),
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 - database details and credentials stay private.
        raise JobsError(f"ASR source overview unavailable ({type(exc).__name__})") from None

    completed: list[str] = []
    failed_refs: list[str] = []
    run_keys: set[str] = set()
    skipped = 0
    skipped_by_reason: dict[str, int] = {}
    segments = 0
    empty = 0
    latest_error: tuple[Any, str] | None = None
    revision_rows = []
    for (
        external_ref,
        media_type,
        media_path,
        size_bytes,
        status,
        http_code,
        error_class,
        run_key,
        segment_count,
        empty_count,
        finished_at,
        created_at,
    ) in rows:
        ref = str(external_ref)
        skip_reason = None
        if isinstance(size_bytes, int) and size_bytes > _EXPORT_MAX_BYTES:
            skip_reason = "over_100_MiB"
        elif not isinstance(media_path, str) or not media_path:
            skip_reason = "missing_media"
        elif not isinstance(size_bytes, int) or size_bytes <= 0:
            skip_reason = "size_unknown"
        elif size_bytes > _PROVIDER_MAX_BYTES:
            skip_reason = "provider_file_limit"

        if status == "complete":
            completed.append(ref)
            segments += int(segment_count)
            empty += int(empty_count)
            if isinstance(run_key, str):
                run_keys.add(run_key)
        elif skip_reason is not None:
            skipped += 1
            skipped_by_reason[skip_reason] = skipped_by_reason.get(skip_reason, 0) + 1
        elif status == "failed":
            failed_refs.append(ref)
            if type(http_code) is int and 400 <= http_code <= 599:
                code = f"HTTP_{http_code}"
            elif isinstance(error_class, str) and _SAFE_ERROR.fullmatch(error_class):
                code = error_class
            else:
                code = "ASR_FAILED"
            event_time = finished_at or created_at
            if latest_error is None or (
                event_time is not None
                and (latest_error[0] is None or event_time > latest_error[0])
            ):
                latest_error = (event_time, code)
        revision_rows.append(
            [ref, media_type, status, run_key, int(segment_count), int(empty_count), skip_reason]
        )

    total = len(rows)
    eligible = max(0, total - skipped)
    pending = max(0, eligible - len(completed) - len(failed_refs))
    embedding_total = int(embedding_row[0]) if embedding_row else 0
    embedding_done = int(embedding_row[1]) if embedding_row else 0
    return {
        "source_id": source_id,
        "documents": int(documents),
        "files_total": total,
        "files_eligible": eligible,
        "files_done": len(completed),
        "files_failed": len(failed_refs),
        "files_skipped": skipped,
        "files_pending": pending,
        "skipped_by_reason": skipped_by_reason,
        "segments": segments,
        "empty": empty,
        "embedding_segments_total": embedding_total,
        "embedding_segments_done": embedding_done,
        "embedding_segments_pending": max(0, embedding_total - embedding_done),
        "last_error_code": latest_error[1] if latest_error else None,
        "completed_refs": completed,
        "failed_refs": failed_refs,
        "run_keys": sorted(run_keys),
        "revision": hashlib.sha256(
            json.dumps(revision_rows, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
