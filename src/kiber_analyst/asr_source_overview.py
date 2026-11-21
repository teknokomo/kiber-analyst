"""Read-only ASR counts for one registered Telegram topic."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from kiber_analyst.jobqueue import JobsError

_AUDIO_TYPES = ("audio", "audio_file", "voice", "voice_message")
_SOURCE_ID = re.compile(r"telegram:(-[1-9][0-9]{0,18}):topic:([1-9][0-9]{0,9})\Z")
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
      AND d.meta->>'topic_id' = %s
), expected AS (
    SELECT DISTINCT ON (g.external_ref)
           g.external_ref,
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
SELECT e.external_ref, e.media_path, e.size_bytes,
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


def asr_source_overview(source_id: Any, project: Any) -> dict[str, Any]:
    if not isinstance(source_id, str) or not isinstance(project, str):
        raise JobsError("Invalid ASR source scope")
    match = _SOURCE_ID.fullmatch(source_id)
    if match is None or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", project):
        raise JobsError("Invalid ASR source scope")
    chat_id, topic_id = match.groups()

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
                     AND d.meta->>'topic_id' = %s""",
                (project, chat_id, topic_id),
            ).fetchone()[0]
            rows = conn.execute(
                _ROWS_SQL,
                (project, chat_id, topic_id, list(_AUDIO_TYPES)),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001 - database details and credentials stay private.
        raise JobsError(f"ASR source overview unavailable ({type(exc).__name__})") from None

    completed: list[str] = []
    failed_refs: list[str] = []
    run_keys: set[str] = set()
    skipped = 0
    segments = 0
    empty = 0
    latest_error: tuple[Any, str] | None = None
    revision_rows = []
    for (
        external_ref,
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
        is_skipped = (
            media_path is None
            or (isinstance(size_bytes, int) and size_bytes > _EXPORT_MAX_BYTES)
            or (isinstance(size_bytes, int) and size_bytes > _PROVIDER_MAX_BYTES)
        )
        if is_skipped:
            skipped += 1
        if status == "complete":
            completed.append(ref)
            segments += int(segment_count)
            empty += int(empty_count)
            if isinstance(run_key, str):
                run_keys.add(run_key)
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
            [ref, status, run_key, int(segment_count), int(empty_count), is_skipped]
        )

    total = len(rows)
    return {
        "source_id": source_id,
        "documents": int(documents),
        "files_total": total,
        "files_done": len(completed),
        "files_failed": len(failed_refs),
        "files_skipped": skipped,
        "files_pending": max(0, total - len(completed) - len(failed_refs) - skipped),
        "segments": segments,
        "empty": empty,
        "last_error_code": latest_error[1] if latest_error else None,
        "completed_refs": completed,
        "failed_refs": failed_refs,
        "run_keys": sorted(run_keys),
        "revision": hashlib.sha256(
            json.dumps(revision_rows, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
