from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date
from typing import Any

from kiber_analyst.embedding_contract import MODEL_KEY
from kiber_analyst.errors import SearchError

SEARCH_SQL = "\nWITH query_args AS (\n  SELECT websearch_to_tsquery('russian', %(query)s) AS tsq,\n         %(vector)s::vector AS query_vector\n),\nsearch_units AS (\n  SELECT\n    'segment:' || s.id::text AS unit_key,\n    'segment'::text AS source,\n    s.id AS segment_id,\n    NULL::bigint AS asr_segment_id,\n    s.document_id,\n    s.text,\n    s.tsv,\n    s.posted_at,\n    COALESCE(NULLIF(s.speaker, ''), NULLIF(s.meta->>'author_id', ''),\n             NULLIF(s.meta->>'sender_id', ''), 'Unknown') AS author,\n    s.meta,\n    s.meta->>'url' AS url,\n    NULL::bigint AS start_ms,\n    NULL::bigint AS end_ms,\n    NULL::uuid AS asr_run_id,\n    so.kind AS source_kind,\n    p.id AS project_id,\n    p.slug AS project_slug,\n    ('telegram:' || so.external_id || ':topic:' || (s.meta->>'topic_id')) AS source_id,\n    so.external_id AS chat_id,\n    CASE WHEN so.kind = 'telegram' THEN p.title ELSE so.title END AS chat_title,\n    s.meta->>'topic_id' AS topic_id,\n    COALESCE(s.meta->>'topic_title', d.meta->>'topic_title') AS topic_title,\n    split_part(s.external_ref, ':', 3) AS message_id\n  FROM segments s\n  JOIN documents d ON d.id = s.document_id\n  JOIN sources so ON so.id = d.source_id\n  JOIN projects p ON p.id = so.project_id\n\n  UNION ALL\n\n  SELECT\n    'asr:' || a.id::text AS unit_key,\n    'asr'::text AS source,\n    s.id AS segment_id,\n    a.id AS asr_segment_id,\n    s.document_id,\n    a.text,\n    a.tsv,\n    s.posted_at,\n    COALESCE(NULLIF(s.speaker, ''), NULLIF(s.meta->>'author_id', ''),\n             NULLIF(s.meta->>'sender_id', ''), 'Unknown') AS author,\n    s.meta,\n    s.meta->>'url' AS url,\n    a.start_ms,\n    a.end_ms,\n    a.run_id AS asr_run_id,\n    so.kind AS source_kind,\n    p.id AS project_id,\n    p.slug AS project_slug,\n    ('telegram:' || so.external_id || ':topic:' || (s.meta->>'topic_id')) AS source_id,\n    so.external_id AS chat_id,\n    CASE WHEN so.kind = 'telegram' THEN p.title ELSE so.title END AS chat_title,\n    s.meta->>'topic_id' AS topic_id,\n    COALESCE(s.meta->>'topic_title', d.meta->>'topic_title') AS topic_title,\n    split_part(s.external_ref, ':', 3) AS message_id\n  FROM asr_segments a\n  JOIN asr_runs ar ON ar.id = a.run_id\n    AND ar.status IN ('done', 'partial')\n  JOIN asr_results r ON r.run_id = a.run_id\n    AND r.external_ref = a.external_ref\n    AND r.status = 'complete'\n  JOIN segments s ON s.external_ref = a.external_ref\n  JOIN documents d ON d.id = s.document_id\n  JOIN sources so ON so.id = d.source_id\n  JOIN projects p ON p.id = so.project_id\n    AND p.id = ar.project_id\n  WHERE btrim(a.text) <> ''\n),\nbase AS (\n  SELECT u.*\n  FROM search_units u\n  WHERE u.project_slug = %(project)s\n    AND (%(source)s::text IS NULL OR u.source = %(source)s::text)\n    AND (%(source_kind)s::text IS NULL OR u.source_kind = %(source_kind)s::text)\n    AND (%(date_from)s::date IS NULL OR u.posted_at >=\n         (%(date_from)s::date::timestamp AT TIME ZONE 'Europe/Moscow'))\n    AND (%(date_to)s::date IS NULL OR u.posted_at <\n         ((%(date_to)s::date + 1)::timestamp AT TIME ZONE 'Europe/Moscow'))\n    AND (%(author)s::text IS NULL OR u.author ILIKE '%%' || %(author)s::text || '%%')\n    AND (%(topic)s::text IS NULL OR u.meta->>'topic_id' = %(topic)s::text)\n    AND (%(source_id)s::text IS NULL OR u.source_id = %(source_id)s::text)\n),\nlexical_rank AS (\n  SELECT b.unit_key,\n         row_number() OVER (\n           ORDER BY ts_rank_cd(b.tsv, q.tsq) DESC, b.unit_key\n         ) AS position\n  FROM base b CROSS JOIN query_args q\n  WHERE b.tsv @@ q.tsq\n),\nsemantic_rank AS (\n  SELECT b.unit_key,\n         row_number() OVER (\n           ORDER BY e.embedding <=> q.query_vector, b.unit_key\n         ) AS position\n  FROM base b\n  JOIN embeddings e\n    ON e.project_id = b.project_id\n   AND e.model = %(model)s\n   AND e.owner_table = CASE\n         WHEN b.source = 'asr' THEN 'asr_segments'\n         ELSE 'segments'\n       END\n   AND e.owner_id = CASE\n         WHEN b.source = 'asr' THEN b.asr_segment_id::text\n         ELSE b.segment_id::text\n       END\n  CROSS JOIN query_args q\n  WHERE q.query_vector IS NOT NULL\n),\nrank_contributions AS (\n  SELECT unit_key, 1.0 / (60 + position) AS contribution FROM lexical_rank\n  UNION ALL\n  SELECT unit_key, 1.0 / (60 + position) AS contribution FROM semantic_rank\n),\nfused AS (\n  SELECT unit_key, sum(contribution) AS score\n  FROM rank_contributions\n  GROUP BY unit_key\n)\nSELECT b.source, b.segment_id, b.asr_segment_id, b.document_id,\n       b.asr_run_id, b.start_ms, b.end_ms, fused.score, b.text, b.url,\n       b.posted_at, b.author, b.source_kind, b.project_slug,\n       b.source_id, b.chat_id, b.chat_title, b.topic_id, b.topic_title, b.message_id\nFROM fused\nJOIN base b USING (unit_key)\nORDER BY fused.score DESC, b.unit_key\nLIMIT %(limit)s\n"


def _timecode(milliseconds: int | None) -> str | None:
    if milliseconds is None:
        return None
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def execute_search(
    conn: Any,
    query: str,
    *,
    project: str,
    vector: list[float] | None,
    limit: int = 10,
    source_kind: str | None = None,
    source: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    author: str | None = None,
    topic: str | None = None,
    source_id: str | None = None,
    degraded_reason: str | None = None,
) -> dict[str, Any]:
    """Run the shared parameterized FTS/vector query on a caller-owned connection."""
    normalized_query = query.strip()
    if not normalized_query:
        raise SearchError("Search query cannot be empty")
    if not project or len(project) > 128:
        raise SearchError("Project is invalid")
    if source not in {None, "segment", "asr"}:
        raise SearchError("Source must be all, segment, or asr")
    if source_kind not in {None, "telegram", "document", "web", "video"}:
        raise SearchError("Source kind is invalid")
    if source_id is not None and (
        not isinstance(source_id, str)
        or len(source_id) > 128
        or not re.fullmatch(r"telegram:-[1-9][0-9]{0,18}:topic:[1-9][0-9]{0,9}", source_id)
    ):
        raise SearchError("Source ID is invalid")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise SearchError("Limit must be between 1 and 100")
    if date_from and date_to and date_from > date_to:
        raise SearchError("Date from must not be later than date to")
    if vector is not None:
        if len(vector) != 1536 or any(not math.isfinite(float(item)) for item in vector):
            raise SearchError("Query vector is invalid")
        vector_json = json.dumps([float(item) for item in vector], separators=(",", ":"))
    else:
        vector_json = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM projects WHERE slug = %s", (project,))
            if not cursor.fetchone():
                raise SearchError(f"Project {project!r} does not exist")
            if source_id is not None:
                cursor.execute(
                    """SELECT 1 FROM sources so
                       JOIN projects p ON p.id = so.project_id
                       JOIN documents d ON d.source_id = so.id
                       WHERE p.slug = %s AND so.kind = 'telegram'
                         AND ('telegram:' || so.external_id || ':topic:' ||
                              (d.meta->>'topic_id')) = %s
                       LIMIT 1""",
                    (project, source_id),
                )
                if not cursor.fetchone():
                    raise SearchError("Telegram source or topic does not exist")
            cursor.execute(
                SEARCH_SQL,
                {
                    "query": normalized_query,
                    "vector": vector_json,
                    "model": MODEL_KEY,
                    "project": project,
                    "source": source,
                    "source_kind": source_kind,
                    "date_from": date_from.isoformat() if date_from else None,
                    "date_to": date_to.isoformat() if date_to else None,
                    "author": author,
                    "topic": topic,
                    "source_id": source_id,
                    "limit": limit,
                },
            )
            rows = cursor.fetchall()
    except SearchError:
        raise
    except Exception as exc:
        sqlstate = getattr(exc, "sqlstate", None)
        suffix = f" SQLSTATE {sqlstate}" if isinstance(sqlstate, str) else ""
        raise SearchError(f"Knowledge search failed ({type(exc).__name__}{suffix})") from exc
    results = []
    for row in rows:
        start_ms = int(row[5]) if row[5] is not None else None
        results.append(
            {
                "source": row[0],
                "segment_id": int(row[1]),
                "asr_segment_id": int(row[2]) if row[2] is not None else None,
                "document_id": str(row[3]),
                "asr_run_id": str(row[4]) if row[4] is not None else None,
                "start_ms": start_ms,
                "end_ms": int(row[6]) if row[6] is not None else None,
                "timecode": _timecode(start_ms),
                "score": float(row[7]),
                "text": row[8],
                "url": row[9],
                "posted_at": row[10].isoformat() if row[10] is not None else None,
                "author": row[11],
                "source_kind": row[12],
                "project_slug": row[13],
                "source_id": row[14],
                "chat_id": row[15],
                "chat_title": row[16],
                "topic_id": row[17],
                "topic_title": row[18],
                "message_id": row[19],
            }
        )
    return {
        "query": normalized_query,
        "query_sha256": hashlib.sha256(normalized_query.encode("utf-8")).hexdigest(),
        "query_length": len(normalized_query),
        "mode": "hybrid" if vector is not None else "fts",
        "degraded": vector is None,
        "degraded_reason": degraded_reason,
        "results": results,
    }
