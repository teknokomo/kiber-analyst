"""Read-only counters for external sources (GitHub/GitLab/GitVerse; ТЗ §13.4)."""

from __future__ import annotations

import re

from kiber_analyst.connectors.source_ids import parse_source_id
from kiber_analyst.embedding_contract import MODEL_KEY
from kiber_analyst.jobqueue import JobsError

_QUERY = """
WITH source AS (
    SELECT s.id FROM sources s JOIN projects p ON p.id=s.project_id
    WHERE p.slug=%s AND s.external_id=%s AND s.kind=%s
), docs AS (
    SELECT d.id, d.meta->>'state' AS state FROM documents d
    JOIN source s ON s.id=d.source_id
    WHERE d.doc_type='thread'
), items AS (
    SELECT g.id, g.text, g.item_kind FROM segments g JOIN docs d ON g.document_id=d.id
)
SELECT (SELECT count(*) FROM docs),
       (SELECT count(*) FROM items),
       (SELECT count(*) FROM items WHERE item_kind='comment'),
       (SELECT count(*) FROM items WHERE item_kind='review'),
       (SELECT count(*) FROM items WHERE item_kind='review_comment'),
       (SELECT count(*) FROM items WHERE item_kind='reply'),
       (SELECT count(*) FROM items WHERE item_kind='event'),
       (SELECT count(*) FROM docs WHERE state='open'),
       (SELECT count(*) FROM docs WHERE state IN ('closed','merged')),
       (SELECT count(*) FROM items WHERE text ~ '[^[:space:]]'),
       (SELECT count(*) FROM embeddings e
        JOIN items i ON i.id::text=e.owner_id
        WHERE e.owner_table='segments' AND e.model=%s)
"""


def external_source_overview(source_id, project):
    if not isinstance(source_id, str) or not isinstance(project, str):
        raise JobsError("Invalid source scope")
    parsed = parse_source_id(source_id)
    if parsed is None or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", project) is None:
        raise JobsError("Invalid source scope")
    from kiber_analyst.load import ENV_PATH, _connect, _required_env

    with _connect(_required_env(ENV_PATH)) as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout='10s'")
        row = conn.execute(
            _QUERY, (project, parsed.external_id, parsed.provider, MODEL_KEY)
        ).fetchone()
    names = (
        "threads",
        "items",
        "comments",
        "reviews",
        "review_comments",
        "replies",
        "events",
        "threads_open",
        "threads_closed_or_merged",
        "nonempty_items",
        "embeddings",
    )
    return dict(zip(names, row, strict=True))
