"""Read-only counters for the broker's registered Telegram sources."""
import re

from kiber_analyst.embedding_contract import MODEL_KEY
from kiber_analyst.jobqueue import JobsError
from kiber_analyst.telegram_source import parse_telegram_source

_QUERY = """
WITH source AS (
    SELECT s.id FROM sources s JOIN projects p ON p.id=s.project_id
    WHERE p.slug=%s AND s.external_id=%s AND s.kind='telegram'
), docs AS (
    SELECT d.id FROM documents d JOIN source s ON s.id=d.source_id
    WHERE d.doc_type='export'
      AND (%s::text IS NULL OR d.meta->>'topic_id'=%s::text)
), messages AS (
    SELECT g.id, g.external_ref, g.text, g.meta
    FROM segments g JOIN docs d ON g.document_id=d.id
)
SELECT (SELECT count(*) FROM docs),
       count(*), count(DISTINCT m.external_ref),
       count(*) FILTER (WHERE m.text ~ '[^[:space:]]'),
       count(*) FILTER (WHERE m.meta->>'media_path' IS NOT NULL),
       (SELECT count(*) FROM telegram_service_events e JOIN source s ON s.id=e.source_id
        WHERE (%s::text IS NULL OR e.topic_id=%s::text)),
       (SELECT count(*) FROM embeddings e JOIN messages ms ON ms.id::text=e.owner_id
        WHERE e.owner_table='segments' AND e.model=%s),
       count(*) FILTER (
         WHERE m.text ~ '[^[:space:]]'
           AND NOT EXISTS (
             SELECT 1 FROM embeddings e
             WHERE e.owner_table='segments' AND e.owner_id=m.id::text AND e.model=%s
           )
       )
FROM messages m
"""


def source_overview(source_id, project):
    if not isinstance(source_id, str) or not isinstance(project, str):
        raise JobsError("Invalid source scope")
    parsed = parse_telegram_source(source_id)
    if parsed is None or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', project):
        raise JobsError("Invalid source scope")
    from kiber_analyst.load import ENV_PATH, _connect, _required_env

    chat, topic = parsed
    with _connect(_required_env(ENV_PATH)) as conn, conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='10s'")
        row = conn.execute(
            _QUERY, (project, chat, topic, topic, topic, topic, MODEL_KEY, MODEL_KEY)
        ).fetchone()
    names = ('documents', 'messages', 'unique_messages', 'nonempty_messages',
             'messages_with_media', 'service_events', 'embeddings',
             'nonempty_messages_without_embeddings')
    return dict(zip(names, row, strict=True))
