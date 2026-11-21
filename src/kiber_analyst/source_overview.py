"""Read-only counters for the broker's registered Telegram sources."""
import re

from kiber_analyst.embedding_contract import MODEL_KEY
from kiber_analyst.jobqueue import JobsError

_QUERY = """
WITH source AS (
    SELECT s.id FROM sources s JOIN projects p ON p.id=s.project_id
    WHERE p.slug=%s AND s.external_id=%s AND s.kind='telegram'
), docs AS (
    SELECT d.id FROM documents d JOIN source s ON s.id=d.source_id
    WHERE d.doc_type='export' AND d.meta->>'topic_id'=%s
), messages AS (
    SELECT g.id, g.external_ref, g.text, g.meta
    FROM segments g JOIN docs d ON g.document_id=d.id
)
SELECT (SELECT count(*) FROM docs),
       count(*), count(DISTINCT m.external_ref),
       count(*) FILTER (WHERE m.text ~ '[^[:space:]]'),
       count(*) FILTER (WHERE m.meta->>'media_path' IS NOT NULL),
       (SELECT count(*) FROM telegram_service_events e JOIN source s ON s.id=e.source_id
        WHERE e.topic_id=%s),
       (SELECT count(*) FROM embeddings e JOIN messages ms ON ms.id::text=e.owner_id
        WHERE e.owner_table='segments' AND e.model=%s)
FROM messages m
"""


def source_overview(source_id, project):
    if not isinstance(source_id, str) or not isinstance(project, str):
        raise JobsError("Invalid source scope")
    match = re.fullmatch(r'telegram:(-[1-9][0-9]{0,18}):topic:([1-9][0-9]{0,9})', source_id)
    if match is None or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', project):
        raise JobsError("Invalid source scope")
    from kiber_analyst.load import ENV_PATH, _connect, _required_env

    chat, topic = match.groups()
    with _connect(_required_env(ENV_PATH)) as conn, conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='10s'")
        row = conn.execute(_QUERY, (project, chat, topic, topic, MODEL_KEY)).fetchone()
    names = ('documents', 'messages', 'unique_messages', 'nonempty_messages',
             'messages_with_media', 'service_events', 'embeddings')
    return dict(zip(names, row, strict=True))
