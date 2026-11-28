from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip('psycopg')
TEST_ENV = os.environ.get('KIBER_ANALYST_INGEST_TEST_ENV')
pytestmark = [
    pytest.mark.skipif(
        not TEST_ENV or not Path(TEST_ENV).is_file(),
        reason='set KIBER_ANALYST_INGEST_TEST_ENV to an isolated disposable test database env file',
    ),
    pytest.mark.usefixtures('disposable_postgres_credentials'),
]


def _connect(credentials: dict[str, str]):
    from conftest import connect_validated_postgres

    return connect_validated_postgres(credentials, psycopg.connect)


def test_asr_overview_counts_current_model_vectors_read_only(
    disposable_postgres_credentials: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    from kiber_analyst import load
    from kiber_analyst.asr_source_overview import asr_source_overview
    from kiber_analyst.embedding_contract import MODEL_KEY

    assert TEST_ENV is not None
    monkeypatch.setattr(load, 'ENV_PATH', Path(TEST_ENV))

    project = f'asr-overview-{uuid.uuid4().hex[:12]}'
    chat_id = f'-100{uuid.uuid4().int % 10**12 + 1}'
    source_id = f'telegram:{chat_id}:topic:17'
    external_ref = f'tg:{chat_id}:501'
    with _connect(disposable_postgres_credentials) as conn, conn.transaction():
        project_id = conn.execute(
            'INSERT INTO projects(slug,title) VALUES(%s,%s) RETURNING id',
            (project, 'Synthetic ASR overview'),
        ).fetchone()[0]
        source_db_id = conn.execute(
            "INSERT INTO sources(project_id,kind,external_id) VALUES(%s,'telegram',%s) RETURNING id",
            (project_id, chat_id),
        ).fetchone()[0]
        document_id = conn.execute(
            "INSERT INTO documents(source_id,doc_type,meta) VALUES(%s,'export',%s::jsonb) RETURNING id",
            (source_db_id, json.dumps({'export_scope': 'topic', 'topic_id': 17})),
        ).fetchone()[0]
        group_document_id = conn.execute(
            "INSERT INTO documents(source_id,doc_type,meta) VALUES(%s,'export',%s::jsonb) RETURNING id",
            (source_db_id, json.dumps({'export_scope': 'chat', 'topic_id': None})),
        ).fetchone()[0]
        conn.execute(
            'INSERT INTO segments(document_id,seq,text,external_ref,meta) '
            'VALUES(%s,1,%s,%s,%s::jsonb)',
            (document_id, 'synthetic audio message', external_ref,
             json.dumps({'topic_id': 17, 'media_type': 'voice_message',
                         'media_path': 'media/501/audio.ogg',
                         'media_export': {'size': 4096}})),
        )
        group_external_ref = f'tg:{chat_id}:502'
        conn.execute(
            'INSERT INTO segments(document_id,seq,text,external_ref,meta) '
            'VALUES(%s,1,%s,%s,%s::jsonb)',
            (group_document_id, 'synthetic group audio message', group_external_ref,
             json.dumps({'media_type': 'audio_file', 'media_path': 'media/502/audio.mp3',
                         'media_export': {'size': 4096}})),
        )
        run_id = conn.execute(
            "INSERT INTO asr_runs(project_id,run_key,provider,model,language,status,finished_at) "
            "VALUES(%s,%s,'groq','whisper-large-v3-turbo','ru','done',now()) RETURNING id",
            (project_id, f'run-{uuid.uuid4().hex}'),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO asr_results(run_id,external_ref,audio_sha256,status,text) "
            "VALUES(%s,%s,%s,'complete','synthetic transcript')",
            (run_id, external_ref, 'a' * 64),
        )
        asr_segment_ids = []
        for number, text in enumerate(('first nonempty', 'second nonempty', '   ')):
            asr_segment_ids.append(conn.execute(
                'INSERT INTO asr_segments(run_id,external_ref,segment_no,start_ms,end_ms,text) '
                'VALUES(%s,%s,%s,%s,%s,%s) RETURNING id',
                (run_id, external_ref, number, number * 100, number * 100 + 100, text),
            ).fetchone()[0])
        group_run_id = conn.execute(
            "INSERT INTO asr_runs(project_id,run_key,provider,model,language,status,finished_at) "
            "VALUES(%s,%s,'groq','whisper-large-v3-turbo','ru','done',now()) RETURNING id",
            (project_id, f'run-group-{uuid.uuid4().hex}'),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO asr_results(run_id,external_ref,audio_sha256,status,text) "
            "VALUES(%s,%s,%s,'complete','synthetic group transcript')",
            (group_run_id, group_external_ref, 'b' * 64),
        )
        group_asr_segment_ids = []
        for number, text in enumerate(('group transcript', '   ')):
            group_asr_segment_ids.append(conn.execute(
                'INSERT INTO asr_segments(run_id,external_ref,segment_no,start_ms,end_ms,text) '
                'VALUES(%s,%s,%s,%s,%s,%s) RETURNING id',
                (group_run_id, group_external_ref, number, number * 100,
                 number * 100 + 100, text),
            ).fetchone()[0])
        vector = '[' + ','.join(['0'] * 1536) + ']'
        conn.execute(
            'INSERT INTO embeddings(project_id,owner_table,owner_id,model,embedding) '
            'VALUES(%s,\'asr_segments\',%s,%s,%s::vector)',
            (project_id, str(asr_segment_ids[0]), MODEL_KEY, vector),
        )
        conn.execute(
            'INSERT INTO embeddings(project_id,owner_table,owner_id,model,embedding) '
            'VALUES(%s,\'asr_segments\',%s,%s,%s::vector)',
            (project_id, str(group_asr_segment_ids[0]), MODEL_KEY, vector),
        )

    result = asr_source_overview(source_id, project)

    assert result['files_total'] == 1
    assert result['files_done'] == 1
    assert result['files_pending'] == 0
    assert result['segments'] == 3
    assert result['empty'] == 1
    assert result['embedding_segments_total'] == 2
    assert result['embedding_segments_done'] == 1
    assert result['embedding_segments_pending'] == 1

    group_result = asr_source_overview(f'telegram:{chat_id}:group', project)

    assert group_result['documents'] == 1
    assert group_result['files_total'] == 1
    assert group_result['files_done'] == 1
    assert group_result['files_pending'] == 0
    assert group_result['segments'] == 2
    assert group_result['empty'] == 1
    assert group_result['embedding_segments_total'] == 1
    assert group_result['embedding_segments_done'] == 1
    assert group_result['embedding_segments_pending'] == 0

    with _connect(disposable_postgres_credentials) as conn:
        assert conn.execute(
            'SELECT count(*) FROM embeddings WHERE owner_table=%s AND owner_id=%s AND model=%s',
            ('asr_segments', str(asr_segment_ids[0]), MODEL_KEY),
        ).fetchone()[0] == 1
