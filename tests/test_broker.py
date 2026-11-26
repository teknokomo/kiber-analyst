from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker
from kiber_analyst.media_limits import DEFAULT_TELEGRAM_MEDIA_BYTES

SOURCE = 'telegram:-1009000:topic:1'


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(broker, 'RESERVE', 0)
    src = tmp_path / 'imports' / '-1009000' / '1'
    payload = b'synthetic audio'
    media = {'path': 'media/1/a.ogg', 'size': len(payload),
             'sha256': hashlib.sha256(payload).hexdigest()}
    files = {'messages.jsonl': (json.dumps({'message_id': 1, 'media': media}) + '\n').encode(),
             'services.jsonl': b'fixture event\n',
             'manifest.json': json.dumps({'source_id': SOURCE}).encode()}
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}
    files['services_manifest.json'] = json.dumps({
        'source_id': SOURCE, 'complete': True, 'services_sha256': hashes['services.jsonl'],
    }).encode()
    for directory in (src / 'media/1', tmp_path / 'broker', tmp_path / 'imports/.broker'):
        directory.mkdir(parents=True)
        for ancestor in (directory, *directory.parents):
            if ancestor == tmp_path:
                break
            ancestor.chmod(0o700)
    tmp_path.chmod(0o700)
    for name, value in {**files, 'media/1/a.ogg': payload}.items():
        (src / name).write_bytes(value)
        (src / name).chmod(0o600)
    jobs = {}
    argv_log = []

    def submit(argv, *, idempotency_key):
        jid = str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))
        jobs.setdefault(jid, {'state': 'queued'})
        argv_log.append(argv)
        return {'job_id': jid}

    def api(action, *, job_id):
        return jobs[job_id]

    instance = broker.Broker(tmp_path, {SOURCE: {'project': 'fixture', 'hashes': hashes}}, api, submit)
    yield instance, src, jobs, argv_log
    instance.db.close()


def test_owned_chain_and_idempotency(setup):
    app, src, jobs, argv = setup
    with pytest.raises(broker.Refused, match='INGEST_REQUIRED'):
        app.handle('client', 'embed', SOURCE)
    first = app.handle('client', 'ingest', SOURCE)
    assert first == app.handle('client', 'ingest', SOURCE)
    staged = Path(argv[0][3])
    assert staged != src and (staged / 'media/1/a.ogg').read_bytes() == b'synthetic audio'
    assert (staged.stat().st_mode & 0o777, (staged / 'messages.jsonl').stat().st_mode & 0o777) == (0o700, 0o600)
    with pytest.raises(broker.Refused, match='REQUEST_NOT_FOUND'):
        app.handle('another-owner', 'job-status', first['request_id'])
    with pytest.raises(broker.Refused, match='REQUEST_NOT_FOUND'):
        app.handle('client', 'job-status', first['job_id'])
    jobs[first['job_id']].update(state='done', inserted=1, service_events=1, path='private', sql='private')
    assert app.handle('client', 'job-status', first['request_id'])['counts'] == {
        'inserted': 1, 'service_events': 1,
    }
    embedding = app.handle('client', 'embed', SOURCE)
    assert embedding == app.handle('client', 'embed', SOURCE)
    assert argv[-1][0] == 'embed' and first['job_id'] in argv[-1][2]


def test_staging_reuse_rechecks_media_bytes(setup):
    app, _source_dir, _jobs, _argv = setup
    entry, fingerprint, chat, topic = app.source(SOURCE)
    staged = app.stage(SOURCE, entry, fingerprint, chat, topic)
    media_file = staged / 'media/1/a.ogg'
    media_file.write_bytes(b'synthetic audix')
    media_file.chmod(0o600)

    with pytest.raises(broker.Refused, match='DATA_MISMATCH'):
        app.stage(SOURCE, entry, fingerprint, chat, topic)


@pytest.mark.parametrize("size", [100 * 1024**2, 100 * 1024**2 + 1, 4_000_000_000])
def test_media_items_enforces_export_ceiling_not_asr_ceiling(size):
    row = {
        "message_id": 9,
        "media": {
            "path": "media/9/payload.bin", "size": size,
            "max_file_bytes": 4_000_000_000, "sha256": "a" * 64,
        },
    }

    assert broker.media_items((json.dumps(row) + "\n").encode(), 4_000_000_000) == [
        (["media", "9", "payload.bin"], size, "a" * 64)
    ]


def test_media_items_rejects_size_above_selected_or_absolute_limit():
    for size, selected in ((101, 100), (4_000_000_001, 4_000_000_001)):
        row = {"message_id": 9, "media": {
            "path": "media/9/payload.bin", "size": size,
            "max_file_bytes": selected, "sha256": "b" * 64,
        }}
        with pytest.raises(broker.Refused, match="UNSAFE_MEDIA"):
            broker.media_items((json.dumps(row) + "\n").encode(), 4_000_000_000)


def test_media_items_cannot_exceed_the_selected_manifest_limit():
    row = {"message_id": 9, "media": {
        "path": "media/9/payload.bin", "size": DEFAULT_TELEGRAM_MEDIA_BYTES + 1,
        "max_file_bytes": 4_000_000_000, "sha256": "c" * 64,
    }}
    with pytest.raises(broker.Refused, match="UNSAFE_MEDIA"):
        broker.media_items((json.dumps(row) + "\n").encode(), 100)

    row["media"]["size"] = DEFAULT_TELEGRAM_MEDIA_BYTES + 1
    row["media"]["max_file_bytes"] = DEFAULT_TELEGRAM_MEDIA_BYTES + 1
    with pytest.raises(broker.Refused, match="UNSAFE_MEDIA"):
        broker.media_items((json.dumps(row) + "\n").encode())


def test_retry_requires_transient_classification_and_stops_at_three(setup):
    app, _, jobs, _ = setup
    original = app.handle('client', 'ingest', SOURCE)
    request_id = original['request_id']
    jobs[original['job_id']].update(state='failed', error={'category': 'worker_error', 'message': 'private'})
    with pytest.raises(broker.Refused, match='RETRY_NOT_ALLOWED'):
        app.handle('client', 'retry', request_id)
    current = original
    for expected_attempt in (2, 3):
        jobs[current['job_id']].update(state='failed', error={'category': 'transient_database_error'})
        current = app.handle('client', 'retry', request_id)
        assert current['attempt'] == expected_attempt
    jobs[current['job_id']].update(state='failed', error={'category': 'transient_database_error'})
    with pytest.raises(broker.Refused, match='RETRY_LIMIT'):
        app.handle('client', 'retry', request_id)


@pytest.mark.parametrize('limit_text', ['1', '5', '50'])
def test_source_search_uses_worker_and_returns_only_provenance_fields(setup, limit_text):
    app, _src, _jobs, argv_log = setup
    results = [{
        'source_id': SOURCE, 'chat_id': '-1009000', 'chat_title': 'Example Group',
        'topic_id': '1', 'topic_title': 'Example topic',
        'message_id': str(index + 1), 'author': 'Example Author',
        'posted_at': '2020-01-02T12:00:00+03:00', 'score': 0.42,
        'text': 'fixture match', 'url': 'private-url', 'document_id': 'private-id',
    } for index in range(50)]
    app.worker_api = lambda action, **kwargs: (
        {'documents': 1} if action == 'source-overview' else
        {'state': 'done'} if action == 'status' else {
            'mode': 'hybrid', 'degraded': False, 'degraded_reason': None,
            'results': results,
        }
    )
    result = app.handle('client', 'search', SOURCE, 'Example Station', limit_text)
    assert result['mode'] == 'hybrid' and result['degraded'] is False
    assert result['result_count'] == int(limit_text)
    assert len(result['results']) == int(limit_text)
    item = result['results'][0]
    assert item['source_id'] == SOURCE and item['message_id'] == '1'
    assert item['chat_title'] == 'Example Group'
    assert item['topic_title'] == 'Example topic'
    assert item['date'] == '2020-01-02T12:00:00+03:00'
    assert 'url' not in item and 'document_id' not in item
    assert argv_log[0][0] == 'search'
    assert argv_log[0][1] == 'Example Station'
    assert argv_log[0][argv_log[0].index('--source-id') + 1] == SOURCE
    assert argv_log[0][argv_log[0].index('--limit') + 1] == limit_text


def test_search_requeries_identical_query_and_preserves_audit_across_code_version_change(setup, monkeypatch):
    app, _src, jobs, argv_log = setup
    current_title = ['Старая группа']
    results_by_job = {}

    def submit(argv, *, idempotency_key):
        job_id = str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))
        jobs[job_id] = {'state': 'done'}
        results_by_job[job_id] = {
            'mode': 'hybrid', 'degraded': False, 'degraded_reason': None,
            'results': [{
                'source_id': SOURCE, 'chat_id': '-1009000', 'chat_title': current_title[0],
                'topic_id': '1', 'topic_title': 'Example topic',
                'message_id': '7', 'author': 'Example Author',
                'posted_at': '2020-01-02T12:00:00+03:00', 'score': 0.42,
                'text': 'fixture match',
            }],
        }
        argv_log.append(argv)
        return {'job_id': job_id}

    def worker_api(action, **kwargs):
        if action == 'source-overview':
            return {'documents': 1}
        if action == 'status':
            return jobs[kwargs['job_id']]
        return results_by_job[kwargs['job_id']]

    app.submit = submit
    app.worker_api = worker_api
    monkeypatch.setattr(broker, 'SEARCH_CACHE_VERSION', 1)
    old = app.handle('client', 'search', SOURCE, 'Example Station', '5')
    old_repeat = app.handle('client', 'search', SOURCE, 'Example Station', '5')
    assert old_repeat['request_id'] != old['request_id']
    assert old_repeat['results'][0]['chat_title'] == 'Старая группа'

    current_title[0] = 'Example Group'
    monkeypatch.setattr(broker, 'SEARCH_CACHE_VERSION', 2)
    current = app.handle('client', 'search', SOURCE, 'Example Station', '5')
    assert current['request_id'] != old['request_id']
    assert current['results'][0]['chat_title'] == 'Example Group'

    repeated = app.handle('client', 'search', SOURCE, 'Example Station', '5')
    assert repeated['request_id'] != current['request_id']
    assert repeated['results'][0]['chat_title'] == 'Example Group'
    assert app.db.execute("SELECT count(*) FROM requests WHERE operation='search'").fetchone()[0] == 4


def test_search_ignores_legacy_unversioned_completed_request(setup):
    app, _src, _jobs, _argv_log = setup
    query = 'Example Station'
    legacy_fingerprint = hashlib.sha256(json.dumps(
        {'source_id': SOURCE, 'query': query, 'limit': 5},
        sort_keys=True, ensure_ascii=False, separators=(',', ':'),
    ).encode()).hexdigest()
    legacy_request_id = str(uuid.uuid4())
    now = broker.timestamp()
    with app.db:
        app.db.execute(
            """INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (legacy_request_id, 'client', SOURCE, legacy_fingerprint, 'search',
             json.dumps(['search', query, '--project', 'fixture', '--limit', '5',
                         '--source-kind', 'telegram', '--source-id', SOURCE, '--json']),
             now, now, 'done'),
        )

    app.worker_api = lambda action, **kwargs: (
        {'documents': 1} if action == 'source-overview' else
        {'state': 'done'} if action == 'status' else {
            'mode': 'hybrid', 'degraded': False, 'degraded_reason': None,
            'results': [{
                'source_id': SOURCE, 'chat_id': '-1009000',
                'chat_title': 'Example Group', 'topic_id': '1',
                'topic_title': 'Example topic', 'message_id': '7',
                'author': 'Example Author', 'posted_at': '2020-01-02T12:00:00+03:00',
                'score': 0.42, 'text': 'fixture match',
            }],
        }
    )
    result = app.handle('client', 'search', SOURCE, query, '5')
    assert result['request_id'] != legacy_request_id
    assert result['mode'] == 'hybrid' and result['degraded'] is False
    assert result['results'][0]['chat_title'] == 'Example Group'
    assert result['results'][0]['topic_title'] == 'Example topic'
    assert app.db.execute("SELECT count(*) FROM requests WHERE operation='search'").fetchone()[0] == 2


def test_search_fingerprint_scopes_source_project_query_limit_and_version(monkeypatch):
    monkeypatch.setattr(broker, 'SEARCH_CACHE_VERSION', 'build-a')
    baseline = broker.search_fingerprint(SOURCE, 'fixture', 'Example Station', 5)
    assert broker.search_fingerprint(SOURCE, 'fixture', 'Example Station', 1) != baseline
    assert broker.search_fingerprint(SOURCE, 'fixture', 'Sample Station', 5) != baseline
    assert broker.search_fingerprint(SOURCE, 'other', 'Example Station', 5) != baseline
    assert broker.search_fingerprint(SOURCE + ':other', 'fixture', 'Example Station', 5) != baseline
    monkeypatch.setattr(broker, 'SEARCH_CACHE_VERSION', 'build-b')
    assert broker.search_fingerprint(SOURCE, 'fixture', 'Example Station', 5) != baseline


def test_retrying_search_revalidates_fingerprint_with_source_project(setup):
    app, _src, _jobs, argv_log = setup
    query = 'Example Station'
    fingerprint = broker.search_fingerprint(SOURCE, 'fixture', query, 5)
    request_id = str(uuid.uuid4())
    now = broker.timestamp()
    argv = ['search', query, '--project', 'fixture', '--limit', '5',
            '--source-kind', 'telegram', '--source-id', SOURCE, '--json']
    with app.db:
        app.db.execute(
            """INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (request_id, 'client', SOURCE, fingerprint, 'search', json.dumps(argv),
             now, now, 'failed'),
        )
        app.db.execute(
            "INSERT INTO attempts(request_id,attempt,job_id,bound,state)"
            " VALUES(?,1,?,?,?)",
            (request_id, 'previous-search-job', now, 'failed'),
        )
    app.status = lambda _row: {'status': 'failed', 'retryable': True}
    retried = app.handle('client', 'retry', request_id)
    assert retried['request_id'] == request_id
    assert retried['attempt'] == 2
    assert argv_log[-1][0] == 'search'


def test_source_search_refuses_missing_topic_explicitly(setup):
    app, _src, _jobs, argv_log = setup
    app.worker_api = lambda action, **_kwargs: {'documents': 0} if action == 'source-overview' else {}
    with pytest.raises(broker.Refused, match='SOURCE_NOT_FOUND'):
        app.handle('client', 'search', SOURCE, 'query')
    assert not argv_log


@pytest.mark.parametrize(('source', 'query', 'limit'), [
    ('telegram:-100777:topic:1', 'query', None),
    (SOURCE, 'x' * (broker.SEARCH_MAX_QUERY_CHARS + 1), None),
    (SOURCE, 'query', '0'),
    (SOURCE, 'query', '101'),
    (SOURCE, 'query', '-1'),
    (SOURCE, 'query', '5x'),
    (SOURCE, 'query', '05'),
    (SOURCE, 'query', ' 5'),
    (SOURCE, 'query', '+5'),
    (SOURCE, 'query', 5),
])
def test_source_search_rejects_unallowlisted_or_unbounded_inputs(setup, source, query, limit):
    app, _src, _jobs, argv_log = setup
    args = (source, query) if limit is None else (source, query, limit)
    with pytest.raises(broker.Refused):
        app.handle('client', 'search', *args)
    assert not argv_log


@pytest.mark.parametrize('kind', ['hash', 'symlink', 'source', 'traversal'])
def test_invalid_input_leaves_no_staging_or_request(setup, kind):
    app, src, _, argv = setup
    source = SOURCE
    if kind == 'hash':
        (src / 'messages.jsonl').write_bytes(b'mismatch')
    elif kind == 'symlink':
        (src / 'manifest.json').unlink()
        (src / 'manifest.json').symlink_to(src / 'services.jsonl')
    elif kind == 'source':
        source = 'telegram:-100777:topic:1'
    else:
        source = '../private'
    expected = OSError if kind == 'symlink' else broker.Refused
    with pytest.raises(expected):
        app.handle('client', 'ingest', source)
    assert not argv
    assert not list((app.root / 'imports/.broker').iterdir())
    assert app.db.execute('SELECT count(*) FROM requests').fetchone()[0] == 0


@pytest.mark.parametrize('live_db', [False, True])
def test_completed_resume_supplies_synthetic_photos_and_preserves_large_files(
    tmp_path, monkeypatch, live_db, loopback_postgres_driver,
):
    dsn = os.environ.get('KIBER_MEDIA_RESUME_TEST_DSN')
    if live_db and not dsn:
        pytest.skip('requires disposable PostgreSQL fixture DSN')
    if live_db:
        import psycopg

        from kiber_analyst import load
        from kiber_analyst.telegram_ingest import load_telegram_export

        monkeypatch.setattr(load, '_required_env', lambda path: {})
        monkeypatch.setattr(load, '_connect', lambda config: psycopg.connect(dsn))
    from kiber_analyst.telegram_ingest import prepare_export

    monkeypatch.setattr(broker, 'RESERVE', 0)
    root = tmp_path / 'imports'
    source = root / '-1009000' / '1'
    repair = tmp_path / 'export-repairs'
    for directory in (root, source.parent, source, repair):
        directory.mkdir(mode=0o700)
    def jsonl(items):
        return b''.join((json.dumps(row) + '\n').encode() for row in items)

    def put(path, data):
        path.write_bytes(data)
        path.chmod(0o600)

    date = '2020-01-01T00:00:00Z'
    payload = b'photo fixture'
    digest = hashlib.sha256(payload).hexdigest()
    records = {}
    messages = []
    for mid in range(1, 8):
        reason = 'unknown_size' if mid <= 5 else 'over_100_MiB'
        messages.append(dict(message_id=mid, topic_id=1, date=date, text=f'fixture {mid}',
                             media={'kind': 'photo', 'reason': reason, 'export_status': 'skipped'}))
        if mid <= 5:
            path = repair / str(mid) / 'fixture.jpg'
            path.parent.mkdir(mode=0o700)
            put(path, payload)
            records[str(mid)] = dict(path=f'media-resume/{mid}/fixture.jpg', size=len(payload),
                                     sha256=digest, export_status='downloaded')
    services = jsonl(dict(event_id=i, topic_id=1, date=date, type='unknown') for i in range(1, 4))
    msg = jsonl(messages)
    sh, mh = (hashlib.sha256(data).hexdigest() for data in (services, msg))
    meta = dict(topic_id=1, source_id=SOURCE, complete=True, scope_before=None)
    control = {
        'messages.jsonl': msg,
        'services.jsonl': services,
        'manifest.json': jsonl([dict(meta, chat_id='-1009000', export_version=1, message_count=7,
                                    messages_sha256=mh)]),
    }
    for name, data in control.items():
        put(source / name, data)
    put(source / 'services_manifest.json', jsonl([
        dict(meta, chat_id=-1009000, event_count=3, range_complete=True, services_sha256=sh),
    ]))
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in control.items()}
    checkpoint = jsonl([dict(inputs=hashes, records=records,
                             over_limit_message_ids=[6, 7])])
    def ingest(name):
        return load_telegram_export(source, tmp_path / name, env_path=tmp_path / 'unused',
                                    project_slug='resume-fixture', root=root)

    assert prepare_export(source, tmp_path / 'baseline', root=root)[2] == 7
    (root / '.broker').mkdir(mode=0o700)
    (tmp_path / 'broker').mkdir(mode=0o700)
    submissions = {}
    def submit(argv, *, idempotency_key):
        job_id = str(uuid.uuid4())
        submissions[job_id] = argv
        return {'job_id': job_id}

    gateway = broker.Broker(tmp_path, {SOURCE: {'project': 'fixture', 'hashes': hashes}},
                            worker_api=lambda action, **payload: {'state': 'done'}, submit=submit)
    before = gateway.handle('client', 'ingest', SOURCE)
    original_stage = Path(submissions[before['job_id']][3])
    if live_db:
        initial = ingest('baseline-db')
        assert (initial['inserted'], initial['media_missing'], initial['service_events_inserted']) == (7, 7, 3)
    sfd = broker.os.open(repair, broker.os.O_RDONLY | broker.os.O_DIRECTORY)
    dfd = broker.os.open(source, broker.os.O_RDONLY | broker.os.O_DIRECTORY)
    try:
        receipts = broker.resumed_photos(checkpoint, hashes, messages)
        broker.publish_resume(sfd, dfd, checkpoint, receipts)
        broker.publish_resume(sfd, dfd, checkpoint, receipts)
    finally:
        broker.os.close(sfd)
        broker.os.close(dfd)
    after = gateway.handle('client', 'ingest', SOURCE)
    assert after['job_id'] != before['job_id']
    assert gateway.handle('client', 'ingest', SOURCE) == after
    assert len(submissions) == 2
    worker_stage = Path(submissions[after['job_id']][3])
    assert worker_stage != original_stage
    assert (original_stage / 'messages.jsonl').is_file()
    assert not (original_stage / 'media-resume').exists()
    assert (worker_stage / 'media-resume/status.json').is_file()
    media = gateway.handle('client', 'media-status', SOURCE)
    assert (media['total'], media['recovered'], media['missing'], media['over_100_MiB']) == (7, 5, 2, 2)
    assert media['missing_message_ids'] == [6, 7]
    assert all('path' not in key for key in media)
    overview = {'documents': 1, 'messages': 7, 'unique_messages': 7,
                'messages_with_media': 5, 'nonempty_messages': 7, 'service_events': 3, 'embeddings': 0}
    previous_worker_api = gateway.worker_api
    gateway.worker_api = lambda action, **kw: overview if action == 'source-overview' else previous_worker_api(action, **kw)
    integrity = gateway.handle('client', 'reindex', SOURCE)['integrity']
    assert integrity == {'media_sha256_verified': 5, 'messages_match': True,
                         'service_events_match': True, 'ingested_media_match': True}
    gateway.db.close()
    for name in ('first', 'repeat'):
        out, _, missing = prepare_export(worker_stage, tmp_path / name, root=root)
        normalized = out / 'normalized' / 'messages.jsonl'
        assert missing == 2
        assert sum(bool(json.loads(line)['media_path']) for line in normalized.read_bytes().splitlines()) == 5
    assert all((source / name).read_bytes() == data for name, data in control.items())
    assert not (source / 'media').exists()
    assert len(list((source / 'media-resume').glob('*/*.jpg'))) == 5
    assert (source / 'media-resume').stat().st_mode & 0o777 == 0o700
    assert (source / 'media-resume/1/fixture.jpg').stat().st_mode & 0o777 == 0o600

    if live_db:
        first = ingest('merged-db')
        repeat = ingest('repeat-db')
        for result, updated in ((first, 5), (repeat, 0)):
            assert result['inserted'] == 0
            assert result['updated'] == updated
            assert result['media_missing'] == 2
            assert result['service_events_skipped'] == 3
        with psycopg.connect(dsn) as conn:
            assert conn.execute("SELECT count(*), count(DISTINCT external_ref) FROM segments").fetchone() == (7, 7)
            assert conn.execute("SELECT count(*) FROM segments WHERE meta->>'media_path' LIKE 'media-resume/%'").fetchone()[0] == 5
            assert conn.execute("SELECT count(*) FROM telegram_service_events").fetchone()[0] == 3

        # Use the same persisted normalized input for both attempts, just as broker does.
        from importlib import import_module

        from kiber_analyst.routing import PermanentProviderError
        embed = import_module('kiber_analyst.embed')
        monkeypatch.setattr(embed, '_required_env', lambda path: {})
        monkeypatch.setattr(embed, '_connect', lambda config: psycopg.connect(dsn))
        monkeypatch.setattr(embed, 'BATCH_SIZE', 2)
        sent = []

        def first_provider(texts):
            if len(sent) == 4:
                raise PermanentProviderError('simulated 429', status_code=429, category='rate_limited')
            sent.extend(texts)
            return [[1.0] + [0.0] * 1535 for _ in texts], None

        args = (tmp_path / 'merged-db',)
        options = dict(project='resume-fixture', env_path=tmp_path / 'unused')
        with pytest.raises(PermanentProviderError):
            embed.embed_job(*args, embedding_call=first_provider, **options)
        with psycopg.connect(dsn) as conn:
            assert conn.execute('SELECT count(*) FROM embeddings').fetchone()[0] == 4

        def resume_provider(texts):
            assert not set(texts).intersection(sent)
            sent.extend(texts)
            return [[1.0] + [0.0] * 1535 for _ in texts], None

        completed = embed.embed_job(*args, embedding_call=resume_provider, **options)
        assert len(sent) == len(set(sent)) == completed['inserted'] == 7
        with psycopg.connect(dsn) as conn:
            assert conn.execute('SELECT count(*),count(distinct owner_id) FROM embeddings').fetchone() == (7, 7)
            assert conn.execute("SELECT count(*) FROM runs WHERE kind='telegram_embed' AND status='done'").fetchone()[0] == 1


@pytest.mark.parametrize('damage', ['symlink', 'sha256'])
def test_publish_resume_rejects_unsafe_or_modified_files(tmp_path, monkeypatch, damage):
    monkeypatch.setattr(broker, 'RESERVE', 0)
    original = tmp_path / 'input'
    output = tmp_path / 'output'
    (original / '1').mkdir(mode=0o700, parents=True)
    output.mkdir(mode=0o700)
    photo = original / '1/photo.jpg'
    if damage == 'symlink':
        photo.symlink_to('/dev/null')
    else:
        photo.write_bytes(b'bad')
        photo.chmod(0o600)
    receipts = {1: {'path': 'media-resume/1/photo.jpg', 'size': 3,
                    'sha256': hashlib.sha256(b'old').hexdigest()}}
    source = os.open(original, os.O_RDONLY | os.O_DIRECTORY)
    target = os.open(output, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises((OSError, broker.Refused)):
            broker.publish_resume(source, target, b'{}', receipts)
    finally:
        os.close(source)
        os.close(target)
    assert not list(output.iterdir())


def test_waiting_status_reports_safe_worker_detail(setup):
    app, _, jobs, _ = setup
    submission = app.handle('client', 'ingest', SOURCE)
    when = '2020-01-02T10:19:14+00:00'
    jobs[submission['job_id']].update(
        state='waiting_retry', next_run_at=when, updated_at=when,
        progress={'completed_batches': 2, 'internal_path': '/private'},
        last_provider_error={'http_code': 429, 'category': 'rate_limited',
                             'scopes': ['google:gemini_embed_2'], 'diagnostic': 'private'},
    )
    status = app.handle('client', 'job-status', submission['request_id'])
    assert status == {
        'status': 'waiting_retry', 'counts': {'completed_batches': 2}, 'error_code': None,
        'attempt': 1, 'last_error_code': 'HTTP_429', 'next_retry_at': when,
        'retryable': False, 'attempts_remaining': 2, 'completed_batches': 2,
        'retry_reason': 'quota', 'retry_after_seconds': None,
        'profile_scope': 'google:gemini_embed_2', 'created_at': status['created_at'],
        'updated_at': when,
    }


def test_failed_embed_429_retries_with_same_checkpoint_input(setup):
    app, _, jobs, argv = setup
    ing = app.handle('client', 'ingest', SOURCE)
    jobs[ing['job_id']]['state'] = 'done'
    emb = app.handle('client', 'embed', SOURCE)
    jobs[emb['job_id']].update(
        state='failed', progress={'completed_batches': 2},
        error={'category': 'retry_exhausted'},
        attempts={'items': [{'status': 'failed', 'category': 'rate_limited'}]},
        last_provider_error={'http_code': 429}, next_run_at=None,
    )
    status = app.handle('client', 'job-status', emb['request_id'])
    assert status['retryable'] and status['last_error_code'] == 'HTTP_429'
    assert status['completed_batches'] == 2
    retry = app.handle('client', 'embed', SOURCE)
    assert retry['request_id'] == emb['request_id'] and retry['job_id'] != emb['job_id']
    assert retry['attempt'] == 2 and argv[-1] == argv[-2]
    assert app.handle('client', 'embed', SOURCE) == retry
    assert app.handle('client', 'job-status', retry['request_id'])['completed_batches'] == 2


def test_control_plane_filters_and_audit_hide_other_owner(setup):
    app, _, jobs, _ = setup
    ours = app.handle('client', 'ingest', SOURCE)
    theirs = app.handle('other', 'ingest', SOURCE)
    jobs[ours['job_id']]['state'] = 'waiting_retry'
    assert [r['request_id'] for r in app.handle('client', 'job-list',
            f'source_id={SOURCE}', 'status=waiting_retry', 'owner=client')['jobs']] == [ours['request_id']]
    with pytest.raises(broker.Refused, match='OWNER_NOT_ALLOWED'):
        app.handle('client', 'job-list', 'owner=other')
    for command in ('job-logs', 'cancel'):
        with pytest.raises(broker.Refused, match='REQUEST_NOT_FOUND'):
            app.handle('client', command, theirs['request_id'])
    app.audit('client', 'job-status', [ours['request_id']], {'ok': True, 'body': 'secret'})
    app.audit('other', 'job-status', [theirs['request_id']], {'ok': True})
    audit = app.handle('client', 'audit-log')['events']
    assert len(audit) == 1 and audit[0]['job_id'] == ours['job_id']
    assert audit[0]['source_id'] == SOURCE and 'secret' not in json.dumps(audit)


def test_provider_health_exposes_counts_and_cooldowns_only(setup):
    import sqlite3
    app, _, _, _ = setup
    with sqlite3.connect(app.root / 'queue.db') as conn:
        conn.executescript("""
        CREATE TABLE attempts(profile, http_code, finished_at, diagnostic, job_id);
        CREATE TABLE interactive_attempts(profile, http_code, finished_at);
        CREATE TABLE routing_state(pool_name, cooldowns_json);
        """)
        conn.execute('INSERT INTO attempts VALUES(?,?,?,?,?)',
                     ('gemini_embed', 429, broker.timestamp(), 'Bearer PRIVATE', 'internal'))
        conn.execute('INSERT INTO attempts VALUES(?,?,?,?,?)',
                     ('gemini_embed', 502, broker.timestamp(), 'proxy PRIVATE', 'internal'))
        conn.execute('INSERT INTO routing_state VALUES(?,?)', ('gemini_embed', json.dumps({
            'gemini_embed': '2099-01-01T00:00:00+00:00', 'https://PRIVATE': '2099-01-01T00:00:00+00:00',
        })))
    from contextlib import closing
    from types import SimpleNamespace

    from kiber_analyst.queue_overview import health

    def connect():
        conn = sqlite3.connect(app.root / 'queue.db')
        conn.row_factory = sqlite3.Row
        return closing(conn)

    app.worker_api = lambda action: health(SimpleNamespace(connect=connect), providers_only=True)
    result = app.handle('client', 'provider-health')
    assert result['profiles'] == [{'profile': 'gemini_embed', 'http_429': 1, 'http_5xx': 1}]
    assert result['blocked_until']['gemini_embed'] == {'gemini_embed': '2099-01-01T00:00:00+00:00'}
    assert 'PRIVATE' not in json.dumps(result)


def test_renew_embed_is_explicit_owner_scoped_and_reuses_checkpoint(setup):
    app, _, jobs, argv = setup
    ing = app.handle('client', 'ingest', SOURCE)
    jobs[ing['job_id']]['state'] = 'done'
    first = app.handle('client', 'embed', SOURCE)
    request_id = first['request_id']
    current = first
    for attempt in range(1, 4):
        jobs[current['job_id']].update(
            state='failed', progress={'completed_batches': 2 if attempt == 1 else 0},
            error={'category': 'retry_exhausted'},
            attempts={'items': [{'status': 'failed', 'category': 'rate_limited'}]},
            last_provider_error={'http_code': 429},
        )
        if attempt < 3:
            with pytest.raises(broker.Refused, match='RENEW_EMBED_NOT_ALLOWED'):
                app.handle('client', 'renew-embed', request_id)
            current = app.handle('client', 'retry', request_id)
    old_job = current['job_id']
    assert app.handle('client', 'job-status', request_id)['attempts_remaining'] == 0
    with pytest.raises(broker.Refused, match='REQUEST_NOT_FOUND'):
        app.handle('other', 'renew-embed', request_id)
    with pytest.raises(broker.Refused, match='RETRY_LIMIT'):
        app.handle('client', 'retry', request_id)

    renewal = app.handle('client', 'renew-embed', request_id)
    assert renewal['request_id'] != request_id and renewal['job_id'] != old_job
    assert renewal['previous_job_id'] == old_job and renewal['renewal_reason'] == 'rate_limited'
    assert renewal['attempt'] == 1 and renewal['completed_batches'] == 2
    assert all(command == argv[-1] for command in argv[1:])
    assert app.handle('client', 'renew-embed', request_id) == renewal
    assert app.handle('client', 'embed', SOURCE)['request_id'] == renewal['request_id']
    assert len(argv) == 5
    assert app.handle('client', 'job-status', request_id)['attempt'] == 3
    assert app.handle('client', 'job-status', renewal['request_id'])['completed_batches'] == 2
    app.audit('client', 'renew-embed', [request_id], {'ok': True, **renewal})
    entry = app.handle('client', 'audit-log')['events'][0]
    assert (entry['previous_request_id'], entry['previous_job_id'], entry['job_id'], entry['renewal_reason']) == (
        request_id, old_job, renewal['job_id'], 'rate_limited',
    )

    jobs[renewal['job_id']].update(state='failed', error={'category': 'server_error'})
    assert app.handle('client', 'retry', renewal['request_id'])['attempt'] == 2
    assert argv[-1] == argv[-2]
