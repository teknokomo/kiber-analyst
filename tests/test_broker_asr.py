from __future__ import annotations

import hashlib
import json
import os
import stat
import tracemalloc
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker

SOURCE = 'telegram:-1009000:topic:1'


def private_write(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    path.write_bytes(data)
    path.chmod(mode)


@pytest.fixture
def asr_setup(tmp_path, monkeypatch):
    # Synthetic ASR tests exercise staging behavior, not the production disk reserve.
    monkeypatch.setattr(broker, 'RESERVE', 0)
    src = tmp_path / 'imports' / '-1009000' / '1'
    payloads = {message_id: f'voice-{message_id}'.encode() for message_id in range(1, 4)}
    payloads[4] = b'audio-4'
    messages_data = []
    for message_id, payload in payloads.items():
        media_type = 'voice_message' if message_id < 4 else 'audio_file'
        extension = 'ogg' if message_id < 4 else 'mp3'
        messages_data.append({
            'message_id': message_id,
            'topic_id': 1,
            'date': '2020-01-02T12:00:00+03:00',
            'text': '',
            'sender_name': 'Example Author',
            'chat_id': '-1009000',
            'media': {'type': media_type, 'path': f'media/{message_id}/audio.{extension}',
                      'size': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()},
        })
    messages = ''.join(json.dumps(record) + '\n' for record in messages_data).encode()
    services = b''
    message_hash = hashlib.sha256(messages).hexdigest()
    service_hash = hashlib.sha256(services).hexdigest()
    manifest = json.dumps({
        'export_version': 1, 'complete': True, 'scope_before': None,
        'chat_id': '-1009000', 'topic_id': 1, 'source_id': SOURCE,
        'message_count': len(messages_data), 'messages_sha256': message_hash,
        'topic_title': 'Synthetic', 'chat_title': 'Test group',
    }, sort_keys=True).encode()
    service_manifest = json.dumps({
        'source_id': SOURCE, 'chat_id': -1009000, 'topic_id': 1,
        'complete': True, 'range_complete': True, 'scope_before': None,
        'errors': [], 'event_count': 0, 'services_sha256': service_hash,
    }, sort_keys=True).encode()
    files = {
        'messages.jsonl': messages,
        'services.jsonl': services,
        'manifest.json': manifest,
        'services_manifest.json': service_manifest,
    }
    for message_id, payload in payloads.items():
        extension = 'ogg' if message_id < 4 else 'mp3'
        files[f'media/{message_id}/audio.{extension}'] = payload
    for name, data in files.items():
        private_write(src / name, data)
    directories = [tmp_path, tmp_path / 'imports', tmp_path / 'imports' / '-1009000', src, src / 'media']
    directories.extend(src / 'media' / str(message_id) for message_id in payloads)
    for directory in directories:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
    hashes = {name: hashlib.sha256(files[name]).hexdigest()
              for name in ('messages.jsonl', 'services.jsonl', 'manifest.json')}
    (tmp_path / 'broker').mkdir(mode=0o700)
    (tmp_path / 'imports/.broker').mkdir(mode=0o700)
    original = {name: (src / name).read_bytes() for name in files}
    jobs = {}
    argv_log = []

    def submit(argv, *, idempotency_key):
        job_id = str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))
        argv_log.append(argv)
        jobs.setdefault(job_id, {'state': 'queued', 'progress': {}})
        return {'job_id': job_id}

    overview = {
        'documents': 1, 'files_total': 4, 'files_done': 0, 'files_failed': 0,
        'files_skipped': 0, 'files_pending': 4, 'segments': 0, 'empty': 0,
        'last_error_code': None, 'completed_refs': [], 'failed_refs': [],
        'run_keys': [], 'revision': 'a' * 64,
    }

    def worker(action, **kwargs):
        if action == 'asr-source-overview':
            return overview.copy()
        job = jobs[kwargs['job_id']]
        if action == 'status':
            return job
        if action == 'result':
            return job
        raise AssertionError(action)

    app = broker.Broker(tmp_path, {SOURCE: {'project': 'sample-project', 'hashes': hashes,
                                           'asr_enabled': True}},
                        worker_api=worker, submit=submit)
    yield app, src, jobs, argv_log, overview, original
    app.db.close()


def test_asr_command_stages_private_audio_and_is_idempotent(asr_setup):
    app, source_dir, jobs, argv_log, _overview, original = asr_setup
    result = app.handle('client', 'asr', SOURCE)
    assert result == app.handle('client', 'asr', SOURCE)
    assert len(argv_log) == 1
    argv = argv_log[0]
    assert argv[:2] == ['telegram', 'asr']
    assert '--load-results' in argv
    assert '--run-id' in argv
    assert argv.count('--external-ref') == 3
    first_ref = argv.index('--external-ref')
    assert {argv[index + 1] for index in range(first_ref, len(argv), 2)} == {
        f'tg:-1009000:{message_id}' for message_id in range(1, 4)
    }
    workspace = Path(argv[argv.index('--job') + 1])
    assert stat.S_IMODE(workspace.stat().st_mode) == 0o700
    assert stat.S_IMODE((workspace / 'normalized/messages.jsonl').stat().st_mode) == 0o600
    staged_audio = workspace / 'unpacked/media/1/audio.ogg'
    assert staged_audio.read_bytes() == b'voice-1'
    assert stat.S_IMODE(staged_audio.stat().st_mode) == 0o600
    assert {name: (source_dir / name).read_bytes() for name in original} == original

    overview = _overview
    overview.update(
        files_done=3,
        files_pending=1,
        completed_refs=[f'tg:-1009000:{message_id}' for message_id in range(1, 4)],
    )
    jobs[result['job_id']]['state'] = 'done'
    second = app.handle('client', 'asr', SOURCE)
    assert argv_log[-1].count('--external-ref') == 1
    assert argv_log[-1][argv_log[-1].index('--external-ref') + 1] == 'tg:-1009000:4'
    assert second['request_id'] != result['request_id']
    assert app.handle('client', 'asr', SOURCE) == second
    assert len(argv_log) == 2


def test_asr_status_returns_only_documented_safe_fields(asr_setup):
    app, _source, _jobs, _argv, overview, _original = asr_setup
    overview.update(files_total=4, files_done=2, files_failed=1,
                    files_skipped=1, segments=8, empty=1,
                    last_error_code='HTTP_413', private_path='/secret')
    result = app.handle('client', 'asr-status', SOURCE)
    assert result == {
        'files_total': 4, 'files_done': 2, 'files_failed': 1,
        'files_skipped': 1, 'segments': 8, 'empty': 1,
        'last_error_code': 'HTTP_413',
    }


def test_asr_retry_after_transient_error_reuses_same_run_and_selection(asr_setup):
    app, _source, jobs, argv_log, _overview, _original = asr_setup
    first = app.handle('client', 'asr', SOURCE)
    jobs[first['job_id']].update(
        state='failed', error={'category': 'rate_limited'},
        last_provider_error={'http_code': 429, 'category': 'rate_limited'},
    )
    status = app.handle('client', 'job-status', first['request_id'])
    assert status['retryable'] is True

    retried = app.handle('client', 'retry', first['request_id'])

    assert retried['request_id'] == first['request_id']
    assert retried['attempt'] == 2
    assert retried['job_id'] != first['job_id']
    assert len(argv_log) == 2
    assert argv_log[0] == argv_log[1]
    assert argv_log[0][argv_log[0].index('--run-id') + 1].startswith('ka-')


def test_asr_retry_uses_enabled_static_source_when_managed_candidate_is_normalized(asr_setup):
    app, _source, jobs, argv_log, _overview, _original = asr_setup
    app.authorized_projects.add('sample-project')
    static_entry = app.sources[SOURCE]
    with app.db:
        app.db.execute(
            'INSERT INTO managed_sources(owner,source,project,provider,metadata,added) '
            'VALUES(?,?,?,?,?,?)',
            ('client', SOURCE, 'sample-project', 'telegram',
             json.dumps({'hashes': static_entry['hashes']}), '2026-01-01T00:00:00+00:00'),
        )
    candidates = app._retry_source_candidates('client', SOURCE, 'sample-project')
    assert candidates[0][0].get('asr_enabled') is None
    assert any(candidate[0].get('asr_enabled') is True for candidate in candidates)
    first = app.handle('client', 'asr', SOURCE)
    original_argv = list(argv_log[-1])
    jobs[first['job_id']].update(
        state='failed', error={'category': 'rate_limited'},
        last_provider_error={'http_code': 429, 'category': 'rate_limited'},
    )

    retried = app.handle('client', 'retry', first['request_id'])

    assert retried['request_id'] == first['request_id']
    assert retried['attempt'] == 2
    assert retried['job_id'] != first['job_id']
    assert len(argv_log) == 2
    assert argv_log[0] == original_argv == argv_log[1]


def test_asr_retry_still_requires_current_static_asr_enablement(asr_setup):
    app, _source, jobs, argv_log, _overview, _original = asr_setup
    app.authorized_projects.add('sample-project')
    static_entry = app.sources[SOURCE]
    with app.db:
        app.db.execute(
            'INSERT INTO managed_sources(owner,source,project,provider,metadata,added) '
            'VALUES(?,?,?,?,?,?)',
            ('client', SOURCE, 'sample-project', 'telegram',
             json.dumps({'hashes': static_entry['hashes']}), '2026-01-01T00:00:00+00:00'),
        )
    first = app.handle('client', 'asr', SOURCE)
    jobs[first['job_id']].update(
        state='failed', error={'category': 'rate_limited'},
        last_provider_error={'http_code': 429, 'category': 'rate_limited'},
    )
    app.sources[SOURCE].pop('asr_enabled')

    with pytest.raises(broker.Refused, match='ASR_NOT_ENABLED'):
        app.handle('client', 'retry', first['request_id'])

    assert len(argv_log) == 1


def test_embed_asr_requires_asr_completion_and_uses_asr_only_job(asr_setup):
    app, _source, jobs, argv_log, overview, _original = asr_setup
    with pytest.raises(broker.Refused, match='ASR_INCOMPLETE'):
        app.handle('client', 'embed-asr', SOURCE)
    overview.update(files_done=1, files_pending=0, completed_refs=['tg:-1009000:1'])
    result = app.handle('client', 'embed-asr', SOURCE)
    assert argv_log[-1] == [
        'asr', 'embed', '--all-runs', '--project', 'sample-project',
        '--source-id', SOURCE,
    ]
    assert jobs[result['job_id']]['state'] == 'queued'
    assert result['request_id']


def test_asr_and_embed_asr_fail_closed_until_operator_enables_source(asr_setup):
    app, _source, _jobs, argv_log, _overview, _original = asr_setup
    app.sources[SOURCE].pop('asr_enabled')
    with pytest.raises(broker.Refused, match='ASR_NOT_ENABLED'):
        app.handle('client', 'asr', SOURCE)
    with pytest.raises(broker.Refused, match='ASR_NOT_ENABLED'):
        app.handle('client', 'embed-asr', SOURCE)
    assert argv_log == []


def test_asr_workspace_skips_missing_oversize_and_provider_limit_without_mutating_source(
    asr_setup, monkeypatch,
):
    app, source_dir, _jobs, _argv, _overview, _original = asr_setup
    records = [json.loads(line) for line in (source_dir / 'messages.jsonl').read_text().splitlines()]
    sizes = {5: broker.ASR_MAX_MEDIA_BYTES + 1, 6: broker.ASR_MAX_FILE_BYTES + 1}
    for message_id, size in sizes.items():
        extension = 'ogg'
        directory = source_dir / 'media' / str(message_id)
        directory.mkdir(mode=0o700)
        path = directory / f'audio.{extension}'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.ftruncate(fd, size)
        finally:
            os.close(fd)
        with path.open('rb') as media_stream:
            media_sha256 = hashlib.file_digest(media_stream, 'sha256').hexdigest()
        records.append({
            'message_id': message_id, 'topic_id': 1,
            'date': '2020-01-02T12:00:00+03:00', 'text': '',
            'sender_name': 'Example Author', 'chat_id': '-1009000',
            'media': {'type': 'voice_message', 'path': f'media/{message_id}/audio.ogg',
                      'size': size, 'max_file_bytes': 4_000_000_000,
                      'sha256': media_sha256},
        })
    records.append({
        'message_id': 7, 'topic_id': 1,
        'date': '2020-01-02T12:00:00+03:00', 'text': '',
        'sender_name': 'Example Author', 'chat_id': '-1009000',
        'media': {'type': 'audio_file', 'path': 'media/7/missing.ogg',
                  'size': 1234, 'max_file_bytes': 4_000_000_000,
                  'sha256': 'b' * 64},
    })
    messages = ''.join(json.dumps(item) + '\n' for item in records).encode()
    private_write(source_dir / 'messages.jsonl', messages)
    message_hash = hashlib.sha256(messages).hexdigest()
    manifest = json.loads((source_dir / 'manifest.json').read_text())
    manifest.update(message_count=len(records), messages_sha256=message_hash,
                    max_file_bytes=4_000_000_000, max_file_size_unit='bytes')
    manifest_data = json.dumps(manifest, sort_keys=True).encode()
    private_write(source_dir / 'manifest.json', manifest_data)
    app.sources[SOURCE]['hashes']['messages.jsonl'] = message_hash
    app.sources[SOURCE]['hashes']['manifest.json'] = hashlib.sha256(manifest_data).hexdigest()
    source_before = {
        name: (source_dir / name).read_bytes()
        for name in ('messages.jsonl', 'services.jsonl', 'manifest.json', 'services_manifest.json')
    }
    monkeypatch.setattr(
        broker.os, 'fstatvfs', lambda _fd: type('FS', (), {'f_bavail': 100_000_000_000, 'f_frsize': 1})(),
    )

    plan, workspace = app.asr_workspace(SOURCE, app.sources[SOURCE], source_dir, 'f' * 64, '')

    assert plan['files_total'] == len(records)
    assert plan['files_eligible'] == 4
    assert plan['skipped_by_reason'] == {
        'over_100_MiB': 1, 'provider_file_limit': 1, 'missing_media': 1,
    }
    assert not (workspace / 'unpacked/media/5/audio.ogg').exists()
    assert not (workspace / 'unpacked/media/6/audio.ogg').exists()
    assert not (workspace / 'unpacked/media/7/missing.ogg').exists()
    assert {
        name: (source_dir / name).read_bytes() for name in source_before
    } == source_before


def test_large_asr_workspace_is_streamed_and_reopens_with_the_same_limit(asr_setup):
    app, source_dir, _jobs, _argv, _overview, _original = asr_setup
    message_count = 4000
    payload = b"a"
    media_sha = hashlib.sha256(payload).hexdigest()
    messages_path = source_dir / "messages.jsonl"
    digest = hashlib.sha256()
    with messages_path.open("wb") as stream:
        for message_id in range(message_count, 0, -1):
            media_dir = source_dir / "media" / str(message_id)
            media_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            media_dir.chmod(0o700)
            private_write(media_dir / "audio.ogg", payload)
            record = {
                "message_id": message_id,
                "topic_id": 1,
                "topic_id_status": "known",
                "date": "2020-01-02T12:00:00+03:00",
                "text": "x" * 10_000,
                "sender_name": "Example Author",
                "sender_id": 42,
                "chat_id": "-1009000",
                "media": {
                    "type": "voice_message", "path": f"media/{message_id}/audio.ogg",
                    "size": 1, "size_bytes": 1, "max_file_bytes": 4_000_000_000,
                    "sha256": media_sha, "export_status": "downloaded",
                },
            }
            encoded = (json.dumps(record, separators=(",", ":")) + "\n").encode()
            stream.write(encoded)
            digest.update(encoded)
    messages_path.chmod(0o600)
    message_hash = digest.hexdigest()
    manifest_path = source_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.update(message_count=message_count, messages_sha256=message_hash,
                    max_file_bytes=4_000_000_000)
    manifest_bytes = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    private_write(manifest_path, manifest_bytes)
    app.sources[SOURCE]["hashes"]["messages.jsonl"] = message_hash
    app.sources[SOURCE]["hashes"]["manifest.json"] = hashlib.sha256(manifest_bytes).hexdigest()

    tracemalloc.start()
    try:
        plan, workspace = app.asr_workspace(
            SOURCE, app.sources[SOURCE], source_dir, "f" * 64, "",
        )
        normalized = workspace / "normalized" / "messages.jsonl"
        assert normalized.stat().st_size > 38 * 1024**2
        again, reused = app.asr_workspace(
            SOURCE, app.sources[SOURCE], source_dir, "f" * 64, "",
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert reused == workspace
    assert again == plan
    assert plan["files_eligible"] == message_count
    assert plan["files_total"] == message_count
    assert peak < 64 * 1024**2
