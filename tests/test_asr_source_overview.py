from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kiber_analyst import asr_source_overview
from kiber_analyst.jobqueue import JobsError


class _Cursor:
    def __init__(self, *, one=None, rows=()):
        self._one = one
        self._rows = list(rows)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Connection(_Transaction):
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    def transaction(self):
        return _Transaction()

    def execute(self, query, params=()):
        self.statements.append((query, params))
        if 'SELECT count(*) FROM documents' in query:
            return _Cursor(one=(1,))
        if query.lstrip().startswith('WITH source AS'):
            return _Cursor(rows=self.rows)
        return _Cursor()


def test_asr_source_overview_is_read_only_and_counts_done_failed_and_skipped(monkeypatch):
    moment = datetime(2026, 11, 20, tzinfo=UTC)
    rows = [
        ('tg:-1001:1', 'voice_message', 'media/1/a.ogg', 100, 'complete', 200, None,
         'run-a', 4, 1, moment, moment),
        ('tg:-1001:2', 'audio', 'media/2/b.ogg', 100, 'failed', 429, 'RateLimitError',
         'run-a', 0, 0, moment, moment),
        ('tg:-1001:3', 'audio', None, None, None, None, None, None, 0, 0, None, None),
        ('tg:-1001:4', 'audio', 'media/4/d.ogg', 100 * 1024**2 + 1, None, None,
         None, None, 0, 0, None, None),
        ('tg:-1001:5', 'audio', 'media/5/e.ogg', 25_000_001, None, None,
         None, None, 0, 0, None, None),
    ]
    conn = _Connection(rows)
    monkeypatch.setattr('kiber_analyst.load._required_env', lambda _path: object())
    monkeypatch.setattr('kiber_analyst.load._connect', lambda _config: conn)

    result = asr_source_overview.asr_source_overview(
        'telegram:-1001:topic:1', 'sample-project'
    )

    assert {key: result[key] for key in (
        'files_total', 'files_eligible', 'files_done', 'files_failed',
        'files_skipped', 'files_pending', 'skipped_by_reason',
        'segments', 'empty', 'last_error_code',
    )} == {
        'files_total': 5, 'files_eligible': 2, 'files_done': 1,
        'files_failed': 1, 'files_skipped': 3, 'files_pending': 0,
        'skipped_by_reason': {
            'missing_media': 1, 'over_100_MiB': 1, 'provider_file_limit': 1,
        },
        'segments': 4, 'empty': 1, 'last_error_code': 'HTTP_429',
    }
    assert result['completed_refs'] == ['tg:-1001:1']
    assert result['failed_refs'] == ['tg:-1001:2']
    assert 'SET TRANSACTION READ ONLY' in [query for query, _ in conn.statements]
    assert 'SET LOCAL statement_timeout = \'10s\'' in [query for query, _ in conn.statements]
    assert all(
        query.lstrip().startswith(('SELECT', 'WITH', 'SET'))
        for query, _ in conn.statements
    )
    source_query, source_params = next(
        (query, params) for query, params in conn.statements
        if query.lstrip().startswith('WITH source AS')
    )
    assert "g.meta->>'media_type' = ANY(%s)" in source_query
    assert "substring(g.meta->>'media_path'" not in source_query
    assert source_params[-1] == list(asr_source_overview._AUDIO_TYPES)


def test_asr_source_overview_does_not_classify_mp4_without_audio_media_type():
    assert 'mp4' not in asr_source_overview._AUDIO_TYPES
    assert "g.meta->>'media_type' = ANY(%s)" in asr_source_overview._ROWS_SQL
    assert "lower(substring(g.meta->>'media_path'" not in asr_source_overview._ROWS_SQL


def test_asr_source_overview_rejects_untrusted_source_before_database_access(monkeypatch):
    monkeypatch.setattr(
        'kiber_analyst.load._connect',
        lambda _config: pytest.fail('database must not be opened'),
    )
    with pytest.raises(JobsError, match='Invalid ASR source scope'):
        asr_source_overview.asr_source_overview('telegram:-1001:topic:0', 'sample-project')
