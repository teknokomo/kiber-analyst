from __future__ import annotations

import io
import json
import shlex

import pytest

from kiber_analyst import broker_client

SOURCE = 'telegram:-1009000:topic:1'


@pytest.mark.parametrize('limit', ['1', '5'])
def test_unquoted_multiword_query_is_joined_before_broker_rpc(limit):
    args = shlex.split(f'search {SOURCE} Example Station {limit}')
    assert broker_client.normalize_args(args) == [
        'search', SOURCE, 'Example Station', limit,
    ]


def test_quoted_multiword_query_and_optional_limit_are_preserved():
    quoted_with_limit = shlex.split(f'search {SOURCE} "Example Station" 5')
    quoted_without_limit = shlex.split(f'search {SOURCE} "Example Station"')
    assert broker_client.normalize_args(quoted_with_limit) == [
        'search', SOURCE, 'Example Station', '5',
    ]
    assert broker_client.normalize_args(quoted_without_limit) == [
        'search', SOURCE, 'Example Station',
    ]


@pytest.mark.parametrize('limit', ['0', '51', '-1', 'junk'])
def test_search_client_preserves_invalid_limit_for_broker_validation(limit):
    args = shlex.split(f'search {SOURCE} Example Station {limit}')
    assert broker_client.normalize_args(args) == [
        'search', SOURCE, 'Example Station', limit,
    ]


def test_client_sends_query_as_one_rpc_argument(monkeypatch, capsys):
    captured = {}

    class FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def settimeout(self, _timeout):
            pass

        def connect(self, _path):
            pass

        def sendall(self, payload):
            captured['args'] = json.loads(payload)

        def makefile(self, *_args):
            return io.BytesIO(b'{"ok":true}\n')

    monkeypatch.setattr(broker_client.socket, 'socket', lambda *_args: FakeSocket())
    args = shlex.split(f'search {SOURCE} Example Station 5')
    broker_client.handle(args)
    capsys.readouterr()
    assert captured['args'] == ['search', SOURCE, 'Example Station', '5']


@pytest.mark.parametrize('args', [
    ['ingest', SOURCE, 'unexpected', 'args', 'here'],
    ['search', SOURCE, *(['x'] * 31), '5'],
    ['search', SOURCE, 'x' * 181, 'y' * 181, '5'],
    ['search', SOURCE, None, '5'],
])
def test_client_rejects_oversized_or_malformed_arguments(args):
    with pytest.raises(ValueError, match='INVALID_REQUEST'):
        broker_client.normalize_args(args)
