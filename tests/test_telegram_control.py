from __future__ import annotations

import json

import pytest

from kiber_analyst import telegram_control
from kiber_analyst.telegram_exporter import ExportError

SOURCE = 'telegram:-10012345:topic:2049'


@pytest.mark.parametrize(
    ('command', 'expected'),
    [
        (f'export {SOURCE}', ['export', '-10012345', '2049', 'MEDIA']),
        (f'export {SOURCE} 100MiB',
         ['export', '-10012345', '2049', 'MEDIA', '104857600B']),
        (f'resume {SOURCE} 4000000000B',
         ['export-resume', '-10012345', '2049', '4000000000B']),
        (f'services {SOURCE}', ['export-services', '-10012345', '2049']),
        (f'status {SOURCE}', ['export-status', '-10012345', '2049']),
        (f'transfer {SOURCE}', ['export-transfer', '-10012345', '2049']),
        (f'transfer-status {SOURCE}',
         ['export-transfer-status', '-10012345', '2049']),
        (f'budget {SOURCE}', ['export-budget', '-10012345', '2049']),
    ],
)
def test_fixed_telegram_control_commands(command, expected):
    assert telegram_control._parse(command) == expected


@pytest.mark.parametrize(
    'command',
    [
        f'export {SOURCE} --path /etc/passwd',
        f'export {SOURCE}; id',
        f'export {SOURCE} 4000000001B',
        f'export {SOURCE} 0B',
        'export telegram:-10012345:topic:99999999999',
        f'export {SOURCE}\nstatus {SOURCE}',
        f'transfer {SOURCE} --force',
        f'../../bin/sh {SOURCE}',
    ],
)
def test_telegram_control_rejects_shell_paths_and_invalid_scopes(command):
    with pytest.raises(ValueError):
        telegram_control._parse(command)


def test_control_execute_calls_only_fixed_exporter_argv(monkeypatch, tmp_path):
    seen = {}

    monkeypatch.setattr(telegram_control, '_trusted_environment', lambda: tmp_path / 'cfg.json')

    def fake_handle(args, *, config_path):
        seen['args'] = args
        seen['config_path'] = config_path
        return {
            'source_id': SOURCE,
            'status': 'complete',
            'messages': 848,
            'manifest_sha256': 'a' * 64,
            'private_path': '/srv/private/export',
        }

    monkeypatch.setattr(telegram_control, 'handle', fake_handle)

    result = telegram_control.execute(f'export {SOURCE} 4000MB')

    assert seen == {
        'args': ['export', '-10012345', '2049', 'MEDIA', '4000000000B'],
        'config_path': str(tmp_path / 'cfg.json'),
    }
    assert result == {
        'ok': True,
        'result': {
            'source_id': SOURCE, 'status': 'complete',
            'manifest_sha256': 'a' * 64,
        },
    }
    assert 'private_path' not in json.dumps(result)


def test_control_returns_safe_specific_transfer_error(monkeypatch, tmp_path):
    monkeypatch.setattr(telegram_control, '_trusted_environment', lambda: tmp_path / 'cfg.json')

    def fail(_args, *, config_path):
        raise ExportError(
            'opaque low-level details', error_code='TRANSFER_RECOVERY_REQUIRED',
        )

    monkeypatch.setattr(telegram_control, 'handle', fail)

    result = telegram_control.execute(f'transfer {SOURCE}')

    assert result == {
        'ok': False,
        'error_code': 'TRANSFER_RECOVERY_REQUIRED',
        'message': 'A previous transfer needs a safe recovery check.',
    }
    assert 'opaque' not in json.dumps(result)


def test_control_preserves_specific_safe_publisher_code(monkeypatch, tmp_path):
    monkeypatch.setattr(telegram_control, '_trusted_environment', lambda: tmp_path / 'cfg.json')
    monkeypatch.setattr(
        telegram_control, 'handle',
        lambda _args, **_kwargs: (_ for _ in ()).throw(
            ExportError('opaque details', error_code='STAGING_REVISION_MISMATCH')
        ),
    )

    result = telegram_control.execute(f'transfer {SOURCE}')

    assert result['error_code'] == 'STAGING_REVISION_MISMATCH'
    assert 'STAGING_REVISION_MISMATCH' in result['message']
    assert 'opaque' not in json.dumps(result)


def test_control_main_uses_forced_command_and_safe_json(monkeypatch, capsys):
    monkeypatch.setenv('SSH_ORIGINAL_COMMAND', f'budget {SOURCE}')
    monkeypatch.setattr(
        telegram_control, 'execute',
        lambda command: {'ok': True, 'result': {'source_id': SOURCE, 'status': 'budget'}},
    )

    assert telegram_control.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        'ok': True, 'result': {'source_id': SOURCE, 'status': 'budget'},
    }
