"""Fixed SSH forced-command interface for the Telegram export account."""

from __future__ import annotations

import json
import os
import pwd
import re
import shlex
import sys
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.telegram_exporter import ExportError, _file_limit_bytes, handle
from kiber_analyst.telegram_source import parse_telegram_source

_MAX_COMMAND_BYTES = 1024
_SAFE_RESULT_FIELDS = {
    'source_id', 'status', 'complete', 'message_count', 'service_event_count',
    'services_complete', 'messages_integrity', 'messages_sha256',
    'services_sha256', 'manifest_sha256', 'max_file_bytes',
    'max_file_bytes_ceiling', 'max_file_size_unit', 'requested_max_file_bytes',
    'effective_max_file_bytes', 'media_count', 'remaining', 'download_errors',
    'downloaded', 'already_present', 'skipped', 'skipped_by_reason',
    'checkpoint_sha256', 'updated_at', 'size_bytes', 'available_bytes',
    'storage_limit_bytes', 'min_free_bytes', 'free_bytes', 'used_bytes',
    'copied_bytes', 'shared_free_bytes', 'shared_used_bytes', 'revision',
    'revisions', 'recovery_cleanup', 'restored', 'reclaimed_bytes',
    'removed_artifacts', 'preserved_candidates', 'preserved_reasons',
}
_SAFE_NESTED_FIELDS = {
    'skipped_by_reason': None,
    'recovery_cleanup': {
        'removed_artifacts', 'reclaimed_bytes', 'preserved_candidates',
        'preserved_reasons',
    },
    'preserved_reasons': None,
}
_SAFE_MESSAGES = {
    'INVALID_REQUEST': 'Use one of the fixed Telegram export commands.',
    'INVALID_SOURCE_ID': 'The Telegram source identifier is invalid.',
    'INVALID_LIMIT': 'The per-file limit must be from 1 byte through 4,000,000,000 bytes.',
    'EXPORT_CONFIG_UNSUPPORTED': 'The server has a legacy exporter configuration that needs migration.',
    'TRANSFER_RECOVERY_REQUIRED': 'A previous transfer needs a safe recovery check.',
    'TRANSFER_HELPER_UNAVAILABLE': 'The configured publisher helper is unavailable.',
    'TRANSFER_HELPER_FAILED': 'The publisher rejected the transfer; inspect its safe error code.',
    'EXPORT_CONFIG_REQUIRED': 'The Telegram exporter configuration is missing.',
    'EXPORT_CONFIG_INVALID': 'The Telegram exporter configuration is invalid.',
    'TRANSFER_CONFIG_INVALID': 'The publisher configuration is invalid or incompatible.',
    'INSUFFICIENT_SHARED_STORAGE': 'The shared storage budget or free-space reserve is insufficient.',
    'STORAGE_POLICY_MISMATCH': 'Exporter and publisher storage limits do not match.',
    'MEDIA_EXPORT_INCOMPLETE': 'The export still has media that is missing or unresolved.',
    'MEDIA_HASH_MISMATCH': 'A media file failed its integrity check.',
    'MESSAGES_HASH_MISMATCH': 'The exported message file failed its integrity check.',
    'DATA_DISK_NOT_MOUNTED': 'The configured data volume is not mounted.',
    'DATA_DISK_UUID_MISMATCH': 'The mounted data volume does not match the configured identity.',
}


def _trusted_environment() -> Path:
    """Drop SSH-provided environment overrides before resolving local config."""
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise ValueError('CONTROL_USER_INVALID') from None
    if not home.is_absolute():
        raise ValueError('CONTROL_USER_INVALID')
    os.environ.clear()
    os.environ.update({
        'HOME': str(home), 'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
    })
    return config_path('telegram-export.json')


def _parse(command: str) -> list[str]:
    if (not isinstance(command, str) or not command or len(command.encode()) > _MAX_COMMAND_BYTES
            or '\x00' in command or '\n' in command or '\r' in command):
        raise ValueError('INVALID_REQUEST')
    try:
        words = shlex.split(command, posix=True)
    except ValueError:
        raise ValueError('INVALID_REQUEST') from None
    if not words or any(len(word) > 180 for word in words):
        raise ValueError('INVALID_REQUEST')
    operation, *args = words
    if operation not in {
        'export', 'resume', 'services', 'status', 'transfer', 'transfer-status', 'budget',
    }:
        raise ValueError('COMMAND_NOT_ALLOWED')
    if not args or parse_telegram_source(args[0]) is None:
        raise ValueError('INVALID_SOURCE_ID')
    source_id, *rest = args
    chat, topic = parse_telegram_source(source_id) or (None, None)
    scope = 'GROUP' if topic is None else topic
    result: list[str]
    if operation == 'export':
        if len(rest) > 1:
            raise ValueError('INVALID_REQUEST')
        result = ['export', chat, scope, 'MEDIA']
        if rest:
            try:
                result.append(f'{_file_limit_bytes(rest[0])}B')
            except ExportError:
                raise ValueError('INVALID_LIMIT') from None
    elif operation == 'resume':
        if len(rest) > 1:
            raise ValueError('INVALID_REQUEST')
        result = ['export-resume', chat, scope]
        if rest:
            try:
                result.append(f'{_file_limit_bytes(rest[0])}B')
            except ExportError:
                raise ValueError('INVALID_LIMIT') from None
    else:
        if rest:
            raise ValueError('INVALID_REQUEST')
        result = {
            'services': ['export-services', chat, scope],
            'status': ['export-status', chat, scope],
            'transfer': ['export-transfer', chat, scope],
            'transfer-status': ['export-transfer-status', chat, scope],
            'budget': ['export-budget', chat, scope],
        }[operation]
    return result


def _safe_result(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError('EXPORT_RESPONSE_INVALID')
    safe: dict[str, Any] = {}
    for key in _SAFE_RESULT_FIELDS.intersection(value):
        item = value[key]
        if key in _SAFE_NESTED_FIELDS and isinstance(item, dict):
            nested_allowed = _SAFE_NESTED_FIELDS[key]
            safe[key] = {
                str(nested_key): nested_value
                for nested_key, nested_value in item.items()
                if (nested_allowed is None or nested_key in nested_allowed)
                and isinstance(nested_key, str)
                and type(nested_value) in (str, int, bool, type(None))
            }
        elif type(item) in (str, int, bool, type(None)):
            safe[key] = item
    if 'source_id' not in safe or 'status' not in safe:
        raise ValueError('EXPORT_RESPONSE_INVALID')
    return safe


def execute(command: str) -> dict[str, Any]:
    args = _parse(command)
    config = _trusted_environment()
    try:
        result = handle(args, config_path=str(config))
        return {'ok': True, 'result': _safe_result(result)}
    except ExportError as exc:
        code = exc.error_code or str(exc)
        if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,80}', code):
            code = 'EXPORT_FAILED'
        return {
            'ok': False,
            'error_code': code,
            'message': _SAFE_MESSAGES.get(
                code, f'Telegram export operation failed safely ({code}).'
            ),
        }
    except ValueError as exc:
        code = str(exc)
        if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,80}', code):
            code = 'EXPORT_FAILED'
        return {
            'ok': False,
            'error_code': code,
            'message': _SAFE_MESSAGES.get(code, 'Telegram export request was rejected.'),
        }
    except Exception:  # noqa: BLE001 - forced-command output must remain safe.
        return {
            'ok': False, 'error_code': 'EXPORT_FAILED',
            'message': 'Telegram export operation failed safely.',
        }


def main() -> int:
    command = os.environ.get('SSH_ORIGINAL_COMMAND', '')
    response = execute(command)
    sys.stdout.write(json.dumps(response, ensure_ascii=False, sort_keys=True) + '\n')
    return 0 if response['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
