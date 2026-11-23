"""Restricted Unix RPC gateway for allowlisted source-ingestion jobs."""
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
import re
import shutil
import socket
import socketserver
import sqlite3
import stat
import struct
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath

from kiber_analyst import __version__ as KIBER_ANALYST_VERSION
from kiber_analyst.config_paths import config_path, data_path, runtime_path
from kiber_analyst.connectors.source_ids import parse_source_id
from kiber_analyst.jobapi import api_call, submit_command
from kiber_analyst.jobqueue import JobsError
from kiber_analyst.media_resume import resumed_photos

ROOT = data_path()
CONFIG = config_path("sources.json")
SOCKET = runtime_path("broker.sock")
CONTROLS = ('messages.jsonl', 'services.jsonl', 'manifest.json')
TERMINAL = {'done', 'failed', 'timed_out', 'cancelled'}
ACTIVE = {'queued', 'running', 'waiting_retry', 'waiting_quota', 'submitting'}
COMMANDS = {'ingest', 'embed', 'retry', 'job-status', 'job-list', 'job-logs',
            'queue-health', 'provider-health', 'source-status', 'media-status',
            'cancel', 'reindex', 'audit-log', 'renew-embed', 'search', 'asr',
            'asr-status', 'embed-asr'}
TRANSIENT = {'transient_database_error', 'server_error', 'network_error',
             'request_timeout', 'network_timeout'}
COUNTS = {'inserted', 'updated', 'skipped', 'absent', 'media_missing',
          'service_events', 'service_events_inserted', 'service_events_updated',
          'service_events_skipped', 'errors', 'segments_total', 'skipped_existing',
          'skipped_empty', 'completed', 'total', 'completed_batches', 'result_count',
          'files_total', 'files_done', 'files_failed', 'files_skipped', 'segments', 'empty'}
RETRYABLE_ERRORS = TRANSIENT | {'rate_limited'}
PUBLIC_ERROR_CATEGORIES = RETRYABLE_ERRORS | {
    'rate_limited', 'profile_auth', 'profile_configuration', 'policy_blocked',
    'request_rejected', 'provider_error', 'invalid_response', 'worker_restart',
}
MAX_ATTEMPTS = 3
RESERVE = 5 * 1024**3
SEARCH_MAX_LIMIT = 100
SEARCH_MAX_QUERY_CHARS = 150
SEARCH_WAIT_SECONDS = 55
# Bump when search behavior or its public result shape changes. Old requests
# remain in the audit database, but their fingerprints will no longer match.
SEARCH_CACHE_VERSION = f'{KIBER_ANALYST_VERSION}:search-2'
ASR_AUDIO_TYPES = {'audio', 'audio_file', 'voice', 'voice_message'}
ASR_VOICE_TYPES = {'voice', 'voice_message'}
ASR_AUDIO_EXTENSIONS = {'.flac', '.m4a', '.mp3', '.mp4', '.mpeg', '.mpga', '.ogg', '.wav', '.webm'}
ASR_MAX_FILE_BYTES = 25_000_000
ASR_MAX_MEDIA_BYTES = 100 * 1024**2


class Refused(Exception):
    pass


def _validated_asr_plan(plan, source_id, input_fingerprint):
    match = re.fullmatch(r'telegram:(-[1-9][0-9]{0,18}):topic:([1-9][0-9]{0,9})', source_id)
    if (match is None or not isinstance(plan, dict) or plan.get('version') != 2
            or plan.get('source_id') != source_id
            or plan.get('input_fingerprint') != input_fingerprint
            or plan.get('run_id') != 'ka-' + input_fingerprint[:28]
            or not re.fullmatch(r'[0-9a-f]{64}', str(plan.get('normalized_sha256', '')))):
        raise Refused('ASR_WORKSPACE_CONFLICT')
    files = plan.get('files')
    skipped = plan.get('skipped_by_reason')
    skipped_valid = isinstance(skipped, dict) and all(
        key in {'missing_media', 'over_100_MiB', 'provider_file_limit', 'unsupported_format'}
        and type(value) is int and value >= 0
        for key, value in skipped.items()
    )
    if (not isinstance(files, list) or not isinstance(skipped, dict)
            or type(plan.get('files_total')) is not int
            or type(plan.get('files_eligible')) is not int
            or type(plan.get('files_skipped')) is not int
            or plan['files_total'] < len(files)
            or plan['files_eligible'] != len(files)
            or plan['files_eligible'] + plan['files_skipped'] != plan['files_total']
            or not skipped_valid
            or plan['files_skipped'] != sum(skipped.values())):
        raise Refused('ASR_WORKSPACE_CONFLICT')
    chat, _topic = match.groups()
    refs = set()
    for item in files:
        if not isinstance(item, dict):
            raise Refused('ASR_WORKSPACE_CONFLICT')
        ref = item.get('external_ref')
        message_id = item.get('message_id')
        media_type = item.get('media_type')
        media_path = item.get('media_path')
        size = item.get('size_bytes')
        digest = item.get('sha256')
        try:
            media_parts = parts(media_path)
        except Refused:
            raise Refused('ASR_WORKSPACE_CONFLICT') from None
        if (not isinstance(ref, str) or not re.fullmatch(rf'tg:{re.escape(chat)}:[1-9][0-9]{{0,18}}', ref)
                or ref in refs or not isinstance(message_id, str)
                or not re.fullmatch(r'[1-9][0-9]{0,18}', message_id)
                or ref != f'tg:{chat}:{message_id}'
                or media_type not in ASR_AUDIO_TYPES
                or len(media_parts) != 3 or media_parts[0] not in {'media', 'media-resume'}
                or media_parts[1] != message_id
                or type(size) is not int or not 0 < size <= ASR_MAX_FILE_BYTES
                or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)):
            raise Refused('ASR_WORKSPACE_CONFLICT')
        refs.add(ref)
    return plan


def identifier(value):
    try:
        if str(uuid.UUID(value)) == value:
            return value
    except (ValueError, TypeError, AttributeError):
        pass
    raise Refused('INVALID_REQUEST_ID')


def timestamp():
    return datetime.datetime.now(datetime.UTC).isoformat()


def search_fingerprint(source_id, project, query, limit):
    payload = {
        'cache_version': SEARCH_CACHE_VERSION,
        'source_id': source_id,
        'project': project,
        'query': query,
        'limit': limit,
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(encoded.encode()).hexdigest()


def private(info, directory=False, *, owner=None):
    mode = 0o700 if directory else 0o600
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (not valid_type or info.st_uid != (os.getuid() if owner is None else owner)
            or stat.S_IMODE(info.st_mode) != mode
            or (not directory and info.st_nlink != 1)):
        raise Refused('UNSAFE_INPUT')


def parts(value):
    if not isinstance(value, str):
        raise Refused('UNSAFE_PATH')
    result = value.split('/')
    if any(not p or p in ('.', '..') or '\x00' in p or '\\' in p for p in result):
        raise Refused('UNSAFE_PATH')
    return result


@contextlib.contextmanager
def descend(fd, components, create=False, *, owner=None):
    with contextlib.ExitStack() as stack:
        for name in components:
            if create:
                try:
                    os.mkdir(name, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            stack.callback(os.close, fd)
            private(os.fstat(fd), directory=True, owner=owner)
        yield fd


@contextlib.contextmanager
def open_input(fd, name, *, owner=None):
    handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    with os.fdopen(handle, 'rb') as stream:
        private(os.fstat(stream.fileno()), owner=owner)
        yield stream


def read_input(fd, name, maximum=32 * 1024**2, *, owner=None):
    with open_input(fd, name, owner=owner) as stream:
        if os.fstat(stream.fileno()).st_size > maximum:
            raise Refused('INPUT_TOO_LARGE')
        result = stream.read(maximum + 1)
        if len(result) > maximum:
            raise Refused('INPUT_TOO_LARGE')
        return result


def registry(path):
    info = path.lstat()
    if (info.st_uid != 0 or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022
            or info.st_nlink != 1):
        raise Refused('UNSAFE_REGISTRY')
    values = json.loads(path.read_text())
    for source, entry in values.items():
        if parse_source_id(source) is not None:
            # External sources carry no staged files, so they register with a
            # project only; no control hashes exist for them.
            if set(entry) != {'project'} or not re.fullmatch(
                    r'[a-z0-9][a-z0-9-]{0,62}', entry['project']):
                raise Refused('INVALID_REGISTRY')
            continue
        if not re.fullmatch(r'telegram:-[1-9][0-9]{0,18}:topic:[1-9][0-9]{0,9}', source):
            raise Refused('INVALID_REGISTRY')
        if (set(entry) not in ({'project', 'hashes'}, {'project', 'hashes', 'asr_enabled'})
                or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', entry['project'])
                or ('asr_enabled' in entry and type(entry['asr_enabled']) is not bool)):
            raise Refused('INVALID_REGISTRY')
        if set(entry['hashes']) != set(CONTROLS) or not all(
            re.fullmatch(r'[a-f0-9]{64}', value) for value in entry['hashes'].values()
        ):
            raise Refused('INVALID_REGISTRY')
    return values


def write_file(fd, name, data):
    out = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
    with os.fdopen(out, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def copy_media(source, output, items, *, owner=None):
    for rel, size, digest in items:
        with descend(source, rel[:-1], owner=owner) as old, descend(output, rel[:-1], create=True) as new:
            with open_input(old, rel[-1], owner=owner) as src:
                if os.fstat(src.fileno()).st_size != size:
                    raise Refused('DATA_MISMATCH')
                dest = os.open(rel[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                               0o600, dir_fd=new)
                actual = hashlib.sha256()
                left = size
                with os.fdopen(dest, 'wb') as dst:
                    while left:
                        data = src.read(min(1024**2, left))
                        if not data:
                            raise Refused('DATA_MISMATCH')
                        dst.write(data)
                        actual.update(data)
                        left -= len(data)
                    if src.read(1) or actual.hexdigest() != digest:
                        raise Refused('DATA_MISMATCH')
                    dst.flush()
                    os.fsync(dst.fileno())
            os.fsync(new)


def media_items(data):
    result = []
    seen = set()
    for line in data.splitlines():
        row = json.loads(line)
        media = row.get('media') or {}
        value = media.get('path')
        if value is None:
            continue
        rel = parts(value)
        size = media.get('size')
        digest = media.get('sha256')
        if (len(rel) != 3 or rel[:2] != ['media', str(row['message_id'])]
                or value in seen or type(size) is not int or not 0 < size <= 100 * 1024**2
                or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)):
            raise Refused('UNSAFE_MEDIA')
        result.append((rel, size, digest))
        seen.add(value)
    return result


def publish_resume(source, output, checkpoint, records, *, owner=None, publish_owner=None):
    """Publish only recovered files. Inputs and existing results stay immutable."""
    files = [(parts(r['path'])[1:], r['size'], r['sha256']) for r in records.values()]
    existing_owner = publish_owner[0] if publish_owner else None
    try:
        with descend(output, ['media-resume'], owner=existing_owner) as existing:
            if read_input(existing, 'status.json', owner=existing_owner) != checkpoint:
                raise Refused('RESUME_REVISION_CONFLICT')
            for rel, size, digest in files:
                with descend(existing, rel[:-1], owner=existing_owner) as parent:
                    with open_input(parent, rel[-1], owner=existing_owner) as stream:
                        if (os.fstat(stream.fileno()).st_size != size
                                or hashlib.file_digest(stream, 'sha256').hexdigest() != digest):
                            raise Refused('DATA_MISMATCH')
            return
    except FileNotFoundError:
        if 'media-resume' in os.listdir(output):
            raise Refused('RESUME_REVISION_CONFLICT') from None
    free = os.fstatvfs(output)
    if free.f_bavail * free.f_frsize < RESERVE + sum(size for _, size, _ in files):
        raise Refused('INSUFFICIENT_SPACE')
    temp = '.resume-' + uuid.uuid4().hex
    try:
        with descend(output, [temp], create=True) as target:
            copy_media(source, target, files, owner=owner)
            write_file(target, 'status.json', checkpoint)
            if publish_owner:
                for _, dirs, names, fd in os.fwalk('.', topdown=False, dir_fd=target):
                    for name in dirs + names:
                        os.chown(name, *publish_owner, dir_fd=fd, follow_symlinks=False)
                os.fchown(target, *publish_owner)
            os.fsync(target)
        os.rename(temp, 'media-resume', src_dir_fd=output, dst_dir_fd=output)
        os.fsync(output)
    finally:
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(temp, dir_fd=output)


def media_revision(root, chat, topic):
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with descend(fd, ['imports', chat, topic]) as source:
            try:
                with descend(source, ['media-resume']) as resume:
                    checkpoint = read_input(resume, 'status.json', 4 * 1024**2)
            except FileNotFoundError:
                if 'media-resume' in os.listdir(source):
                    raise Refused('RESUME_REVISION_CONFLICT') from None
                return ''
            return hashlib.sha256(checkpoint).hexdigest()
    finally:
        os.close(fd)


class Broker:
    def __init__(self, root, sources, worker_api=api_call, submit=submit_command):
        self.root = root
        self.sources = sources
        self.worker_api = worker_api
        self.submit = submit
        self.db = sqlite3.connect(root / 'broker' / 'requests.sqlite3')
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, owner TEXT NOT NULL, source TEXT NOT NULL,
                fingerprint TEXT NOT NULL, operation TEXT NOT NULL,
                argv TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 1,
                created TEXT NOT NULL, updated TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'submitting', counts TEXT NOT NULL DEFAULT '{}',
                error_code TEXT, UNIQUE(owner,source,fingerprint,operation)
            );
            CREATE TABLE IF NOT EXISTS embed_renewals (
                previous_request_id TEXT PRIMARY KEY REFERENCES requests(id),
                request_id TEXT NOT NULL UNIQUE REFERENCES requests(id),
                previous_job_id TEXT NOT NULL,
                base_fingerprint TEXT NOT NULL,
                reason TEXT NOT NULL,
                completed_batches INTEGER NOT NULL,
                created TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, owner TEXT NOT NULL,
                command TEXT NOT NULL, source TEXT, request_id TEXT, job_id TEXT, result TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS broker_audit_owner_idx ON audit_log(owner,seq);
            CREATE TABLE IF NOT EXISTS attempts (
                request_id TEXT NOT NULL REFERENCES requests(id), attempt INTEGER NOT NULL,
                job_id TEXT NOT NULL UNIQUE, bound TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued', counts TEXT NOT NULL DEFAULT '{}',
                error_code TEXT, checked TEXT,
                PRIMARY KEY(request_id,attempt)
            );
            CREATE TABLE IF NOT EXISTS asr_request_meta (
                request_id TEXT PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE,
                input_fingerprint TEXT NOT NULL,
                run_id TEXT NOT NULL,
                phase TEXT NOT NULL,
                selected_refs_json TEXT NOT NULL
            );
        """)

    def source(self, source):
        if source not in self.sources:
            raise Refused('SOURCE_NOT_ALLOWED')
        entry = self.sources[source]
        fingerprint = hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()
        if parse_source_id(source) is not None:
            # External sources have no staged import tree.
            return entry, fingerprint, None, None
        _, chat, _, topic = source.split(':')
        return entry, fingerprint, chat, topic

    def stage(self, source_id, entry, fingerprint, chat, topic, revision=""):
        with contextlib.ExitStack() as stack:
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, fd)
            private(os.fstat(fd), directory=True)
            source = stack.enter_context(descend(fd, ['imports', chat, topic]))
            data = {name: read_input(source, name) for name in CONTROLS}
            if any(hashlib.sha256(data[n]).hexdigest() != entry['hashes'][n] for n in CONTROLS):
                raise Refused('HASH_MISMATCH')
            services = read_input(source, 'services_manifest.json', 1024**2)
            service_meta = json.loads(services)
            if (service_meta.get('source_id') != source_id
                    or service_meta.get('services_sha256') != entry['hashes']['services.jsonl']
                    or service_meta.get('complete') is not True):
                raise Refused('DATA_MISMATCH')
            if json.loads(data['manifest.json']).get('source_id') != source_id:
                raise Refused('DATA_MISMATCH')
            data['services_manifest.json'] = services
            base = stack.enter_context(descend(fd, ['imports', '.broker']))
            def merge(final):
                if not revision:
                    return
                with descend(source, ['media-resume']) as resumed:
                    checkpoint = read_input(resumed, 'status.json', 4 * 1024**2)
                    if hashlib.sha256(checkpoint).hexdigest() != revision:
                        raise Refused('RESUME_REVISION_CONFLICT')
                    records = resumed_photos(checkpoint, entry['hashes'],
                                            map(json.loads, data['messages.jsonl'].splitlines()))
                    publish_resume(resumed, final, checkpoint, records)

            destination = self.root / 'imports' / '.broker' / fingerprint / chat / topic
            try:
                with descend(base, [fingerprint, chat, topic]) as final:
                    if any(read_input(final, n) != v for n, v in data.items()):
                        raise Refused('HASH_MISMATCH')
                    merge(final)
                    return destination
            except FileNotFoundError:
                # A partially missing existing final directory is never replaced.
                if fingerprint in os.listdir(base):
                    raise Refused('DATA_MISMATCH') from None
            files = media_items(data['messages.jsonl'])
            free = os.fstatvfs(base)
            required = sum(size for _, size, _ in files) + sum(map(len, data.values()))
            if free.f_bavail * free.f_frsize < required + RESERVE:
                raise Refused('INSUFFICIENT_SPACE')
            # Named children remain under the verified broker-owned directory.
            with tempfile.TemporaryDirectory(prefix='.stage-', dir=destination.parents[2]) as temporary:
                name = Path(temporary).name
                with descend(base, [name, chat, topic], create=True) as final:
                    for key, value in data.items():
                        write_file(final, key, value)
                    copy_media(source, final, files)
                    merge(final)
                    os.fsync(final)
                with descend(base, [name, chat]) as parent:
                    os.fsync(parent)
                with descend(base, [name]) as parent:
                    os.fsync(parent)
                os.rename(name, fingerprint, src_dir_fd=base, dst_dir_fd=base)
                os.fsync(base)
            return destination

    def asr_workspace(self, source_id, entry, staged, fingerprint, revision):
        """Build or reuse a private normalized ASR input from the staged export."""
        from kiber_analyst.telegram_ingest import prepare_export

        input_fingerprint = hashlib.sha256(json.dumps(
            {"source": fingerprint, "media_resume": revision},
            sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        workspace_name = 'asr-input-' + input_fingerprint[:20]
        chat = source_id.split(':')[1]
        with contextlib.ExitStack() as stack:
            root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, root_fd)
            relative = staged.relative_to(self.root).parts
            source_fd = stack.enter_context(descend(root_fd, list(relative)))
            try:
                workspace_fd = stack.enter_context(descend(source_fd, [workspace_name]))
                plan_data = read_input(workspace_fd, 'asr-plan.json', 8 * 1024**2)
                plan = _validated_asr_plan(json.loads(plan_data), source_id, input_fingerprint)
                with descend(workspace_fd, ['normalized']) as normalized_fd:
                    normalized = read_input(normalized_fd, 'messages.jsonl')
                    if hashlib.sha256(normalized).hexdigest() != plan['normalized_sha256']:
                        raise Refused('ASR_WORKSPACE_CONFLICT')
                with descend(workspace_fd, ['unpacked']) as unpacked_fd:
                    for item in plan['files']:
                        media_parts = parts(item['media_path'])
                        with descend(unpacked_fd, media_parts[:-1]) as parent:
                            with open_input(parent, media_parts[-1]) as stream:
                                if (os.fstat(stream.fileno()).st_size != item['size_bytes']
                                        or hashlib.file_digest(stream, 'sha256').hexdigest() != item['sha256']):
                                    raise Refused('ASR_WORKSPACE_CONFLICT')
                return plan, staged / workspace_name
            except FileNotFoundError:
                if workspace_name in os.listdir(source_fd):
                    raise Refused('ASR_WORKSPACE_CONFLICT') from None

            temporary = '.asr-' + uuid.uuid4().hex
            os.mkdir(temporary, 0o700, dir_fd=source_fd)
            try:
                with descend(source_fd, [temporary]) as temp_fd:
                    temp_path = staged / temporary
                    prepare_export(
                        staged,
                        temp_path,
                        root=self.root / 'imports',
                    )
                    normalized_path = temp_path / 'normalized' / 'messages.jsonl'
                    normalized_info = normalized_path.lstat()
                    if (not stat.S_ISREG(normalized_info.st_mode)
                            or normalized_info.st_nlink != 1
                            or stat.S_IMODE(normalized_info.st_mode) != 0o600):
                        raise Refused('ASR_INPUT_UNSAFE')
                    normalized_records = []
                    total_audio = 0
                    skipped = {}
                    copies = []
                    eligible = []
                    for line in normalized_path.read_text(encoding='utf-8').splitlines():
                        record = json.loads(line)
                        media_type = record.get('media_type')
                        if not isinstance(media_type, str) or media_type.lower() not in ASR_AUDIO_TYPES:
                            continue
                        total_audio += 1
                        message_id = str(record.get('message_id', ''))
                        media_path = record.get('media_path')
                        metadata = record.get('meta')
                        media_export = metadata.get('media_export', {}) if isinstance(metadata, dict) else {}
                        declared_size = media_export.get('size') if isinstance(media_export, dict) else None
                        if not isinstance(media_path, str) or not media_path:
                            reason = 'over_100_MiB' if type(declared_size) is int and declared_size > ASR_MAX_MEDIA_BYTES else 'missing_media'
                            skipped[reason] = skipped.get(reason, 0) + 1
                            continue
                        relative_media = parts(media_path)
                        if (len(relative_media) != 3
                                or relative_media[0] not in {'media', 'media-resume'}
                                or relative_media[1] != message_id):
                            raise Refused('ASR_MEDIA_PATH_UNSAFE')
                        if PurePosixPath(media_path).suffix.lower() not in ASR_AUDIO_EXTENSIONS:
                            skipped['unsupported_format'] = skipped.get('unsupported_format', 0) + 1
                            continue
                        try:
                            with descend(source_fd, relative_media[:-1]) as parent:
                                with open_input(parent, relative_media[-1]) as stream:
                                    size = os.fstat(stream.fileno()).st_size
                                    if not 0 < size <= ASR_MAX_MEDIA_BYTES:
                                        reason = 'over_100_MiB' if size > ASR_MAX_MEDIA_BYTES else 'missing_media'
                                        skipped[reason] = skipped.get(reason, 0) + 1
                                        continue
                                    if size > ASR_MAX_FILE_BYTES:
                                        skipped['provider_file_limit'] = skipped.get('provider_file_limit', 0) + 1
                                        continue
                                    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                        except FileNotFoundError:
                            skipped['missing_media'] = skipped.get('missing_media', 0) + 1
                            continue
                        if type(declared_size) is int and declared_size != size:
                            raise Refused('ASR_MEDIA_SIZE_MISMATCH')
                        declared_hash = media_export.get('sha256') if isinstance(media_export, dict) else None
                        if isinstance(declared_hash, str) and declared_hash != digest:
                            raise Refused('ASR_MEDIA_HASH_MISMATCH')
                        external_ref = f'tg:{chat}:{message_id}'
                        copies.append((relative_media, size, digest))
                        eligible.append({
                            'external_ref': external_ref,
                            'message_id': message_id,
                            'media_type': media_type.lower(),
                            'media_path': media_path,
                            'size_bytes': size,
                            'sha256': digest,
                        })
                        normalized_records.append(record)

                    free = os.fstatvfs(source_fd)
                    required = sum(size for _path, size, _digest_value in copies)
                    if free.f_bavail * free.f_frsize < required + RESERVE:
                        raise Refused('INSUFFICIENT_SPACE')

                    normalized_fd = stack.enter_context(descend(temp_fd, ['normalized']))
                    filtered = ''.join(
                        json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n'
                        for record in normalized_records
                    ).encode('utf-8')
                    write_file(normalized_fd, '.asr-messages.jsonl', filtered)
                    os.rename('.asr-messages.jsonl', 'messages.jsonl',
                              src_dir_fd=normalized_fd, dst_dir_fd=normalized_fd)
                    unpacked_fd = stack.enter_context(descend(temp_fd, ['unpacked'], create=True))
                    copy_media(source_fd, unpacked_fd, copies)
                    run_id = 'ka-' + input_fingerprint[:28]
                    plan = {
                        'version': 2,
                        'source_id': source_id,
                        'input_fingerprint': input_fingerprint,
                        'run_id': run_id,
                        'normalized_sha256': hashlib.sha256(filtered).hexdigest(),
                        'files_total': total_audio,
                        'files_eligible': len(eligible),
                        'files_skipped': sum(skipped.values()),
                        'skipped_by_reason': skipped,
                        'files': eligible,
                    }
                    plan = _validated_asr_plan(plan, source_id, input_fingerprint)
                    write_file(
                        temp_fd, 'asr-plan.json',
                        (json.dumps(plan, sort_keys=True, separators=(',', ':')) + '\n').encode(),
                    )
                    os.fsync(temp_fd)
                os.rename(temporary, workspace_name, src_dir_fd=source_fd, dst_dir_fd=source_fd)
                os.fsync(source_fd)
            except BaseException:
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(temporary, dir_fd=source_fd)
                raise
        return plan, staged / workspace_name

    def asr_overview(self, source_id):
        entry, _fingerprint, _chat, _topic = self.source(source_id)
        try:
            result = self.worker_api(
                'asr-source-overview', source_id=source_id, project=entry['project']
            )
        except JobsError:
            raise Refused('ASR_STATUS_UNAVAILABLE') from None
        if not isinstance(result, dict) or result.get('documents') != 1:
            raise Refused('INGEST_REQUIRED')
        return result

    def source_asr_status(self, owner, source_id):
        overview = self.asr_overview(source_id)
        return {
            key: overview.get(key)
            for key in ('files_total', 'files_done', 'files_failed', 'files_skipped',
                        'segments', 'empty', 'last_error_code')
        }

    def source_asr(self, owner, source_id):
        entry, fingerprint, chat, topic = self.source(source_id)
        if entry.get('asr_enabled') is not True:
            raise Refused('ASR_NOT_ENABLED')
        revision = media_revision(self.root, chat, topic)
        staged = self.stage(source_id, entry, fingerprint, chat, topic, revision)
        plan, workspace = self.asr_workspace(
            source_id, entry, staged, fingerprint, revision
        )
        overview = self.asr_overview(source_id)
        completed = set(overview.get('completed_refs', []))
        failed = set(overview.get('failed_refs', []))
        voice_refs = [item['external_ref'] for item in plan['files']
                      if item['media_type'] in ASR_VOICE_TYPES]
        unresolved_voice = [ref for ref in voice_refs if ref not in completed | failed]
        if unresolved_voice:
            phase = 'asr_pilot'
            selected = unresolved_voice
        else:
            phase = 'asr_full'
            selected = [item['external_ref'] for item in plan['files']
                        if item['external_ref'] not in completed | failed]
        if not selected:
            return {
                'status': 'partial' if overview['files_failed'] else 'complete',
                'source_id': source_id,
                'files_total': plan['files_total'],
                'files_done': overview['files_done'],
                'files_failed': overview['files_failed'],
                'files_skipped': plan['files_skipped'],
                'skip_reasons': plan['skipped_by_reason'],
                'segments': overview['segments'], 'empty': overview['empty'],
            }

        def submit_result(row):
            return {
                **self.dispatch(row), 'source_id': source_id, 'phase': phase,
                'files_total': plan['files_total'], 'files_selected': len(selected),
                'files_skipped': plan['files_skipped'],
                'skip_reasons': plan['skipped_by_reason'],
            }

        for row in self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND operation IN ('asr_pilot','asr_full')",
            (owner, source_id),
        ).fetchall():
            if self.status(row)['status'] not in TERMINAL:
                if row['operation'] == phase:
                    meta = self.db.execute(
                        'SELECT selected_refs_json FROM asr_request_meta WHERE request_id=?',
                        (row['id'],),
                    ).fetchone()
                    if meta and json.loads(meta[0]) == selected:
                        return submit_result(row)
                raise Refused('SOURCE_BUSY')

        payload = {
            'source_id': source_id,
            'input_fingerprint': plan['input_fingerprint'],
            'phase': phase,
            'selected_refs': selected,
        }
        request_fingerprint = hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(',', ':')
        ).encode()).hexdigest()
        row = self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation=?',
            (owner, source_id, request_fingerprint, phase),
        ).fetchone()
        if row:
            return submit_result(row)
        argv = ['telegram', 'asr', '--job', str(workspace), '--project', entry['project'],
                '--load-results', '--run-id', plan['run_id']]
        for ref in selected:
            argv.extend(['--external-ref', ref])
        now = timestamp()
        request_id = str(uuid.uuid4())
        with self.db:
            self.db.execute(
                'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                (request_id, owner, source_id, request_fingerprint, phase,
                 json.dumps(argv), now, now),
            )
            self.db.execute(
                'INSERT INTO asr_request_meta(request_id,input_fingerprint,run_id,phase,selected_refs_json) VALUES(?,?,?,?,?)',
                (request_id, plan['input_fingerprint'], plan['run_id'], phase,
                 json.dumps(selected, separators=(',', ':'))),
            )
        return submit_result(self.row(owner, request_id))

    def source_embed_asr(self, owner, source_id):
        entry, fingerprint, _chat, _topic = self.source(source_id)
        if entry.get('asr_enabled') is not True:
            raise Refused('ASR_NOT_ENABLED')
        overview = self.asr_overview(source_id)
        for row in self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND operation IN ('asr_pilot','asr_full')",
            (owner, source_id),
        ).fetchall():
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('ASR_ACTIVE')
        if (overview.get('files_done', 0) < 1 or overview.get('files_pending', 0) != 0
                or overview.get('files_failed', 0) != 0):
            raise Refused('ASR_INCOMPLETE')
        request_fingerprint = hashlib.sha256(json.dumps(
            {'source_id': source_id, 'source_fingerprint': fingerprint,
             'asr_revision': overview.get('revision'), 'project': entry['project']},
            sort_keys=True, separators=(',', ':'),
        ).encode()).hexdigest()
        row = self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation='asr_embed'",
            (owner, source_id, request_fingerprint),
        ).fetchone()
        if row:
            return self.dispatch(row)
        argv = ['asr', 'embed', '--all-runs', '--project', entry['project'],
                '--source-id', source_id]
        now = timestamp()
        request_id = str(uuid.uuid4())
        with self.db:
            self.db.execute(
                'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                (request_id, owner, source_id, request_fingerprint, 'asr_embed',
                 json.dumps(argv), now, now),
            )
        return self.dispatch(self.row(owner, request_id))

    def row(self, owner, request_id):
        row = self.db.execute('SELECT * FROM requests WHERE id=? AND owner=?',
                              (request_id, owner)).fetchone()
        if row is None:
            raise Refused('REQUEST_NOT_FOUND')
        return row

    def dispatch(self, row):
        attempt = self.db.execute('SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                                  (row['id'], row['attempt'])).fetchone()
        if attempt:
            job_id = attempt['job_id']
        else:
            key = f"broker:{row['id']}:{row['attempt']}"
            job_id = identifier(self.submit(json.loads(row['argv']), idempotency_key=key)['job_id'])
            with self.db:
                self.db.execute('INSERT INTO attempts(request_id,attempt,job_id,bound) VALUES(?,?,?,?)',
                                (row['id'], row['attempt'], job_id, timestamp()))
                self.db.execute('UPDATE requests SET state=?,updated=? WHERE id=?',
                                ('queued', timestamp(), row['id']))
        return {'request_id': row['id'], 'job_id': job_id, 'attempt': row['attempt']}

    def status(self, row):
        inherited = self.db.execute(
            'SELECT completed_batches FROM embed_renewals WHERE request_id=?', (row['id'],),
        ).fetchone()
        inherited_batches = inherited[0] if inherited else 0
        attempt = self.db.execute('SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                                  (row['id'], row['attempt'])).fetchone()
        if attempt is None:
            return {'status': 'submitting', 'counts': {}, 'error_code': None,
                    'attempt': row['attempt'], 'last_error_code': None,
                    'next_retry_at': None, 'profile_scope': None,
                    'retry_reason': None, 'retry_after_seconds': None,
                    'retryable': False, 'attempts_remaining': MAX_ATTEMPTS - row['attempt'],
                    'completed_batches': inherited_batches,
                    'created_at': row['created'], 'updated_at': row['updated']}
        jid = attempt['job_id']
        status = self.worker_api('status', job_id=jid)
        state = status['state']
        payload = self.worker_api('result', job_id=jid) if state in TERMINAL else status.get('progress', {})
        combined = {**status.get('progress', {}), **payload}
        counts = {k: combined[k] for k in COUNTS if type(combined.get(k)) is int and combined[k] >= 0}
        history = self.db.execute(
            'SELECT counts FROM attempts WHERE request_id=? AND attempt<?', (row['id'], row['attempt']),
        ).fetchall()
        completed_batches = inherited_batches + counts.get('completed_batches', 0) + sum(
            json.loads(previous['counts']).get('completed_batches', 0) for previous in history
        )
        category = (payload.get('error') or {}).get('category')
        if category == 'retry_exhausted':
            failures = [entry.get('category') for entry in
                        (payload.get('attempts') or {}).get('items', [])
                        if entry.get('status') != 'succeeded']
            if failures and all(c in RETRYABLE_ERRORS for c in failures):
                category = failures[-1]
        code = category if category in RETRYABLE_ERRORS else (
            'JOB_FAILED' if state == 'failed' else 'JOB_TIMED_OUT' if state == 'timed_out' else None
        )
        now = timestamp()
        encoded = json.dumps(counts, sort_keys=True)
        changed = (state, json.loads(row['counts']), row['error_code']) != (row['state'], counts, code)
        if changed:
            with self.db:
                self.db.execute('UPDATE requests SET state=?, counts=?, error_code=?, updated=? WHERE id=?',
                                (state, encoded, code, now, row['id']))
                self.db.execute('UPDATE attempts SET state=?,counts=?,error_code=?,checked=? WHERE request_id=? AND attempt=?',
                                (state, encoded, code, now, row['id'], row['attempt']))
        provider_error = status.get('last_provider_error') or {}
        http_code = provider_error.get('http_code')
        provider_category = provider_error.get('category')
        last_error_code = code
        progress_error = combined.get('last_error_code')
        if isinstance(progress_error, str) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,39}', progress_error):
            last_error_code = progress_error
        if type(http_code) is int and 400 <= http_code <= 599:
            last_error_code = f'HTTP_{http_code}'
        elif provider_category in PUBLIC_ERROR_CATEGORIES:
            last_error_code = provider_category
        scopes = [scope for scope in provider_error.get('scopes', [])
                  if isinstance(scope, str) and re.fullmatch(r'[a-z0-9_-]+:[a-z0-9:_-]{1,100}', scope)]
        reason = None
        if state in {'failed', 'waiting_retry', 'waiting_quota'}:
            if http_code == 429 or state == 'waiting_quota' or code == 'rate_limited':
                reason = 'quota'
            elif provider_category in {'network_error', 'network_timeout', 'request_timeout'}:
                reason = 'network'
            elif http_code or provider_category in PUBLIC_ERROR_CATEGORIES:
                reason = 'provider'
        retry_after = provider_error.get('retry_after_seconds') if reason else None
        return {'status': state, 'counts': counts, 'error_code': code,
                'attempt': row['attempt'], 'last_error_code': last_error_code,
                'retryable': state == 'failed' and code in RETRYABLE_ERRORS and row['attempt'] < MAX_ATTEMPTS,
                'attempts_remaining': MAX_ATTEMPTS - row['attempt'],
                'completed_batches': completed_batches,
                'next_retry_at': status.get('next_run_at') if state in {'waiting_retry', 'waiting_quota'} else None,
                'profile_scope': scopes[0] if scopes else None,
                'retry_reason': reason,
                'retry_after_seconds': retry_after if type(retry_after) in (int, float) and 0 <= retry_after <= 99_999_999 else None,
                'created_at': row['created'], 'updated_at': status.get('updated_at', now if changed else row['updated'])}

    def start(self, owner, operation, source_id):
        entry, fingerprint, chat, topic = self.source(source_id)
        if chat is None:
            return self.start_external(owner, operation, source_id, entry, fingerprint)
        base_fingerprint = fingerprint
        revision = media_revision(self.root, chat, topic) if operation == 'ingest' else ''
        if revision:
            fingerprint = hashlib.sha256((fingerprint + revision).encode()).hexdigest()
        relevant = self.db.execute(
            """SELECT r.*, coalesce(e.base_fingerprint,r.fingerprint) AS source_fingerprint
               FROM requests r LEFT JOIN embed_renewals e ON e.request_id=r.id
               WHERE owner=? AND source=? AND operation=? ORDER BY r.created DESC""",
            (owner, source_id, operation),
        ).fetchall()
        for row in relevant:
            if row['source_fingerprint'] == fingerprint:
                if operation == 'ingest':
                    self.stage(source_id, entry, base_fingerprint, chat, topic, revision)
                elif self.status(row)['retryable']:
                    return self.handle(owner, 'retry', row['id'])
                return self.dispatch(row)
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        if operation == 'ingest':
            staged = self.stage(source_id, entry, base_fingerprint, chat, topic, revision)
            argv = ['telegram', 'ingest', '--job', str(staged), '--project', entry['project']]
        else:
            resume_hash = media_revision(self.root, chat, topic)
            ingest_key = hashlib.sha256((fingerprint + resume_hash).encode()).hexdigest() if resume_hash else fingerprint
            ingest = self.db.execute('SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation=?',
                                     (owner, source_id, ingest_key, 'ingest')).fetchone()
            if ingest is None or self.status(ingest)['status'] != 'done':
                raise Refused('INGEST_REQUIRED')
            job_id = self.dispatch(ingest)['job_id']
            argv = ['embed', '--job', str(self.root / 'jobs' / job_id / 'telegram-ingest'),
                    '--project', entry['project']]
        now = timestamp()
        request_id = str(uuid.uuid4())
        with self.db:
            self.db.execute('INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                            (request_id, owner, source_id, fingerprint, operation, json.dumps(argv), now, now))
        return self.dispatch(self.row(owner, request_id))

    def start_external(self, owner, operation, source_id, entry, fingerprint):
        # External sources: no staged files, the connector fetches from the API.
        provider, external_id = source_id.split(':', 1)
        relevant = self.db.execute(
            """SELECT r.*, coalesce(e.base_fingerprint,r.fingerprint) AS source_fingerprint
               FROM requests r LEFT JOIN embed_renewals e ON e.request_id=r.id
               WHERE owner=? AND source=? AND operation=? ORDER BY r.created DESC""",
            (owner, source_id, operation),
        ).fetchall()
        for row in relevant:
            if row['source_fingerprint'] == fingerprint:
                if operation == 'embed' and self.status(row)['retryable']:
                    return self.handle(owner, 'retry', row['id'])
                return self.dispatch(row)
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        if operation == 'ingest':
            if provider == 'gitlab':
                host, project_path = external_id.split('/', 1)
                argv = ['gitlab', 'ingest', '--host', host, '--project', project_path,
                        '--project-slug', entry['project']]
            else:
                argv = [provider, 'ingest', '--repo', external_id, '--project', entry['project']]
        elif operation == 'embed':
            ingest = self.db.execute(
                'SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation=?',
                (owner, source_id, fingerprint, 'ingest'),
            ).fetchone()
            if ingest is None or self.status(ingest)['status'] != 'done':
                raise Refused('INGEST_REQUIRED')
            argv = ['external', 'embed', '--source', source_id, '--project', entry['project']]
        else:
            raise Refused('COMMAND_NOT_ALLOWED')
        now = timestamp()
        request_id = str(uuid.uuid4())
        with self.db:
            self.db.execute(
                'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                (request_id, owner, source_id, fingerprint, operation, json.dumps(argv), now, now),
            )
        return self.dispatch(self.row(owner, request_id))

    def external_source_status(self, owner, source_id, entry, command):
        if command != 'source-status':
            raise Refused('COMMAND_NOT_ALLOWED')
        db = self.worker_api('source-overview', source_id=source_id, project=entry['project'])
        rows = self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND source=? ORDER BY created DESC',
            (owner, source_id),
        ).fetchall()
        by_operation = {}
        for row in rows:
            if row['operation'] not in by_operation:
                by_operation[row['operation']] = {
                    'request_id': row['id'], 'status': self.status(row)['status'],
                }
        return {'source_id': source_id, 'provider': source_id.split(':', 1)[0],
                'counts': db, 'ingest': by_operation.get('ingest'),
                'embed': by_operation.get('embed')}

    def source_search(self, owner, source_id, query, limit_text=None):
        entry, _source_fingerprint, _chat, _topic = self.source(source_id)
        if not isinstance(query, str) or not query.strip() or len(query) > SEARCH_MAX_QUERY_CHARS:
            raise Refused('INVALID_QUERY')
        if limit_text is None:
            limit = 5
        elif (
            isinstance(limit_text, str)
            and re.fullmatch(r'(?:[1-9][0-9]?|100)', limit_text)
        ):
            limit = int(limit_text)
        else:
            raise Refused('INVALID_LIMIT')
        if limit > SEARCH_MAX_LIMIT:
            raise Refused('INVALID_LIMIT')
        try:
            overview = self.worker_api('source-overview', source_id=source_id, project=entry['project'])
        except JobsError:
            raise Refused('WORKER_UNAVAILABLE') from None
        external = parse_source_id(source_id) is not None
        if external:
            if not overview.get('threads'):
                raise Refused('SOURCE_NOT_FOUND')
        elif overview.get('documents') != 1:
            raise Refused('SOURCE_NOT_FOUND')
        normalized = query.strip()
        fingerprint = search_fingerprint(source_id, entry['project'], normalized, limit)
        row = self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation='search'",
            (owner, source_id, fingerprint),
        ).fetchone()
        if row is None:
            argv = ['search', normalized, '--project', entry['project'], '--limit', str(limit),
                    '--source-kind', source_id.split(':', 1)[0] if external else 'telegram',
                    '--source-id', source_id, '--json']
            now = timestamp()
            request_id = str(uuid.uuid4())
            with self.db:
                self.db.execute(
                    'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                    (request_id, owner, source_id, fingerprint, 'search', json.dumps(argv), now, now),
                )
            row = self.row(owner, request_id)
        dispatched = self.dispatch(row)
        job_id = dispatched['job_id']
        deadline = time.monotonic() + SEARCH_WAIT_SECONDS
        while True:
            status = self.worker_api('status', job_id=job_id)
            state = status.get('state')
            if state == 'done':
                result = self.worker_api('result', job_id=job_id)
                if not isinstance(result, dict) or not isinstance(result.get('results'), list):
                    raise Refused('SEARCH_RESULT_INVALID')
                mode = result.get('mode')
                degraded = result.get('degraded')
                if mode not in {'hybrid', 'fts'} or type(degraded) is not bool:
                    raise Refused('SEARCH_RESULT_INVALID')
                rows = []
                for item in result['results'][:limit]:
                    if not isinstance(item, dict) or item.get('source_id') != source_id:
                        raise Refused('SEARCH_RESULT_INVALID')
                    text = item.get('text')
                    if not isinstance(text, str):
                        text = ''
                    row_data = {
                        key: item.get(key) for key in (
                            'source_id', 'chat_id', 'chat_title', 'topic_id', 'topic_title',
                            'message_id', 'author', 'score',
                        )
                    }
                    if external:
                        for key in ('thread_ref', 'item_ref', 'item_kind', 'url',
                                    'state', 'labels'):
                            row_data[key] = item.get(key)
                    row_data['date'] = item.get('posted_at')
                    row_data['text'] = text[:1200]
                    row_data['text_truncated'] = len(text) > 1200
                    row_data['mode'] = mode
                    row_data['degraded'] = degraded
                    rows.append(row_data)
                with self.db:
                    self.db.execute(
                        "UPDATE requests SET state='done',counts=?,updated=? WHERE id=?",
                        (json.dumps({'result_count': len(rows)}, sort_keys=True), timestamp(), row['id']),
                    )
                return {'request_id': row['id'], 'job_id': job_id, 'source_id': source_id,
                        'mode': mode, 'degraded': degraded,
                        'degraded_reason': result.get('degraded_reason'),
                        'result_count': len(rows), 'results': rows}
            if state in {'failed', 'timed_out', 'cancelled'}:
                with self.db:
                    self.db.execute(
                        'UPDATE requests SET state=?,error_code=?,updated=? WHERE id=?',
                        (state, 'SEARCH_FAILED', timestamp(), row['id']),
                    )
                return {'request_id': row['id'], 'job_id': job_id,
                        'source_id': source_id, 'status': state,
                        'error_code': 'SEARCH_FAILED'}
            if time.monotonic() >= deadline:
                return {'request_id': row['id'], 'job_id': job_id,
                        'source_id': source_id, 'status': state or 'running',
                        'error_code': 'SEARCH_PENDING'}
            time.sleep(0.5)

    def renew_embed(self, owner, request_id):
        previous = self.row(owner, request_id)
        if previous['operation'] != 'embed':
            raise Refused('RENEW_EMBED_NOT_ALLOWED')
        prior_renewal = self.db.execute(
            'SELECT * FROM embed_renewals WHERE previous_request_id=?', (request_id,),
        ).fetchone()
        if prior_renewal is not None:
            return self.renewal_result(owner, prior_renewal)
        status = self.status(previous)
        if (status['status'] != 'failed' or status['attempts_remaining'] != 0
                or status['error_code'] not in RETRYABLE_ERRORS):
            raise Refused('RENEW_EMBED_NOT_ALLOWED')
        _, fingerprint, _, _ = self.source(previous['source'])
        old_base = self.db.execute(
            'SELECT base_fingerprint FROM embed_renewals WHERE request_id=?', (request_id,),
        ).fetchone()
        if fingerprint != (old_base[0] if old_base else previous['fingerprint']):
            raise Refused('SOURCE_REVISION_CHANGED')
        for row in self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND operation='embed' AND id<>?",
            (owner, previous['source'], request_id),
        ).fetchall():
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        previous_job = self.db.execute(
            'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
            (request_id, previous['attempt']),
        ).fetchone()[0]
        new_id = str(uuid.uuid4())
        new_fingerprint = hashlib.sha256(f"{fingerprint}:renew:{request_id}".encode()).hexdigest()
        now = timestamp()
        with self.db:
            self.db.execute(
                'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                (new_id, owner, previous['source'], new_fingerprint, 'embed', previous['argv'], now, now),
            )
            self.db.execute(
                'INSERT INTO embed_renewals VALUES(?,?,?,?,?,?,?)',
                (request_id, new_id, previous_job, fingerprint, status['error_code'],
                 status['completed_batches'], now),
            )
        return self.renewal_result(owner, self.db.execute(
            'SELECT * FROM embed_renewals WHERE previous_request_id=?', (request_id,),
        ).fetchone())

    def renewal_result(self, owner, link):
        row = self.row(owner, link['request_id'])
        return {
            **self.dispatch(row),
            'source_id': row['source'],
            'previous_request_id': link['previous_request_id'],
            'previous_job_id': link['previous_job_id'],
            'renewal_reason': link['reason'],
            'completed_batches': link['completed_batches'],
        }

    def job_list(self, owner, filters):
        selection = {}
        for token in filters:
            key, sep, value = token.partition('=')
            if not sep or key not in {'source_id', 'status', 'owner'} or key in selection:
                raise Refused('INVALID_FILTER')
            selection[key] = value
        if selection.get('owner', owner) != owner:
            raise Refused('OWNER_NOT_ALLOWED')
        if 'source_id' in selection:
            self.source(selection['source_id'])
        if 'status' in selection and selection['status'] not in TERMINAL | ACTIVE:
            raise Refused('INVALID_STATUS')
        rows = self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND (? IS NULL OR source=?) ORDER BY created DESC',
            (owner, selection.get('source_id'), selection.get('source_id')),
        )
        results = []
        for row in rows:
            status = self.status(row)
            if selection.get('status', status['status']) == status['status']:
                results.append({'request_id': row['id'], 'source_id': row['source'],
                                'owner': owner, 'operation': row['operation'], **status})
            if len(results) == 25:
                break
        return {'jobs': results}

    def job_logs(self, row, limit):
        per_attempt = self.db.execute(
            'SELECT attempt,job_id FROM attempts WHERE request_id=? ORDER BY attempt DESC', (row['id'],),
        ).fetchall()
        logs = []
        for attempt in per_attempt:
            result = self.worker_api('job-log', job_id=attempt['job_id'], limit=limit)
            logs.extend({**event, 'attempt': attempt['attempt']} for event in result['events'])
        return {'request_id': row['id'], 'events': sorted(logs, key=lambda event: event['at'], reverse=True)[:limit]}

    def health(self, providers_only=False):
        try:
            return self.worker_api('provider-overview' if providers_only else 'queue-overview')
        except JobsError:
            return {'worker': 'unavailable'}

    def inspect_source(self, owner, source_id, command):
        entry, _, chat, topic = self.source(source_id)
        if chat is None:
            return self.external_source_status(owner, source_id, entry, command)
        with contextlib.ExitStack() as stack:
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, fd)
            source = stack.enter_context(descend(fd, ['imports', chat, topic]))
            controls = {name: read_input(source, name) for name in CONTROLS}
            hashes = {name: hashlib.sha256(data).hexdigest() for name, data in controls.items()}
            if hashes != entry['hashes']:
                raise Refused('HASH_MISMATCH')
            source_rows = list(map(json.loads, controls['messages.jsonl'].splitlines()))
            downloads = media_items(controls['messages.jsonl'])
            recovered = {}
            resume_hash = None
            try:
                resume = stack.enter_context(descend(source, ['media-resume']))
            except FileNotFoundError:
                resume = None
            if resume is not None:
                checkpoint = read_input(resume, 'status.json', 4 * 1024**2)
                resume_hash = hashlib.sha256(checkpoint).hexdigest()
                recovered = resumed_photos(checkpoint, hashes, source_rows)
            missing = set()
            over = set()
            for row in source_rows:
                media = row.get('media')
                if not media:
                    continue
                mid = row['message_id']
                if media.get('reason') == 'over_100_MiB':
                    over.add(mid)
                if mid not in recovered and media.get('path') is None:
                    missing.add(mid)
            verify_hashes = command == 'reindex'
            for group, root in (
                (downloads, source),
                ([(parts(r['path'])[1:], r['size'], r['sha256']) for r in recovered.values()], resume),
            ):
                for rel, size, digest in group:
                    with descend(root, rel[:-1]) as parent:
                        with open_input(parent, rel[-1]) as stream:
                            if os.fstat(stream.fileno()).st_size != size or (
                                verify_hashes and hashlib.file_digest(stream, 'sha256').hexdigest() != digest
                            ):
                                raise Refused('DATA_MISMATCH')
        media = {'total': len(downloads) + len(recovered) + len(missing),
                 'recovered': len(recovered), 'missing': len(missing), 'over_100_MiB': len(over),
                 'missing_message_ids': sorted(missing)[:1000],
                 'over_100_MiB_message_ids': sorted(over)[:1000],
                 'message_ids_truncated': max(len(missing), len(over)) > 1000}
        if command == 'media-status':
            return {'source_id': source_id, **media}
        db = self.worker_api('source-overview', source_id=source_id, project=entry['project'])
        rows = self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND source=? ORDER BY created DESC',
            (owner, source_id),
        ).fetchall()
        by_operation = {}
        for row in rows:
            if row['operation'] not in by_operation:
                by_operation[row['operation']] = {
                    'request_id': row['id'], 'status': self.status(row)['status'],
                }
        export = {'complete': json.loads(controls['manifest.json']).get('complete') is True,
                  'messages': len(source_rows),
                  'service_events': len(controls['services.jsonl'].splitlines()),
                  'sha256': hashes}
        result = {'source_id': source_id, 'export': export, 'media': media,
                  'staging': {'checked': True, 'media_resume_sha256': resume_hash},
                  'ingest': by_operation.get('ingest'), 'embed': by_operation.get('embed'),
                  'counts': db, 'media_missing': len(missing)}
        if command == 'reindex':
            result['integrity'] = {
                'media_sha256_verified': len(downloads) + len(recovered),
                'messages_match': export['messages'] == db['messages'] == db['unique_messages'],
                'service_events_match': export['service_events'] == db['service_events'],
                'ingested_media_match': db['messages_with_media'] == len(downloads) + len(recovered),
            }
        return result

    def audit_start(self, owner, command):
        with self.db:
            return self.db.execute(
                'INSERT INTO audit_log(at,owner,command,result) VALUES(?,?,?,?)',
                (timestamp(), owner, command if command in COMMANDS else 'invalid', 'STARTED'),
            ).lastrowid

    def audit(self, owner, command, args, response, *, seq=None):
        candidates = [token.removeprefix('source_id=') for token in args]
        source_id = next((value for value in candidates if value in self.sources), None)
        request_id = response.get('request_id')
        job_id = response.get('job_id')
        if not request_id and command in {'job-status', 'job-logs', 'retry', 'cancel'} and args:
            row = self.db.execute('SELECT id,source,attempt FROM requests WHERE id=? AND owner=?',
                                  (args[0], owner)).fetchone()
            if row:
                request_id, source_id = row['id'], row['source']
                bound = self.db.execute('SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                                        (request_id, row['attempt'])).fetchone()
                job_id = bound[0] if bound else None
        if request_id and source_id is None:
            row = self.db.execute('SELECT source FROM requests WHERE id=? AND owner=?',
                                  (request_id, owner)).fetchone()
            source_id = row[0] if row else None
        if response.get('ok'):
            outcome = 'OK'
        else:
            outcome = response['error_code']
        if seq is None:
            seq = self.audit_start(owner, command)
        with self.db:
            self.db.execute(
                'UPDATE audit_log SET source=?,request_id=?,job_id=?,result=? WHERE seq=? AND owner=?',
                (source_id, request_id, job_id, outcome, seq, owner),
            )

    def handle(self, owner, command, *args):
        if command == 'job-list' and len(args) <= 3:
            return self.job_list(owner, args)
        if command == 'provider-health' and args == ('--probe',):
            return {'probe': self.worker_api('provider-probe')}
        if command in {'queue-health', 'provider-health'} and not args:
            return self.health(providers_only=command == 'provider-health')
        if command == 'audit-log' and not args:
            return {'events': [dict(row) for row in self.db.execute(
                """SELECT a.at,a.owner,a.command,a.source AS source_id,a.request_id,a.job_id,a.result,
                          e.previous_request_id,e.previous_job_id,e.reason AS renewal_reason
                   FROM audit_log a LEFT JOIN embed_renewals e ON e.request_id=a.request_id
                   WHERE a.owner=? ORDER BY a.seq DESC LIMIT 25""", (owner,),
            )]}
        if command == 'job-logs' and len(args) in {1, 2}:
            n = args[1] if len(args) == 2 else '20'
            if not re.fullmatch(r'[1-9][0-9]?', n) or int(n) > 50:
                raise Refused('INVALID_LIMIT')
            return self.job_logs(self.row(owner, identifier(args[0])), int(n))
        if command == 'search' and len(args) in {2, 3}:
            return self.source_search(owner, args[0], args[1], args[2] if len(args) == 3 else None)
        if command == 'asr' and len(args) == 1:
            return self.source_asr(owner, args[0])
        if command == 'asr-status' and len(args) == 1:
            return self.source_asr_status(owner, args[0])
        if command == 'embed-asr' and len(args) == 1:
            return self.source_embed_asr(owner, args[0])
        if len(args) != 1:
            raise Refused('INVALID_REQUEST')
        value = args[0]
        if command in {'source-status', 'media-status', 'reindex'}:
            return self.inspect_source(owner, value, command)
        if command in ('ingest', 'embed'):
            return self.start(owner, command, value)
        if command == 'renew-embed':
            return self.renew_embed(owner, identifier(value))
        if command not in ('job-status', 'retry', 'cancel'):
            raise Refused('COMMAND_NOT_ALLOWED')
        row = self.row(owner, identifier(value))
        if command == 'job-status':
            return self.status(row)
        status = self.status(row)
        if command == 'cancel':
            if status['status'] not in {'running', 'waiting_quota', 'waiting_retry'}:
                raise Refused('CANCEL_NOT_ALLOWED')
            bound = self.db.execute('SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                                    (row['id'], row['attempt'])).fetchone()
            cancelled = self.worker_api('cancel', job_id=bound[0])
            return {'request_id': row['id'], 'job_id': bound[0], 'status': cancelled['state'],
                    'cancel_requested': cancelled['cancel_requested'] is True}
        if status['status'] == 'submitting':
            return self.dispatch(row)
        if row['attempt'] >= MAX_ATTEMPTS:
            raise Refused('RETRY_LIMIT')
        if not status['retryable']:
            raise Refused('RETRY_NOT_ALLOWED')
        entry, fingerprint, chat, topic = self.source(row['source'])
        operation = row['operation']
        if operation in {'asr_pilot', 'asr_full', 'asr_embed'} and entry.get('asr_enabled') is not True:
            raise Refused('ASR_NOT_ENABLED')
        renewal = self.db.execute(
            'SELECT base_fingerprint FROM embed_renewals WHERE request_id=?', (row['id'],),
        ).fetchone()
        expected_fingerprint = renewal[0] if renewal else row['fingerprint']
        if operation == 'ingest' and chat is None:
            # External ingest: no staged import tree, the fingerprint is stable.
            if fingerprint != expected_fingerprint:
                raise Refused('SOURCE_REVISION_CHANGED')
        elif operation == 'ingest':
            revision = media_revision(self.root, chat, topic)
            current = hashlib.sha256((fingerprint + revision).encode()).hexdigest() if revision else fingerprint
            if current != expected_fingerprint:
                raise Refused('SOURCE_REVISION_CHANGED')
            self.stage(row['source'], entry, fingerprint, chat, topic, revision)
        elif operation in {'asr_pilot', 'asr_full'}:
            meta = self.db.execute(
                'SELECT input_fingerprint FROM asr_request_meta WHERE request_id=?',
                (row['id'],),
            ).fetchone()
            if meta is None:
                raise Refused('ASR_REQUEST_METADATA_MISSING')
            revision = media_revision(self.root, chat, topic)
            staged = self.stage(row['source'], entry, fingerprint, chat, topic, revision)
            plan, _workspace = self.asr_workspace(
                row['source'], entry, staged, fingerprint, revision
            )
            if plan['input_fingerprint'] != meta[0]:
                raise Refused('SOURCE_REVISION_CHANGED')
        elif operation == 'asr_embed':
            overview = self.asr_overview(row['source'])
            current = hashlib.sha256(json.dumps(
                {'source_id': row['source'], 'source_fingerprint': fingerprint,
                 'asr_revision': overview.get('revision'), 'project': entry['project']},
                sort_keys=True, separators=(',', ':'),
            ).encode()).hexdigest()
            if current != expected_fingerprint:
                raise Refused('SOURCE_REVISION_CHANGED')
        elif operation == 'search':
            argv = json.loads(row['argv'])
            if (not isinstance(argv, list) or len(argv) != 11 or argv[0] != 'search'
                    or argv[2:4] != ['--project', entry['project']]
                    or argv[4] != '--limit' or argv[6:8] != ['--source-kind', 'telegram']
                    or argv[8:10] != ['--source-id', row['source']] or argv[10] != '--json'):
                raise Refused('SOURCE_REVISION_CHANGED')
            try:
                limit = int(argv[5])
            except (TypeError, ValueError):
                raise Refused('SOURCE_REVISION_CHANGED') from None
            current = search_fingerprint(row['source'], argv[1].strip(), limit)
            if current != expected_fingerprint:
                raise Refused('SOURCE_REVISION_CHANGED')
        elif operation == 'embed':
            if fingerprint != expected_fingerprint:
                raise Refused('SOURCE_REVISION_CHANGED')
        else:
            raise Refused('RETRY_NOT_ALLOWED')
        active_operations = (
            {'asr_pilot', 'asr_full', 'asr_embed'}
            if operation in {'asr_pilot', 'asr_full', 'asr_embed'}
            else {operation}
        )
        for active in self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND source=? AND id<>?',
            (owner, row['source'], row['id']),
        ).fetchall():
            if active['operation'] not in active_operations:
                continue
            if self.status(active)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        with self.db:
            self.db.execute("UPDATE requests SET attempt=attempt+1,state='submitting',counts='{}',error_code=NULL,updated=? WHERE id=?",
                            (timestamp(), row['id']))
        return self.dispatch(self.row(owner, row['id']))


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        allowed_peer = False
        command, args = '', []
        audit_seq = None
        try:
            _pid, uid, _gid = struct.unpack('3i', self.request.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')))
            if uid != self.server.uid:
                raise Refused('PEER_NOT_ALLOWED')
            allowed_peer = True
            line = self.rfile.readline(1025)
            if len(line) > 1024 or not line.endswith(b'\n'):
                raise Refused('INVALID_REQUEST')
            tokens = json.loads(line)
            if not isinstance(tokens, list) or not 1 <= len(tokens) <= 4 or not all(
                isinstance(v, str) and len(v) <= 180 for v in tokens
            ):
                raise Refused('INVALID_REQUEST')
            command, *args = tokens
            try:
                audit_seq = self.server.broker.audit_start('client', command)
            except sqlite3.Error:
                raise Refused('AUDIT_UNAVAILABLE') from None
            response = {'ok': True, **self.server.broker.handle('client', command, *args)}
            probe = response.get('probe')
            if command == 'provider-health' and probe and probe.get('status') in {'error', 'refused'}:
                response.update(ok=False, error_code=probe['error_code'])
        except Refused as exc:
            response = {'ok': False, 'error_code': str(exc)}
        except (json.JSONDecodeError, UnicodeError):
            response = {'ok': False, 'error_code': 'INVALID_REQUEST'}
        except Exception:
            response = {'ok': False, 'error_code': 'BROKER_OPERATION_FAILED'}
        if allowed_peer:
            try:
                self.server.broker.audit('client', command, args, response, seq=audit_seq)
            except sqlite3.Error:
                response = {'ok': False, 'error_code': 'AUDIT_UNAVAILABLE'}
        with contextlib.suppress(OSError):
            self.wfile.write((json.dumps(response) + '\n').encode())


def main():
    os.umask(0o077)
    with (ROOT / 'broker' / 'broker.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        staging = ROOT / 'imports' / '.broker'
        for candidate in staging.iterdir():
            if candidate.name.startswith('.stage-'):
                private(candidate.lstat(), directory=True)
                shutil.rmtree(candidate)
        SOCKET.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        SOCKET.unlink(missing_ok=True)
        with socketserver.UnixStreamServer(str(SOCKET), Handler) as server:
            server.uid = int(os.environ.get("KIBER_ANALYST_BROKER_PEER_UID", str(os.getuid())))
            server.broker = Broker(ROOT, registry(CONFIG))
            SOCKET.chmod(0o660)
            server.serve_forever()


if __name__ == '__main__':
    main()
