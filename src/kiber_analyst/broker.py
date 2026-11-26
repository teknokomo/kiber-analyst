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
from kiber_analyst.media_limits import (
    DEFAULT_TELEGRAM_MEDIA_BYTES,
    MAX_TELEGRAM_MEDIA_BYTES,
    validate_telegram_media_limit,
)
from kiber_analyst.media_resume import resumed_photos
from kiber_analyst.project_catalog import SLUG_RE, read_project_policy
from kiber_analyst.project_catalog import request_key as project_request_key

ROOT = data_path()
CONFIG = config_path("sources.json")
PROJECT_ACCESS = config_path("project-access.json")
SOCKET = runtime_path("broker.sock")
CONTROLS = ('messages.jsonl', 'services.jsonl', 'manifest.json')
CONTROL_FILE_MAX_BYTES = 32 * 1024**2
SERVICES_MANIFEST_MAX_BYTES = 1024**2
TERMINAL = {'done', 'failed', 'timed_out', 'cancelled'}
ACTIVE = {'queued', 'running', 'waiting_retry', 'waiting_quota', 'submitting'}
COMMANDS = {'ingest', 'embed', 'retry', 'job-status', 'job-list', 'job-logs',
            'queue-health', 'provider-health', 'source-status', 'media-status',
            'cancel', 'reindex', 'audit-log', 'renew-embed', 'search', 'asr',
            'asr-status', 'embed-asr', 'capabilities', 'project-list',
            'project-create', 'source-list', 'source-add', 'ingest-dry-run',
            'search-source', 'search-project', 'search-result'}
TRANSIENT = {'transient_database_error', 'server_error', 'network_error',
             'request_timeout', 'network_timeout'}
COUNTS = {'inserted', 'updated', 'skipped', 'absent', 'media_missing',
          'service_events', 'service_events_inserted', 'service_events_updated',
          'service_events_skipped', 'errors', 'segments_total', 'skipped_existing',
          'skipped_empty', 'completed', 'total', 'completed_batches', 'result_count',
          'files_total', 'files_done', 'files_failed', 'files_skipped', 'segments', 'empty',
          'threads', 'items', 'nonempty', 'incomplete', 'embeddings',
          'nonempty_items_without_embeddings', 'messages', 'unique_messages',
          'nonempty_messages', 'messages_with_media', 'nonempty_messages_without_embeddings'}
RETRYABLE_ERRORS = TRANSIENT | {'rate_limited'}
PUBLIC_ERROR_CATEGORIES = RETRYABLE_ERRORS | {
    'rate_limited', 'profile_auth', 'profile_configuration', 'policy_blocked',
    'request_rejected', 'provider_error', 'invalid_response', 'worker_restart',
}
MAX_ATTEMPTS = 3
RESERVE = 5 * 1024**3
SEARCH_MAX_LIMIT = 50
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
TELEGRAM_SOURCE_RE = re.compile(
    r'telegram:(?P<chat>-[1-9][0-9]{0,18}):topic:(?P<topic>[1-9][0-9]{0,9})\Z'
)


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


def telegram_topic_source(source_id):
    """Parse a Telegram topic identifier without accepting paths or flags."""
    if not isinstance(source_id, str):
        return None
    match = TELEGRAM_SOURCE_RE.fullmatch(source_id)
    return (match.group('chat'), match.group('topic')) if match else None


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


def _media_stat_token(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def verify_media_files(root, items, *, owner=None):
    """Stream-check staged media each time an immutable revision is reused."""
    for rel, size, digest in items:
        with descend(root, rel[:-1], owner=owner) as parent:
            with open_input(parent, rel[-1], owner=owner) as stream:
                before = os.fstat(stream.fileno())
                if before.st_size != size:
                    raise Refused('DATA_MISMATCH')
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
                after = os.fstat(stream.fileno())
                if (_media_stat_token(before) != _media_stat_token(after)
                        or actual != digest):
                    raise Refused('DATA_MISMATCH')


def copy_media(source, output, items, *, owner=None):
    for rel, size, digest in items:
        with descend(source, rel[:-1], owner=owner) as old, descend(output, rel[:-1], create=True) as new:
            with open_input(old, rel[-1], owner=owner) as src:
                before = os.fstat(src.fileno())
                if before.st_size != size:
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
                    after = os.fstat(src.fileno())
                    if (src.read(1) or actual.hexdigest() != digest
                            or _media_stat_token(before) != _media_stat_token(after)):
                        raise Refused('DATA_MISMATCH')
                    dst.flush()
                    if os.fstat(dst.fileno()).st_size != size:
                        raise Refused('DATA_MISMATCH')
                    os.fsync(dst.fileno())
            os.fsync(new)


def _manifest_media_limit(data):
    try:
        manifest = json.loads(data)
        return validate_telegram_media_limit(
            manifest.get("max_file_bytes", DEFAULT_TELEGRAM_MEDIA_BYTES)
        )
    except (ValueError, AttributeError):
        raise Refused("UNSAFE_MEDIA") from None


def media_items(data, manifest_limit=DEFAULT_TELEGRAM_MEDIA_BYTES):
    try:
        validate_telegram_media_limit(manifest_limit)
    except ValueError:
        raise Refused("UNSAFE_MEDIA") from None
    result = []
    seen = set()
    for line in data.splitlines():
        try:
            row = json.loads(line)
        except (ValueError, UnicodeError):
            raise Refused('UNSAFE_MEDIA') from None
        if not isinstance(row, dict) or type(row.get('message_id')) is not int or row['message_id'] <= 0:
            raise Refused('UNSAFE_MEDIA')
        media = row.get('media')
        if media is None:
            continue
        if not isinstance(media, dict):
            raise Refused('UNSAFE_MEDIA')
        value = media.get('path')
        if value is None:
            continue
        rel = parts(value)
        size = media.get('size', media.get('size_bytes'))
        digest = media.get('sha256')
        limit = media.get('max_file_bytes', manifest_limit)
        if (len(rel) != 3 or rel[:2] != ['media', str(row['message_id'])]
                or value in seen or type(size) is not int or not 0 < size <= MAX_TELEGRAM_MEDIA_BYTES
                or type(limit) is not int or not 0 < limit <= MAX_TELEGRAM_MEDIA_BYTES
                or limit > manifest_limit or size > limit
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
            verify_media_files(existing, files, owner=existing_owner)
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
    def __init__(self, root, sources, worker_api=api_call, submit=submit_command,
                 authorized_projects=None, allow_project_create=False):
        self.root = root
        self.sources = sources
        self.authorized_projects = set(authorized_projects or ())
        self.legacy_source_projects = {
            source_id: entry['project']
            for source_id, entry in sources.items()
            if isinstance(entry, dict) and isinstance(entry.get('project'), str)
        }
        self.allow_project_create = bool(allow_project_create)
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
            CREATE TABLE IF NOT EXISTS project_grants (
                owner TEXT NOT NULL, project TEXT NOT NULL, title TEXT NOT NULL,
                request_key TEXT NOT NULL, created TEXT NOT NULL,
                PRIMARY KEY(owner,project)
            );
            CREATE TABLE IF NOT EXISTS managed_sources (
                owner TEXT NOT NULL, source TEXT NOT NULL, project TEXT NOT NULL,
                provider TEXT NOT NULL, metadata TEXT NOT NULL DEFAULT '{}', added TEXT NOT NULL,
                PRIMARY KEY(owner,source,project)
            );
            CREATE TABLE IF NOT EXISTS project_search_scope (
                request_id TEXT PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE,
                project TEXT NOT NULL, query_sha256 TEXT NOT NULL,
                result_limit INTEGER NOT NULL, request_fingerprint TEXT NOT NULL
            );
        """)
        managed_columns = {
            row['name'] for row in self.db.execute('PRAGMA table_info(managed_sources)')
        }
        if 'metadata' not in managed_columns:
            self.db.execute(
                "ALTER TABLE managed_sources ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'"
            )
        self._backfill_project_search_scope()

    def _backfill_project_search_scope(self):
        """Pin parameters of pre-scope project-search requests for future retries."""
        rows = self.db.execute(
            """SELECT r.* FROM requests r LEFT JOIN project_search_scope s ON s.request_id=r.id
               WHERE r.operation='project-search' AND s.request_id IS NULL"""
        ).fetchall()
        for row in rows:
            try:
                argv = json.loads(row['argv'])
            except (TypeError, json.JSONDecodeError):
                continue
            project = self._argv_project(argv)
            if (not isinstance(argv, list) or len(argv) != 7 or argv[0] != 'search'
                    or project is None or row['source'] != f'project:{project}'
                    or argv[2:5] != ['--project', project, '--limit']
                    or argv[6] != '--json'):
                continue
            query = argv[1]
            limit_text = argv[5]
            if (not isinstance(row['fingerprint'], str)
                    or re.fullmatch(r'[0-9a-f]{64}', row['fingerprint']) is None
                    or not isinstance(query, str) or not query or query != query.strip()
                    or len(query) > SEARCH_MAX_QUERY_CHARS
                    or any(ord(char) < 32 or ord(char) == 127 for char in query)
                    or query.lstrip().startswith('-')
                    or not isinstance(limit_text, str)
                    or re.fullmatch(r'(?:[1-9][0-9]?|50)', limit_text) is None):
                continue
            with self.db:
                self.db.execute(
                    """INSERT OR IGNORE INTO project_search_scope
                       (request_id,project,query_sha256,result_limit,request_fingerprint)
                       VALUES(?,?,?,?,?)""",
                    (row['id'], project, hashlib.sha256(query.encode()).hexdigest(),
                     int(limit_text), row['fingerprint']),
                )

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

    @staticmethod
    def _source_fingerprint_variants(source_id, entry):
        compact = hashlib.sha256(
            json.dumps(entry, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
        ).hexdigest()
        variants = {compact}
        if parse_source_id(source_id) is not None:
            # The legacy static-source path used json.dumps' default spaces.
            # Accept that exact historical identity for the same project.
            legacy = hashlib.sha256(
                json.dumps(entry, sort_keys=True).encode()
            ).hexdigest()
            variants.add(legacy)
        return variants

    def _project_granted(self, owner, project):
        if project in self.authorized_projects:
            return True
        return self.db.execute(
            'SELECT 1 FROM project_grants WHERE owner=? AND project=?', (owner, project),
        ).fetchone() is not None

    def _require_request_project(self, owner, row):
        project = self._argv_project(json.loads(row['argv']))
        legacy_source_scope = self.legacy_source_projects.get(row['source']) == project
        if project is not None and not self._project_granted(owner, project) and not legacy_source_scope:
            raise Refused('PROJECT_NOT_ALLOWED')
        return project

    def _retry_source_candidates(self, owner, source_id, project):
        """Return current source identities without changing the request's saved scope."""
        candidates = []
        static = self.sources.get(source_id)
        static_matches = isinstance(static, dict) and static.get('project') == project
        registered = self.db.execute(
            'SELECT 1 FROM managed_sources WHERE owner=? AND source=? AND project=?',
            (owner, source_id, project),
        ).fetchone() is not None

        if registered or static_matches:
            try:
                candidates.append(self._managed_source_entry(owner, source_id, project))
            except Refused:
                # A legacy static request can still use its exact configured
                # source scope when no managed project grant exists.
                pass
        if static_matches:
            try:
                candidates.append(self.source(source_id))
            except Refused:
                pass
        unique = []
        seen = set()
        for candidate in candidates:
            if len(candidate) == 3:
                entry, fingerprint, parsed = candidate
                chat, topic = parsed if isinstance(parsed, tuple) and len(parsed) == 2 else (None, None)
            else:
                entry, fingerprint, chat, topic = candidate
            if fingerprint not in seen:
                seen.add(fingerprint)
                unique.append((entry, fingerprint, chat, topic))
        return unique

    def _embed_source_for_request(self, owner, row, expected_fingerprint):
        project = self._require_request_project(owner, row)
        candidates = self._retry_source_candidates(owner, row['source'], project)
        for candidate in candidates:
            if (self._base_fingerprint(candidate[1])
                    == self._base_fingerprint(expected_fingerprint)):
                return candidate
        raise Refused('SOURCE_REVISION_CHANGED')

    def project_list(self, owner):
        catalog = self.worker_api('project-catalog', mode='list')
        projects = catalog.get('projects') if isinstance(catalog, dict) else None
        if not isinstance(projects, list):
            raise Refused('PROJECT_CATALOG_UNAVAILABLE')
        grants = {
            row['project'] for row in self.db.execute(
                'SELECT project FROM project_grants WHERE owner=?', (owner,),
            )
        }
        visible = []
        for item in projects:
            if not isinstance(item, dict):
                continue
            slug, title = item.get('slug'), item.get('title')
            if (isinstance(slug, str) and SLUG_RE.fullmatch(slug)
                    and isinstance(title, str) and (slug in self.authorized_projects or slug in grants)):
                visible.append({'slug': slug, 'title': title})
        return {'projects': visible}

    def project_create(self, owner, slug, title):
        if not self.allow_project_create:
            raise Refused('PROJECT_CREATE_DISABLED')
        if (not isinstance(slug, str) or SLUG_RE.fullmatch(slug) is None
                or not isinstance(title, str) or not title.strip() or len(title.strip()) > 120
                or any(ord(char) < 32 or ord(char) == 127 for char in title)):
            raise Refused('INVALID_PROJECT')
        title = title.strip()
        key = project_request_key(owner, slug, title)
        grant = self.db.execute(
            'SELECT title,request_key FROM project_grants WHERE owner=? AND project=?',
            (owner, slug),
        ).fetchone()
        if grant is not None:
            if grant['title'] != title or grant['request_key'] != key:
                raise Refused('PROJECT_ALREADY_EXISTS')
            return {'slug': slug, 'title': title, 'created': False}
        try:
            created = self.worker_api(
                'project-catalog', mode='create', slug=slug, title=title,
                owner=owner, request_key=key,
            )
        except JobsError as exc:
            if 'PROJECT_ALREADY_EXISTS' in str(exc):
                raise Refused('PROJECT_ALREADY_EXISTS') from None
            raise Refused('PROJECT_CREATE_FAILED') from None
        if (created.get('slug') != slug or created.get('title') != title
                or created.get('request_key') != key
                or type(created.get('created')) is not bool):
            raise Refused('PROJECT_CREATE_FAILED')
        with self.db:
            self.db.execute(
                'INSERT INTO project_grants(owner,project,title,request_key,created) VALUES(?,?,?,?,?)',
                (owner, slug, title, key, timestamp()),
            )
        return {'slug': slug, 'title': title, 'created': bool(created.get('created'))}

    @staticmethod
    def _valid_external_source(source_id):
        return parse_source_id(source_id) is not None

    def _managed_source_entry(self, owner, source_id, project):
        parsed_external = parse_source_id(source_id)
        telegram_parts = telegram_topic_source(source_id)
        if parsed_external is None and telegram_parts is None:
            raise Refused('UNSUPPORTED_SOURCE')
        if not isinstance(project, str) or SLUG_RE.fullmatch(project) is None:
            raise Refused('INVALID_PROJECT')
        if not self._project_granted(owner, project):
            raise Refused('PROJECT_NOT_ALLOWED')
        registered = self.db.execute(
            'SELECT provider,metadata FROM managed_sources WHERE owner=? AND source=? AND project=?',
            (owner, source_id, project),
        ).fetchone()
        static = self.sources.get(source_id)
        if registered is None and not (isinstance(static, dict) and static.get('project') == project):
            raise Refused('SOURCE_NOT_REGISTERED')
        entry = {'project': project}
        if telegram_parts is not None:
            if registered is not None:
                try:
                    metadata = json.loads(registered['metadata'])
                except (TypeError, json.JSONDecodeError):
                    raise Refused('SOURCE_REGISTRATION_INVALID') from None
                hashes = metadata.get('hashes') if isinstance(metadata, dict) else None
                if (not isinstance(hashes, dict) or set(hashes) != set(CONTROLS)
                        or any(not isinstance(value, str)
                               or re.fullmatch(r'[0-9a-f]{64}', value) is None
                               for value in hashes.values())):
                    raise Refused('SOURCE_REGISTRATION_INVALID')
                entry['hashes'] = hashes
            elif isinstance(static.get('hashes'), dict):
                entry['hashes'] = static['hashes']
        fingerprint = hashlib.sha256(
            json.dumps(entry, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest()
        return entry, fingerprint, parsed_external or telegram_parts

    def source_add(self, owner, source_id, project):
        entry, _fingerprint, parsed = self._managed_source_entry_for_add(owner, source_id, project)
        provider = parsed.provider if hasattr(parsed, 'provider') else 'telegram'
        metadata = {'hashes': entry['hashes']} if provider == 'telegram' else {}
        encoded_metadata = json.dumps(metadata, sort_keys=True, separators=(',', ':'))
        current = self.db.execute(
            'SELECT metadata FROM managed_sources WHERE owner=? AND source=? AND project=?',
            (owner, source_id, project),
        ).fetchone()
        if current is not None and current['metadata'] != encoded_metadata:
            active = self.db.execute(
                "SELECT * FROM requests WHERE owner=? AND source=? AND operation IN ('ingest','embed')",
                (owner, source_id),
            ).fetchall()
            if any(self.status(row)['status'] not in TERMINAL for row in active):
                raise Refused('SOURCE_BUSY')
        with self.db:
            self.db.execute(
                'INSERT INTO managed_sources(owner,source,project,provider,metadata,added) '
                'VALUES(?,?,?,?,?,?) ON CONFLICT(owner,source,project) DO UPDATE SET '
                'provider=excluded.provider,metadata=excluded.metadata,added=excluded.added',
                (owner, source_id, project, provider, encoded_metadata, timestamp()),
            )
        return {'source_id': source_id, 'project': entry['project'],
                'provider': provider, 'registered': True}

    def _managed_source_entry_for_add(self, owner, source_id, project):
        if not isinstance(source_id, str):
            raise Refused('UNSUPPORTED_SOURCE')
        if not isinstance(project, str) or SLUG_RE.fullmatch(project) is None:
            raise Refused('INVALID_PROJECT')
        if not self._project_granted(owner, project):
            raise Refused('PROJECT_NOT_ALLOWED')
        catalog = self.worker_api('project-catalog', mode='list')
        projects = catalog.get('projects', []) if isinstance(catalog, dict) else []
        if not any(isinstance(item, dict) and item.get('slug') == project for item in projects):
            raise Refused('PROJECT_NOT_FOUND')
        parsed = parse_source_id(source_id)
        telegram_parts = telegram_topic_source(source_id)
        if parsed is None and telegram_parts is None:
            raise Refused('UNSUPPORTED_SOURCE')
        entry = {'project': project}
        if telegram_parts is not None:
            from kiber_analyst.telegram_ingest import inspect_export

            export_path = self.root / 'imports' / telegram_parts[0] / telegram_parts[1]
            try:
                export = inspect_export(
                    export_path,
                    root=self.root / 'imports',
                    max_jsonl_bytes=CONTROL_FILE_MAX_BYTES,
                )
            except Exception:
                raise Refused('TELEGRAM_EXPORT_UNAVAILABLE') from None
            if (export.chat_id != telegram_parts[0]
                    or str(export.topic_id) != telegram_parts[1]):
                raise Refused('TELEGRAM_EXPORT_MISMATCH')
            if (export.messages_size > CONTROL_FILE_MAX_BYTES
                    or export.services_size > CONTROL_FILE_MAX_BYTES
                    or export.manifest_size > CONTROL_FILE_MAX_BYTES
                    or export.services_manifest_size > SERVICES_MANIFEST_MAX_BYTES):
                raise Refused('TELEGRAM_EXPORT_TOO_LARGE')
            entry['hashes'] = {
                'messages.jsonl': export.message_sha256,
                'services.jsonl': export.service_sha256,
                'manifest.json': export.manifest_sha256,
            }
        fingerprint = hashlib.sha256(
            json.dumps(entry, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest()
        return entry, fingerprint, parsed or telegram_parts

    def source_list(self, owner, project=None):
        grants = {row['project'] for row in self.db.execute(
            'SELECT project FROM project_grants WHERE owner=?', (owner,),
        )}
        granted_projects = self.authorized_projects | grants
        static_sources = {
            (source_id, entry.get('project'), source_id.split(':', 1)[0])
            for source_id, entry in self.sources.items()
            if isinstance(entry, dict) and isinstance(entry.get('project'), str)
        }
        if project is not None:
            if not isinstance(project, str) or SLUG_RE.fullmatch(project) is None:
                raise Refused('INVALID_PROJECT')
            static_for_project = {record for record in static_sources if record[1] == project}
            if project not in granted_projects and not static_for_project:
                raise Refused('PROJECT_NOT_ALLOWED')
            static_sources = static_for_project
            granted_projects = {project} if project in granted_projects else set()
        records = set(static_sources)
        records.update(
            (row['source'], row['project'], row['provider'])
            for row in self.db.execute(
                'SELECT source,project,provider FROM managed_sources WHERE owner=?', (owner,),
            ) if row['project'] in granted_projects
        )
        return {'sources': [
            {'source_id': source_id, 'project': target, 'provider': provider}
            for source_id, target, provider in sorted(records)
        ]}

    def capabilities(self):
        return {'providers': {
            'github': {'operations': ['register', 'dry_run', 'ingest', 'retry', 'embed', 'status', 'search'],
                       'coverage': 'issues, pull requests, comments and available review/discussion endpoints'},
            'gitlab': {'operations': ['register', 'dry_run', 'ingest', 'retry', 'embed', 'status', 'search'],
                       'coverage': 'issues, merge requests, notes, discussions and available system events'},
            'gitverse': {'operations': ['register', 'dry_run', 'ingest', 'retry', 'embed', 'status', 'search'],
                         'coverage': 'issues, pull requests, comments and available reviews; unsupported endpoints remain partial'},
            'telegram': {
                'operations': ['register_complete_topic_export', 'dry_run', 'ingest', 'retry',
                               'embed', 'status', 'search'],
                'scopes': {'topic': True, 'group': False},
                'export': {'available_via_separate_interface': True,
                           'invoked_by_kiber_broker': False},
                'ingest_requires': 'complete, unbounded topic export already staged in the fixed Telegram import directory',
            },
        }, 'project_operations': {
            'list': True, 'select_existing': True,
            'create': self.allow_project_create,
        },
            'unsupported': ['web', 'documents', 'unimplemented connectors']}

    def stage(self, source_id, entry, fingerprint, chat, topic, revision=""):
        # A media-resume checkpoint is an immutable input revision. Keep the
        # earlier stage intact and place each later checkpoint in its own tree.
        staging_fingerprint = (
            hashlib.sha256((fingerprint + revision).encode()).hexdigest()
            if revision else fingerprint
        )
        with contextlib.ExitStack() as stack:
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, fd)
            private(os.fstat(fd), directory=True)
            source = stack.enter_context(descend(fd, ['imports', chat, topic]))
            data = {name: read_input(source, name, CONTROL_FILE_MAX_BYTES) for name in CONTROLS}
            if any(hashlib.sha256(data[n]).hexdigest() != entry['hashes'][n] for n in CONTROLS):
                raise Refused('HASH_MISMATCH')
            services = read_input(source, 'services_manifest.json', SERVICES_MANIFEST_MAX_BYTES)
            service_meta = json.loads(services)
            if (service_meta.get('source_id') != source_id
                    or service_meta.get('services_sha256') != entry['hashes']['services.jsonl']
                    or service_meta.get('complete') is not True):
                raise Refused('DATA_MISMATCH')
            if json.loads(data['manifest.json']).get('source_id') != source_id:
                raise Refused('DATA_MISMATCH')
            manifest_limit = _manifest_media_limit(data['manifest.json'])
            files = media_items(data['messages.jsonl'], manifest_limit)
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

            destination = self.root / 'imports' / '.broker' / staging_fingerprint / chat / topic
            try:
                with descend(base, [staging_fingerprint, chat, topic]) as final:
                    if any(read_input(final, n) != v for n, v in data.items()):
                        raise Refused('HASH_MISMATCH')
                    verify_media_files(final, files)
                    merge(final)
                    return destination
            except FileNotFoundError:
                # A partially missing existing final directory is never replaced.
                if staging_fingerprint in os.listdir(base):
                    raise Refused('DATA_MISMATCH') from None
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
                os.rename(name, staging_fingerprint, src_dir_fd=base, dst_dir_fd=base)
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
            unbound_state = 'cancelled' if row['state'] == 'cancelled' else 'submitting'
            return {'status': unbound_state, 'counts': {}, 'error_code': None,
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

    def safe_job_status(self, owner, request_id):
        row = self.row(owner, identifier(request_id))
        self._require_request_project(owner, row)
        report = self.status(row)
        managed = self.db.execute(
            'SELECT 1 FROM managed_sources WHERE owner=? AND source=? LIMIT 1',
            (owner, row['source']),
        ).fetchone() is not None
        # Preserve the established static Telegram status contract.
        if parse_source_id(row['source']) is None and not managed and row['operation'] != 'project-search':
            return report
        argv = json.loads(row['argv'])
        project = self._argv_project(argv)
        report.update({'request_id': row['id'], 'job_id': self.db.execute(
            'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
            (row['id'], row['attempt']),
        ).fetchone()[0] if self.db.execute(
            'SELECT 1 FROM attempts WHERE request_id=? AND attempt=?',
            (row['id'], row['attempt']),
        ).fetchone() else None})
        if project is not None:
            report['project'] = project
        if row['operation'] == 'ingest' and report['status'] in TERMINAL:
            job_id = report.get('job_id')
            if job_id:
                try:
                    payload = self.worker_api('result', job_id=job_id)
                except JobsError:
                    payload = {}
                if isinstance(payload, dict):
                    outcome = payload.get('status')
                    if outcome in {'done', 'partial', 'dry_run'}:
                        report['ingest_status'] = outcome
                    reasons = payload.get('partial_reasons')
                    if isinstance(reasons, list):
                        report['partial_reasons'] = [
                            value for value in reasons
                            if isinstance(value, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,39}', value)
                        ][:20]
                    load = payload.get('load')
                    if isinstance(load, dict):
                        status = load.get('status')
                        if isinstance(status, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,39}', status):
                            report['load_status'] = status
                        if type(load.get('rollback_confirmed')) is bool:
                            report['rollback_confirmed'] = load['rollback_confirmed']
                    counts = payload.get('counts')
                    if isinstance(counts, dict):
                        safe = {key: value for key, value in counts.items()
                                if key in COUNTS and type(value) is int and value >= 0}
                        report['counts'].update(safe)
        return report

    def start(self, owner, operation, source_id, *, entry_override=None,
              since=None, limit=None, dry_run=False):
        if entry_override is None:
            entry, fingerprint, chat, topic = self.source(source_id)
        else:
            topic_source = telegram_topic_source(source_id)
            if topic_source is None:
                raise Refused('UNSUPPORTED_SOURCE')
            entry = entry_override
            chat, topic = topic_source
            fingerprint = hashlib.sha256(
                json.dumps(entry, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest()
        if chat is None:
            return self.start_external(
                owner, operation, source_id, entry, fingerprint,
                since=since, limit=limit, dry_run=dry_run,
            )
        base_fingerprint = fingerprint
        revision = media_revision(self.root, chat, topic) if operation == 'ingest' else ''
        if revision:
            fingerprint = hashlib.sha256((fingerprint + revision).encode()).hexdigest()
        if operation == 'ingest' and dry_run:
            fingerprint = f'{fingerprint}:dry-run'
        relevant = self.db.execute(
            """SELECT r.*, coalesce(e.base_fingerprint,r.fingerprint) AS source_fingerprint
               FROM requests r LEFT JOIN embed_renewals e ON e.request_id=r.id
               WHERE owner=? AND source=? AND operation=? ORDER BY r.created DESC""",
            (owner, source_id, operation),
        ).fetchall()
        for row in relevant:
            if row['source_fingerprint'] == fingerprint:
                if operation == 'ingest':
                    bound_attempt = self.db.execute(
                        'SELECT 1 FROM attempts WHERE request_id=? AND attempt=?',
                        (row['id'], row['attempt']),
                    ).fetchone()
                    if row['state'] == 'cancelled' and bound_attempt is None:
                        # The worker may have recorded a cancellation tombstone
                        # for this idempotency key before a job was bound. Never
                        # redispatch that key; require an explicit new source
                        # revision/request instead.
                        raise Refused('INGEST_CANCELLED')
                    self.stage(source_id, entry, base_fingerprint, chat, topic, revision)
                elif self.status(row)['retryable']:
                    return self.handle(owner, 'retry', row['id'])
                return self.dispatch(row)
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        if operation == 'ingest':
            staged = self.stage(source_id, entry, base_fingerprint, chat, topic, revision)
            argv = ['telegram', 'ingest', '--job', str(staged), '--project', entry['project']]
            if dry_run:
                argv.append('--dry-run')
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

    @staticmethod
    def _base_fingerprint(value):
        # New runs use "<base>:<nonce>" so the requests uniqueness constraint
        # still allows a fresh request while the source revision stays stable.
        return str(value).split(':', 1)[0]

    def start_external(self, owner, operation, source_id, entry, fingerprint, *,
                       since=None, limit=None, dry_run=False):
        # External sources: no staged files, the connector fetches from the API.
        provider, external_id = source_id.split(':', 1)
        fingerprint_variants = self._source_fingerprint_variants(source_id, entry)
        fingerprint_variants.add(fingerprint)
        if operation == 'ingest':
            if provider == 'gitlab':
                host, project_path = external_id.split('/', 1)
                argv = ['gitlab', 'ingest', '--host', host, '--project', project_path,
                        '--project-slug', entry['project']]
            else:
                argv = [provider, 'ingest', '--repo', external_id, '--project', entry['project']]
            if since is not None:
                argv.extend(['--since', since])
            if limit is not None:
                argv.extend(['--limit', str(limit)])
            if dry_run:
                argv.append('--dry-run')
        elif operation == 'embed':
            ingests = self.db.execute(
                'SELECT * FROM requests WHERE owner=? AND source=? AND operation=?'
                ' ORDER BY created DESC',
                (owner, source_id, 'ingest'),
            ).fetchall()
            ingest = next((row for row in ingests
                           if self._base_fingerprint(row['fingerprint']) in fingerprint_variants
                           and self._argv_project(json.loads(row['argv'])) == entry['project']
                           and '--dry-run' not in json.loads(row['argv'])), None)
            if ingest is None or self.status(ingest)['status'] != 'done':
                raise Refused('INGEST_REQUIRED')
            argv = ['external', 'embed', '--source', source_id, '--project', entry['project']]
        else:
            raise Refused('COMMAND_NOT_ALLOWED')
        relevant = self.db.execute(
            """SELECT r.*, coalesce(e.base_fingerprint,r.fingerprint) AS source_fingerprint
               FROM requests r LEFT JOIN embed_renewals e ON e.request_id=r.id
               WHERE owner=? AND source=? AND operation=? ORDER BY r.created DESC""",
            (owner, source_id, operation),
        ).fetchall()
        request_fingerprint = f'{fingerprint}:dry-run' if dry_run else f'{fingerprint}:real'
        matching_rows = []
        active_row = None
        for row in relevant:
            try:
                row_project = self._argv_project(json.loads(row['argv']))
            except (TypeError, json.JSONDecodeError):
                continue
            row_argv = json.loads(row['argv'])
            row_is_dry_run = isinstance(row_argv, list) and '--dry-run' in row_argv
            if (self._base_fingerprint(row['source_fingerprint']) in fingerprint_variants
                    and row_project == entry['project']
                    and row_is_dry_run == dry_run):
                matching_rows.append(row)
                if row['state'] not in TERMINAL:
                    current_status = self.status(row)
                    if current_status['status'] not in TERMINAL and active_row is None:
                        active_row = row
        if active_row is not None:
            if json.loads(active_row['argv']) != argv:
                raise Refused('SOURCE_BUSY')
            return self.dispatch(active_row)
        if matching_rows:
            # Deterministic next generation: concurrent callers race on the
            # requests uniqueness constraint instead of both creating a run.
            request_fingerprint = f"{request_fingerprint}:{len(matching_rows)}"
        now = timestamp()
        request_id = str(uuid.uuid4())
        try:
            with self.db:
                self.db.execute(
                    'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                    (request_id, owner, source_id, request_fingerprint, operation,
                     json.dumps(argv), now, now),
                )
        except sqlite3.IntegrityError:
            row = self.db.execute(
                'SELECT * FROM requests WHERE owner=? AND source=? AND fingerprint=? AND operation=?',
                (owner, source_id, request_fingerprint, operation),
            ).fetchone()
            if row is None:
                raise Refused('SOURCE_BUSY') from None
            if json.loads(row['argv']) != argv:
                raise Refused('SOURCE_BUSY') from None
            return self.dispatch(row)
        return self.dispatch(self.row(owner, request_id))

    @staticmethod
    def _argv_project(argv):
        if not isinstance(argv, list):
            return None
        if len(argv) >= 2 and argv[0] == 'gitlab' and argv[1] == 'ingest':
            flag = '--project-slug'
            try:
                index = argv.index(flag)
            except ValueError:
                return None
            return argv[index + 1] if index + 1 < len(argv) else None
        for flag in ('--project', '--project-slug'):
            try:
                index = argv.index(flag)
            except ValueError:
                continue
            if index + 1 < len(argv):
                return argv[index + 1]
        return None

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
            try:
                argv = json.loads(row['argv'])
            except (TypeError, json.JSONDecodeError):
                continue
            if self._argv_project(argv) != entry['project']:
                continue
            if row['operation'] not in by_operation:
                by_operation[row['operation']] = {
                    'request_id': row['id'], 'status': self.status(row)['status'],
                }
        return {'source_id': source_id, 'provider': source_id.split(':', 1)[0],
                'counts': db, 'ingest': by_operation.get('ingest'),
                'embed': by_operation.get('embed')}

    def managed_ingest(self, owner, source_id, project, *, since=None, limit=None, dry_run=False):
        entry, fingerprint, _parsed = self._managed_source_entry(owner, source_id, project)
        is_telegram = telegram_topic_source(source_id) is not None
        if since is not None:
            if (not isinstance(since, str) or len(since) > 40
                    or any(ord(char) < 32 or ord(char) == 127 for char in since)):
                raise Refused('INVALID_SINCE')
            try:
                if re.fullmatch(r'\d{4}-\d{2}-\d{2}', since):
                    datetime.date.fromisoformat(since)
                else:
                    parsed_since = datetime.datetime.fromisoformat(since.replace('Z', '+00:00'))
                    if parsed_since.tzinfo is None or parsed_since.utcoffset() is None:
                        raise ValueError
            except ValueError:
                raise Refused('INVALID_SINCE') from None
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 100):
            raise Refused('INVALID_LIMIT')
        if is_telegram and (since is not None or limit is not None):
            raise Refused('TELEGRAM_EXPORT_IS_ALREADY_BOUNDED')
        if dry_run and since is None and not is_telegram:
            raise Refused('SINCE_REQUIRED_FOR_DRY_RUN')
        if not dry_run and since is None and not is_telegram:
            overview = self.worker_api('source-overview', source_id=source_id, project=project)
            if overview.get('threads', 0) == 0:
                raise Refused('SINCE_REQUIRED_FOR_INITIAL_IMPORT')
        if is_telegram:
            return self.start(
                owner, 'ingest', source_id, entry_override=entry, dry_run=dry_run,
            )
        return self.start_external(
            owner, 'ingest', source_id, entry, fingerprint,
            since=since, limit=limit, dry_run=dry_run,
        )

    def managed_embed(self, owner, source_id, project):
        entry, fingerprint, _parsed = self._managed_source_entry(owner, source_id, project)
        if telegram_topic_source(source_id) is not None:
            return self.start(owner, 'embed', source_id, entry_override=entry)
        return self.start_external(owner, 'embed', source_id, entry, fingerprint)

    def managed_search(self, owner, source_id, project, query, limit_text=None):
        entry, _fingerprint, _parsed = self._managed_source_entry(owner, source_id, project)
        return self.source_search(
            owner, source_id, query, limit_text, entry_override=entry,
        )

    def project_search(self, owner, project, query, limit_text=None):
        if not isinstance(project, str) or SLUG_RE.fullmatch(project) is None:
            raise Refused('INVALID_PROJECT')
        if not self._project_granted(owner, project):
            raise Refused('PROJECT_NOT_ALLOWED')
        if not isinstance(query, str) or not query.strip() or len(query) > SEARCH_MAX_QUERY_CHARS:
            raise Refused('INVALID_QUERY')
        if any(ord(char) < 32 or ord(char) == 127 for char in query) or query.lstrip().startswith('-'):
            raise Refused('INVALID_QUERY')
        if limit_text is None:
            limit = 5
        elif isinstance(limit_text, str) and re.fullmatch(r'(?:[1-9][0-9]?|100)', limit_text):
            limit = int(limit_text)
        else:
            raise Refused('INVALID_LIMIT')
        if limit > SEARCH_MAX_LIMIT:
            raise Refused('INVALID_LIMIT')
        nonce = str(uuid.uuid4())
        source_key = f'project:{project}'
        request_id = str(uuid.uuid4())
        fingerprint = hashlib.sha256(json.dumps(
            {'cache_version': SEARCH_CACHE_VERSION, 'project': project,
             'query': query.strip(), 'limit': limit, 'request': nonce},
            sort_keys=True, ensure_ascii=False, separators=(',', ':'),
        ).encode()).hexdigest()
        argv = ['search', query.strip(), '--project', project, '--limit', str(limit), '--json']
        now = timestamp()
        with self.db:
            self.db.execute(
                'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) '
                'VALUES(?,?,?,?,?,?,?,?)',
                (request_id, owner, source_key, fingerprint, 'project-search',
                 json.dumps(argv), now, now),
            )
            self.db.execute(
                """INSERT INTO project_search_scope
                   (request_id,project,query_sha256,result_limit,request_fingerprint)
                   VALUES(?,?,?,?,?)""",
                (request_id, project, hashlib.sha256(query.strip().encode()).hexdigest(),
                 limit, fingerprint),
            )
        row = self.row(owner, request_id)
        dispatched = self.dispatch(row)
        job_id = dispatched['job_id']
        deadline = time.monotonic() + SEARCH_WAIT_SECONDS
        while True:
            state = self.worker_api('status', job_id=job_id).get('state')
            if state == 'done':
                result = self.worker_api('result', job_id=job_id)
                if not isinstance(result, dict) or not isinstance(result.get('results'), list):
                    raise Refused('SEARCH_RESULT_INVALID')
                mode, degraded = result.get('mode'), result.get('degraded')
                if mode not in {'hybrid', 'fts'} or type(degraded) is not bool:
                    raise Refused('SEARCH_RESULT_INVALID')
                results = []
                for item in result['results'][:limit]:
                    if not isinstance(item, dict):
                        raise Refused('SEARCH_RESULT_INVALID')
                    text = item.get('text') if isinstance(item.get('text'), str) else ''
                    row_data = {key: item.get(key) for key in (
                        'source_id', 'chat_id', 'chat_title', 'topic_id', 'topic_title',
                        'message_id', 'author', 'score', 'thread_ref', 'item_ref',
                        'item_kind', 'url', 'state', 'labels',
                    )}
                    row_data.update({
                        'date': item.get('posted_at'), 'text': text[:1200],
                        'text_truncated': len(text) > 1200,
                    })
                    results.append(row_data)
                with self.db:
                    self.db.execute(
                        "UPDATE requests SET state='done',counts=?,updated=? WHERE id=?",
                        (json.dumps({'result_count': len(results)}, sort_keys=True), timestamp(), request_id),
                    )
                return {'request_id': request_id, 'job_id': job_id, 'project': project,
                        'mode': mode, 'degraded': degraded,
                        'degraded_reason': result.get('degraded_reason'),
                        'result_count': len(results), 'results': results}
            if state in {'failed', 'timed_out', 'cancelled'}:
                with self.db:
                    self.db.execute(
                        'UPDATE requests SET state=?,error_code=?,updated=? WHERE id=?',
                        (state, 'SEARCH_FAILED', timestamp(), request_id),
                    )
                return {'request_id': request_id, 'job_id': job_id, 'project': project,
                        'status': state, 'error_code': 'SEARCH_FAILED'}
            if time.monotonic() >= deadline:
                return {'request_id': request_id, 'job_id': job_id, 'project': project,
                        'status': state or 'running', 'error_code': 'SEARCH_PENDING'}
            time.sleep(0.5)

    def _validate_project_search_retry(self, row, argv, project):
        if (not isinstance(project, str) or SLUG_RE.fullmatch(project) is None
                or row['source'] != f'project:{project}'
                or not isinstance(argv, list) or len(argv) != 7
                or argv[0] != 'search' or argv[2:5] != ['--project', project, '--limit']
                or argv[6] != '--json'):
            raise Refused('SOURCE_REVISION_CHANGED')
        query = argv[1]
        limit_text = argv[5]
        if (not isinstance(query, str) or not query or query != query.strip()
                or len(query) > SEARCH_MAX_QUERY_CHARS
                or any(ord(char) < 32 or ord(char) == 127 for char in query)
                or query.lstrip().startswith('-') or not isinstance(limit_text, str)
                or re.fullmatch(r'(?:[1-9][0-9]?|50)', limit_text) is None):
            raise Refused('SOURCE_REVISION_CHANGED')
        scope = self.db.execute(
            'SELECT * FROM project_search_scope WHERE request_id=?', (row['id'],),
        ).fetchone()
        if (scope is None or scope['project'] != project
                or scope['query_sha256'] != hashlib.sha256(query.encode()).hexdigest()
                or scope['result_limit'] != int(limit_text)
                or scope['request_fingerprint'] != row['fingerprint']):
            raise Refused('SOURCE_REVISION_CHANGED')

    def source_search(self, owner, source_id, query, limit_text=None, *, entry_override=None):
        if entry_override is None:
            entry, _source_fingerprint, _chat, _topic = self.source(source_id)
        else:
            if parse_source_id(source_id) is None and telegram_topic_source(source_id) is None:
                raise Refused('UNSUPPORTED_SOURCE')
            entry = entry_override
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
        matching = self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND operation='search' "
            "AND (fingerprint=? OR fingerprint LIKE ?) ORDER BY created DESC",
            (owner, source_id, fingerprint, f'{fingerprint}:%'),
        ).fetchall()
        row = None
        for candidate in matching:
            if candidate['state'] in TERMINAL:
                continue
            if self.status(candidate)['status'] not in TERMINAL:
                row = candidate
                break
        if row is None:
            generation = self.db.execute(
                "SELECT count(*) FROM requests WHERE owner=? AND source=? AND operation='search' "
                "AND (fingerprint=? OR fingerprint LIKE ?)",
                (owner, source_id, fingerprint, f'{fingerprint}:%'),
            ).fetchone()[0]
            request_fingerprint = fingerprint if generation == 0 else f'{fingerprint}:{generation}'
            argv = ['search', normalized, '--project', entry['project'], '--limit', str(limit),
                    '--source-kind', source_id.split(':', 1)[0] if external else 'telegram',
                    '--source-id', source_id, '--json']
            now = timestamp()
            request_id = str(uuid.uuid4())
            with self.db:
                self.db.execute(
                    'INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                    (request_id, owner, source_id, request_fingerprint, 'search', json.dumps(argv), now, now),
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

    def search_result(self, owner, request_id):
        row = self.row(owner, identifier(request_id))
        if row['operation'] not in {'search', 'project-search'}:
            raise Refused('SEARCH_REQUEST_NOT_FOUND')
        project = self._require_request_project(owner, row)
        attempt = self.db.execute(
            'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
            (row['id'], row['attempt']),
        ).fetchone()
        if attempt is None:
            current = self.status(row)['status']
            if current == 'cancelled':
                return {'request_id': row['id'], 'job_id': None, 'status': 'cancelled',
                        'error_code': None}
            return {'request_id': row['id'], 'job_id': None, 'status': 'submitting',
                    'error_code': 'SEARCH_PENDING'}

        job_id = attempt['job_id']
        try:
            status = self.worker_api('status', job_id=job_id)
        except JobsError:
            raise Refused('WORKER_UNAVAILABLE') from None
        state = status.get('state') if isinstance(status, dict) else None
        if state not in TERMINAL | ACTIVE:
            raise Refused('SEARCH_STATUS_INVALID')
        if state != 'done':
            if state in TERMINAL:
                with self.db:
                    self.db.execute(
                        'UPDATE requests SET state=?,error_code=?,updated=? WHERE id=? AND owner=?',
                        (state, 'SEARCH_FAILED', timestamp(), row['id'], owner),
                    )
                return {'request_id': row['id'], 'job_id': job_id, 'project': project,
                        'status': state, 'error_code': 'SEARCH_FAILED'}
            return {'request_id': row['id'], 'job_id': job_id, 'project': project,
                    'status': state, 'error_code': 'SEARCH_PENDING'}

        try:
            result = self.worker_api('result', job_id=job_id)
        except JobsError:
            raise Refused('WORKER_UNAVAILABLE') from None
        if (not isinstance(result, dict) or not isinstance(result.get('results'), list)
                or result.get('mode') not in {'hybrid', 'fts'}
                or type(result.get('degraded')) is not bool):
            raise Refused('SEARCH_RESULT_INVALID')
        argv = json.loads(row['argv'])
        if not isinstance(argv, list):
            raise Refused('SEARCH_RESULT_INVALID')
        limit = 5
        try:
            limit_index = argv.index('--limit')
        except ValueError:
            limit_index = None
        if limit_index is not None:
            try:
                limit = int(argv[limit_index + 1])
            except (IndexError, TypeError, ValueError):
                raise Refused('SEARCH_RESULT_INVALID') from None
        if not 1 <= limit <= SEARCH_MAX_LIMIT:
            raise Refused('SEARCH_RESULT_INVALID')
        expected_source = row['source'] if row['operation'] == 'search' else None
        safe_results = []
        for item in result['results'][:limit]:
            if not isinstance(item, dict):
                raise Refused('SEARCH_RESULT_INVALID')
            if expected_source is not None and item.get('source_id') != expected_source:
                raise Refused('SEARCH_RESULT_INVALID')
            safe_item = {key: item.get(key) for key in (
                'source_id', 'chat_id', 'chat_title', 'topic_id', 'topic_title',
                'message_id', 'author', 'score', 'thread_ref', 'item_ref',
                'item_kind', 'url', 'state', 'labels',
            )}
            text_value = item.get('text') if isinstance(item.get('text'), str) else ''
            safe_item.update({
                'date': item.get('posted_at'), 'text': text_value[:1200],
                'text_truncated': len(text_value) > 1200,
            })
            safe_results.append(safe_item)
        degraded_reason = result.get('degraded_reason')
        if degraded_reason not in {
            None, 'quota', 'worker_unavailable', 'worker_busy', 'timeout',
            'provider_unavailable',
        }:
            degraded_reason = None
        with self.db:
            self.db.execute(
                "UPDATE requests SET state='done',counts=?,error_code=NULL,updated=? "
                'WHERE id=? AND owner=?',
                (json.dumps({'result_count': len(safe_results)}, sort_keys=True),
                 timestamp(), row['id'], owner),
            )
        response = {
            'request_id': row['id'], 'job_id': job_id, 'status': 'done',
            'mode': result['mode'], 'degraded': result['degraded'],
            'degraded_reason': degraded_reason, 'result_count': len(safe_results),
            'results': safe_results,
        }
        if row['operation'] == 'search':
            response['source_id'] = row['source']
        else:
            response['project'] = project
        return response

    def renew_embed(self, owner, request_id):
        previous = self.row(owner, request_id)
        if previous['operation'] != 'embed':
            raise Refused('RENEW_EMBED_NOT_ALLOWED')
        self._require_request_project(owner, previous)
        prior_renewal = self.db.execute(
            'SELECT * FROM embed_renewals WHERE previous_request_id=?', (request_id,),
        ).fetchone()
        if prior_renewal is not None:
            return self.renewal_result(owner, prior_renewal)
        status = self.status(previous)
        if (status['status'] != 'failed' or status['attempts_remaining'] != 0
                or status['error_code'] not in RETRYABLE_ERRORS):
            raise Refused('RENEW_EMBED_NOT_ALLOWED')
        old_base = self.db.execute(
            'SELECT base_fingerprint FROM embed_renewals WHERE request_id=?', (request_id,),
        ).fetchone()
        expected_fingerprint = (
            old_base[0] if old_base else self._base_fingerprint(previous['fingerprint'])
        )
        _entry, fingerprint, _chat, _topic = self._embed_source_for_request(
            owner, previous, expected_fingerprint,
        )
        for row in self.db.execute(
            "SELECT * FROM requests WHERE owner=? AND source=? AND operation='embed' AND id<>?",
            (owner, previous['source'], request_id),
        ).fetchall():
            if self.status(row)['status'] not in TERMINAL:
                raise Refused('SOURCE_BUSY')
        previous_job = self.db.execute(
            'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
            (request_id, previous['attempt']),
        ).fetchone()
        if previous_job is None:
            raise Refused('RENEW_EMBED_NOT_ALLOWED')
        previous_job = previous_job[0]
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
        previous = self.row(owner, link['previous_request_id'])
        row = self.row(owner, link['request_id'])
        if (previous['operation'] != 'embed' or row['operation'] != 'embed'
                or row['source'] != previous['source'] or row['argv'] != previous['argv']):
            raise Refused('RENEWAL_SCOPE_INVALID')
        self._embed_source_for_request(owner, previous, link['base_fingerprint'])
        self._embed_source_for_request(owner, row, link['base_fingerprint'])
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
            if selection['source_id'] not in self.sources:
                registered = self.db.execute(
                    'SELECT 1 FROM managed_sources WHERE owner=? AND source=? LIMIT 1',
                    (owner, selection['source_id']),
                ).fetchone()
                if registered is None:
                    raise Refused('SOURCE_NOT_ALLOWED')
        if 'status' in selection and selection['status'] not in TERMINAL | ACTIVE:
            raise Refused('INVALID_STATUS')
        rows = self.db.execute(
            'SELECT * FROM requests WHERE owner=? AND (? IS NULL OR source=?) ORDER BY created DESC',
            (owner, selection.get('source_id'), selection.get('source_id')),
        )
        results = []
        for row in rows:
            try:
                self._require_request_project(owner, row)
            except Refused:
                continue
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
            manifest_limit = _manifest_media_limit(controls['manifest.json'])
            downloads = media_items(controls['messages.jsonl'], manifest_limit)
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
            over_100_mib = set()
            for row in source_rows:
                media = row.get('media')
                if not media:
                    continue
                mid = row['message_id']
                if media.get('reason') in {'over_100_MiB', 'over_limit'}:
                    over.add(mid)
                if media.get('reason') == 'over_100_MiB':
                    over_100_mib.add(mid)
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
                 'recovered': len(recovered), 'missing': len(missing),
                 'over_100_MiB': len(over_100_mib), 'over_limit': len(over),
                 'missing_message_ids': sorted(missing)[:1000],
                 'over_100_MiB_message_ids': sorted(over_100_mib)[:1000],
                 'over_limit_message_ids': sorted(over)[:1000],
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
            try:
                saved_argv = json.loads(row['argv'])
            except (TypeError, json.JSONDecodeError):
                continue
            if self._argv_project(saved_argv) != entry['project']:
                continue
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
        source_id = next((value for value in candidates
                          if value in self.sources or parse_source_id(value) is not None), None)
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
        if command == 'capabilities' and not args:
            return self.capabilities()
        if command == 'project-list' and not args:
            return self.project_list(owner)
        if command == 'project-create' and len(args) == 2:
            return self.project_create(owner, args[0], args[1])
        if command == 'source-list' and len(args) <= 1:
            return self.source_list(owner, args[0] if args else None)
        if command == 'source-add' and len(args) == 2:
            return self.source_add(owner, args[0], args[1])
        if command == 'source-status' and len(args) == 2:
            entry, _fingerprint, _parsed = self._managed_source_entry(owner, args[0], args[1])
            return self.external_source_status(owner, args[0], entry, command)
        if command == 'ingest' and len(args) in {2, 3}:
            return self.managed_ingest(owner, args[0], args[1], since=args[2] if len(args) == 3 else None)
        if command == 'ingest-dry-run' and len(args) in {2, 3, 4}:
            limit = None
            since = args[2] if len(args) >= 3 else None
            if len(args) == 4:
                if not re.fullmatch(r'(?:[1-9][0-9]?|100)', args[3]):
                    raise Refused('INVALID_LIMIT')
                limit = int(args[3])
            return self.managed_ingest(owner, args[0], args[1], since=since,
                                       limit=limit, dry_run=True)
        if command == 'embed' and len(args) == 2:
            return self.managed_embed(owner, args[0], args[1])
        if command == 'search-source' and len(args) in {3, 4}:
            return self.managed_search(owner, args[0], args[1], args[2], args[3] if len(args) == 4 else None)
        if command == 'search-project' and len(args) in {2, 3, 4}:
            if len(args) == 4:
                query = ' '.join(args[1:3])
                limit_text = args[3]
            else:
                query = args[1]
                limit_text = args[2] if len(args) == 3 else None
            return self.project_search(owner, args[0], query, limit_text)
        if command == 'job-status' and len(args) == 1:
            return self.safe_job_status(owner, args[0])
        if command == 'search-result' and len(args) == 1:
            return self.search_result(owner, args[0])
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
            row = self.row(owner, identifier(args[0]))
            self._require_request_project(owner, row)
            return self.job_logs(row, int(n))
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
        if command not in ('retry', 'cancel'):
            raise Refused('COMMAND_NOT_ALLOWED')
        row = self.row(owner, identifier(value))
        self._require_request_project(owner, row)
        status = self.status(row)
        if command == 'cancel':
            attempt = self.db.execute(
                'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                (row['id'], row['attempt']),
            ).fetchone()
            if (status['status'] not in {'queued', 'running', 'waiting_quota', 'waiting_retry'}
                    and not (attempt is None and row['state'] in {'queued', 'submitting'})):
                raise Refused('CANCEL_NOT_ALLOWED')
            if attempt is None:
                try:
                    cancelled = self.worker_api(
                        'cancel-idempotency',
                        idempotency_key=f"broker:{row['id']}:{row['attempt']}",
                    )
                except JobsError:
                    raise Refused('CANCEL_UNCONFIRMED') from None
                if not isinstance(cancelled, dict):
                    raise Refused('CANCEL_UNCONFIRMED')
                if (cancelled.get('found') is False and cancelled.get('job_id') is None
                        and cancelled.get('state') == 'cancelled'
                        and cancelled.get('cancel_requested') is False):
                    with self.db:
                        self.db.execute(
                            "UPDATE requests SET state='cancelled',updated=? WHERE id=? AND owner=?",
                            (timestamp(), row['id'], owner),
                        )
                    return {'request_id': row['id'], 'job_id': None, 'status': 'cancelled',
                            'cancel_requested': False}
                job_id = cancelled.get('job_id')
                state = cancelled.get('state')
                cancel_requested = cancelled.get('cancel_requested')
                if (cancelled.get('found') is not True or not isinstance(job_id, str)
                        or state not in {'queued', 'running', 'waiting_quota', 'waiting_retry',
                                         'done', 'failed', 'timed_out', 'cancelled'}
                        or type(cancel_requested) is not bool):
                    raise Refused('CANCEL_UNCONFIRMED')
                job_id = identifier(job_id)
                with self.db:
                    bound = self.db.execute(
                        'SELECT job_id FROM attempts WHERE request_id=? AND attempt=?',
                        (row['id'], row['attempt']),
                    ).fetchone()
                    if bound is not None and bound['job_id'] != job_id:
                        raise Refused('CANCEL_UNCONFIRMED')
                    self.db.execute(
                        'INSERT OR IGNORE INTO attempts(request_id,attempt,job_id,bound) VALUES(?,?,?,?)',
                        (row['id'], row['attempt'], job_id, timestamp()),
                    )
                    self.db.execute(
                        'UPDATE requests SET state=?,updated=? WHERE id=? AND owner=?',
                        (state, timestamp(), row['id'], owner),
                    )
                return {'request_id': row['id'], 'job_id': job_id, 'status': state,
                        'cancel_requested': cancel_requested}
            cancelled = self.worker_api('cancel', job_id=attempt['job_id'])
            if (not isinstance(cancelled, dict)
                    or cancelled.get('state') not in {
                        'queued', 'running', 'waiting_quota', 'waiting_retry',
                        'done', 'failed', 'timed_out', 'cancelled',
                    }
                    or type(cancelled.get('cancel_requested')) is not bool):
                raise Refused('CANCEL_UNCONFIRMED')
            return {'request_id': row['id'], 'job_id': attempt['job_id'],
                    'status': cancelled['state'],
                    'cancel_requested': cancelled['cancel_requested'] is True}
        if status['status'] in ACTIVE:
            if row['operation'] == 'embed':
                renewal = self.db.execute(
                    'SELECT base_fingerprint FROM embed_renewals WHERE request_id=?',
                    (row['id'],),
                ).fetchone()
                expected_fingerprint = renewal[0] if renewal else row['fingerprint']
                self._embed_source_for_request(owner, row, expected_fingerprint)
            return self.dispatch(row)
        if row['attempt'] >= MAX_ATTEMPTS:
            raise Refused('RETRY_LIMIT')
        if not status['retryable']:
            raise Refused('RETRY_NOT_ALLOWED')
        saved_argv = json.loads(row['argv'])
        project = self._argv_project(saved_argv)
        operation = row['operation']
        renewal = self.db.execute(
            'SELECT base_fingerprint FROM embed_renewals WHERE request_id=?', (row['id'],),
        ).fetchone()
        expected_fingerprint = renewal[0] if renewal else row['fingerprint']
        if operation == 'project-search':
            entry, fingerprint, chat, topic = {'project': project}, None, None, None
            candidates = []
        else:
            candidates = self._retry_source_candidates(owner, row['source'], project)
            if not candidates:
                raise Refused('SOURCE_REVISION_CHANGED')
            if operation in {'asr_pilot', 'asr_full', 'asr_embed'}:
                candidates = [
                    candidate for candidate in candidates
                    if candidate[0].get('asr_enabled') is True
                ]
                if not candidates:
                    raise Refused('ASR_NOT_ENABLED')
            entry, fingerprint, chat, topic = candidates[0]
        if operation == 'ingest' and chat is None:
            # External reruns add a uniqueness suffix to the request fingerprint;
            # validate the source revision against its unsuffixed base.
            selected = next((candidate for candidate in candidates
                             if candidate[2] is None and
                             self._base_fingerprint(expected_fingerprint) in
                             (self._source_fingerprint_variants(row['source'], candidate[0])
                              | {candidate[1]})), None)
            if selected is None:
                raise Refused('SOURCE_REVISION_CHANGED')
            entry, fingerprint, chat, topic = selected
        elif operation == 'ingest':
            selected = None
            revision = ''
            for candidate in candidates:
                candidate_entry, candidate_fingerprint, candidate_chat, candidate_topic = candidate
                if candidate_chat is None:
                    continue
                candidate_revision = media_revision(
                    self.root, candidate_chat, candidate_topic,
                )
                current = (hashlib.sha256(
                    (candidate_fingerprint + candidate_revision).encode()
                ).hexdigest() if candidate_revision else candidate_fingerprint)
                if '--dry-run' in saved_argv:
                    current = f'{current}:dry-run'
                if current == expected_fingerprint:
                    selected = candidate
                    revision = candidate_revision
                    break
            if selected is None:
                raise Refused('SOURCE_REVISION_CHANGED')
            entry, fingerprint, chat, topic = selected
            self.stage(row['source'], entry, fingerprint, chat, topic, revision)
        elif operation in {'asr_pilot', 'asr_full'}:
            meta = self.db.execute(
                'SELECT input_fingerprint FROM asr_request_meta WHERE request_id=?',
                (row['id'],),
            ).fetchone()
            if meta is None:
                raise Refused('ASR_REQUEST_METADATA_MISSING')
            selected = None
            for candidate in candidates:
                candidate_entry, candidate_fingerprint, candidate_chat, candidate_topic = candidate
                revision = media_revision(self.root, candidate_chat, candidate_topic)
                staged = self.stage(
                    row['source'], candidate_entry, candidate_fingerprint,
                    candidate_chat, candidate_topic, revision,
                )
                plan, _workspace = self.asr_workspace(
                    row['source'], candidate_entry, staged,
                    candidate_fingerprint, revision,
                )
                if plan['input_fingerprint'] == meta[0]:
                    selected = candidate
                    break
            if selected is None:
                raise Refused('SOURCE_REVISION_CHANGED')
            entry, fingerprint, chat, topic = selected
        elif operation == 'asr_embed':
            overview = self.asr_overview(row['source'])
            selected = next((candidate for candidate in candidates if hashlib.sha256(json.dumps(
                {'source_id': row['source'], 'source_fingerprint': candidate[1],
                 'asr_revision': overview.get('revision'), 'project': candidate[0]['project']},
                sort_keys=True, separators=(',', ':'),
            ).encode()).hexdigest() == expected_fingerprint), None)
            if selected is None:
                raise Refused('SOURCE_REVISION_CHANGED')
            entry, fingerprint, chat, topic = selected
        elif operation == 'project-search':
            self._validate_project_search_retry(row, saved_argv, project)
        elif operation == 'search':
            argv = json.loads(row['argv'])
            parsed_source = parse_source_id(row['source'])
            source_kind = (
                parsed_source.provider if parsed_source is not None
                else 'telegram' if telegram_topic_source(row['source']) is not None
                else None
            )
            if (not isinstance(argv, list) or len(argv) != 11 or argv[0] != 'search'
                    or source_kind is None
                    or not isinstance(project, str) or entry.get('project') != project
                    or argv[2:4] != ['--project', project]
                    or argv[4] != '--limit' or argv[6:8] != ['--source-kind', source_kind]
                    or argv[8:10] != ['--source-id', row['source']] or argv[10] != '--json'):
                raise Refused('SOURCE_REVISION_CHANGED')
            query = argv[1]
            if (not isinstance(query, str) or not query or query != query.strip()
                    or len(query) > SEARCH_MAX_QUERY_CHARS
                    or any(ord(char) < 32 or ord(char) == 127 for char in query)
                    or query.lstrip().startswith('-')):
                raise Refused('SOURCE_REVISION_CHANGED')
            try:
                limit = int(argv[5])
            except (TypeError, ValueError):
                raise Refused('SOURCE_REVISION_CHANGED') from None
            if not 1 <= limit <= SEARCH_MAX_LIMIT or str(limit) != argv[5]:
                raise Refused('SOURCE_REVISION_CHANGED')
            current = search_fingerprint(
                row['source'], project, query, limit
            )
            if self._base_fingerprint(expected_fingerprint) != current:
                raise Refused('SOURCE_REVISION_CHANGED')
        elif operation == 'embed':
            entry, fingerprint, chat, topic = self._embed_source_for_request(
                owner, row, expected_fingerprint,
            )
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
            if not isinstance(tokens, list) or not 1 <= len(tokens) <= 5 or not all(
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
            authorized_projects, allow_project_create = read_project_policy(PROJECT_ACCESS)
            server.broker = Broker(
                ROOT, registry(CONFIG), authorized_projects=authorized_projects,
                allow_project_create=allow_project_create,
            )
            SOCKET.chmod(0o660)
            server.serve_forever()


if __name__ == '__main__':
    main()
