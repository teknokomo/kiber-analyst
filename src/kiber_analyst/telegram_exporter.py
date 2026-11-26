"""Restricted resumable export of Telegram messages and service events."""
import datetime
import fcntl
import hashlib
import json
import os
import re
import resource
import selectors
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from kiber_analyst.media_limits import MAX_TELEGRAM_EXPORT_BYTES
from kiber_analyst.telegram_source import (
    format_telegram_source,
    telegram_scope_directory,
)

DEFAULT_FILE_BYTES = 100 * 1024**2
MAX_FILE_BYTES = 4_000_000_000  # 4000 decimal MB
MAX_FILE = DEFAULT_FILE_BYTES
PIPELINE_MEDIA_COPIES = 3  # Export, published package, and Kiber worker staging.
GIB = 1024**3
DEFAULT_RESUME_BATCH = 25
MAX_CLI_STDOUT_BYTES = 32 * 1024**2
MAX_CLI_STDERR_BYTES = 256 * 1024
MAX_JSONL_LINE_BYTES = 4 * 1024**2
MAX_EXPORT_BYTES = MAX_TELEGRAM_EXPORT_BYTES
MAX_SKIPPED_MEDIA_SAMPLE = 100


class ExportError(ValueError):
    def __init__(self, message, *, error_code=None):
        super().__init__(message)
        self.error_code = error_code


def numeric(value, maximum=2147483647):
    if not re.fullmatch(r'[0-9]+', value) or not 1 <= int(value) <= maximum:
        raise ExportError('Invalid numeric argument')
    return int(value)


def chat_arg(value):
    if not re.fullmatch(r'-?[1-9][0-9]{0,18}|@[A-Za-z0-9_]{5,32}|me', value):
        raise ExportError('Invalid chat')
    return value


def _file_limit_bytes(value=None):
    if value is None:
        return DEFAULT_FILE_BYTES
    match = re.fullmatch(r'([1-9][0-9]*)(MB|MIB|B)?', value, re.IGNORECASE)
    if match is None:
        raise ExportError('Use a positive size with MB, MiB, or B; a bare integer means MiB')
    amount = int(match.group(1))
    unit = (match.group(2) or 'MiB').upper()
    multiplier = 1_000_000 if unit == 'MB' else 1 if unit == 'B' else 1024**2
    result = amount * multiplier
    if not 0 < result <= MAX_FILE_BYTES:
        raise ExportError('File size limit must be between 1 byte and 4000 MB')
    return result


def _is_explicit_size(value):
    return isinstance(value, str) and re.fullmatch(r'[1-9][0-9]*(?:MB|MIB|B)', value, re.IGNORECASE) is not None


def parse(args):
    if len(args) < 3 or args[0] not in {
        'export', 'export-status', 'export-services', 'export-resume',
        'export-transfer', 'export-transfer-status',
    }:
        raise ExportError('Use export CHAT TOPIC [MEDIA] [LIMIT] [BEFORE] [MAX_FILE_SIZE], export-resume CHAT TOPIC [MAX_FILE_SIZE] [BATCH], export-services CHAT TOPIC [LIMIT] [BEFORE], export-status CHAT TOPIC, or export-transfer[ -status] CHAT TOPIC')
    cmd, chat, topic, *rest = args
    chat_arg(chat)
    topic = None if topic.upper() == 'GROUP' else numeric(topic)
    if cmd in {'export-transfer', 'export-transfer-status'}:
        if rest or not re.fullmatch(r'-?[1-9][0-9]{0,18}', chat):
            raise ExportError('Transfer requires a numeric chat ID and topic or GROUP')
        return cmd, chat, topic, False, 0, None, MAX_FILE
    if cmd == 'export-resume':
        if not re.fullmatch(r'-?[1-9][0-9]{0,18}', chat) or len(rest) > 2:
            raise ExportError('Use export-resume NUMERIC_CHAT TOPIC|GROUP [MAX_FILE_SIZE] [BATCH]')
        max_file_bytes = _file_limit_bytes(rest[0] if rest else None)
        batch = numeric(rest[1], 100) if len(rest) > 1 else DEFAULT_RESUME_BATCH
        return cmd, chat, topic, False, batch, None, max_file_bytes
    if cmd == 'export-status':
        if rest:
            raise ExportError('Unexpected status argument')
        return cmd, chat, topic, False, 100, None, MAX_FILE
    if cmd == 'export-services':
        if len(rest) > 2:
            raise ExportError('Unexpected export-services argument')
        limit = numeric(rest[0], maximum=1000) if rest else 100
        before = numeric(rest[1]) if len(rest) == 2 else None
        return cmd, chat, topic, False, limit, before, MAX_FILE
    media = bool(rest and rest[0] == 'MEDIA')
    if media:
        rest.pop(0)
    if len(rest) > 3:
        raise ExportError('Unexpected export argument')
    before = None
    max_file_value = None
    if len(rest) == 1 and _is_explicit_size(rest[0]):
        limit, max_file_value = 100, rest[0]
    else:
        limit = numeric(rest[0], maximum=1000) if rest else 100
        tail = rest[1:]
        if tail and _is_explicit_size(tail[0]):
            if len(tail) != 1:
                raise ExportError('Unexpected export argument')
            max_file_value = tail[0]
        else:
            before = numeric(tail[0]) if tail else None
            max_file_value = tail[1] if len(tail) == 2 else None
            if len(tail) > 2:
                raise ExportError('Unexpected export argument')
    max_file_bytes = _file_limit_bytes(max_file_value)
    return cmd, chat, topic, media, limit, before, max_file_bytes


class Dir:
    """Keep open directory descriptors; never resolve then reopen an input path."""
    def __init__(self, fd):
        self.fd = fd

    @classmethod
    def root(cls, path):
        fd = os.open('/', os.O_PATH | os.O_DIRECTORY)
        try:
            parts = Path(path).parts[1:]
            for index, part in enumerate(parts):
                access = os.O_RDONLY if index == len(parts) - 1 else os.O_PATH
                new = os.open(part, access | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = new
            return cls(fd)
        except BaseException:
            os.close(fd)
            raise

    def close(self):
        os.close(self.fd)

    def child(self, name, create=False):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or name in {'.', '..'}:
            raise ExportError('Unsafe directory component')
        if create:
            try:
                os.mkdir(name, 0o700, dir_fd=self.fd)
            except FileExistsError:
                pass
        return Dir(os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.fd))

    def names(self):
        return os.listdir(self.fd)

    @staticmethod
    def filename(name):
        if not isinstance(name, str) or name in {'', '.', '..'} or '/' in name or '\\' in name or '\x00' in name:
            raise ExportError('Unsafe filename')
        return name

    def read(self, name, maximum=32*1024**2):
        self.filename(name)
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.fd)
        with os.fdopen(fd, 'rb') as stream:
            s = os.fstat(stream.fileno())
            if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_size > maximum:
                raise ExportError('Unsafe or oversized input file')
            return stream.read(maximum + 1)

    def digest(self, name, maximum):
        """Hash a regular file in bounded memory without following links."""
        self.filename(name)
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.fd)
        try:
            before = os.fstat(fd)
            if before.st_size > maximum:
                raise MediaLimitExceeded('Media exceeds the selected file size limit')
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_size <= 0):
                raise ExportError('Unsafe or oversized media file')
            digest = hashlib.sha256()
            total = 0
            while chunk := os.read(fd, 1024 * 1024):
                total += len(chunk)
                if total > maximum:
                    raise ExportError('Media exceeds its size limit')
                digest.update(chunk)
            after = os.fstat(fd)
            if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    or total != before.st_size):
                raise ExportError('Media changed while being checked')
            return total, digest.hexdigest()
        finally:
            os.close(fd)

    def json(self, name):
        return json.loads(self.read(name))

    def atomic(self, name, data):
        self.filename(name)
        temp = '.tmp-' + name
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=self.fd)
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            os.close(fd)
            raise ExportError('Unsafe temporary output')
        os.ftruncate(fd, 0)
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        # Replacing a symlink is safe but forbidden by the interface contract.
        if name in self.names() and stat.S_ISLNK(os.stat(name, dir_fd=self.fd, follow_symlinks=False).st_mode):
            raise ExportError('Symlink destination refused')
        os.rename(temp, name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
        os.fsync(self.fd)

    def atomic_private(self, name, data):
        """Publish one 0600 regular file without following any path component."""
        self.filename(name)
        if name in self.names():
            existing = os.stat(name, dir_fd=self.fd, follow_symlinks=False)
            if not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1:
                raise ExportError('Unsafe service output')
        temp = '.tmp-services-' + uuid.uuid4().hex
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=self.fd)
        try:
            os.fchmod(fd, 0o600)
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise ExportError('Unsafe temporary service output')
            with os.fdopen(fd, 'wb') as stream:
                fd = -1
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            if name in self.names():
                existing = os.stat(name, dir_fd=self.fd, follow_symlinks=False)
                if not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1:
                    raise ExportError('Unsafe service output')
            os.rename(temp, name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
            os.fsync(self.fd)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temp, dir_fd=self.fd)
            except FileNotFoundError:
                pass

    def private_stream_temp(self, chunks):
        """Write a private streamed candidate and return its name, size, and SHA-256."""
        temp = '.tmp-services-' + uuid.uuid4().hex
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=self.fd)
        digest = hashlib.sha256()
        total = 0
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                fd = -1
                for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise ExportError('Invalid streamed output chunk')
                    stream.write(chunk)
                    digest.update(chunk)
                    total += len(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            os.fsync(self.fd)
            return temp, total, digest.hexdigest()
        except BaseException:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temp, dir_fd=self.fd)
            except FileNotFoundError:
                pass
            raise

    def publish_private_temp(self, temp, name):
        self.filename(name)
        if not re.fullmatch(r'\.tmp-services-[0-9a-f]{32}', temp):
            raise ExportError('Unsafe streamed output temporary')
        info = os.stat(temp, dir_fd=self.fd, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
            raise ExportError('Unsafe streamed output temporary')
        if name in self.names():
            existing = os.stat(name, dir_fd=self.fd, follow_symlinks=False)
            if not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1:
                raise ExportError('Unsafe service output')
        os.rename(temp, name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
        os.fsync(self.fd)

    def write_json(self, name, obj):
        self.atomic(name, (json.dumps(obj, ensure_ascii=False, sort_keys=True) + '\n').encode())


def config(path=None):
    configured_path = path or os.environ.get('KIBER_TELEGRAM_EXPORT_CONFIG')
    if not isinstance(configured_path, str) or not Path(configured_path).is_absolute():
        raise ExportError('Set KIBER_TELEGRAM_EXPORT_CONFIG to an absolute config path')
    try:
        fd = os.open(configured_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022):
                raise ExportError('Telegram exporter config is unsafe')
            raw = stream.read(16 * 1024 + 1)
        if len(raw) > 16 * 1024:
            raise ExportError('Telegram exporter config is too large')
        c = json.loads(raw)
    except ExportError:
        raise
    except (OSError, ValueError, UnicodeError):
        raise ExportError('Telegram exporter config is unavailable or invalid') from None
    required = {
        'export_root', 'storage_root', 'telegram_cli', 'telegram_home',
        'publisher_executable', 'publisher_config', 'storage_limit_bytes',
        'min_free_bytes', 'expected_mount_uuid',
    }
    if not isinstance(c, dict) or set(c) != required:
        raise ExportError('Telegram exporter config has unsupported fields')
    for key in ('export_root', 'storage_root', 'telegram_cli', 'telegram_home',
                'publisher_executable', 'publisher_config'):
        if (not isinstance(c[key], str) or not Path(c[key]).is_absolute()
                or '..' in Path(c[key]).parts or '\x00' in c[key]):
            raise ExportError('Telegram exporter config contains an invalid path')
    if (type(c['storage_limit_bytes']) is not int
            or not GIB <= c['storage_limit_bytes'] <= 1024 * GIB
            or type(c['min_free_bytes']) is not int
            or not 0 <= c['min_free_bytes'] < c['storage_limit_bytes']):
        raise ExportError('Telegram storage quota is invalid')
    mount_uuid = c['expected_mount_uuid']
    if mount_uuid is not None and (
        not isinstance(mount_uuid, str)
        or not re.fullmatch(r'[0-9a-fA-F-]{36}', mount_uuid)
    ):
        raise ExportError('Telegram storage mount UUID is invalid')
    return c


def tree_size(directory):
    total = 0
    for name in directory.names():
        s = os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
        if stat.S_ISLNK(s.st_mode):
            raise ExportError('Symlink inside storage refused')
        if stat.S_ISDIR(s.st_mode):
            sub = directory.child(name)
            try:
                total += tree_size(sub)
            finally:
                sub.close()
        elif stat.S_ISREG(s.st_mode) and s.st_nlink == 1:
            total += s.st_size
        else:
            raise ExportError('Non-regular or hard-linked storage entry refused')
    return total


class Budget:
    def __init__(self, root, cfg):
        self.root, self.cfg = root, cfg
        self.reserved_bytes = 0

    def available_bytes(self):
        fs = os.fstatvfs(self.root.fd)
        storage = max(0, self.cfg['storage_limit_bytes'] - tree_size(self.root))
        disk = max(0, fs.f_bavail * fs.f_frsize - self.cfg['min_free_bytes'])
        available = min(storage, disk)
        aggregate = self.cfg.get('aggregate_available_bytes')
        if type(aggregate) is int:
            pipeline_remaining = max(0, aggregate - self.reserved_bytes)
            available = min(available, pipeline_remaining // PIPELINE_MEDIA_COPIES)
        return available

    def consume(self, amount):
        if type(amount) is not int or amount < 0:
            raise ExportError('Invalid storage reservation')
        self.reserved_bytes += amount * PIPELINE_MEDIA_COPIES

    def check(self, extra=0):
        if type(extra) is not int or extra < 0:
            raise ExportError('Storage budget exhausted')
        fs = os.fstatvfs(self.root.fd)
        storage_margin = self.cfg['storage_limit_bytes'] - tree_size(self.root)
        disk_margin = fs.f_bavail * fs.f_frsize - self.cfg['min_free_bytes']
        if storage_margin < extra or disk_margin < extra:
            raise ExportError('Storage budget exhausted')
        aggregate = self.cfg.get('aggregate_available_bytes')
        if type(aggregate) is int:
            pipeline_margin = aggregate - self.reserved_bytes
            if pipeline_margin < extra * PIPELINE_MEDIA_COPIES:
                raise ExportError('Storage budget exhausted')


class MediaLimitExceeded(ExportError):
    """The downloader reached the kernel-enforced attachment size limit."""


class Client:
    def __init__(self, cfg):
        self.cli = cfg['telegram_cli']
        self.home = cfg['telegram_home']
        try:
            info = os.lstat(self.home)
        except OSError:
            raise ExportError('Telegram CLI home is unavailable') from None
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise ExportError('Telegram CLI home must be private mode 0700')
        self.env = {
            'HOME': self.home, 'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
            'TG_READ_ONLY': '1', 'TG_NO_UPDATE_CHECK': '1', 'TG_FLOOD_WAIT_MAX': '60',
        }

    def call(self, argv, output=None, file_limit=MAX_FILE, timeout=300):
        def limits():
            resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))
        process = None

        def stop_child():
            if process is None:
                return
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            # The direct child may already have exited while a descendant still
            # holds stdout/stderr open. Kill the process group even in that case.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                process.wait()

        try:
            process = subprocess.Popen(
                [self.cli, *argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=self.env, cwd=self.home, close_fds=True,
                pass_fds=(output.fd,) if output else (), preexec_fn=limits,
                start_new_session=True,
            )
            buffers = {process.stdout: bytearray(), process.stderr: bytearray()}
            caps = {process.stdout: MAX_CLI_STDOUT_BYTES,
                    process.stderr: MAX_CLI_STDERR_BYTES}
            selector = selectors.DefaultSelector()
            try:
                for pipe in buffers:
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ)
                deadline = time.monotonic() + timeout
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        stop_child()
                        raise ExportError('Telegram CLI timed out')
                    for key, _ in selector.select(min(remaining, 0.25)):
                        pipe = key.fileobj
                        chunk = os.read(pipe.fileno(), 65536)
                        if not chunk:
                            selector.unregister(pipe)
                            pipe.close()
                            continue
                        buffer = buffers[pipe]
                        if len(buffer) + len(chunk) > caps[pipe]:
                            stop_child()
                            raise ExportError('Telegram CLI output limit exceeded')
                        buffer.extend(chunk)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    stop_child()
                    raise ExportError('Telegram CLI timed out')
                return_code = process.wait(timeout=remaining)
            finally:
                selector.close()
                for pipe in buffers:
                    if not pipe.closed:
                        pipe.close()
            stdout = bytes(buffers[process.stdout])
            stderr = bytes(buffers[process.stderr])
        except ExportError:
            stop_child()
            raise
        except (OSError, subprocess.SubprocessError):
            stop_child()
            raise ExportError('Telegram CLI unavailable or timed out') from None
        if return_code:
            if output is not None and (
                return_code == -signal.SIGXFSZ
                or re.search(rb"\bEFBIG\b|file too large", stderr[:16384], re.IGNORECASE)
            ):
                raise MediaLimitExceeded('Media exceeds the file size limit')
            error_code = _safe_cli_error_code(stdout, stderr, return_code)
            raise ExportError(
                f'Telegram CLI failed (exit {return_code}); raw diagnostics withheld',
                error_code=error_code,
            )
        try:
            return json.loads(stdout)
        except ValueError:
            raise ExportError('Invalid Telegram CLI JSON') from None

    def info(self, chat):
        return self.call(['info', chat, '--json'])

    def title(self, chat, topic):
        if topic is None:
            return None
        data = self.call(['messages', 'get-forum-topics-by-id', '--peer', chat, '--topics', str(topic)])
        for item in data['topics']:
            if item['id'] == topic:
                return item['title']
        raise ExportError('Topic ID not found by exact lookup')

    def page(self, chat, topic, limit, before):
        args = ['read', chat, '--limit', str(limit), '--full', '--json']
        if topic not in (None, 1):
            args += ['--topic', str(topic)]
        if before:
            args += ['--before', str(before)]
        while True:
            data = self.call(args)
            if data.get('threadChatId'):
                raise ExportError('Cross-chat discussion thread refused')
            if data['messages']:
                return data['messages']
            # The pinned read command can stop on service-only raw batches.
            params = {'peer': {'_chat': chat}, 'offsetId': before or 0, 'offsetDate': 0,
                      'addOffset': 0, 'limit': limit, 'maxId': 0, 'minId': 0, 'hash': '0'}
            method = 'messages.GetHistory' if topic in (None, 1) else 'messages.GetReplies'
            if topic not in (None, 1):
                params['msgId'] = topic
            raw = self.call(['api', method, '--params', json.dumps(params)])['messages']
            if not raw:
                return []
            if not all(isinstance(m, dict) and m.get('action') and type(m.get('id')) is int for m in raw):
                raise ExportError('Empty formatted page is not proven end-of-history')
            cursor = min(m['id'] for m in raw)
            if cursor <= 0 or (before is not None and cursor >= before):
                raise ExportError('Raw pagination made no progress')
            before = cursor
            args = ['read', chat, '--limit', str(limit), '--full', '--json', '--before', str(cursor)]
            if topic not in (None, 1):
                args += ['--topic', str(topic)]

    def service_page(self, chat, topic, limit, before):
        params = {'peer': {'_chat': chat}, 'offsetId': before or 0, 'offsetDate': 0,
                  'addOffset': 0, 'limit': limit, 'maxId': 0, 'minId': 0, 'hash': '0'}
        method = 'messages.GetHistory' if topic in (None, 1) else 'messages.GetReplies'
        if topic not in (None, 1):
            params['msgId'] = topic
        result = self.call(['api', method, '--params', json.dumps(params)])
        messages = result.get('messages')
        if not isinstance(messages, list) or len(messages) > limit:
            raise ExportError('Invalid Telegram service page shape')
        return messages

    def media_metadata(self, chat, ids):
        params = {'channel': {'_chat': chat},
                  'id': [{'_': 'inputMessageID', 'id': mid} for mid in ids]}
        result = self.call(['api', 'channels.GetMessages', '--params', json.dumps(params)])
        messages = result.get('messages')
        if not isinstance(messages, list):
            raise ExportError('Invalid Telegram media metadata response')
        return {m['id']: m.get('media') for m in messages
                if isinstance(m, dict) and type(m.get('id')) is int}

    def download(self, chat, message, directory, file_limit=MAX_FILE):
        # The pinned client streams to a file. The kernel rejects writes beyond
        # the cap even if Telegram supplies missing or incorrect metadata.
        # At 128 KiB/s the largest allowed attachment needs under nine hours;
        # keep a finite bound while allowing the full 4000 MB ceiling.
        timeout = min(12 * 60 * 60, max(300, 300 + file_limit // (128 * 1024)))
        return self.call(['download', chat, str(message), '--output',
                          '/proc/self/fd/'+str(directory.fd), '--json'],
                         directory, file_limit=file_limit, timeout=timeout)


def belongs(message, topic):
    if topic is None:
        return True
    reply = (message.get('full') or {}).get('replyTo') or {}
    top = reply.get('topMsgId')
    if top is not None:
        return top == topic
    if topic == 1:
        return not reply.get('forumTopic') or reply.get('replyToMsgId') == 1
    # GetReplies itself scopes ordinary topics; explicit conflicting metadata is rejected above.
    return True


def record(message, topic):
    if type(message.get('id')) is not int or message['id'] <= 0:
        raise ExportError('Invalid message ID in page')
    for name in ['date', 'text', 'sender']:
        if not isinstance(message.get(name), str):
            raise ExportError('Missing or invalid message field: '+name)
    actual_topic = topic
    if topic is None:
        reply = (message.get('full') or {}).get('replyTo') or {}
        top = reply.get('topMsgId') if isinstance(reply, dict) else None
        full = message.get('full')
        if top is None and isinstance(full, dict):
            top = full.get('topicId', full.get('topic_id'))
        if top is None:
            top = message.get('topicId', message.get('topic_id'))
        if type(top) is int and top > 0:
            actual_topic = top
        elif isinstance(reply, dict) and (reply.get('forumTopic') or reply.get('replyToMsgId') == 1):
            actual_topic = 1
    return {'message_id': message['id'], 'topic_id': actual_topic, 'date': message['date'],
            'sender_id': message.get('senderId'), 'sender_name': message['sender'],
            'text': message['text'], 'reply_to_message_id': message.get('replyToMsgId'),
            'media': message.get('media')}


def _snake_name(value):
    if not isinstance(value, str):
        return ''
    value = value.rsplit('.', 1)[-1]
    value = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', value)
    value = re.sub(r'[^A-Za-z0-9]+', '_', value).strip('_').lower()
    return value


def _action_name(action):
    if isinstance(action, str):
        return _snake_name(action)
    if isinstance(action, dict):
        for key in ('_', '@type', 'type', 'constructor', 'class'):
            if isinstance(action.get(key), str):
                return _snake_name(action[key])
    return 'unknown_action'


def _first(mapping, *keys):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _identifier(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and re.fullmatch(r'-?[0-9]{1,20}', value):
        return value
    if isinstance(value, dict):
        for key in ('user_id', 'userId', 'channel_id', 'channelId', 'chat_id', 'chatId', 'id'):
            result = _identifier(value.get(key))
            if result is not None:
                return result
    return None


def _event_date(value):
    try:
        if type(value) is int and value > 0:
            return datetime.datetime.fromtimestamp(value, datetime.UTC).isoformat().replace('+00:00', 'Z')
        if isinstance(value, str):
            parsed = datetime.datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(datetime.UTC).isoformat().replace('+00:00', 'Z')
    except (OverflowError, OSError, ValueError):
        pass
    return None


def service_event(message, topic):
    """Normalize one raw Telegram service message without retaining its text."""
    if not isinstance(message, dict) or not message.get('action'):
        return None, set()
    event_id = message.get('id')
    if type(event_id) is not int or event_id <= 0:
        raise ExportError('Invalid Telegram service event ID')
    action = message['action']
    action_name = _action_name(action)
    fields = action if isinstance(action, dict) else {}
    actor_id = _identifier(_first(message, 'fromId', 'from_id', 'senderId', 'sender_id'))
    actor_name = _first(message, 'sender', 'senderName', 'sender_name', 'fromName', 'from_name')
    if not isinstance(actor_name, str):
        actor_name = None
    target_id = target_name = message_id = None
    old_value = new_value = None
    limits = set()

    if actor_id is None:
        limits.add('actor_id_unavailable')
    if actor_name is None:
        limits.add('actor_name_unavailable')
    date = _event_date(_first(message, 'date', 'date_utc'))
    if date is None:
        limits.add('event_date_unavailable')

    action_type = 'unknown'
    if action_name in {'message_action_chat_add_user', 'message_action_chat_joined_by_link',
                       'message_action_chat_joined_by_request'}:
        action_type = 'member_added' if action_name == 'message_action_chat_add_user' else 'member_joined'
        users = fields.get('users')
        if isinstance(users, list):
            user_ids = [value for value in (_identifier(item) for item in users) if value is not None]
            if len(users) == 1 and len(user_ids) == 1:
                target_id = user_ids[0]
            elif len(users) > 1:
                new_value = {'target_ids': user_ids}
                if len(user_ids) != len(users):
                    limits.add('some_target_ids_unavailable')
            elif users:
                limits.add('target_ids_unavailable')
        elif action_name == 'message_action_chat_add_user':
            limits.add('target_ids_unavailable')
        else:
            limits.add('target_id_unavailable')
    elif action_name == 'message_action_chat_delete_user':
        target_id = _identifier(_first(fields, 'user_id', 'userId'))
        if target_id is None:
            limits.add('target_id_unavailable')
        if target_id is not None and actor_id is not None:
            action_type = 'member_left' if actor_id == target_id else 'member_removed'
        else:
            limits.add('member_left_vs_removed_unavailable')
    elif action_name in {'message_action_pin_message', 'message_action_unpin_message'}:
        action_type = 'message_pinned' if action_name.endswith('pin_message') and not action_name.endswith('unpin_message') else 'message_unpinned'
        reply = _first(message, 'replyTo', 'reply_to')
        if isinstance(reply, dict):
            message_id = _first(reply, 'replyToMsgId', 'reply_to_msg_id', 'msg_id')
        if type(message_id) is not int or message_id <= 0:
            message_id = None
            limits.add('pinned_message_id_unavailable')
    elif action_name == 'message_action_topic_create':
        action_type = 'topic_created'
        title = fields.get('title')
        if isinstance(title, str):
            new_value = title
        else:
            limits.add('topic_title_unavailable')
    elif action_name == 'message_action_topic_edit':
        title_present = isinstance(fields.get('title'), str)
        closed_present = type(fields.get('closed')) is bool
        if title_present:
            action_type = 'topic_renamed'
            new_value = {'title': fields['title']}
            if closed_present:
                new_value['closed'] = fields['closed']
            if 'icon_emoji_id' in fields:
                new_value['icon_emoji_id'] = fields['icon_emoji_id']
        elif closed_present:
            action_type = 'topic_closed' if fields['closed'] else 'topic_reopened'
            new_value = {'closed': fields['closed']}
        else:
            limits.add('topic_edit_fields_not_represented')
    elif action_name == 'message_action_chat_edit_title':
        action_type = 'title_changed'
        new_value = fields.get('title') if isinstance(fields.get('title'), str) else None
        if new_value is None:
            limits.add('new_title_unavailable')
    elif action_name in {'message_action_chat_edit_photo', 'message_action_chat_delete_photo'}:
        action_type = 'photo_changed'
        new_value = {'present': action_name == 'message_action_chat_edit_photo'}
    elif action_name in {'message_action_chat_edit_about', 'message_action_chat_edit_description'}:
        action_type = 'description_changed'
        new_value = _first(fields, 'about', 'description')
        if not isinstance(new_value, str):
            new_value = None
            limits.add('new_description_unavailable')
    elif action_name in {'message_action_message_deleted', 'message_action_delete_message'}:
        action_type = 'message_deleted'
    elif action_name in {
        'message_action_chat_edit_admin', 'message_action_chat_edit_banned',
        'message_action_chat_toggle_invites', 'message_action_chat_toggle_signatures',
        'message_action_chat_toggle_pre_history_hidden', 'message_action_chat_toggle_no_forwards',
        'message_action_chat_toggle_forum', 'message_action_chat_toggle_group_call',
    }:
        action_type = 'permissions_changed'
        target_id = _identifier(_first(fields, 'user_id', 'userId'))
        safe_fields = {}
        allowed = {'prev_rights', 'new_rights', 'rights', 'banned_rights', 'invite', 'prehistory',
                   'signatures', 'admins_enabled', 'anyone_can_add_members', 'until_date', 'view_messages',
                   'send_messages', 'send_media', 'send_stickers', 'send_gifs', 'send_games', 'send_inline',
                   'embed_links', 'manage_topics', 'pin_messages', 'change_info', 'post_messages',
                   'edit_messages', 'delete_messages', 'ban_users', 'invite_users', 'add_admins',
                   'anonymous', 'is_admin'}
        for key in allowed:
            if key in fields and isinstance(fields[key], (bool, int, str, list, dict)):
                safe_fields[key] = fields[key]
        new_value = safe_fields or None
        if new_value is None:
            limits.add('permission_details_unavailable')
    elif action_name == 'message_action_group_call':
        duration = fields.get('duration')
        if type(duration) is int and duration >= 0:
            action_type = 'video_chat_ended'
            new_value = {'duration_seconds': duration}
        elif 'duration' not in fields:
            action_type = 'video_chat_started'
        else:
            limits.add('video_chat_duration_invalid')

    if action_type == 'unknown':
        limits.add('unknown_service_action_seen')
    if (target_id is not None or isinstance(new_value, dict) and new_value.get('target_ids')) and target_name is None:
        limits.add('target_name_unavailable')
    actual_topic = topic
    if topic is None:
        reply = _first(message, 'replyTo', 'reply_to') or {}
        top = _first(reply, 'topMsgId', 'top_msg_id') if isinstance(reply, dict) else None
        if type(top) is int and top > 0:
            actual_topic = top
        elif event_id == 1 and action_name == 'message_action_topic_create':
            actual_topic = 1
    return ({'event_id': event_id, 'topic_id': actual_topic, 'date': date, 'type': action_type,
             'actor_id': actor_id, 'actor_name': actor_name, 'target_id': target_id,
             'target_name': target_name, 'message_id': message_id, 'old_value': old_value,
             'new_value': new_value, 'raw_action': action_name}, limits)


def _safe_cli_error_code(stdout, stderr, returncode):
    """Extract only a bounded RPC identifier; never persist CLI output."""
    raw = (stderr[:16384] + b"\n" + stdout[:16384]).decode("utf-8", "replace")
    if re.search(r"\bFLOOD_WAIT(?:[_ ]\d+)?\b", raw, re.IGNORECASE):
        return "TELEGRAM_FLOOD_WAIT"
    patterns = (
        r"(?i)(?:rpc[_ ]?error|error[_ ]?code|code)\s*[:= ]\s*([A-Z][A-Z0-9_]{2,63})",
        r"\b(?:400|401|403|404|420|500|503)\s+([A-Z][A-Z0-9_]{2,63})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            return "TELEGRAM_" + match.group(1).upper()
    numeric = re.search(r"(?i)(?:rpc[_ ]?error|error[_ ]?code|code)\s*[:= ]\s*(400|401|403|404|420|500|503)\b", raw)
    if numeric:
        return "TELEGRAM_RPC_" + numeric.group(1)
    rpc_code = re.search(
        r"\b([A-Z][A-Z0-9_]{2,63}_(?:INVALID|EMPTY|DELETED|FORBIDDEN|NOT_FOUND|ERROR))\b",
        raw,
    )
    if rpc_code:
        return "TELEGRAM_" + rpc_code.group(1)
    exception = re.search(r"\b([A-Za-z][A-Za-z0-9]{1,55}(?:Error|Exception))\b", raw)
    if exception:
        return "TELEGRAM_" + re.sub(r"[^A-Za-z0-9]+", "_", exception.group(1)).upper()
    return f"TELEGRAM_CLI_EXIT_{returncode}"


def _service_error_record(exc, cursor):
    code = getattr(exc, "error_code", None)
    if not isinstance(code, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{2,80}", code):
        code = "TELEGRAM_EXPORT_ERROR"
    return {
        "error_code": code,
        "message_id": cursor if type(cursor) is int and cursor > 0 else None,
        "event_type": None,
        "media_type": None,
        "stage": "service_history_page",
    }


_UNSUPPORTED_CURSOR_CODES = {
    "TELEGRAM_MESSAGE_ID_INVALID",
    "TELEGRAM_MSG_ID_INVALID",
    "TELEGRAM_MESSAGE_EMPTY",
    "TELEGRAM_MESSAGE_DELETED",
}


def _unsupported_service_event(message_id, topic, error_code):
    return {
        "event_id": message_id,
        "topic_id": topic,
        "date": None,
        "type": "unsupported",
        "actor_id": None,
        "actor_name": None,
        "target_id": None,
        "target_name": None,
        "message_id": message_id,
        "old_value": None,
        "new_value": None,
        "raw_action": None,
        "error_code": error_code,
    }


def service_belongs(message, topic):
    if topic is None:
        return True
    if message.get('id') == topic and _action_name(message.get('action')) == 'message_action_topic_create':
        return True
    reply = _first(message, 'replyTo', 'reply_to') or {}
    if not isinstance(reply, dict):
        return topic == 1
    top = _first(reply, 'topMsgId', 'top_msg_id', 'reply_to_top_id')
    if top is not None:
        return top == topic
    if topic == 1:
        forum_topic = bool(_first(reply, 'forumTopic', 'forum_topic'))
        reply_id = _first(reply, 'replyToMsgId', 'reply_to_msg_id')
        return not forum_topic or reply_id == 1
    # messages.GetReplies is already restricted to the requested topic.
    return True


class _ServiceRecordStore:
    """Disk-backed event index; large service histories do not accumulate in RAM."""

    def __init__(self):
        self._temporary = tempfile.TemporaryDirectory(prefix='kiber-telegram-services-')
        os.chmod(self._temporary.name, 0o700)
        self._path = Path(self._temporary.name) / 'events.sqlite3'
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        os.chmod(self._path, 0o600)
        self._db = sqlite3.connect(self._path)
        self._db.execute('PRAGMA journal_mode=OFF')
        self._db.execute('PRAGMA synchronous=OFF')
        self._db.execute(
            'CREATE TABLE events(event_id INTEGER PRIMARY KEY, payload TEXT NOT NULL) WITHOUT ROWID'
        )

    def __len__(self):
        return self._db.execute('SELECT count(*) FROM events').fetchone()[0]

    def get(self, event_id):
        row = self._db.execute(
            'SELECT payload FROM events WHERE event_id=?', (event_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def __setitem__(self, event_id, record):
        payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
        self._db.execute(
            'INSERT INTO events(event_id,payload) VALUES(?,?) '
            'ON CONFLICT(event_id) DO UPDATE SET payload=excluded.payload',
            (event_id, payload),
        )

    def setdefault(self, event_id, record):
        existing = self.get(event_id)
        if existing is not None:
            return existing
        self[event_id] = record
        return record

    def iter_records(self):
        cursor = self._db.execute('SELECT payload FROM events ORDER BY event_id')
        for (payload,) in cursor:
            yield json.loads(payload)

    def first_last(self):
        return self._db.execute(
            'SELECT min(event_id), max(event_id) FROM events'
        ).fetchone()

    def close(self):
        if self._db is not None:
            self._db.close()
            self._db = None
            self._temporary.cleanup()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


def _load_service_records(destination):
    records = _ServiceRecordStore()
    if 'services.jsonl' not in destination.names():
        return records, hashlib.sha256(b'').hexdigest()
    fd = os.open('services.jsonl', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                 dir_fd=destination.fd)
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > MAX_EXPORT_BYTES):
        os.close(fd)
        raise ExportError('Unsafe services.jsonl permissions or file type')
    digest = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            while line := stream.readline(MAX_JSONL_LINE_BYTES + 1):
                total += len(line)
                if len(line) > MAX_JSONL_LINE_BYTES or total > before.st_size:
                    raise ExportError('Existing services.jsonl contains an oversized line')
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise TypeError
                    event_id = record['event_id']
                except (ValueError, KeyError, TypeError, UnicodeError):
                    raise ExportError('Invalid existing services.jsonl') from None
                if type(event_id) is not int or event_id <= 0 or records.get(event_id) is not None:
                    raise ExportError('Duplicate or invalid service event ID')
                records[event_id] = record
                digest.update(line)
            after = os.fstat(stream.fileno())
            if (total != before.st_size
                    or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                        before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size,
                                                after.st_mtime_ns, after.st_ctime_ns)):
                raise ExportError('services.jsonl changed while being read')
    except BaseException:
        raise
    return records, digest.hexdigest()


def _read_services_manifest(destination):
    if 'services_manifest.json' not in destination.names():
        return None
    manifest = destination.json('services_manifest.json')
    if not isinstance(manifest, dict):
        raise ExportError('Invalid services_manifest.json')
    info = os.stat('services_manifest.json', dir_fd=destination.fd, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
        raise ExportError('Unsafe services_manifest.json permissions or file type')
    return manifest


def _cleanup_service_temps(destination):
    removed = False
    for name in destination.names():
        if not re.fullmatch(r'\.tmp-services-[0-9a-f]{32}', name):
            continue
        info = os.stat(name, dir_fd=destination.fd, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid()):
            raise ExportError('Unsafe abandoned service temporary file')
        os.unlink(name, dir_fd=destination.fd)
        removed = True
    if removed:
        os.fsync(destination.fd)


def _service_limitations(record_limits=()):
    entries = {
        'deleted_events_unavailable': 'Telegram history does not return service messages that were deleted.',
        'events_before_join_unavailable': 'Events from before the account joined the chat may be unavailable.',
        'unpin_events_may_not_be_in_history': 'Telegram may expose unpinning as an update rather than a service message in history.',
        'topic_deletion_events_may_not_be_in_history': 'Deleted forum topics and their service messages may no longer be present in history.',
    }
    for code in record_limits:
        entries[code] = code.replace('_', ' ')
    return [{'code': code, 'detail': entries[code]} for code in sorted(entries)]


def _publish_services(destination, meta, records, cursor, exhausted, budget, record_limits=(), error=None,
                      unsupported_ranges=()):
    count = len(records)
    first_id, last_id = records.first_last()
    temp, data_size, digest = destination.private_stream_temp(
        (json.dumps(item, ensure_ascii=False, sort_keys=True) + '\n').encode()
        for item in records.iter_records()
    )
    manifest = {**meta, 'exported_at': now(), 'event_count': count,
                'first_event_id': first_id,
                'last_event_id': last_id,
                'next_before': cursor, 'services_sha256': digest,
                'complete': bool(exhausted and meta['scope_before'] is None and error is None),
                'range_complete': bool(exhausted and error is None),
                'active': not exhausted and error is None,
                'service_messages_included': True,
                'limitations': _service_limitations(record_limits),
                'errors': [error] if error else [],
                'unsupported_ranges': list(unsupported_ranges)}
    try:
        budget.check(data_size + len(json.dumps(manifest).encode()) + 65536)
        previous = _read_services_manifest(destination)
        if previous and 'pending_publish' in previous:
            raise ExportError('Pending service publication requires recovery')
        pending = {'previous': previous, 'next': manifest}
    except BaseException:
        try:
            os.unlink(temp, dir_fd=destination.fd)
        except FileNotFoundError:
            pass
        raise
    try:
        destination.atomic_private(
            'services_manifest.json',
            (json.dumps({**manifest, 'pending_publish': pending}, ensure_ascii=False, sort_keys=True) + '\n').encode(),
        )
        destination.publish_private_temp(temp, 'services.jsonl')
        destination.atomic_private(
            'services_manifest.json',
            (json.dumps(manifest, ensure_ascii=False, sort_keys=True) + '\n').encode(),
        )
    finally:
        try:
            os.unlink(temp, dir_fd=destination.fd)
        except FileNotFoundError:
            pass
    return manifest, digest


def _recover_service_publication(destination, manifest, records, digest):
    if not manifest or 'pending_publish' not in manifest:
        return manifest
    pending = manifest['pending_publish']
    if not isinstance(pending, dict) or set(pending) != {'previous', 'next'}:
        raise ExportError('Invalid pending service publication')
    previous, following = pending['previous'], pending['next']
    if 'services.jsonl' in destination.names() and isinstance(following, dict) and following.get('services_sha256') == digest and following.get('event_count') == len(records):
        if previous is not None and isinstance(previous, dict) and previous.get('services_sha256') == digest and previous.get('event_count') == len(records):
            destination.atomic_private('services_manifest.json', (json.dumps(previous, ensure_ascii=False, sort_keys=True) + '\n').encode())
            return previous
        destination.atomic_private('services_manifest.json', (json.dumps(following, ensure_ascii=False, sort_keys=True) + '\n').encode())
        return following
    if previous is not None and isinstance(previous, dict) and previous.get('services_sha256') == digest and previous.get('event_count') == len(records):
        destination.atomic_private('services_manifest.json', (json.dumps(previous, ensure_ascii=False, sort_keys=True) + '\n').encode())
        return previous
    if previous is None and 'services.jsonl' not in destination.names():
        os.unlink('services_manifest.json', dir_fd=destination.fd)
        os.fsync(destination.fd)
        return None
    raise ExportError('Pending service publication does not match either committed state')


def _service_manifest_view(manifest, records, digest, data_present):
    if not manifest or 'pending_publish' not in manifest:
        return manifest
    pending = manifest['pending_publish']
    if not isinstance(pending, dict) or set(pending) != {'previous', 'next'}:
        raise ExportError('Invalid pending service publication')
    previous, following = pending['previous'], pending['next']
    if data_present and isinstance(following, dict) and following.get('services_sha256') == digest and following.get('event_count') == len(records):
        if previous is not None and isinstance(previous, dict) and previous.get('services_sha256') == digest and previous.get('event_count') == len(records):
            return previous
        return following
    if previous is not None and isinstance(previous, dict) and previous.get('services_sha256') == digest and previous.get('event_count') == len(records):
        return previous
    if previous is None and not data_present:
        return None
    raise ExportError('Pending service publication does not match either committed state')


def export_services(root, cfg, supplied_chat, topic, limit, before, client=None):
    client = client or Client(cfg)
    info = client.info(supplied_chat)
    canonical = str(info.get('id'))
    if not re.fullmatch(r'-?[1-9][0-9]{0,18}', canonical):
        raise ExportError('Service export requires a numeric group ID')
    chat_dir = root.child(canonical)
    try:
        scope_dir = telegram_scope_directory(topic)
        source_id = format_telegram_source(canonical, topic)
        destination = chat_dir.child(scope_dir)
        records = None
        try:
            if stat.S_IMODE(os.fstat(destination.fd).st_mode) != 0o700:
                raise ExportError('Topic directory must have mode 0700')
            _cleanup_service_temps(destination)
            ordinary = destination.json('manifest.json')
            if not isinstance(ordinary, dict):
                raise ExportError('Invalid topic export manifest')
            expected_source = source_id
            if (ordinary.get('export_version') != 1 or ordinary.get('source_id') != expected_source
                    or str(ordinary.get('chat_id')) != canonical or ordinary.get('topic_id') != topic
                    or ordinary.get('scope') != ('chat' if topic is None else 'topic')):
                raise ExportError('Group/topic directory does not match its export manifest')
            manifest = _read_services_manifest(destination)
            records, digest = _load_service_records(destination)
            manifest = _recover_service_publication(destination, manifest, records, digest)
            if ('services.jsonl' in destination.names()) != bool(manifest):
                raise ExportError('Service outputs are incomplete; inspect before resuming')
            if manifest:
                if manifest.get('services_sha256') != digest or manifest.get('event_count') != len(records):
                    raise ExportError('Service output integrity mismatch')
                if manifest.get('source_id') != source_id:
                    raise ExportError('Existing service export belongs to another group/topic scope')
                stored_before = manifest.get('scope_before')
                if before is not None and before != stored_before:
                    raise ExportError('Existing service export scope differs; no data overwritten')
                if manifest.get('range_complete') is True and (before is None or before == stored_before):
                    return {**manifest, 'services_sha256': digest}
                cursor = manifest.get('next_before')
                scope_before = stored_before
            else:
                if 'services.jsonl' in destination.names():
                    raise ExportError('Existing service data has no manifest')
                cursor = before
                scope_before = before
                meta = {'source_id': source_id, 'chat_id': int(canonical),
                        'topic_id': topic, 'scope': 'chat' if topic is None else 'topic',
                        'scope_before': scope_before, 'started_at': now()}
                manifest, digest = _publish_services(destination, meta, records, cursor, False,
                                                     Budget(root, cfg), error=None)
            meta = {'source_id': source_id, 'chat_id': int(canonical),
                    'topic_id': topic, 'scope': 'chat' if topic is None else 'topic',
                    'scope_before': scope_before,
                    'started_at': manifest.get('started_at') if manifest else now()}
            budget = Budget(root, cfg)
            all_limits = set()
            unsupported_ranges = list(manifest.get('unsupported_ranges', [])) if manifest else []
            if manifest:
                for item in manifest.get('limitations', []):
                    if isinstance(item, dict) and item.get('code') not in {
                        'deleted_events_unavailable', 'events_before_join_unavailable',
                        'unpin_events_may_not_be_in_history', 'topic_deletion_events_may_not_be_in_history'
                    }:
                        all_limits.add(item['code'])
            try:
                while True:
                    budget.check(65536)
                    try:
                        page = client.service_page(canonical, topic, limit, cursor)
                    except ExportError as exc:
                        diagnostic = _service_error_record(exc, cursor)
                        if cursor is not None and diagnostic["error_code"] in _UNSUPPORTED_CURSOR_CODES:
                            if len(unsupported_ranges) >= 20:
                                _publish_services(
                                    destination, meta, records, cursor, False, budget,
                                    all_limits, diagnostic, unsupported_ranges,
                                )
                                raise ExportError("Too many unsupported Telegram service events") from None
                            unsupported_ranges.append(diagnostic)
                            records.setdefault(
                                cursor,
        _unsupported_service_event(cursor, topic, diagnostic["error_code"]),
                            )
                            all_limits.add("service_event_unavailable_at_message_id")
                            if cursor <= 1:
                                cursor = 0
                                result, _ = _publish_services(
                                    destination, meta, records, cursor, True, budget,
                                    all_limits, unsupported_ranges=unsupported_ranges,
                                )
                                return result
                            # The next request uses exclusive before/offsetId.
                            # Keep this ID as the boundary so the preceding
                            # service event is not skipped.
                            _publish_services(
                                destination, meta, records, cursor, False, budget,
                                all_limits, unsupported_ranges=unsupported_ranges,
                            )
                            time.sleep(0.25)
                            continue
                        _publish_services(
                            destination, meta, records, cursor, False, budget,
                            all_limits, diagnostic, unsupported_ranges,
                        )
                        raise
                    if not page:
                        result, _ = _publish_services(destination, meta, records, cursor, True,
                                                      budget, all_limits,
                                                      unsupported_ranges=unsupported_ranges)
                        return result
                    if not all(isinstance(item, dict) and type(item.get('id')) is int and item['id'] > 0 for item in page):
                        raise ExportError('Invalid Telegram service message record')
                    valid = [item for item in page if cursor is None or item['id'] < cursor]
                    if not valid:
                        raise ExportError('Service pagination made no progress')
                    next_cursor = min(item['id'] for item in valid)
                    for item in valid:
                        if not service_belongs(item, topic):
                            continue
                        normalized, limitations = service_event(item, topic)
                        if normalized is None:
                            continue
                        all_limits.update(limitations)
                        existing = records.get(normalized['event_id'])
                        if existing is not None and existing != normalized:
                            raise ExportError('Conflicting duplicate Telegram service event')
                        records[normalized['event_id']] = normalized
                    cursor = next_cursor
                    _publish_services(destination, meta, records, cursor, False, budget, all_limits,
                                      unsupported_ranges=unsupported_ranges)
                    time.sleep(0.25)
            except BaseException as exc:
                # Persist a terminal checkpoint for every Python-level interruption,
                # including unexpected provider/parser exceptions and KeyboardInterrupt.
                try:
                    _publish_services(destination, meta, records, cursor, False, budget,
                                      all_limits, error=_service_error_record(exc, cursor),
                                      unsupported_ranges=unsupported_ranges)
                except (OSError, ValueError, TypeError):
                    # Preserve the original failure if disk space or publication itself
                    # prevents writing the diagnostic checkpoint.
                    raise exc from None
                raise
        finally:
            if records is not None:
                records.close()
            destination.close()
    finally:
        chat_dir.close()


def _positive_size(value):
    if isinstance(value, str) and re.fullmatch(r'[0-9]{1,19}', value):
        value = int(value)
    return value if type(value) is int and value > 0 else None


def _tl_class(value):
    return _snake_name(_first(value, 'className', '_'))


def _photo_size(variant):
    cls = _tl_class(variant)
    if cls in {'photo_size', 'video_size'}:
        return _positive_size(variant.get('size'))
    if cls == 'photo_size_progressive':
        sizes = variant.get('sizes')
        if isinstance(sizes, list):
            return max(filter(None, map(_positive_size, sizes)), default=None)
    return None


def _over_limit_reason(file_limit):
    if file_limit == 100 * 1024**2:
        return 'over_100_MiB'
    return 'over_limit'


def media_size_metadata(metadata, raw, file_limit=None):
    """Return safe metadata and any reason to skip before calling download."""
    file_limit = MAX_FILE if file_limit is None else file_limit
    kind = metadata.get('kind')
    allowed_kinds = {
        'photo', 'video', 'audio', 'document', 'voice', 'video_note',
        'sticker', 'gif', 'animation', 'file', 'round_video',
    }
    size = _positive_size(metadata.get('size'))
    source = metadata.get('size_source') or ('document.size' if size is not None else 'size_unknown')
    reason = None
    if isinstance(raw, dict):
        document = raw.get('document')
        photo = raw.get('photo')
        if isinstance(document, dict) and _tl_class(document) == 'document':
            size = _positive_size(document.get('size'))
            source = 'document.size' if size is not None else 'size_unknown'
        elif isinstance(photo, dict) and _tl_class(photo) == 'photo':
            variants = [v for v in (photo.get('sizes') or []) if isinstance(v, dict)]
            sizes = [n for v in variants if (n := _photo_size(v)) is not None]
            if sizes:
                size = max((n for n in sizes if n <= file_limit), default=min(sizes))
                source = 'photo.sizes'
                videos = [n for v in (photo.get('videoSizes') or []) if isinstance(v, dict)
                          and (n := _photo_size(v)) is not None]
                # The pinned CLI selects its largest thumbnail automatically
                # and cannot be instructed to fetch a smaller variant.
                if size <= file_limit and max(sizes + videos) != size:
                    reason = 'photo_variant_selection_unsupported'
            else:
                size, source = None, 'size_unknown'
        else:
            size, source = None, 'size_unknown'
            if kind not in allowed_kinds:
                reason = 'unsupported_media'
    elif kind not in allowed_kinds:
        reason = 'unsupported_media'
    if size is not None and size > file_limit:
        reason = _over_limit_reason(file_limit)
    return {**metadata, 'size_bytes': size, 'size_source': source}, reason


def _clear_media_stage(stage):
    names = stage.names()
    for name in names:
        entry = os.stat(name, dir_fd=stage.fd, follow_symlinks=False)
        if not stat.S_ISREG(entry.st_mode) or entry.st_nlink != 1:
            raise ExportError('Unsafe staged media output')
    for name in names:
        os.unlink(name, dir_fd=stage.fd)


def _media_receipt(directory, size, file_limit=None):
    file_limit = MAX_FILE if file_limit is None else file_limit
    receipt = directory.json('receipt.json')
    if (not isinstance(receipt, dict)
            or not isinstance(receipt.get('name'), str)
            or type(receipt.get('size')) is not int
            or not 0 < receipt['size'] <= file_limit
            or not isinstance(receipt.get('sha256'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', receipt['sha256'])):
        raise ExportError('Stored media receipt is invalid')
    actual_size, actual_sha = directory.digest(receipt['name'], file_limit)
    if (actual_size != receipt['size'] or (size is not None and actual_size != size)
            or actual_sha != receipt['sha256']):
        raise ExportError('Stored media integrity mismatch')
    return receipt


def attachment(client, chat, item, media_dir, budget, raw=None, file_limit=None):
    file_limit = MAX_FILE if file_limit is None else file_limit
    if not item['media']:
        return None
    metadata, reason = media_size_metadata(item['media'], raw, file_limit)
    if reason and reason not in {'size_unknown'}:
        return {**metadata, 'export_status': 'skipped', 'reason': reason,
                'max_file_bytes': file_limit}
    size = metadata['size_bytes']
    mid = str(item['message_id'])
    if mid in media_dir.names():
        saved = media_dir.child(mid)
        try:
            receipt = _media_receipt(saved, size, file_limit)
        finally:
            saved.close()
        effective_limit = file_limit
    else:
        requested_limit = file_limit
        stage_name = '.part-'+mid
        stage = None
        promoted = False
        complete_stage = False
        try:
            if stage_name in media_dir.names():
                stage = media_dir.child(stage_name)
            if stage is not None and 'receipt.json' in stage.names():
                receipt = _media_receipt(stage, size, file_limit)
                complete_stage = True
                effective_limit = file_limit
            else:
                if stage is not None:
                    _clear_media_stage(stage)
                    stage.close()
                    stage = None
                    os.rmdir(stage_name, dir_fd=media_dir.fd)
                    os.fsync(media_dir.fd)
                has_live_budget = budget is not None and hasattr(budget, 'available_bytes')
                available = budget.available_bytes() if has_live_budget else file_limit
                overhead = 65536 if has_live_budget else 0
                effective_limit = min(file_limit, max(0, available - overhead))
                if effective_limit <= 0 or (size is not None and size > effective_limit):
                    return {**metadata, 'export_status': 'skipped',
                            'reason': 'storage_budget_exhausted',
                            'requested_max_file_bytes': requested_limit,
                            'max_file_bytes': effective_limit}
                if budget is not None:
                    budget.check(effective_limit + 65536)
                stage = media_dir.child(stage_name, True)
                download_limit = size if size is not None else effective_limit
                client.download(chat, item['message_id'], stage, download_limit)
                names = stage.names()
                if len(names) != 1 or names[0] == 'receipt.json':
                    raise ExportError('Unexpected downloaded media outputs')
                actual_size, actual_sha = stage.digest(names[0], effective_limit)
                if size is not None and actual_size != size:
                    raise ExportError('Media size mismatch')
                os.chmod(names[0], 0o600, dir_fd=stage.fd, follow_symlinks=False)
                receipt = {'name': names[0], 'size': actual_size, 'sha256': actual_sha}
                fd = os.open(names[0], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=stage.fd)
                with os.fdopen(fd, 'rb') as stream:
                    os.fsync(stream.fileno())
                stage.write_json('receipt.json', receipt)
                complete_stage = True
            if budget is not None:
                # Check raw signed margins after the download. A zero public
                # available count can hide a quota deficit, so check(0) must
                # reject negative storage, filesystem, and shared-budget margins.
                budget.check(0)
            os.rename(stage_name, mid, src_dir_fd=media_dir.fd, dst_dir_fd=media_dir.fd)
            promoted = True
            os.fsync(media_dir.fd)
            if budget is not None and hasattr(budget, 'consume'):
                budget.consume(receipt['size'])
        except MediaLimitExceeded:
            return {**metadata, 'export_status': 'skipped',
                    'reason': ('storage_budget_exhausted' if effective_limit < requested_limit
                               else _over_limit_reason(requested_limit)),
                    'requested_max_file_bytes': requested_limit,
                    'max_file_bytes': effective_limit}
        finally:
            try:
                if stage is not None and not promoted and not complete_stage:
                    _clear_media_stage(stage)
                    os.rmdir(stage_name, dir_fd=media_dir.fd)
            finally:
                if stage is not None:
                    stage.close()
    return {**metadata, 'size': receipt['size'], 'size_bytes': receipt['size'],
            'size_source': metadata['size_source'] if size is not None else 'downloaded_bytes',
            'export_status': 'downloaded', 'path': 'media/'+mid+'/'+receipt['name'],
            'sha256': receipt['sha256'], 'requested_max_file_bytes': file_limit,
            'max_file_bytes': effective_limit if 'effective_limit' in locals() else file_limit}


def now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def publish(destination, pages, meta, budget, error=None, active=False):
    count = 0
    previous_message_id = None
    first = last = cursor = None
    cursor = meta['scope_before']
    exhausted = False
    skipped = []
    skipped_count = 0
    skipped_by_reason = {}
    temp = '.tmp-messages.jsonl'
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=destination.fd)
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
        os.close(fd)
        raise ExportError('Unsafe temporary messages output')
    os.ftruncate(fd, 0)
    digest = hashlib.sha256()
    page_names = sorted(n for n in pages.names() if not re.fullmatch(r'\.tmp-[0-9]{8}\.json', n))
    budget.check(sum(os.stat(n, dir_fd=pages.fd, follow_symlinks=False).st_size for n in page_names) + 65536)
    with os.fdopen(fd, 'wb') as f:
        for index, name in enumerate(page_names):
            if name != f'{index:08d}.json':
                raise ExportError('Unexpected checkpoint layout')
            p = pages.json(name)
            if meta['snapshot_newest_id'] is None and p.get('observed_newest_id') is not None:
                meta['snapshot_newest_id'] = p['observed_newest_id']
            if p['previous_before'] != cursor or exhausted:
                raise ExportError('Broken checkpoint chain')
            for item in p['records']:
                mid = item['message_id']
                if type(mid) is not int or mid <= 0 or (
                    previous_message_id is not None and mid >= previous_message_id
                ):
                    raise ExportError('Duplicate or out-of-order ID in checkpoints')
                previous_message_id = mid
                raw = (json.dumps(item, ensure_ascii=False, sort_keys=True)+'\n').encode()
                f.write(raw)
                digest.update(raw)
                count += 1
                first = mid if first is None else min(first, mid)
                last = mid if last is None else max(last, mid)
                m = item.get('media')
                if m and m.get('export_status') == 'skipped':
                    skipped_count += 1
                    reason = m.get('reason') if isinstance(m.get('reason'), str) else 'unknown'
                    skipped_by_reason[reason] = skipped_by_reason.get(reason, 0) + 1
                    if len(skipped) < MAX_SKIPPED_MEDIA_SAMPLE:
                        sample = {
                            key: m[key] for key in (
                                'kind', 'size_bytes', 'size_source', 'reason', 'error_code',
                                'requested_max_file_bytes', 'max_file_bytes',
                            ) if key in m and isinstance(m[key], (str, int))
                        }
                        skipped.append({'message_id': mid, 'metadata': sample})
            cursor = p['next_before']
            exhausted = p['exhausted']
        f.flush()
        os.fsync(f.fileno())
    if 'messages.jsonl' in destination.names() and stat.S_ISLNK(os.stat('messages.jsonl', dir_fd=destination.fd, follow_symlinks=False).st_mode):
        raise ExportError('Symlink output refused')
    os.rename(temp, 'messages.jsonl', src_dir_fd=destination.fd, dst_dir_fd=destination.fd)
    manifest = {**meta, 'exported_at': now(), 'message_count': count, 'first_message_id': first,
                'last_message_id': last, 'last_processed_message_id': cursor,
                'messages_sha256': digest.hexdigest(), 'complete': exhausted and meta['scope_before'] is None and not error,
                'range_complete': exhausted and not error, 'active': active, 'errors': [error] if error else [],
                'completeness_scope': 'ordinary_messages_exposed_by_better-tg-cli', 'service_messages_included': False,
                'skipped_media': skipped, 'skipped_media_count': skipped_count,
                'skipped_media_by_reason': dict(sorted(skipped_by_reason.items())),
                'skipped_media_sample_truncated': skipped_count > len(skipped),
                'page_count': len(page_names)}
    budget.check(len(json.dumps(manifest).encode()))
    destination.write_json('manifest.json', manifest)
    return manifest, count


def _private_resume_dir(directory):
    info = os.fstat(directory.fd)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ExportError('RESUME_PRIVATE_DIRECTORY_REQUIRED')


def _private_topic_file(directory, name, maximum):
    info = os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size > maximum):
        raise ExportError('RESUME_PRIVATE_INPUT_REQUIRED')
    return directory.read(name, maximum)


def _scan_resume_jsonl(source, name, maximum, validate):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source.fd)
    digest = hashlib.sha256()
    count = scanned = 0
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_size > maximum):
            raise ExportError('RESUME_PRIVATE_INPUT_REQUIRED')
        with os.fdopen(fd, 'rb') as stream:
            fd = -1
            while line := stream.readline(MAX_JSONL_LINE_BYTES + 1):
                scanned += len(line)
                if len(line) > MAX_JSONL_LINE_BYTES or scanned > before.st_size or not line.endswith(b'\n'):
                    raise ExportError('RESUME_MESSAGES_INVALID')
                try:
                    value = json.loads(line)
                except (ValueError, UnicodeError):
                    raise ExportError('RESUME_MESSAGES_INVALID') from None
                if not isinstance(value, dict):
                    raise ExportError('RESUME_MESSAGES_INVALID')
                validate(value)
                digest.update(line)
                count += 1
            after = os.fstat(stream.fileno())
            if (scanned != before.st_size
                    or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                        before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size,
                                                after.st_mtime_ns, after.st_ctime_ns)):
                raise ExportError('RESUME_SOURCE_CHANGED')
        return digest.hexdigest(), count
    finally:
        if fd >= 0:
            os.close(fd)


def _read_resume_source(source, chat, topic):
    source_id = format_telegram_source(chat, topic)
    message_limit = MAX_EXPORT_BYTES
    service_limit = MAX_EXPORT_BYTES
    data = {
        'manifest.json': _private_topic_file(source, 'manifest.json', 1024**2),
        'services_manifest.json': _private_topic_file(source, 'services_manifest.json', 1024**2),
    }
    manifest = json.loads(data['manifest.json'])
    service_manifest = json.loads(data['services_manifest.json'])
    message_rows = {}
    previous_id = None

    def validate_message(row):
        nonlocal previous_id
        message_id = row.get('message_id')
        row_topic = row.get('topic_id')
        if (type(message_id) is not int or message_id <= 0
                or (previous_id is not None and message_id >= previous_id)
                or (topic is not None and row_topic != topic)
                or (topic is None and row_topic is not None
                    and (type(row_topic) is not int or row_topic <= 0))
                or ('chat_id' in row and str(row['chat_id']) != chat)):
            raise ExportError('RESUME_MESSAGES_INVALID')
        previous_id = message_id
        media = row.get('media')
        if media is not None and not isinstance(media, dict):
            raise ExportError('RESUME_MEDIA_METADATA_INVALID')
        if isinstance(media, dict) and not media.get('path'):
            message_rows[str(message_id)] = row

    message_sha, message_count = _scan_resume_jsonl(
        source, 'messages.jsonl', message_limit, validate_message,
    )
    hashes = {
        'messages.jsonl': message_sha,
        'services.jsonl': None,
        'manifest.json': hashlib.sha256(data['manifest.json']).hexdigest(),
    }
    scope = 'chat' if topic is None else 'topic'
    if (not isinstance(manifest, dict) or manifest.get('source_id') != source_id
            or str(manifest.get('chat_id')) != chat or manifest.get('topic_id') != topic
            or manifest.get('scope') != scope or manifest.get('complete') is not True
            or manifest.get('range_complete') is not True or manifest.get('scope_before') is not None
            or manifest.get('errors') not in ([], None)
            or manifest.get('messages_sha256') != message_sha
            or manifest.get('message_count') != message_count):
        raise ExportError('RESUME_EXPORT_INCOMPLETE')

    service_count = 0

    def validate_event(event):
        nonlocal service_count
        topic_value = event.get('topic_id')
        if (type(event.get('event_id')) is not int or event['event_id'] <= 0
                or (topic is not None and topic_value != topic)
                or (topic is None and topic_value is not None
                    and (type(topic_value) is not int or topic_value <= 0))):
            raise ExportError('RESUME_SERVICES_INVALID')
        service_count += 1

    service_sha, service_count = _scan_resume_jsonl(
        source, 'services.jsonl', service_limit, validate_event,
    )
    hashes['services.jsonl'] = service_sha
    hashes['services_manifest.json'] = hashlib.sha256(data['services_manifest.json']).hexdigest()
    if (not isinstance(service_manifest, dict) or service_manifest.get('source_id') != source_id
            or service_manifest.get('chat_id') != int(chat)
            or service_manifest.get('topic_id') != topic
            or service_manifest.get('scope', scope) != scope
            or service_manifest.get('complete') is not True
            or service_manifest.get('range_complete') is not True
            or service_manifest.get('scope_before') is not None
            or service_manifest.get('errors') not in ([], None)
            or service_manifest.get('services_sha256') != service_sha
            or service_manifest.get('event_count') != service_count):
        raise ExportError('RESUME_SERVICES_INCOMPLETE')
    return source_id, data, hashes, manifest, message_rows


def _resume_checkpoint(directory, hashes, source_id, message_rows):
    try:
        checkpoint_bytes = directory.read('status.json', 64 * 1024**2)
    except FileNotFoundError:
        checkpoint_bytes = None
    if checkpoint_bytes is None:
        if 'manifest.json' in directory.names():
            raise ExportError('RESUME_CHECKPOINT_MISSING')
        return {'version': 2, 'source_id': source_id, 'inputs': hashes, 'records': {}}, False
    checkpoint = json.loads(checkpoint_bytes)
    if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get('records'), dict):
        raise ExportError('RESUME_CHECKPOINT_INVALID')
    if checkpoint.get('version') == 2:
        if checkpoint.get('source_id') != source_id or checkpoint.get('inputs') != hashes:
            raise ExportError('RESUME_CHECKPOINT_MISMATCH')
        records = checkpoint['records']
        expected = {mid for mid, row in message_rows.items()
                    if isinstance(row.get('media'), dict) and not row['media'].get('path')}
        if not records.keys() <= expected:
            raise ExportError('RESUME_CHECKPOINT_INVALID')
        return checkpoint, True
    if checkpoint.get('version') != 1 or checkpoint.get('inputs') != hashes:
        raise ExportError('RESUME_CHECKPOINT_MISMATCH')
    expected_legacy = {mid for mid, row in message_rows.items()
                       if isinstance(row.get('media'), dict) and not row['media'].get('path')}
    legacy_records = checkpoint['records']
    if not legacy_records.keys() <= expected_legacy:
        raise ExportError('RESUME_CHECKPOINT_INVALID')
    return {'version': 2, 'source_id': source_id, 'inputs': hashes,
            'records': legacy_records, 'legacy_checkpoint_imported': True}, True


def _resume_reason(record):
    return record.get('reason') if isinstance(record, dict) else None


def _resume_counts(records):
    result = {'downloaded': 0, 'skipped': 0, 'download_errors': 0, 'already_present': 0}
    reasons = {}
    for row in records.values():
        status = row.get('export_status')
        if status == 'downloaded':
            result['downloaded'] += 1
        elif status == 'already_present':
            result['already_present'] += 1
        else:
            result['skipped'] += 1
        reason = row.get('reason')
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1
            if reason in {'download_error', 'metadata_unavailable', 'storage_budget_exhausted'}:
                result['download_errors'] += 1
    result['skipped_by_reason'] = reasons
    return result


def _count_jsonl_media(directory, name, expected_sha256):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory.fd)
    digest = hashlib.sha256()
    count = 0
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise ExportError('Unsafe messages file')
        with os.fdopen(fd, 'rb') as stream:
            fd = -1
            scanned = 0
            while line := stream.readline(MAX_JSONL_LINE_BYTES + 1):
                scanned += len(line)
                if len(line) > MAX_JSONL_LINE_BYTES or scanned > before.st_size or not line.endswith(b'\n'):
                    raise ExportError('Invalid or oversized message line')
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeError):
                    raise ExportError('Invalid messages JSONL') from None
                if isinstance(row, dict) and isinstance(row.get('media'), dict):
                    count += 1
                digest.update(line)
            after = os.fstat(stream.fileno())
            if (scanned != before.st_size or digest.hexdigest() != expected_sha256
                    or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                        before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size,
                                                after.st_mtime_ns, after.st_ctime_ns)):
                raise ExportError('Output/checkpoint mismatch after interruption; repeat export to recover')
        return count
    finally:
        if fd >= 0:
            os.close(fd)


def transfer_helper_call(cfg, action, source_id):
    from kiber_analyst.telegram_source import parse_telegram_source

    if action not in {'budget', 'copy', 'status'} or parse_telegram_source(source_id) is None:
        raise ExportError('Invalid transfer helper request')
    try:
        timeout = 12 * 60 * 60 if action == 'copy' else 30
        result = subprocess.run(
            [cfg['publisher_executable'], '--config', cfg['publisher_config']],
            input=f'{action}\n{source_id}\n'.encode(), capture_output=True,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        raise ExportError('TRANSFER_HELPER_UNAVAILABLE', error_code='TRANSFER_HELPER_UNAVAILABLE') from None
    if result.returncode or len(result.stdout) > 16 * 1024:
        raise ExportError('TRANSFER_HELPER_FAILED', error_code='TRANSFER_HELPER_FAILED')
    try:
        payload = json.loads(result.stdout)
    except (ValueError, UnicodeError):
        raise ExportError('TRANSFER_HELPER_INVALID_RESPONSE') from None
    if not isinstance(payload, dict) or payload.get('source_id') != source_id:
        raise ExportError('TRANSFER_HELPER_INVALID_RESPONSE')
    return payload


def export_resume(root, cfg, chat, topic, limit, max_file_bytes, client):
    """Add bounded media to an existing complete export without rewriting it."""
    chat_dir = root.child(chat)
    try:
        source = chat_dir.child(telegram_scope_directory(topic))
    finally:
        chat_dir.close()
    try:
        _private_resume_dir(source)
        source_id, _data, hashes, _source_manifest, message_rows = _read_resume_source(
            source, chat, topic,
        )
        missing = {mid: row for mid, row in message_rows.items()
                   if isinstance(row.get('media'), dict) and not row['media'].get('path')}
        if not missing:
            return {
                'source_id': source_id, 'status': 'complete',
                'requested_max_file_bytes': max_file_bytes,
                'effective_max_file_bytes': max_file_bytes,
                'max_file_bytes_ceiling': MAX_FILE_BYTES,
                'max_file_size_unit': 'bytes', 'media_count': 0, 'remaining': 0,
                **_resume_counts({}),
            }
        budget = Budget(root, cfg)
        budget.check(65536)
        repair = source.child('media-resume', True)
        try:
            _private_resume_dir(repair)
            checkpoint, _had_checkpoint = _resume_checkpoint(repair, hashes, source_id, message_rows)
            records = checkpoint['records']
            if checkpoint.get('legacy_checkpoint_imported'):
                checkpoint = {
                    'version': 2, 'source_id': source_id, 'inputs': hashes,
                    'records': records, 'requested_max_file_bytes': max_file_bytes,
                    'max_file_size_unit': 'bytes',
                    'media_count': len(missing), 'updated_at': now(),
                }
                repair.atomic_private('status.json',
                                      (json.dumps(checkpoint, sort_keys=True) + '\n').encode())
            aggregate_available = cfg.get('aggregate_available_bytes')
            if type(aggregate_available) is not int or aggregate_available < 0:
                fs = os.fstatvfs(root.fd)
                aggregate_available = min(
                    max(0, cfg['storage_limit_bytes'] - tree_size(root)),
                    max(0, fs.f_bavail * fs.f_frsize - cfg['min_free_bytes']),
                )
            retryable = {'download_error', 'metadata_unavailable', 'storage_budget_exhausted', 'size_unknown'}
            unattempted = [mid for mid in missing if mid not in records]
            retry_pending = []
            for mid, old in records.items():
                if mid not in missing:
                    continue
                reason = _resume_reason(old)
                recorded_limit = old.get('max_file_bytes', MAX_FILE)
                if (reason in retryable
                        or (reason in {'over_100_MiB', 'over_limit',
                                       'photo_variant_selection_unsupported'}
                            and type(recorded_limit) is int
                            and max_file_bytes > recorded_limit)):
                    retry_pending.append(mid)
            # A persistent failure must not starve later not-yet-attempted files
            # when a small batch is used.
            pending = (unattempted + retry_pending)[:limit]
            if not pending and 'manifest.json' in repair.names():
                existing_status = repair.json('manifest.json')
                checkpoint_bytes = repair.read('status.json', 64 * 1024**2)
                checkpoint_sha = hashlib.sha256(checkpoint_bytes).hexdigest()
                if (existing_status.get('source_id') != source_id
                        or existing_status.get('inputs') != hashes
                        or existing_status.get('checkpoint_sha256') != checkpoint_sha
                        or existing_status.get('requested_max_file_bytes')
                        != checkpoint.get('requested_max_file_bytes')
                        or existing_status.get('complete') is not True):
                    raise ExportError('RESUME_CHECKPOINT_MANIFEST_MISMATCH')
                return {
                    'source_id': source_id, 'status': 'complete',
                    'requested_max_file_bytes': existing_status['requested_max_file_bytes'],
                    'effective_max_file_bytes': existing_status['effective_max_file_bytes'],
                    'max_file_bytes_ceiling': MAX_FILE_BYTES,
                    'max_file_size_unit': 'bytes',
                    'media_count': existing_status['media_count'],
                    'remaining': existing_status['remaining'],
                    **{key: existing_status[key] for key in (
                        'downloaded', 'skipped', 'download_errors', 'already_present',
                        'skipped_by_reason',
                    )},
                }
            existing_ids = set()
            if pending:
                try:
                    original_media = source.child('media')
                except FileNotFoundError:
                    pass
                else:
                    try:
                        existing_ids = set(original_media.names())
                    finally:
                        original_media.close()
                lookup = [int(mid) for mid in pending if mid not in existing_ids]
                try:
                    raw = client.media_metadata(chat, lookup) if lookup else {}
                    metadata_error = None
                except (ValueError, OSError, subprocess.SubprocessError) as exc:
                    raw = {}
                    metadata_error = getattr(exc, 'error_code', None) or 'METADATA_UNAVAILABLE'
                for mid in pending:
                    item = missing[mid]
                    if mid in existing_ids:
                        original_media = source.child('media')
                        try:
                            original = original_media.child(mid)
                            try:
                                receipt = _media_receipt(original, None, max_file_bytes)
                            finally:
                                original.close()
                        finally:
                            original_media.close()
                        attachment_info = {
                            **(item.get('media') or {}), 'export_status': 'downloaded',
                            'path': f"media/{mid}/{receipt['name']}", 'size': receipt['size'],
                            'size_bytes': receipt['size'], 'sha256': receipt['sha256'],
                            'max_file_bytes': max_file_bytes,
                        }
                    elif metadata_error:
                        attachment_info = {
                            **(item.get('media') or {}), 'export_status': 'skipped',
                            'reason': 'metadata_unavailable', 'error_code': metadata_error,
                            'max_file_bytes': max_file_bytes,
                        }
                    else:
                        remaining = min(max(0, aggregate_available), budget.available_bytes())
                        effective_limit = min(max_file_bytes, max(0, remaining - 65536))
                        if effective_limit <= 0:
                            attachment_info = {
                                **(item.get('media') or {}), 'export_status': 'skipped',
                                'reason': 'storage_budget_exhausted',
                                'max_file_bytes': effective_limit,
                            }
                        else:
                            media_meta, pre_reason = media_size_metadata(
                                item['media'], raw.get(int(mid)), max_file_bytes,
                            )
                            if pre_reason and pre_reason != 'size_unknown':
                                attachment_info = {
                                    **media_meta, 'export_status': 'skipped',
                                    'reason': pre_reason, 'max_file_bytes': max_file_bytes,
                                }
                            elif media_meta.get('size_bytes') is not None and media_meta['size_bytes'] > effective_limit:
                                attachment_info = {
                                    **media_meta, 'export_status': 'skipped',
                                    'reason': 'storage_budget_exhausted',
                                    'max_file_bytes': effective_limit,
                                }
                            else:
                                try:
                                    attachment_info = attachment(
                                        client, chat, item, repair, budget,
                                        raw.get(int(mid)), effective_limit,
                                    )
                                except (ValueError, OSError, subprocess.SubprocessError) as exc:
                                    attachment_info = {
                                        **media_meta, 'export_status': 'skipped',
                                        'reason': 'download_error',
                                        'error_code': getattr(exc, 'error_code', None) or 'DOWNLOAD_ERROR',
                                        'max_file_bytes': effective_limit,
                                    }
                    if 'path' in attachment_info:
                        if attachment_info['path'].startswith('media/'):
                            attachment_info['path'] = attachment_info['path'].replace('media/', 'media-resume/', 1)
                        aggregate_available = max(0, aggregate_available - attachment_info.get('size_bytes', 0))
                    records[mid] = attachment_info
                    checkpoint = {
                        'version': 2, 'source_id': source_id, 'inputs': hashes,
                        'records': records, 'requested_max_file_bytes': max_file_bytes,
                        'max_file_size_unit': 'bytes',
                        'media_count': len(missing), 'updated_at': now(),
                    }
                    repair.atomic_private('status.json',
                                          (json.dumps(checkpoint, sort_keys=True) + '\n').encode())
            counts = _resume_counts(records)
            unresolved = [mid for mid in missing if mid not in records]
            errors = counts['download_errors']
            complete = not unresolved and errors == 0
            requested_limit = (max_file_bytes if pending else checkpoint.get(
                'requested_max_file_bytes', max_file_bytes,
            ))
            current_limits = [r.get('max_file_bytes', requested_limit) for r in records.values()
                              if r.get('export_status') == 'downloaded']
            effective_max = min([requested_limit, *current_limits]) if current_limits else requested_limit
            status = {
                'version': 1, 'source_id': source_id, 'inputs': hashes,
                'checkpoint_sha256': None, 'requested_max_file_bytes': requested_limit,
                'effective_max_file_bytes': effective_max, 'max_file_bytes_ceiling': MAX_FILE_BYTES,
                'max_file_size_unit': 'bytes',
                'media_count': len(missing), 'complete': complete,
                'remaining': len(unresolved), 'download_errors': errors,
                **counts, 'updated_at': checkpoint.get('updated_at', now()),
            }
            if records or missing:
                checkpoint_bytes = repair.read('status.json', 64 * 1024**2)
                status['checkpoint_sha256'] = hashlib.sha256(checkpoint_bytes).hexdigest()
                repair.atomic_private('manifest.json',
                                      (json.dumps(status, sort_keys=True) + '\n').encode())
            result = {
                'source_id': source_id, 'status': 'complete' if complete else 'partial',
                'requested_max_file_bytes': requested_limit,
                'effective_max_file_bytes': effective_max,
                'max_file_bytes_ceiling': MAX_FILE_BYTES, 'max_file_size_unit': 'bytes',
                'media_count': len(missing), 'remaining': len(unresolved),
                **counts,
            }
            return result
        finally:
            repair.close()
    finally:
        source.close()


def export(root, cfg, args, client=None):
    command, supplied_chat, topic, media, limit, before, max_file_bytes = parse(args)
    if command in {'export-transfer', 'export-transfer-status'}:
        source_id = format_telegram_source(supplied_chat, topic)
        return transfer_helper_call(
            cfg, 'copy' if command == 'export-transfer' else 'status', source_id,
        )
    client = client or Client(cfg)
    if command == 'export-resume':
        try:
            return export_resume(root, cfg, supplied_chat, topic, limit, max_file_bytes, client)
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            code = str(exc)
            if not re.fullmatch(r'[A-Z][A-Z0-9_]{2,80}', code):
                code = getattr(exc, 'error_code', None) or 'RESUME_FAILED'
            raise ExportError(code) from None
    if command == 'export-services':
        return export_services(root, cfg, supplied_chat, topic, limit, before, client)
    aliases = root.json('.aliases.json') if '.aliases.json' in root.names() else {}
    if command == 'export-status':
        canonical = aliases.get(supplied_chat, supplied_chat)
        if not re.fullmatch(r'-?[1-9][0-9]{0,18}', canonical):
            raise ExportError('Unknown export alias; use the numeric chat ID')
    else:
        info = client.info(supplied_chat)
        canonical = str(info['id'])
        if not re.fullmatch(r'-?[1-9][0-9]{0,18}', canonical):
            raise ExportError('Export requires a numeric group ID')
    cdir = root.child(canonical, command == 'export')
    try:
        scope_dir = telegram_scope_directory(topic)
        dest = cdir.child(scope_dir, command == 'export')
        try:
            if command == 'export-status':
                manifest = dest.json('manifest.json')
                if not isinstance(manifest, dict):
                    raise ExportError('Invalid topic export manifest')
                fd = os.open('messages.jsonl', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dest.fd)
                with os.fdopen(fd, 'rb') as stream:
                    st = os.fstat(stream.fileno())
                    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                        raise ExportError('Unsafe messages file')
                    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                if digest != manifest.get('messages_sha256'):
                    raise ExportError('Output/checkpoint mismatch after interruption; repeat export to recover')
                status = {'service_event_count': 0, 'services_complete': False, 'services_sha256': None}
                names = set(dest.names())
                has_service_manifest = 'services_manifest.json' in names
                has_service_data = 'services.jsonl' in names
                if has_service_data and not has_service_manifest:
                    raise ExportError('Service data exists without manifest')
                if has_service_manifest:
                    service_manifest = _read_services_manifest(dest)
                    service_records, service_digest = _load_service_records(dest)
                    try:
                        service_manifest = _service_manifest_view(
                            service_manifest, service_records, service_digest,
                            has_service_data,
                        )
                        service_count = len(service_records)
                    finally:
                        service_records.close()
                    # A pending first publication with no data has no committed
                    # service snapshot yet. Other missing halves are corruption.
                    if service_manifest is None and not has_service_data:
                        return {**manifest, **status, 'messages_integrity': 'ok', 'size_bytes': tree_size(dest)}
                    if service_manifest is None or not has_service_data:
                        raise ExportError('Service output is missing a committed file')
                    if service_manifest.get('services_sha256') != service_digest or service_manifest.get('event_count') != service_count:
                        raise ExportError('Service output integrity mismatch')
                    status = {'service_event_count': service_count,
                              'services_complete': bool(service_manifest.get('complete')),
                              'services_sha256': service_digest}
                media_status = {
                    'media_resume_status': 'not_started',
                    'requested_max_file_bytes': manifest.get('max_file_bytes', MAX_FILE),
                    'max_file_bytes_ceiling': MAX_FILE_BYTES,
                    'max_file_size_unit': 'bytes',
                    'media_count': _count_jsonl_media(
                        dest, 'messages.jsonl', manifest.get('messages_sha256'),
                    ),
                }
                if 'media-resume' in names:
                    resume_dir = dest.child('media-resume')
                    try:
                        _private_resume_dir(resume_dir)
                        if 'status.json' in resume_dir.names():
                            checkpoint_bytes = resume_dir.read('status.json', 64 * 1024**2)
                            checkpoint = json.loads(checkpoint_bytes)
                            media_status['requested_max_file_bytes'] = checkpoint.get(
                                'requested_max_file_bytes', MAX_FILE,
                            )
                            if 'manifest.json' in resume_dir.names():
                                media_manifest = resume_dir.json('manifest.json')
                                if media_manifest.get('checkpoint_sha256') != hashlib.sha256(checkpoint_bytes).hexdigest():
                                    raise ExportError('Media resume manifest/checkpoint mismatch')
                                if media_manifest.get('source_id') != manifest.get('source_id'):
                                    raise ExportError('Media resume source identity mismatch')
                                media_status.update({
                                    'media_resume_status': 'complete' if media_manifest.get('complete') else 'partial',
                                    'media_manifest': media_manifest,
                                })
                            else:
                                media_status['media_resume_status'] = 'in_progress'
                    finally:
                        resume_dir.close()
                return {**manifest, **status, **media_status,
                        'messages_integrity': 'ok', 'size_bytes': tree_size(dest)}
            budget = Budget(root, cfg)
            budget.check(65536)
            if 'manifest.json' in dest.names():
                meta = dest.json('manifest.json')
                if meta.get('export_version') != 1:
                    raise ExportError('Destination is occupied by another operation')
                if meta['media_requested'] != media or (before is not None and before != meta['scope_before']):
                    raise ExportError('Existing export scope/media mode differs; no data overwritten')
                stored_file_limit = meta.get('max_file_bytes', MAX_FILE)
                if stored_file_limit != max_file_bytes:
                    raise ExportError('Existing export file limit differs; no data overwritten')
                meta = {k: meta[k] for k in [
                    'export_version', 'source_id', 'chat_id', 'topic_id', 'scope',
                    'chat_title', 'topic_title', 'scope_before', 'media_requested',
                    'started_at', 'snapshot_newest_id',
                ] if k in meta}
                meta['max_file_bytes'] = stored_file_limit
            else:
                if set(dest.names()) - {'.tmp-manifest.json'}:
                    raise ExportError('Nonempty destination without an export manifest')
                meta = {'export_version': 1,
                        'source_id': format_telegram_source(canonical, topic),
                        'chat_id': canonical, 'topic_id': topic,
                        'scope': 'chat' if topic is None else 'topic',
                        'chat_title': info.get('title') if isinstance(info.get('title'), str) else None,
                        'topic_title': client.title(canonical, topic),
                        'scope_before': before, 'media_requested': media,
                        'max_file_bytes': max_file_bytes,
                        'max_file_size_unit': 'bytes',
                        'started_at': now(), 'snapshot_newest_id': None}
                dest.write_json('manifest.json', {**meta, 'complete': False, 'errors': [], 'message_count': 0})
            if aliases.get(supplied_chat) != canonical:
                aliases[supplied_chat] = canonical
                budget.check(65536)
                root.write_json('.aliases.json', aliases)
            pages = dest.child('.pages', True)
            media_dir = dest.child('media', True)
            try:
                manifest, _count = publish(dest, pages, meta, budget, active=True)
                if manifest['range_complete']:
                    manifest, _ = publish(dest, pages, meta, budget)
                    return manifest
                cursor = manifest['last_processed_message_id']
                while True:
                    budget.check(65536)
                    raw = client.page(canonical, topic, limit, cursor)
                    if not isinstance(raw, list) or len(raw) > limit:
                        raise ExportError('Invalid page shape')
                    ids = [record(m, topic)['message_id'] for m in raw]
                    valid = [m for m in raw if cursor is None or m['id'] < cursor]
                    if raw and not valid:
                        raise ExportError('Pagination made no progress')
                    next_cursor = min(ids) if ids else cursor
                    if cursor is not None and ids:
                        next_cursor = min(m['id'] for m in valid)
                    lookup_ids = sorted({
                        m['id'] for m in valid if belongs(m, topic)
                        and m.get('media') and (m['media'].get('kind') == 'photo'
                                                or not _positive_size(m['media'].get('size')))
                    })
                    raw_media = client.media_metadata(canonical, lookup_ids) if lookup_ids else {}
                    records = []
                    page_seen = set()
                    for message in sorted(valid, key=lambda m: m['id'], reverse=True):
                        mid = message['id']
                        if mid in page_seen:
                            continue
                        page_seen.add(mid)
                        if not belongs(message, topic):
                            if topic == 1:
                                continue
                            raise ExportError('Message explicitly belongs to another topic')
                        item = record(message, topic)
                        if item['media']:
                            raw_attachment = raw_media.get(mid)
                            if media:
                                item['media'] = attachment(
                                    client, canonical, item, media_dir, budget,
                                    raw_attachment, max_file_bytes,
                                )
                            else:
                                metadata, _ = media_size_metadata(item['media'], raw_attachment, max_file_bytes)
                                item['media'] = {**metadata, 'export_status': 'not_requested'}
                        records.append(item)
                    if meta['snapshot_newest_id'] is None and ids:
                        meta['snapshot_newest_id'] = max(ids)
                    page = {'previous_before':cursor,'next_before':next_cursor,'exhausted':not raw,'records':records, 'observed_newest_id': max(ids) if ids else None}
                    encoded = (json.dumps(page, ensure_ascii=False)+'\n').encode()
                    budget.check(len(encoded)*2+65536)
                    pages.atomic(f"{manifest['page_count']:08d}.json", encoded)
                    manifest, _count = publish(dest, pages, meta, budget, active=bool(raw))
                    if not raw:
                        return manifest
                    cursor = next_cursor
                    time.sleep(0.25)
            except BaseException as exc:
                # Every committed page can be reconstructed after interruption.
                try:
                    publish(dest, pages, meta, budget, error=type(exc).__name__)
                except (OSError, ValueError):
                    pass
                raise
            finally:
                media_dir.close()
                pages.close()
        finally:
            dest.close()
    finally:
        cdir.close()


def _check_storage_mount(cfg):
    expected = cfg['expected_mount_uuid']
    if expected is None:
        return
    mountpoint = cfg['storage_root']
    if not os.path.ismount(mountpoint):
        raise ExportError('Configured Telegram storage is not mounted')
    try:
        result = subprocess.run(
            ['findmnt', '-n', '-o', 'UUID', '--target', mountpoint],
            capture_output=True, check=False, timeout=5,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'},
        )
    except (OSError, subprocess.SubprocessError):
        raise ExportError('Configured Telegram storage mount check failed') from None
    actual = result.stdout.decode('ascii', 'ignore').strip()
    if result.returncode or actual.lower() != expected.lower():
        raise ExportError('Configured Telegram storage mount UUID does not match')


def handle(args, *, config_path=None):
    parsed = parse(args)
    command, chat, topic, media, _, _, _max_file_bytes = parsed
    if os.geteuid() == 0:
        raise ExportError('ROOT_EXECUTION_FORBIDDEN')
    os.umask(0o077)
    cfg = config(config_path)
    _check_storage_mount(cfg)
    if command == 'export-resume' or (command == 'export' and media):
        budget = transfer_helper_call(cfg, 'budget', format_telegram_source(chat, topic))
        available = budget.get('available_bytes')
        if budget.get('status') != 'budget' or type(available) is not int or available < 0:
            raise ExportError('TRANSFER_BUDGET_UNAVAILABLE')
        cfg['aggregate_available_bytes'] = available
    def interrupted(signum, frame):
        raise ExportError('Export interrupted')
    previous = {s:signal.signal(s, interrupted) for s in [signal.SIGTERM, signal.SIGHUP]}
    root = Dir.root(cfg['export_root'])
    try:
        info = os.fstat(root.fd)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise ExportError('Telegram export root must be owned by the service user and mode 0700')
        lock_fd = os.open('.operation.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                          0o600, dir_fd=root.fd)
        try:
            lock_info = os.fstat(lock_fd)
            if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1
                    or lock_info.st_uid != os.geteuid()
                    or stat.S_IMODE(lock_info.st_mode) != 0o600):
                raise ExportError('Telegram export lock is unsafe')
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ExportError('Another Telegram operation is running') from None
            result = export(root, cfg, args)
            return {key: value for key, value in result.items() if key != 'skipped_media'}
        finally:
            os.close(lock_fd)
    finally:
        root.close()
        for signal_number, handler in previous.items():
            signal.signal(signal_number, handler)


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args in (['--help'], ['-h']):
        print('Usage: kiber-analyst-telegram-export [--config FILE] '
              'export|export-resume|export-services|export-status|export-transfer|export-transfer-status ...')
        return 0
    config_path = None
    if args[:1] == ['--config']:
        if len(args) < 3:
            print(json.dumps({'status': 'error', 'error_code': 'INVALID_ARGUMENTS'}))
            return 2
        config_path, args = args[1], args[2:]
    try:
        result = handle(args, config_path=config_path)
    except ExportError as exc:
        code = exc.error_code or str(exc)
        if not re.fullmatch(r'[A-Z][A-Z0-9_]{1,80}', code):
            code = 'EXPORT_FAILED'
        print(json.dumps({'status': 'error', 'error_code': code}, sort_keys=True))
        return 1
    except Exception:  # noqa: BLE001 - keep filesystem and provider diagnostics private.
        print(json.dumps({'status': 'error', 'error_code': 'EXPORT_FAILED'}, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
