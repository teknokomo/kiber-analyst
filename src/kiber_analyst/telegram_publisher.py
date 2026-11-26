#!/usr/bin/python3
"""Publish validated Telegram exports into an operator-configured staging root."""

from __future__ import annotations

import contextlib
import ctypes
import datetime
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import uuid
from pathlib import PurePosixPath

from kiber_analyst.media_limits import MAX_TELEGRAM_EXPORT_BYTES
from kiber_analyst.telegram_source import parse_telegram_source, telegram_scope_directory

GIB = 1024**3
DEFAULT_FILE_BYTES = 100 * 1024**2
MAX_FILE_BYTES = 4_000_000_000  # 4000 decimal MB; separate from ASR limits.
MAX_STORAGE_BYTES = 1024 * GIB
MAX_METADATA_BYTES = 4 * 1024**2
MAX_CHECKPOINT_BYTES = 64 * 1024**2
# JSONL histories are streamed and bounded by the configured storage ceiling,
# not by an unrelated in-memory control-message limit.
MAX_CONTROL_BYTES = MAX_TELEGRAM_EXPORT_BYTES
MAX_JSONL_LINE_BYTES = 4 * 1024**2
MAX_SERVICES_MANIFEST_BYTES = 1024**2
ALLOWED_SKIP_REASONS = {
    "over_100_MiB", "over_limit", "unsupported_media",
    "photo_variant_selection_unsupported", "size_unknown",
    "download_error", "metadata_unavailable", "storage_budget_exhausted",
}
EVENT_TYPES = {
    "member_added", "member_joined", "member_left", "member_removed",
    "message_pinned", "message_unpinned", "topic_created", "topic_renamed",
    "topic_closed", "topic_reopened", "title_changed", "photo_changed",
    "description_changed", "permissions_changed", "video_chat_started",
    "video_chat_ended", "message_deleted", "unknown", "unsupported",
}


class TransferError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _fail(code: str):
    raise TransferError(code)


def _source_parts(source_id: str) -> tuple[str, str]:
    parsed = parse_telegram_source(source_id)
    if parsed is None:
        _fail("INVALID_SOURCE_ID")
    chat, topic = parsed
    return chat, telegram_scope_directory(topic)


def _component(value: str) -> str:
    if (not isinstance(value, str) or not value or value in {".", ".."}
            or "/" in value or "\\" in value or "\x00" in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        _fail("UNSAFE_PATH_COMPONENT")
    try:
        encoded = os.fsencode(value)
    except UnicodeEncodeError:
        _fail("UNSAFE_PATH_COMPONENT")
    if len(encoded) > 255:
        _fail("UNSAFE_PATH_COMPONENT")
    return value


def _open_root(path: str) -> int:
    if not path.startswith("/"):
        _fail("INVALID_FIXED_ROOT")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.split("/")[1:]:
            if not part:
                continue
            child = os.open(_component(part), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_child(parent: int, name: str, *, create: bool = False, uid: int | None = None,
                gid: int | None = None, mode: int = 0o700) -> int:
    name = _component(name)
    created = False
    if create:
        try:
            os.mkdir(name, mode, dir_fd=parent)
            created = True
        except FileExistsError:
            pass
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    info = os.fstat(fd)
    if created and uid is not None:
        os.fchown(fd, uid, -1 if gid is None else gid)
        os.fchmod(fd, mode)
        info = os.fstat(fd)
    if (not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != mode
            or (uid is not None and info.st_uid != uid)):
        os.close(fd)
        _fail("UNSAFE_DIRECTORY")
    return fd


def _destination_chat(parent: int, name: str, *, uid: int, gid: int) -> int:
    """Open the media-owned chat staging directory, adopting only an empty root placeholder."""
    name = _component(name)
    try:
        os.mkdir(name, 0o700, dir_fd=parent)
    except FileExistsError:
        pass
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                 dir_fd=parent)
    info = os.fstat(fd)
    if _can_adopt_destination_chat(info, os.listdir(fd), uid=uid):
        os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o700)
        os.fsync(fd)
        os.fsync(parent)
        info = os.fstat(fd)
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != uid
            or info.st_gid != gid or stat.S_IMODE(info.st_mode) != 0o700):
        os.close(fd)
        _fail("UNSAFE_DESTINATION_CHAT_DIRECTORY")
    return fd


def _can_adopt_destination_chat(info, entries: list[str], *, uid: int) -> bool:
    return (stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and uid != 0
            and stat.S_IMODE(info.st_mode) == 0o700 and not entries)


def _open_regular(parent: int, name: str, *, uid: int, maximum: int,
                  mode: int = 0o600, gid: int | None = None) -> int:
    name = _component(name)
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != uid
            or (gid is not None and info.st_gid != gid)
            or stat.S_IMODE(info.st_mode) != mode or info.st_size > maximum):
        os.close(fd)
        _fail("UNSAFE_FILE")
    return fd


def _read_file(parent: int, name: str, *, uid: int, maximum: int,
               mode: int = 0o600, gid: int | None = None) -> bytes:
    fd = _open_regular(parent, name, uid=uid, maximum=maximum, mode=mode, gid=gid)
    try:
        chunks = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > maximum:
            _fail("OVERSIZED_FILE")
        return data
    finally:
        os.close(fd)


def _read_absolute(path: str, *, uid: int, maximum: int, mode: int = 0o600,
                   gid: int | None = None) -> bytes:
    parent_path, name = path.rsplit("/", 1)
    parent = _open_root(parent_path or "/")
    try:
        return _read_file(parent, name, uid=uid, maximum=maximum, mode=mode, gid=gid)
    finally:
        os.close(parent)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _digest_file(parent: int, name: str, *, uid: int, maximum: int) -> tuple[int, str]:
    fd = _open_regular(parent, name, uid=uid, maximum=maximum)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := os.read(fd, 1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    finally:
        os.close(fd)
    return size, digest.hexdigest()


def _write_file(parent: int, name: str, data: bytes, *, uid: int, gid: int):
    name = _component(name)
    temporary = ".tmp-" + uuid.uuid4().hex
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                 0o600, dir_fd=parent)
    try:
        os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.rename(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
        os.fsync(parent)
    finally:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=parent)


def _copy_control(source_parent: int, name: str, destination_parent: int, *,
                  source_uid: int, dest_uid: int, dest_gid: int,
                  expected_size: int, expected_sha: str, maximum: int) -> tuple[int, str]:
    """Stream a control file into a private candidate and verify source stability."""
    source_fd = _open_regular(source_parent, name, uid=source_uid, maximum=maximum)
    before = os.fstat(source_fd)
    if before.st_size != expected_size:
        os.close(source_fd)
        _fail("CONTROL_FILE_CHANGED")
    temporary = ".tmp-control-" + uuid.uuid4().hex
    output_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                        0o600, dir_fd=destination_parent)
    digest = hashlib.sha256()
    total = 0
    try:
        os.fchown(output_fd, dest_uid, dest_gid)
        os.fchmod(output_fd, 0o600)
        while chunk := os.read(source_fd, 1024 * 1024):
            total += len(chunk)
            if total > maximum:
                _fail("CONTROL_FILE_TOO_LARGE")
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(output_fd, view)
                view = view[written:]
        after = os.fstat(source_fd)
        actual = digest.hexdigest()
        if (_file_token(before) != _file_token(after) or total != expected_size
                or actual != expected_sha):
            _fail("CONTROL_FILE_HASH_MISMATCH")
        os.fsync(output_fd)
        os.close(output_fd)
        output_fd = -1
        os.rename(temporary, _component(name), src_dir_fd=destination_parent,
                  dst_dir_fd=destination_parent)
        os.fsync(destination_parent)
        return total, actual
    finally:
        os.close(source_fd)
        if output_fd >= 0:
            os.close(output_fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=destination_parent)


def _rename_noreplace(source_parent: int, source_name: str,
                      destination_parent: int, destination_name: str) -> None:
    """Atomically publish a new name without replacing an existing entry."""
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError:
        _fail("ATOMIC_NOREPLACE_UNAVAILABLE")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p,
                          ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_parent, os.fsencode(_component(source_name)),
        destination_parent, os.fsencode(_component(destination_name)), 1,
    )  # RENAME_NOREPLACE
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        _fail("DESTINATION_CONFLICT")
    raise OSError(error, os.strerror(error))


def _rename_exchange(parent: int, first: str, second: str) -> None:
    """Atomically exchange two complete sibling trees on the same filesystem."""
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError:
        _fail("ATOMIC_EXCHANGE_UNAVAILABLE")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p,
                          ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(parent, os.fsencode(_component(first)),
                       parent, os.fsencode(_component(second)), 2)  # RENAME_EXCHANGE
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.ENOENT, errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
        _fail("ATOMIC_EXCHANGE_UNAVAILABLE")
    raise OSError(error, os.strerror(error))


def _transfer_record(parent: int, name: str, *, uid: int):
    directory = _open_child(parent, name, uid=uid)
    try:
        raw = _read_file(directory, "telegram_transfer_manifest.json", uid=uid,
                         maximum=MAX_METADATA_BYTES)
        record = json.loads(raw)
        if not isinstance(record, dict) or not isinstance(record.get("source_id"), str):
            _fail("TRANSFER_MANIFEST_INVALID")
        status = _staged_status(parent, name, record["source_id"], dest_uid=uid)
        if not status:
            _fail("STAGING_REVISION_INVALID")
        return record, hashlib.sha256(raw).hexdigest()
    finally:
        os.close(directory)


def _media_manifest_sha(media_manifest: dict | None) -> str | None:
    if media_manifest is None:
        return None
    return _sha(json.dumps(media_manifest, sort_keys=True, separators=(",", ":")).encode())


def _temp_marker_matches(parent: int, name: str, *, uid: int, source_id: str,
                         source_hashes: dict, media_manifest_sha: str | None) -> bool:
    try:
        directory = _open_child(parent, name, uid=uid)
    except (OSError, TransferError):
        return False
    try:
        marker = json.loads(_read_file(directory, ".publisher-temp.json", uid=uid,
                                       maximum=MAX_METADATA_BYTES))
        return marker == {
            "source_id": source_id, "source_hashes": source_hashes,
            "media_manifest_sha256": media_manifest_sha,
        }
    except (TransferError, OSError, ValueError, UnicodeError):
        return False
    finally:
        os.close(directory)


def _read_init_intent(parent: int, name: str, *, uid: int) -> dict:
    try:
        value = json.loads(_read_file(parent, name + ".json", uid=uid,
                                      maximum=MAX_METADATA_BYTES))
    except (TransferError, OSError, ValueError, UnicodeError):
        _fail("TRANSFER_RECOVERY_REQUIRED")
    if (not isinstance(value, dict) or value.get("version") != 1
            or value.get("init_name") != name
            or not isinstance(value.get("source_id"), str)
            or not isinstance(value.get("source_hashes"), dict)
            or value.get("media_manifest_sha256") is not None
            and not re.fullmatch(r"[0-9a-f]{64}", str(value["media_manifest_sha256"]))):
        _fail("TRANSFER_RECOVERY_REQUIRED")
    return value


def _write_init_intent(parent: int, name: str, *, uid: int, gid: int,
                       source_id: str, source_hashes: dict,
                       media_manifest_sha: str | None) -> None:
    payload = (json.dumps({
        "version": 1, "init_name": name, "source_id": source_id,
        "source_hashes": source_hashes,
        "media_manifest_sha256": media_manifest_sha,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary = ".publisher-init-write-" + uuid.uuid4().hex + ".tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                 | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
    try:
        os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        _rename_noreplace(parent, temporary, parent, name + ".json")
        os.fsync(parent)
    finally:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=parent)


def _cleanup_init_intent_temporaries(parent: int, *, uid: int, gid: int) -> bool:
    """Remove only private, singly-linked scratch files from the exact writer namespace."""
    removed = False
    pattern = re.compile(r"\.publisher-init-write-[0-9a-f]{32}\.tmp\Z")
    for name in sorted(os.listdir(parent)):
        if not pattern.fullmatch(name):
            continue
        try:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != uid or info.st_gid != gid
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > MAX_METADATA_BYTES):
            _fail("TRANSFER_RECOVERY_REQUIRED")
        os.unlink(name, dir_fd=parent)
        removed = True
    if removed:
        os.fsync(parent)
    return removed


def _recover_initialization_directories(parent: int, *, uid: int, source_id: str,
                                        source_hashes: dict,
                                        media_manifest_sha: str | None,
                                        temp_name: str, gid: int) -> None:
    """Recover only scratch paths proven by a durable, matching intent file."""
    removed = _cleanup_init_intent_temporaries(parent, uid=uid, gid=gid)
    names = set(os.listdir(parent))
    init_re = re.compile(r"\.publisher-init-[0-9a-f]{32}\Z")
    for name in sorted(names):
        if not init_re.fullmatch(name):
            continue
        intent = _read_init_intent(parent, name, uid=uid)
        # A valid other-scope intent is isolated from this transfer. Leave it
        # for the matching source operation instead of blocking this one.
        if intent["source_id"] != source_id:
            continue
        if (intent["source_hashes"] != source_hashes
                or intent["media_manifest_sha256"] != media_manifest_sha):
            _fail("TRANSFER_RECOVERY_REQUIRED")
        if name in names:
            _remove_tree(parent, name, expected_uid=uid)
        os.unlink(name + ".json", dir_fd=parent)
        removed = True

    for name in sorted(names):
        if not re.fullmatch(r"\.publisher-init-[0-9a-f]{32}\.json\Z", name):
            continue
        init_name = name[:-5]
        if init_name in names:
            continue
        intent = _read_init_intent(parent, init_name, uid=uid)
        if intent["source_id"] != source_id:
            continue
        if (intent["source_hashes"] != source_hashes
                or intent["media_manifest_sha256"] != media_manifest_sha):
            _fail("TRANSFER_RECOVERY_REQUIRED")
        if temp_name in names and not _temp_marker_matches(
            parent, temp_name, uid=uid, source_id=source_id,
            source_hashes=source_hashes, media_manifest_sha=media_manifest_sha,
        ):
            _fail("TRANSFER_RECOVERY_REQUIRED")
        # No directory means the crash happened before mkdir, or the proven
        # directory was atomically renamed to the verified transfer candidate.
        os.unlink(name, dir_fd=parent)
        removed = True
    if removed:
        os.fsync(parent)


def _media_inventory_count(transfer: dict) -> int:
    if transfer.get("version") == 1:
        values = transfer.get("media_files")
        if isinstance(values, list):
            return len(values)
    elif transfer.get("version") == 2:
        spec = transfer.get("media_inventory")
        if (isinstance(spec, dict) and spec.get("name") == "media_files.jsonl"
                and type(spec.get("count")) is int and spec["count"] >= 0
                and isinstance(spec.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", spec["sha256"])):
            return spec["count"]
    _fail("TRANSFER_MANIFEST_INVALID")


def _iter_media_inventory(parent: int, transfer: dict, *, uid: int):
    """Yield v1 inline or v2 streamed inventory and verify v2 bytes at EOF."""
    if transfer.get("version") == 1:
        values = transfer.get("media_files")
        if not isinstance(values, list):
            _fail("TRANSFER_MANIFEST_INVALID")
        yield from values
        return
    count = _media_inventory_count(transfer)
    spec = transfer["media_inventory"]
    actual = 0
    if count:
        for item in _iter_jsonl(
            parent, spec["name"], uid=uid, expected_sha256=spec["sha256"],
        ):
            actual += 1
            yield item
    elif spec["sha256"] != _sha(b""):
        _fail("TRANSFER_MANIFEST_INVALID")
    if actual != count:
        _fail("TRANSFER_MANIFEST_INVALID")


def _revision_compatible(previous: dict, candidate: dict, *, source_hashes: dict,
                         media_manifest_sha: str | None, parent: int,
                         previous_name: str, candidate_name: str, uid: int) -> None:
    if previous.get("source_hashes") != source_hashes:
        _fail("DESTINATION_CONFLICT")
    if candidate.get("source_hashes") != source_hashes:
        _fail("DESTINATION_CONFLICT")
    if candidate.get("media_manifest_sha256") != media_manifest_sha:
        _fail("STAGING_REVISION_MISMATCH")
    previous_dir = _open_child(parent, previous_name, uid=uid)
    candidate_dir = _open_child(parent, candidate_name, uid=uid)
    try:
        def ordered(items):
            previous_id = None
            for item in items:
                if not isinstance(item, dict):
                    _fail("TRANSFER_MANIFEST_INVALID")
                message_id = item.get("message_id")
                if (type(message_id) is not int or message_id <= 0
                        or (previous_id is not None and message_id >= previous_id)):
                    _fail("TRANSFER_MANIFEST_INVALID")
                previous_id = message_id
                yield item

        old_iter = iter(ordered(_iter_media_inventory(previous_dir, previous, uid=uid)))
        new_iter = iter(ordered(_iter_media_inventory(candidate_dir, candidate, uid=uid)))
        old_item = next(old_iter, None)
        new_item = next(new_iter, None)
        while old_item is not None:
            if not isinstance(old_item, dict) or not isinstance(new_item, dict):
                _fail("TRANSFER_MANIFEST_INVALID")
            old_id, new_id = old_item.get("message_id"), new_item.get("message_id")
            while new_item is not None and new_id > old_id:
                new_item = next(new_iter, None)
                if new_item is not None:
                    new_id = new_item.get("message_id") if isinstance(new_item, dict) else None
            keys = ("message_id", "path", "size", "sha256")
            if new_item is None or new_id != old_id or any(
                new_item.get(key) != old_item.get(key) for key in keys
            ):
                _fail("NON_ADDITIVE_MEDIA_REVISION")
            old_item = next(old_iter, None)
            new_item = next(new_iter, None)
        # Exhaust both iterators so v2 inventory checksums/counts are verified.
        for _item in old_iter:
            _fail("TRANSFER_MANIFEST_INVALID")
        for _item in new_iter:
            pass
    finally:
        os.close(previous_dir)
        os.close(candidate_dir)


def _archive_displaced(parent: int, displaced_name: str, *, source_id: str, uid: int,
                       source_hashes: dict, desired_media_sha: str | None) -> None:
    previous, previous_sha = _transfer_record(parent, displaced_name, uid=uid)
    if previous.get("source_id") != source_id or previous.get("source_hashes") != source_hashes:
        _fail("DESTINATION_CONFLICT")
    if previous.get("media_manifest_sha256") == desired_media_sha:
        _fail("STAGING_REVISION_NOT_ADVANCED")
    try:
        os.mkdir(".revisions", 0o700, dir_fd=parent)
    except FileExistsError:
        pass
    revisions = _open_child(parent, ".revisions", uid=uid, mode=0o700)
    try:
        archive_name = "revision-" + previous_sha
        if archive_name in os.listdir(revisions):
            archived, archived_sha = _transfer_record(revisions, archive_name, uid=uid)
            if archived_sha != previous_sha or archived != previous:
                _fail("REVISION_ARCHIVE_CONFLICT")
            _remove_tree(parent, displaced_name, expected_uid=uid)
        else:
            _rename_noreplace(parent, displaced_name, revisions, archive_name)
        os.fsync(revisions)
        os.fsync(parent)
    finally:
        os.close(revisions)


def _copy_file(source_parent: int, source_name: str, destination_parent: int,
               destination_name: str, *, source_uid: int, dest_uid: int, dest_gid: int,
               expected_size: int, expected_sha: str, max_bytes: int):
    source_fd = _open_regular(source_parent, source_name, uid=source_uid, maximum=max_bytes)
    temporary = ".tmp-" + uuid.uuid4().hex
    dest_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                      0o600, dir_fd=destination_parent)
    digest = hashlib.sha256()
    total = 0
    try:
        os.fchown(dest_fd, dest_uid, dest_gid)
        os.fchmod(dest_fd, 0o600)
        while chunk := os.read(source_fd, 1024 * 1024):
            total += len(chunk)
            if total > max_bytes:
                _fail("FILE_LIMIT_EXCEEDED")
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(dest_fd, view)
                view = view[written:]
        if total != expected_size or digest.hexdigest() != expected_sha:
            _fail("MEDIA_HASH_MISMATCH")
        os.fsync(dest_fd)
        os.close(dest_fd)
        dest_fd = -1
        os.rename(temporary, _component(destination_name),
                  src_dir_fd=destination_parent, dst_dir_fd=destination_parent)
        os.fsync(destination_parent)
        return total
    finally:
        os.close(source_fd)
        if dest_fd >= 0:
            os.close(dest_fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=destination_parent)


def _walk_size(fd: int) -> int:
    total = 0
    for name in os.listdir(fd):
        _component(name)
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            _fail("UNSAFE_STORAGE_ENTRY")
        if stat.S_ISDIR(info.st_mode):
            child = _open_child(fd, name, uid=info.st_uid, mode=stat.S_IMODE(info.st_mode))
            try:
                total += _walk_size(child)
            finally:
                os.close(child)
        elif stat.S_ISREG(info.st_mode):
            if info.st_nlink != 1:
                _fail("UNSAFE_STORAGE_ENTRY")
            total += info.st_size
        else:
            _fail("UNSAFE_STORAGE_ENTRY")
    return total


def _remove_tree(parent: int, name: str, *, expected_uid: int):
    child = _open_child(parent, name, uid=expected_uid, mode=0o700)
    try:
        for item in os.listdir(child):
            _component(item)
            info = os.stat(item, dir_fd=child, follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                _fail("UNSAFE_TEMP_ENTRY")
            if stat.S_ISDIR(info.st_mode):
                _remove_tree(child, item, expected_uid=expected_uid)
            elif stat.S_ISREG(info.st_mode) and info.st_uid == expected_uid:
                if info.st_nlink != 1:
                    _fail("UNSAFE_TEMP_ENTRY")
                os.unlink(item, dir_fd=child)
            else:
                _fail("UNSAFE_TEMP_ENTRY")
    finally:
        os.close(child)
    os.rmdir(name, dir_fd=parent)


def _safe_config(config_path: str) -> dict:
    if not isinstance(config_path, str) or not config_path.startswith("/"):
        _fail("TRANSFER_CONFIG_INVALID")
    raw = _read_absolute(config_path, uid=os.geteuid(), maximum=16 * 1024, mode=0o600)
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeError):
        _fail("TRANSFER_CONFIG_INVALID")
    expected_keys = {
        "version", "data_root", "source_root", "destination_root", "export_config",
        "lock_path", "expected_data_uuid", "require_mount", "storage_limit_bytes",
        "min_free_bytes", "source_uid", "source_gid", "destination_uid",
        "destination_gid",
    }
    if not isinstance(data, dict) or set(data) != expected_keys or data.get("version") != 1:
        _fail("TRANSFER_CONFIG_INVALID")
    for key in ("data_root", "source_root", "destination_root", "export_config", "lock_path"):
        value = data.get(key)
        if (not isinstance(value, str) or not value.startswith("/")
                or "\x00" in value or ".." in PurePosixPath(value).parts):
            _fail("TRANSFER_CONFIG_INVALID")
    try:
        uuid.UUID(data["expected_data_uuid"]) if data["expected_data_uuid"] is not None else None
    except (ValueError, TypeError, AttributeError):
        _fail("TRANSFER_CONFIG_INVALID")
    if (type(data["require_mount"]) is not bool
            or (data["require_mount"] and data["expected_data_uuid"] is None)
            or type(data["storage_limit_bytes"]) is not int
            or not 1024**2 <= data["storage_limit_bytes"] <= MAX_STORAGE_BYTES
            or type(data["min_free_bytes"]) is not int
            or not 0 <= data["min_free_bytes"] < data["storage_limit_bytes"]
            or any(type(data[k]) is not int or data[k] < 0
                   for k in ("source_uid", "source_gid", "destination_uid", "destination_gid"))):
        _fail("TRANSFER_CONFIG_INVALID")
    if not data["source_uid"] or not data["destination_uid"] or not data["source_gid"]:
        _fail("TRANSFER_CONFIG_INVALID")
    # Both managed trees must remain beneath the configured storage boundary.
    root = os.path.normpath(data["data_root"])
    source_root = os.path.normpath(data["source_root"])
    destination_root = os.path.normpath(data["destination_root"])
    for key in ("source_root", "destination_root"):
        candidate = os.path.normpath(data[key])
        if os.path.commonpath((root, candidate)) != root or candidate == root:
            _fail("TRANSFER_CONFIG_INVALID")
    if os.path.commonpath((source_root, destination_root)) in {source_root, destination_root}:
        _fail("TRANSFER_CONFIG_INVALID")
    return data


def _check_mount(config: dict):
    if not config["require_mount"]:
        return
    mountpoint = config["data_root"]
    if not os.path.ismount(mountpoint):
        _fail("DATA_DISK_NOT_MOUNTED")
    try:
        result = subprocess.run(
            ["findmnt", "-n", "-o", "UUID", "--target", mountpoint],
            capture_output=True, check=False, timeout=5,
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        )
    except (OSError, subprocess.SubprocessError):
        _fail("DATA_DISK_CHECK_FAILED")
    actual = result.stdout.decode("ascii", "ignore").strip()
    if result.returncode or actual.lower() != config["expected_data_uuid"].lower():
        _fail("DATA_DISK_UUID_MISMATCH")


def _export_config(config: dict) -> tuple[int, int]:
    raw = _read_absolute(config["export_config"], uid=os.geteuid(), gid=config["source_gid"],
                         maximum=16 * 1024, mode=0o640)
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeError):
        _fail("EXPORT_CONFIG_INVALID")
    if (not isinstance(data, dict) or type(data.get("storage_limit_bytes")) is not int
            or type(data.get("min_free_bytes")) is not int
            or not 1024**2 <= data["storage_limit_bytes"] <= MAX_STORAGE_BYTES
            or not 0 <= data["min_free_bytes"] < data["storage_limit_bytes"]):
        _fail("EXPORT_CONFIG_INVALID")
    return data["storage_limit_bytes"], data["min_free_bytes"]


def _storage_roots(config: dict) -> tuple[int, int]:
    source = _open_root(config["source_root"])
    destination = _open_root(config["destination_root"])
    source_info = os.fstat(source)
    destination_info = os.fstat(destination)
    if (source_info.st_uid != config["source_uid"] or stat.S_IMODE(source_info.st_mode) != 0o700
            or destination_info.st_uid != config["destination_uid"]
            or stat.S_IMODE(destination_info.st_mode) != 0o700):
        os.close(source)
        os.close(destination)
        _fail("UNSAFE_STORAGE_ROOT")
    return source, destination


def _available(config: dict) -> tuple[int, int, int]:
    export_limit, reserve = _export_config(config)
    if reserve != config["min_free_bytes"] or export_limit != config["storage_limit_bytes"]:
        _fail("STORAGE_POLICY_MISMATCH")
    source, destination = _storage_roots(config)
    try:
        used = _walk_size(source) + _walk_size(destination)
        usage = os.statvfs(config["data_root"])
        free = usage.f_bavail * usage.f_frsize
    finally:
        os.close(source)
        os.close(destination)
    return max(0, min(export_limit - used, free - reserve)), free, used


def _check_pipeline_storage(available: int, required: int) -> None:
    """Reserve one publisher copy and one later worker-staging copy."""
    if type(available) is not int or type(required) is not int or required < 0:
        _fail("STORAGE_BUDGET_INVALID")
    if required * 2 > available:
        _fail("INSUFFICIENT_SHARED_STORAGE")


def _iter_jsonl(parent: int, name: str, *, uid: int, maximum: int = MAX_CONTROL_BYTES,
                expected_sha256: str | None = None):
    """Read a control JSONL one bounded record at a time and verify file stability."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                 dir_fd=parent)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != uid
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > maximum):
            _fail("CONTROL_FILE_UNSAFE")
        total = 0
        digest = hashlib.sha256()
        with os.fdopen(os.dup(fd), "rb") as stream:
            while raw := stream.readline(MAX_JSONL_LINE_BYTES + 1):
                total += len(raw)
                if len(raw) > MAX_JSONL_LINE_BYTES or total > maximum:
                    _fail("CONTROL_FILE_TOO_LARGE")
                digest.update(raw)
                try:
                    value = json.loads(raw)
                except (ValueError, UnicodeError):
                    _fail("MESSAGES_INVALID" if name == "messages.jsonl" else "SERVICES_INVALID")
                if not isinstance(value, dict):
                    _fail("MESSAGES_INVALID" if name == "messages.jsonl" else "SERVICES_INVALID")
                yield value
        after = os.fstat(fd)
        if (_file_token(before) != _file_token(after) or total != before.st_size):
            _fail("CONTROL_FILE_CHANGED")
        if expected_sha256 is not None and digest.hexdigest() != expected_sha256:
            _fail("MESSAGES_HASH_MISMATCH" if name == "messages.jsonl" else "SERVICES_HASH_MISMATCH")
    finally:
        os.close(fd)


def _file_token(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _validated_message_rows(parent: int, *, chat: str, topic: int | None,
                            scope: str, uid: int, expected_sha256: str | None = None):
    count = 0
    previous_id = None
    for row in _iter_jsonl(parent, "messages.jsonl", uid=uid,
                            expected_sha256=expected_sha256):
        message_id = row.get("message_id")
        row_topic = row.get("topic_id")
        if (type(message_id) is not int or message_id <= 0
                or (scope == "topic" and row_topic != topic)
                or (scope == "chat" and row_topic is not None
                    and (type(row_topic) is not int or row_topic <= 0))
                or ("chat_id" in row and str(row.get("chat_id")) != chat)
                or not isinstance(row.get("date"), str)
                or not isinstance(row.get("text"), str)
                or (previous_id is not None and message_id >= previous_id)):
            _fail("MESSAGES_INVALID")
        previous_id = message_id
        media = row.get("media")
        if media is not None and not isinstance(media, dict):
            _fail("MEDIA_METADATA_INVALID")
        count += 1
        yield row
    return count


def _read_control(source: int, name: str, expected_uid: int, maximum: int) -> bytes:
    return _read_file(source, name, uid=expected_uid, maximum=maximum)


def _media_source(source: int, path: str, message_id: int, *, source_uid: int,
                  limit: int, expected_size: int, expected_sha: str) -> tuple[int, str, str]:
    if not isinstance(path, str) or "\\" in path or "\x00" in path:
        _fail("UNSAFE_MEDIA_PATH")
    parts = path.split("/")
    if (len(parts) != 3 or parts[0] not in {"media", "media-resume"}
            or parts[1] != str(message_id) or any(p in {"", ".", ".."} for p in parts)):
        _fail("UNSAFE_MEDIA_PATH")
    media_root = _open_child(source, parts[0], uid=source_uid, mode=0o700)
    try:
        item_dir = _open_child(media_root, parts[1], uid=source_uid, mode=0o700)
        try:
            if parts[0] == "media":
                receipt_bytes = _read_file(item_dir, "receipt.json", uid=source_uid,
                                           maximum=MAX_METADATA_BYTES)
                receipt = json.loads(receipt_bytes)
                if (receipt.get("name") != parts[2] or receipt.get("size") != expected_size
                        or receipt.get("sha256") != expected_sha):
                    _fail("MEDIA_RECEIPT_MISMATCH")
            try:
                actual_size, actual_sha = _digest_file(item_dir, parts[2], uid=source_uid,
                                                       maximum=limit)
            except OSError:
                _fail("MEDIA_FILE_UNAVAILABLE_OR_UNSAFE")
            if actual_size != expected_size or actual_sha != expected_sha:
                _fail("MEDIA_HASH_MISMATCH")
            return media_root, item_dir, parts[2]
        except BaseException:
            os.close(item_dir)
            raise
    except BaseException:
        os.close(media_root)
        raise


def _copy_media_reference(source: int, dest: int, media: dict, message_id: int,
                          *, source_uid: int, dest_uid: int, dest_gid: int,
                          max_file_bytes: int) -> tuple[str, int, str]:
    path = media.get("path")
    size = media.get("size_bytes", media.get("size"))
    digest = media.get("sha256")
    limit = media.get("max_file_bytes", max_file_bytes)
    if (type(size) is not int or not 0 < size <= max_file_bytes or size > limit
            or type(limit) is not int or not 0 < limit <= max_file_bytes
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        _fail("MEDIA_METADATA_INVALID")
    opened = _media_source(source, path, message_id, source_uid=source_uid,
                           limit=limit, expected_size=size, expected_sha=digest)
    media_root, item_dir, filename = opened
    try:
        dst_media = _open_child(dest, "media", create=True, uid=dest_uid, gid=dest_gid)
        try:
            dst_item = _open_child(dst_media, str(message_id), create=True,
                                   uid=dest_uid, gid=dest_gid)
            try:
                _copy_file(item_dir, filename, dst_item, filename, source_uid=source_uid,
                           dest_uid=dest_uid, dest_gid=dest_gid, expected_size=size,
                           expected_sha=digest, max_bytes=limit)
            finally:
                os.close(dst_item)
        finally:
            os.close(dst_media)
    finally:
        os.close(item_dir)
        os.close(media_root)
    return f"media/{message_id}/{filename}", size, digest


def _validate_media_reference(source: int, media: dict, message_id: int, *,
                              source_uid: int, max_file_bytes: int) -> dict:
    path = media.get("path")
    size = media.get("size_bytes", media.get("size"))
    digest = media.get("sha256")
    limit = media.get("max_file_bytes", max_file_bytes)
    if (type(size) is not int or not 0 < size <= max_file_bytes
            or type(limit) is not int or not 0 < limit <= max_file_bytes or size > limit
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        _fail("MEDIA_METADATA_INVALID")
    opened = _media_source(source, path, message_id, source_uid=source_uid,
                           limit=limit, expected_size=size, expected_sha=digest)
    media_root, item_dir, filename = opened
    try:
        return {"name": filename, "size": size, "sha256": digest, "limit": limit}
    finally:
        os.close(item_dir)
        os.close(media_root)


def _media_inputs(source: int, rows, checkpoint: dict | None,
                  media_manifest: dict | None, source_manifest: dict,
                  *, source_uid: int):
    record_map = checkpoint.get("records", {}) if checkpoint else {}
    expected = set()
    unresolved = False
    for row in rows:
        media = row.get("media")
        if isinstance(media, dict) and not media.get("path"):
            expected.add(str(row["message_id"]))
            unresolved = unresolved or media.get("export_status") != "skipped"
    if checkpoint is None:
        if unresolved:
            _fail("MEDIA_EXPORT_NOT_READY")
        # Legacy manifests use the original 100 MiB default. New exports
        # record the selected bound explicitly, up to the storage ceiling.
        max_file = source_manifest.get("max_file_bytes", DEFAULT_FILE_BYTES)
        if type(max_file) is not int or not 0 < max_file <= MAX_FILE_BYTES:
            _fail("MEDIA_LIMIT_INVALID")
        return {}, max_file, None
    resume_dir = _open_child(source, "media-resume", uid=source_uid)
    try:
        current_checkpoint_sha = _sha(_read_file(
            resume_dir, "status.json", uid=source_uid, maximum=MAX_CHECKPOINT_BYTES,
        ))
    finally:
        os.close(resume_dir)
    if (checkpoint.get("version") != 2 or not isinstance(record_map, dict)
            or set(record_map) != expected or not media_manifest
            or media_manifest.get("source_id") != checkpoint.get("source_id")
            or media_manifest.get("complete") is not True
            or media_manifest.get("download_errors") != 0
            or media_manifest.get("checkpoint_sha256") != current_checkpoint_sha):
        _fail("MEDIA_EXPORT_INCOMPLETE")
    max_file = media_manifest.get("requested_max_file_bytes")
    if (type(max_file) is not int or not 0 < max_file <= MAX_FILE_BYTES
            or checkpoint.get("requested_max_file_bytes") != max_file
            or media_manifest.get("media_count") != len(expected)
            or media_manifest.get("remaining") != 0):
        _fail("MEDIA_LIMIT_INVALID")
    for mid, record in record_map.items():
        if not isinstance(record, dict):
            _fail("MEDIA_RECORD_INVALID")
        status = record.get("export_status")
        if status == "skipped":
            if record.get("reason") not in ALLOWED_SKIP_REASONS:
                _fail("MEDIA_SKIP_REASON_INVALID")
        elif status == "downloaded":
            if not mid.isdecimal() or int(mid) <= 0:
                _fail("MEDIA_RECORD_INVALID")
            _validate_media_reference(source, record, int(mid), source_uid=source_uid,
                                      max_file_bytes=max_file)
        else:
            _fail("MEDIA_RECORD_INVALID")
    return record_map, max_file, media_manifest


def _read_transfer_inputs(source: int, source_id: str, chat: str, scope_dir: str,
                          source_uid: int):
    parsed = parse_telegram_source(source_id)
    if parsed is None:
        _fail("INVALID_SOURCE_ID")
    _chat, topic_text = parsed
    scope = "chat" if topic_text is None else "topic"
    topic = None if topic_text is None else int(topic_text)
    manifest_bytes = _read_control(source, "manifest.json", source_uid, MAX_METADATA_BYTES)
    services_bytes = _read_control(source, "services_manifest.json", source_uid,
                                   MAX_SERVICES_MANIFEST_BYTES)
    try:
        manifest = json.loads(manifest_bytes)
        service_manifest = json.loads(services_bytes)
    except (ValueError, UnicodeError):
        _fail("EXPORT_MANIFEST_INVALID")
    sizes, hashes = {}, {}
    for name in ("messages.jsonl", "services.jsonl"):
        size, digest = _digest_file(source, name, uid=source_uid, maximum=MAX_CONTROL_BYTES)
        sizes[name] = size
        hashes[name] = digest
    hashes["manifest.json"] = _sha(manifest_bytes)
    hashes["services_manifest.json"] = _sha(services_bytes)
    if (manifest.get("export_version") != 1 or manifest.get("source_id") != source_id
            or str(manifest.get("chat_id")) != chat or manifest.get("topic_id") != topic
            or manifest.get("scope", "topic") != scope
            or manifest.get("complete") is not True
            or manifest.get("range_complete") is not True or manifest.get("scope_before") is not None
            or manifest.get("errors") not in ([], None)
            or manifest.get("messages_sha256") != hashes["messages.jsonl"]
            or type(manifest.get("message_count")) is not int):
        _fail("EXPORT_MANIFEST_MISMATCH")
    if (service_manifest.get("source_id") != source_id
            or service_manifest.get("chat_id") != int(chat)
            or service_manifest.get("topic_id") != topic
            or service_manifest.get("scope", scope) != scope
            or service_manifest.get("complete") is not True
            or service_manifest.get("range_complete") is not True
            or service_manifest.get("scope_before") is not None
            or service_manifest.get("errors") not in ([], None)
            or service_manifest.get("services_sha256") != hashes["services.jsonl"]
            or type(service_manifest.get("event_count")) is not int):
        _fail("SERVICES_MANIFEST_MISMATCH")
    row_count = sum(1 for _row in _validated_message_rows(
        source, chat=chat, topic=topic, scope=scope, uid=source_uid,
        expected_sha256=hashes["messages.jsonl"],
    ))
    if row_count != manifest["message_count"]:
        _fail("MESSAGE_COUNT_MISMATCH")
    service_count = 0
    previous_event = set()
    for event in _iter_jsonl(source, "services.jsonl", uid=source_uid):
        if (not isinstance(event, dict) or type(event.get("event_id")) is not int
                or event["event_id"] <= 0
                or (scope == "topic" and event.get("topic_id") != topic)
                or (scope == "chat" and event.get("topic_id") is not None
                    and (type(event.get("topic_id")) is not int or event["topic_id"] <= 0))
                or event["event_id"] in previous_event
                or event.get("type") not in EVENT_TYPES):
            _fail("SERVICES_INVALID")
        previous_event.add(event["event_id"])
        service_count += 1
    if service_count != service_manifest["event_count"]:
        _fail("SERVICE_COUNT_MISMATCH")
    if (scope_dir != telegram_scope_directory(topic)
            or hashes["manifest.json"] != manifest.get("manifest_sha256", hashes["manifest.json"])):
        _fail("EXPORT_MANIFEST_MISMATCH")
    return manifest, service_manifest, hashes, sizes, manifest_bytes, services_bytes


def _load_resume(source: int, hashes: dict, source_id: str, source_uid: int):
    names = os.listdir(source)
    if "media-resume" not in names:
        return None, None
    resume = _open_child(source, "media-resume", uid=source_uid)
    try:
        checkpoint_bytes = _read_file(resume, "status.json", uid=source_uid,
                                      maximum=MAX_CHECKPOINT_BYTES)
        checkpoint = json.loads(checkpoint_bytes)
        media_manifest = json.loads(_read_file(resume, "manifest.json", uid=source_uid,
                                               maximum=MAX_METADATA_BYTES))
    except BaseException:
        os.close(resume)
        raise
    os.close(resume)
    current_checkpoint_sha = _sha(checkpoint_bytes)
    if (checkpoint.get("version") != 2 or checkpoint.get("source_id") != source_id
            or checkpoint.get("inputs") != hashes
            or media_manifest.get("inputs") != hashes
            or media_manifest.get("source_id") != source_id
            or media_manifest.get("checkpoint_sha256") != current_checkpoint_sha):
        _fail("MEDIA_CHECKPOINT_MISMATCH")
    return checkpoint, media_manifest


def _new_topic_dir(parent: int, name: str, *, uid: int, gid: int) -> int:
    try:
        os.mkdir(name, 0o700, dir_fd=parent)
    except FileExistsError:
        _fail("DESTINATION_CONFLICT")
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    os.fchown(fd, uid, gid)
    os.fchmod(fd, 0o700)
    return fd


def _staged_status(destination_chat: int, scope_dir: str, source_id: str,
                   *, dest_uid: int) -> dict | None:
    try:
        final = _open_child(destination_chat, scope_dir, uid=dest_uid)
    except FileNotFoundError:
        return None
    try:
        transfer = json.loads(_read_file(final, "telegram_transfer_manifest.json", uid=dest_uid,
                                         maximum=MAX_METADATA_BYTES))
        if transfer.get("source_id") != source_id:
            _fail("DESTINATION_CONFLICT")
        inventory_count = _media_inventory_count(transfer)
        limit = transfer.get("max_file_bytes")
        message_count = transfer.get("message_count")
        service_count = transfer.get("service_event_count")
        downloaded = transfer.get("downloaded_media")
        skipped = transfer.get("skipped_media")
        media_manifest_sha = transfer.get("media_manifest_sha256")
        if (type(limit) is not int or not 0 < limit <= MAX_FILE_BYTES
                or type(message_count) is not int or message_count < 0
                or type(service_count) is not int or service_count < 0
                or type(downloaded) is not int or downloaded != inventory_count
                or type(skipped) is not int or skipped < 0
                or (media_manifest_sha is not None and
                    (not isinstance(media_manifest_sha, str)
                     or not re.fullmatch(r"[0-9a-f]{64}", media_manifest_sha)))):
            _fail("TRANSFER_MANIFEST_INVALID")
        staged_hashes = transfer.get("staged_hashes")
        expected_files = {"messages.jsonl", "services.jsonl", "manifest.json", "services_manifest.json"}
        if not isinstance(staged_hashes, dict) or set(staged_hashes) != expected_files:
            _fail("TRANSFER_MANIFEST_INVALID")
        expected_entries = expected_files | {"telegram_transfer_manifest.json"}
        if transfer.get("version") == 2 and inventory_count:
            expected_entries.add("media_files.jsonl")
        if media_manifest_sha is not None:
            expected_entries.add("media_manifest.json")
            media_manifest = json.loads(_read_file(
                final, "media_manifest.json", uid=dest_uid, maximum=MAX_METADATA_BYTES,
            ))
            if not isinstance(media_manifest, dict):
                _fail("MEDIA_MANIFEST_INVALID")
            canonical_media_manifest = json.dumps(
                media_manifest, sort_keys=True, separators=(",", ":"),
            ).encode()
            if _sha(canonical_media_manifest) != media_manifest_sha:
                _fail("MEDIA_MANIFEST_HASH_MISMATCH")
        if inventory_count:
            expected_entries.add("media")
        if set(os.listdir(final)) != expected_entries:
            _fail("UNEXPECTED_STAGING_ENTRY")
        for filename, expected in staged_hashes.items():
            size, digest = _digest_file(final, filename, uid=dest_uid, maximum=MAX_CONTROL_BYTES)
            if digest != expected:
                _fail("STAGING_HASH_MISMATCH")
        staged_manifest = json.loads(_read_file(
            final, "manifest.json", uid=dest_uid, maximum=MAX_METADATA_BYTES,
        ))
        staged_services = json.loads(_read_file(
            final, "services_manifest.json", uid=dest_uid,
            maximum=MAX_SERVICES_MANIFEST_BYTES,
        ))
        if not isinstance(staged_manifest, dict) or not isinstance(staged_services, dict):
            _fail("STAGING_MANIFEST_INVALID")
        chat, parsed_topic = parse_telegram_source(source_id)
        scope = "chat" if parsed_topic is None else "topic"
        topic_id = None if parsed_topic is None else int(parsed_topic)
        source_hashes = transfer.get("source_hashes")
        if (not isinstance(source_hashes, dict)
                or set(source_hashes) != expected_files
                or any(not isinstance(value, str)
                       or not re.fullmatch(r"[0-9a-f]{64}", value)
                       for value in source_hashes.values())):
            _fail("TRANSFER_MANIFEST_INVALID")
        transfer_summary = staged_manifest.get("telegram_transfer")
        if (staged_manifest.get("source_id") != source_id
                or staged_manifest.get("chat_id") != chat
                or staged_manifest.get("topic_id") != topic_id
                or staged_manifest.get("scope", "topic") != scope
                or staged_manifest.get("messages_sha256") != staged_hashes["messages.jsonl"]
                or staged_manifest.get("message_count") != message_count
                or not isinstance(transfer_summary, dict)
                or transfer_summary.get("downloaded_media") != downloaded
                or transfer_summary.get("skipped_media") != skipped
                or staged_manifest.get("max_file_bytes") != limit
                or staged_services.get("source_id") != source_id
                or staged_services.get("chat_id") != int(chat)
                or staged_services.get("topic_id") != topic_id
                or staged_services.get("scope", scope) != scope
                or staged_services.get("services_sha256") != staged_hashes["services.jsonl"]
                or staged_services.get("event_count") != service_count):
            _fail("STAGING_IDENTITY_MISMATCH")
        media_root = _open_child(final, "media", uid=dest_uid) if inventory_count else None
        previous_media_id = None
        validated_media = 0
        validated_bytes = 0
        try:
            for item in _iter_media_inventory(final, transfer, uid=dest_uid):
                if not isinstance(item, dict) or type(item.get("message_id")) is not int:
                    _fail("TRANSFER_MANIFEST_INVALID")
                message_id = item["message_id"]
                if message_id <= 0 or (previous_media_id is not None and message_id >= previous_media_id):
                    _fail("TRANSFER_MANIFEST_INVALID")
                previous_media_id = message_id
                path = item.get("path")
                parts = path.split("/") if isinstance(path, str) else []
                size_expected = item.get("size")
                sha_expected = item.get("sha256")
                if (len(parts) != 3 or parts[0] != "media"
                        or parts[1] != str(message_id)
                        or not isinstance(sha_expected, str)
                        or not re.fullmatch(r"[0-9a-f]{64}", sha_expected)
                        or type(size_expected) is not int or not 0 < size_expected <= MAX_FILE_BYTES
                        or size_expected > limit):
                    _fail("TRANSFER_MANIFEST_INVALID")
                _component(parts[2])
                media_dir = _open_child(media_root, parts[1], uid=dest_uid)
                try:
                    if set(os.listdir(media_dir)) != {parts[2]}:
                        _fail("UNEXPECTED_STAGING_MEDIA_ENTRY")
                    size, digest = _digest_file(media_dir, parts[2], uid=dest_uid,
                                                maximum=MAX_FILE_BYTES)
                finally:
                    os.close(media_dir)
                if size != size_expected or digest != sha_expected:
                    _fail("STAGING_MEDIA_HASH_MISMATCH")
                validated_media += 1
                validated_bytes += size
            if validated_media != inventory_count:
                _fail("TRANSFER_MANIFEST_INVALID")
            if transfer.get("copied_bytes") != validated_bytes:
                _fail("TRANSFER_MANIFEST_INVALID")
            if media_root is not None:
                directory_count = 0
                with os.scandir(media_root) as entries:
                    for entry in entries:
                        if not entry.is_dir(follow_symlinks=False):
                            _fail("UNEXPECTED_STAGING_MEDIA_ENTRY")
                        directory_count += 1
                if directory_count != inventory_count:
                    _fail("UNEXPECTED_STAGING_MEDIA_ENTRY")
        finally:
            if media_root is not None:
                os.close(media_root)
        return {
            "source_id": source_id, "status": "ready",
            "message_count": transfer.get("message_count"),
            "service_event_count": transfer.get("service_event_count"),
            "downloaded_media": transfer.get("downloaded_media"),
            "skipped_media": transfer.get("skipped_media"),
            "max_file_bytes": transfer.get("max_file_bytes"),
            "messages_sha256": transfer.get("staged_hashes", {}).get("messages.jsonl"),
        }
    finally:
        os.close(final)


@contextlib.contextmanager
def _exclusive_lock(config: dict):
    parent_path, name = config["lock_path"].rsplit("/", 1)
    parent = _open_root(parent_path)
    try:
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                _fail("UNSAFE_TRANSFER_LOCK")
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)
    finally:
        os.close(parent)


def _atomic_publish(source_id: str, chat: str, scope_dir: str, config: dict):
    source_root, destination_root = _storage_roots(config)
    try:
        source_chat = _open_child(source_root, chat, uid=config["source_uid"])
        try:
            source = _open_child(source_chat, scope_dir, uid=config["source_uid"])
        finally:
            os.close(source_chat)
        try:
            source_manifest, service_manifest, hashes, sizes, manifest_bytes, services_bytes = _read_transfer_inputs(
                source, source_id, chat, scope_dir, config["source_uid"],
            )
            parsed_source = parse_telegram_source(source_id)
            if parsed_source is None:
                _fail("INVALID_SOURCE_ID")
            _parsed_chat, topic_text = parsed_source
            scope = "chat" if topic_text is None else "topic"
            topic_id = None if topic_text is None else int(topic_text)
            checkpoint, media_manifest = _load_resume(
                source, hashes, source_id, config["source_uid"],
            )
            record_map, max_file, media_manifest = _media_inputs(
                source, _validated_message_rows(
                    source, chat=chat, topic=topic_id, scope=scope,
                    uid=config["source_uid"], expected_sha256=hashes["messages.jsonl"],
                ), checkpoint, media_manifest, source_manifest,
                source_uid=config["source_uid"],
            )
            destination_chat = _destination_chat(
                destination_root, chat, uid=config["destination_uid"],
                gid=config["destination_gid"],
            )
            try:
                desired_media_sha = _media_manifest_sha(media_manifest)
                temp_name = ".transfer-" + hashlib.sha256(
                    (source_id + json.dumps(hashes, sort_keys=True) + (desired_media_sha or "none"))
                    .encode()
                ).hexdigest()[:32]
                _recover_initialization_directories(
                    destination_chat, uid=config["destination_uid"],
                    gid=config["destination_gid"],
                    source_id=source_id, source_hashes=hashes,
                    media_manifest_sha=desired_media_sha,
                    temp_name=temp_name,
                )
                final_status = _staged_status(destination_chat, scope_dir, source_id,
                                              dest_uid=config["destination_uid"])
                # Recover the three crash windows around the atomic publish:
                # a marked partial tree is safe to discard, a complete staged
                # candidate can be promoted, and an exchanged old tree is
                # archived before returning idempotently.
                if temp_name in os.listdir(destination_chat):
                    try:
                        temp_record, _temp_sha = _transfer_record(
                            destination_chat, temp_name, uid=config["destination_uid"],
                        )
                    except (TransferError, OSError):
                        if not _temp_marker_matches(
                            destination_chat, temp_name, uid=config["destination_uid"],
                            source_id=source_id, source_hashes=hashes,
                            media_manifest_sha=desired_media_sha,
                        ):
                            _fail("TRANSFER_RECOVERY_REQUIRED")
                        _remove_tree(destination_chat, temp_name,
                                     expected_uid=config["destination_uid"])
                        # Persist removal of the discarded partial candidate so
                        # a crash cannot resurrect a stale directory entry.
                        os.fsync(destination_chat)
                        final_status = _staged_status(
                            destination_chat, scope_dir, source_id,
                            dest_uid=config["destination_uid"],
                        )
                    else:
                        if (temp_record.get("source_hashes") != hashes):
                            _fail("TRANSFER_RECOVERY_REQUIRED")
                        if temp_record.get("media_manifest_sha256") == desired_media_sha:
                            if final_status:
                                previous, _ = _transfer_record(
                                    destination_chat, scope_dir, uid=config["destination_uid"],
                                )
                                _revision_compatible(
                                    previous, temp_record, source_hashes=hashes,
                                    media_manifest_sha=desired_media_sha,
                                    parent=destination_chat, previous_name=scope_dir,
                                    candidate_name=temp_name, uid=config["destination_uid"],
                                )
                                _rename_exchange(destination_chat, temp_name, scope_dir)
                                _archive_displaced(
                                    destination_chat, temp_name, source_id=source_id,
                                    uid=config["destination_uid"], source_hashes=hashes,
                                    desired_media_sha=desired_media_sha,
                                )
                                final_status = _staged_status(
                                    destination_chat, scope_dir, source_id,
                                    dest_uid=config["destination_uid"],
                                )
                                os.fsync(destination_chat)
                                return {**final_status, "status": "ready"}
                            _rename_noreplace(destination_chat, temp_name,
                                              destination_chat, scope_dir)
                            os.fsync(destination_chat)
                            return {**_staged_status(
                                destination_chat, scope_dir, source_id,
                                dest_uid=config["destination_uid"],
                            ), "status": "ready"}
                        if final_status:
                            current, _ = _transfer_record(
                                destination_chat, scope_dir, uid=config["destination_uid"],
                            )
                            if (current.get("source_hashes") == hashes
                                    and current.get("media_manifest_sha256") == desired_media_sha):
                                _archive_displaced(
                                    destination_chat, temp_name, source_id=source_id,
                                    uid=config["destination_uid"], source_hashes=hashes,
                                    desired_media_sha=desired_media_sha,
                                )
                                final_status = _staged_status(
                                    destination_chat, scope_dir, source_id,
                                    dest_uid=config["destination_uid"],
                                )
                            else:
                                _fail("TRANSFER_RECOVERY_REQUIRED")
                if final_status:
                    final_dir = _open_child(destination_chat, scope_dir, uid=config["destination_uid"])
                    try:
                        transfer = json.loads(_read_file(
                            final_dir, "telegram_transfer_manifest.json",
                            uid=config["destination_uid"], maximum=MAX_METADATA_BYTES,
                        ))
                    finally:
                        os.close(final_dir)
                    if transfer.get("source_hashes") != hashes:
                        _fail("DESTINATION_CONFLICT")
                    if transfer.get("media_manifest_sha256") == desired_media_sha:
                        final_status["status"] = "already_ready"
                        os.fsync(destination_chat)
                        return final_status

                def transform(row):
                    media = row.get("media")
                    if media is None:
                        return dict(row), None, False
                    if not isinstance(media, dict):
                        _fail("MEDIA_METADATA_INVALID")
                    mid = str(row["message_id"])
                    record = media if media.get("path") else record_map.get(mid, media)
                    if not isinstance(record, dict):
                        _fail("MEDIA_EXPORT_INCOMPLETE")
                    staged_row = dict(row)
                    staged_media = dict(media)
                    if record.get("export_status") == "downloaded" and record.get("path"):
                        checked = _validate_media_reference(
                            source, record, row["message_id"], source_uid=config["source_uid"],
                            max_file_bytes=max_file,
                        )
                        staged_media.update({
                            "path": f"media/{mid}/{checked['name']}",
                            "size": checked["size"], "size_bytes": checked["size"],
                            "sha256": checked["sha256"], "max_file_bytes": checked["limit"],
                            "export_status": "downloaded",
                        })
                        staged_media.pop("reason", None)
                        staged_row["media"] = staged_media
                        return staged_row, (record, row["message_id"], checked), False
                    if record.get("export_status") != "skipped":
                        _fail("MEDIA_RECORD_INVALID")
                    reason = record.get("reason")
                    if reason not in ALLOWED_SKIP_REASONS:
                        _fail("MEDIA_SKIP_REASON_INVALID")
                    staged_media.pop("path", None)
                    staged_media.update({
                        "export_status": "skipped", "reason": reason,
                        "size_bytes": record.get("size_bytes", media.get("size_bytes")),
                        "size_source": record.get("size_source", media.get("size_source")),
                    })
                    staged_row["media"] = staged_media
                    return staged_row, None, True

                transformed_bytes = 0
                media_bytes = 0
                copied_media = 0
                skipped_media = 0
                for row in _validated_message_rows(
                    source, chat=chat, topic=topic_id, scope=scope,
                    uid=config["source_uid"], expected_sha256=hashes["messages.jsonl"],
                ):
                    staged_row, plan, skipped = transform(row)
                    transformed_bytes += len(json.dumps(
                        staged_row, ensure_ascii=False, separators=(",", ":"),
                    ).encode()) + 1
                    if plan:
                        copied_media += 1
                        media_bytes += plan[2]["size"]
                    skipped_media += int(skipped)
                limit_bytes = max_file or MAX_FILE_BYTES
                required = (transformed_bytes + sizes["services.jsonl"] + media_bytes
                            + len(manifest_bytes) + len(services_bytes) + 16 * 1024
                            + 4096 * copied_media)
                available, free, used = _available(config)
                _check_pipeline_storage(available, required)
                if temp_name in os.listdir(destination_chat):
                    _fail("TRANSFER_RECOVERY_REQUIRED")
                init_name = ".publisher-init-" + uuid.uuid4().hex
                _write_init_intent(
                    destination_chat, init_name, uid=config["destination_uid"],
                    gid=config["destination_gid"], source_id=source_id,
                    source_hashes=hashes, media_manifest_sha=desired_media_sha,
                )
                init = _new_topic_dir(destination_chat, init_name,
                                      uid=config["destination_uid"], gid=config["destination_gid"])
                try:
                    _write_file(init, ".publisher-temp.json", (json.dumps({
                        "source_id": source_id, "source_hashes": hashes,
                        "media_manifest_sha256": desired_media_sha,
                    }, sort_keys=True) + "\n").encode(), uid=config["destination_uid"],
                                gid=config["destination_gid"])
                    os.fsync(init)
                    os.fsync(destination_chat)
                finally:
                    os.close(init)
                _rename_noreplace(destination_chat, init_name, destination_chat, temp_name)
                os.fsync(destination_chat)
                os.unlink(init_name + ".json", dir_fd=destination_chat)
                os.fsync(destination_chat)
                temp = _open_child(destination_chat, temp_name, uid=config["destination_uid"])
                try:
                    media_inventory_fd = None
                    media_inventory_digest = hashlib.sha256()
                    media_inventory_count = 0
                    copied_bytes = 0
                    message_fd = os.open(
                        "messages.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                        | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=temp,
                    )
                    os.fchown(message_fd, config["destination_uid"], config["destination_gid"])
                    os.fchmod(message_fd, 0o600)
                    message_digest = hashlib.sha256()
                    actual_count = 0
                    try:
                        for row in _validated_message_rows(
                            source, chat=chat, topic=topic_id, scope=scope,
                            uid=config["source_uid"], expected_sha256=hashes["messages.jsonl"],
                        ):
                            staged_row, plan, skipped = transform(row)
                            encoded = (json.dumps(staged_row, ensure_ascii=False,
                                                  separators=(",", ":")) + "\n").encode()
                            view = memoryview(encoded)
                            while view:
                                written = os.write(message_fd, view)
                                view = view[written:]
                            message_digest.update(encoded)
                            actual_count += 1
                            if plan:
                                record, mid, checked = plan
                                staged_path, copied_size, copied_digest = _copy_media_reference(
                                    source, temp, record, mid, source_uid=config["source_uid"],
                                    dest_uid=config["destination_uid"],
                                    dest_gid=config["destination_gid"],
                                    max_file_bytes=checked["limit"],
                                )
                                if (staged_path != staged_row["media"]["path"]
                                        or copied_size != checked["size"]
                                        or copied_digest != checked["sha256"]):
                                    _fail("STAGED_MEDIA_REFERENCE_INVALID")
                                if media_inventory_fd is None:
                                    media_inventory_fd = os.open(
                                        "media_files.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                        | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=temp,
                                    )
                                    os.fchown(media_inventory_fd, config["destination_uid"],
                                              config["destination_gid"])
                                    os.fchmod(media_inventory_fd, 0o600)
                                inventory_record = {
                                    "message_id": mid, "path": staged_path,
                                    "size": copied_size, "sha256": copied_digest,
                                }
                                inventory_line = (json.dumps(
                                    inventory_record, sort_keys=True, separators=(",", ":"),
                                ) + "\n").encode()
                                view = memoryview(inventory_line)
                                while view:
                                    written = os.write(media_inventory_fd, view)
                                    view = view[written:]
                                media_inventory_digest.update(inventory_line)
                                media_inventory_count += 1
                                copied_bytes += copied_size
                        os.fsync(message_fd)
                    finally:
                        os.close(message_fd)
                    if media_inventory_fd is not None:
                        os.fsync(media_inventory_fd)
                        os.close(media_inventory_fd)
                        media_inventory_fd = None
                        os.fsync(temp)
                    if media_inventory_count != copied_media:
                        _fail("STAGED_MEDIA_REFERENCE_INVALID")
                    if actual_count != source_manifest["message_count"]:
                        _fail("MESSAGE_COUNT_MISMATCH")
                    staged_message_sha = message_digest.hexdigest()
                    service_size, service_sha = _copy_control(
                        source, "services.jsonl", temp,
                        source_uid=config["source_uid"],
                        dest_uid=config["destination_uid"], dest_gid=config["destination_gid"],
                        expected_size=sizes["services.jsonl"],
                        expected_sha=hashes["services.jsonl"], maximum=MAX_CONTROL_BYTES,
                    )
                    if service_size != sizes["services.jsonl"] or service_sha != hashes["services.jsonl"]:
                        _fail("SERVICES_HASH_MISMATCH")
                    staged_manifest = dict(source_manifest)
                    staged_manifest.update({
                        "messages_sha256": staged_message_sha,
                        "scope": scope,
                        "media_requested": True,
                        "max_file_bytes": limit_bytes,
                        "max_file_size_unit": "bytes",
                        "telegram_transfer": {
                            "source_messages_sha256": hashes["messages.jsonl"],
                            "source_manifest_sha256": hashes["manifest.json"],
                            "media_manifest_sha256": _sha(json.dumps(
                                media_manifest, sort_keys=True, separators=(",", ":"),
                            ).encode()) if media_manifest else None,
                            "downloaded_media": copied_media,
                            "skipped_media": skipped_media,
                        },
                    })
                    staged_manifest_bytes = (json.dumps(staged_manifest, ensure_ascii=False,
                                                        sort_keys=True) + "\n").encode()
                    service_count = service_manifest["event_count"]
                    _write_file(temp, "manifest.json", staged_manifest_bytes,
                                uid=config["destination_uid"], gid=config["destination_gid"])
                    _write_file(temp, "services_manifest.json", services_bytes,
                                uid=config["destination_uid"], gid=config["destination_gid"])
                    if media_manifest:
                        _write_file(temp, "media_manifest.json",
                                    (json.dumps(media_manifest, sort_keys=True) + "\n").encode(),
                                    uid=config["destination_uid"], gid=config["destination_gid"])
                    staged_hashes = {
                        "messages.jsonl": staged_message_sha,
                        "services.jsonl": service_sha,
                        "manifest.json": _sha(staged_manifest_bytes),
                        "services_manifest.json": _sha(services_bytes),
                    }
                    transfer = {
                        "version": 2, "source_id": source_id, "source_hashes": hashes,
                        "staged_hashes": staged_hashes,
                        "message_count": source_manifest["message_count"],
                        "service_event_count": service_count,
                        "downloaded_media": copied_media, "skipped_media": skipped_media,
                        "max_file_bytes": limit_bytes,
                        "max_file_size_unit": "bytes",
                        "media_inventory": {
                            "name": "media_files.jsonl", "count": media_inventory_count,
                            "sha256": media_inventory_digest.hexdigest(),
                        },
                        "media_manifest_sha256": _sha(json.dumps(
                            media_manifest, sort_keys=True, separators=(",", ":"),
                        ).encode()) if media_manifest else None,
                        "copied_bytes": copied_bytes,
                        "created_at": datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                    _write_file(temp, "telegram_transfer_manifest.json",
                                (json.dumps(transfer, sort_keys=True) + "\n").encode(),
                                uid=config["destination_uid"], gid=config["destination_gid"])
                    os.unlink(".publisher-temp.json", dir_fd=temp)
                    os.fsync(temp)
                except BaseException:
                    if media_inventory_fd is not None:
                        with contextlib.suppress(OSError):
                            os.close(media_inventory_fd)
                    os.close(temp)
                    _remove_tree(destination_chat, temp_name, expected_uid=config["destination_uid"])
                    raise
                os.close(temp)
                candidate_status = _staged_status(
                    destination_chat, temp_name, source_id,
                    dest_uid=config["destination_uid"],
                )
                if not candidate_status:
                    _fail("STAGING_REVISION_INVALID")
                if final_status:
                    previous, _ = _transfer_record(
                        destination_chat, scope_dir, uid=config["destination_uid"],
                    )
                    candidate, _ = _transfer_record(
                        destination_chat, temp_name, uid=config["destination_uid"],
                    )
                    _revision_compatible(
                        previous, candidate, source_hashes=hashes,
                        media_manifest_sha=desired_media_sha,
                        parent=destination_chat, previous_name=scope_dir,
                        candidate_name=temp_name, uid=config["destination_uid"],
                    )
                    _rename_exchange(destination_chat, temp_name, scope_dir)
                    _archive_displaced(
                        destination_chat, temp_name, source_id=source_id,
                        uid=config["destination_uid"], source_hashes=hashes,
                        desired_media_sha=desired_media_sha,
                    )
                else:
                    _rename_noreplace(destination_chat, temp_name, destination_chat, scope_dir)
                os.fsync(destination_chat)
                return {
                    "source_id": source_id, "status": "ready",
                    "message_count": source_manifest["message_count"],
                    "service_event_count": service_count,
                    "downloaded_media": copied_media, "skipped_media": skipped_media,
                    "max_file_bytes": limit_bytes,
                    "messages_sha256": staged_message_sha,
                    "copied_bytes": copied_bytes,
                    "shared_free_bytes": free, "shared_used_bytes": used + required,
                }
            finally:
                os.close(destination_chat)
        finally:
            os.close(source)
    finally:
        os.close(source_root)
        os.close(destination_root)


def handle(action: str, source_id: str, *, config_path: str) -> dict:
    if os.geteuid() == 0:
        _fail("ROOT_EXECUTION_FORBIDDEN")
    chat, scope_dir = _source_parts(source_id)
    config = _safe_config(config_path)
    _check_mount(config)
    if action == "budget":
        available, free, used = _available(config)
        return {"source_id": source_id, "status": "budget", "available_bytes": available,
                "storage_limit_bytes": config["storage_limit_bytes"],
                "min_free_bytes": config["min_free_bytes"], "free_bytes": free,
                "used_bytes": used, "max_file_bytes": min(MAX_FILE_BYTES, available)}
    if action == "copy":
        return _atomic_publish(source_id, chat, scope_dir, config)
    if action == "status":
        source_root, destination_root = _storage_roots(config)
        try:
            try:
                destination_chat = _open_child(destination_root, chat,
                                               uid=config["destination_uid"])
            except FileNotFoundError:
                return {"source_id": source_id, "status": "not_staged"}
            try:
                status = _staged_status(destination_chat, scope_dir, source_id,
                                        dest_uid=config["destination_uid"])
                return status or {"source_id": source_id, "status": "not_staged"}
            finally:
                os.close(destination_chat)
        finally:
            os.close(source_root)
            os.close(destination_root)
    _fail("INVALID_OPERATION")


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    try:
        args = list(sys.argv[1:] if argv is None else argv)
        if args in (["--help"], ["-h"]):
            print("Usage: kiber-analyst-telegram-publish --config FILE < request.jsonl")
            return 0
        if len(args) != 2 or args[0] != "--config" or not args[1].startswith("/"):
            _fail("INVALID_REQUEST")
        config_path = args[1]
        config = _safe_config(config_path)
        data = sys.stdin.buffer.read(513)
        if len(data) > 512 or not data.endswith(b"\n"):
            _fail("INVALID_REQUEST")
        try:
            lines = data.decode("ascii").splitlines()
        except UnicodeDecodeError:
            _fail("INVALID_REQUEST")
        if len(lines) != 2 or lines[0] not in {"budget", "copy", "status"}:
            _fail("INVALID_REQUEST")
        with _exclusive_lock(config):
            result = handle(lines[0], lines[1], config_path=config_path)
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0
    except TransferError as exc:
        sys.stdout.write(json.dumps({"status": "error", "error_code": exc.code}) + "\n")
        return 1
    except Exception:  # noqa: BLE001 - never expose unexpected filesystem details.
        sys.stdout.write(json.dumps({"status": "error", "error_code": "TRANSFER_FAILED"}) + "\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
