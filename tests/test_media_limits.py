from __future__ import annotations

import hashlib
import json

import pytest

from kiber_analyst.errors import LoadError
from kiber_analyst.media_limits import (
    DEFAULT_TELEGRAM_MEDIA_BYTES,
    MAX_TELEGRAM_MEDIA_BYTES,
    parse_telegram_media_limit,
)
from kiber_analyst.media_resume import resumed_media


def test_media_limit_units_and_bounds_are_explicit():
    assert DEFAULT_TELEGRAM_MEDIA_BYTES == 104_857_600
    assert MAX_TELEGRAM_MEDIA_BYTES == 4_000_000_000
    assert parse_telegram_media_limit(None) == DEFAULT_TELEGRAM_MEDIA_BYTES
    assert parse_telegram_media_limit("100") == 100 * 1024**2
    assert parse_telegram_media_limit("100MiB") == 100 * 1024**2
    assert parse_telegram_media_limit("100MB") == 100_000_000
    assert parse_telegram_media_limit("4000MB") == MAX_TELEGRAM_MEDIA_BYTES

    for value in ("0", "-1", "4000MiB", "4001MB", "not-a-size"):
        with pytest.raises(ValueError):
            parse_telegram_media_limit(value)


def test_media_resume_accepts_large_non_asr_media_without_allocating_it():
    hashes = {"messages.jsonl": "a" * 64}
    receipt = {
        "export_status": "downloaded",
        "path": "media-resume/42/archive.bin",
        "size": MAX_TELEGRAM_MEDIA_BYTES,
        "sha256": hashlib.sha256(b"synthetic metadata only").hexdigest(),
        "type": "document",
        "max_file_bytes": MAX_TELEGRAM_MEDIA_BYTES,
    }
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": MAX_TELEGRAM_MEDIA_BYTES,
        "records": {"42": receipt},
    }).encode()
    messages = [{"message_id": 42, "media": {"kind": "document", "reason": "over_limit"}}]

    result = resumed_media(checkpoint, hashes, messages)

    assert result[42]["size"] == 4_000_000_000
    assert result[42]["max_file_bytes"] == 4_000_000_000
    assert result[42]["type"] == "document"


@pytest.mark.parametrize("size,limit", [(4_000_000_001, 4_000_000_000),
                                          (4_000_000_000, 3_999_999_999)])
def test_media_resume_rejects_receipts_over_their_declared_limit(size, limit):
    hashes = {"messages.jsonl": "b" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 4_000_000_000,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.bin",
            "size": size, "sha256": "c" * 64, "type": "video",
            "max_file_bytes": limit,
        }},
    }).encode()
    messages = [{"message_id": 7, "media": {"kind": "video", "reason": "over_limit"}}]

    with pytest.raises(LoadError, match="unsafe path or checksum"):
        resumed_media(checkpoint, hashes, messages)


def test_media_resume_rejects_record_limit_above_export_limit():
    hashes = {"messages.jsonl": "d" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 100 * 1024**2,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.bin",
            "size": 101, "sha256": "e" * 64, "type": "document",
            "max_file_bytes": 4_000_000_000,
        }},
    }).encode()
    messages = [{"message_id": 7, "media": {"kind": "document"}}]

    with pytest.raises(LoadError, match="unsafe path or checksum"):
        resumed_media(checkpoint, hashes, messages)


def test_media_resume_keeps_source_audio_type_when_receipt_omits_it():
    hashes = {"messages.jsonl": "f" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 101,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.ogg",
            "size": 101, "sha256": "a" * 64, "max_file_bytes": 101,
        }},
    }).encode()
    messages = [{"message_id": 7, "media": {"type": "voice_message"}}]

    result = resumed_media(checkpoint, hashes, messages)

    assert result[7]["type"] == "voice_message"


def test_media_resume_rejects_receipt_type_conflicting_with_source():
    hashes = {"messages.jsonl": "0" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 101,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.ogg",
            "size": 101, "sha256": "a" * 64, "max_file_bytes": 101,
            "type": "document",
        }},
    }).encode()
    messages = [{"message_id": 7, "media": {"type": "voice_message"}}]

    with pytest.raises(LoadError, match="unsafe path or checksum"):
        resumed_media(checkpoint, hashes, messages)


@pytest.mark.parametrize("source_media", [
    ...,
    None,
    {},
    [],
    "voice_message",
    {"type": "voice_message", "kind": "document", "reason": "over_limit"},
])
def test_media_resume_rejects_absent_or_malformed_source_attachment(source_media):
    hashes = {"messages.jsonl": "1" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 101,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.ogg",
            "size": 3, "sha256": hashlib.sha256(b"abc").hexdigest(),
            "type": "voice_message", "max_file_bytes": 101,
        }},
    }).encode()
    message = {"message_id": 7}
    if source_media is not ...:
        message["media"] = source_media

    with pytest.raises(LoadError):
        resumed_media(checkpoint, hashes, [message])


def test_media_resume_accepts_a_valid_skipped_source_attachment():
    hashes = {"messages.jsonl": "2" * 64}
    checkpoint = json.dumps({
        "inputs": hashes,
        "requested_max_file_bytes": 101,
        "records": {"7": {
            "export_status": "downloaded", "path": "media-resume/7/file.ogg",
            "size": 3, "sha256": hashlib.sha256(b"abc").hexdigest(),
            "max_file_bytes": 101,
        }},
    }).encode()
    messages = [{"message_id": 7, "media": {
        "type": "voice_message", "export_status": "skipped", "reason": "over_limit",
    }}]

    assert resumed_media(checkpoint, hashes, messages)[7]["type"] == "voice_message"
