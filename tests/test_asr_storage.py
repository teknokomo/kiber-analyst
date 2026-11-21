from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from kiber_analyst.asr_store import (
    ASRLoadError,
    _find_audio_file,
    _item_record,
    _run_status,
)


def _fixture_item(
    tmp_path: Path, *, text: str = "Текст", tokens: list[int] | None = None
) -> tuple[dict, dict]:
    result_path = tmp_path / "asr" / "results" / "result.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        json.dumps(
            {
                "duration": 1.5,
                "text": text,
                "segments": [
                    {
                        "start": 0.0,
                        "end": 1.5,
                        "text": text,
                        "tokens": tokens or [],
                        "avg_logprob": -0.2,
                        "no_speech_prob": 0.01,
                        "compression_ratio": 1.1,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    digest = hashlib.sha256(result_path.read_bytes()).hexdigest()
    manifest = {
        "run_id": "run-fixture",
        "provider": "groq",
        "model": "whisper-large-v3-turbo",
        "language": "ru",
        "prompt_sha256": "a" * 64,
        "status": "complete",
        "items": [],
    }
    item = {
        "run_key": "run-fixture",
        "external_ref": "tg:-1009000:1101",
        "media_path": "chats/topic/voice.ogg",
        "audio_sha256": "b" * 64,
        "duration_seconds": 1.5,
        "result": "asr/results/result.json",
        "result_sha256": digest,
        "model": "whisper-large-v3-turbo",
        "language": "ru",
        "prompt_sha256": "a" * 64,
        "segments_count": 1,
        "empty_text_count": int(not text.strip()),
        "empty_text_with_tokens_count": int(not text.strip() and bool(tokens)),
        "attempts": [
            {
                "profile": "groq_asr",
                "status_code": 200,
                "outcome": "success",
                "latency_seconds": 1.2,
            }
        ],
        "status": "complete",
        "http_code": 200,
        "profile": "groq_asr",
    }
    return manifest, item


def test_asr_load_manifest_validates_external_ref_file_and_hash(tmp_path: Path) -> None:
    manifest, item = _fixture_item(tmp_path)

    without_ref = dict(item)
    without_ref.pop("external_ref")
    with pytest.raises(ASRLoadError, match="external_ref"):
        _item_record(manifest, without_ref, tmp_path, None)

    missing_file = dict(item, result="asr/results/missing.json")
    with pytest.raises(ASRLoadError, match="missing or outside"):
        _item_record(manifest, missing_file, tmp_path, None)

    bad_hash = dict(item, result_sha256="0" * 64)
    with pytest.raises(ASRLoadError, match="SHA-256 mismatch"):
        _item_record(manifest, bad_hash, tmp_path, None)


def test_asr_load_rejects_absolute_traversal_and_symlink_paths(tmp_path: Path) -> None:
    manifest, item = _fixture_item(tmp_path)
    for invalid in ("/tmp/result.json", "../result.json"):
        with pytest.raises(ASRLoadError, match="job directory"):
            _item_record(manifest, dict(item, result=invalid), tmp_path, None)

    outside = tmp_path.parent / "external-asr.json"
    outside.write_text("{}", encoding="utf-8")
    link = tmp_path / "asr" / "results" / "linked.json"
    link.symlink_to(outside)
    with pytest.raises(ASRLoadError, match="symlinks"):
        _item_record(
            manifest,
            dict(
                item,
                result="asr/results/linked.json",
                result_sha256=hashlib.sha256(b"{}").hexdigest(),
            ),
            tmp_path,
            None,
        )


def test_asr_load_marks_empty_text_with_tokens(tmp_path: Path) -> None:
    manifest, item = _fixture_item(tmp_path, text="  ", tokens=[17, 29])
    record = _item_record(manifest, item, tmp_path, None)
    assert record["segments"][0]["empty_text_with_tokens"] is True
    assert record["meta"]["empty_text_count"] == 1


def test_asr_load_verifies_audio_inside_export_directory(tmp_path: Path) -> None:
    relative = Path("chats/chat/topic/voice.ogg")
    audio = tmp_path / "unpacked" / "ChatExport" / relative
    audio.parent.mkdir(parents=True)
    audio.write_bytes(b"fixture audio")
    assert _find_audio_file(tmp_path, relative) == audio.resolve()

    manifest, item = _fixture_item(tmp_path)
    item["media_path"] = relative.as_posix()
    item["audio_sha256"] = hashlib.sha256(audio.read_bytes()).hexdigest()
    record = _item_record(manifest, item, tmp_path, None)
    assert record["meta"]["audio_sha256_verified"] is True


def test_asr_run_status_maps_manifest_states() -> None:
    assert _run_status([{"status": "complete"}], "complete") == "done"
    assert (
        _run_status([{"status": "complete"}, {"status": "failed"}], "partial")
        == "partial"
    )
    assert _run_status([{"status": "pending"}], "running") == "running"
    assert _run_status([{"status": "failed"}], "failed") == "failed"


def test_migration_0003_self_registers_and_does_not_change_prior_migrations() -> None:
    root = Path(__file__).parents[1]
    migration = (root / "migrations" / "0003_asr.sql").read_text(encoding="utf-8")
    assert "INSERT INTO schema_migrations(version) VALUES ('0003_asr')" in migration
    assert "ON CONFLICT DO NOTHING" in migration
    assert "idx_asr_segments_tsv" in migration
    assert "HNSW" not in migration.upper()

    existing = hashlib.sha256(
        (root / "migrations" / "0002_telegram.sql").read_bytes()
    ).hexdigest()
    assert (
        existing == "ddf69f45031225cf44f7633ce892ba5d5c1753e8cf529920f214b5cb38db12a1"
    )
    migration_path = root / "migrations" / "0002_telegram.sql"
    skill_migration_path = (
        root / "skills" / "telegram-ingest" / "migrations" / "0002_telegram.sql"
    )
    if skill_migration_path.exists():
        assert skill_migration_path.read_bytes() == migration_path.read_bytes()
