from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from kiber_analyst.errors import ParseError
from kiber_analyst.load import document_title_for
from kiber_analyst.parse import _media, iter_messages, parse_job
from kiber_analyst.paths import job_paths, write_json_atomic
from kiber_analyst.report import build_report

FIXTURES = Path(__file__).parent / "fixtures"
CHAT_ID = "-10012345"


def make_job(
    tmp_path: Path,
    fixture: str,
    *,
    scope: str,
    scope_topic_id: str | None = None,
    scope_topic_title: str | None = None,
) -> Path:
    job_dir = tmp_path / fixture.removesuffix(".json")
    paths = job_paths(job_dir)
    paths["unpacked"].mkdir(parents=True)
    paths["normalized"].mkdir()
    shutil.copyfile(FIXTURES / fixture, paths["unpacked"] / "result.json")
    write_json_atomic(
        paths["manifest"],
        {
            "archive_uri": "fixture://telegram/export.zip",
            "archive_name": "fixture.zip",
            "archive_md5": "0" * 32,
            "archive_size_bytes": (FIXTURES / fixture).stat().st_size,
            "snapshot_sha256": "1" * 64,
            "scope": scope,
            "scope_topic_id": scope_topic_id,
            "scope_topic_title": scope_topic_title,
            "chat_id": CHAT_ID,
            "result_json": "result.json",
            "steps": {},
            "load": {"status": "not_run"},
        },
    )
    return job_dir


def records(job_dir: Path) -> list[dict]:
    return list(iter_messages(job_paths(job_dir)["messages"]))


def test_topic_scope_without_topic_created_attributes_every_message_to_scope(
    tmp_path: Path,
) -> None:
    job = make_job(
        tmp_path,
        "topic_scope_without_topic_created.json",
        scope="topic",
        scope_topic_id="77",
        scope_topic_title="Sample Station",
    )
    result = parse_job(job)
    parsed = records(job)
    topics = json.loads(job_paths(job)["topics"].read_text(encoding="utf-8"))

    assert result["topic_created_count"] == 0
    assert result["attribution_counts"] == {"chain": 0, "scope": 2, "none": 0}
    assert [item["attribution"] for item in parsed] == ["scope", "scope"]
    assert {item["topic_id"] for item in parsed} == {"77"}
    assert len(topics) == 1
    assert topics[0]["topic_id"] == "77"
    assert "Без установленной темы" not in {item["topic_title"] for item in topics}

    report = build_report(job)
    assert report["topics_count"] == 1
    assert report["attribution"] == {
        "chain": 0,
        "scope": 2,
        "none": 0,
        "chain_share": 0.0,
    }
    assert "не оценка полноты или качества экспорта" in report[
        "attribution_interpretation"
    ]["chain_share"]
    assert "scope авторитетен" in report["attribution_interpretation"]["topic_scope"]
    assert "экспорту всего чата" in report["attribution_interpretation"]["desktop_export"]
    assert report["messages_by_topic"] == {"77": 2}


def test_full_chat_without_topic_chain_is_none_and_has_separate_document_group(
    tmp_path: Path,
) -> None:
    job = make_job(tmp_path, "full_chat_without_topic_chain.json", scope="chat")
    result = parse_job(job)
    parsed = records(job)
    topics = json.loads(job_paths(job)["topics"].read_text(encoding="utf-8"))

    assert result["attribution_counts"] == {"chain": 0, "scope": 0, "none": 2}
    assert all(item["topic_id"] is None for item in parsed)
    assert all(item["topic_title"] is None for item in parsed)
    assert all(item["url"] is None for item in parsed)
    assert parsed[0]["meta"]["deep_link"] == (
        "tg://openmessage?chat_id=-10012345&message_id=201"
    )
    assert topics == [
        {
            "topic_id": None,
            "topic_title": "Без установленной темы",
            "attribution": "none",
        }
    ]
    assert document_title_for(None, None) == "Без установленной темы"
    report = build_report(job)
    assert report["topics_count"] == 0
    assert report["messages_by_topic"] == {"none": 2}


def test_topic_chain_text_entities_dates_media_and_service_messages(
    tmp_path: Path,
) -> None:
    job = make_job(tmp_path, "full_chat_with_topic_chain.json", scope="chat")
    result = parse_job(job)
    parsed = records(job)

    assert result["messages_total"] == 2
    assert result["service_messages_skipped"] == 1
    assert result["topic_created_count"] == 1
    assert result["attribution_counts"] == {"chain": 2, "scope": 0, "none": 0}
    assert parsed[0]["topic_id"] == "77"
    assert parsed[0]["attribution"] == "chain"
    assert parsed[0]["url"] == "https://t.me/c/12345/77/6985"
    assert parsed[0]["text"] == "Сводка: план"
    assert len(parsed[0]["entities"]) == 2
    assert parsed[0]["date"] == "2026-11-01T10:01:00+03:00"
    assert parsed[0]["date_raw"] == "2026-11-01T10:01:00"
    assert parsed[0]["media_type"] == "photo"
    assert parsed[0]["media_path"] == "photos/photo_1.jpg"
    assert parsed[0]["content_hash"].startswith("sha256:")
    assert parsed[1]["edited_at"] == "2026-11-01T10:03:00+03:00"
    assert result["links_in_text_count"] == 1


def test_explicit_media_type_uses_normalized_or_export_path() -> None:
    assert _media(
        {
            "media_type": "voice_message",
            "file": "voice_messages/audio_1@13-02-2023_00-41-30.ogg",
        }
    ) == (
        "voice_message",
        "voice_messages/audio_1@13-02-2023_00-41-30.ogg",
    )
    assert _media(
        {
            "media_type": "voice_message",
            "media_path": "normalized/audio.ogg",
            "file": "voice_messages/audio.ogg",
        }
    ) == ("voice_message", "normalized/audio.ogg")
    assert _media(
        {"media_type": "photo", "media_path": None, "photo": "photos/photo.jpg"}
    ) == ("photo", "photos/photo.jpg")
    assert _media({"media_type": "voice_message", "file": ["not-a-path"]}) == (
        "voice_message",
        None,
    )


def test_broken_reply_chains_are_counted_and_fall_back_to_chat_none(
    tmp_path: Path,
) -> None:
    job = tmp_path / "broken"
    paths = job_paths(job)
    paths["unpacked"].mkdir(parents=True)
    paths["normalized"].mkdir()
    export = {
        "id": 12345,
        "type": "supergroup",
        "name": "Test chat",
        "messages": [
            {"id": 1, "type": "message", "reply_to_message_id": 900, "text": "missing"},
            {"id": 2, "type": "message", "reply_to_message_id": 3, "text": "cycle a"},
            {"id": 3, "type": "message", "reply_to_message_id": 2, "text": "cycle b"},
        ],
    }
    (paths["unpacked"] / "result.json").write_text(
        json.dumps(export, ensure_ascii=False), encoding="utf-8"
    )
    write_json_atomic(
        paths["manifest"],
        {"scope": "chat", "chat_id": CHAT_ID, "result_json": "result.json"},
    )

    result = parse_job(job)
    assert result["broken_chain_count"] == 3
    assert result["attribution_counts"] == {"chain": 0, "scope": 0, "none": 3}


def test_report_rejects_attribution_total_mismatch(tmp_path: Path) -> None:
    job = make_job(
        tmp_path,
        "topic_scope_without_topic_created.json",
        scope="topic",
        scope_topic_id="77",
        scope_topic_title="Sample Station",
    )
    parse_job(job)
    paths = job_paths(job)
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    manifest["parse"]["attribution_counts"]["scope"] = 1
    write_json_atomic(paths["manifest"], manifest)
    with pytest.raises(ParseError, match="do not add up"):
        build_report(job)
