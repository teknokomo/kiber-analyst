from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from kiber_analyst.errors import LoadError
from kiber_analyst.load import (
    _mark_absent_in_scope,
    _required_env,
    document_title_for,
    load_job,
)
from kiber_analyst.parse import NO_TOPIC_TITLE, iter_messages, parse_job
from kiber_analyst.paths import job_paths, write_json_atomic


def test_database_module_does_not_import_psycopg_at_package_import_time() -> None:
    project_root = Path(__file__).parents[1]
    package_root = project_root / "src"
    if not package_root.is_dir():
        package_root = project_root / "scripts"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, kiber_analyst.cli; assert 'psycopg' not in sys.modules",
        ],
        cwd=project_root,
            env={"PYTHONPATH": str(package_root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_database_env_is_loopback_and_knowledge_app_only(tmp_path: Path) -> None:
    env_file = tmp_path / "knowledge.env"
    env_file.write_text(
        "KNOWLEDGE_HOST=127.0.0.1\n"
        "KNOWLEDGE_PORT=5433\n"
        "KNOWLEDGE_DB=knowledge\n"
        "KNOWLEDGE_USER=knowledge_app\n"
        "KNOWLEDGE_PASSWORD=not-a-real-secret\n",
        encoding="utf-8",
    )
    config = _required_env(env_file)
    assert config["KNOWLEDGE_HOST"] == "127.0.0.1"
    assert config["KNOWLEDGE_USER"] == "knowledge_app"

    env_file.write_text(
        env_file.read_text().replace("127.0.0.1", "192.0.2.5"), encoding="utf-8"
    )
    with pytest.raises(LoadError, match="must be 127.0.0.1"):
        _required_env(env_file)


def test_unattributed_document_has_separate_title() -> None:
    assert document_title_for(None, None) == NO_TOPIC_TITLE
    assert document_title_for("23", "Example Station") == "Example Station"


def test_full_chat_without_none_marks_old_none_document_segments_absent(
    tmp_path: Path,
) -> None:
    fixture = Path(__file__).parent / "fixtures" / "full_chat_with_topic_chain.json"
    paths = job_paths(tmp_path / "full-chat")
    paths["unpacked"].mkdir(parents=True)
    paths["normalized"].mkdir()
    shutil.copyfile(fixture, paths["unpacked"] / "result.json")
    write_json_atomic(
        paths["manifest"],
        {
            "archive_uri": "fixture://telegram/export.zip",
            "archive_name": "fixture.zip",
            "archive_md5": "0" * 32,
            "archive_size_bytes": fixture.stat().st_size,
            "snapshot_sha256": "1" * 64,
            "scope": "chat",
            "scope_topic_id": None,
            "chat_id": "-1009000",
            "result_json": "result.json",
            "steps": {},
            "load": {"status": "not_run"},
        },
    )
    parse_job(paths["root"])
    current_messages = list(iter_messages(paths["messages"]))
    assert current_messages
    assert all(message["topic_id"] is not None for message in current_messages)

    class MemoryCursor:
        def __init__(self) -> None:
            self.documents = [("topic-doc", "23"), ("none-doc", None)]
            self.segments = {
                "topic-segment": {
                    "document_id": "topic-doc",
                    "external_ref": "tg:-1009000:24",
                    "meta": {"presence": "present"},
                },
                "none-segment-1": {
                    "document_id": "none-doc",
                    "external_ref": "tg:-1009000:201",
                    "meta": {"presence": "present"},
                },
                "none-segment-2": {
                    "document_id": "none-doc",
                    "external_ref": "tg:-1009000:202",
                    "meta": {"presence": "present"},
                },
            }
            self.rows = []

        def execute(self, statement: str, params: tuple = ()) -> None:
            normalized = " ".join(statement.lower().split())
            if "from documents" in normalized:
                self.rows = self.documents
            elif normalized.startswith("select id, external_ref, meta from segments"):
                document_id = params[0]
                self.rows = [
                    (segment_id, row["external_ref"], row["meta"])
                    for segment_id, row in self.segments.items()
                    if row["document_id"] == document_id
                ]
            elif normalized.startswith("update segments set meta"):
                meta_json, segment_id = params
                self.segments[segment_id]["meta"] = json.loads(meta_json)
                self.rows = []
            else:
                raise AssertionError(f"Unexpected SQL in fake cursor: {statement}")

        def fetchall(self) -> list:
            return self.rows

    cursor = MemoryCursor()
    topic_refs = {
        f"tg:{message['chat_id']}:{message['message_id']}"
        for message in current_messages
    }
    absent_count = _mark_absent_in_scope(
        cursor,
        "source-id",
        "chat",
        None,
        {"topic-doc": topic_refs},
        "new-snapshot",
    )

    assert absent_count == 2
    for segment_id in ("none-segment-1", "none-segment-2"):
        assert cursor.segments[segment_id]["meta"] == {
            "presence": "absent_in_snapshot",
            "snapshot_id": "new-snapshot",
        }
    assert cursor.segments["topic-segment"]["meta"] == {"presence": "present"}

    topic_scope_cursor = MemoryCursor()
    topic_absent_count = _mark_absent_in_scope(
        topic_scope_cursor,
        "source-id",
        "topic",
        "23",
        {"topic-doc": topic_refs},
        "topic-snapshot",
    )
    assert topic_absent_count == 0
    assert topic_scope_cursor.segments["none-segment-1"]["meta"] == {
        "presence": "present"
    }


def test_telegram_migration_records_its_version_idempotently() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0002_telegram.sql"
    sql = migration.read_text(encoding="utf-8")
    assert "INSERT INTO schema_migrations(version) VALUES ('0002_telegram')" in sql
    assert "ON CONFLICT DO NOTHING;" in sql


def test_telegram_services_migration_records_its_version_idempotently() -> None:
    migration = Path(__file__).parents[1] / "migrations" / "0005_telegram_services.sql"
    sql = migration.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS telegram_service_events" in sql
    assert "INSERT INTO schema_migrations(version) VALUES ('0005_telegram_services')" in sql
    assert "ON CONFLICT DO NOTHING;" in sql


def test_failed_dry_run_does_not_persist_a_failed_run(tmp_path: Path) -> None:
    paths = job_paths(tmp_path / "job")
    paths["normalized"].mkdir(parents=True)
    paths["messages"].write_text("", encoding="utf-8")
    write_json_atomic(
        paths["manifest"],
        {
            "snapshot_sha256": "a" * 64,
            "archive_size_bytes": 1,
            "archive_uri": "fixture://test.zip",
            "scope": "topic",
            "scope_topic_id": "23",
            "load": {"status": "not_run"},
        },
    )
    with (
        patch("kiber_analyst.load._required_env", return_value={}),
        patch(
            "kiber_analyst.load._connect", side_effect=RuntimeError("db unavailable")
        ),
        patch("kiber_analyst.load._write_failed_run") as failed_run,
        pytest.raises(LoadError, match="RuntimeError"),
    ):
        load_job(paths["root"], dry_run=True)

    failed_run.assert_not_called()
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["load"]["status"] == "dry_run_failed"
    assert manifest["load"]["failed_run_recorded"] is False
