from __future__ import annotations

import json
import logging
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Self

import pytest

from kiber_analyst.embedding_contract import DIMENSIONS, MODEL_KEY
from kiber_analyst.errors import SearchError
from kiber_analyst.search_core import SEARCH_SQL, execute_search
from kiber_analyst_mcp.config import ConfigError, MCPConfig, config_from_mapping
from kiber_analyst_mcp.tools import ToolFailure, ToolService

PROJECT_ID = "00000000-0000-0000-0000-000000000001"
DOCUMENT_ID = "00000000-0000-0000-0000-000000000002"
CREATED_AT = datetime(2026, 11, 7, tzinfo=UTC)


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.rows: list[tuple] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.connection.statements.append((query, params))
        if query.startswith(("SELECT 1 FROM projects", "SELECT id FROM projects WHERE slug =")):
            self.rows = [(PROJECT_ID,)]
            return
        if query.startswith("SELECT 1 FROM sources so"):
            self.rows = [(1,)] if self.connection.source_exists else []
            return
        if query == SEARCH_SQL:
            assert isinstance(params, dict)
            self.connection.search_params.append(params)
            self.rows = self.connection.search_rows(params)
            return
        if "SELECT d.id,so.kind,d.doc_type" in query:
            self.rows = [
                (DOCUMENT_ID, "telegram", "export", "ru", CREATED_AT, 403, 9,
                 None, None, None),
            ]
            return
        if "FROM segments WHERE document_id=%s AND id>%s" in query:
            assert isinstance(params, tuple)
            _document, after, limit = params
            self.rows = [self.connection.segment_row(i) for i in range(after + 1, min(404, after + limit + 1))]
            return
        if query.startswith("SELECT r.external_ref,ar.run_key,ar.model,ar.language"):
            assert isinstance(params, tuple)
            refs, _document = params
            self.rows = [
                (ref, "run-a", "whisper-large-v3-turbo", "ru", 12.5, f"transcript {ref}")
                for ref in refs
                if ref in self.connection.asr_refs
            ]
            return
        if "SELECT p.slug,p.title" in query:
            self.rows = [("sample-project", "Great Ring", 1, 1, 403, 2, 9, 704, 1062)]
            return
        if query.startswith("SELECT id FROM projects WHERE slug=%s"):
            self.rows = [(PROJECT_ID,)]
            return
        if "SELECT kind,status,started_at,finished_at FROM runs" in query:
            self.rows = [("telegram_load", "done", CREATED_AT, CREATED_AT)]
            return
        if "SELECT run_key,model,status,finished_at FROM asr_runs" in query:
            self.rows = [("asr-run", "whisper-large-v3-turbo", "done", CREATED_AT)]
            return
        if "SELECT s.scope,s.sha256,s.created_at FROM snapshots" in query:
            self.rows = [("topic", "a" * 64, CREATED_AT)]
            return
        if "SELECT\n                          (SELECT count(*) FROM sources" in query:
            self.rows = [(1, 1, 403, 2, 9, 704, 10, 681, 381)]
            return
        raise AssertionError(f"Unexpected SQL: {query[:100]}")

    def fetchone(self) -> tuple | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple]:
        return self.rows


class FakeConnection:
    def __init__(self, *, asr_refs: set[str] | None = None, source_exists: bool = True) -> None:
        self.statements: list[tuple[str, object]] = []
        self.search_params: list[dict] = []
        self.asr_refs = asr_refs or {f"tg:message:{i}" for i in range(1, 10)}
        self.source_exists = source_exists

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def search_rows(self, params: dict) -> list[tuple]:
        segment = (
            "segment", 11, None, DOCUMENT_ID, None, None, None, 0.031,
            "source message", "https://t.me/example", CREATED_AT, "Test User",
            "telegram", "sample-project", "telegram:-10012345:topic:77",
            "-10012345", "Group title", "77", "Kibervoyna", "7087",
            None, None, None, None, None, None,
        )
        asr = (
            "asr", 12, 22, DOCUMENT_ID, "00000000-0000-0000-0000-000000000003",
            1200, 3500, 0.029, "recognized speech", "https://t.me/example",
            CREATED_AT, "Test User", "telegram", "sample-project",
            "telegram:-10012345:topic:77", "-10012345",
            "Group title", "77", "Kibervoyna", "7087",
            None, None, None, None, None, None,
        )
        source = params["source"]
        if source == "segment":
            return [segment]
        if source == "asr":
            return [asr]
        return [segment, asr]

    @staticmethod
    def segment_row(number: int) -> tuple:
        return (
            number,
            f"tg:message:{number}",
            CREATED_AT,
            "Test User",
            f"message {number}",
            "https://t.me/example",
            "voice_message" if number <= 9 else None,
            "77",
            None,
            None,
            None,
        )


def _config() -> MCPConfig:
    return MCPConfig(
        db_host="127.0.0.1",
        db_port=5433,
        db_name="knowledge",
        db_user="knowledge_reader",
        db_password="test-db-password",
        auth_token="a" * 64,
    )


def _service(connection: FakeConnection, worker_result: dict | None = None) -> ToolService:
    @contextmanager
    def factory(_config: MCPConfig):
        yield connection

    def worker_call(payload: dict, *, timeout: float) -> dict:
        assert timeout == 10.0
        assert payload["model"] == MODEL_KEY
        return worker_result or {
            "status": "done",
            "vector": [0.0] * DIMENSIONS,
            "dimensions": DIMENSIONS,
            "model": MODEL_KEY,
            "scope": "google:test",
            "request_id": str(uuid.uuid4()),
        }

    return ToolService(_config(), connection_factory=factory, worker_call=worker_call)


def test_mcp_config_pins_database_and_never_reprs_secrets() -> None:
    values = {
        "MCP_DB_HOST": "127.0.0.1",
        "MCP_DB_PORT": "5433",
        "MCP_DB_NAME": "knowledge",
        "MCP_DB_USER": "knowledge_reader",
        "MCP_DB_PASSWORD": "password-sentinel",
        "MCP_AUTH_TOKEN": "c" * 64,
    }
    config = config_from_mapping(values)
    assert "password-sentinel" not in repr(config)
    assert "c" * 64 not in repr(config)
    values["MCP_DB_PORT"] = "5434"
    with pytest.raises(ConfigError, match="database target"):
        config_from_mapping(values)


@pytest.mark.parametrize(
    ("source", "expected_sources"),
    [("segment", ["segment"]), ("asr", ["asr"]), ("all", ["segment", "asr"])],
)
def test_mcp_search_uses_shared_core_for_segment_asr_and_union(
    source: str, expected_sources: list[str]
) -> None:
    connection = FakeConnection()
    result = _service(connection).search(
        "Космостанция", "sample-project", source=source,
        date_from="2023-01-01", date_to="2026-11-07", author="Test User", topic="77",
    )

    assert [row["source"] for row in result["results"]] == expected_sources
    assert result["mode"] == "hybrid"
    assert connection.search_params[-1]["source"] == (None if source == "all" else source)
    assert connection.search_params[-1]["author"] == "Test User"
    assert connection.search_params[-1]["topic"] == "77"
    assert connection.search_params[-1]["date_from"] == "2023-01-01"
    assert connection.search_params[-1]["date_to"] == "2026-11-07"
    assert any(query == SEARCH_SQL for query, _ in connection.statements)


def test_mcp_search_asr_has_timecode_and_safe_fts_degradation() -> None:
    connection = FakeConnection()
    service = _service(connection, {"status": "waiting_quota", "reason": "quota"})

    result = service.search("phrase", "sample-project", source="asr")

    item = result["results"][0]
    assert result["mode"] == "fts"
    assert result["degraded"] is True
    assert result["degraded_reason"] == "quota"
    assert item["asr_run_id"] == "00000000-0000-0000-0000-000000000003"
    assert (item["start_ms"], item["end_ms"], item["timecode"]) == (1200, 3500, "00:00:01.200")
    assert item["url"] == "https://t.me/example"


def test_shared_search_filters_by_source_and_returns_telegram_provenance() -> None:
    connection = FakeConnection()
    result = execute_search(
        connection, "topic query", project="sample-project", vector=[0.0] * DIMENSIONS,
        source_id="telegram:-10012345:topic:77", limit=5,
    )

    item = result["results"][0]
    assert connection.search_params[-1]["source_id"] == "telegram:-10012345:topic:77"
    assert item["source_id"] == "telegram:-10012345:topic:77"
    assert (item["chat_id"], item["chat_title"]) == ("-10012345", "Group title")
    assert (item["topic_id"], item["topic_title"], item["message_id"]) == (
        "77", "Kibervoyna", "7087",
    )


def test_shared_search_reports_missing_source_or_topic() -> None:
    with pytest.raises(SearchError, match="source or topic"):
        execute_search(
            FakeConnection(source_exists=False), "query", project="sample-project",
            vector=None, source_id="telegram:-10012345:topic:999999",
        )


def test_mcp_invalid_search_input_is_rejected_before_database_or_worker() -> None:
    connection = FakeConnection()
    service = _service(connection)

    with pytest.raises(ToolFailure, match="limit"):
        service.search("query", "sample-project", limit=51)
    with pytest.raises(ToolFailure, match="date_from"):
        service.search("query", "sample-project", date_from="2026-01-01", date_to="2025-01-01")


def test_mcp_get_document_keyset_pages_return_403_segments_and_nine_asr_results() -> None:
    connection = FakeConnection()
    service = _service(connection)
    cursor = None
    segment_ids: list[int] = []
    transcripts: list[str] = []
    pages = 0
    while True:
        page = service.get_document(DOCUMENT_ID, limit=100, cursor=cursor)
        pages += 1
        segment_ids.extend(item["segment_id"] for item in page["segments"])
        transcripts.extend(item["external_ref"] for item in page["asr_results"])
        assert all("raw_path" not in item and "meta" not in item for item in page["segments"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert pages == 5
    assert segment_ids == list(range(1, 404))
    assert transcripts == [f"tg:message:{i}" for i in range(1, 10)]


def test_mcp_list_projects_and_ingest_status_return_explicit_counts() -> None:
    service = _service(FakeConnection())

    projects = service.list_projects()["projects"]
    status = service.ingest_status("sample-project")

    assert projects[0]["slug"] == "sample-project"
    assert projects[0]["embeddings"] == 1062
    assert status["counts"] == {
        "sources": 1,
        "documents": 1,
        "segments": 403,
        "asr_runs": 2,
        "asr_results": 9,
        "asr_segments": 704,
        "asr_attempts": 10,
        "asr_embeddings": 681,
        "source_embeddings": 381,
    }
    assert "checkpoint" not in json.dumps(status)


def test_mcp_db_errors_are_safe_and_do_not_leak_exception_text(caplog: pytest.LogCaptureFixture) -> None:
    @contextmanager
    def unavailable(_config: MCPConfig):
        raise RuntimeError("secret-dsn and private query text")
        yield  # pragma: no cover

    service = ToolService(_config(), connection_factory=unavailable, worker_call=lambda *_a, **_k: {})
    caplog.set_level(logging.DEBUG)

    with pytest.raises(ToolFailure) as caught:
        service.list_projects()

    assert caught.value.code == "db_unavailable" or caught.value.code == "internal"
    assert "secret-dsn" not in caplog.text
    assert "private query text" not in caplog.text


def test_mcp_query_text_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    secret_query = "private-query-sentinel"
    caplog.set_level(logging.INFO)
    _service(FakeConnection()).search(secret_query, "sample-project")
    assert secret_query not in caplog.text
