from __future__ import annotations

import ipaddress
import os
import uuid

import pytest
from starlette.testclient import TestClient

psycopg = pytest.importorskip("psycopg")

from kiber_analyst.embedding_contract import DIMENSIONS, MODEL_KEY  # noqa: E402
from kiber_analyst.search_core import execute_search  # noqa: E402
from kiber_analyst_mcp.config import MCPConfig  # noqa: E402
from kiber_analyst_mcp.db import readonly_connection  # noqa: E402
from kiber_analyst_mcp.server import create_app  # noqa: E402
from kiber_analyst_mcp.tools import ToolFailure, ToolService  # noqa: E402

TEST_DSN = os.environ.get("KIBER_ANALYST_MCP_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not TEST_DSN,
    reason="set KIBER_ANALYST_MCP_TEST_DSN to an isolated disposable acceptance database",
)
DOCUMENT_ID = "00000000-0000-0000-0000-000000000020"


@pytest.fixture(scope="module")
def mcp_config() -> MCPConfig:
    assert TEST_DSN
    values = psycopg.conninfo.conninfo_to_dict(TEST_DSN)
    host = values.get("host", "127.0.0.1")
    resolved_host = "127.0.0.1" if host == "localhost" else host
    assert ipaddress.ip_address(resolved_host).is_private
    assert values.get("dbname") == "knowledge"
    assert values.get("user") == "knowledge_reader"
    return MCPConfig(
        db_host=host,
        db_port=int(values.get("port", "5433")),
        db_name="knowledge",
        db_user="knowledge_reader",
        db_password=values.get("password", "test-only"),
        auth_token="b" * 64,
    )


@pytest.fixture(scope="module")
def service(mcp_config: MCPConfig) -> ToolService:
    def worker_call(payload: dict, *, timeout: float) -> dict:
        assert timeout == 10
        assert payload["model"] == MODEL_KEY
        return {
            "status": "done",
            "vector": [0.0] * DIMENSIONS,
            "dimensions": DIMENSIONS,
            "model": MODEL_KEY,
            "scope": "google:test",
            "request_id": str(uuid.uuid4()),
        }

    return ToolService(
        mcp_config, connection_factory=readonly_connection, worker_call=worker_call
    )


def test_reader_connection_is_read_only_and_acl_denies_all_writes(mcp_config: MCPConfig) -> None:
    with psycopg.connect(
        host=mcp_config.db_host,
        port=mcp_config.db_port,
        dbname=mcp_config.db_name,
        user=mcp_config.db_user,
        password=mcp_config.db_password,
        autocommit=True,
    ) as conn:
        assert conn.execute("SELECT current_setting('default_transaction_read_only')").fetchone()[0] == "on"
        assert conn.execute("SELECT count(*) FROM segments").fetchone()[0] == 12
        privileges = conn.execute(
            """SELECT
                 has_database_privilege('knowledge_reader','knowledge','TEMP'),
                 has_database_privilege('knowledge_app','knowledge','TEMP'),
                 has_database_privilege('knowledge_owner','knowledge','TEMP'),
                 pg_has_role('knowledge_reader','knowledge_app','MEMBER'),
                 pg_has_role('knowledge_reader','knowledge_owner','MEMBER'),
                 has_table_privilege('knowledge_reader','segments','SELECT'),
                 has_table_privilege('knowledge_reader','segments','INSERT'),
                 has_table_privilege('knowledge_reader','segments','UPDATE'),
                 has_table_privilege('knowledge_reader','segments','DELETE')"""
        ).fetchone()
    assert tuple(privileges) == (False, True, True, False, False, True, False, False, False)

    statements = [
        "INSERT INTO segments(document_id,seq,text) VALUES ('00000000-0000-0000-0000-000000000020',999,'x')",
        "UPDATE segments SET text=text WHERE id=1",
        "DELETE FROM segments WHERE id=1",
        "CREATE TABLE public.reader_forbidden(id integer)",
        "CREATE TEMP TABLE reader_temp_forbidden(id integer)",
        "SELECT count(*) FROM pg_authid",
    ]
    for statement in statements:
        with psycopg.connect(
            host=mcp_config.db_host,
            port=mcp_config.db_port,
            dbname=mcp_config.db_name,
            user=mcp_config.db_user,
            password=mcp_config.db_password,
            autocommit=True,
        ) as conn:
            conn.execute("BEGIN READ WRITE")
            with pytest.raises(psycopg.Error) as rejected:
                conn.execute(statement)
            assert rejected.value.sqlstate == "42501"
            conn.rollback()


@pytest.mark.parametrize("source,expected", [("segment", "segment"), ("asr", "asr"), ("all", None)])
def test_reader_search_segment_asr_union_and_filters(
    service: ToolService, source: str, expected: str | None
) -> None:
    result = service.search(
        "Example Station",
        "sample-project",
        limit=20,
        source=source,
        source_kind="telegram",
        date_from="2020-01-02",
        date_to="2020-01-02",
        author="Example Author",
        topic="1",
    )
    sources = {item["source"] for item in result["results"]}
    if expected is None:
        assert sources == {"segment", "asr"}
    else:
        assert sources == {expected}
    assert result["degraded"] is False
    assert all(item["project_slug"] == "sample-project" for item in result["results"])
    assert all(item["author"] == "Example Author" for item in result["results"])
    assert all(item["url"].startswith("https://t.me/") for item in result["results"])
    if source in {"asr", "all"}:
        asr = next(item for item in result["results"] if item["source"] == "asr")
        assert asr["asr_run_id"] is not None
        assert asr["start_ms"] is not None and asr["end_ms"] is not None
        assert asr["timecode"] is not None


def test_reader_search_topic1_provenance_limits_and_sender_fallback(
    service: ToolService,
) -> None:
    query = "Example Station"
    five = service.search(
        query, "sample-project", limit=5, source_kind="telegram", topic="1"
    )
    assert len(five["results"]) == 5
    item = five["results"][0]
    assert item["chat_title"] == "Example Group"
    assert item["topic_title"] == "Example topic"
    assert item["topic_id"] == "1"
    assert item["source_id"] == "telegram:-1009000:topic:1"

    one = service.search(
        query, "sample-project", limit=1, source_kind="telegram", topic="1"
    )
    assert len(one["results"]) == 1

    for invalid_limit in (0, 100):
        with pytest.raises(ToolFailure, match="limit"):
            service.search(query, "sample-project", limit=invalid_limit, topic="1")

    sender = service.search(
        "идентификатор отправителя",
        "sample-project",
        limit=5,
        source_kind="telegram",
        author="sender-42",
        topic="1",
    )
    assert sender["results"]
    assert all(item["author"] == "sender-42" for item in sender["results"])
    assert any(item["source"] == "segment" for item in sender["results"])

    unknown = service.search(
        "значения Unknown",
        "sample-project",
        limit=1,
        source_kind="telegram",
        author="Unknown",
        topic="1",
    )
    assert len(unknown["results"]) == 1
    assert unknown["results"][0]["author"] == "Unknown"


def test_reader_get_document_paginates_all_segments_and_asr(service: ToolService) -> None:
    cursor = None
    segments: list[dict] = []
    asr_results: list[dict] = []
    while True:
        page = service.get_document(DOCUMENT_ID, limit=100, cursor=cursor)
        segments.extend(page["segments"])
        asr_results.extend(page["asr_results"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert len(segments) == 12
    assert len({item["segment_id"] for item in segments}) == 12
    assert len(asr_results) == 3
    assert len({item["external_ref"] for item in asr_results}) == 3
    assert all("raw_path" not in item and "meta" not in item for item in segments)


def test_reader_project_and_ingest_counters(service: ToolService) -> None:
    project = service.list_projects()["projects"][0]
    status = service.ingest_status("sample-project")

    assert project["slug"] == "sample-project"
    assert project["segments"] == 12
    assert project["asr_results"] == 3
    assert project["asr_segments"] == 8
    assert project["embeddings"] == 14
    assert status["counts"] == {
        "sources": 1,
        "documents": 1,
        "segments": 12,
        "asr_runs": 2,
        "asr_results": 3,
        "asr_segments": 8,
        "asr_attempts": 4,
        "asr_embeddings": 6,
        "source_embeddings": 8,
    }
    assert status["latest_snapshot"]["scope"] == "topic"


def test_reader_search_matches_shared_cli_search_core(
    mcp_config: MCPConfig, service: ToolService
) -> None:
    query = "Example Station"
    with readonly_connection(mcp_config) as conn:
        direct = execute_search(
            conn,
            query,
            project="sample-project",
            vector=[0.0] * DIMENSIONS,
            limit=10,
        )
    through_mcp = service.search(query, "sample-project", limit=10)
    through_mcp.pop("count")
    assert through_mcp == direct


def test_streamable_http_initialize_list_and_all_tool_calls(
    mcp_config: MCPConfig, service: ToolService
) -> None:
    token = mcp_config.auth_token
    app = create_app(mcp_config, service)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
    }

    def rpc(method: str, request_id: int, params: dict | None = None) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {},
        }

    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        initialized = client.post(
            "/mcp",
            json=rpc(
                "initialize",
                1,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "postgres-acceptance", "version": "1"},
                },
            ),
            headers=headers,
        )
        assert initialized.status_code == 200
        listed = client.post("/mcp", json=rpc("tools/list", 2), headers=headers)
        assert listed.status_code == 200
        assert {tool["name"] for tool in listed.json()["result"]["tools"]} == {
            "search", "get_document", "list_projects", "ingest_status"
        }
        arguments = {
            "search": {"query": "Example Station", "project": "sample-project", "source": "all"},
            "get_document": {"document_id": DOCUMENT_ID, "limit": 2},
            "list_projects": {},
            "ingest_status": {"project": "sample-project"},
        }
        for offset, (name, tool_arguments) in enumerate(arguments.items(), start=3):
            response = client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    offset,
                    {"name": name, "arguments": tool_arguments},
                ),
                headers=headers,
            )
            assert response.status_code == 200, response.text
            result = response.json()["result"]
            assert result["isError"] is False, result
            assert "structuredContent" in result
