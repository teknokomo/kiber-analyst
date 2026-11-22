from __future__ import annotations

from starlette.testclient import TestClient

from kiber_analyst_mcp.config import MCPConfig
from kiber_analyst_mcp.server import create_app

TOKEN = "test-placeholder-token-that-is-long-enough"


def _config() -> MCPConfig:
    return MCPConfig(
        db_host="127.0.0.1",
        db_port=5433,
        db_name="knowledge",
        db_user="knowledge_reader",
        db_password="fake-db-password",
        auth_token=TOKEN,
    )


def _rpc(method: str, request_id: int, params: dict | None = None) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params or {},
    }


def test_mcp_auth_required_tools_list_call_and_healthz() -> None:
    app = create_app(_config())
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        health = client.get("/healthz")
        assert health.status_code == 200
        assert set(health.json()) == {"status", "version"}

        initialize = _rpc(
            "initialize",
            1,
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "stage3-test", "version": "1"},
            },
        )
        assert client.post("/mcp", json=initialize).status_code == 401
        assert client.post(
            "/mcp", json=initialize, headers={"Authorization": "Bearer wrong"}
        ).status_code == 401

        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/json, text/event-stream",
        }
        initialized = client.post("/mcp", json=initialize, headers=headers)
        assert initialized.status_code == 200, initialized.text
        assert initialized.headers["content-type"].startswith("application/json")

        listed = client.post(
            "/mcp", json=_rpc("tools/list", 2), headers=headers
        )
        assert listed.status_code == 200, listed.text
        tool_names = {item["name"] for item in listed.json()["result"]["tools"]}
        assert tool_names == {"search", "get_document", "list_projects", "ingest_status"}

        called = client.post(
            "/mcp",
            json=_rpc(
                "tools/call",
                3,
                {"name": "search", "arguments": {"query": "", "project": "sample-project"}},
            ),
            headers=headers,
        )
        assert called.status_code == 200, called.text
        result = called.json()["result"]
        assert result["isError"] is True
        assert '"code":"invalid_argument"' in result["content"][0]["text"]


def test_mcp_sdk_host_check_remains_enabled() -> None:
    app = create_app(_config())
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        response = client.post(
            "/mcp",
            json=_rpc("initialize", 1, {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}),
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Host": "untrusted.example",
                "Accept": "application/json, text/event-stream",
            },
        )
    assert response.status_code in {400, 403, 421}


def test_mcp_tools_call_returns_structured_success() -> None:
    class StubService:
        def list_projects(self) -> dict:
            return {"projects": [{"slug": "sample-project"}]}

    app = create_app(_config(), service=StubService())
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json, text/event-stream",
    }
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        init = client.post(
            "/mcp",
            json=_rpc(
                "initialize",
                1,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "stage3-test", "version": "1"},
                },
            ),
            headers=headers,
        )
        assert init.status_code == 200
        called = client.post(
            "/mcp",
            json=_rpc("tools/call", 2, {"name": "list_projects", "arguments": {}}),
            headers=headers,
        )

    assert called.status_code == 200
    result = called.json()["result"]
    assert result["isError"] is False
    assert result["structuredContent"] == {"projects": [{"slug": "sample-project"}]}
