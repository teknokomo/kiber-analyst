from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from kiber_analyst_mcp import __version__
from kiber_analyst_mcp.auth import BearerAuthMiddleware
from kiber_analyst_mcp.config import MCPConfig
from kiber_analyst_mcp.tools import ToolFailure, ToolService

SourceKind = Literal["telegram", "document", "web", "video", "github", "gitlab", "gitverse"]
Source = Literal["all", "segment", "asr"]


def _tool_result(operation: Callable[[],dict[str,Any]]) -> CallToolResult:
    try:
        payload=operation()
        is_error=False
    except ToolFailure as exc:
        payload={"error":{"code":exc.code,"message":exc.message}}
        is_error=True
    encoded=json.dumps(payload,ensure_ascii=False,separators=(",",":"))
    return CallToolResult(
        content=[TextContent(type="text",text=encoded)],
        structuredContent=payload,
        isError=is_error,
    )


def _register_tools(mcp:FastMCP,service:ToolService)->None:
    @mcp.tool()
    def search(
        query:str,
        project:str,
        limit:int=10,
        source:Source="all",
        source_kind:SourceKind|None=None,
        date_from:str|None=None,
        date_to:str|None=None,
        author:str|None=None,
        topic:str|None=None,
    )->CallToolResult:
        """Search source and ASR segments in the selected project."""
        return _tool_result(lambda:service.search(query,project,limit,source,source_kind,date_from,date_to,author,topic))

    @mcp.tool()
    def get_document(
        document_id:str,
        limit:int=100,
        cursor:str|None=None,
        include_asr:bool=True,
    )->CallToolResult:
        """Read one document page and its linked ASR transcripts."""
        return _tool_result(lambda:service.get_document(document_id,limit,cursor,include_asr))

    @mcp.tool()
    def list_projects()->CallToolResult:
        """List projects and aggregate corpus counts."""
        return _tool_result(service.list_projects)

    @mcp.tool()
    def ingest_status(project:str)->CallToolResult:
        """Read ingestion, ASR, embedding, and snapshot status for a project."""
        return _tool_result(lambda:service.ingest_status(project))


async def _healthz(_request:Request)->JSONResponse:
    return JSONResponse({"status":"ok","version":__version__})


def create_app(config:MCPConfig,service:ToolService|None=None)->Any:
    mcp=FastMCP(
        "knowledge",
        host=config.host,
        port=config.port,
        streamable_http_path=config.path,
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["127.0.0.1:8765", "localhost:8765"],
            allowed_origins=[
                "http://127.0.0.1:8765",
                "http://localhost:8765",
            ],
        ),
    )
    # FastMCP 1.x omits a public version argument; expose the package version in initialize.
    mcp._mcp_server.version = __version__
    _register_tools(mcp,service or ToolService(config))
    app=mcp.streamable_http_app()
    app.routes.insert(0,Route("/healthz",_healthz,methods=["GET"]))
    app.add_middleware(BearerAuthMiddleware,token=config.auth_token)
    return app


def main()->int:
    from uvicorn import run

    from kiber_analyst_mcp.config import load_config

    config=load_config()
    logging.basicConfig(level=config.log_level,format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("uvicorn.access").disabled=True
    run(create_app(config),host=config.host,port=config.port,access_log=False,log_config=None)
    return 0
