from __future__ import annotations

import hmac
from typing import Any

from starlette.responses import JSONResponse


class BearerAuthMiddleware:
    """Require the configured bearer token for all Streamable HTTP paths."""

    def __init__(self, app: Any, token: str) -> None:
        self.app = app
        self.token = token.encode("ascii")

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope.get("path", "")
        if path == "/mcp" or path.startswith("/mcp/"):
            headers = {key.lower(): value for key, value in scope.get("headers", [])}
            supplied = headers.get(b"authorization", b"")
            scheme, separator, value = supplied.partition(b" ")
            valid = (
                separator == b" "
                and scheme.lower() == b"bearer"
                and hmac.compare_digest(value, self.token)
            )
            if not valid:
                response = JSONResponse(
                    {"error": {"code": "unauthorized", "message": "Bearer authorization is required"}},
                    status_code=401,
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
