"""Stable connector error codes; messages never carry tokens or raw responses."""

from __future__ import annotations

from kiber_analyst.errors import IngestError


class ConnectorCodes:
    UNSUPPORTED_PROVIDER = "UNSUPPORTED_PROVIDER"
    SOURCE_NOT_FOUND = "SOURCE_NOT_FOUND"
    SOURCE_NOT_ALLOWED = "SOURCE_NOT_ALLOWED"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    AUTH_FORBIDDEN = "AUTH_FORBIDDEN"
    RATE_LIMITED = "RATE_LIMITED"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    CHECKPOINT_INVALID = "CHECKPOINT_INVALID"
    THREAD_NOT_FOUND = "THREAD_NOT_FOUND"
    NORMALIZE_FAILED = "NORMALIZE_FAILED"
    LOAD_FAILED = "LOAD_FAILED"
    EMBED_FAILED = "EMBED_FAILED"
    SEARCH_INVALID_SOURCE = "SEARCH_INVALID_SOURCE"


_RETRYABLE_CODES = frozenset(
    {ConnectorCodes.RATE_LIMITED, ConnectorCodes.UPSTREAM_UNAVAILABLE}
)


class ConnectorError(IngestError):
    """Expected connector failure with a stable public code."""

    def __init__(self, code: str, message: str, *, status: int | None = None) -> None:
        self.code = code
        self.status = status
        super().__init__(message)

    @property
    def retryable(self) -> bool:
        return self.code in _RETRYABLE_CODES
