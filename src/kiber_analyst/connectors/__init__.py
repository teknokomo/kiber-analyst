"""Read-only discussion connectors for GitHub, GitLab, and GitVerse."""

from __future__ import annotations

from kiber_analyst.connectors.base import (
    Connector,
    ConnectorHttpClient,
    NormalizedThread,
    RawItem,
    RawThread,
    ThreadRef,
)
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.github import GitHubConnector
from kiber_analyst.connectors.gitlab import GitLabConnector
from kiber_analyst.connectors.gitverse import GitVerseConnector
from kiber_analyst.connectors.source_ids import parse_source_id

PROVIDERS = ("github", "gitlab", "gitverse")

_CONNECTORS = {
    "github": GitHubConnector,
    "gitlab": GitLabConnector,
    "gitverse": GitVerseConnector,
}


def get_connector(provider: str, *, raw_dir=None) -> Connector:
    """Build the connector for a supported provider; tokens come from the environment."""
    cls = _CONNECTORS.get(provider)
    if cls is None:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"Provider {provider!r} is not supported",
        )
    return cls(raw_dir=raw_dir)


__all__ = [
    "PROVIDERS",
    "Connector",
    "ConnectorError",
    "ConnectorCodes",
    "ConnectorHttpClient",
    "GitHubConnector",
    "GitLabConnector",
    "GitVerseConnector",
    "NormalizedThread",
    "RawItem",
    "RawThread",
    "ThreadRef",
    "get_connector",
    "parse_source_id",
]
