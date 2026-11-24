"""Connector configuration (ТЗ 0.2.0 §11).

Tokens come only from the environment and are never logged. The optional
``connectors.yaml`` in the config directory overrides per-provider API
settings; ``config/connectors.example.yaml`` documents the shape.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.source_ids import PROVIDERS

TOKEN_ENV_VARS = {
    "github": "KIBER_GITHUB_TOKEN",
    "gitlab": "KIBER_GITLAB_TOKEN",
    "gitverse": "KIBER_GITVERSE_TOKEN",
}

_CONNECTOR_YAML = "connectors.yaml"

_PAGE_SIZE_RANGE = (1, 100)
_TIMEOUT_RANGE = (1, 300)
_RETRIES_RANGE = (0, 10)
_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")


@dataclass(frozen=True)
class ProviderConfig:
    provider: str
    api_url: str
    page_size: int
    timeout_seconds: int
    max_retries: int
    # Extra hosts (self-hosted GitLab, GitHub Enterprise) that may receive the
    # token. The api_url host is always allowed implicitly.
    allowed_hosts: tuple[str, ...] = ()

    @property
    def token(self) -> str:
        # repr=False is implicit for dataclass str fields we never print; the
        # value is deliberately excluded from every diagnostic and log line.
        token = os.environ.get(TOKEN_ENV_VARS[self.provider], "")
        if not token:
            raise ConnectorError(
                ConnectorCodes.AUTH_REQUIRED,
                f"Set {TOKEN_ENV_VARS[self.provider]} to a read-only token",
            )
        return token


def default_provider_config(provider: str) -> ProviderConfig:
    defaults = {
        "github": ("https://api.github.com", 100, 30, 5),
        "gitlab": ("https://gitlab.com/api/v4", 100, 30, 5),
        "gitverse": ("https://api.gitverse.ru", 100, 30, 5),
    }
    if provider not in defaults:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"Provider {provider!r} is not supported",
        )
    api_url, page_size, timeout, retries = defaults[provider]
    return ProviderConfig(
        provider=provider,
        api_url=api_url,
        page_size=page_size,
        timeout_seconds=timeout,
        max_retries=retries,
    )


def _validated_int(provider: str, key: str, value: Any, bounds: tuple[int, int]) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not bounds[0] <= value <= bounds[1]:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"{provider} config {key} must be an integer in {bounds[0]}..{bounds[1]}",
        )
    return value


def _validated_hosts(provider: str, value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(
        isinstance(host, str) and _HOST.fullmatch(host) for host in value
    ):
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"{provider} config allowed_hosts must be a list of host names",
        )
    return tuple(host.lower() for host in value)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            "Connector configuration support is unavailable; install kiber-analyst[connectors]",
        ) from exc
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            "Connector configuration file is unreadable or invalid",
        ) from None
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            "Connector configuration must be a mapping of providers",
        )
    return value


def load_provider_config(provider: str, *, config_file: Path | None = None) -> ProviderConfig:
    """Validated provider configuration; unknown providers are rejected at start."""
    base = default_provider_config(provider)
    path = config_file if config_file is not None else config_path(_CONNECTOR_YAML)
    if not path.is_file():
        return base
    document = _load_yaml(path)
    section = document.get(provider)
    if section is None:
        return base
    if not isinstance(section, dict):
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"{provider} connector configuration must be a mapping",
        )
    for key in section:
        if key not in {"api_url", "page_size", "timeout_seconds", "max_retries", "allowed_hosts"}:
            raise ConnectorError(
                ConnectorCodes.UNSUPPORTED_PROVIDER,
                f"{provider} connector configuration has an unknown key",
            )
    api_url = section.get("api_url", base.api_url)
    if not isinstance(api_url, str) or not api_url.startswith("https://"):
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"{provider} api_url must be an https:// URL",
        )
    return ProviderConfig(
        provider=provider,
        api_url=api_url,
        page_size=_validated_int(
            provider, "page_size", section.get("page_size", base.page_size), _PAGE_SIZE_RANGE
        ),
        timeout_seconds=_validated_int(
            provider,
            "timeout_seconds",
            section.get("timeout_seconds", base.timeout_seconds),
            _TIMEOUT_RANGE,
        ),
        max_retries=_validated_int(
            provider, "max_retries", section.get("max_retries", base.max_retries), _RETRIES_RANGE
        ),
        allowed_hosts=_validated_hosts(provider, section.get("allowed_hosts")),
    )


def supported_providers() -> tuple[str, ...]:
    return PROVIDERS
