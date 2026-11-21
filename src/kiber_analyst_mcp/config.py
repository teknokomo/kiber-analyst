from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

from kiber_analyst.config_paths import config_path


class ConfigError(ValueError):
    """A safe MCP configuration error that never includes configured values."""


@dataclass(frozen=True)
class MCPConfig:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str = field(repr=False)
    auth_token: str = field(repr=False)
    host: str = "127.0.0.1"
    port: int = 8765
    path: str = "/mcp"
    log_level: str = "INFO"


def config_from_mapping(values: Mapping[str, object]) -> MCPConfig:
    required = (
        "MCP_DB_HOST", "MCP_DB_PORT", "MCP_DB_NAME", "MCP_DB_USER",
        "MCP_DB_PASSWORD", "MCP_AUTH_TOKEN",
    )
    if any(not isinstance(values.get(key), str) or not values[key] for key in required):
        raise ConfigError("Required MCP settings are missing")
    host = str(values["MCP_DB_HOST"])
    if host != "127.0.0.1":
        raise ConfigError("MCP database must use the IPv4 loopback address")
    try:
        db_port = int(str(values["MCP_DB_PORT"]))
    except ValueError:
        raise ConfigError("MCP database port is invalid") from None
    if db_port != 5433 or str(values["MCP_DB_NAME"]) != "knowledge" or str(values["MCP_DB_USER"]) != "knowledge_reader":
        raise ConfigError("MCP database target is invalid")
    token = str(values["MCP_AUTH_TOKEN"])
    if len(token) != 64 or re.fullmatch(r"[0-9a-fA-F]{64}", token) is None:
        raise ConfigError("MCP bearer token must be 32 random bytes encoded as hex")
    if str(values.get("MCP_HOST", "127.0.0.1")) != "127.0.0.1":
        raise ConfigError("MCP must bind to IPv4 loopback")
    try:
        port = int(str(values.get("MCP_PORT", "8765")))
    except ValueError:
        raise ConfigError("MCP listener port is invalid") from None
    if port != 8765 or str(values.get("MCP_PATH", "/mcp")) != "/mcp":
        raise ConfigError("MCP listener must use 127.0.0.1:8765/mcp")
    level = str(values.get("MCP_LOG_LEVEL", "INFO")).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError("MCP log level is invalid")
    return MCPConfig(
        db_host=host,
        db_port=db_port,
        db_name="knowledge",
        db_user="knowledge_reader",
        db_password=str(values["MCP_DB_PASSWORD"]),
        auth_token=token,
        log_level=level,
    )


def load_config(path: Path = config_path("mcp.env")) -> MCPConfig:
    try:
        values = dotenv_values(path)
    except OSError:
        raise ConfigError("MCP settings file is unavailable") from None
    merged = {key: value for key, value in values.items() if value is not None}
    # systemd exposes EnvironmentFile values through the process environment.
    # Environment overrides also support controlled tests and CLI launches.
    merged.update({key: value for key, value in os.environ.items() if key.startswith("MCP_")})
    return config_from_mapping(merged)
