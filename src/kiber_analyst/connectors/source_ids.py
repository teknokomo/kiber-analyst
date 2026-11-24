"""Provider source_id, thread_ref, and item_ref formats (ТЗ 0.2.0 §4).

Formats:
- github:[<host>]<owner>/<repo>            host only for GitHub Enterprise (FQDN with a dot)
- gitlab:<host>/<namespace>[/<group>...]/<project>   host is always required
- gitverse:<owner>/<repo>

Thread refs:  <source_id>:issue:12 | pull | discussion | merge_request
Item refs:    <thread_ref>:body or <thread_ref>:<item_kind>:<external_id>
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError

PROVIDERS = ("github", "gitlab", "gitverse")

_THREAD_KINDS = {
    "github": frozenset({"issue", "pull", "discussion"}),
    "gitlab": frozenset({"issue", "merge_request"}),
    "gitverse": frozenset({"issue", "pull"}),
}

ITEM_KINDS = frozenset(
    {"body", "comment", "review", "review_comment", "reply", "event"}
)

_HOST = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+"
_GITHUB_OWNER = r"[A-Za-z0-9][A-Za-z0-9-]{0,38}"
_REPO_SEGMENT = r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}"
_GITLAB_PATH = r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+"
_EXTERNAL_ITEM_ID = r"[A-Za-z0-9_-]{1,80}={0,2}"

_SOURCE_PATTERNS = {
    "github": re.compile(
        rf"github:(?:(?P<host>{_HOST})/(?P<path>{_GITHUB_OWNER}/{_REPO_SEGMENT})"
        rf"|(?P<path2>{_GITHUB_OWNER}/{_REPO_SEGMENT}))\Z"
    ),
    "gitlab": re.compile(rf"gitlab:(?P<host>{_HOST})/(?P<path>{_GITLAB_PATH})\Z"),
    "gitverse": re.compile(rf"gitverse:(?P<path>{_REPO_SEGMENT}/{_REPO_SEGMENT})\Z"),
}

_NUMBER = re.compile(r"[1-9][0-9]{0,17}\Z")


@dataclass(frozen=True)
class ExternalSourceId:
    provider: str
    external_id: str
    host: str | None
    path: str

    @property
    def repo_path(self) -> str:
        """Path after the optional host: owner/repo or namespace/project."""
        return self.path


def provider_for(source_id: str) -> str | None:
    if not isinstance(source_id, str):
        return None
    prefix = source_id.split(":", 1)[0]
    return prefix if prefix in PROVIDERS else None


def parse_source_id(source_id: str) -> ExternalSourceId | None:
    """Parse a provider source_id; return None when the format is invalid."""
    provider = provider_for(source_id)
    if provider is None or len(source_id) > 200:
        return None
    match = _SOURCE_PATTERNS[provider].fullmatch(source_id)
    if match is None:
        return None
    groups = match.groupdict()
    host = groups.get("host")
    path = groups.get("path") or groups.get("path2")
    return ExternalSourceId(
        provider=provider, external_id=source_id.split(":", 1)[1], host=host, path=path
    )


def is_external_source_id(source_id: str) -> bool:
    return parse_source_id(source_id) is not None


def validate_source_id(source_id: str) -> ExternalSourceId:
    """Parse or raise; unknown provider and malformed ids get stable codes."""
    provider = provider_for(source_id)
    if provider is None:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            "Source provider is not supported",
        )
    parsed = parse_source_id(source_id)
    if parsed is None:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            f"{provider} source_id format is invalid",
        )
    return parsed


def thread_ref(source_id: str, kind: str, number: int) -> str:
    parsed = validate_source_id(source_id)
    allowed = _THREAD_KINDS.get(parsed.provider, frozenset())
    if kind not in allowed:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            f"Thread kind {kind!r} is invalid for {parsed.provider}",
        )
    if type(number) is not int or number <= 0:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            "Thread number must be a positive integer",
        )
    return f"{source_id}:{kind}:{number}"


def parse_thread_ref(thread_ref_value: str) -> tuple[ExternalSourceId, str, int] | None:
    if not isinstance(thread_ref_value, str):
        return None
    parts = thread_ref_value.rsplit(":", 2)
    if len(parts) != 3:
        return None
    source_part, kind, number_part = parts
    if _NUMBER.fullmatch(number_part) is None or kind not in _all_thread_kinds():
        return None
    parsed = parse_source_id(source_part)
    if parsed is None or kind not in _THREAD_KINDS.get(parsed.provider, frozenset()):
        return None
    return parsed, kind, int(number_part)


def item_ref(thread_ref_value: str, item_kind: str, external_id: str | None = None) -> str:
    if item_kind not in ITEM_KINDS:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            f"Item kind {item_kind!r} is invalid",
        )
    if item_kind == "body":
        return f"{thread_ref_value}:body"
    if not isinstance(external_id, str) or re.fullmatch(_EXTERNAL_ITEM_ID, external_id) is None:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            "External item identifier is invalid",
        )
    return f"{thread_ref_value}:{item_kind}:{external_id}"


def normalize_external_id(value: object) -> str | None:
    """Coerce an upstream identifier into the safe item-ref token shape."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        if value <= 0:
            return None
        return str(value)
    if isinstance(value, str):
        token = value.strip()
        if re.fullmatch(_EXTERNAL_ITEM_ID, token):
            return token
        return None
    return None


def _all_thread_kinds() -> frozenset[str]:
    result: set[str] = set()
    for kinds in _THREAD_KINDS.values():
        result |= kinds
    return frozenset(result)
