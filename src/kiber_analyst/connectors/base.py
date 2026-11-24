"""Connector protocol and shared HTTP layer (ТЗ 0.2.0 §5).

Rules enforced here:
- all network calls happen through connectors, never in normalize or load;
- raw upstream responses are written to the job directory when raw_dir is set;
- tokens are sent as Authorization headers only and never logged;
- pagination always runs to the end unless the caller stops consuming;
- retryable failures (429, 5xx, timeouts, network) back off with jitter and
  respect Retry-After; non-retryable ones raise immediately with stable codes.
"""

from __future__ import annotations

import email.utils
import json
import math
import random
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlencode, urljoin, urlsplit

from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.source_ids import parse_source_id

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_RAW_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class ThreadRef:
    provider: str
    source_id: str
    thread_ref: str
    kind: str
    number: int
    updated_at: str | None = None
    state: str | None = None
    title: str | None = None


@dataclass
class RawItem:
    item_kind: str
    external_id: str | None
    data: dict[str, Any]
    url: str | None = None


@dataclass
class RawThread:
    ref: ThreadRef
    body: dict[str, Any] | None
    items: list[RawItem]
    complete: bool = True
    errors: list[dict[str, str]] = field(default_factory=list)

    def add_unsupported(self, method: str, endpoint: str, status: str) -> None:
        # Exact method/endpoint/status only; never the response body.
        self.errors.append(
            {"code": "unsupported", "method": method, "endpoint": endpoint, "status": status}
        )


@dataclass
class NormalizedThread:
    thread: dict[str, Any]
    items: list[dict[str, Any]]


class Connector(Protocol):
    provider: str

    def validate_source(self, source_id: str) -> None: ...

    def discover(self, source_id: str, checkpoint: dict | None) -> Iterator[ThreadRef]: ...

    def fetch_thread(self, ref: ThreadRef) -> RawThread: ...

    def fetch_items(self, ref: ThreadRef) -> Iterator[RawItem]: ...

    def normalize(self, raw: RawThread) -> NormalizedThread: ...


class ConnectorHttpClient:
    """Shared httpx transport with retry, pagination, and raw dumps."""

    def __init__(
        self,
        config: ProviderConfig,
        *,
        transport: Any = None,
        sleep: Any = None,
        jitter: Any = None,
        raw_dir: Path | None = None,
        default_headers: dict[str, str] | None = None,
    ) -> None:
        self._config = config
        self._sleep = sleep or time.sleep
        self._jitter = jitter or (lambda seconds: random.uniform(seconds * 0.8, seconds * 1.2))
        self._raw_dir = raw_dir
        self._client: Any = None
        self._transport = transport
        self._default_headers = default_headers or {}
        self._last_response: Any = None
        config_parts = urlsplit(config.api_url)
        config_host = (config_parts.hostname or "").lower()
        try:
            config_port = config_parts.port or 443
        except ValueError:
            config_port = -1
        self._configured_origin = (config_parts.scheme.lower(), config_host, config_port)
        self._allowed_hosts = {config_host.lower()} | {
            host.lower() for host in config.allowed_hosts
        }

    @property
    def allowed_hosts(self) -> frozenset[str]:
        return frozenset(self._allowed_hosts)

    def _validate_target(self, url: str) -> None:
        """Reject any authenticated request outside the configured HTTPS hosts.

        The target is resolved against the configured API base first, so
        relative paths, protocol-relative references, and pagination links are
        all checked before the Authorization header can be attached.
        """
        target = urljoin(self._config.api_url.rstrip("/") + "/", url)
        parts = urlsplit(target)
        host = (parts.hostname or "").lower()
        try:
            port = parts.port or 443
        except ValueError:
            port = -1
        same_configured_host = host == self._configured_origin[1]
        allowed_origin = (
            (parts.scheme.lower(), host, port) == self._configured_origin
            if same_configured_host
            else host in self._allowed_hosts and port == 443
        )
        if parts.scheme.lower() != "https" or not host or not allowed_origin:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                f"{self._config.provider} request target is not an allowed HTTPS endpoint",
            )
        if parts.username or parts.password:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                f"{self._config.provider} request target contains credentials",
            )

    # -- lifecycle ---------------------------------------------------------

    def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                import httpx
            except ImportError as exc:
                raise ConnectorError(
                    ConnectorCodes.UNSUPPORTED_PROVIDER,
                    "HTTP support is unavailable; install kiber-analyst[connectors]",
                ) from exc
            self._client = httpx.Client(
                base_url=self._config.api_url,
                timeout=self._config.timeout_seconds,
                transport=self._transport,
                follow_redirects=False,
                headers={
                    "Authorization": f"Bearer {self._config.token}",
                    "Accept": "application/json",
                    "User-Agent": "kiber-analyst-connector",
                    **self._default_headers,
                },
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- raw dumps ---------------------------------------------------------

    def dump_raw(self, name: str, payload: Any) -> None:
        if self._raw_dir is None:
            return
        safe = _SAFE_NAME.sub("-", name)[:120] or "unnamed"
        target = self._raw_dir / f"{safe}.json"
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        if len(encoded) > _MAX_RAW_BYTES:
            encoded = json.dumps(
                {"truncated": True, "name": safe}, ensure_ascii=False
            ).encode("utf-8")
        target.write_bytes(encoded)
        target.chmod(0o600)

    # -- requests ----------------------------------------------------------

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        etag: str | None = None,
        json_body: Any = None,
    ) -> tuple[Any, bool]:
        """Return (response, not_modified); retryable failures back off and retry."""
        self._validate_target(url)
        client = self._ensure_client()
        attempt = 0
        while True:
            attempt += 1
            request_headers = dict(headers or {})
            if etag is not None:
                request_headers["If-None-Match"] = etag
            try:
                response = client.request(
                    method,
                    url,
                    params=params,
                    headers=request_headers,
                    json=json_body,
                )
            except Exception as exc:  # httpx transport errors share this base
                if attempt > self._config.max_retries or not _is_transport_error(exc):
                    raise ConnectorError(
                        ConnectorCodes.UPSTREAM_UNAVAILABLE,
                        f"{self._config.provider} request failed after retries",
                    ) from exc
                self._sleep(min(self._jitter(_backoff_seconds(attempt)), 60.0))
                continue
            status = response.status_code
            self._last_response = response
            if status == 304:
                return response, True
            if status in _RETRYABLE_STATUS:
                if attempt > self._config.max_retries:
                    code = (
                        ConnectorCodes.RATE_LIMITED
                        if status == 429
                        else ConnectorCodes.UPSTREAM_UNAVAILABLE
                    )
                    raise ConnectorError(
                        code,
                        f"{self._config.provider} is throttling or unavailable",
                        status=status,
                    )
                delay = _backoff_seconds(attempt)
                retry_after = response.headers.get("Retry-After")
                server_delay = _retry_after_seconds(retry_after)
                if status == 429 and server_delay is not None:
                    # Retry-After is a server-specified lower bound; never cap
                    # it below what the provider asked for.
                    self._sleep(max(server_delay, delay))
                else:
                    self._sleep(min(self._jitter(delay), 60.0))
                continue
            if status in (401, 403):
                raise ConnectorError(
                    ConnectorCodes.AUTH_FORBIDDEN,
                    f"{self._config.provider} rejected the token for {method} {url}",
                    status=status,
                )
            if status == 404:
                raise ConnectorError(
                    ConnectorCodes.SOURCE_NOT_FOUND,
                    f"{self._config.provider} endpoint {method} {url} was not found",
                    status=status,
                )
            if status == 422:
                raise ConnectorError(
                    ConnectorCodes.MALFORMED_RESPONSE,
                    f"{self._config.provider} rejected {method} {url} as invalid",
                    status=status,
                )
            if status in (301, 302, 307, 308):
                raise ConnectorError(
                    ConnectorCodes.UPSTREAM_UNAVAILABLE,
                    f"{self._config.provider} redirected {method} {url}; base URL is wrong",
                    status=status,
                )
            if status != 200 and status != 201:
                raise ConnectorError(
                    ConnectorCodes.UPSTREAM_UNAVAILABLE,
                    f"{self._config.provider} returned {status} for {method} {url}",
                    status=status,
                )
            return response, False

    def request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        etag: str | None = None,
        json_body: Any = None,
    ) -> tuple[Any, bool]:
        response, not_modified = self.request(
            method, url, params=params, headers=headers, etag=etag, json_body=json_body
        )
        if not_modified:
            return None, True
        try:
            payload = response.json()
        except ValueError as exc:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                f"{self._config.provider} response was not JSON for {method} {url}",
            ) from exc
        self.dump_raw(f"{method}-{url}-{json.dumps(params or {}, sort_keys=True)}", payload)
        return payload, False

    # -- pagination --------------------------------------------------------

    def iter_pages(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        paging: str = "link",
        page_param: str = "page",
    ) -> Iterator[tuple[list, dict[str, str]]]:
        """Yield (items, headers) per page, following the provider's paging style.

        A page chain ends only when the provider omits the next-page marker.
        Malformed or unparsable markers raise MALFORMED_RESPONSE instead of
        silently reporting a truncated result as complete.
        """
        next_url: str | None = url
        next_params = dict(params) if params else None
        seen_targets = {_page_marker(next_url, next_params, self._config.api_url)}
        while next_url is not None:
            payload, _not_modified = self.request_json(
                "GET", next_url, params=next_params, headers=headers
            )
            if payload is None:
                return
            response = self._last_response
            if isinstance(payload, dict):
                # Some endpoints wrap lists; callers expecting lists treat
                # dicts as a single object page and stop after it.
                items: list = [payload]
            elif isinstance(payload, list):
                items = payload
            else:
                raise ConnectorError(
                    ConnectorCodes.MALFORMED_RESPONSE,
                    f"{self._config.provider} returned an unexpected payload shape",
                )
            yield items, dict(response.headers) if response is not None else {}
            next_url, next_params = _next_page(
                response,
                paging=paging,
                page_param=page_param,
                current_params=next_params or {},
                current_url=next_url,
                provider=self._config.provider,
            )
            if next_url is not None:
                marker = _page_marker(next_url, next_params, self._config.api_url)
                if marker in seen_targets:
                    raise ConnectorError(
                        ConnectorCodes.MALFORMED_RESPONSE,
                        f"{self._config.provider} pagination does not advance",
                    )
                seen_targets.add(marker)

    def request_raw(self, method: str, url: str, *, params=None, headers=None):
        """Return the httpx.Response itself (used for header-only probes)."""
        response, _not_modified = self.request(method, url, params=params, headers=headers)
        return response


def _is_transport_error(exc: Exception) -> bool:
    module = type(exc).__module__ or ""
    return module.startswith("httpx")


def _backoff_seconds(attempt: int) -> float:
    return min(2.0 ** (attempt - 1), 60.0)


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = (retry_at - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _page_marker(
    url: str, params: dict[str, Any] | None, base_url: str
) -> tuple[str, str]:
    resolved = urljoin(base_url.rstrip("/") + "/", url)
    encoded_params = urlencode(sorted((params or {}).items()), doseq=True)
    return resolved, encoded_params


def _next_page(
    response: Any,
    *,
    paging: str,
    page_param: str,
    current_params: dict[str, Any],
    current_url: str | None,
    provider: str,
) -> tuple[str | None, dict[str, Any] | None]:
    if response is None:
        return None, {}
    if paging == "link":
        link = response.headers.get("Link") or ""
        match = re.search(r'<([^>]+)>;\s*rel="next"', link)
        if match is None:
            return None, {}
        target = match.group(1).strip()
        parts = urlsplit(target)
        if parts.scheme != "https" or not parts.hostname:
            # A next link we cannot resolve to an absolute HTTPS URL must not
            # be treated as the end of the collection.
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                f"{provider} returned an unusable next-page link",
            )
        return target, None
    header = (response.headers.get("X-Next-Page") or "").strip()
    if not header:
        return None, {}
    if re.fullmatch(r"[1-9][0-9]{0,9}", header) is None:
        raise ConnectorError(
            ConnectorCodes.MALFORMED_RESPONSE,
            f"{provider} returned an invalid next-page cursor",
        )
    params = dict(current_params)
    params[page_param] = header
    return current_url, params


def validate_connector_source(source_id: str, provider: str) -> None:
    parsed = parse_source_id(source_id)
    if parsed is None or parsed.provider != provider:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            f"source_id is not a valid {provider} repository identifier",
        )
