"""Contract tests for the shared connector HTTP layer (ТЗ §8.4)."""

from __future__ import annotations

import httpx
import pytest

from kiber_analyst.connectors.base import ConnectorHttpClient
from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError


def make_client(monkeypatch, transport, *, raw_dir=None, sleeps=None):
    monkeypatch.setenv("KIBER_GITHUB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="github",
        api_url="https://api.github.com",
        page_size=100,
        timeout_seconds=30,
        max_retries=3,
    )
    calls = []
    if sleeps is None:
        sleeps = []
    return ConnectorHttpClient(
        config,
        transport=transport,
        sleep=lambda seconds: sleeps.append(seconds),
        jitter=lambda seconds: seconds,
        raw_dir=raw_dir,
        default_headers={"Accept": "application/vnd.github+json"},
    ), sleeps, calls


def test_missing_token_is_auth_required(monkeypatch) -> None:
    monkeypatch.delenv("KIBER_GITHUB_TOKEN", raising=False)
    config = ProviderConfig(
        provider="github", api_url="https://api.github.com",
        page_size=100, timeout_seconds=30, max_retries=5,
    )
    with pytest.raises(ConnectorError) as exc:
        _ = config.token
    assert exc.value.code == ConnectorCodes.AUTH_REQUIRED


def test_link_pagination_follows_rel_next_until_the_end(monkeypatch) -> None:
    pages = {"page": 1}

    def handler(request: httpx.Request) -> httpx.Response:
        current = pages["page"]
        if current == 1:
            pages["page"] = 2
            return httpx.Response(
                200, json=[{"n": 1}],
                headers={"Link": '<https://api.github.com/x?page=2>; rel="next"'},
            )
        return httpx.Response(200, json=[{"n": 2}, {"n": 3}])

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    collected = []
    for items, _headers in client.iter_pages("/repos/o/r/issues"):
        collected.extend(items)
    assert [item["n"] for item in collected] == [1, 2, 3]
    client.close()


def test_header_pagination_follows_x_next_page(monkeypatch) -> None:
    seen_pages = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page", "1")
        seen_pages.append(page)
        if page == "1":
            return httpx.Response(200, json=[{"n": 1}], headers={"X-Next-Page": "2"})
        return httpx.Response(200, json=[{"n": 2}], headers={"X-Next-Page": ""})

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    collected = []
    for items, _headers in client.iter_pages(
        "/projects/p/issues", params={"per_page": 1}, paging="header"
    ):
        collected.extend(items)
    assert seen_pages == ["1", "2"]
    assert [item["n"] for item in collected] == [1, 2]
    client.close()


def test_5xx_retries_with_backoff_then_succeeds(monkeypatch) -> None:
    attempts = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json=[{"ok": True}])

    client, sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    payload, _modified = client.request_json("GET", "/x")
    assert payload == [{"ok": True}]
    assert len(sleeps) == 2
    client.close()


def test_429_respects_retry_after_header(monkeypatch) -> None:
    attempts = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=[{"ok": True}])

    client, sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    payload, _modified = client.request_json("GET", "/x")
    assert payload == [{"ok": True}]
    assert sleeps == [7.0]
    client.close()


def test_429_after_max_retries_is_rate_limited(monkeypatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(ConnectorError) as exc:
        client.request_json("GET", "/x")
    assert exc.value.code == ConnectorCodes.RATE_LIMITED
    assert exc.value.retryable is True
    client.close()


def test_5xx_after_max_retries_is_upstream_unavailable(monkeypatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(ConnectorError) as exc:
        client.request_json("GET", "/x")
    assert exc.value.code == ConnectorCodes.UPSTREAM_UNAVAILABLE
    client.close()


@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, ConnectorCodes.AUTH_FORBIDDEN), (403, ConnectorCodes.AUTH_FORBIDDEN),
     (404, ConnectorCodes.SOURCE_NOT_FOUND), (422, ConnectorCodes.MALFORMED_RESPONSE)],
)
def test_non_retryable_statuses_map_to_stable_codes(
    monkeypatch, status: int, expected: str
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    client, sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(ConnectorError) as exc:
        client.request_json("GET", "/x")
    assert exc.value.code == expected
    assert exc.value.retryable is False
    assert sleeps == []
    client.close()


def test_transport_error_is_upstream_unavailable(monkeypatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(ConnectorError) as exc:
        client.request_json("GET", "/x")
    assert exc.value.code == ConnectorCodes.UPSTREAM_UNAVAILABLE
    client.close()


def test_etag_conditional_request_returns_not_modified(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("If-None-Match") == '"W/abc"':
            return httpx.Response(304)
        return httpx.Response(200, json=[{"n": 1}], headers={"ETag": '"W/abc"'})

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    response, _modified = client.request("GET", "/x")
    etag = response.headers.get("ETag")
    payload, not_modified = client.request_json("GET", "/x", etag=etag)
    assert payload is None and not_modified is True
    client.close()


def test_raw_responses_are_dumped_private_and_sanitized(monkeypatch, tmp_path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"title": "ok"})

    raw_dir = tmp_path / "raw"
    client, _sleeps, _calls = make_client(
        monkeypatch, httpx.MockTransport(handler), raw_dir=raw_dir
    )
    client.request_json("GET", "/repos/o/r/issues/../..", params={"page": 2})
    files = list(raw_dir.iterdir())
    assert len(files) == 1
    assert all(ch not in files[0].name for ch in "/:?")
    assert files[0].stat().st_mode & 0o777 == 0o600
    client.close()


def test_authorization_header_is_sent_but_never_logged(monkeypatch) -> None:
    seen_headers = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers["authorization"] = request.headers.get("Authorization")
        return httpx.Response(200, json=[])

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    client.request_json("GET", "/x")
    assert seen_headers["authorization"] == "Bearer test-token"
    message = ""
    try:
        client.request_json("GET", "/x", headers={"Authorization": "Bearer other"})
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
    assert "test-token" not in message
    client.close()


def test_non_json_response_is_malformed(monkeypatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>not json</html>")

    client, _sleeps, _calls = make_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(ConnectorError) as exc:
        client.request_json("GET", "/x")
    assert exc.value.code == ConnectorCodes.MALFORMED_RESPONSE
    client.close()
