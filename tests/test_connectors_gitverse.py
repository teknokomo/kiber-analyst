"""Contract tests for the GitVerse connector over a mock HTTP transport."""

from __future__ import annotations

import httpx
import pytest

from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.gitverse import GitVerseConnector

SOURCE = "gitverse:example-owner/example-repo"
ISSUE = {
    "number": 5,
    "title": "Investigate connector",
    "state": "open",
    "user": {"login": "alice"},
    "created_at": "2026-11-01T10:00:00Z",
    "updated_at": "2026-11-02T10:00:00Z",
    "html_url": "https://gitverse.ru/example-owner/example-repo/issues/5",
    "body": "Check the API",
    "labels": [{"name": "question"}],
}
COMMENT = {
    "id": 33,
    "user": {"login": "bob"},
    "body": "Endpoint confirmed",
    "created_at": "2026-11-02T12:00:00Z",
    "html_url": "https://gitverse.ru/example-owner/example-repo/issues/5#issuecomment-33",
}
PULL = {
    "number": 9,
    "title": "Fix parser",
    "state": "open",
    "user": {"login": "alice"},
    "created_at": "2026-11-03T10:00:00Z",
    "updated_at": "2026-11-03T11:00:00Z",
    "html_url": "https://gitverse.ru/example-owner/example-repo/pulls/9",
    "body": "Fixes the parser",
}
REVIEW = {
    "id": 71,
    "state": "APPROVED",
    "user": {"login": "bob"},
    "body": "Good",
    "submitted_at": "2026-11-03T12:00:00Z",
    "html_url": "https://gitverse.ru/example-owner/example-repo/pulls/9#review-71",
}


def gitverse_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    assert request.headers.get("Accept") == "application/vnd.gitverse.object+json;version=1"
    if path == "/repos/example-owner/example-repo/issues":
        return httpx.Response(200, json=[ISSUE, {**PULL, "pull_request": {"url": "x"}}])
    if path == "/repos/example-owner/example-repo/pulls":
        return httpx.Response(200, json=[PULL])
    if path == "/repos/example-owner/example-repo/issues/5":
        return httpx.Response(200, json=ISSUE)
    if path == "/repos/example-owner/example-repo/issues/5/comments":
        return httpx.Response(200, json=[COMMENT])
    if path == "/repos/example-owner/example-repo/issues/5/timeline":
        return httpx.Response(200, json=[{"event": "commented", "user": {"login": "bob"},
                                          "created_at": "2026-11-02T12:00:00Z"}])
    if path == "/repos/example-owner/example-repo/pulls/9":
        return httpx.Response(200, json={**PULL, "merged": False})
    if path == "/repos/example-owner/example-repo/pulls/9/reviews":
        return httpx.Response(200, json=[REVIEW])
    if path == "/repos/example-owner/example-repo/pulls/9/comments":
        return httpx.Response(404)
    if path == "/repos/example-owner/example-repo/issues/9/timeline":
        return httpx.Response(200, json=[])
    return httpx.Response(404)


def make_connector(monkeypatch) -> GitVerseConnector:
    monkeypatch.setenv("KIBER_GITVERSE_TOKEN", "test-token")
    config = ProviderConfig(
        provider="gitverse", api_url="https://api.gitverse.ru",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    return GitVerseConnector(
        config=config,
        transport=httpx.MockTransport(gitverse_handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )


def test_discover_excludes_pull_entries_and_sends_required_headers(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    refs = list(connector.discover(SOURCE, None))
    issue_numbers = [ref.number for ref in refs if ref.kind == "issue"]
    pull_numbers = [ref.number for ref in refs if ref.kind == "pull"]
    assert issue_numbers == [5]
    assert pull_numbers == [9]
    connector.close()


def test_issue_thread_normalizes_comments_and_timeline(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    ref = next(iter(connector.discover(SOURCE, None)))
    raw = connector.fetch_thread(ref)
    normalized = connector.normalize(raw)
    assert normalized.thread["state"] == "open"
    assert normalized.thread["labels"] == ["question"]
    assert normalized.thread["meta"]["events"][0]["type"] == "commented"
    comment = next(
        item for item in normalized.items if item["item_kind"] == "comment"
    )
    assert comment["external_ref"] == f"{SOURCE}:issue:5:comment:33"
    assert comment["url"] == COMMENT["html_url"]
    connector.close()


def test_unsupported_review_comment_endpoint_is_recorded_not_fatal(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    pull_ref = next(
        ref for ref in connector.discover(SOURCE, None) if ref.kind == "pull"
    )
    raw = connector.fetch_thread(pull_ref)
    endpoints = {entry["endpoint"] for entry in raw.errors}
    assert endpoints == {
        "/repos/example-owner/example-repo/issues/9/comments",
        "/repos/example-owner/example-repo/pulls/9/comments",
    }
    assert all(entry["code"] == "unsupported" for entry in raw.errors)
    normalized = connector.normalize(raw)
    kinds = {item["item_kind"] for item in normalized.items}
    assert "review" in kinds
    assert "review_comment" not in kinds
    assert normalized.thread["state"] == "open"
    connector.close()


def test_validate_source_rejects_non_gitverse(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    with pytest.raises(ConnectorError) as exc:
        connector.validate_source("github:o/r")
    assert exc.value.code == ConnectorCodes.SEARCH_INVALID_SOURCE
    connector.close()
