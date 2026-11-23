"""Contract tests for the GitHub connector over a mock HTTP transport."""

from __future__ import annotations

import httpx
import pytest

from kiber_analyst.connectors.base import RawThread, ThreadRef
from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.github import GitHubConnector

SOURCE = "github:teknokomo/kiber-analyst"
ISSUE = {
    "number": 12,
    "title": "Add connectors",
    "state": "open",
    "locked": False,
    "user": {"login": "alice"},
    "created_at": "2026-11-01T10:00:00Z",
    "updated_at": "2026-11-02T11:00:00Z",
    "html_url": "https://github.com/teknokomo/kiber-analyst/issues/12",
    "body": "Implement the connectors",
    "labels": [{"name": "bug"}, {"name": "api"}],
    "milestone": {"title": "0.2.0"},
    "assignees": [{"login": "bob"}],
}
COMMENT = {
    "id": 345,
    "user": {"login": "bob"},
    "body": "Working on it",
    "created_at": "2026-11-02T12:00:00Z",
    "updated_at": "2026-11-02T12:00:00Z",
    "html_url": "https://github.com/teknokomo/kiber-analyst/issues/12#issuecomment-345",
}
PULL = {
    "number": 34,
    "title": "Add GitHub connector",
    "state": "closed",
    "merged": True,
    "user": {"login": "alice"},
    "created_at": "2026-11-03T10:00:00Z",
    "updated_at": "2026-11-04T10:00:00Z",
    "html_url": "https://github.com/teknokomo/kiber-analyst/pull/34",
    "body": "Fixes #12",
}
REVIEW = {
    "id": 900,
    "state": "APPROVED",
    "user": {"login": "bob"},
    "body": "Looks good",
    "submitted_at": "2026-11-04T09:00:00Z",
    "html_url": "https://github.com/teknokomo/kiber-analyst/pull/34#pullrequestreview-900",
}
REVIEW_COMMENT = {
    "id": 901,
    "user": {"login": "bob"},
    "body": "Rename this",
    "created_at": "2026-11-04T08:00:00Z",
    "path": "src/kiber_analyst/connectors/github.py",
    "line": 42,
    "side": "RIGHT",
    "commit_id": "abc123",
    "diff_hunk": "@@ -40,6 +40,7 @@\n context",
    "html_url": "https://github.com/teknokomo/kiber-analyst/pull/34#discussion_r901",
}
TIMELINE = [
    {"event": "closed", "actor": {"login": "alice"}, "created_at": "2026-11-04T10:00:00Z"},
    {"event": "renamed", "actor": {"login": "alice"}, "created_at": "2026-11-03T11:00:00Z"},
] + [{"event": "commented", "actor": {"login": f"u{i}"}, "created_at": "2026-11-03T12:00:00Z"} for i in range(250)]


def github_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/repos/teknokomo/kiber-analyst/issues":
        return httpx.Response(
            200,
            json=[ISSUE, {**PULL, "pull_request": {"url": "x"}}],
        )
    if path == "/repos/teknokomo/kiber-analyst/pulls":
        return httpx.Response(200, json=[PULL])
    if path == "/repos/teknokomo/kiber-analyst/issues/12":
        return httpx.Response(200, json=ISSUE)
    if path == "/repos/teknokomo/kiber-analyst/issues/12/comments":
        return httpx.Response(200, json=[COMMENT])
    if path == "/repos/teknokomo/kiber-analyst/issues/12/timeline":
        return httpx.Response(200, json=TIMELINE)
    if path == "/repos/teknokomo/kiber-analyst/issues/34/comments":
        return httpx.Response(200, json=[])
    if path == "/repos/teknokomo/kiber-analyst/issues/34/timeline":
        return httpx.Response(200, json=[])
    if path == "/repos/teknokomo/kiber-analyst/pulls/34":
        return httpx.Response(200, json=PULL)
    if path == "/repos/teknokomo/kiber-analyst/pulls/34/reviews":
        return httpx.Response(200, json=[REVIEW])
    if path == "/repos/teknokomo/kiber-analyst/pulls/34/comments":
        return httpx.Response(200, json=[REVIEW_COMMENT])
    return httpx.Response(404)


def make_connector(monkeypatch, handler=github_handler) -> GitHubConnector:
    monkeypatch.setenv("KIBER_GITHUB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="github", api_url="https://api.github.com",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    return GitHubConnector(
        config=config,
        transport=httpx.MockTransport(handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )


def test_discover_excludes_pull_entries_from_issues_and_yields_pulls(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    refs = list(connector.discover(SOURCE, None))
    kinds = [(ref.kind, ref.number) for ref in refs]
    assert ("issue", 12) in kinds
    assert ("pull", 34) in kinds
    issue_refs = [ref for ref in refs if ref.kind == "issue"]
    assert [ref.number for ref in issue_refs] == [12]
    connector.close()


def test_issue_thread_normalizes_body_comments_and_timeline_cap(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    ref = connector.discover(SOURCE, None).__next__()
    assert ref.kind == "issue"
    raw = connector.fetch_thread(ref)
    normalized = connector.normalize(raw)
    thread = normalized.thread
    assert thread["thread_ref"] == f"{SOURCE}:issue:12"
    assert thread["state"] == "open"
    assert thread["author"] == "alice"
    assert thread["labels"] == ["api", "bug"]
    assert thread["meta"]["milestone"] == "0.2.0"
    assert thread["meta"]["assignees"] == ["bob"]
    assert len(thread["meta"]["events"]) == 200
    kinds = [item["item_kind"] for item in normalized.items]
    assert kinds.count("body") == 1 and kinds.count("comment") == 1
    body = normalized.items[0]
    assert body["external_ref"] == f"{SOURCE}:issue:12:body"
    assert body["content_hash"] == body["content_hash"]  # deterministic hash present
    comment = next(item for item in normalized.items if item["item_kind"] == "comment")
    assert comment["external_ref"] == f"{SOURCE}:issue:12:comment:345"
    assert comment["parent_external_ref"] is None
    assert comment["url"].endswith("#issuecomment-345")
    connector.close()


def test_pull_thread_normalizes_reviews_and_merged_state(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    refs = [ref for ref in connector.discover(SOURCE, None) if ref.kind == "pull"]
    raw = connector.fetch_thread(refs[0])
    normalized = connector.normalize(raw)
    assert normalized.thread["state"] == "merged"
    reviews = [item for item in normalized.items if item["item_kind"] == "review"]
    assert reviews[0]["external_ref"] == f"{SOURCE}:pull:34:review:900"
    assert reviews[0]["meta"]["review_state"] == "APPROVED"
    comments = [
        item for item in normalized.items if item["item_kind"] == "review_comment"
    ]
    assert comments[0]["external_ref"] == f"{SOURCE}:pull:34:review_comment:901"
    assert comments[0]["meta"]["path"] == "src/kiber_analyst/connectors/github.py"
    assert comments[0]["meta"]["line"] == 42
    assert comments[0]["meta"]["diff_hunk"].startswith("@@")
    connector.close()


def test_discussion_replies_keep_parent_links_and_answer_flag(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    discussion = {
        "number": 7,
        "title": "Design question",
        "state": "OPEN",
        "createdAt": "2026-11-05T09:00:00Z",
        "updatedAt": "2026-11-05T10:00:00Z",
        "url": "https://github.com/teknokomo/kiber-analyst/discussions/7",
        "author": {"login": "alice"},
        "body": "How should we model this?",
        "answer": {"id": "ans1", "bodyText": "Use documents + segments.",
                   "author": {"login": "bob"},
                   "createdAt": "2026-11-05T10:00:00Z", "url": "https://x"},
        "comments": {"nodes": [
            {"id": "c1", "bodyText": "First idea", "createdAt": "2026-11-05T09:30:00Z",
             "url": "https://u1", "author": {"login": "bob"},
             "replies": {"nodes": [
                 {"id": "ans1", "bodyText": "Use documents + segments.",
                  "createdAt": "2026-11-05T10:00:00Z", "url": "https://x",
                  "author": {"login": "bob"}},
             ]}},
        ]},
    }
    ref = ThreadRef(
        provider="github", source_id=SOURCE,
        thread_ref=f"{SOURCE}:discussion:7", kind="discussion", number=7,
    )
    items = connector._discussion_reply_items(discussion)
    raw = RawThread(ref=ref, body=discussion, items=items)
    normalized = connector.normalize(raw)
    replies = [item for item in normalized.items if item["item_kind"] == "reply"]
    top = next(item for item in replies if item["external_ref"].endswith(":reply:c1"))
    assert top["parent_external_ref"] == f"{SOURCE}:discussion:7:body"
    answer = next(item for item in replies if item["external_ref"].endswith(":reply:ans1"))
    assert answer["parent_external_ref"] == f"{SOURCE}:discussion:7:reply:c1"
    assert answer["meta"]["is_answer"] is True
    assert top["meta"]["is_answer"] is False
    connector.close()


def test_graphql_scope_failure_marks_discussions_unsupported(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/graphql":
            return httpx.Response(200, json={"errors": [
                {"type": "INSUFFICIENT_SCOPES", "message": "Your token has not been granted"}]})
        if request.url.path == "/repos/teknokomo/kiber-analyst/issues":
            return httpx.Response(200, json=[ISSUE])
        if request.url.path == "/repos/teknokomo/kiber-analyst/pulls":
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    connector = make_connector(monkeypatch, handler)
    refs = list(connector.discover(SOURCE, None))
    assert all(ref.kind != "discussion" for ref in refs)
    assert connector.discovery_errors
    assert connector.discovery_errors[0]["code"] == "unsupported"
    assert connector.discovery_errors[0]["status"] == "forbidden"
    connector.close()


def test_enterprise_host_uses_its_own_api_base(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "github.example.com"
        if request.url.path == "/api/graphql":
            return httpx.Response(200, json={"errors": [
                {"type": "INSUFFICIENT_SCOPES", "message": "no scope"}]})
        assert request.url.path.startswith("/api/v3/repos/team/repo")
        if request.url.path == "/api/v3/repos/team/repo/issues":
            return httpx.Response(200, json=[{**ISSUE, "number": 1}])
        if request.url.path == "/api/v3/repos/team/repo/pulls":
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    monkeypatch.setenv("KIBER_GITHUB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="github", api_url="https://api.github.com",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    connector = GitHubConnector(
        config=config,
        transport=httpx.MockTransport(handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )
    refs = list(connector.discover("github:github.example.com/team/repo", None))
    assert [ref.number for ref in refs] == [1]
    assert all(ref.kind != "discussion" for ref in refs)
    assert connector.discovery_errors
    connector.close()


def test_validate_source_rejects_other_providers(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    with pytest.raises(ConnectorError) as exc:
        connector.validate_source("gitlab:gitlab.com/g/p")
    assert exc.value.code == ConnectorCodes.SEARCH_INVALID_SOURCE
    with pytest.raises(ConnectorError):
        connector.validate_source("github:not-a-repo")
    connector.close()
