"""Contract tests for the GitLab connector over a mock HTTP transport."""

from __future__ import annotations

import httpx
import pytest

from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.gitlab import GitLabConnector

SOURCE = "gitlab:gitlab.com/group/project"
API = "https://gitlab.com/api/v4/projects/group%2Fproject"
API_PATH = "/api/v4/projects/group%2Fproject"
ISSUE = {
    "iid": 12,
    "title": "Broken search",
    "state": "opened",
    "author": {"username": "alice"},
    "created_at": "2026-11-01T10:00:00Z",
    "updated_at": "2026-11-02T10:00:00Z",
    "web_url": "https://gitlab.com/group/project/-/issues/12",
    "description": "Search fails",
    "labels": ["bug", "api"],
    "milestone": {"title": "0.2.0"},
    "assignees": [{"username": "bob"}],
}
NOTE = {
    "id": 345,
    "system": False,
    "body": "Reproduced on main",
    "author": {"username": "bob"},
    "created_at": "2026-11-02T11:00:00Z",
}
SYSTEM_NOTE = {
    "id": 346,
    "system": True,
    "body": "changed the description",
    "author": {"username": "alice"},
    "created_at": "2026-11-02T11:30:00Z",
}
MERGE_REQUEST = {
    "iid": 34,
    "title": "Fix search",
    "state": "merged",
    "author": {"username": "alice"},
    "created_at": "2026-11-03T10:00:00Z",
    "updated_at": "2026-11-04T10:00:00Z",
    "web_url": "https://gitlab.com/group/project/-/merge_requests/34",
    "description": "Closes #12",
    "labels": ["backend"],
}
DISCUSSION_NOTE = {
    "id": 901,
    "system": False,
    "body": "Rename this method",
    "author": {"username": "bob"},
    "created_at": "2026-11-04T08:00:00Z",
    "resolvable": True,
    "resolved": False,
    "type": "DiffNote",
    "position": {
        "new_path": "src/connectors/gitlab.py",
        "new_line": 42,
        "head_sha": "abc123",
    },
}


def gitlab_handler(request: httpx.Request) -> httpx.Response:
    # httpx decodes the path; match on the raw path (query stripped) to
    # preserve the %2F escaping of the project id.
    path = request.url.raw_path.decode().split("?", 1)[0]
    if path == f"{API_PATH}/issues":
        return httpx.Response(200, json=[ISSUE], headers={"X-Next-Page": ""})
    if path == f"{API_PATH}/merge_requests":
        return httpx.Response(200, json=[MERGE_REQUEST], headers={"X-Next-Page": ""})
    if path == f"{API_PATH}/issues/12":
        return httpx.Response(200, json=ISSUE)
    if path == f"{API_PATH}/issues/12/notes":
        return httpx.Response(200, json=[SYSTEM_NOTE, NOTE], headers={"X-Next-Page": ""})
    if path == f"{API_PATH}/merge_requests/34":
        return httpx.Response(200, json=MERGE_REQUEST)
    if path == f"{API_PATH}/merge_requests/34/discussions":
        return httpx.Response(200, json=[
            {"id": "d1", "notes": [DISCUSSION_NOTE]},
            {"id": "d2", "notes": [{**SYSTEM_NOTE, "id": 902}]},
        ], headers={"X-Next-Page": ""})
    return httpx.Response(404)


def make_connector(monkeypatch) -> GitLabConnector:
    monkeypatch.setenv("KIBER_GITLAB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="gitlab", api_url="https://gitlab.com/api/v4",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    return GitLabConnector(
        config=config,
        transport=httpx.MockTransport(gitlab_handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )


def test_issue_notes_split_system_events_from_comments(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    refs = list(connector.discover(SOURCE, None))
    issue_ref = next(ref for ref in refs if ref.kind == "issue")
    raw = connector.fetch_thread(issue_ref)
    normalized = connector.normalize(raw)
    assert normalized.thread["state"] == "opened"
    assert normalized.thread["labels"] == ["api", "bug"]
    events = [item for item in normalized.items if item["item_kind"] == "event"]
    comments = [item for item in normalized.items if item["item_kind"] == "comment"]
    assert len(events) == 1 and len(comments) == 1
    assert events[0]["external_ref"] == f"{SOURCE}:issue:12:event:346"
    assert events[0]["text"] == ""
    assert events[0]["meta"]["system"] is True
    assert events[0]["meta"]["action"] == "changed the description"
    assert comments[0]["external_ref"] == f"{SOURCE}:issue:12:comment:345"
    assert comments[0]["url"] == "https://gitlab.com/group/project/-/issues/12#note_345"
    connector.close()


def test_merge_request_discussions_carry_position_and_resolution(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    refs = list(connector.discover(SOURCE, None))
    mr_ref = next(ref for ref in refs if ref.kind == "merge_request")
    raw = connector.fetch_thread(mr_ref)
    normalized = connector.normalize(raw)
    assert normalized.thread["state"] == "merged"
    review_comments = [
        item for item in normalized.items if item["item_kind"] == "review_comment"
    ]
    events = [item for item in normalized.items if item["item_kind"] == "event"]
    assert len(events) == 1 and events[0]["external_ref"].endswith(":event:902")
    assert len(review_comments) == 1
    comment = review_comments[0]
    assert comment["external_ref"] == f"{SOURCE}:merge_request:34:review_comment:901"
    assert comment["meta"]["discussion_id"] == "d1"
    assert comment["meta"]["position"]["file"] == "src/connectors/gitlab.py"
    assert comment["meta"]["position"]["line"] == 42
    assert comment["meta"]["position"]["commit"] == "abc123"
    assert comment["meta"]["resolvable"] is True
    assert comment["meta"]["resolved"] is False
    connector.close()


def test_project_path_is_url_encoded_for_the_api(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.setdefault("paths", []).append(
            request.url.raw_path.decode().split("?", 1)[0]
        )
        return httpx.Response(200, json=[])

    connector._http._transport = httpx.MockTransport(handler)
    connector._http._client = None
    list(connector.discover("gitlab:gitlab.com/group/sub/project", None))
    assert "/api/v4/projects/group%2Fsub%2Fproject/issues" in captured["paths"]
    assert "/api/v4/projects/group%2Fsub%2Fproject/merge_requests" in captured["paths"]
    connector.close()


def test_missing_host_in_source_id_is_invalid(monkeypatch) -> None:
    connector = make_connector(monkeypatch)
    with pytest.raises(ConnectorError) as exc:
        connector.validate_source("gitlab:group/project")
    assert exc.value.code == ConnectorCodes.SEARCH_INVALID_SOURCE
    connector.close()


def test_configured_api_path_is_honored_for_the_matching_host(monkeypatch) -> None:
    captured = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url.raw_path.decode().split("?", 1)[0])
        return httpx.Response(200, json=[])

    monkeypatch.setenv("KIBER_GITLAB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="gitlab", api_url="https://gitlab.com/custom/api/v4",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    connector = GitLabConnector(
        config=config,
        transport=httpx.MockTransport(handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )
    list(connector.discover(SOURCE, None))
    assert captured[0] == "/custom/api/v4/projects/group%2Fproject/issues"
    connector.close()


def test_unconfigured_self_hosted_host_is_rejected(monkeypatch) -> None:
    calls = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=[])

    monkeypatch.setenv("KIBER_GITLAB_TOKEN", "test-token")
    config = ProviderConfig(
        provider="gitlab", api_url="https://gitlab.com/api/v4",
        page_size=100, timeout_seconds=30, max_retries=2,
    )
    connector = GitLabConnector(
        config=config,
        transport=httpx.MockTransport(handler),
        sleep=lambda seconds: None,
        jitter=lambda seconds: seconds,
    )
    with pytest.raises(ConnectorError) as exc:
        list(connector.discover("gitlab:gitlab.evil.example/group/project", None))
    assert exc.value.code == ConnectorCodes.SOURCE_NOT_ALLOWED
    assert calls["n"] == 0
    connector.close()
