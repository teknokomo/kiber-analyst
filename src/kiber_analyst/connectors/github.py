"""GitHub connector: Issues, Pull Requests, Reviews, and Discussions (ТЗ §6.1).

REST covers issues, comments, pulls, reviews, review comments, and timeline
events. GraphQL covers Discussions when the token has the required scope; when
it does not, Discussions are recorded as unsupported and Issues/PRs continue.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from kiber_analyst.connectors.base import (
    ConnectorHttpClient,
    NormalizedThread,
    RawItem,
    RawThread,
    ThreadRef,
    validate_connector_source,
)
from kiber_analyst.connectors.config import ProviderConfig, load_provider_config
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.normalize import (
    item_record,
    thread_record,
)
from kiber_analyst.connectors.source_ids import (
    item_ref as build_item_ref,
)
from kiber_analyst.connectors.source_ids import (
    normalize_external_id,
    parse_source_id,
)
from kiber_analyst.connectors.source_ids import (
    thread_ref as build_thread_ref,
)

DEFAULT_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
_MAX_TIMELINE_EVENTS = 200
_MAX_DIFF_HUNK_BYTES = 8 * 1024
_DISCUSSIONS_UNSUPPORTED_DETAIL = (
    "Discussions are unavailable for this token or repository"
)


class GitHubConnector:
    provider = "github"

    def __init__(
        self,
        *,
        raw_dir=None,
        config: ProviderConfig | None = None,
        transport: Any = None,
        sleep: Any = None,
        jitter: Any = None,
    ) -> None:
        self._config = config or load_provider_config("github")
        self._http = ConnectorHttpClient(
            self._config,
            transport=transport,
            sleep=sleep,
            jitter=jitter,
            raw_dir=raw_dir,
            default_headers=DEFAULT_HEADERS,
        )
        self.discovery_errors: list[dict[str, str]] = []

    # -- shared helpers ----------------------------------------------------

    def validate_source(self, source_id: str) -> None:
        validate_connector_source(source_id, self.provider)

    def _repo(self, source_id: str) -> tuple[str | None, str, str]:
        parsed = parse_source_id(source_id)
        if parsed is None:
            raise ConnectorError(
                ConnectorCodes.SEARCH_INVALID_SOURCE,
                "source_id is not a valid GitHub repository identifier",
            )
        owner, repo = parsed.path.split("/", 1)
        host = parsed.host
        api_base = f"https://{host}/api/v3" if host else self._config.api_url
        graphql_base = f"https://{host}/api/graphql" if host else _graphql_url(self._config.api_url)
        return host, api_base, graphql_base

    def _api(self, source_id: str) -> tuple[str, str]:
        _host, api_base, _graphql = self._repo(source_id)
        parsed = parse_source_id(source_id)
        assert parsed is not None
        return api_base, parsed.path

    def close(self) -> None:
        self._http.close()

    # -- discover ----------------------------------------------------------

    def discover(
        self, source_id: str, checkpoint: dict | None = None
    ) -> Iterator[ThreadRef]:
        self.validate_source(source_id)
        api_base, repo = self._api(source_id)
        params: dict[str, Any] = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "per_page": self._config.page_size,
        }
        since = _since_value(checkpoint)
        if since is not None:
            params["since"] = since
        for items, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/issues", params=params
        ):
            for payload in items:
                if isinstance(payload, dict) and "pull_request" not in payload:
                    yield self._list_ref(source_id, "issue", payload)
        pull_params: dict[str, Any] = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "per_page": self._config.page_size,
        }
        if since is not None:
            pull_params["since"] = since
        for items, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/pulls", params=pull_params
        ):
            for payload in items:
                yield self._list_ref(source_id, "pull", payload)
        yield from self._discover_discussions(source_id, checkpoint)

    def _list_ref(self, source_id: str, kind: str, payload: dict[str, Any]) -> ThreadRef:
        number = payload.get("number")
        if not isinstance(number, int) or number <= 0:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitHub listing entry is missing a valid number",
            )
        return ThreadRef(
            provider=self.provider,
            source_id=source_id,
            thread_ref=build_thread_ref(source_id, kind, number),
            kind=kind,
            number=number,
            updated_at=payload.get("updated_at"),
            state=payload.get("state"),
            title=payload.get("title") if isinstance(payload.get("title"), str) else None,
        )

    def _discover_discussions(
        self, source_id: str, checkpoint: dict | None
    ) -> Iterator[ThreadRef]:
        _host, _api_base, graphql_base = self._repo(source_id)
        parsed = parse_source_id(source_id)
        assert parsed is not None
        owner, name = parsed.path.split("/", 1)
        cursor = (checkpoint or {}).get("discussions_cursor")
        while True:
            try:
                payload = self._graphql(
                    graphql_base,
                    _DISCUSSIONS_LIST_QUERY,
                    {"owner": owner, "name": name, "cursor": cursor},
                )
            except ConnectorError as exc:
                # A missing or blocked Discussions surface must not break the
                # Issues/PR discovery; record it once and stop this walk.
                if exc.code in {
                    ConnectorCodes.SOURCE_NOT_FOUND,
                    ConnectorCodes.AUTH_FORBIDDEN,
                    ConnectorCodes.MALFORMED_RESPONSE,
                }:
                    if not self.discovery_errors:
                        self.discovery_errors.append(
                            {
                                "code": "unsupported",
                                "method": "POST /graphql discussions",
                                "endpoint": "graphql",
                                "status": exc.code,
                                "detail": _DISCUSSIONS_UNSUPPORTED_DETAIL,
                            }
                        )
                    return
                raise
            if payload is None:
                return
            repository = payload.get("data", {}).get("repository") or {}
            discussions = (repository.get("discussions") or {})
            nodes = discussions.get("nodes") or []
            for node in nodes:
                number = node.get("number")
                if not isinstance(number, int) or number <= 0:
                    raise ConnectorError(
                        ConnectorCodes.MALFORMED_RESPONSE,
                        "GitHub discussion entry is missing a valid number",
                    )
                yield ThreadRef(
                    provider=self.provider,
                    source_id=source_id,
                    thread_ref=build_thread_ref(source_id, "discussion", number),
                    kind="discussion",
                    number=number,
                    updated_at=node.get("updatedAt"),
                    state=node.get("state") if isinstance(node.get("state"), str) else None,
                    title=node.get("title") if isinstance(node.get("title"), str) else None,
                )
            page_info = discussions.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return
            cursor = page_info.get("endCursor")
            if not cursor:
                return

    def _graphql(
        self, url: str, query: str, variables: dict[str, Any]
    ) -> dict[str, Any] | None:
        payload, _not_modified = self._http.request_json(
            "POST",
            url,
            headers={"Accept": "application/vnd.github+json"},
            json_body={"query": query, "variables": variables},
        )
        if payload is None:
            return None
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            if _discussions_blocked(errors):
                if not self.discovery_errors:
                    self.discovery_errors.append(
                        {
                            "code": "unsupported",
                            "method": "POST /graphql discussions",
                            "endpoint": url,
                            "status": "forbidden",
                            "detail": _DISCUSSIONS_UNSUPPORTED_DETAIL,
                        }
                    )
                return None
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitHub GraphQL response reported errors",
            )
        self._http.dump_raw("graphql-discussions", payload)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitHub GraphQL response is missing data",
            )
        return payload

    # -- fetch -------------------------------------------------------------

    def fetch_thread(self, ref: ThreadRef) -> RawThread:
        if ref.kind == "discussion":
            body_payload = self._fetch_discussion(ref)
            items: list[RawItem] = [
                RawItem(item_kind="body", external_id=None, data=body_payload)
            ]
            items.extend(self._discussion_reply_items(body_payload))
            return RawThread(ref=ref, body=body_payload, items=items)
        api_base, repo = self._api(ref.source_id)
        body_payload = self._fetch_body(api_base, repo, ref)
        items = [RawItem(item_kind="body", external_id=None, data=body_payload)]
        raw = RawThread(ref=ref, body=body_payload, items=items)
        items.extend(self._fetch_issue_comments(api_base, repo, ref))
        if ref.kind == "pull":
            items.extend(self._fetch_reviews(api_base, repo, ref))
            items.extend(self._fetch_review_comments(api_base, repo, ref))
        items.extend(self._fetch_timeline(api_base, repo, ref, raw))
        return raw

    def fetch_items(self, ref: ThreadRef) -> Iterator[RawItem]:
        yield from self.fetch_thread(ref).items

    def _fetch_body(self, api_base: str, repo: str, ref: ThreadRef) -> dict[str, Any]:
        path = "pulls" if ref.kind == "pull" else "issues"
        payload, _modified = self._http.request_json(
            "GET", f"{api_base}/repos/{repo}/{path}/{ref.number}"
        )
        if not isinstance(payload, dict):
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitHub thread body is not an object",
            )
        return payload

    def _fetch_issue_comments(
        self, api_base: str, repo: str, ref: ThreadRef
    ) -> list[RawItem]:
        items: list[RawItem] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/issues/{ref.number}/comments",
            params={"per_page": self._config.page_size},
        ):
            for payload in page:
                items.append(
                    RawItem(
                        item_kind="comment",
                        external_id=normalize_external_id(payload.get("id")),
                        data=payload,
                        url=payload.get("html_url"),
                    )
                )
        return items

    def _fetch_reviews(self, api_base: str, repo: str, ref: ThreadRef) -> list[RawItem]:
        items: list[RawItem] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/pulls/{ref.number}/reviews",
            params={"per_page": self._config.page_size},
        ):
            for payload in page:
                items.append(
                    RawItem(
                        item_kind="review",
                        external_id=normalize_external_id(payload.get("id")),
                        data=payload,
                        url=payload.get("html_url"),
                    )
                )
        return items

    def _fetch_review_comments(
        self, api_base: str, repo: str, ref: ThreadRef
    ) -> list[RawItem]:
        items: list[RawItem] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/pulls/{ref.number}/comments",
            params={"per_page": self._config.page_size},
        ):
            for payload in page:
                items.append(
                    RawItem(
                        item_kind="review_comment",
                        external_id=normalize_external_id(payload.get("id")),
                        data=payload,
                        url=payload.get("html_url"),
                    )
                )
        return items

    def _fetch_timeline(
        self, api_base: str, repo: str, ref: ThreadRef, raw: RawThread
    ) -> list[RawItem]:
        # Timeline events land in documents.meta.events, capped at 200 latest.
        events: list[dict[str, Any]] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/repos/{repo}/issues/{ref.number}/timeline",
            params={"per_page": self._config.page_size},
        ):
            events.extend(item for item in page if isinstance(item, dict))
        raw.body = dict(raw.body or {})
        raw.body["kiber_timeline_events"] = events[-_MAX_TIMELINE_EVENTS:]
        return []

    def _fetch_discussion(self, ref: ThreadRef) -> dict[str, Any]:
        _host, _api_base, graphql_base = self._repo(ref.source_id)
        parsed = parse_source_id(ref.source_id)
        assert parsed is not None
        owner, name = parsed.path.split("/", 1)
        payload = self._graphql(
            graphql_base,
            _DISCUSSION_QUERY,
            {"owner": owner, "name": name, "number": ref.number},
        )
        if payload is None:
            raise ConnectorError(
                ConnectorCodes.THREAD_NOT_FOUND,
                "GitHub discussion content is unavailable for this token",
            )
        repository = payload.get("data", {}).get("repository") or {}
        discussion = repository.get("discussion")
        if not isinstance(discussion, dict):
            raise ConnectorError(
                ConnectorCodes.THREAD_NOT_FOUND,
                "GitHub discussion was not found",
            )
        return discussion

    def _discussion_reply_items(self, discussion: dict[str, Any]) -> list[RawItem]:
        # Flatten discussion comments and nested replies, preserving ancestry;
        # the accepted answer gets the is_answer flag on its item.
        answer_id = normalize_external_id((discussion.get("answer") or {}).get("id"))
        items: list[RawItem] = []
        for comment in (discussion.get("comments") or {}).get("nodes") or []:
            comment_id = normalize_external_id(comment.get("id"))
            items.append(
                RawItem(
                    item_kind="reply",
                    external_id=comment_id,
                    data={
                        "comment": comment,
                        "parent": None,
                        "is_answer": comment_id is not None and comment_id == answer_id,
                    },
                    url=comment.get("url"),
                )
            )
            for reply in (comment.get("replies") or {}).get("nodes") or []:
                reply_id = normalize_external_id(reply.get("id"))
                items.append(
                    RawItem(
                        item_kind="reply",
                        external_id=reply_id,
                        data={
                            "comment": reply,
                            "parent": comment_id,
                            "is_answer": reply_id is not None and reply_id == answer_id,
                        },
                        url=reply.get("url"),
                    )
                )
        return items

    # -- normalize ---------------------------------------------------------

    def normalize(self, raw: RawThread) -> NormalizedThread:
        body = raw.body or {}
        state = _github_state(body)
        labels = [
            label.get("name") for label in (body.get("labels") or []) if isinstance(label, dict)
        ]
        meta: dict[str, Any] = {
            "milestone": _milestone_name(body.get("milestone")),
            "assignees": _logins(body.get("assignees")),
        }
        events = body.get("kiber_timeline_events")
        if events is not None:
            meta["events"] = [
                _timeline_event(event) for event in events[-_MAX_TIMELINE_EVENTS:]
            ]
        thread = thread_record(
            thread_ref_value=raw.ref.thread_ref,
            title=body.get("title") or "",
            state=state,
            author=_login(body.get("user")),
            created_at=body.get("created_at"),
            updated_at=body.get("updated_at"),
            url=body.get("html_url"),
            labels=labels,
            meta=meta,
        )
        items: list[dict[str, Any]] = []
        body_text = body.get("body") or ""
        thread_body_ref = build_item_ref(raw.ref.thread_ref, "body", None)
        items.append(
            item_record(
                thread_ref_value=raw.ref.thread_ref,
                item_kind="body",
                external_id=None,
                author=_login(body.get("user")),
                text=body_text,
                posted_at=body.get("created_at"),
                url=body.get("html_url"),
                meta={},
            )
        )
        for raw_item in raw.items:
            if raw_item.item_kind == "body":
                continue
            payload = raw_item.data
            if raw_item.item_kind == "comment":
                items.append(
                    item_record(
                        thread_ref_value=raw.ref.thread_ref,
                        item_kind="comment",
                        external_id=raw_item.external_id,
                        author=_login(payload.get("user")),
                        text=payload.get("body") or "",
                        posted_at=payload.get("created_at"),
                        url=raw_item.url,
                        meta=_comment_meta(payload),
                    )
                )
            elif raw_item.item_kind == "review":
                items.append(
                    item_record(
                        thread_ref_value=raw.ref.thread_ref,
                        item_kind="review",
                        external_id=raw_item.external_id,
                        author=_login(payload.get("user")),
                        text=payload.get("body") or "",
                        posted_at=payload.get("submitted_at"),
                        url=raw_item.url,
                        meta={"review_state": payload.get("state")},
                    )
                )
            elif raw_item.item_kind == "review_comment":
                hunk = payload.get("diff_hunk")
                if isinstance(hunk, str) and len(hunk.encode("utf-8")) > _MAX_DIFF_HUNK_BYTES:
                    hunk = None
                items.append(
                    item_record(
                        thread_ref_value=raw.ref.thread_ref,
                        item_kind="review_comment",
                        external_id=raw_item.external_id,
                        author=_login(payload.get("user")),
                        text=payload.get("body") or "",
                        posted_at=payload.get("created_at"),
                        url=raw_item.url,
                        meta={
                            "path": payload.get("path"),
                            "line": payload.get("line"),
                            "side": payload.get("side"),
                            "commit_id": payload.get("commit_id"),
                            "diff_hunk": hunk,
                        },
                    )
                )
            elif raw_item.item_kind == "reply":
                comment = payload.get("comment") or {}
                parent = payload.get("parent")
                parent_ref = (
                    build_item_ref(raw.ref.thread_ref, "reply", str(parent))
                    if parent
                    else thread_body_ref
                )
                items.append(
                    item_record(
                        thread_ref_value=raw.ref.thread_ref,
                        item_kind="reply",
                        external_id=raw_item.external_id,
                        parent_external_ref=parent_ref,
                        author=_login(comment.get("author")),
                        text=comment.get("bodyText") or "",
                        posted_at=comment.get("createdAt"),
                        url=raw_item.url,
                        meta={"is_answer": bool(payload.get("is_answer"))},
                    )
                )
        return NormalizedThread(thread=thread, items=items)


_DISCUSSIONS_LIST_QUERY = """
query($owner:String!, $name:String!, $cursor:String) {
  repository(owner:$owner, name:$name) {
    discussions(first:50, after:$cursor, orderBy:{field:UPDATED_AT, direction:DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes { number title state updatedAt url }
    }
  }
}
"""

_DISCUSSION_QUERY = """
query($owner:String!, $name:String!, $number:Int!) {
  repository(owner:$owner, name:$name) {
    discussion(number:$number) {
      number title state createdAt updatedAt url answer {
        id bodyText author { login } createdAt url
      }
      author { login }
      comments(first:50) {
        nodes {
          id bodyText createdAt url author { login }
          replies(first:50) {
            nodes { id bodyText createdAt url author { login } }
          }
        }
      }
    }
  }
}
"""


def _graphql_url(api_url: str) -> str:
    if api_url.rstrip("/").endswith("/api/v3"):
        return api_url.rstrip("/")[: -len("/api/v3")] + "/api/graphql"
    return "https://api.github.com/graphql"


def _discussions_blocked(errors: list[Any]) -> bool:
    for error in errors:
        if not isinstance(error, dict):
            continue
        error_type = str(error.get("type") or "")
        message = str(error.get("message") or "")
        if error_type in {"FORBIDDEN", "INSUFFICIENT_SCOPES"}:
            return True
        if "discussions" in message.lower() and "not" in message.lower():
            return True
    return False


def _since_value(checkpoint: dict | None) -> str | None:
    if not checkpoint:
        return None
    value = checkpoint.get("since")
    if isinstance(value, str) and value:
        return value
    return None


def _login(value: Any) -> str | None:
    if isinstance(value, dict):
        login = value.get("login")
        if isinstance(login, str) and login:
            return login
    return None


def _logins(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted(login for login in (_login(item) for item in value) if login)


def _milestone_name(value: Any) -> str | None:
    if isinstance(value, dict):
        title = value.get("title")
        if isinstance(title, str):
            return title
    return None


def _github_state(body: dict[str, Any]) -> str | None:
    if body.get("merged") is True:
        return "merged"
    if body.get("locked") is True:
        return "locked"
    state = body.get("state")
    if not isinstance(state, str):
        return None
    return state.lower()


def _comment_meta(payload: dict[str, Any]) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if payload.get("created_at") != payload.get("updated_at"):
        meta["edited_at"] = payload.get("updated_at")
    return meta


def _timeline_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": event.get("event"),
        "actor": _login(event.get("actor")),
        "created_at": event.get("created_at"),
        "commit_id": event.get("commit_id"),
    }
