"""GitVerse connector: Issues, comments, Pull Requests, reviews (ТЗ §6.3).

The public API is Gitea/Forgejo-shaped. Endpoint support is conditional: when
an endpoint answers 404/501 the fact is recorded as unsupported with the exact
method and status, no HTML scraping happens, and the remaining discussion types
continue. URLs come from the API (html_url); there is no self-built template.
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
from kiber_analyst.connectors.normalize import item_record, thread_record
from kiber_analyst.connectors.source_ids import (
    normalize_external_id,
    parse_source_id,
)
from kiber_analyst.connectors.source_ids import (
    thread_ref as build_thread_ref,
)

DEFAULT_HEADERS = {"Accept": "application/vnd.gitverse.object+json;version=1"}
_MAX_TIMELINE_EVENTS = 200
_UNSUPPORTED_STATUSES = frozenset({404, 501})


class GitVerseConnector:
    provider = "gitverse"

    def __init__(
        self,
        *,
        raw_dir=None,
        config: ProviderConfig | None = None,
        transport: Any = None,
        sleep: Any = None,
        jitter: Any = None,
    ) -> None:
        self._config = config or load_provider_config("gitverse")
        self._http = ConnectorHttpClient(
            self._config,
            transport=transport,
            sleep=sleep,
            jitter=jitter,
            raw_dir=raw_dir,
            default_headers=DEFAULT_HEADERS,
        )
        self.discovery_errors: list[dict[str, str]] = []

    def close(self) -> None:
        self._http.close()

    # -- shared helpers ----------------------------------------------------

    def validate_source(self, source_id: str) -> None:
        validate_connector_source(source_id, self.provider)

    def _repo_path(self, source_id: str) -> str:
        parsed = parse_source_id(source_id)
        if parsed is None:
            raise ConnectorError(
                ConnectorCodes.SEARCH_INVALID_SOURCE,
                "source_id is not a valid GitVerse repository identifier",
            )
        return parsed.path

    # -- discover ----------------------------------------------------------

    def discover(
        self, source_id: str, checkpoint: dict | None = None
    ) -> Iterator[ThreadRef]:
        self.validate_source(source_id)
        repo = self._repo_path(source_id)
        params: dict[str, Any] = {
            "state": "all",
            "type": "issues",
            "sort": "updated",
            "per_page": self._config.page_size,
        }
        since = (checkpoint or {}).get("since")
        if isinstance(since, str) and since:
            params["since"] = since
        for items, _headers in self._http.iter_pages(
            f"/repos/{repo}/issues", params=params
        ):
            for payload in items:
                if isinstance(payload, dict) and "pull_request" not in payload:
                    yield self._list_ref(source_id, "issue", payload)
        pull_params: dict[str, Any] = {"state": "all", "per_page": self._config.page_size}
        for items, _headers in self._http.iter_pages(
            f"/repos/{repo}/pulls", params=pull_params
        ):
            for payload in items:
                yield self._list_ref(source_id, "pull", payload)

    def _list_ref(self, source_id: str, kind: str, payload: dict[str, Any]) -> ThreadRef:
        number = payload.get("number")
        if not isinstance(number, int) or number <= 0:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitVerse listing entry is missing a valid number",
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

    # -- fetch -------------------------------------------------------------

    def fetch_thread(self, ref: ThreadRef) -> RawThread:
        repo = self._repo_path(ref.source_id)
        body_path = "pulls" if ref.kind == "pull" else "issues"
        body_payload, _modified = self._http.request_json(
            "GET", f"/repos/{repo}/{body_path}/{ref.number}"
        )
        if not isinstance(body_payload, dict):
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitVerse thread body is not an object",
            )
        items: list[RawItem] = [
            RawItem(item_kind="body", external_id=None, data=body_payload)
        ]
        raw = RawThread(ref=ref, body=body_payload, items=items)
        items.extend(self._fetch_comments(repo, ref, raw))
        if ref.kind == "pull":
            items.extend(self._fetch_reviews(repo, ref, raw))
            items.extend(self._fetch_review_comments(repo, ref, raw))
        items.extend(self._fetch_timeline(repo, ref, raw))
        return raw

    def fetch_items(self, ref: ThreadRef) -> Iterator[RawItem]:
        yield from self.fetch_thread(ref).items

    def _fetch_comments(self, repo: str, ref: ThreadRef, raw: RawThread) -> list[RawItem]:
        items: list[RawItem] = []
        endpoint = f"/repos/{repo}/issues/{ref.number}/comments"
        try:
            pages = list(
                self._http.iter_pages(endpoint, params={"per_page": self._config.page_size})
            )
        except ConnectorError as exc:
            if _unsupported(exc):
                raw.add_unsupported("GET", endpoint, "404")
                return items
            raise
        for page, _headers in pages:
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

    def _fetch_reviews(self, repo: str, ref: ThreadRef, raw: RawThread) -> list[RawItem]:
        items: list[RawItem] = []
        try:
            pages = list(
                self._http.iter_pages(
                    f"/repos/{repo}/pulls/{ref.number}/reviews",
                    params={"per_page": self._config.page_size},
                )
            )
        except ConnectorError as exc:
            if _unsupported(exc):
                raw.add_unsupported("GET", f"/repos/{repo}/pulls/{ref.number}/reviews", "404")
                return items
            raise
        for page, _headers in pages:
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
        self, repo: str, ref: ThreadRef, raw: RawThread
    ) -> list[RawItem]:
        items: list[RawItem] = []
        endpoint = f"/repos/{repo}/pulls/{ref.number}/comments"
        try:
            pages = list(
                self._http.iter_pages(endpoint, params={"per_page": self._config.page_size})
            )
        except ConnectorError as exc:
            if _unsupported(exc):
                raw.add_unsupported("GET", endpoint, "404")
                return items
            raise
        for page, _headers in pages:
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

    def _fetch_timeline(self, repo: str, ref: ThreadRef, raw: RawThread) -> list[RawItem]:
        endpoint = f"/repos/{repo}/issues/{ref.number}/timeline"
        events: list[dict[str, Any]] = []
        try:
            pages = list(
                self._http.iter_pages(endpoint, params={"per_page": self._config.page_size})
            )
        except ConnectorError as exc:
            if _unsupported(exc):
                raw.add_unsupported("GET", endpoint, "404")
                return []
            raise
        for page, _headers in pages:
            events.extend(item for item in page if isinstance(item, dict))
        raw.body = dict(raw.body or {})
        raw.body["kiber_timeline_events"] = events[-_MAX_TIMELINE_EVENTS:]
        return []

    # -- normalize ---------------------------------------------------------

    def normalize(self, raw: RawThread) -> NormalizedThread:
        body = raw.body or {}
        labels = [
            label.get("name") for label in (body.get("labels") or []) if isinstance(label, dict)
        ]
        meta: dict[str, Any] = {}
        events = body.get("kiber_timeline_events")
        if events is not None:
            meta["events"] = [
                {
                    "type": event.get("event") or event.get("type"),
                    "actor": _login(event.get("user") or event.get("actor")),
                    "created_at": event.get("created_at"),
                }
                for event in events[-_MAX_TIMELINE_EVENTS:]
            ]
        thread = thread_record(
            thread_ref_value=raw.ref.thread_ref,
            title=body.get("title") or "",
            state=_gitverse_state(body),
            author=_login(body.get("user")),
            created_at=body.get("created_at"),
            updated_at=body.get("updated_at"),
            url=body.get("html_url"),
            labels=labels,
            meta=meta,
        )
        items: list[dict[str, Any]] = [
            item_record(
                thread_ref_value=raw.ref.thread_ref,
                item_kind="body",
                external_id=None,
                author=_login(body.get("user")),
                text=body.get("body") or "",
                posted_at=body.get("created_at"),
                url=body.get("html_url"),
                meta={},
            )
        ]
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
                        meta={},
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
                        posted_at=payload.get("submitted_at") or payload.get("created_at"),
                        url=raw_item.url,
                        meta={"review_state": payload.get("state")},
                    )
                )
            elif raw_item.item_kind == "review_comment":
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
                            "line": payload.get("line") or payload.get("new_line"),
                            "commit_id": payload.get("commit_id"),
                        },
                    )
                )
        return NormalizedThread(thread=thread, items=items)


def _unsupported(exc: ConnectorError) -> bool:
    # Conditional endpoint support: an explicit upstream 404/501 on a
    # sub-endpoint is recorded as unsupported, never substituted or fatal.
    return exc.status in _UNSUPPORTED_STATUSES


def _login(value: Any) -> str | None:
    if isinstance(value, dict):
        login = value.get("login") or value.get("username")
        if isinstance(login, str) and login:
            return login
    return None


def _gitverse_state(body: dict[str, Any]) -> str | None:
    if body.get("merged") is True:
        return "merged"
    state = body.get("state")
    return state if isinstance(state, str) else None
