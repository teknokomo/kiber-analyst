"""GitLab connector: Issues, notes, Merge Requests, and discussions (ТЗ §6.2).

System notes are stored as events with empty text, review positions travel in
meta.position, and pagination follows X-Next-Page. The host is always part of
the source_id, including gitlab.com.
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

DEFAULT_HEADERS = {"Accept": "application/json"}
_MAX_TIMELINE_EVENTS = 200


class GitLabConnector:
    provider = "gitlab"

    def __init__(
        self,
        *,
        raw_dir=None,
        config: ProviderConfig | None = None,
        transport: Any = None,
        sleep: Any = None,
        jitter: Any = None,
    ) -> None:
        self._config = config or load_provider_config("gitlab")
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

    def _target(self, source_id: str) -> tuple[str, str]:
        parsed = parse_source_id(source_id)
        if parsed is None or parsed.host is None:
            raise ConnectorError(
                ConnectorCodes.SEARCH_INVALID_SOURCE,
                "GitLab source_id must include the host",
            )
        api_base = f"https://{parsed.host}/api/v4"
        project_path = quote_reserved(parsed.path)
        return api_base, project_path

    # -- discover ----------------------------------------------------------

    def discover(
        self, source_id: str, checkpoint: dict | None = None
    ) -> Iterator[ThreadRef]:
        self.validate_source(source_id)
        api_base, project_path = self._target(source_id)
        base_params: dict[str, Any] = {
            "scope": "all",
            "state": "all",
            "order_by": "updated_at",
            "sort": "desc",
            "per_page": self._config.page_size,
        }
        since = (checkpoint or {}).get("since")
        if isinstance(since, str) and since:
            base_params["updated_after"] = since
        for items, _headers in self._http.iter_pages(
            f"{api_base}/projects/{project_path}/issues",
            params=dict(base_params),
            paging="header",
        ):
            for payload in items:
                yield self._list_ref(source_id, "issue", payload)
        for items, _headers in self._http.iter_pages(
            f"{api_base}/projects/{project_path}/merge_requests",
            params=dict(base_params),
            paging="header",
        ):
            for payload in items:
                yield self._list_ref(source_id, "merge_request", payload)

    def _list_ref(self, source_id: str, kind: str, payload: dict[str, Any]) -> ThreadRef:
        iid = payload.get("iid")
        if not isinstance(iid, int) or iid <= 0:
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitLab listing entry is missing a valid iid",
            )
        return ThreadRef(
            provider=self.provider,
            source_id=source_id,
            thread_ref=build_thread_ref(source_id, kind, iid),
            kind=kind,
            number=iid,
            updated_at=payload.get("updated_at"),
            state=payload.get("state"),
            title=payload.get("title") if isinstance(payload.get("title"), str) else None,
        )

    # -- fetch -------------------------------------------------------------

    def fetch_thread(self, ref: ThreadRef) -> RawThread:
        api_base, project_path = self._target(ref.source_id)
        body_path = "merge_requests" if ref.kind == "merge_request" else "issues"
        body_payload, _modified = self._http.request_json(
            "GET", f"{api_base}/projects/{project_path}/{body_path}/{ref.number}"
        )
        if not isinstance(body_payload, dict):
            raise ConnectorError(
                ConnectorCodes.MALFORMED_RESPONSE,
                "GitLab thread body is not an object",
            )
        items: list[RawItem] = [
            RawItem(item_kind="body", external_id=None, data=body_payload)
        ]
        raw = RawThread(ref=ref, body=body_payload, items=items)
        if ref.kind == "merge_request":
            # MR discussions already contain every note (comments, system notes,
            # diff notes) with thread structure; a separate /notes fetch would
            # duplicate them.
            items.extend(self._fetch_discussions(api_base, project_path, ref))
        else:
            items.extend(self._fetch_notes(api_base, project_path, ref))
        return raw

    def fetch_items(self, ref: ThreadRef) -> Iterator[RawItem]:
        yield from self.fetch_thread(ref).items

    def _fetch_notes(
        self,
        api_base: str,
        project_path: str,
        ref: ThreadRef,
    ) -> list[RawItem]:
        items: list[RawItem] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/projects/{project_path}/issues/{ref.number}/notes",
            params={
                "sort": "asc",
                "order_by": "created_at",
                "per_page": self._config.page_size,
            },
            paging="header",
        ):
            for payload in page:
                if isinstance(payload, dict) and payload.get("system") is True:
                    items.append(
                        RawItem(
                            item_kind="event",
                            external_id=normalize_external_id(payload.get("id")),
                            data=payload,
                            url=None,
                        )
                    )
                    continue
                items.append(
                    RawItem(
                        item_kind="comment",
                        external_id=normalize_external_id(payload.get("id")),
                        data=payload,
                        url=None,
                    )
                )
        return items

    def _fetch_discussions(
        self,
        api_base: str,
        project_path: str,
        ref: ThreadRef,
    ) -> list[RawItem]:
        # /discussions carries thread structure (discussion_id, resolvable,
        # positions) that plain notes lack; merge both into the item set.
        items: list[RawItem] = []
        for page, _headers in self._http.iter_pages(
            f"{api_base}/projects/{project_path}/merge_requests/{ref.number}/discussions",
            params={"per_page": self._config.page_size},
            paging="header",
        ):
            for discussion in page:
                if not isinstance(discussion, dict):
                    continue
                discussion_id = normalize_external_id(discussion.get("id"))
                for note in discussion.get("notes") or []:
                    if not isinstance(note, dict):
                        continue
                    if note.get("system") is True:
                        items.append(
                            RawItem(
                                item_kind="event",
                                external_id=normalize_external_id(note.get("id")),
                                data=note,
                                url=None,
                            )
                        )
                        continue
                    kind = "review_comment" if note.get("position") else "comment"
                    if discussion_id is not None:
                        note = dict(note)
                        note["kiber_discussion_id"] = discussion_id
                    items.append(
                        RawItem(
                            item_kind=kind,
                            external_id=normalize_external_id(note.get("id")),
                            data=note,
                            url=None,
                        )
                    )
        return items

    # -- normalize ---------------------------------------------------------

    def normalize(self, raw: RawThread) -> NormalizedThread:
        body = raw.body or {}
        labels = [
            label.get("title")
            for label in (body.get("labels") or [])
            if isinstance(label, dict) and isinstance(label.get("title"), str)
        ]
        if isinstance(body.get("labels"), list) and all(
            isinstance(label, str) for label in body["labels"]
        ):
            labels = list(body["labels"])
        meta: dict[str, Any] = {
            "milestone": _milestone_name(body.get("milestone")),
            "assignees": _usernames(body.get("assignees")),
        }
        thread = thread_record(
            thread_ref_value=raw.ref.thread_ref,
            title=body.get("title") or "",
            state=body.get("state"),
            author=_username(body.get("author")),
            created_at=body.get("created_at"),
            updated_at=body.get("updated_at"),
            url=_web_url(body.get("web_url")),
            labels=labels,
            meta=meta,
        )
        items: list[dict[str, Any]] = [
            item_record(
                thread_ref_value=raw.ref.thread_ref,
                item_kind="body",
                external_id=None,
                author=_username(body.get("author")),
                text=body.get("description") or "",
                posted_at=body.get("created_at"),
                url=_web_url(body.get("web_url")),
                meta={},
            )
        ]
        for raw_item in raw.items:
            if raw_item.item_kind == "body":
                continue
            payload = raw_item.data
            if raw_item.item_kind == "event":
                items.append(
                    item_record(
                        thread_ref_value=raw.ref.thread_ref,
                        item_kind="event",
                        external_id=raw_item.external_id,
                        author=_username(payload.get("author")),
                        text="",
                        posted_at=payload.get("created_at"),
                        url=None,
                        meta={"system": True, "action": payload.get("body")},
                    )
                )
                continue
            kind = raw_item.item_kind
            meta_note: dict[str, Any] = {}
            discussion_id = payload.get("kiber_discussion_id")
            if discussion_id is not None:
                meta_note["discussion_id"] = discussion_id
            if payload.get("resolvable") is not None:
                meta_note["resolvable"] = payload.get("resolvable")
            if payload.get("resolved") is not None:
                meta_note["resolved"] = payload.get("resolved")
            if kind == "review_comment":
                meta_note["position"] = _position(payload.get("position"))
            items.append(
                item_record(
                    thread_ref_value=raw.ref.thread_ref,
                    item_kind=kind,
                    external_id=raw_item.external_id,
                    author=_username(payload.get("author")),
                    text=payload.get("body") or "",
                    posted_at=payload.get("created_at"),
                    url=_note_url(body.get("web_url"), raw.ref.number, payload.get("id")),
                    meta=meta_note,
                )
            )
        return NormalizedThread(thread=thread, items=items)


def quote_reserved(path: str) -> str:
    """URL-encode the '/' separators of a project path (GitLab API requirement)."""
    return path.replace("/", "%2F")


def _username(value: Any) -> str | None:
    if isinstance(value, dict):
        username = value.get("username")
        if isinstance(username, str) and username:
            return username
    return None


def _usernames(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted(name for name in (_username(item) for item in value) if name)


def _milestone_name(value: Any) -> str | None:
    if isinstance(value, dict):
        title = value.get("title")
        if isinstance(title, str):
            return title
    return None


def _web_url(value: Any) -> str | None:
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        return value
    return None


def _note_url(web_url: Any, number: int, note_id: Any) -> str | None:
    # Documented GitLab anchor template; NULL when the base URL is unknown.
    if not isinstance(web_url, str) or not web_url.startswith(("http://", "https://")):
        return None
    note_ref = normalize_external_id(note_id)
    if note_ref is None:
        return None
    return f"{web_url}#note_{note_ref}"


def _position(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "file": value.get("new_path") or value.get("old_path"),
        "line": value.get("new_line") if value.get("new_line") is not None else value.get("old_line"),
        "commit": value.get("head_sha"),
    }
