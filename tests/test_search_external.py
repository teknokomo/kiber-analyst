"""Search validation for external source kinds and ids (ТЗ §9.2)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kiber_analyst.errors import SearchError
from kiber_analyst.search_core import execute_search

VECTOR = [0.1] * 1536
CREATED = datetime(2026, 11, 10, 10, 0, tzinfo=UTC)


class FakeCursor:
    def __init__(self, rows_by_marker: dict[str, list[tuple]]) -> None:
        self.rows_by_marker = rows_by_marker
        self.statements: list[tuple[str, object]] = []
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.statements.append((query, params))
        for marker, rows in self.rows_by_marker.items():
            if marker in query:
                self.rows = rows
                return
        self.rows = []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows_by_marker: dict[str, list[tuple]]) -> None:
        self._cursor = FakeCursor(rows_by_marker)

    def cursor(self):
        return self._cursor


def _external_rows() -> list[tuple]:
    return [
        (
            "segment", 21, None, "00000000-0000-0000-0000-0000000000aa", None,
            None, None, 0.04, "connector text", "https://github.com/o/r/issues/1",
            CREATED, "alice", "github", "demo",
            "github:o/r", "o/r", "o/r", None, "First issue", "1",
            "github:o/r:issue:1", "github:o/r:issue:1:comment:345", None,
            "comment", "open", ["bug"],
        ),
    ]


def test_external_source_ids_validate_and_pass_existence_check() -> None:
    conn = FakeConn(
        {
            "SELECT id FROM projects": [("pid",)],
            "SELECT 1 FROM sources": [(1,)],
            "WITH query_args": _external_rows(),
        }
    )
    result = execute_search(
        conn, "connector", project="demo", vector=VECTOR,
        source_kind="github", source_id="github:o/r", limit=5,
    )
    assert result["results"][0]["thread_ref"] == "github:o/r:issue:1"
    assert result["results"][0]["item_ref"] == "github:o/r:issue:1:comment:345"
    assert result["results"][0]["item_kind"] == "comment"
    assert result["results"][0]["provider"] == "github"
    assert result["results"][0]["state"] == "open"
    assert result["results"][0]["labels"] == ["bug"]
    existence = [
        (query, params)
        for query, params in conn._cursor.statements
        if "SELECT 1 FROM sources" in query
    ]
    assert existence, "external existence pre-check must run"
    assert existence[0][1] == ("demo", "github", "o/r")


@pytest.mark.parametrize(
    "source_id",
    ["github:owner", "gitlab:group/project", "gitverse:owner/a/b", "github:o/r:issue:1"],
)
def test_malformed_external_source_ids_are_rejected(source_id: str) -> None:
    conn = FakeConn({"SELECT id FROM projects": [("pid",)]})
    with pytest.raises(SearchError, match="Source ID is invalid"):
        execute_search(conn, "q", project="demo", vector=None, source_id=source_id)


@pytest.mark.parametrize("kind", ["github", "gitlab", "gitverse"])
def test_external_source_kinds_are_accepted(kind: str) -> None:
    conn = FakeConn(
        {
            "SELECT id FROM projects": [("pid",)],
            "SELECT 1 FROM sources": [(1,)],
            "WITH query_args": [],
        }
    )
    result = execute_search(
        conn, "q", project="demo", vector=None, source_kind=kind, limit=5,
    )
    assert result["results"] == []


def test_unknown_source_kind_is_still_rejected() -> None:
    conn = FakeConn({})
    with pytest.raises(SearchError, match="Source kind is invalid"):
        execute_search(conn, "q", project="demo", vector=None, source_kind="gitea")


def test_missing_external_source_is_reported() -> None:
    conn = FakeConn(
        {
            "SELECT id FROM projects": [("pid",)],
            "SELECT 1 FROM sources": [],
        }
    )
    with pytest.raises(SearchError, match="External source does not exist"):
        execute_search(
            conn, "q", project="demo", vector=None, source_id="gitverse:o/r",
        )


def test_external_source_ids_up_to_200_characters_are_accepted() -> None:
    long_path = "a" * 120 + "/project"
    source_id = f"gitlab:gitlab.com/{long_path}"
    assert 128 < len(source_id) <= 200
    conn = FakeConn(
        {
            "SELECT id FROM projects": [("pid",)],
            "SELECT 1 FROM sources": [(1,)],
            "WITH query_args": [],
        }
    )
    result = execute_search(
        conn, "q", project="demo", vector=None, source_id=source_id, limit=5,
    )
    assert result["results"] == []
    existence = [
        params for query, params in conn._cursor.statements
        if "SELECT 1 FROM sources" in query
    ]
    assert existence[0][2] == f"gitlab.com/{long_path}"
