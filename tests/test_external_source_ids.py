from __future__ import annotations

import pytest

from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.source_ids import (
    is_external_source_id,
    item_ref,
    normalize_external_id,
    parse_source_id,
    parse_thread_ref,
    provider_for,
    thread_ref,
    validate_source_id,
)


@pytest.mark.parametrize(
    ("source_id", "expected"),
    [
        ("github:teknokomo/kiber-analyst", ("github", None, "teknokomo/kiber-analyst")),
        (
            "github:github.example.com/team/repo",
            ("github", "github.example.com", "team/repo"),
        ),
        ("gitlab:gitlab.com/group/project", ("gitlab", "gitlab.com", "group/project")),
        (
            "gitlab:gitlab.example.com/group/sub/project",
            ("gitlab", "gitlab.example.com", "group/sub/project"),
        ),
        ("gitverse:example-owner/example-repo", ("gitverse", None, "example-owner/example-repo")),
    ],
)
def test_source_id_formats_from_the_specification_parse(
    source_id: str, expected: tuple[str, str | None, str]
) -> None:
    parsed = parse_source_id(source_id)
    assert parsed is not None
    assert (parsed.provider, parsed.host, parsed.path) == expected
    assert parsed.external_id == source_id.split(":", 1)[1]
    assert is_external_source_id(source_id) is True


@pytest.mark.parametrize(
    "source_id",
    [
        "github:owner",
        "github:owner/a/b",
        "github:owner/",
        "github:/repo",
        "gitlab:group/project",
        "gitlab:gitlab.com/only-one-segment",
        "gitverse:owner",
        "gitverse:owner/a/b",
        "telegram:-1001234:topic:7",
        "gitea:owner/repo",
        "github:owner/repo:issue:1",
        "",
        "github:",
    ],
)
def test_invalid_source_ids_reject(source_id: str) -> None:
    assert parse_source_id(source_id) is None
    assert is_external_source_id(source_id) is False


def test_unknown_provider_has_no_source_prefix() -> None:
    assert provider_for("gitea:owner/repo") is None
    assert provider_for("github:owner/repo") == "github"
    with pytest.raises(ConnectorError) as exc:
        validate_source_id("gitea:owner/repo")
    assert exc.value.code == ConnectorCodes.UNSUPPORTED_PROVIDER


def test_malformed_source_id_uses_search_invalid_source() -> None:
    with pytest.raises(ConnectorError) as exc:
        validate_source_id("github:owner")
    assert exc.value.code == ConnectorCodes.SEARCH_INVALID_SOURCE


@pytest.mark.parametrize(
    ("source_id", "kind", "number", "expected"),
    [
        ("github:o/r", "issue", 12, "github:o/r:issue:12"),
        ("github:o/r", "pull", 34, "github:o/r:pull:34"),
        ("github:o/r", "discussion", 7, "github:o/r:discussion:7"),
        ("gitlab:gitlab.com/g/p", "issue", 3, "gitlab:gitlab.com/g/p:issue:3"),
        ("gitlab:gitlab.com/g/p", "merge_request", 9, "gitlab:gitlab.com/g/p:merge_request:9"),
        ("gitverse:o/r", "pull", 9, "gitverse:o/r:pull:9"),
    ],
)
def test_thread_refs_build_per_provider(
    source_id: str, kind: str, number: int, expected: str
) -> None:
    assert thread_ref(source_id, kind, number) == expected


def test_thread_kind_is_rejected_for_the_wrong_provider() -> None:
    with pytest.raises(ConnectorError):
        thread_ref("gitlab:gitlab.com/g/p", "discussion", 1)
    with pytest.raises(ConnectorError):
        thread_ref("github:o/r", "merge_request", 1)


def test_thread_ref_roundtrip() -> None:
    value = thread_ref("github:teknokomo/kiber-analyst", "issue", 12)
    parsed = parse_thread_ref(value)
    assert parsed is not None
    assert parsed[0].provider == "github" and parsed[1] == "issue" and parsed[2] == 12


@pytest.mark.parametrize(
    ("source_id", "kind", "expected"),
    [
        ("github:o/r", "merge_request", "invalid kind"),
        ("github:o/r", "issue", "number"),
    ],
)
def test_thread_ref_rejects_bad_input(source_id: str, kind: str, expected: str) -> None:
    with pytest.raises(ConnectorError):
        thread_ref(source_id, kind, 0 if expected == "number" else 1)


def test_item_ref_formats() -> None:
    base = "github:o/r:issue:12"
    assert item_ref(base, "body") == f"{base}:body"
    assert item_ref(base, "comment", "345") == f"{base}:comment:345"
    assert item_ref(base, "review", "9") == f"{base}:review:9"
    assert item_ref(base, "review_comment", "77") == f"{base}:review_comment:77"
    assert item_ref(base, "reply", "DIC_kwDOAbc123==") == f"{base}:reply:DIC_kwDOAbc123=="
    assert item_ref(base, "event", "5") == f"{base}:event:5"


def test_item_ref_rejects_unknown_kind_and_bad_ids() -> None:
    base = "github:o/r:issue:12"
    with pytest.raises(ConnectorError):
        item_ref(base, "note", "1")
    with pytest.raises(ConnectorError):
        item_ref(base, "comment", "not a safe id:with-colon")
    with pytest.raises(ConnectorError):
        item_ref(base, "comment", None)


def test_normalize_external_id() -> None:
    assert normalize_external_id(345) == "345"
    assert normalize_external_id("345") == "345"
    assert normalize_external_id("DIC_kwDOAbc123==") == "DIC_kwDOAbc123=="
    assert normalize_external_id(None) is None
    assert normalize_external_id(True) is None
    assert normalize_external_id(0) is None
    assert normalize_external_id("with:colon") is None
