from __future__ import annotations

import json
from contextlib import nullcontext

import pytest

from kiber_analyst import project_catalog
from kiber_analyst.jobqueue import JobsError


def _policy(path, value, *, mode=0o640):
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(mode)
    return path


def test_project_policy_missing_fails_closed(tmp_path):
    assert project_catalog.read_project_policy(tmp_path / "missing.json") == (set(), False)


def test_project_policy_accepts_root_owned_allowlist_shape(tmp_path):
    path = _policy(tmp_path / "policy.json", {
        "version": 1,
        "authorized_projects": ["sample-project", "another-project"],
        "allow_project_create": True,
    })
    uid = path.stat().st_uid
    assert project_catalog.read_project_policy(path, root_uid=uid) == (
        {"sample-project", "another-project"}, True,
    )


def test_project_policy_rejects_wrong_owner(tmp_path):
    path = _policy(tmp_path / "policy.json", {
        "version": 1,
        "authorized_projects": ["sample-project"],
        "allow_project_create": False,
    })
    with pytest.raises(JobsError, match="Project access policy is unsafe"):
        project_catalog.read_project_policy(path, root_uid=path.stat().st_uid + 1)


def test_project_policy_rejects_duplicate_json_keys(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(
        '{"version":1,"version":1,"authorized_projects":[],"allow_project_create":false}',
        encoding="utf-8",
    )
    path.chmod(0o640)
    with pytest.raises(JobsError, match="Project access policy is invalid"):
        project_catalog.read_project_policy(path, root_uid=path.stat().st_uid)


@pytest.mark.parametrize("value", [
    {"version": 2, "authorized_projects": [], "allow_project_create": False},
    {"version": 1, "authorized_projects": ["Bad/slug"], "allow_project_create": False},
    {"version": 1, "authorized_projects": ["ok", "ok"], "allow_project_create": False},
    {"version": 1, "authorized_projects": [], "allow_project_create": 1},
    {"version": 1, "authorized_projects": [], "allow_project_create": False, "extra": 1},
])
def test_project_policy_rejects_invalid_schema(tmp_path, value):
    path = _policy(tmp_path / "policy.json", value)
    with pytest.raises(JobsError, match="Project access policy is invalid"):
        project_catalog.read_project_policy(path, root_uid=path.stat().st_uid)


def test_project_policy_rejects_symlink_and_writable_file(tmp_path):
    real = _policy(tmp_path / "real.json", {
        "version": 1, "authorized_projects": [], "allow_project_create": False,
    })
    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(JobsError, match="Project access policy is unavailable"):
        project_catalog.read_project_policy(link, root_uid=real.stat().st_uid)
    real.chmod(0o666)
    with pytest.raises(JobsError, match="Project access policy is unsafe"):
        project_catalog.read_project_policy(real, root_uid=real.stat().st_uid)


class _FakeConnection:
    def __init__(self, *, insert_row=None, existing_row=None):
        self.insert_row = insert_row
        self.existing_row = existing_row
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def transaction(self):
        return nullcontext()

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if sql.startswith("INSERT INTO projects"):
            return _Cursor(self.insert_row)
        if "SELECT slug, title," in sql:
            return _Cursor(self.existing_row)
        return _Cursor(None)


class _Cursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.row or []


def test_project_catalog_list_is_read_only_and_returns_only_slug_title(monkeypatch):
    connection = _FakeConnection()
    connection.execute = lambda sql, params=(): (
        connection.calls.append((sql, params)) or _Cursor([("sample-project", "Sample Project")])
    )
    monkeypatch.setattr(project_catalog, "_connect", lambda: connection)
    assert project_catalog.list_projects() == [{"slug": "sample-project", "title": "Sample Project"}]
    assert any("SET TRANSACTION READ ONLY" in sql for sql, _ in connection.calls)
    assert any("SELECT slug, title FROM projects" in sql for sql, _ in connection.calls)
    assert all("password" not in sql.lower() for sql, _ in connection.calls)


def test_project_create_uses_bound_parameters_and_is_idempotent(monkeypatch):
    key = project_catalog.request_key("operator", "new-project", "New project")
    connection = _FakeConnection(insert_row=("new-project", "New project"))
    monkeypatch.setattr(project_catalog, "_connect", lambda: connection)
    assert project_catalog.create_project("new-project", "New project", "operator", key) == {
        "slug": "new-project", "title": "New project", "created": True,
        "request_key": key,
    }
    insert_sql, params = connection.calls[1]
    assert "ON CONFLICT (slug) DO NOTHING" in insert_sql
    assert "'kiber_control_owner', %s::text" in insert_sql
    assert "'kiber_control_request_key', %s::text" in insert_sql
    assert params == ("new-project", "New project", "operator", key)
    assert "New project" not in insert_sql

    replay = _FakeConnection(
        existing_row=("new-project", "New project", "operator", key),
    )
    monkeypatch.setattr(project_catalog, "_connect", lambda: replay)
    assert project_catalog.create_project("new-project", "New project", "operator", key) == {
        "slug": "new-project", "title": "New project", "created": False,
        "request_key": key,
    }


def test_project_create_refuses_to_adopt_an_existing_project(monkeypatch):
    key = project_catalog.request_key("operator", "existing", "Existing")
    connection = _FakeConnection(existing_row=("existing", "Existing", None, None))
    monkeypatch.setattr(project_catalog, "_connect", lambda: connection)
    with pytest.raises(JobsError, match="PROJECT_ALREADY_EXISTS"):
        project_catalog.create_project("existing", "Existing", "operator", key)
