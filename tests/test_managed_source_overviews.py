from __future__ import annotations

from contextlib import nullcontext

from kiber_analyst import external_source_overview, load, source_overview


class _Connection:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def transaction(self):
        return nullcontext()

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if "SET TRANSACTION READ ONLY" in sql or "SET LOCAL statement_timeout" in sql:
            return self
        return _Result(self.row)


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


def test_external_overview_reports_remaining_nonempty_items_without_vector(monkeypatch):
    connection = _Connection((3, 12, 5, 2, 1, 1, 2, 2, 1, 9, 6, 3))
    monkeypatch.setattr(load, "_required_env", lambda _path: {})
    monkeypatch.setattr(load, "_connect", lambda _config: connection)

    result = external_source_overview.external_source_overview(
        "gitlab:gitlab.com/group/project", "sample-project",
    )

    assert result["nonempty_items"] == 9
    assert result["embeddings"] == 6
    assert result["nonempty_items_without_embeddings"] == 3
    query, params = connection.calls[-1]
    assert "NOT EXISTS" in query
    assert params == ("sample-project", "gitlab.com/group/project", "gitlab",
                     external_source_overview.MODEL_KEY,
                     external_source_overview.MODEL_KEY)


def test_telegram_overview_reports_remaining_nonempty_messages_without_vector(monkeypatch):
    connection = _Connection((1, 10, 10, 8, 2, 4, 6, 2))
    monkeypatch.setattr(load, "_required_env", lambda _path: {})
    monkeypatch.setattr(load, "_connect", lambda _config: connection)

    result = source_overview.source_overview(
        "telegram:-1009000:topic:1", "sample-project",
    )

    assert result["messages"] == 10
    assert result["embeddings"] == 6
    assert result["nonempty_messages_without_embeddings"] == 2
    query, params = connection.calls[-1]
    assert "NOT EXISTS" in query
    assert params[-2:] == (source_overview.MODEL_KEY, source_overview.MODEL_KEY)
