"""Broker tests for external source registration, ingest, embed, and status."""

from __future__ import annotations

import json
import uuid

import pytest

from kiber_analyst import broker

GITHUB_SOURCE = "github:owner/repo"
GITLAB_SOURCE = "gitlab:gitlab.com/group/project"
GITVERSE_SOURCE = "gitverse:owner/repo"
EXTERNAL_SOURCES = {
    GITHUB_SOURCE: {"project": "demo-github"},
    GITLAB_SOURCE: {"project": "demo-gitlab"},
    GITVERSE_SOURCE: {"project": "demo-gitverse"},
}


class RegistryStub:
    """Minimal stand-in for the sources.json path object."""

    def __init__(self, payload: str) -> None:
        self._payload = payload

    def lstat(self):
        import os
        import stat as stat_module

        info = os.lstat(__file__)
        # Fabricate a root-owned, non-writable, singly linked regular file.
        return type(
            "Stat",
            (),
            {
                "st_uid": 0,
                "st_mode": stat_module.S_IFREG | 0o644,
                "st_nlink": 1,
                "st_size": info.st_size,
            },
        )()

    def read_text(self) -> str:
        return self._payload


@pytest.fixture
def setup(tmp_path):
    (tmp_path / "broker").mkdir(parents=True)
    jobs = {}
    argv_log = []

    def submit(argv, *, idempotency_key):
        jid = str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))
        jobs.setdefault(jid, {"state": "queued"})
        argv_log.append(argv)
        return {"job_id": jid}

    def api(action, *, job_id):
        return jobs[job_id]

    instance = broker.Broker(tmp_path, dict(EXTERNAL_SOURCES), api, submit)
    yield instance, jobs, argv_log
    instance.db.close()


def test_registry_accepts_external_entries_and_rejects_extra_keys() -> None:
    good = json.dumps({GITHUB_SOURCE: {"project": "demo"}})
    parsed = broker.registry(RegistryStub(good))
    assert parsed[GITHUB_SOURCE]["project"] == "demo"
    bad = json.dumps({GITHUB_SOURCE: {"project": "demo", "hashes": {}}})
    with pytest.raises(broker.Refused, match="INVALID_REGISTRY"):
        broker.registry(RegistryStub(bad))
    bad_slug = json.dumps({GITHUB_SOURCE: {"project": "Not a Slug"}})
    with pytest.raises(broker.Refused, match="INVALID_REGISTRY"):
        broker.registry(RegistryStub(bad_slug))
    bad_source = json.dumps({"github:only-owner": {"project": "demo"}})
    with pytest.raises(broker.Refused, match="INVALID_REGISTRY"):
        broker.registry(RegistryStub(bad_source))


def test_github_ingest_builds_worker_argv_and_is_idempotent(setup) -> None:
    app, _jobs, argv_log = setup
    first = app.handle("client", "ingest", GITHUB_SOURCE)
    again = app.handle("client", "ingest", GITHUB_SOURCE)
    assert first == again
    assert argv_log[0] == ["github", "ingest", "--repo", "owner/repo", "--project", "demo-github"]


def test_gitlab_ingest_passes_host_and_project_slug(setup) -> None:
    app, _jobs, argv_log = setup
    app.handle("client", "ingest", GITLAB_SOURCE)
    assert argv_log[-1] == [
        "gitlab", "ingest", "--host", "gitlab.com",
        "--project", "group/project", "--project-slug", "demo-gitlab",
    ]


def test_gitverse_ingest_builds_worker_argv(setup) -> None:
    app, _jobs, argv_log = setup
    app.handle("client", "ingest", GITVERSE_SOURCE)
    assert argv_log[-1] == [
        "gitverse", "ingest", "--repo", "owner/repo", "--project", "demo-gitverse",
    ]


def test_external_embed_requires_done_ingest(setup) -> None:
    app, jobs, argv_log = setup
    with pytest.raises(broker.Refused, match="INGEST_REQUIRED"):
        app.handle("client", "embed", GITHUB_SOURCE)
    ingest = app.handle("client", "ingest", GITHUB_SOURCE)
    jobs[ingest["job_id"]].update(state="done")
    embedding = app.handle("client", "embed", GITHUB_SOURCE)
    assert embedding == app.handle("client", "embed", GITHUB_SOURCE)
    assert argv_log[-1] == [
        "external", "embed", "--source", GITHUB_SOURCE, "--project", "demo-github",
    ]


def test_source_status_uses_worker_overview_for_external(setup) -> None:
    app, jobs, _argv = setup

    def worker(action, **kwargs):
        if action == "source-overview":
            return {"threads": 3, "items": 12, "comments": 7, "reviews": 1,
                    "nonempty_items": 10, "embeddings": 8}
        job = jobs.get(kwargs.get("job_id"), {})
        if action == "status":
            return {"state": job.get("state", "queued"), "progress": {}}
        return job

    app.worker_api = worker
    ingest = app.handle("client", "ingest", GITHUB_SOURCE)
    jobs[ingest["job_id"]].update(state="done")
    result = app.handle("client", "source-status", GITHUB_SOURCE)
    assert result["provider"] == "github"
    assert result["counts"]["threads"] == 3
    assert result["counts"]["embeddings"] == 8
    with pytest.raises(broker.Refused, match="COMMAND_NOT_ALLOWED"):
        app.handle("client", "media-status", GITHUB_SOURCE)


def test_search_requires_ingested_external_source(setup, monkeypatch) -> None:
    app, jobs, argv_log = setup

    def worker(action, **kwargs):
        if action == "source-overview":
            return {"threads": 0}
        job = jobs.get(kwargs.get("job_id"), {})
        if action == "status":
            return {"state": job.get("state", "queued"), "progress": {}}
        return job

    app.worker_api = worker
    with pytest.raises(broker.Refused, match="SOURCE_NOT_FOUND"):
        app.handle("client", "search", GITHUB_SOURCE, "query")

    def worker_found(action, **kwargs):
        if action == "source-overview":
            return {"threads": 4}
        job = jobs.get(kwargs.get("job_id"), {})
        if action == "status":
            return {"state": job.get("state", "queued"), "progress": {}}
        return job

    app.worker_api = worker_found
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 1)
    search_request = app.handle("client", "search", GITHUB_SOURCE, "query", "3")
    state = search_request.get("status")
    assert state in {"queued", "running", "SEARCH_PENDING", None} or state is None
    assert argv_log[-1][:6] == [
        "search", "query", "--project", "demo-github", "--limit", "3",
    ]
    assert "--source-kind" in argv_log[-1] and "github" in argv_log[-1]
    assert "--source-id" in argv_log[-1] and GITHUB_SOURCE in argv_log[-1]


def test_unknown_external_source_is_not_allowed(setup) -> None:
    app, _jobs, _argv = setup
    with pytest.raises(broker.Refused, match="SOURCE_NOT_ALLOWED"):
        app.handle("client", "ingest", "github:other/repo")


def test_search_accepts_the_documented_limit_of_100(setup, monkeypatch) -> None:
    app, jobs, argv_log = setup

    def worker(action, **kwargs):
        if action == "source-overview":
            return {"threads": 1}
        job = jobs.get(kwargs.get("job_id"), {})
        if action == "status":
            return {"state": job.get("state", "queued"), "progress": {}}
        return job

    app.worker_api = worker
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 1)
    app.handle("client", "search", GITHUB_SOURCE, "query", "100")
    assert argv_log[-1][:6] == [
        "search", "query", "--project", "demo-github", "--limit", "100",
    ]
    with pytest.raises(broker.Refused, match="INVALID_LIMIT"):
        app.handle("client", "search", GITHUB_SOURCE, "query", "101")


def test_completed_ingest_does_not_suppress_a_new_provider_check(setup) -> None:
    app, jobs, argv_log = setup
    first = app.handle("client", "ingest", GITHUB_SOURCE)
    jobs[first["job_id"]].update(state="done")

    second = app.handle("client", "ingest", GITHUB_SOURCE)

    assert second["request_id"] != first["request_id"]
    assert second["job_id"] != first["job_id"]
    assert len(argv_log) == 2
    # A duplicate is still prevented while the request is active.
    active = app.handle("client", "ingest", GITHUB_SOURCE)
    assert active["request_id"] == second["request_id"]
    assert len(argv_log) == 2


def test_embed_after_completed_ingest_creates_a_new_run(setup) -> None:
    app, jobs, argv_log = setup
    ingest = app.handle("client", "ingest", GITHUB_SOURCE)
    jobs[ingest["job_id"]].update(state="done")
    first = app.handle("client", "embed", GITHUB_SOURCE)
    jobs[first["job_id"]].update(state="done")

    second = app.handle("client", "embed", GITHUB_SOURCE)

    assert second["request_id"] != first["request_id"]
    assert argv_log[-1] == [
        "external", "embed", "--source", GITHUB_SOURCE, "--project", "demo-github",
    ]
