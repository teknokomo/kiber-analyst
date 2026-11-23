"""Regression: external ingest through the real queue and worker.

The queue (SQLite JobStore), the worker operation dispatch, job items, and the
connector pipeline are real; only the provider HTTP transport is mocked and the
PostgreSQL load is stubbed (the database path is covered by the gated
test_external_postgres.py suite).
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from kiber_analyst.connectors.config import ProviderConfig
from kiber_analyst.connectors.github import GitHubConnector
from kiber_analyst.jobqueue import JobStore
from kiber_analyst.worker import JobWorker

SOURCE = "github:owner/repo"


def _github_transport() -> httpx.MockTransport:
    issue = {
        "number": 1, "title": "First issue", "state": "open", "locked": False,
        "user": {"login": "alice"},
        "created_at": "2026-11-01T09:00:00Z", "updated_at": "2026-11-10T10:00:00Z",
        "html_url": "https://github.com/owner/repo/issues/1",
        "body": "Body of the issue", "labels": [], "milestone": None, "assignees": [],
    }
    comment = {
        "id": 101, "user": {"login": "bob"}, "body": "A comment",
        "created_at": "2026-11-10T12:00:00Z", "updated_at": "2026-11-10T12:00:00Z",
        "html_url": "https://github.com/owner/repo/issues/1#issuecomment-101",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/repos/owner/repo/issues":
            return httpx.Response(200, json=[issue])
        if path == "/repos/owner/repo/pulls":
            return httpx.Response(200, json=[])
        if path == "/repos/owner/repo/issues/1":
            return httpx.Response(200, json=issue)
        if path == "/repos/owner/repo/issues/1/comments":
            return httpx.Response(200, json=[comment])
        if path == "/repos/owner/repo/issues/1/timeline":
            return httpx.Response(200, json=[])
        if path == "/graphql":
            return httpx.Response(200, json={"data": {"repository": {"discussions": {
                "pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [],
            }}}})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def _make_worker(store: JobStore, monkeypatch: pytest.MonkeyPatch) -> JobWorker:
    import kiber_analyst.external_ingest as ingest_module

    monkeypatch.setenv("KIBER_GITHUB_TOKEN", "test-token")

    def connector_factory(provider: str, *, raw_dir=None) -> GitHubConnector:
        config = ProviderConfig(
            provider=provider, api_url="https://api.github.com",
            page_size=100, timeout_seconds=30, max_retries=1,
        )
        return GitHubConnector(
            config=config,
            transport=_github_transport(),
            sleep=lambda seconds: None,
            jitter=lambda seconds: seconds,
            raw_dir=raw_dir,
        )

    monkeypatch.setattr(ingest_module, "get_connector", connector_factory)
    monkeypatch.setattr(
        ingest_module,
        "load_external",
        lambda *args, **kwargs: {
            "status": "dry_run", "inserted": 2, "updated": 0, "skipped": 0,
            "deleted": 0,
        },
    )
    return JobWorker(store=store, pools={})


def test_external_ingest_reaches_connector_through_real_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = _make_worker(store, monkeypatch)

    submitted = worker.submit(
        {
            "argv": [
                "github", "ingest", "--repo", "owner/repo",
                "--project", "sample-project", "--since", "2026-11-01",
            ]
        },
        uid=0,
    )
    job = store.claim()
    assert job is not None and job.id == submitted["job_id"]

    worker.process(job)

    status = store.status(job.id)
    assert status["state"] == "done", status
    result = store.result(job.id)
    assert result["status"] == "done"
    assert result["threads"] == 1
    assert result["items"] == 2
    assert result["provider"] == "github"
    # The item registered by the job API is finalized by the worker; the old
    # source_id fingerprint mismatch failed before the connector was called.
    items = {entry["key"]: entry for entry in store.job_items(job.id)}
    assert items["external-source"]["state"] == "done"
    fetched = store.jobs_dir / job.id / "external-ingest" / "fetched.jsonl"
    assert fetched.is_file()
    payload = json.loads(fetched.read_text().splitlines()[0])
    assert payload["ref"]["thread_ref"] == f"{SOURCE}:issue:1"


def test_retryable_connector_exhaustion_is_reported_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kiber_analyst.external_ingest as ingest_module
    from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = _make_worker(store, monkeypatch)

    def failing_run(**_kwargs):
        raise ConnectorError(
            ConnectorCodes.RATE_LIMITED, "provider is throttling", status=429
        )

    monkeypatch.setattr(ingest_module, "run_external_ingest", failing_run)
    submitted = worker.submit(
        {"argv": ["github", "ingest", "--repo", "owner/repo", "--project", "demo"]},
        uid=0,
    )
    job = store.claim()
    assert job is not None and job.id == submitted["job_id"]
    worker.process(job)

    result = store.result(job.id)
    assert result["status"] == "failed"
    assert result["error"]["category"] == "rate_limited"
    assert result["error"]["code"] == ConnectorCodes.RATE_LIMITED
