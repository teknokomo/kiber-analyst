"""End-to-end orchestration tests for external_ingest with a fake connector."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kiber_analyst.connectors.base import (
    NormalizedThread,
    RawItem,
    RawThread,
    ThreadRef,
)
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.external_ingest import run_external_ingest

SOURCE = "github:owner/repo"


class FakeConnector:
    provider = "github"

    def __init__(self, *, fail_thread: str | None = None) -> None:
        self.discovery_errors: list[dict[str, str]] = []
        self.fail_thread = fail_thread
        self.fetched: list[str] = []

    def validate_source(self, source_id: str) -> None:
        assert source_id == SOURCE

    def discover(self, source_id: str, checkpoint: dict | None):
        yield ThreadRef(
            provider="github", source_id=source_id,
            thread_ref=f"{source_id}:issue:1", kind="issue", number=1,
            updated_at="2026-11-10T10:00:00Z", state="open", title="First issue",
        )
        yield ThreadRef(
            provider="github", source_id=source_id,
            thread_ref=f"{source_id}:pull:2", kind="pull", number=2,
            updated_at="2026-11-11T10:00:00Z", state="closed", title="A pull",
        )

    def fetch_thread(self, ref: ThreadRef) -> RawThread:
        self.fetched.append(ref.thread_ref)
        if self.fail_thread == ref.thread_ref:
            raise ConnectorError(
                ConnectorCodes.THREAD_NOT_FOUND, "thread disappeared", status=404
            )
        body = {
            "number": ref.number,
            "title": ref.title,
            "state": ref.state,
            "user": {"login": "alice"},
            "created_at": "2026-11-10T09:00:00Z",
            "updated_at": ref.updated_at,
            "html_url": f"https://github.com/owner/repo/{ref.kind}/{ref.number}",
            "body": f"Body of {ref.kind} {ref.number}",
            "labels": [{"name": "z"}, {"name": "a"}],
            "merged": ref.kind == "pull",
        }
        comment = {
            "id": 100 + ref.number,
            "user": {"login": "bob"},
            "body": "A comment",
            "created_at": "2026-11-10T12:00:00Z",
            "updated_at": "2026-11-10T12:00:00Z",
            "html_url": "https://github.com/owner/repo/x#issuecomment-1",
        }
        return RawThread(
            ref=ref,
            body=body,
            items=[
                RawItem(item_kind="body", external_id=None, data=body),
                RawItem(item_kind="comment", external_id="101", data=comment,
                        url=comment["html_url"]),
            ],
            complete=True,
        )

    def fetch_items(self, ref: ThreadRef):
        yield from self.fetch_thread(ref).items

    def normalize(self, raw: RawThread) -> NormalizedThread:
        from kiber_analyst.connectors.normalize import item_record, thread_record

        body = raw.body or {}
        thread = thread_record(
            thread_ref_value=raw.ref.thread_ref,
            title=body.get("title") or "",
            state="merged" if body.get("merged") else body.get("state"),
            author=body.get("user", {}).get("login"),
            created_at=body.get("created_at"),
            updated_at=body.get("updated_at"),
            url=body.get("html_url"),
            labels=[label["name"] for label in body.get("labels", [])],
            meta={"milestone": None, "assignees": []},
        )
        items = [
            item_record(
                thread_ref_value=raw.ref.thread_ref,
                item_kind="body",
                external_id=None,
                author=body.get("user", {}).get("login"),
                text=body.get("body") or "",
                posted_at=body.get("created_at"),
                url=body.get("html_url"),
                meta={},
            )
        ]
        for raw_item in raw.items:
            if raw_item.item_kind != "comment":
                continue
            payload = raw_item.data
            items.append(
                item_record(
                    thread_ref_value=raw.ref.thread_ref,
                    item_kind="comment",
                    external_id=raw_item.external_id,
                    author=payload.get("user", {}).get("login"),
                    text=payload.get("body") or "",
                    posted_at=payload.get("created_at"),
                    url=raw_item.url,
                    meta={},
                )
            )
        return NormalizedThread(thread=thread, items=items)


@pytest.fixture
def artifact(tmp_path) -> Path:
    return tmp_path / "job" / "external-ingest"


def test_full_pipeline_writes_deterministic_bundle(
    artifact, monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    result = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        since="2026-11-01", artifact_dir=artifact, env_path=tmp_path / "env",
        connector=FakeConnector(),
    )
    assert result["status"] == "done"
    assert result["threads"] == 2
    assert result["items"] == 4
    assert result["nonempty"] == 4

    threads_file = artifact / "normalized" / "threads.jsonl"
    records = [json.loads(line) for line in threads_file.read_text().splitlines()]
    assert [record["thread_ref"] for record in records] == [
        f"{SOURCE}:issue:1", f"{SOURCE}:pull:2",
    ]
    manifest = json.loads(
        (artifact / "normalized" / "manifest.json").read_text()
    )
    assert manifest["source_id"] == SOURCE
    assert manifest["provider"] == "github"
    assert manifest["thread_count"] == 2
    assert manifest["item_count"] == 4
    assert manifest["nonempty_count"] == 4
    assert manifest["skipped_empty_count"] == 0
    assert manifest["threads_complete"][f"{SOURCE}:issue:1"] is True
    assert manifest["checkpoint_after"]["threads_fetched"] == 2
    assert manifest["api_version"] == "2022-11-28"
    # The 5-minute overlap window moves the watermark backwards.
    assert manifest["checkpoint_after"]["since"].startswith("2026-11-11T09:55:")

    first_threads = threads_file.read_bytes()
    first_items = (artifact / "normalized" / "items.jsonl").read_bytes()
    result2 = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        since="2026-11-01", artifact_dir=tmp_path / "second" / "external-ingest",
        env_path=tmp_path / "env", connector=FakeConnector(),
    )
    assert result2["threads"] == 2
    assert (
        tmp_path / "second" / "external-ingest" / "normalized" / "threads.jsonl"
    ).read_bytes() == first_threads
    assert (
        tmp_path / "second" / "external-ingest" / "normalized" / "items.jsonl"
    ).read_bytes() == first_items


def test_limit_bounds_discovery_and_is_recorded(artifact, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    result = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        limit=1, artifact_dir=artifact, env_path=tmp_path / "env",
        connector=FakeConnector(),
    )
    assert result["threads"] == 1
    manifest = json.loads((artifact / "normalized" / "manifest.json").read_text())
    assert manifest["limit_applied"] == 1
    assert manifest["checkpoint_after"]["watermark_suppressed"] == "limit"
    assert manifest["checkpoint_after"]["since"] == result["checkpoint_after"]["since"]


def test_failed_thread_fetch_records_partial(artifact, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    result = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        artifact_dir=artifact, env_path=tmp_path / "env",
        connector=FakeConnector(fail_thread=f"{SOURCE}:pull:2"),
    )
    assert result["status"] == "partial"
    assert result["threads"] == 1
    assert "thread_fetch_failed" in result["partial_reasons"]
    manifest = json.loads((artifact / "normalized" / "manifest.json").read_text())
    assert manifest["threads_complete"][f"{SOURCE}:pull:2"] is False
    # A failed thread must not be skipped by the next incremental run.
    assert (
        manifest["checkpoint_after"]["since"]
        == manifest["checkpoint_before"]["since"]
    )
    assert manifest["checkpoint_after"]["watermark_suppressed"] == "partial"


def test_invalid_provider_or_source_is_rejected(artifact, tmp_path) -> None:
    with pytest.raises(Exception, match="not supported"):
        run_external_ingest(
            provider="gitea", source_id=SOURCE, project_slug="demo",
            artifact_dir=artifact, env_path=tmp_path / "env",
        )
    with pytest.raises(Exception, match="not a valid"):
        run_external_ingest(
            provider="github", source_id="github:bad id", project_slug="demo",
            artifact_dir=artifact, env_path=tmp_path / "env",
        )


def test_bad_since_and_limit_are_rejected(artifact, tmp_path) -> None:
    with pytest.raises(Exception, match="ISO 8601"):
        run_external_ingest(
            provider="github", source_id=SOURCE, project_slug="demo",
            since="not-a-date", artifact_dir=artifact, env_path=tmp_path / "env",
        )
    with pytest.raises(Exception, match="--limit"):
        run_external_ingest(
            provider="github", source_id=SOURCE, project_slug="demo",
            limit=0, artifact_dir=artifact, env_path=tmp_path / "env",
        )


def test_naive_since_is_interpreted_as_utc(artifact, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    result = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        since="2026-01-01", artifact_dir=artifact, env_path=tmp_path / "env",
        connector=FakeConnector(),
    )
    assert result["checkpoint_after"]["since"].startswith("2026-11-11T09:55:")


def test_retryable_failure_keeps_fetched_threads_and_resumes(
    artifact, monkeypatch, tmp_path
) -> None:
    class RetryableConnector(FakeConnector):
        def fetch_thread(self, ref):
            if ref.thread_ref == f"{SOURCE}:pull:2":
                self.fetched.append(ref.thread_ref)
                raise ConnectorError(
                    ConnectorCodes.UPSTREAM_UNAVAILABLE, "provider down", status=503
                )
            return super().fetch_thread(ref)

    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    first = RetryableConnector()
    with pytest.raises(ConnectorError):
        run_external_ingest(
            provider="github", source_id=SOURCE, project_slug="demo",
            since="2026-11-01", artifact_dir=artifact, env_path=tmp_path / "env",
            connector=first,
        )
    fetched_file = artifact / "fetched.jsonl"
    assert fetched_file.is_file()
    lines = [line for line in fetched_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 1, "the successfully fetched thread must survive the retry"

    second = FakeConnector()
    result = run_external_ingest(
        provider="github", source_id=SOURCE, project_slug="demo",
        since="2026-11-01", artifact_dir=artifact, env_path=tmp_path / "env",
        connector=second,
    )
    assert result["threads"] == 2
    assert second.fetched == [f"{SOURCE}:pull:2"], "only the missing thread is refetched"


def test_stop_hook_interrupts_the_pipeline(artifact, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "kiber_analyst.external_ingest.load_external",
        lambda *args, **kwargs: {"status": "dry_run", "inserted": 0},
    )
    calls = {"n": 0}

    def stop() -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("deadline exceeded")

    with pytest.raises(RuntimeError, match="deadline exceeded"):
        run_external_ingest(
            provider="github", source_id=SOURCE, project_slug="demo",
            artifact_dir=artifact, env_path=tmp_path / "env",
            connector=FakeConnector(), check_stop=stop,
        )
    assert calls["n"] >= 2
