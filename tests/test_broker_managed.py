from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest

from kiber_analyst import broker
from kiber_analyst.jobqueue import JobsError

TELEGRAM_SOURCE = "telegram:-1009000:topic:1"
GITHUB_SOURCE = "github:example-org/example-repo"
PROJECT = "sample-project"


def _telegram_export(root: Path, *, text="prepared message") -> Path:
    export_dir = root / "imports" / "-1009000" / "1"
    export_dir.mkdir(mode=0o700, parents=True)
    message_bytes = (json.dumps({
        "chat_id": -1009000,
        "message_id": 1,
        "topic_id": 1,
        "date": "2026-01-01T00:00:00Z",
        "text": text,
        "sender_id": 12,
        "sender_name": "Fixture",
    }, separators=(",", ":")) + "\n").encode()
    service_bytes = b""
    message_hash = hashlib.sha256(message_bytes).hexdigest()
    service_hash = hashlib.sha256(service_bytes).hexdigest()
    source_id = TELEGRAM_SOURCE
    manifest_bytes = (json.dumps({
        "export_version": 1,
        "source_id": source_id,
        "chat_id": -1009000,
        "topic_id": 1,
        "topic_title": "News",
        "chat_title": "Fixture group",
        "complete": True,
        "scope_before": None,
        "message_count": 1,
        "messages_sha256": message_hash,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
    services_manifest = (json.dumps({
        "source_id": source_id,
        "chat_id": -1009000,
        "topic_id": 1,
        "complete": True,
        "range_complete": True,
        "scope_before": None,
        "errors": [],
        "event_count": 0,
        "services_sha256": service_hash,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
    for name, payload in {
        "messages.jsonl": message_bytes,
        "services.jsonl": service_bytes,
        "manifest.json": manifest_bytes,
        "services_manifest.json": services_manifest,
    }.items():
        file_path = export_dir / name
        file_path.write_bytes(payload)
        file_path.chmod(0o600)
    for directory in (root, root / "imports", root / "imports" / "-1009000", export_dir):
        directory.chmod(0o700)
    return export_dir


@pytest.fixture
def control(tmp_path, monkeypatch):
    monkeypatch.setattr(broker, "RESERVE", 0)
    (tmp_path / "broker").mkdir(mode=0o700)
    (tmp_path / "imports" / ".broker").mkdir(mode=0o700, parents=True)
    projects = {PROJECT: {"slug": PROJECT, "title": "Sample Project"}}
    jobs = {}
    submitted = []
    api_calls = []

    def worker_api(action, **kwargs):
        api_calls.append((action, kwargs))
        if action == "project-catalog":
            if kwargs.get("mode") == "list":
                return {"projects": list(projects.values())}
            if kwargs.get("mode") == "create":
                slug = kwargs["slug"]
                if slug in projects:
                    raise JobsError("PROJECT_ALREADY_EXISTS")
                projects[slug] = {"slug": slug, "title": kwargs["title"]}
                return {"slug": slug, "title": kwargs["title"], "created": True,
                        "request_key": kwargs["request_key"]}
        if action == "source-overview":
            if broker.parse_source_id(kwargs["source_id"]):
                return {"threads": 2, "items": 7, "nonempty_items": 5,
                        "embeddings": 3, "nonempty_items_without_embeddings": 2}
            return {"documents": 1, "messages": 4, "unique_messages": 4,
                    "nonempty_messages": 3, "messages_with_media": 1,
                    "service_events": 2, "embeddings": 2,
                    "nonempty_messages_without_embeddings": 1}
        if action == "status":
            return jobs[kwargs["job_id"]]
        if action == "result":
            job = jobs[kwargs["job_id"]]
            return job.get("result", {"status": job.get("state"), "counts": {}})
        if action == "cancel":
            job = jobs[kwargs["job_id"]]
            job["state"] = "cancelled"
            return {"state": "cancelled", "cancel_requested": True}
        if action == "cancel-idempotency":
            job_id = str(uuid.uuid5(uuid.NAMESPACE_OID, kwargs["idempotency_key"]))
            job = jobs.get(job_id)
            if job is None:
                return {"found": False, "job_id": None, "state": "cancelled",
                        "cancel_requested": False}
            job["state"] = "cancelled"
            return {"found": True, "job_id": job_id, "state": "cancelled",
                    "cancel_requested": False}
        raise AssertionError(f"unexpected worker operation: {action}")

    def submit(argv, *, idempotency_key):
        job_id = str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))
        jobs.setdefault(job_id, {"state": "queued", "progress": {}})
        submitted.append((job_id, list(argv), idempotency_key))
        return {"job_id": job_id}

    app = broker.Broker(
        tmp_path, {}, worker_api, submit,
        authorized_projects={PROJECT}, allow_project_create=True,
    )
    yield app, projects, jobs, submitted, api_calls
    app.db.close()


def test_project_list_is_filtered_and_create_is_idempotent(control):
    app, projects, _jobs, _submitted, api_calls = control
    projects["foreign"] = {"slug": "foreign", "title": "Foreign"}
    listed = app.handle("client", "project-list")
    assert listed == {"projects": [{"slug": PROJECT, "title": "Sample Project"}]}

    created = app.handle("client", "project-create", "new-project", "New project")
    assert created["created"] is True
    replay = app.handle("client", "project-create", "new-project", "New project")
    assert replay == {"slug": "new-project", "title": "New project", "created": False}
    assert len([call for call in api_calls if call[0] == "project-catalog"
                and call[1].get("mode") == "create"]) == 1
    with pytest.raises(broker.Refused, match="PROJECT_ALREADY_EXISTS"):
        app.handle("client", "project-create", "foreign", "Foreign")


def test_sources_are_project_scoped_and_external_registration_is_idempotent(control):
    app, _projects, _jobs, _submitted, _api_calls = control
    with pytest.raises(broker.Refused, match="PROJECT_NOT_ALLOWED"):
        app.handle("client", "source-add", GITHUB_SOURCE, "foreign")
    with pytest.raises(broker.Refused, match="UNSUPPORTED_SOURCE"):
        app.handle("client", "source-add", "https://example.invalid/path", PROJECT)

    first = app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    second = app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    assert first == second == {
        "source_id": GITHUB_SOURCE, "project": PROJECT,
        "provider": "github", "registered": True,
    }
    assert app.handle("client", "source-list", PROJECT) == {
        "sources": [{"source_id": GITHUB_SOURCE, "project": PROJECT, "provider": "github"}],
    }


def test_external_dry_run_is_bounded_and_real_ingest_is_separate(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    dry = app.handle("client", "ingest-dry-run", GITHUB_SOURCE, PROJECT, "2000-01-01", "1")
    assert submitted[-1][1] == [
        "github", "ingest", "--repo", "example-org/example-repo",
        "--project", PROJECT, "--since", "2000-01-01", "--limit", "1", "--dry-run",
    ]
    jobs[dry["job_id"]].update(state="done", result={
        "status": "dry_run", "counts": {"threads": 1, "items": 2, "nonempty": 1},
        "partial_reasons": ["optional_endpoint_unavailable"],
        "load": {"status": "dry_run", "rollback_confirmed": True},
    })
    status = app.handle("client", "job-status", dry["request_id"])
    assert status["job_id"] == dry["job_id"]
    assert status["ingest_status"] == "dry_run"
    assert status["rollback_confirmed"] is True
    assert status["counts"] == {"threads": 1, "items": 2, "nonempty": 1}
    assert status["partial_reasons"] == ["optional_endpoint_unavailable"]
    assert not any("path" in str(value).lower() or "sql" in str(value).lower()
                   for value in status.values())

    real = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    assert real["request_id"] != dry["request_id"]
    assert "--dry-run" not in submitted[-1][1]
    assert app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01") == real


@pytest.mark.parametrize(
    ("source_id", "expected"),
    [
        (
            "gitlab:gitlab.com/example-group/example-project",
            ["gitlab", "ingest", "--host", "gitlab.com", "--project",
             "example-group/example-project", "--project-slug", PROJECT,
             "--since", "2000-01-01", "--limit", "10", "--dry-run"],
        ),
        (
            "gitverse:example-org/example-repo",
            ["gitverse", "ingest", "--repo", "example-org/example-repo",
             "--project", PROJECT, "--since", "2000-01-01", "--limit", "10", "--dry-run"],
        ),
    ],
)
def test_external_connectors_register_and_build_fixed_dry_run_commands(
    control, source_id, expected,
):
    app, _projects, _jobs, submitted, _api_calls = control
    app.handle("client", "source-add", source_id, PROJECT)
    app.handle("client", "ingest-dry-run", source_id, PROJECT, "2000-01-01", "10")
    assert submitted[-1][1] == expected


def test_external_embed_requires_real_ingest_and_reports_remaining_vectors(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    with pytest.raises(broker.Refused, match="INGEST_REQUIRED"):
        app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    ingest = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[ingest["job_id"]].update(state="done")
    status = app.handle("client", "source-status", GITHUB_SOURCE, PROJECT)
    assert status["counts"]["nonempty_items_without_embeddings"] == 2
    embed = app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    assert submitted[-1][1] == [
        "external", "embed", "--source", GITHUB_SOURCE, "--project", PROJECT,
    ]
    assert app.handle("client", "embed", GITHUB_SOURCE, PROJECT) == embed


def test_external_initial_ingest_requires_explicit_cutoff_and_rejects_naive_time(control):
    app, _projects, _jobs, _submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    app.worker_api = lambda action, **_kwargs: (
        {"threads": 0} if action == "source-overview" else {"projects": []}
    )
    with pytest.raises(broker.Refused, match="SINCE_REQUIRED_FOR_INITIAL_IMPORT"):
        app.handle("client", "ingest", GITHUB_SOURCE, PROJECT)
    with pytest.raises(broker.Refused, match="INVALID_SINCE"):
        app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2024-01-01T00:00:00")


def test_project_search_rejects_limits_outside_the_search_ceiling(control):
    app, *_ = control
    with pytest.raises(broker.Refused, match="INVALID_LIMIT"):
        app.handle("client", "search-project", PROJECT, "two words", "51")
    with pytest.raises(broker.Refused, match="INVALID_LIMIT"):
        app.handle("client", "search-project", PROJECT, "two words", "not-a-number")


def test_project_search_accepts_limit_ceiling(control):
    app, *_ = control

    def failed_worker(action, **_kwargs):
        assert action == "status"
        return {"state": "failed"}

    app.worker_api = failed_worker
    result = app.handle("client", "search-project", PROJECT, "two words", "50")
    assert result["error_code"] == "SEARCH_FAILED"


def test_managed_external_retry_resumes_only_the_same_source_and_is_bounded(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    first = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[first["job_id"]].update(
        state="failed", progress={"completed_batches": 2},
        error={"category": "network_error", "message": "must not be exposed"},
        result={"status": "failed", "counts": {},
                "error": {"category": "network_error", "message": "must not be exposed"}},
    )
    failed = app.handle("client", "job-status", first["request_id"])
    assert failed["retryable"] is True
    assert failed["completed_batches"] == 2
    retry = app.handle("client", "retry", first["request_id"])
    assert retry["attempt"] == 2
    assert retry["job_id"] != first["job_id"]
    assert submitted[-1][1] == submitted[0][1]
    assert app.handle("client", "retry", first["request_id"]) == retry
    assert app.handle("client", "job-status", first["request_id"])["completed_batches"] == 2
    assert "must not be exposed" not in json.dumps(
        app.handle("client", "job-status", first["request_id"])
    )


def test_telegram_prepared_export_has_hash_checked_dry_run_and_ingest(control, tmp_path):
    app, _projects, jobs, submitted, _api_calls = control
    export_dir = _telegram_export(tmp_path)
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in export_dir.iterdir() if path.is_file()}

    registered = app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    assert registered["provider"] == "telegram"
    assert app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT) == registered

    dry = app.handle("client", "ingest-dry-run", TELEGRAM_SOURCE, PROJECT)
    assert submitted[-1][1][0:2] == ["telegram", "ingest"]
    assert submitted[-1][1][-1] == "--dry-run"
    jobs[dry["job_id"]].update(state="done", result={
        "status": "dry_run", "counts": {"inserted": 1, "service_events": 0},
        "load": {"status": "dry_run", "rollback_confirmed": True},
    })
    real = app.handle("client", "ingest", TELEGRAM_SOURCE, PROJECT)
    assert real["request_id"] != dry["request_id"]
    assert "--dry-run" not in submitted[-1][1]
    jobs[real["job_id"]].update(state="done", result={
        "status": "done", "counts": {"inserted": 1, "service_events": 0},
    })
    embedding = app.handle("client", "embed", TELEGRAM_SOURCE, PROJECT)
    assert submitted[-1][1][0] == "embed"
    assert real["job_id"] in submitted[-1][1][2]

    original_submit = app.submit

    def completed_search(argv, *, idempotency_key):
        submitted_job = original_submit(argv, idempotency_key=idempotency_key)
        if argv[0] == "search":
            jobs[submitted_job["job_id"]].update(
                state="done",
                result={"mode": "fts", "degraded": True, "degraded_reason": "quota",
                        "results": [{
                            "source_id": TELEGRAM_SOURCE, "message_id": "1",
                            "author": "Fixture", "posted_at": "2026-01-01T00:00:00Z",
                            "text": "prepared message", "chat_id": "-1009000",
                            "topic_id": "1",
                        }]},
            )
        return submitted_job

    app.submit = completed_search
    found = app.handle("client", "search-source", TELEGRAM_SOURCE, PROJECT,
                       "prepared message", "1")
    assert found["results"][0]["source_id"] == TELEGRAM_SOURCE
    assert found["results"][0]["message_id"] == "1"
    assert found["degraded_reason"] == "quota"
    refreshed = app.handle("client", "search-source", TELEGRAM_SOURCE, PROJECT,
                           "prepared message", "1")
    assert refreshed["request_id"] != found["request_id"]
    assert refreshed["job_id"] != found["job_id"]
    assert jobs[embedding["job_id"]]["state"] == "queued"
    assert {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in export_dir.iterdir() if path.is_file()} == before


def test_large_telegram_media_is_registered_staged_and_ingest_ready(control, tmp_path):
    app, _projects, _jobs, submitted, _api_calls = control
    export_dir = _telegram_export(tmp_path)
    media_size = broker.ASR_MAX_MEDIA_BYTES + 1
    media_path = export_dir / "media" / "1" / "synthetic-video.bin"
    media_path.parent.mkdir(mode=0o700, parents=True)
    media_path.parent.parent.chmod(0o700)
    media_path.parent.chmod(0o700)
    fd = broker.os.open(media_path, broker.os.O_WRONLY | broker.os.O_CREAT | broker.os.O_EXCL, 0o600)
    try:
        broker.os.ftruncate(fd, media_size)
    finally:
        broker.os.close(fd)
    digest = hashlib.sha256()
    block = bytes(1024 * 1024)
    for _ in range(media_size // len(block)):
        digest.update(block)
    digest.update(bytes(media_size % len(block)))

    message = {
        "chat_id": -1009000, "message_id": 1, "topic_id": 1,
        "date": "2026-01-01T00:00:00Z", "text": "large synthetic attachment",
        "sender_id": 12, "sender_name": "Fixture",
        "media": {
            "type": "video", "path": "media/1/synthetic-video.bin",
            "size": media_size, "size_bytes": media_size,
            "max_file_bytes": 4_000_000_000, "sha256": digest.hexdigest(),
            "export_status": "downloaded",
        },
    }
    message_bytes = (json.dumps(message, separators=(",", ":")) + "\n").encode()
    (export_dir / "messages.jsonl").write_bytes(message_bytes)
    (export_dir / "messages.jsonl").chmod(0o600)
    manifest_path = export_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.update({
        "messages_sha256": hashlib.sha256(message_bytes).hexdigest(),
        "max_file_bytes": 4_000_000_000,
        "media_requested": True,
    })
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
    manifest_path.chmod(0o600)

    registered = app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    assert registered["registered"] is True
    request = app.handle("client", "ingest", TELEGRAM_SOURCE, PROJECT)
    staged = Path(submitted[-1][1][3])
    staged_file = staged / "media" / "1" / "synthetic-video.bin"

    assert app.handle("client", "job-status", request["request_id"])["status"] == "queued"
    assert staged_file.stat().st_size == media_size
    with staged_file.open("rb") as stream:
        assert hashlib.file_digest(stream, "sha256").hexdigest() == digest.hexdigest()

    from kiber_analyst.telegram_ingest import prepare_export

    artifact, _export, media_missing = prepare_export(
        staged, tmp_path / "large-media-artifact", root=app.root / "imports",
    )
    normalized = json.loads((artifact / "normalized" / "messages.jsonl").read_text().splitlines()[0])
    assert media_missing == 0
    assert normalized["media_path"] == "media/1/synthetic-video.bin"
    assert normalized["meta"]["media_export"]["size"] == media_size
    assert normalized["meta"]["media_export"]["max_file_bytes"] == 4_000_000_000

def test_telegram_source_add_rejects_symlinked_export(control, tmp_path):
    app, _projects, _jobs, _submitted, _api_calls = control
    target = tmp_path / "elsewhere"
    _telegram_export(target)
    imports = tmp_path / "imports"
    imports.mkdir(mode=0o700, exist_ok=True)
    (imports / "-1009000").symlink_to(target / "imports" / "-1009000", target_is_directory=True)
    with pytest.raises(broker.Refused, match="TELEGRAM_EXPORT_UNAVAILABLE"):
        app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)


def test_revoked_project_hides_status_and_cancels_and_job_list(control):
    app, _projects, jobs, _submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    request = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    app.authorized_projects.clear()
    with pytest.raises(broker.Refused, match="PROJECT_NOT_ALLOWED"):
        app.handle("client", "job-status", request["request_id"])
    with pytest.raises(broker.Refused, match="PROJECT_NOT_ALLOWED"):
        app.handle("client", "cancel", request["request_id"])
    assert app.handle("client", "job-list") == {"jobs": []}
    assert jobs[request["job_id"]]["state"] == "queued"


def test_capabilities_do_not_claim_kiber_controls_telegram_export(control):
    app, *_ = control
    telegram = app.handle("client", "capabilities")["providers"]["telegram"]
    assert telegram["export"]["available_via_separate_interface"] is True
    assert telegram["export"]["invoked_by_kiber_broker"] is False
    assert "register_complete_topic_export" in telegram["operations"]
    assert telegram["scopes"] == {"topic": True, "group": False}


def test_static_source_does_not_grant_project_wide_access(control):
    original, *_ = control
    app = broker.Broker(
        original.root, {TELEGRAM_SOURCE: {"project": PROJECT, "hashes": {}}},
        original.worker_api, original.submit,
    )
    try:
        assert app._project_granted("client", PROJECT) is False
        assert app.project_list("client") == {"projects": []}
        assert app.handle("client", "source-list", PROJECT) == {
            "sources": [{
                "source_id": TELEGRAM_SOURCE,
                "project": PROJECT,
                "provider": "telegram",
            }],
        }
        with pytest.raises(broker.Refused, match="PROJECT_NOT_ALLOWED"):
            app.project_search("client", PROJECT, "query")
        with pytest.raises(broker.Refused, match="PROJECT_NOT_ALLOWED"):
            app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)

        legacy_request = {
            "source": TELEGRAM_SOURCE,
            "argv": json.dumps(["telegram", "ingest", "--job", "job", "--project", PROJECT]),
        }
        assert app._require_request_project("client", legacy_request) == PROJECT
    finally:
        app.db.close()


def test_gitlab_ingest_project_slug_is_used_for_request_authorization():
    assert broker.Broker._argv_project([
        "gitlab", "ingest", "--host", "gitlab.com", "--project", "group/repo",
        "--project-slug", PROJECT,
    ]) == PROJECT


def test_telegram_registration_rejects_control_file_larger_than_stage_limit(
    control, monkeypatch,
):
    from types import SimpleNamespace

    from kiber_analyst import telegram_ingest

    app, *_ = control
    monkeypatch.setattr(telegram_ingest, "inspect_export", lambda *_args, **_kwargs: SimpleNamespace(
        chat_id="-1009000", topic_id=1,
        messages_size=broker.CONTROL_FILE_MAX_BYTES + 1,
        services_size=0, manifest_size=0, services_manifest_size=0,
        message_sha256="a" * 64, service_sha256="b" * 64,
        manifest_sha256="c" * 64,
    ))
    with pytest.raises(broker.Refused, match="TELEGRAM_EXPORT_TOO_LARGE"):
        app._managed_source_entry_for_add("client", TELEGRAM_SOURCE, PROJECT)


def test_external_embed_accepts_legacy_spaced_source_fingerprint(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.sources[GITHUB_SOURCE] = {"project": PROJECT}
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    legacy_fingerprint = hashlib.sha256(
        json.dumps({"project": PROJECT}, sort_keys=True).encode()
    ).hexdigest()
    request_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    now = broker.timestamp()
    argv = ["github", "ingest", "--repo", "example-org/example-repo",
            "--project", PROJECT, "--since", "2000-01-01"]
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (request_id, "client", GITHUB_SOURCE, legacy_fingerprint, "ingest",
             json.dumps(argv), now, now, "done"),
        )
        app.db.execute(
            "INSERT INTO attempts(request_id,attempt,job_id,bound,state) VALUES(?,?,?,?,?)",
            (request_id, 1, job_id, now, "done"),
        )
    jobs[job_id] = {"state": "done"}

    embed = app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    assert embed["job_id"] == submitted[-1][0]
    assert submitted[-1][1] == [
        "external", "embed", "--source", GITHUB_SOURCE, "--project", PROJECT,
    ]


def test_external_generation_conflict_does_not_dispatch_different_argv(control):
    app, _projects, _jobs, submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    _entry, fingerprint, _parsed = app._managed_source_entry("client", GITHUB_SOURCE, PROJECT)
    conflicting_id = str(uuid.uuid4())
    now = broker.timestamp()
    conflicting_argv = [
        "github", "ingest", "--repo", "example-org/example-repo",
        "--project", "different-project", "--since", "2020-01-01",
    ]
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (conflicting_id, "client", GITHUB_SOURCE, f"{fingerprint}:real", "ingest",
             json.dumps(conflicting_argv), now, now, "queued"),
        )

    with pytest.raises(broker.Refused, match="SOURCE_BUSY"):
        app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    assert submitted == []


def test_managed_retry_uses_the_saved_project_not_static_source_project(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.sources[GITHUB_SOURCE] = {"project": "legacy-project"}
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    first = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retry = app.handle("client", "retry", first["request_id"])
    assert retry["attempt"] == 2
    assert submitted[-1][1] == submitted[0][1]


def test_legacy_static_retry_keeps_its_scope_after_managed_registration(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.sources[GITHUB_SOURCE] = {"project": PROJECT}
    first = app.handle("client", "ingest", GITHUB_SOURCE)
    original_argv = submitted[-1][1]
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retried = app.handle("client", "retry", first["request_id"])

    assert retried["attempt"] == 2
    assert submitted[-1][1] == original_argv
    assert broker.Broker._argv_project(json.loads(
        app.row("client", first["request_id"])["argv"]
    )) == PROJECT


def test_managed_embed_retry_uses_normalized_static_source_with_extra_config(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.sources[GITHUB_SOURCE] = {
        "project": PROJECT,
        "connector_options": {"include_drafts": False},
    }
    ingest = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[ingest["job_id"]]["state"] = "done"
    embedding = app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    original_argv = submitted[-1][1]
    jobs[embedding["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retried = app.handle("client", "retry", embedding["request_id"])

    assert retried["attempt"] == 2
    assert submitted[-1][1] == original_argv


def test_project_search_retry_preserves_saved_project_query_and_limit(
    control, monkeypatch,
):
    app, _projects, jobs, submitted, _api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    first = app.handle("client", "search-project", PROJECT, "two word query", "7")
    assert first["error_code"] == "SEARCH_PENDING"
    original_argv = submitted[-1][1]
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retried = app.handle("client", "retry", first["request_id"])

    assert retried["attempt"] == 2
    assert submitted[-1][1] == original_argv
    assert original_argv == [
        "search", "two word query", "--project", PROJECT, "--limit", "7", "--json",
    ]


def test_legacy_project_search_retry_scope_is_backfilled_after_restart(
    control, monkeypatch,
):
    app, _projects, jobs, submitted, _api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    first = app.handle("client", "search-project", PROJECT, "legacy query", "3")
    original_argv = submitted[-1][1]
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )
    with app.db:
        app.db.execute(
            "DELETE FROM project_search_scope WHERE request_id=?", (first["request_id"],),
        )
    app.db.close()

    restarted = broker.Broker(
        app.root, app.sources, app.worker_api, app.submit,
        authorized_projects={PROJECT}, allow_project_create=True,
    )
    try:
        assert restarted.db.execute(
            "SELECT count(*) FROM project_search_scope WHERE request_id=?",
            (first["request_id"],),
        ).fetchone()[0] == 1
        retried = restarted.handle("client", "retry", first["request_id"])
        assert retried["attempt"] == 2
        assert submitted[-1][1] == original_argv
    finally:
        restarted.db.close()


@pytest.mark.parametrize(
    ("revocation", "expected_error"),
    [("project", "PROJECT_NOT_ALLOWED"), ("source", "SOURCE_REVISION_CHANGED")],
)
def test_saved_embed_renewal_rechecks_access_after_broker_restart(
    control, revocation, expected_error,
):
    app, _projects, jobs, submitted, _api_calls = control
    app.sources[GITHUB_SOURCE] = {"project": PROJECT}
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    ingest = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[ingest["job_id"]]["state"] = "done"
    embedding = app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    current = embedding
    for attempt in range(1, 4):
        jobs[current["job_id"]].update(
            state="failed", progress={"completed_batches": 2},
            error={"category": "network_error"},
            result={"status": "failed", "error": {"category": "network_error"}},
        )
        if attempt < 3:
            current = app.handle("client", "retry", embedding["request_id"])

    before_dispatch_count = len(submitted)
    failed_submit_calls = []

    def fail_worker_submit(_argv, *, idempotency_key):
        failed_submit_calls.append(idempotency_key)
        raise JobsError("simulated worker submit outage")

    app.submit = fail_worker_submit
    with pytest.raises(JobsError, match="simulated worker submit outage"):
        app.handle("client", "renew-embed", embedding["request_id"])
    renewal = app.db.execute(
        "SELECT request_id FROM embed_renewals WHERE previous_request_id=?",
        (embedding["request_id"],),
    ).fetchone()
    assert renewal is not None
    if revocation == "source":
        with app.db:
            app.db.execute(
                "DELETE FROM managed_sources WHERE owner=? AND source=? AND project=?",
                ("client", GITHUB_SOURCE, PROJECT),
            )
    app.db.close()

    restarted = broker.Broker(
        app.root, {}, app.worker_api, fail_worker_submit,
        authorized_projects={PROJECT} if revocation == "source" else set(),
    )
    try:
        with pytest.raises(broker.Refused, match=expected_error):
            restarted.handle("client", "renew-embed", embedding["request_id"])
        assert len(submitted) == before_dispatch_count
        assert len(failed_submit_calls) == 1
        assert restarted.db.execute(
            "SELECT count(*) FROM attempts WHERE request_id=?",
            (renewal["request_id"],),
        ).fetchone()[0] == 0
    finally:
        restarted.db.close()


def test_unbound_embed_retry_rechecks_source_registration_after_broker_restart(control):
    app, _projects, jobs, submitted, _api_calls = control
    app.handle("client", "source-add", GITHUB_SOURCE, PROJECT)
    ingest = app.handle("client", "ingest", GITHUB_SOURCE, PROJECT, "2000-01-01")
    jobs[ingest["job_id"]]["state"] = "done"
    original = app.handle("client", "embed", GITHUB_SOURCE, PROJECT)
    current = original
    for attempt in range(1, broker.MAX_ATTEMPTS + 1):
        jobs[current["job_id"]].update(
            state="failed", progress={"completed_batches": 2},
            error={"category": "network_error"},
            result={"status": "failed", "error": {"category": "network_error"}},
        )
        if attempt < broker.MAX_ATTEMPTS:
            current = app.handle("client", "retry", original["request_id"])

    failed_submissions = []

    def interrupted_submit(argv, *, idempotency_key):
        failed_submissions.append((list(argv), idempotency_key))
        raise JobsError("simulated worker submit outage")

    app.submit = interrupted_submit
    with pytest.raises(JobsError, match="simulated worker submit outage"):
        app.handle("client", "renew-embed", original["request_id"])
    renewal = app.db.execute(
        "SELECT request_id FROM embed_renewals WHERE previous_request_id=?",
        (original["request_id"],),
    ).fetchone()
    assert renewal is not None
    request = app.row("client", renewal["request_id"])
    assert app.db.execute(
        "SELECT 1 FROM attempts WHERE request_id=?", (request["id"],),
    ).fetchone() is None

    with app.db:
        app.db.execute(
            "DELETE FROM managed_sources WHERE owner=? AND source=? AND project=?",
            ("client", GITHUB_SOURCE, PROJECT),
        )
    app.db.close()

    retry_submissions = []

    def record_submit(argv, *, idempotency_key):
        retry_submissions.append((list(argv), idempotency_key))
        return {"job_id": str(uuid.uuid5(uuid.NAMESPACE_OID, idempotency_key))}

    restarted = broker.Broker(
        app.root, {}, app.worker_api, record_submit, authorized_projects={PROJECT},
    )
    try:
        with pytest.raises(broker.Refused, match="SOURCE_REVISION_CHANGED"):
            restarted.handle("client", "retry", request["id"])
        assert len(failed_submissions) == 1
        assert len(submitted) == broker.MAX_ATTEMPTS + 1
        assert retry_submissions == []
        assert restarted.db.execute(
            "SELECT 1 FROM attempts WHERE request_id=?", (request["id"],),
        ).fetchone() is None
    finally:
        restarted.db.close()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("argv_query", "changed query"), ("argv_limit", "8"),
     ("argv_project", "other-project"), ("source", "project:other-project")],
)
def test_project_search_retry_rejects_tampered_scope_or_parameters(
    control, monkeypatch, field, replacement,
):
    app, _projects, jobs, submitted, _api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    first = app.handle("client", "search-project", PROJECT, "two word query", "7")
    row = app.row("client", first["request_id"])
    argv = json.loads(row["argv"])
    if field == "argv_query":
        argv[1] = replacement
    elif field == "argv_limit":
        argv[5] = replacement
    elif field == "argv_project":
        argv[3] = replacement
    with app.db:
        if field == "source":
            app.db.execute("UPDATE requests SET source=? WHERE id=?", (replacement, row["id"]))
        else:
            app.db.execute("UPDATE requests SET argv=? WHERE id=?", (json.dumps(argv), row["id"]))
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    with pytest.raises(broker.Refused):
        app.handle("client", "retry", first["request_id"])
    assert len(submitted) == 1


def test_telegram_dry_run_retry_keeps_the_dry_run_fingerprint_and_flag(
    control, tmp_path,
):
    app, _projects, jobs, submitted, _api_calls = control
    _telegram_export(tmp_path)
    app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    first = app.handle("client", "ingest-dry-run", TELEGRAM_SOURCE, PROJECT)
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retry = app.handle("client", "retry", first["request_id"])
    assert retry["attempt"] == 2
    assert "--dry-run" in submitted[-1][1]


def test_managed_telegram_retry_prefers_registration_over_static_source(
    control, tmp_path,
):
    app, _projects, jobs, submitted, _api_calls = control
    _telegram_export(tmp_path)
    app.sources[TELEGRAM_SOURCE] = {
        "project": PROJECT,
        "hashes": {name: "0" * 64 for name in broker.CONTROLS},
    }
    app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    first = app.handle("client", "ingest-dry-run", TELEGRAM_SOURCE, PROJECT)
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retried = app.handle("client", "retry", first["request_id"])

    assert retried["attempt"] == 2
    assert submitted[-1][1] == submitted[-2][1]
    assert "--dry-run" in submitted[-1][1]


def test_managed_telegram_retry_rejects_changed_export_with_static_source(
    control, tmp_path,
):
    app, _projects, jobs, _submitted, _api_calls = control
    export = _telegram_export(tmp_path)
    app.sources[TELEGRAM_SOURCE] = {"project": PROJECT}
    app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    first = app.handle("client", "ingest", TELEGRAM_SOURCE, PROJECT)
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )
    messages = export / "messages.jsonl"
    messages.write_bytes(messages.read_bytes().replace(b"prepared message", b"changed message"))

    with pytest.raises(broker.Refused, match="HASH_MISMATCH"):
        app.handle("client", "retry", first["request_id"])


@pytest.mark.parametrize(
    ("source_id", "provider"),
    [
        ("github:example-org/example-repo", "github"),
        ("gitlab:gitlab.com/example-group/example-project", "gitlab"),
        ("gitverse:example-org/example-repo", "gitverse"),
        (TELEGRAM_SOURCE, "telegram"),
    ],
)
def test_managed_search_retry_preserves_source_provider_project_query_and_limit(
    control, tmp_path, monkeypatch, source_id, provider,
):
    app, _projects, jobs, submitted, _api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    if provider == "telegram":
        _telegram_export(tmp_path)
    app.handle("client", "source-add", source_id, PROJECT)
    first = app.handle("client", "search-source", source_id, PROJECT, "two word query", "7")
    assert first["error_code"] == "SEARCH_PENDING"
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    retried = app.handle("client", "retry", first["request_id"])

    assert retried["attempt"] == 2
    assert submitted[-1][1] == submitted[-2][1]
    assert submitted[-1][1][1] == "two word query"
    assert submitted[-1][1][3] == PROJECT
    assert submitted[-1][1][5] == "7"
    assert submitted[-1][1][7] == provider
    assert submitted[-1][1][9] == source_id


@pytest.mark.parametrize(
    ("index", "replacement"),
    [
        (1, "changed query"),
        (3, "foreign-project"),
        (5, "8"),
        (7, "gitlab"),
        (9, "github:other/repository"),
    ],
)
def test_managed_search_retry_rejects_tampered_saved_parameters(
    control, monkeypatch, index, replacement,
):
    app, _projects, jobs, submitted, _api_calls = control
    source_id = "github:example-org/example-repo"
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    app.handle("client", "source-add", source_id, PROJECT)
    first = app.handle("client", "search-source", source_id, PROJECT, "two word query", "7")
    row = app.row("client", first["request_id"])
    argv = json.loads(row["argv"])
    argv[index] = replacement
    with app.db:
        app.db.execute("UPDATE requests SET argv=? WHERE id=?", (json.dumps(argv), row["id"]))
    jobs[first["job_id"]].update(
        state="failed", error={"category": "network_error"},
        result={"status": "failed", "error": {"category": "network_error"}},
    )

    with pytest.raises(broker.Refused):
        app.handle("client", "retry", first["request_id"])
    assert len(submitted) == 1


def test_search_result_retrieves_completed_project_and_source_searches(
    control, monkeypatch,
):
    app, _projects, jobs, _submitted, _api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    project_pending = app.handle("client", "search-project", PROJECT, "two words", "1")
    assert project_pending["error_code"] == "SEARCH_PENDING"
    jobs[project_pending["job_id"]].update(
        state="done", result={
            "mode": "hybrid", "degraded": False, "degraded_reason": None,
            "results": [{"source_id": GITHUB_SOURCE, "message_id": "42",
                         "text": "safe result", "path": "private", "sql": "private"}],
        },
    )
    project_result = app.handle("client", "search-result", project_pending["request_id"])
    assert project_result["status"] == "done"
    assert project_result["project"] == PROJECT
    assert project_result["results"][0]["message_id"] == "42"
    assert "path" not in project_result["results"][0]
    assert "sql" not in json.dumps(project_result)
    with pytest.raises(broker.Refused, match="REQUEST_NOT_FOUND"):
        app.handle("other-owner", "search-result", project_pending["request_id"])

    app.sources[TELEGRAM_SOURCE] = {"project": PROJECT, "hashes": {}}
    source_pending = app.handle("client", "search", TELEGRAM_SOURCE, "topic query", "1")
    assert source_pending["error_code"] == "SEARCH_PENDING"
    jobs[source_pending["job_id"]].update(
        state="done", result={
            "mode": "fts", "degraded": True, "degraded_reason": "quota",
            "results": [{"source_id": TELEGRAM_SOURCE, "message_id": "1",
                         "text": "safe source result"}],
        },
    )
    source_result = app.handle("client", "search-result", source_pending["request_id"])
    assert source_result["status"] == "done"
    assert source_result["source_id"] == TELEGRAM_SOURCE
    assert source_result["degraded_reason"] == "quota"


def test_source_search_skips_status_lookup_for_known_terminal_requests(
    control, monkeypatch,
):
    app, _projects, jobs, _submitted, api_calls = control
    monkeypatch.setattr(broker, "SEARCH_WAIT_SECONDS", 0)
    app.sources[TELEGRAM_SOURCE] = {"project": PROJECT, "hashes": {}}
    fingerprint = broker.search_fingerprint(TELEGRAM_SOURCE, PROJECT, "repeat query", 1)
    old_request_id = str(uuid.uuid4())
    old_job_id = str(uuid.uuid4())
    jobs[old_job_id] = {"state": "done", "progress": {}}
    now = broker.timestamp()
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (old_request_id, "client", TELEGRAM_SOURCE, fingerprint, "search",
             json.dumps(["search", "repeat query", "--project", PROJECT,
                         "--limit", "1", "--source-kind", "telegram",
                         "--source-id", TELEGRAM_SOURCE, "--json"]),
             now, now, "done"),
        )
        app.db.execute(
            "INSERT INTO attempts(request_id,attempt,job_id,bound,state) VALUES(?,?,?,?,?)",
            (old_request_id, 1, old_job_id, now, "done"),
        )

    app.handle("client", "search", TELEGRAM_SOURCE, "repeat query", "1")
    status_ids = [kwargs["job_id"] for action, kwargs in api_calls if action == "status"]
    assert old_job_id not in status_ids
    assert len(status_ids) == 1


def test_unbound_cancel_requires_worker_confirmation(control, monkeypatch):
    app, _projects, jobs, _submitted, _api_calls = control
    request_id = str(uuid.uuid4())
    now = broker.timestamp()
    argv = ["github", "ingest", "--repo", "example-org/example-repo",
            "--project", PROJECT, "--since", "2000-01-01"]
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (request_id, "client", GITHUB_SOURCE, "c" * 64, "ingest",
             json.dumps(argv), now, now, "submitting"),
        )
    result = app.handle("client", "cancel", request_id)
    assert result == {
        "request_id": request_id, "job_id": None, "status": "cancelled",
        "cancel_requested": False,
    }
    assert app.handle("client", "job-status", request_id)["status"] == "cancelled"

    another_id = str(uuid.uuid4())
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (another_id, "client", GITHUB_SOURCE, "d" * 64, "ingest",
             json.dumps(argv), now, now, "submitting"),
        )
    original_api = app.worker_api

    def unavailable(action, **kwargs):
        if action == "cancel-idempotency":
            raise JobsError("worker unavailable")
        return original_api(action, **kwargs)

    app.worker_api = unavailable
    with pytest.raises(broker.Refused, match="CANCEL_UNCONFIRMED"):
        app.handle("client", "cancel", another_id)
    assert app.row("client", another_id)["state"] == "submitting"


def test_cancelled_unbound_telegram_ingest_is_not_redispatched(control, tmp_path):
    app, _projects, _jobs, _submitted, _api_calls = control
    export = _telegram_export(tmp_path)
    app.sources[TELEGRAM_SOURCE] = {"project": PROJECT}
    app.handle("client", "source-add", TELEGRAM_SOURCE, PROJECT)
    original_submit = app.submit
    original_worker_api = app.worker_api
    submit_calls = []
    tombstones = set()

    def submit(argv, *, idempotency_key):
        submit_calls.append(idempotency_key)
        if idempotency_key in tombstones:
            raise JobsError("Job submission was cancelled")
        if len(submit_calls) == 1:
            raise JobsError("simulated worker submission outage")
        return original_submit(argv, idempotency_key=idempotency_key)

    def worker_api(action, **kwargs):
        result = original_worker_api(action, **kwargs)
        if action == "cancel-idempotency" and result.get("found") is False:
            tombstones.add(kwargs["idempotency_key"])
        return result

    app.submit = submit
    app.worker_api = worker_api
    with pytest.raises(JobsError, match="simulated worker submission outage"):
        app.handle("client", "ingest", TELEGRAM_SOURCE, PROJECT)
    request_id = app.db.execute(
        "SELECT id FROM requests WHERE source=? AND operation='ingest'",
        (TELEGRAM_SOURCE,),
    ).fetchone()[0]
    cancelled = app.handle("client", "cancel", request_id)
    assert cancelled["status"] == "cancelled"
    assert (export / "messages.jsonl").is_file()
    attempts_before = len(submit_calls)

    with pytest.raises(broker.Refused, match="INGEST_CANCELLED"):
        app.handle("client", "ingest", TELEGRAM_SOURCE, PROJECT)

    assert len(submit_calls) == attempts_before


def test_telegram_source_status_ignores_newer_request_for_another_project(
    control, tmp_path,
):
    app, *_ = control
    export_dir = _telegram_export(tmp_path)
    hashes = {
        name: hashlib.sha256((export_dir / name).read_bytes()).hexdigest()
        for name in broker.CONTROLS
    }
    app.sources[TELEGRAM_SOURCE] = {"project": PROJECT, "hashes": hashes}
    older_id, newer_id = str(uuid.uuid4()), str(uuid.uuid4())
    with app.db:
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (older_id, "client", TELEGRAM_SOURCE, "e" * 64, "ingest",
             json.dumps(["telegram", "ingest", "--job", "old", "--project", PROJECT]),
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "done"),
        )
        app.db.execute(
            "INSERT INTO requests(id,owner,source,fingerprint,operation,argv,created,updated,state) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (newer_id, "client", TELEGRAM_SOURCE, "f" * 64, "ingest",
             json.dumps(["telegram", "ingest", "--job", "new", "--project", "other-project"]),
             "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z", "done"),
        )

    result = app.handle("client", "source-status", TELEGRAM_SOURCE)
    assert result["ingest"]["request_id"] == older_id
