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

    app.handle("client", "search", TELEGRAM_SOURCE, "repeat query", "1")
    status_ids = [kwargs["job_id"] for action, kwargs in api_calls if action == "status"]
    assert old_request_id not in status_ids
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
