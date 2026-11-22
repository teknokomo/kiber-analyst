from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import types
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

from kiber_analyst.embedding_contract import DIMENSIONS, MODEL, MODEL_KEY
from kiber_analyst.jobqueue import JobsError, JobStore, utc_now
from kiber_analyst.routing import MeteredResult, PermanentProviderError, PoolConfig, WaitForQuota
from kiber_analyst.worker import JobWorker


def make_pool() -> PoolConfig:
    return PoolConfig(
        name="gemini_query",
        operation="search",
        provider="google",
        model="embedding-test",
        profiles=("profile_a", "profile_b"),
        strategy="round_robin",
        weights={"profile_a": 1, "profile_b": 1},
        quota_scopes={
            "profile_a": ("google:project-a",),
            "profile_b": ("google:project-b",),
        },
        limits=(),
        max_cycles=2,
        retry_initial_seconds=10,
        retry_max_seconds=60,
        retry_jitter_fraction=0.2,
    )


def make_query_pool() -> PoolConfig:
    return PoolConfig(
        name="gemini_query",
        operation="search",
        provider="google",
        model=MODEL_KEY,
        profiles=("profile_a", "profile_b"),
        strategy="failover",
        weights={"profile_a": 1, "profile_b": 1},
        quota_scopes={
            "profile_a": ("google:project-a",),
            "profile_b": ("google:project-b",),
        },
        limits=(),
        max_cycles=5,
        retry_initial_seconds=10,
        retry_max_seconds=300,
        retry_jitter_fraction=0.2,
    )


def make_job(store: JobStore):
    return store.create_job(
        operation="search",
        command={"pool": "gemini_query", "params": {"query": "q"}},
        fingerprint="f" * 64,
        idempotency_key="worker-search",
    )[0]


def test_agents_cannot_pin_a_provider_profile(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    request = {
        "argv": ["search", "query", "--project", "sample-project"],
        "profile": "profile_a",
    }

    with pytest.raises(JobsError, match="requires root"):
        worker.submit(request, uid=1001)

    submitted = worker.submit(request, uid=0)
    assert submitted["operation"] == "search"
    assert submitted["state"] == "queued"


def test_asr_worker_continues_after_one_permanent_file_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.asr as asr_module
    import kiber_analyst.asr_store as asr_store
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    job_dir = tmp_path / "asr-source"
    job_dir.mkdir(mode=0o700)
    audio_items = [
        asr_module.AudioInput(f"tg:-1001:{index}", f"media/{index}/voice.ogg",
                              tmp_path / f"voice-{index}.ogg", 8, f"{index:064x}")
        for index in (1, 2)
    ]
    job_record, _created = store.create_job(
        operation="telegram_asr",
        command={
            "params": {
                "job_dir": str(job_dir), "project": "sample-project",
                "load_results": True, "run_id": "ka-test", "external_refs": [
                    item.external_ref for item in audio_items
                ],
            },
            "pool": "groq_asr",
        },
        fingerprint="d" * 64,
        idempotency_key="asr-continue-after-file-error",
    )
    job = store.claim()
    assert job and job.id == job_record.id
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    monkeypatch.setattr(asr_module, "_collect_audio", lambda _job_dir: audio_items)
    monkeypatch.setattr(worker_module, "_duration_seconds", lambda _path: 1.5)
    attempts = []
    manifest_items = []
    run_dir = job_dir / "asr" / "batches" / "ka-test"
    results_dir = run_dir / "results"
    results_dir.mkdir(mode=0o700, parents=True)
    for directory in (job_dir / "asr", job_dir / "asr" / "batches", run_dir, results_dir):
        directory.chmod(0o700)

    def fake_run_asr_batch(_job_dir, *, external_refs, **_kwargs):
        reference = next(iter(external_refs))
        attempts.append(reference)
        if reference == audio_items[0].external_ref:
            result = {"text": "успешная расшифровка", "segments": [
                {"start": 0.0, "end": 1.5, "text": "успешная расшифровка"},
            ]}
            result_path = results_dir / "one.json"
            result_path.write_text(json.dumps(result), encoding="utf-8")
            result_path.chmod(0o600)
            manifest_items.append({
                "external_ref": reference, "status": "complete", "result": "results/one.json",
                "result_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                "profile": "groq_asr", "attempts": [],
            })
        else:
            manifest_items.append({
                "external_ref": reference, "status": "failed", "http_code": 413,
                "failure": {"category": "request_rejected", "status_code": 413},
                "attempts": [{"outcome": "request_rejected", "status_code": 413}],
            })
        manifest = {"run_id": "ka-test", "items": manifest_items}
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        manifest_path.chmod(0o600)
        return {"run_dir": str(run_dir), "items": [manifest_items[-1]]}

    monkeypatch.setattr(asr_module, "run_asr_batch", fake_run_asr_batch)
    monkeypatch.setattr(
        worker, "_route_call",
        lambda _job, _item_id, invoke, _usage: _route_asr(invoke),
    )
    loaded = []
    monkeypatch.setattr(asr_store, "load_asr", lambda *args, **kwargs: loaded.append(args) or {"inserted": 1})

    report = worker._run_asr(job)

    assert attempts == [item.external_ref for item in audio_items]
    assert report["status"] == "partial"
    assert report["files_done"] == 1
    assert report["files_failed"] == 1
    assert report["segments"] == 1
    assert len(loaded) == 1


def test_asr_resume_reuses_saved_success_after_quota_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.asr as asr_module
    import kiber_analyst.asr_store as asr_store
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    job_dir = tmp_path / "asr-source"
    job_dir.mkdir(mode=0o700)
    audio_items = [
        asr_module.AudioInput(f"tg:-1001:{index}", f"media/{index}/voice.ogg",
                              tmp_path / f"voice-{index}.ogg", 8, f"{index:064x}")
        for index in (1, 2)
    ]

    def new_job(idempotency_key: str):
        record, _created = store.create_job(
            operation="telegram_asr",
            command={
                "params": {
                    "job_dir": str(job_dir), "project": "sample-project",
                    "load_results": True, "run_id": "ka-resume", "external_refs": [
                        item.external_ref for item in audio_items
                    ],
                },
                "pool": "groq_asr",
            },
            fingerprint="e" * 64,
            idempotency_key=idempotency_key,
        )
        claimed = store.claim()
        assert claimed and claimed.id == record.id
        return claimed

    first_job = new_job("asr-resume-first")
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    monkeypatch.setattr(asr_module, "_collect_audio", lambda _job_dir: audio_items)
    monkeypatch.setattr(worker_module, "_duration_seconds", lambda _path: 1.5)
    run_dir = job_dir / "asr" / "batches" / "ka-resume"
    results_dir = run_dir / "results"
    results_dir.mkdir(mode=0o700, parents=True)
    for directory in (job_dir / "asr", job_dir / "asr" / "batches", run_dir, results_dir):
        directory.chmod(0o700)

    manifest_items = {
        item.external_ref: {"external_ref": item.external_ref, "status": "pending"}
        for item in audio_items
    }
    api_calls = []

    def fake_run_asr_batch(_job_dir, *, external_refs, **_kwargs):
        reference = next(iter(external_refs))
        api_calls.append(reference)
        result = {"text": f"transcript {reference}", "segments": [
            {"start": 0.0, "end": 1.5, "text": f"transcript {reference}"},
        ]}
        filename = f"{reference.rsplit(':', 1)[1]}.json"
        result_path = results_dir / filename
        result_path.write_text(json.dumps(result), encoding="utf-8")
        result_path.chmod(0o600)
        state = {
            "external_ref": reference, "status": "complete",
            "result": f"results/{filename}",
            "result_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
            "profile": "groq_asr", "attempts": [],
        }
        manifest_items[reference] = state
        (run_dir / "manifest.json").write_text(json.dumps({
            "run_id": "ka-resume", "items": list(manifest_items.values()),
        }), encoding="utf-8")
        (run_dir / "manifest.json").chmod(0o600)
        return {"run_dir": str(run_dir), "items": [state]}

    monkeypatch.setattr(asr_module, "run_asr_batch", fake_run_asr_batch)
    route_calls = 0

    def wait_after_first_success(_job, _item_id, invoke, _usage):
        nonlocal route_calls
        route_calls += 1
        if route_calls == 2:
            from kiber_analyst.routing import WaitForQuota

            raise WaitForQuota(next_run_at=None, scope="test-scope", reason="synthetic 429")
        return _route_asr(invoke)

    monkeypatch.setattr(worker, "_route_call", wait_after_first_success)
    loaded = []
    monkeypatch.setattr(asr_store, "load_asr", lambda *args, **kwargs: loaded.append(args) or {"inserted": 1})

    from kiber_analyst.routing import WaitForQuota

    with pytest.raises(WaitForQuota):
        worker._run_asr(first_job)
    assert api_calls == [audio_items[0].external_ref]
    assert len(loaded) == 1

    second_job = new_job("asr-resume-second")
    monkeypatch.setattr(worker, "_route_call", lambda _job, _item_id, invoke, _usage: _route_asr(invoke))
    report = worker._run_asr(second_job)

    assert api_calls == [item.external_ref for item in audio_items]
    assert report["status"] == "complete"
    assert report["files_done"] == 2
    assert report["segments"] == 2
    assert len(loaded) == 2


def test_asr_worker_loads_cached_pilot_results_without_provider_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kiber_analyst.asr as asr_module
    import kiber_analyst.asr_store as asr_store
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    job_dir = tmp_path / "asr-source"
    job_dir.mkdir(mode=0o700)
    run_id = "ka-saved-pilot"
    external_ref = "tg:-1001:10367"
    audio = asr_module.AudioInput(
        external_ref, "media/10367/voice.ogg", tmp_path / "voice.ogg", 8, "a" * 64
    )
    job_record, _created = store.create_job(
        operation="telegram_asr",
        command={
            "params": {
                "job_dir": str(job_dir), "project": "sample-project",
                "load_results": True, "run_id": run_id,
                "external_refs": [external_ref],
            },
            "pool": "groq_asr",
        },
        fingerprint="f" * 64,
        idempotency_key="asr-saved-pilot-results",
    )
    job = store.claim()
    assert job and job.id == job_record.id

    run_dir = job_dir / "asr" / "batches" / run_id
    results_dir = run_dir / "results"
    results_dir.mkdir(mode=0o700, parents=True)
    result_path = results_dir / "10367.json"
    result_bytes = json.dumps({
        "text": "сохранённая расшифровка",
        "segments": [{"start": 0.0, "end": 1.5, "text": "сохранённая расшифровка"}],
    }).encode()
    result_path.write_bytes(result_bytes)
    result_path.chmod(0o600)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps({
        "run_id": run_id,
        "items": [{
            "external_ref": external_ref, "status": "complete",
            "result": "results/10367.json",
            "result_sha256": hashlib.sha256(result_bytes).hexdigest(),
        }],
    }), encoding="utf-8")
    manifest_path.chmod(0o600)

    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    monkeypatch.setattr(asr_module, "_collect_audio", lambda _job_dir: [audio])
    monkeypatch.setattr(worker_module, "_duration_seconds", lambda _path: 1.5)
    monkeypatch.setattr(
        asr_module, "run_asr_batch",
        lambda *_args, **_kwargs: pytest.fail("cached result must not call Groq"),
    )
    loaded = []
    monkeypatch.setattr(
        asr_store, "load_asr",
        lambda *args, **kwargs: loaded.append((args, kwargs)) or {"status": "done"},
    )

    report = worker._run_asr(job)

    assert report["status"] == "complete"
    assert report["files_done"] == 1
    assert report["segments"] == 1
    assert len(loaded) == 1
    assert loaded[0][1]["external_refs"] == [external_ref]
    assert loaded[0][1]["input_durations"] == {external_ref: 1.5}


def _route_asr(invoke):
    from kiber_analyst.worker import ReportedProviderError

    try:
        result = invoke("groq_asr")
    except ReportedProviderError as exc:
        raise PermanentProviderError(
            "synthetic rejected input", status_code=exc.status_code, category=exc.category
        ) from None
    assert isinstance(result, MeteredResult)
    return result.value


def test_search_worker_uses_submitted_fingerprint_and_saves_query_vector(
    tmp_path: Path, monkeypatch
) -> None:
    import kiber_analyst.search as search_module
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    submitted = worker.submit(
        {
            "argv": [
                "search",
                "Sample Station",
                "--project",
                "sample-project",
                "--limit",
                "1",
            ]
        },
        uid=1001,
    )
    job = store.claim()
    assert job is not None and job.id == submitted["job_id"]

    def fake_embed(profile: str, texts: list[str], *, timeout: float):
        assert profile in {"profile_a", "profile_b"}
        assert texts == ["Sample Station"]
        assert timeout > 0
        return [[0.0] * 1536], 4

    def fake_search(query: str, **kwargs):
        assert query == "Sample Station"
        vector = kwargs["embedding_provider"](query)
        assert len(vector) == 1536
        return {"degraded": False, "results": [{"source": "segment"}]}

    monkeypatch.setattr(worker_module, "_provider_embed", fake_embed, raising=False)
    monkeypatch.setattr(search_module, "search", fake_search)

    result = worker._search(job)

    assert result["degraded"] is False
    assert len(result["results"]) == 1
    items = {item["key"]: item for item in store.job_items(job.id)}
    assert items["search-query"]["state"] == "done"
    query_item = next(value for key, value in items.items() if key.startswith("query-embedding:"))
    assert query_item["state"] == "done"
    assert query_item["artifact_path"] is not None
    assert store.attempts_summary(job.id)["count"] == 1


def test_query_embedding_rejects_unconfigured_model_without_audit_or_send(
    tmp_path: Path,
) -> None:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = JobWorker(store=store, pools={"gemini_query": make_query_pool()})
    request_id = str(uuid.uuid4())

    result = worker.query_embedding(
        {
            "request_id": request_id,
            "query": "hello",
            "project": "sample-project",
            "model": "another-model:1536",
        }
    )

    assert result["reason"] == "invalid_argument"
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM interactive_requests").fetchone()[0] == 0


def test_query_embedding_sends_once_and_returns_audited_1536_vector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = JobWorker(store=store, pools={"gemini_query": make_query_pool()})
    request_id = str(uuid.uuid4())
    sent: list[tuple[str, str]] = []

    class Part:
        @staticmethod
        def from_text(text: str) -> str:
            return text

    class Content:
        def __init__(self, parts: list[str]) -> None:
            self.parts = parts

    class EmbedContentConfig:
        def __init__(self, output_dimensionality: int) -> None:
            self.output_dimensionality = output_dimensionality

    fake_types = types.SimpleNamespace(
        Part=Part,
        Content=Content,
        EmbedContentConfig=EmbedContentConfig,
    )
    fake_google = types.ModuleType("google")
    fake_google.__path__ = []  # type: ignore[attr-defined]
    fake_genai = types.ModuleType("google.genai")
    fake_genai.types = fake_types  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)

    class FakeModels:
        async def embed_content(self, *, model: str, contents: list[Content], config: EmbedContentConfig):
            assert config.output_dimensionality == DIMENSIONS
            assert contents[0].parts == ["safe query"]
            sent.append((model, contents[0].parts[0]))
            return types.SimpleNamespace(
                embeddings=[types.SimpleNamespace(values=[0.25] * DIMENSIONS)],
                usage_metadata=types.SimpleNamespace(prompt_token_count=2),
            )

    class FakeAio:
        models = FakeModels()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class FakeSDK:
        aio = FakeAio()

        def close(self) -> None:
            return None

    import kiber_analyst.provider_http as provider_http

    profiles: list[str] = []

    def fake_client(profile: str, *, timeout: float, retry_attempts: int):
        assert timeout <= 10
        assert retry_attempts == 1
        profiles.append(profile)
        return FakeSDK()

    monkeypatch.setattr(provider_http, "genai_client", fake_client)

    result = worker.query_embedding(
        {
            "request_id": request_id,
            "query": "safe query",
            "project": "sample-project",
            "model": MODEL_KEY,
        }
    )

    assert result["status"] == "done"
    assert result["dimensions"] == DIMENSIONS
    assert result["model"] == MODEL_KEY
    assert result["vector"] == [0.25] * DIMENSIONS
    assert sent == [(MODEL, "safe query")]
    assert profiles == ["profile_a"]
    with store.connect() as db:
        request = db.execute(
            "SELECT state,query_sha256 FROM interactive_requests WHERE id=?", (request_id,)
        ).fetchone()
        attempt = db.execute(
            "SELECT status,http_code FROM interactive_attempts WHERE request_id=?", (request_id,)
        ).fetchone()
        quota = db.execute(
            "SELECT DISTINCT state FROM quota_events WHERE request_id=?", (request_id,)
        ).fetchall()
    assert tuple(request) == ("done", hashlib.sha256(b"safe query").hexdigest())
    assert tuple(attempt) == ("succeeded", None)
    assert {row[0] for row in quota} == {"consumed"}


def test_query_embedding_timeout_cancels_provider_call_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kiber_analyst.provider_http as provider_http
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    worker = JobWorker(store=store, pools={"gemini_query": make_query_pool()})
    request_id = str(uuid.uuid4())
    cancelled: list[bool] = []
    profiles: list[str] = []

    class Part:
        @staticmethod
        def from_text(text: str) -> str:
            return text

    class Content:
        def __init__(self, parts: list[str]) -> None:
            self.parts = parts

    class EmbedContentConfig:
        def __init__(self, output_dimensionality: int) -> None:
            self.output_dimensionality = output_dimensionality

    fake_google = types.ModuleType("google")
    fake_google.__path__ = []  # type: ignore[attr-defined]
    fake_genai = types.ModuleType("google.genai")
    fake_genai.types = types.SimpleNamespace(
        Part=Part, Content=Content, EmbedContentConfig=EmbedContentConfig
    )  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)

    class FakeModels:
        async def embed_content(self, **_kwargs):
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

    class FakeAio:
        models = FakeModels()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class FakeSDK:
        aio = FakeAio()

        def close(self) -> None:
            return None

    def fake_client(profile: str, *, timeout: float, retry_attempts: int):
        assert timeout <= 10
        assert retry_attempts == 1
        profiles.append(profile)
        return FakeSDK()

    real_wait_for = asyncio.wait_for

    async def short_wait_for(awaitable, timeout: float):
        return await real_wait_for(awaitable, timeout=0.01)

    monkeypatch.setattr(provider_http, "genai_client", fake_client)
    monkeypatch.setattr(worker_module.asyncio, "wait_for", short_wait_for)

    result = worker.query_embedding(
        {
            "request_id": request_id,
            "query": "cancel this request",
            "project": "sample-project",
            "model": MODEL_KEY,
        }
    )

    assert result["status"] == "failed"
    assert result["reason"] == "timeout"
    assert profiles == ["profile_a"]
    assert cancelled == [True]
    with store.connect() as db:
        request = db.execute(
            "SELECT state,outcome_uncertain FROM interactive_requests WHERE id=?",
            (request_id,),
        ).fetchone()
        attempt = db.execute(
            "SELECT status,outcome_uncertain FROM interactive_attempts WHERE request_id=?",
            (request_id,),
        ).fetchone()
        quota = db.execute(
            "SELECT DISTINCT state FROM quota_events WHERE request_id=?", (request_id,)
        ).fetchall()
    assert tuple(request) == ("failed", 1)
    assert tuple(attempt) == ("uncertain", 1)
    assert {row[0] for row in quota} == {"uncertain"}


def test_embedding_callback_splits_batches_when_local_item_budget_is_small(
    tmp_path: Path, monkeypatch
) -> None:
    import kiber_analyst.worker as worker_module

    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    job, _ = store.create_job(
        operation="embed",
        command={"pool": "gemini_query", "params": {}},
        fingerprint="e" * 64,
        idempotency_key="adaptive-batch",
    )
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    calls: list[int] = []

    def fake_embed(_profile: str, texts: list[str], *, timeout: float):
        assert timeout > 0
        return [[float(len(text))] for text in texts], len(texts)

    def limited_route(_job, _item_id, invoke, usage, **_kwargs):
        count = int(usage["items"])
        calls.append(count)
        if count > 3:
            raise WaitForQuota(
                next_run_at=None,
                scope="google:test",
                reason="quota_budget_exhausted",
            )
        return invoke("profile_a").value

    monkeypatch.setattr(worker_module, "_provider_embed", fake_embed, raising=False)
    monkeypatch.setattr(worker, "_route_call", limited_route)
    callback = worker._embedding_callback(job)

    vectors, tokens = callback(["a", "bb", "ccc", "dddd", "eeeee", "ffffff", "g", "hh"])

    assert len(vectors) == 8
    assert tokens == 8
    assert calls == [8, 4, 2, 2, 4, 2, 2]
    items = store.job_items(job.id)
    assert sum(item["state"] == "skipped" for item in items) == 3
    assert sum(item["state"] == "done" for item in items) == 4


def test_temporary_server_outage_persists_waiting_retry_state(tmp_path: Path, monkeypatch) -> None:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    job, _ = store.create_job(
        operation="search",
        command={"pool": "gemini_query", "params": {"query": "q"}},
        fingerprint="9" * 64,
        idempotency_key="retry-state",
    )
    worker = JobWorker(store=store, pools={"gemini_query": make_pool()})
    claimed = store.claim()
    assert claimed is not None and claimed.id == job.id

    def temporary_failure(_job):
        raise WaitForQuota(
            next_run_at=utc_now() + timedelta(seconds=30),
            scope="google:project-a",
            reason="profile_retry_cooldown",
        )

    monkeypatch.setattr(worker, "_search", temporary_failure)
    worker.process(claimed)

    assert store.status(job.id)["state"] == "waiting_retry"
