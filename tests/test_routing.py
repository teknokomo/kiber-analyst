from __future__ import annotations

import email.utils
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from kiber_analyst.jobqueue import JobStore, iso_utc, utc_now
from kiber_analyst.routing import (
    MeteredResult,
    PermanentProviderError,
    PoolConfig,
    ProviderRouter,
    QuotaLimit,
    RoutingError,
    WaitForQuota,
    _retry_after,
    load_pools,
    select_profile,
)


class ProviderFailure(Exception):
    def __init__(self, status: int, headers: dict[str, str] | None = None):
        self.status_code = status
        self.response = type("Response", (), {"headers": headers or {}})()
        super().__init__(f"HTTP {status}")


def make_store(tmp_path: Path) -> JobStore:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    return store


def make_pool(
    *,
    strategy: str = "round_robin",
    scopes: dict[str, tuple[str, ...]] | None = None,
    limits: tuple[QuotaLimit, ...] = (),
    max_cycles: int = 2,
) -> PoolConfig:
    profiles = ("profile_a", "profile_b")
    return PoolConfig(
        name="test_pool",
        operation="embed",
        provider="google",
        model="embedding-test",
        profiles=profiles,
        strategy=strategy,
        weights={"profile_a": 2, "profile_b": 1},
        quota_scopes=(
            scopes
            if scopes is not None
            else {name: (f"google:{name}",) for name in profiles}
        ),
        limits=limits,
        max_cycles=max_cycles,
        retry_initial_seconds=10,
        retry_max_seconds=60,
        retry_jitter_fraction=0.2,
    )


def make_query_pool(
    *,
    scopes: dict[str, tuple[str, ...]] | None = None,
    strategy: str = "failover",
    limits: tuple[QuotaLimit, ...] = (),
) -> PoolConfig:
    profiles = ("profile_a", "profile_b")
    return PoolConfig(
        name="gemini_query",
        operation="search",
        provider="google",
        model="embedding-test",
        profiles=profiles,
        strategy=strategy,
        weights={"profile_a": 1, "profile_b": 1},
        quota_scopes=scopes or {
            "profile_a": ("google:project-a",),
            "profile_b": ("google:project-b",),
        },
        limits=limits,
        max_cycles=5,
        retry_initial_seconds=10,
        retry_max_seconds=300,
        retry_jitter_fraction=0.2,
    )


def make_job(store: JobStore, key: str):
    return store.create_job(
        operation="embed", command={}, fingerprint=key * 64, idempotency_key=key
    )[0]


def test_routing_example_config_contains_two_profile_pools() -> None:
    config = Path(__file__).parents[1] / "config" / "provider-routing.example.yaml"

    pools = load_pools(config)

    assert pools["groq_asr"].profiles == ("groq_asr", "groq_asr_2")
    assert pools["gemini_embed"].profiles == ("gemini_embed", "gemini_embed_2")
    assert pools["gemini_query"].strategy == "failover"
    assert pools["gemini_embed"].scope_for("gemini_embed") == "google:gemini_embed"
    assert pools["gemini_embed"].scope_for("gemini_embed") != pools[
        "gemini_embed"
    ].scope_for("gemini_embed_2")
    assert pools["gemini_asr_embed"].scope_for("gemini_embed") == "google:gemini_embed"


def test_unmapped_profiles_get_independent_default_quota_scopes() -> None:
    pool = make_pool(scopes={})

    assert pool.scope_for("profile_a") == "google:profile:profile_a"
    assert pool.scope_for("profile_b") == "google:profile:profile_b"
    assert pool.scope_for("profile_a") != pool.scope_for("profile_b")


def test_routing_config_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    config = tmp_path / "routing.yaml"
    config.write_text("version: 1\nversion: 1\npools: {}\n", encoding="utf-8")

    with pytest.raises(RoutingError, match="duplicate"):
        load_pools(config)


def test_round_robin_cursor_persists_across_selections(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool()

    selected = [select_profile(store, pool)[0] for _ in range(4)]

    assert selected == ["profile_a", "profile_b", "profile_a", "profile_b"]


def test_failover_prefers_primary_when_it_is_available(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(strategy="failover")

    assert [select_profile(store, pool)[0] for _ in range(3)] == [
        "profile_a", "profile_a", "profile_a"
    ]


def test_earliest_profile_retry_wake_is_not_delayed_by_later_quota_reset(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = make_pool()
    now = utc_now()
    profile_ready = now.replace(microsecond=0) + timedelta(seconds=5)
    quota_ready = now.replace(microsecond=0) + timedelta(seconds=100)
    with store.transaction() as db:
        db.execute(
            """INSERT INTO routing_state
               (pool_name,cursor_index,weighted_current_json,disabled_profiles_json,cooldowns_json,updated_at)
               VALUES(?,0,'{}','[]',?,?)""",
            (pool.name, json.dumps({"profile_b": iso_utc(profile_ready)}), iso_utc(now)),
        )
        db.execute(
            """INSERT INTO quota_buckets
               (scope,provider,model,metric,window_name,window_seconds,available_at,updated_at)
               VALUES('google:profile_a','google',?,'*','cooldown',0,?,?)""",
            (pool.model, iso_utc(quota_ready), iso_utc(now)),
        )

    selected, next_at, reason = select_profile(store, pool, now=now)

    assert selected is None
    assert next_at is not None and 0 < (next_at - now).total_seconds() < 10
    assert reason == "profile_retry_cooldown"


def test_excluded_profile_short_cooldown_drives_next_wake(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = make_pool()
    now = utc_now()
    profile_ready = now.replace(microsecond=0) + timedelta(seconds=5)
    quota_ready = now.replace(microsecond=0) + timedelta(seconds=100)
    with store.transaction() as db:
        db.execute(
            """INSERT INTO routing_state
               (pool_name,cursor_index,weighted_current_json,disabled_profiles_json,cooldowns_json,updated_at)
               VALUES(?,0,'{}','[]',?,?)""",
            (pool.name, json.dumps({"profile_b": iso_utc(profile_ready)}), iso_utc(now)),
        )
        db.execute(
            """INSERT INTO quota_buckets
               (scope,provider,model,metric,window_name,window_seconds,available_at,updated_at)
               VALUES('google:profile_a','google',?,'*','cooldown',0,?,?)""",
            (pool.model, iso_utc(quota_ready), iso_utc(now)),
        )

    selected, next_at, reason = select_profile(
        store, pool, exclude_profiles={"profile_b"}, now=now
    )

    assert selected is None
    assert next_at is not None and 0 < (next_at - now).total_seconds() < 10
    assert reason == "profile_retry_cooldown"


def test_router_call_uses_short_profile_cooldown_after_429(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=2)
    job = make_job(store, "short-cooldown")
    now = utc_now()
    quota_ready = now.replace(microsecond=0) + timedelta(seconds=100)
    with store.transaction() as db:
        db.execute(
            """INSERT INTO quota_buckets
               (scope,provider,model,metric,window_name,window_seconds,available_at,updated_at)
               VALUES('google:profile_a','google',?,'*','cooldown',0,?,?)""",
            (pool.model, iso_utc(quota_ready), iso_utc(now)),
        )
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        raise ProviderFailure(429, {"retry-after": "33"})

    with pytest.raises(WaitForQuota) as caught:
        router.call(
            pool_name=pool.name,
            job_id=job.id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )

    assert calls == ["profile_b"]
    assert caught.value.next_run_at is not None
    remaining = (caught.value.next_run_at - utc_now()).total_seconds()
    assert 0 < remaining < 60


def test_weighted_round_robin_observes_configured_ratio(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(strategy="weighted_round_robin")

    selected = [select_profile(store, pool)[0] for _ in range(3)]

    assert selected.count("profile_a") == 2
    assert selected.count("profile_b") == 1


def test_weighted_strategy_override_requires_a_weight_for_each_profile(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = replace(make_pool(), weights={"profile_a": 1})

    with pytest.raises(RoutingError, match="every profile"):
        select_profile(store, pool, strategy_override="weighted_round_robin")


def test_unavailable_profile_is_skipped_and_401_rotates(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, "a")
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        if profile == "profile_a":
            raise ProviderFailure(401)
        return MeteredResult("ok", {"requests": 1})

    assert router.call(
        pool_name=pool.name,
        job_id=job.id,
        item_id=None,
        invoke=invoke,
        usage={"requests": 1},
    ) == "ok"
    assert calls == ["profile_a", "profile_b"]
    with store.connect() as db:
        disabled = db.execute(
            "SELECT disabled_profiles_json FROM routing_state WHERE pool_name=?",
            (pool.name,),
        ).fetchone()[0]
    assert '"profile_a"' in disabled


def test_401_disables_a_profile_across_all_pools(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    shared_pool = PoolConfig(
        name="shared_profile_pool",
        operation="asr_embed",
        provider=pool.provider,
        model=pool.model,
        profiles=pool.profiles,
        strategy=pool.strategy,
        weights=pool.weights,
        quota_scopes=pool.quota_scopes,
        limits=pool.limits,
        max_cycles=pool.max_cycles,
        retry_initial_seconds=pool.retry_initial_seconds,
        retry_max_seconds=pool.retry_max_seconds,
        retry_jitter_fraction=pool.retry_jitter_fraction,
    )
    job = make_job(store, "cross-pool-401")
    router = ProviderRouter(
        store, {pool.name: pool, shared_pool.name: shared_pool}
    )

    def invoke(profile: str):
        if profile == "profile_a":
            raise ProviderFailure(401)
        return MeteredResult("ok", {"requests": 1})

    assert router.call(
        pool_name=pool.name,
        job_id=job.id,
        item_id=None,
        invoke=invoke,
        usage={"requests": 1},
    ) == "ok"

    with store.connect() as db:
        states = {
            row["pool_name"]: json.loads(row["disabled_profiles_json"])
            for row in db.execute(
                "SELECT pool_name,disabled_profiles_json FROM routing_state"
            )
        }
    assert states[pool.name] == ["profile_a"]
    assert states[shared_pool.name] == ["profile_a"]


def test_429_rotates_to_profile_with_a_distinct_quota_scope(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, "b")
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        if profile == "profile_a":
            raise ProviderFailure(429, {"retry-after": "120"})
        return MeteredResult("ok", {"requests": 1})

    assert router.call(
        pool_name=pool.name,
        job_id=job.id,
        item_id=None,
        invoke=invoke,
        usage={"requests": 1},
    ) == "ok"
    assert calls == ["profile_a", "profile_b"]
    attempts = store.attempts_summary(job.id)["items"]
    assert [attempt["http_code"] for attempt in attempts] == [429, None]
    with store.connect() as conn:
        observation = conn.execute(
            "SELECT profile,operation,http_status,details_json FROM gemini_quota_observations"
        ).fetchone()
    assert tuple(observation[:3]) == ("profile_a", "embed", 429)
    assert json.loads(observation[3]) == {"retry_after_seconds": 120.0}


def test_5xx_fails_over_to_another_profile_without_waiting(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, "server-failover")
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        if profile == "profile_a":
            raise ProviderFailure(503, {"retry-after": "90"})
        return MeteredResult("ok", {"requests": 1})

    assert router.call(
        pool_name=pool.name,
        job_id=job.id,
        item_id=None,
        invoke=invoke,
        usage={"requests": 1},
    ) == "ok"
    assert calls == ["profile_a", "profile_b"]


def test_shared_429_scope_waits_without_pinging_second_key(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(
        scopes={"profile_a": ("google:shared",), "profile_b": ("google:shared",)},
        max_cycles=1,
    )
    job = make_job(store, "c")
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        raise ProviderFailure(429, {"retry-after": "90"})

    with pytest.raises(WaitForQuota) as caught:
        router.call(
            pool_name=pool.name,
            job_id=job.id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )

    assert calls == ["profile_a"]
    assert caught.value.reason == "quota_cooldown"
    assert caught.value.next_run_at is not None


def test_reservations_prevent_overbooking_a_shared_window(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(
        scopes={"profile_a": ("google:project",), "profile_b": ("google:project",)},
        limits=(QuotaLimit("google:project", "requests", "minute", 60, 1),),
        max_cycles=1,
    )
    router = ProviderRouter(store, {pool.name: pool})
    first, second = make_job(store, "d"), make_job(store, "e")
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        return MeteredResult("ok", {"requests": 1})

    router.call(pool_name=pool.name, job_id=first.id, item_id=None, invoke=invoke, usage={"requests": 1})
    with pytest.raises(WaitForQuota) as caught:
        router.call(pool_name=pool.name, job_id=second.id, item_id=None, invoke=invoke, usage={"requests": 1})

    assert len(calls) == 1
    assert caught.value.reason == "quota_budget_exhausted"
    assert caught.value.next_run_at is not None


def test_concurrent_calls_reserve_a_shared_budget_only_once(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(
        scopes={"profile_a": ("google:project",), "profile_b": ("google:project",)},
        limits=(QuotaLimit("google:project", "requests", "minute", 60, 1),),
        max_cycles=1,
    )
    router = ProviderRouter(store, {pool.name: pool})
    jobs = [make_job(store, "parallel-a"), make_job(store, "parallel-b")]
    start = threading.Barrier(2)
    calls: list[str] = []

    def execute(job_id: str) -> str:
        start.wait()

        def invoke(profile: str) -> MeteredResult:
            calls.append(profile)
            return MeteredResult("ok", {"requests": 1})

        try:
            return router.call(
                pool_name=pool.name,
                job_id=job_id,
                item_id=None,
                invoke=invoke,
                usage={"requests": 1},
            )
        except WaitForQuota:
            return "waiting_quota"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(execute, (job.id for job in jobs)))

    assert sorted(outcomes) == ["ok", "waiting_quota"]
    assert len(calls) == 1


def test_retry_after_accepts_seconds_and_http_date() -> None:
    now = datetime(2026, 11, 7, tzinfo=UTC)
    numeric = ProviderFailure(429, {"retry-after": "12.5"})
    dated = ProviderFailure(429, {"retry-after": email.utils.format_datetime(now.replace(second=30))})

    assert _retry_after(numeric, now) == 12.5
    assert _retry_after(dated, now) == 30


def test_retry_after_reads_google_retry_info_and_groq_reset_header() -> None:
    now = datetime(2026, 11, 7, tzinfo=UTC)
    google = ProviderFailure(429)
    google.details = [
        {
            "@type": "type.googleapis.com/google.rpc.RetryInfo",
            "retryDelay": {"seconds": 7, "nanos": 500_000_000},
        }
    ]
    groq = ProviderFailure(429, {"x-ratelimit-reset-tokens": "1m30s"})

    assert _retry_after(google, now) == 7.5
    assert _retry_after(groq, now) == 90


def test_interactive_429_is_audited_and_does_not_retry_with_another_profile(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = make_query_pool()
    router = ProviderRouter(store, {pool.name: pool})
    request_id = str(uuid.uuid4())
    calls: list[str] = []

    def invoke(profile: str, _timeout: float) -> MeteredResult:
        calls.append(profile)
        raise ProviderFailure(429, {"Retry-After": "9"})

    result = router.interactive_call(
        request_id=request_id,
        project="sample-project",
        model=pool.model,
        query="лимитированный запрос",
        invoke=invoke,
    )

    assert result["status"] == "waiting_quota"
    assert result["retry_after_seconds"] == 9
    assert calls == ["profile_a"]
    with store.connect() as db:
        request = db.execute(
            "SELECT state,diagnostic_json FROM interactive_requests WHERE id=?",
            (request_id,),
        ).fetchone()
        attempt = db.execute(
            "SELECT http_code,status,retry_after_seconds FROM interactive_attempts WHERE request_id=?",
            (request_id,),
        ).fetchone()
        event_states = {
            row[0]
            for row in db.execute(
                "SELECT DISTINCT state FROM quota_events WHERE request_id=?",
                (request_id,),
            )
        }
        cooldown = db.execute(
            """SELECT available_at FROM quota_buckets
               WHERE scope='google:project-a' AND metric='*' AND window_name='cooldown'"""
        ).fetchone()
    assert request[0] == "waiting_quota"
    assert '"retry_after_seconds":9.0' in request[1]
    assert tuple(attempt) == (429, "failed", 9.0)
    assert event_states == {"consumed"}
    assert cooldown is not None
    with store.connect() as db:
        observation = db.execute("SELECT profile,operation,details_json FROM gemini_quota_observations").fetchone()
    assert tuple(observation[:2]) == ("profile_a", "search")
    assert json.loads(observation[2]) == {"retry_after_seconds": 9.0}


def test_interactive_429_cools_scope_for_next_query_only(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_query_pool()
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def first(profile: str, _timeout: float) -> MeteredResult:
        calls.append(profile)
        raise ProviderFailure(429, {"Retry-After": "60"})

    first_result = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="first", invoke=first,
    )
    second_result = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="second",
        invoke=lambda profile, _timeout: (
            calls.append(profile) or MeteredResult([0.0] * 1536, {"requests": 1})
        ),
    )

    assert first_result["status"] == "waiting_quota"
    assert second_result["status"] == "done"
    assert calls == ["profile_a", "profile_b"]


def test_interactive_profile_retry_cooldown_is_not_reported_as_quota(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    pool = make_query_pool()
    router = ProviderRouter(
        store, {pool.name: pool}, random_uniform=lambda low, _high: low
    )

    def temporary_failure(_profile: str, _timeout: float) -> MeteredResult:
        raise ProviderFailure(503)

    first = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="first network failure",
        invoke=temporary_failure,
    )
    second = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="second network failure",
        invoke=temporary_failure,
    )
    third = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="profiles cooling down",
        invoke=lambda _profile, _timeout: pytest.fail("cooldown must prevent a provider call"),
    )

    assert first["status"] == second["status"] == "failed"
    assert first["reason"] == second["reason"] == "provider_unavailable"
    assert third["status"] == "failed"
    assert third["reason"] == "provider_unavailable"
    with store.connect() as db:
        request = db.execute(
            "SELECT state,diagnostic_json FROM interactive_requests WHERE id=?",
            (third["request_id"],),
        ).fetchone()
    assert request[0] == "failed"
    assert '"category":"provider_unavailable"' in request[1]


def test_interactive_scope_lock_contention_releases_partial_acquisition(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    scopes = {
        "profile_a": ("google:alpha", "google:zeta"),
        "profile_b": ("google:alpha", "google:zeta"),
    }
    pool = make_query_pool(scopes=scopes)
    router = ProviderRouter(store, {pool.name: pool})
    held = router._scope_lock("google:zeta")
    assert held.acquire(blocking=False)

    busy = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="busy", invoke=lambda _profile, _timeout: MeteredResult([], {}),
    )
    held.release()

    first_scope = router._scope_lock("google:alpha")
    assert first_scope.acquire(blocking=False)
    first_scope.release()
    success = router.interactive_call(
        request_id=str(uuid.uuid4()), project="sample-project", model=pool.model,
        query="retry on next search",
        invoke=lambda _profile, _timeout: MeteredResult([0.0] * 1536, {"requests": 1}),
    )
    assert busy["reason"] == "worker_busy"
    assert success["status"] == "done"


def test_interactive_query_observes_job_quota_reservation_in_shared_ledger(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    scopes = {
        "profile_a": ("google:shared-project",),
        "profile_b": ("google:shared-project",),
    }
    limit = QuotaLimit(
        scope="google:shared-project",
        metric="requests",
        window="minute",
        window_seconds=60,
        limit=1,
    )
    pool = make_query_pool(scopes=scopes, limits=(limit,))
    job = make_job(store, "interactive-shared-budget")
    attempt_id = str(uuid.uuid4())
    now = iso_utc()
    with store.transaction() as db:
        db.execute(
            """INSERT INTO attempts
               (id,job_id,profile,provider,model,started_at,status)
               VALUES(?,?,?,'google',?,?,'started')""",
            (attempt_id, job.id, "profile_a", pool.model, now),
        )
        db.execute(
            """INSERT INTO quota_reservations
               (id,attempt_id,job_id,scope,provider,model,metric,window_name,
                window_seconds,amount,created_at,state,expires_at)
               VALUES(?,?,?,?,?,?,?,?,?,1,?,'reserved',?)""",
            (
                str(uuid.uuid4()), attempt_id, job.id, "google:shared-project",
                "google", pool.model, "requests", "minute", 60, now,
                iso_utc(utc_now() + timedelta(days=1)),
            ),
        )

    calls: list[str] = []
    result = ProviderRouter(store, {pool.name: pool}).interactive_call(
        request_id=str(uuid.uuid4()),
        project="sample-project",
        model=pool.model,
        query="shared project quota",
        invoke=lambda profile, _timeout: (
            calls.append(profile) or MeteredResult([0.0] * 1536, {"requests": 1})
        ),
    )

    assert result["status"] == "waiting_quota"
    assert result["reason"] == "quota"
    assert calls == []
    with store.connect() as db:
        assert db.execute(
            "SELECT count(*) FROM quota_ledger WHERE state='reserved'"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM interactive_attempts"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("status", [400, 403, 404, 413, 422])
def test_permanent_http_errors_stop_without_rotating(
    tmp_path: Path, status: int
) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, f"permanent-{status}")
    router = ProviderRouter(store, {pool.name: pool})
    calls: list[str] = []

    def invoke(profile: str):
        calls.append(profile)
        raise ProviderFailure(status)

    with pytest.raises(PermanentProviderError) as caught:
        router.call(
            pool_name=pool.name,
            job_id=job.id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )

    assert caught.value.status_code == status
    assert calls == ["profile_a"]


def test_timeout_profile_is_put_in_cooldown(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, "f")
    router = ProviderRouter(store, {pool.name: pool}, random_uniform=lambda low, high: low)

    def invoke(_profile: str):
        raise TimeoutError("client timeout")

    with pytest.raises(WaitForQuota):
        router.call(
            pool_name=pool.name,
            job_id=job.id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )
    with store.connect() as db:
        row = db.execute("SELECT cooldowns_json FROM routing_state WHERE pool_name=?", (pool.name,)).fetchone()
    assert '"profile_a"' in row[0]
    attempts = store.attempts_summary(job.id)["items"]
    assert attempts[0]["outcome_uncertain"]


def test_server_failures_have_retry_cooldown_distinct_from_quota_wait(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(max_cycles=1)
    job = make_job(store, "g")
    router = ProviderRouter(store, {pool.name: pool}, random_uniform=lambda low, high: low)

    def invoke(_profile: str):
        raise ProviderFailure(503)

    with pytest.raises(WaitForQuota) as caught:
        router.call(
            pool_name=pool.name,
            job_id=job.id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )
    assert caught.value.reason in {"profile_retry_cooldown", "profile_cycles_exhausted"}


def test_shared_quota_scope_serializes_api_requests(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    pool = make_pool(
        scopes={"profile_a": ("google:shared",), "profile_b": ("google:shared",)}
    )
    router = ProviderRouter(store, {pool.name: pool})
    jobs = [make_job(store, "h"), make_job(store, "i")]
    guard = threading.Lock()
    active = 0
    peak = 0

    def execute(job_id: str) -> str:
        def invoke(_profile: str) -> MeteredResult:
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
            time.sleep(0.03)
            with guard:
                active -= 1
            return MeteredResult("ok", {"requests": 1})

        return router.call(
            pool_name=pool.name,
            job_id=job_id,
            item_id=None,
            invoke=invoke,
            usage={"requests": 1},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(execute, (job.id for job in jobs)))

    assert results == ["ok", "ok"]
    assert peak == 1
