from __future__ import annotations

import json
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest

from kiber_analyst import gemini_quota
from kiber_analyst.embedding_api import _safe_provider_error
from kiber_analyst.jobqueue import JobStore, iso_utc, utc_now

METRIC = "generativelanguage.googleapis.com/embed_content_requests"
PRIVATE = "Bearer TEST_SECRET https://example.invalid/?key=TEST_SECRET"


class Gemini429(Exception):
    code = 429

    def __init__(self, details):
        super().__init__(PRIVATE)
        self.details = {"error": {"message": PRIVATE, "details": details}}


def violation():
    return {"@type": "type.googleapis.com/google.rpc.QuotaFailure", "violations": [
        {"quotaMetric": METRIC, "quotaValue": "40", "description": PRIVATE,
         "quotaDimensions": {"project": PRIVATE}},
        {"quotaMetric": "https://example.invalid/?key=TEST_SECRET", "quotaValue": "10"},
    ]}


def retry():
    return {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "13.25s"}


def store(tmp_path):
    value = JobStore(tmp_path / "queue", require_mount=False)
    value.initialize()
    return value


@pytest.mark.parametrize("details,expected", [
    ([violation(), retry()], {"quota_metric": METRIC, "quota_value": 40, "retry_delay": 13.25}),
    ([violation()], {"quota_metric": METRIC, "quota_value": 40}),
    ([{"@type": "type.googleapis.com/google.rpc.RetryInfo",
       "retryDelay": {"seconds": "7", "nanos": 500_000_000}}], {"retry_delay": 7.5}),
])
def test_google_error_is_reduced_to_safe_fields(details, expected, tmp_path):
    clean = _safe_provider_error(Gemini429(details))
    assert clean.quota_diagnostic == expected
    assert PRIVATE not in str(clean)
    db = store(tmp_path)
    with db.transaction() as conn:
        gemini_quota.record_429(conn, profile="gemini_embed", operation="embed", at=iso_utc(),
                                detail=clean.quota_diagnostic, retry_after=clean.retry_after_seconds)
        row = conn.execute("SELECT profile,operation,http_status,details_json FROM gemini_quota_observations").fetchone()
    assert tuple(row[:3]) == ("gemini_embed", "embed", 429)
    assert "TEST_SECRET" not in row[3]
    assert json.loads(row[3]).get("retry_delay") == expected.get("retry_delay")


def test_two_profiles_and_project_overlap(tmp_path):
    db = store(tmp_path)
    now = utc_now()
    profiles = {"one": ("google:one",), "two": ("google:two",), "peer": ("google:peer",)}
    first = gemini_quota.project_scope_hash("sample-project")
    second = gemini_quota.project_scope_hash("other-project")
    assert first == gemini_quota.project_scope_hash("sample-project") and first != second
    assert gemini_quota.project_scope_hash(PRIVATE) == "unknown"
    with db.transaction() as conn:
        conn.execute("SELECT 1")
        gemini_quota.record_429(conn, profile="one", operation="embed", at=iso_utc(now),
                                detail={"quota_metric": METRIC, "retry_delay": 32})
        gemini_quota.record_429(conn, profile="two", operation="search", at=iso_utc(now-timedelta(seconds=1)),
                                detail={"quota_metric": METRIC})
        result = gemini_quota.profile_windows(conn, profiles, {"one": first, "peer": first, "two": second}, now)
    assert result["one"]["last_429_at"] and result["two"]["last_429_at"]
    assert result["one"]["last_retry_after"] == 32 and result["two"]["last_retry_after"] is None
    assert result["one"]["next_probe_at"] == result["peer"]["next_probe_at"]
    assert not result["peer"]["probe_allowed"] and result["two"]["probe_allowed"]


def test_probe_uses_fixed_provider_path_once_even_after_transport_error(tmp_path, monkeypatch):
    db = store(tmp_path)
    profile = "gemini_embed"
    pool = SimpleNamespace(provider="google", profiles=(profile,), scopes_for=lambda name: (f"google:{name}",))
    worker = SimpleNamespace(store=db, pools={profile: pool}, router=SimpleNamespace(api_slots=threading.Semaphore(1)))
    monkeypatch.setattr(gemini_quota, "profile_hashes", lambda profiles: {profile: "unknown"})
    calls = []

    def unavailable(profile, contents, *, timeout):
        calls.append((profile, contents, timeout))
        raise OSError(PRIVATE)

    first = gemini_quota.probe(worker, unavailable)
    assert first["error_code"] == "PROXY_OR_NETWORK_UNAVAILABLE"
    assert gemini_quota.probe(worker, unavailable)["status"] == "refused"
    assert calls == [(profile, ["quota probe"], 10)]
    assert "TEST_SECRET" not in json.dumps(first)


def test_probe_respects_provider_retry_info_and_global_gate(tmp_path, monkeypatch):
    db = store(tmp_path)
    profile = "gemini_embed"
    pool = SimpleNamespace(provider="google", profiles=(profile,), scopes_for=lambda name: (f"google:{name}",))
    worker = SimpleNamespace(store=db, pools={profile: pool}, router=SimpleNamespace(api_slots=threading.Semaphore(1)))
    monkeypatch.setattr(gemini_quota, "profile_hashes", lambda profiles: {profile: "unknown"})

    def limited(*args, **kwargs):
        raise Gemini429([violation(), retry()])

    hit = gemini_quota.probe(worker, limited)
    assert (hit["http_status"], hit["quota_metric"], hit["retry_delay"]) == (429, METRIC, 13.25)
    with db.transaction() as conn:
        assert gemini_quota.reserve_probe(conn, ["other_profile"], utc_now() + timedelta(seconds=299)) is None
        assert gemini_quota.reserve_probe(conn, ["other_profile"], utc_now() + timedelta(seconds=301)) == "other_profile"


def test_project_ids_are_hashed_from_config_without_key_access(tmp_path):
    config = tmp_path / "providers.yaml"
    config.write_text("""profiles:
  one: {provider: google, key_env: FIRST, proxy_env: PROXY, project_id: shared-project}
  two: {provider: google, key_env: SECOND, proxy_env: PROXY, project_id: shared-project}
  three: {provider: google, key_env: THIRD, proxy_env: PROXY, project_id: other-project}
  four: {provider: google, key_env: FOURTH, proxy_env: PROXY}
""")
    hashes = gemini_quota.profile_hashes(("one", "two", "three", "four"), config)
    assert hashes["one"] == hashes["two"] != hashes["three"]
    assert hashes["four"] == "unknown"
    assert "shared-project" not in hashes.values()


def test_probe_refuses_while_job_is_waiting(tmp_path, monkeypatch):
    db = store(tmp_path)
    profile = "gemini_embed"
    pool = SimpleNamespace(provider="google", profiles=(profile,), scopes_for=lambda name: (f"google:{name}",))
    worker = SimpleNamespace(store=db, pools={profile: pool}, router=SimpleNamespace(api_slots=threading.Semaphore(1)))
    monkeypatch.setattr(gemini_quota, "profile_hashes", lambda profiles: {profile: "unknown"})
    job, _ = db.create_job(operation="embed", command={}, fingerprint="f", idempotency_key="test")
    db.claim()
    db.postpone(job.id, "waiting_retry", utc_now()+timedelta(minutes=10), {"reason": "profile_retry_cooldown"})

    def unexpected(*args, **kwargs):
        pytest.fail("probe called the provider while an embedding job was waiting")

    assert gemini_quota.probe(worker, unexpected)["status"] == "refused"
