"""Allowlisted Gemini quota diagnostics and a durable, opt-in probe gate."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re

_METRIC = re.compile(r"(?:generativelanguage|aiplatform)\.googleapis\.com/[a-z][a-z0-9_/]{0,119}")
_PROJECT = re.compile(r"(?:[a-z][a-z0-9-]{4,29}|[0-9]{6,20})")
_TYPE = "type.googleapis.com/google.rpc."
PROBE_INTERVAL = 300

SCHEMA = """
CREATE TABLE IF NOT EXISTS gemini_quota_observations (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    profile TEXT NOT NULL,
    operation TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    http_status INTEGER NOT NULL CHECK(http_status=429),
    details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gemini_quota_profile_seq
    ON gemini_quota_observations(profile, seq);
CREATE TABLE IF NOT EXISTS gemini_probe_gate (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    profile TEXT NOT NULL, started_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gemini_probe_profiles (
    profile TEXT PRIMARY KEY, last_probe_at TEXT NOT NULL
);
"""


def project_scope_hash(project_id):
    if not isinstance(project_id, str) or not _PROJECT.fullmatch(project_id):
        return "unknown"
    return hashlib.sha256(project_id.encode()).hexdigest()


def _quantity(value, maximum):
    if isinstance(value, str):
        if not re.fullmatch(r"[0-9]{1,19}", value):
            return None
        value = int(value)
    if type(value) is int and 0 <= value <= maximum:
        return value
    return None


def _duration(duration):
    if isinstance(duration, str) and re.fullmatch(r"[0-9]{1,8}(?:\.[0-9]{1,9})?s", duration):
        return float(duration[:-1])
    if isinstance(duration, dict):
        seconds = _quantity(duration.get("seconds", 0), 99_999_999)
        nanos = _quantity(duration.get("nanos", 0), 999_999_999)
        if seconds is not None and nanos is not None:
            return seconds + nanos / 1_000_000_000
    return None


def quota_details(exc):
    raw = getattr(exc, "details", None)
    if isinstance(raw, dict):
        error = raw.get("error", raw)
        raw = error.get("details") if isinstance(error, dict) else None
    if not isinstance(raw, list):
        return {}
    violations = []
    delays = []
    for detail in raw[:32]:
        if not isinstance(detail, dict):
            continue
        if detail.get("@type") == _TYPE + "RetryInfo":
            delay = _duration(detail.get("retryDelay"))
            if delay is not None:
                delays.append(delay)
        if detail.get("@type") != _TYPE + "QuotaFailure":
            continue
        rows = detail.get("violations")
        if not isinstance(rows, list):
            continue
        for row in rows[:16]:
            metric = row.get("quotaMetric") if isinstance(row, dict) else None
            if isinstance(metric, str) and _METRIC.fullmatch(metric):
                value = {"quota_metric": metric}
                limit = _quantity(row.get("quotaValue"), 2**63 - 1)
                if limit is not None:
                    value["quota_value"] = limit
                violations.append(value)
    result = dict(violations[0]) if violations else {}
    if len(violations) > 1:
        result["violations"] = violations[:16]
    if delays:
        result["retry_delay"] = max(delays)
    return result


def record_429(db, *, profile, operation, at, detail, retry_after=None):
    values = dict(detail or {})
    if type(retry_after) in (int, float) and math.isfinite(retry_after) and retry_after >= 0:
        values["retry_after_seconds"] = retry_after
    db.execute(
        "INSERT INTO gemini_quota_observations(profile,operation,occurred_at,http_status,details_json) "
        "VALUES(?,?,?,429,?)", (profile, operation, at, json.dumps(values, separators=(",", ":"))),
    )


def reserve_probe(db, candidates, now):
    """Persist the global gate before the one allowed network request."""
    last = db.execute("SELECT started_at FROM gemini_probe_gate WHERE singleton=1").fetchone()
    if last and dt.datetime.fromisoformat(last[0]) + dt.timedelta(seconds=PROBE_INTERVAL) > now:
        return None
    if not candidates:
        return None
    profile = candidates[0]
    stamp = now.isoformat(timespec="microseconds")
    db.execute("INSERT OR REPLACE INTO gemini_probe_gate VALUES(1,?,?)", (profile, stamp))
    db.execute("INSERT OR REPLACE INTO gemini_probe_profiles VALUES(?,?)", (profile, stamp))
    return profile


def profile_hashes(profiles, profiles_path=None):
    from kiber_analyst.provider_http import PROFILES_PATH, ProfileError, _profiles

    try:
        specs = _profiles(profiles_path or PROFILES_PATH)
    except ProfileError:
        specs = {}
    return {profile: project_scope_hash(specs.get(profile, {}).get("project_id"))
            if isinstance(specs.get(profile), dict) else "unknown" for profile in profiles}


def profile_windows(db, profiles, hashes, now):
    """Combine the observed wait, the routing bounds and the durable probe gate."""
    last_probe = db.execute("SELECT started_at FROM gemini_probe_gate WHERE singleton=1").fetchone()
    gate = dt.datetime.fromisoformat(last_probe[0]) + dt.timedelta(seconds=PROBE_INTERVAL) if last_probe else now
    active = db.execute(
        "SELECT 1 FROM jobs WHERE state IN ('queued','running','waiting_retry','waiting_quota') LIMIT 1"
    ).fetchone() is not None
    cooldowns = {}
    disabled = set()
    for (cooldown_json,) in db.execute("SELECT cooldowns_json FROM routing_state"):
        for profile, until in json.loads(cooldown_json).items():
            cooldowns[profile] = max(cooldowns.get(profile, now), dt.datetime.fromisoformat(until))
    # A disabled profile in an unrelated pool must not hide an eligible embedding profile.
    row = db.execute("SELECT disabled_profiles_json FROM routing_state WHERE pool_name='gemini_embed'").fetchone()
    if row:
        disabled.update(json.loads(row[0]))
    result = {}
    for profile, scopes in profiles.items():
        latest = db.execute(
            "SELECT occurred_at,details_json FROM gemini_quota_observations WHERE profile=? ORDER BY seq DESC LIMIT 1",
            (profile,),
        ).fetchone()
        when, info = (latest[0], json.loads(latest[1])) if latest else (None, {})
        legacy = db.execute(
            """SELECT finished_at,retry_info_json FROM attempts WHERE profile=? AND http_code=429
               AND finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1""", (profile,),
        ).fetchone()
        interactive = db.execute(
            """SELECT finished_at FROM interactive_attempts WHERE profile=? AND http_code=429
               AND finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1""", (profile,),
        ).fetchone()
        if legacy and (when is None or legacy[0] > when):
            when, info = legacy[0], json.loads(legacy[1])
        if interactive and (when is None or interactive[0] > when):
            when, info = interactive[0], {}
        after = info.get("retry_delay", info.get("retry_after_seconds"))
        until = max(gate, cooldowns.get(profile, now))
        if when and type(after) in (int, float) and math.isfinite(after) and 0 <= after <= 99_999_999:
            until = max(until, dt.datetime.fromisoformat(when) + dt.timedelta(seconds=after))
        for scope in scopes:
            row = db.execute(
                "SELECT max(available_at) FROM quota_buckets WHERE provider='google' AND scope=?", (scope,),
            ).fetchone()
            if row and row[0]:
                until = max(until, dt.datetime.fromisoformat(row[0]))
        result[profile] = {
            "last_429_at": when, "last_retry_after": after,
            "quota_metric": info.get("quota_metric", "unknown"), "quota_value": info.get("quota_value"),
            "project_scope_hash": hashes[profile], "_until": until,
        }
    for profile, entry in result.items():
        digest = hashes[profile]
        if digest != "unknown":
            entry["_until"] = max(peer["_until"] for name, peer in result.items() if hashes[name] == digest)
    for profile, entry in result.items():
        until = entry.pop("_until")
        entry["next_probe_at"] = until.isoformat() if until > now else None
        entry["probe_allowed"] = not active and until <= now and profile not in disabled
    return result


def probe(worker, invoke):
    """Use the embedding profile transport once. Never retry or change profiles after the call."""
    from kiber_analyst.jobqueue import iso_utc, utc_now
    from kiber_analyst.routing import _retry_after, _status

    pool = worker.pools.get("gemini_embed")
    if pool is None or pool.provider != "google":
        return {"status": "refused", "error_code": "PROBE_NOT_AVAILABLE"}
    profiles = {p: pool.scopes_for(p) for p in pool.profiles}
    hashes = profile_hashes(profiles)
    if not worker.router.api_slots.acquire(blocking=False):
        return {"status": "refused", "error_code": "PROBE_NOT_AVAILABLE"}
    try:
        now = utc_now()
        with worker.store.transaction() as db:
            health = profile_windows(db, profiles, hashes, now)
            previous = dict(db.execute("SELECT profile,last_probe_at FROM gemini_probe_profiles"))
            allowed = sorted((p for p in profiles if health[p]["probe_allowed"]),
                             key=lambda p: (previous.get(p, ""), p))
            selected = reserve_probe(db, allowed, now)
        if selected is None:
            return {"status": "refused", "error_code": "PROBE_NOT_AVAILABLE"}
        try:
            invoke(selected, ["quota probe"], timeout=10)
        except Exception as exc:
            code = _status(exc)
            if code == 429:
                detail = getattr(exc, "quota_diagnostic", None) or quota_details(exc)
                delay = _retry_after(exc, utc_now())
                with worker.store.transaction() as db:
                    record_429(db, profile=selected, operation="quota_probe", at=iso_utc(),
                               detail=detail, retry_after=delay)
                return {"status": "rate_limited", "profile": selected, "http_status": 429,
                        "quota_metric": detail.get("quota_metric", "unknown"),
                        "quota_value": detail.get("quota_value"), "retry_delay": detail.get("retry_delay"),
                        "retry_after_seconds": delay, "project_scope_hash": hashes[selected]}
            return {"status": "error", "profile": selected, "http_status": code,
                    "error_code": "PROXY_OR_NETWORK_UNAVAILABLE" if code is None else "PROVIDER_HTTP_ERROR"}
        return {"status": "ok", "profile": selected}
    finally:
        worker.router.api_slots.release()
