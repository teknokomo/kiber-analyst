from __future__ import annotations

import email.utils
import hashlib
import json
import math
import random
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.errors import IngestError
from kiber_analyst.jobqueue import JobStore, canonical_json, iso_utc, parse_utc, utc_now
from kiber_analyst.provider_http import ProfileError

DEFAULT_ROUTING_PATH = config_path("provider-routing.yaml")
TERMINAL_CODES = {400, 403, 404, 413, 422}
RETRY_CODES = {408, 429, 500, 502, 503, 504}


class RoutingError(IngestError):
    """Safe routing or provider execution error."""


class WaitForQuota(RoutingError):
    def __init__(self, *, next_run_at: datetime | None, scope: str, reason: str) -> None:
        super().__init__(reason)
        self.next_run_at = next_run_at
        self.scope = scope
        self.reason = reason


class PermanentProviderError(RoutingError):
    def __init__(self, message: str, *, status_code: int | None = None, category: str = "provider_error") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.category = category


@dataclass(frozen=True)
class MeteredResult:
    value: Any
    usage: dict[str, float]


@dataclass(frozen=True)
class QuotaLimit:
    scope: str
    metric: str
    window: str
    window_seconds: int
    limit: float


@dataclass(frozen=True)
class PoolConfig:
    name: str
    operation: str
    provider: str
    model: str
    profiles: tuple[str, ...]
    strategy: str
    weights: dict[str, int]
    quota_scopes: dict[str, tuple[str, ...]]
    limits: tuple[QuotaLimit, ...]
    max_cycles: int
    retry_initial_seconds: float
    retry_max_seconds: float
    retry_jitter_fraction: float

    def scope_for(self, profile: str) -> str:
        return self.scopes_for(profile)[0]

    def scopes_for(self, profile: str) -> tuple[str, ...]:
        return self.quota_scopes.get(profile, (f"{self.provider}:profile:{profile}",))


def load_pools(
    path: Path = DEFAULT_ROUTING_PATH,
) -> dict[str, PoolConfig]:
    try:
        import yaml
    except ImportError:
        raise RoutingError("Provider routing requires PyYAML") from None
    class UniqueLoader(yaml.SafeLoader):
        pass

    def mapping(loader: Any, node: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            if not isinstance(key, str) or key in result:
                raise RoutingError("Invalid or duplicate provider routing entry")
            result[key] = loader.construct_object(value_node)
        return result

    UniqueLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping
    )
    try:
        with path.open(encoding="utf-8") as stream:
            raw = yaml.load(stream, Loader=UniqueLoader)
    except RoutingError:
        raise
    except (OSError, UnicodeError, TypeError, yaml.YAMLError):
        raise RoutingError("Cannot read provider routing configuration") from None
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "pools"}
        or type(raw["version"]) is not int
        or raw["version"] != 1
    ):
        raise RoutingError("Invalid provider routing configuration")
    entries = raw["pools"]
    if not isinstance(entries, dict) or not entries:
        raise RoutingError("Provider routing configuration has no pools")
    result: dict[str, PoolConfig] = {}
    for name, item in entries.items():
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name):
            raise RoutingError("Invalid pool name")
        expected = {
            "operation", "provider", "model", "profiles", "strategy", "weights",
            "quota_scopes", "limits", "retry",
        }
        if not isinstance(item, dict) or set(item) != expected:
            raise RoutingError(f"Pool {name} is incomplete or has unknown settings")
        operation, provider, model = item["operation"], item["provider"], item["model"]
        profiles = item["profiles"]
        strategy = item["strategy"]
        if not isinstance(operation, str) or operation not in {"telegram_asr", "embed", "asr_embed", "search"}:
            raise RoutingError(f"Pool {name} has unsupported operation")
        if (
            not isinstance(provider, str)
            or provider not in {"groq", "google"}
            or not isinstance(model, str)
            or not model.strip()
        ):
            raise RoutingError(f"Pool {name} has invalid provider/model")
        if (
            not isinstance(profiles, list)
            or not profiles
            or any(not isinstance(profile, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", profile) for profile in profiles)
            or len(set(profiles)) != len(profiles)
        ):
            raise RoutingError(f"Pool {name} must list unique profiles")
        if not isinstance(strategy, str) or strategy not in {
            "round_robin", "failover", "weighted_round_robin"
        }:
            raise RoutingError(f"Pool {name} has an unsupported strategy")
        weights = item["weights"]
        if (
            not isinstance(weights, dict)
            or set(weights) - set(profiles)
            or (strategy == "weighted_round_robin" and set(weights) != set(profiles))
            or any(type(value) is not int or value <= 0 for value in weights.values())
        ):
            raise RoutingError(f"Pool {name} has invalid weights")
        scopes = item["quota_scopes"]
        if not isinstance(scopes, dict) or set(scopes) - set(profiles):
            raise RoutingError(f"Pool {name} has invalid quota scopes")
        normalized_scopes: dict[str, tuple[str, ...]] = {}
        for profile, value in scopes.items():
            values = [value] if isinstance(value, str) else value
            if not isinstance(values, list) or not values or any(not isinstance(scope, str) or not scope.strip() for scope in values):
                raise RoutingError(f"Pool {name} has invalid quota scope mapping")
            normalized_scopes[profile] = tuple(dict.fromkeys(values))
        limits: list[QuotaLimit] = []
        limit_keys: set[tuple[str, str, str]] = set()
        known_scopes = {scope for values in normalized_scopes.values() for scope in values}
        if not isinstance(item["limits"], list):
            raise RoutingError(f"Pool {name} limits must be a list")
        for limit in item["limits"]:
            if not isinstance(limit, dict) or set(limit) != {"scope", "metric", "window", "window_seconds", "limit"}:
                raise RoutingError(f"Pool {name} has an invalid quota limit")
            if not isinstance(limit["metric"], str) or limit["metric"] not in {
                "requests", "items", "tokens", "audio_seconds"
            }:
                raise RoutingError(f"Pool {name} has an unknown quota metric")
            if (
                not isinstance(limit["scope"], str)
                or limit["scope"] not in known_scopes
                or not isinstance(limit["window"], str)
                or not limit["window"].strip()
            ):
                raise RoutingError(f"Pool {name} has an invalid quota scope/window")
            if type(limit["window_seconds"]) is not int or limit["window_seconds"] <= 0:
                raise RoutingError(f"Pool {name} window_seconds must be positive")
            if (
                isinstance(limit["limit"], bool)
                or not isinstance(limit["limit"], (int, float))
                or not math.isfinite(limit["limit"])
                or limit["limit"] <= 0
            ):
                raise RoutingError(f"Pool {name} quota limits must be positive")
            key = (limit["scope"], limit["metric"], limit["window"])
            if key in limit_keys:
                raise RoutingError(f"Pool {name} has duplicate quota limits")
            limit_keys.add(key)
            limits.append(QuotaLimit(**limit))
        retry = item["retry"]
        if not isinstance(retry, dict) or set(retry) != {"max_cycles", "initial_seconds", "max_seconds", "jitter_fraction"}:
            raise RoutingError(f"Pool {name} has invalid retry settings")
        if type(retry["max_cycles"]) is not int or not 1 <= retry["max_cycles"] <= 20:
            raise RoutingError(f"Pool {name} max_cycles must be 1..20")
        if (
            isinstance(retry["initial_seconds"], bool)
            or isinstance(retry["max_seconds"], bool)
            or not isinstance(retry["initial_seconds"], (int, float))
            or not isinstance(retry["max_seconds"], (int, float))
            or not math.isfinite(retry["initial_seconds"])
            or not math.isfinite(retry["max_seconds"])
            or not 0 < retry["initial_seconds"] <= retry["max_seconds"] <= 3600
        ):
            raise RoutingError(f"Pool {name} has invalid backoff bounds")
        if (
            isinstance(retry["jitter_fraction"], bool)
            or not isinstance(retry["jitter_fraction"], (int, float))
            or not math.isfinite(retry["jitter_fraction"])
            or not 0 <= retry["jitter_fraction"] <= 0.2
        ):
            raise RoutingError(f"Pool {name} jitter must be 0..0.2")
        result[name] = PoolConfig(
            name=name,
            operation=operation,
            provider=provider,
            model=model,
            profiles=tuple(profiles),
            strategy=strategy,
            weights=dict(weights),
            quota_scopes=normalized_scopes,
            limits=tuple(limits),
            max_cycles=retry["max_cycles"],
            retry_initial_seconds=float(retry["initial_seconds"]),
            retry_max_seconds=float(retry["max_seconds"]),
            retry_jitter_fraction=float(retry["jitter_fraction"]),
        )
    return result


def _state(db: sqlite3.Connection, pool: str) -> sqlite3.Row | None:
    return db.execute("SELECT * FROM routing_state WHERE pool_name=?", (pool,)).fetchone()


def _persist_state(
    db: sqlite3.Connection,
    pool: str,
    *,
    cursor: int | None = None,
    weighted: dict[str, int] | None = None,
    disabled: list[str] | None = None,
    cooldowns: dict[str, str] | None = None,
) -> None:
    row = _state(db, pool)
    values = {
        "cursor": int(row["cursor_index"]) if row and cursor is None else (cursor or 0),
        "weighted": json.loads(row["weighted_current_json"]) if row and weighted is None else (weighted or {}),
        "disabled": json.loads(row["disabled_profiles_json"]) if row and disabled is None else (disabled or []),
        "cooldowns": json.loads(row["cooldowns_json"]) if row and cooldowns is None else (cooldowns or {}),
    }
    db.execute(
        """INSERT INTO routing_state
           (pool_name,cursor_index,weighted_current_json,disabled_profiles_json,cooldowns_json,updated_at)
           VALUES(?,?,?,?,?,?) ON CONFLICT(pool_name) DO UPDATE SET
             cursor_index=excluded.cursor_index,
             weighted_current_json=excluded.weighted_current_json,
             disabled_profiles_json=excluded.disabled_profiles_json,
             cooldowns_json=excluded.cooldowns_json,
             updated_at=excluded.updated_at""",
        (pool, values["cursor"], canonical_json(values["weighted"]), canonical_json(values["disabled"]), canonical_json(values["cooldowns"]), iso_utc()),
    )


def _quota_available_at(db: sqlite3.Connection, pool: PoolConfig, profile: str, now: datetime) -> datetime | None:
    wakes: list[datetime] = []
    for scope in pool.scopes_for(profile):
        row = db.execute(
            """SELECT available_at FROM quota_buckets WHERE scope=? AND provider=? AND model=?
               AND metric='*' AND window_name='cooldown'""",
            (scope, pool.provider, pool.model),
        ).fetchone()
        if row and row[0]:
            try:
                at = parse_utc(row[0])
                if at > now:
                    wakes.append(at)
            except ValueError:
                pass
    return max(wakes) if wakes else None


def select_profile(
    store: JobStore,
    pool: PoolConfig,
    *,
    explicit_profile: str | None = None,
    strategy_override: str | None = None,
    exclude_profiles: set[str] | None = None,
    now: datetime | None = None,
) -> tuple[str | None, datetime | None, str]:
    moment = now or utc_now()
    strategy = strategy_override or pool.strategy
    if strategy not in {"round_robin", "failover", "weighted_round_robin"}:
        raise RoutingError("Unsupported routing strategy")
    if strategy == "weighted_round_robin" and set(pool.weights) != set(pool.profiles):
        raise RoutingError("Weighted routing requires a positive weight for every profile")
    candidates = (explicit_profile,) if explicit_profile else pool.profiles
    if explicit_profile and explicit_profile not in pool.profiles:
        raise RoutingError("Explicit profile must belong to the selected pool")
    with store.transaction() as db:
        row = _state(db, pool.name)
        cursor = int(row["cursor_index"]) if row else 0
        weighted = json.loads(row["weighted_current_json"]) if row else {}
        disabled = set(json.loads(row["disabled_profiles_json"])) if row else set()
        cooldowns = json.loads(row["cooldowns_json"]) if row else {}
        eligible: list[str] = []
        profile_wakes: list[datetime] = []
        scope_wakes: list[datetime] = []
        for profile in candidates:
            excluded = bool(exclude_profiles and profile in exclude_profiles)
            if profile in disabled:
                continue
            profile_until = cooldowns.get(profile)
            if profile_until:
                try:
                    until = parse_utc(profile_until)
                    if until > moment:
                        profile_wakes.append(until)
                        continue
                except ValueError:
                    pass
            scope_until = _quota_available_at(db, pool, profile, moment)
            if scope_until:
                scope_wakes.append(scope_until)
                continue
            if excluded:
                continue
            eligible.append(profile)
        if not eligible:
            _persist_state(db, pool.name, cursor=cursor, weighted=weighted, disabled=list(disabled), cooldowns=cooldowns)
            if all(profile in disabled for profile in candidates):
                return None, None, "all_profiles_disabled"
            all_wakes = profile_wakes + scope_wakes
            if all_wakes:
                earliest = min(all_wakes)
                if earliest in scope_wakes:
                    return None, earliest, "quota_cooldown"
                return None, earliest, "profile_retry_cooldown"
            if exclude_profiles and all(
                profile in exclude_profiles or profile in disabled for profile in candidates
            ):
                return None, None, "cycle_exhausted"
            return None, None, "no_profile_available"
        if explicit_profile or strategy == "failover":
            chosen = eligible[0]
        elif strategy == "round_robin":
            start = cursor % len(pool.profiles)
            chosen = next(pool.profiles[(start + offset) % len(pool.profiles)] for offset in range(len(pool.profiles)) if pool.profiles[(start + offset) % len(pool.profiles)] in eligible)
            cursor = (pool.profiles.index(chosen) + 1) % len(pool.profiles)
        else:
            weights = {profile: pool.weights[profile] for profile in eligible}
            for profile in eligible:
                weighted[profile] = weighted.get(profile, 0) + weights[profile]
            chosen = max(eligible, key=lambda profile: (weighted[profile], -pool.profiles.index(profile)))
            weighted[chosen] -= sum(weights.values())
            cursor = (pool.profiles.index(chosen) + 1) % len(pool.profiles)
        _persist_state(db, pool.name, cursor=cursor, weighted=weighted, disabled=list(disabled), cooldowns=cooldowns)
        return chosen, None, "available"


def _retry_after(exc: Exception, now: datetime) -> float | None:
    from kiber_analyst.gemini_quota import quota_details

    structured = getattr(exc, "quota_diagnostic", None) or quota_details(exc)
    if structured.get("retry_delay") is not None:
        return structured["retry_delay"]
    direct = getattr(exc,"retry_after_seconds",None)
    if isinstance(direct,(int,float)) and direct >= 0:
        return float(direct)
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("retry-after") or headers.get("Retry-After")
    if isinstance(value, str):
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return max(0.0, (parsed - now).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    reset_headers=[]
    for key,header_value in headers.items():
        normalized=str(key).lower()
        if not normalized.startswith("x-ratelimit-reset-") or not isinstance(header_value,str):
            continue
        seconds=_duration_seconds(header_value)
        if seconds is not None:
            reset_headers.append((normalized,seconds))
    if reset_headers:
        error_text=str(exc).lower()
        if "token" in error_text:
            matching=[seconds for name,seconds in reset_headers if "token" in name]
        elif "request" in error_text:
            matching=[seconds for name,seconds in reset_headers if "request" in name]
        else:
            matching=[]
        return max(matching or [seconds for _name,seconds in reset_headers])
    text = str(exc)
    match = re.search(r"retryDelay\s*[:=]\s*[\"']?(\d+(?:\.\d+)?)s", text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    details = getattr(exc,"details",None)
    if isinstance(details,list):
        for detail in details:
            if not isinstance(detail,dict) or not str(detail.get("@type","")).endswith("RetryInfo"):
                continue
            duration=detail.get("retryDelay")
            if isinstance(duration,dict):
                seconds=duration.get("seconds",0)
                nanos=duration.get("nanos",0)
                if isinstance(seconds,(int,float)) and isinstance(nanos,(int,float)):
                    return max(0.0,float(seconds)+float(nanos)/1_000_000_000)
    return None


def _duration_seconds(value: str) -> float | None:
    stripped=value.strip().lower()
    try:
        return max(0.0,float(stripped))
    except ValueError:
        pass
    parts=re.findall(r"(\d+(?:\.\d+)?)(d|h|m|s)",stripped)
    if not parts or "".join(number+unit for number,unit in parts)!=stripped:
        return None
    factors={"d":86400,"h":3600,"m":60,"s":1}
    return sum(float(number)*factors[unit] for number,unit in parts)


def _status(exc: Exception) -> int | None:
    for candidate in (getattr(exc, "status_code", None), getattr(exc, "code", None), getattr(getattr(exc, "response", None), "status_code", None)):
        try:
            value = int(candidate)
        except (TypeError, ValueError):
            continue
        if 100 <= value <= 599:
            return value
    return None


def _is_timeout(exc: Exception) -> bool:
    return isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower()


def _safe_error(exc: Exception) -> str:
    status = _status(exc)
    if status is not None:
        return f"HTTP {status} ({type(exc).__name__})"
    return type(exc).__name__


class ProviderRouter:
    def __init__(
        self,
        store: JobStore,
        pools: dict[str, PoolConfig],
        *,
        random_uniform: Callable[[float, float], float] = random.uniform,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.pools = pools
        self.random_uniform = random_uniform
        self.sleep = sleep
        self._scope_locks: dict[str, threading.Lock] = {}
        self._scope_locks_guard = threading.Lock()
        self.api_slots = threading.BoundedSemaphore(2)

    def begin_interactive_request(
        self, request_id: str, project: str, model: str, query: str
    ) -> bool:
        now=iso_utc()
        digest=hashlib.sha256(query.encode("utf-8")).hexdigest()
        with self.store.transaction() as db:
            existing=db.execute("SELECT id FROM interactive_requests WHERE id=?",(request_id,)).fetchone()
            if existing:
                return False
            db.execute(
                """INSERT INTO interactive_requests(id,project_slug,model,query_sha256,state,created_at,updated_at)
                   VALUES(?,?,?,?, 'running',?,?)""",
                (request_id,project,model,digest,now,now),
            )
        return True

    def _scope_lock(self, scope: str) -> threading.Lock:
        with self._scope_locks_guard:
            return self._scope_locks.setdefault(scope,threading.Lock())

    def _reserve_interactive(
        self,
        pool: PoolConfig,
        profile: str,
        request_id: str,
        usage: dict[str, float],
    ) -> tuple[str | None, datetime | None]:
        now = utc_now()
        scopes = pool.scopes_for(profile)
        attempt_id = str(uuid.uuid4())
        configured = [limit for limit in pool.limits if limit.scope in scopes]
        reservations: list[tuple[str,str,str,int,float,float|None]] = []
        for metric,amount in usage.items():
            for scope in scopes:
                limits=[limit for limit in configured if limit.scope==scope and limit.metric==metric]
                if limits:
                    reservations.extend((scope,metric,item.window,item.window_seconds,amount,item.limit) for item in limits)
                else:
                    reservations.append((scope,metric,"lifetime",0,amount,None))
        with self.store.transaction() as db:
            cooldowns=[]
            for scope in scopes:
                row=db.execute(
                    """SELECT available_at FROM quota_buckets WHERE scope=? AND provider=? AND model=?
                       AND metric='*' AND window_name='cooldown'""",
                    (scope,pool.provider,pool.model),
                ).fetchone()
                if row and row[0]:
                    try:
                        until=parse_utc(row[0])
                    except ValueError:
                        continue
                    if until>now:
                        cooldowns.append(until)
            if cooldowns:
                return None,max(cooldowns)
            ready_at=None
            for scope,metric,window,seconds,amount,limit in reservations:
                db.execute(
                    """INSERT INTO quota_buckets(scope,provider,model,metric,window_name,window_seconds,quota_limit,updated_at)
                       VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope,provider,model,metric,window_name)
                       DO UPDATE SET quota_limit=COALESCE(excluded.quota_limit,quota_buckets.quota_limit),
                         window_seconds=excluded.window_seconds,updated_at=excluded.updated_at""",
                    (scope,pool.provider,pool.model,metric,window,seconds,limit,iso_utc(now)),
                )
                if limit is None or seconds<=0:
                    continue
                since=iso_utc(now-timedelta(seconds=seconds))
                totals=db.execute(
                    """SELECT state,COALESCE(sum(amount),0) FROM quota_ledger
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                         AND created_at>=? GROUP BY state""",
                    (scope,pool.provider,pool.model,metric,window,since),
                ).fetchall()
                by_state={row[0]:float(row[1]) for row in totals}
                used=by_state.get("consumed",0.0)+by_state.get("uncertain",0.0)
                reserved=by_state.get("reserved",0.0)
                db.execute(
                    """UPDATE quota_buckets SET used=?,reserved=?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (used,reserved,iso_utc(now),scope,pool.provider,pool.model,metric,window),
                )
                if used+reserved+amount>limit:
                    oldest=db.execute(
                        """SELECT min(created_at) FROM quota_ledger
                           WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                             AND created_at>=? AND state IN ('consumed','uncertain')""",
                        (scope,pool.provider,pool.model,metric,window,since),
                    ).fetchone()[0]
                    candidate=parse_utc(oldest)+timedelta(seconds=seconds) if oldest else now+timedelta(seconds=seconds)
                    ready_at=candidate if ready_at is None else max(ready_at,candidate)
            if ready_at:
                return None,ready_at
            db.execute(
                """INSERT INTO interactive_attempts
                   (id,request_id,attempt_no,profile,provider,model,started_at,status)
                   VALUES(?,?,1,?,?,?,?,'started')""",
                (attempt_id,request_id,profile,pool.provider,pool.model,iso_utc(now)),
            )
            for scope,metric,window,seconds,amount,_limit in reservations:
                db.execute(
                    """INSERT INTO quota_events
                       (id,attempt_id,request_id,scope,provider,model,metric,window_name,
                        window_seconds,amount,created_at,state,expires_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,'reserved',?)""",
                    (str(uuid.uuid4()),attempt_id,request_id,scope,pool.provider,pool.model,
                     metric,window,seconds,amount,iso_utc(now),iso_utc(now+timedelta(days=30))),
                )
                db.execute(
                    """UPDATE quota_buckets SET reserved=reserved+?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (amount,iso_utc(now),scope,pool.provider,pool.model,metric,window),
                )
        return attempt_id,None

    def _finish_interactive(
        self,
        pool: PoolConfig,
        profile: str,
        request_id: str,
        attempt_id: str,
        *,
        status_code: int | None,
        category: str,
        retry_after: float | None,
        uncertain: bool,
        consumed: bool,
        actual_usage: dict[str,float] | None = None,
        quota_diagnostic: dict | None = None,
        observed_retry_after: float | None = None,
    ) -> None:
        now=utc_now()
        state="uncertain" if uncertain else ("consumed" if consumed else "released")
        request_state=(
            "waiting_quota"
            if status_code == 429
            else "failed"
            if uncertain or not consumed or status_code is not None
            else "done"
        )
        diagnostic={"category":category}
        if retry_after is not None:
            diagnostic["retry_after_seconds"]=max(0.0,retry_after)
        with self.store.transaction() as db:
            if status_code == 429 and pool.provider == "google":
                from kiber_analyst.gemini_quota import record_429
                record_429(db, profile=profile, operation=pool.operation, at=iso_utc(now),
                           detail=quota_diagnostic, retry_after=observed_retry_after)
            if actual_usage:
                for metric,amount in actual_usage.items():
                    db.execute("UPDATE quota_events SET amount=? WHERE attempt_id=? AND metric=?",(amount,attempt_id,metric))
            db.execute(
                """UPDATE interactive_attempts SET finished_at=?,http_code=?,category=?,
                   retry_after_seconds=?,outcome_uncertain=?,status=? WHERE id=?""",
                (
                    iso_utc(now),
                    status_code,
                    category,
                    retry_after,
                    int(uncertain),
                    "uncertain" if uncertain else "succeeded" if status_code is None and consumed else "failed",
                    attempt_id,
                ),
            )
            db.execute("UPDATE quota_events SET state=? WHERE attempt_id=? AND state='reserved'",(state,attempt_id))
            rows=db.execute(
                """SELECT DISTINCT scope,provider,model,metric,window_name,window_seconds
                   FROM quota_events WHERE attempt_id=?""",
                (attempt_id,),
            ).fetchall()
            for bucket in rows:
                seconds=int(bucket[5])
                since=iso_utc(now-timedelta(seconds=seconds)) if seconds>0 else None
                totals=db.execute(
                    """SELECT state,COALESCE(sum(amount),0) FROM quota_ledger
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                         AND (? IS NULL OR created_at>=?) GROUP BY state""",
                    (bucket[0],bucket[1],bucket[2],bucket[3],bucket[4],since,since),
                ).fetchall()
                by_state={row[0]:float(row[1]) for row in totals}
                db.execute(
                    """UPDATE quota_buckets SET used=?,reserved=?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (by_state.get("consumed",0.0)+by_state.get("uncertain",0.0),by_state.get("reserved",0.0),iso_utc(now),*bucket[:5]),
                )
            db.execute(
                """UPDATE interactive_requests SET state=?,updated_at=?,finished_at=?,
                   outcome_uncertain=?,diagnostic_json=? WHERE id=?""",
                (request_state,iso_utc(now),iso_utc(now),int(uncertain),canonical_json(diagnostic),request_id),
            )
            scopes=pool.scopes_for(profile)
            if status_code==401 or category=="profile_configuration":
                for configured_pool in self.pools.values():
                    if profile in configured_pool.profiles:
                        row=_state(db,configured_pool.name)
                        disabled=json.loads(row["disabled_profiles_json"]) if row else []
                        if profile not in disabled:
                            disabled.append(profile)
                        _persist_state(db,configured_pool.name,disabled=disabled)
            elif status_code==429 or status_code in {408,500,502,503,504} or category in {"request_timeout","network_error"}:
                delay=retry_after
                if delay is None:
                    delay=self.random_uniform(pool.retry_initial_seconds,pool.retry_initial_seconds*(1+pool.retry_jitter_fraction))
                available=iso_utc(now+timedelta(seconds=max(0.0,delay)))
                for configured_pool in self.pools.values():
                    if profile in configured_pool.profiles:
                        row=_state(db,configured_pool.name)
                        cooldowns=json.loads(row["cooldowns_json"]) if row else {}
                        cooldowns[profile]=available
                        _persist_state(db,configured_pool.name,cooldowns=cooldowns)
                if status_code==429:
                    for scope in scopes:
                        db.execute(
                            """INSERT INTO quota_buckets(scope,provider,model,metric,window_name,window_seconds,available_at,updated_at)
                               VALUES(?,?,?,'*','cooldown',0,?,?) ON CONFLICT(scope,provider,model,metric,window_name)
                               DO UPDATE SET available_at=max(COALESCE(quota_buckets.available_at,''),excluded.available_at),
                                 updated_at=excluded.updated_at""",
                            (scope,pool.provider,pool.model,available,iso_utc(now)),
                        )

    def interactive_call(
        self,
        *,
        request_id: str,
        project: str,
        model: str,
        query: str,
        invoke: Callable[[str,float],MeteredResult],
        timeout_seconds: float = 10.0,
        request_started: bool = False,
    ) -> dict[str,Any]:
        pool=self.pools.get("gemini_query")
        if pool is None or pool.provider!="google" or model!=pool.model:
            return {"status":"failed","reason":"provider_unavailable","request_id":request_id}
        if not query.strip() or len(query)>2000 or not project or len(project)>128:
            return {"status":"failed","reason":"invalid_argument","request_id":request_id}
        if not request_started and not self.begin_interactive_request(request_id,project,model,query):
            return {"status":"failed","reason":"duplicate_request","request_id":request_id}
        if request_started:
            with self.store.connect() as db:
                existing=db.execute("SELECT state,query_sha256,project_slug,model FROM interactive_requests WHERE id=?",(request_id,)).fetchone()
            if not existing or existing["state"]!="running" or existing["query_sha256"]!=hashlib.sha256(query.encode("utf-8")).hexdigest() or existing["project_slug"]!=project or existing["model"]!=model:
                return {"status":"failed","reason":"duplicate_request","request_id":request_id}
        deadline=time.monotonic()+min(10.0,max(0.1,timeout_seconds))
        profiles_tried:set[str]=set()
        quota_wakes:list[datetime]=[]
        try:
            while len(profiles_tried)<len(pool.profiles):
                profile,next_at,reason=select_profile(self.store,pool,exclude_profiles=profiles_tried)
                if profile is None:
                    if next_at and reason=="quota_cooldown":
                        quota_wakes.append(next_at)
                    final_reason="quota" if quota_wakes else "provider_unavailable"
                    retry=max(0.0,(min(quota_wakes)-utc_now()).total_seconds()) if quota_wakes else None
                    self._finish_unattempted_interactive(request_id,final_reason,retry)
                    return {"status":"waiting_quota" if final_reason=="quota" else "failed","reason":final_reason,"retry_after_seconds":retry,"request_id":request_id}
                if time.monotonic()>=deadline:
                    self._finish_unattempted_interactive(request_id,"timeout",None)
                    return {"status":"failed","reason":"timeout","request_id":request_id}
                scopes=pool.scopes_for(profile)
                locks=[self._scope_lock(scope) for scope in sorted(scopes)]
                acquired=[]
                try:
                    for lock in locks:
                        if not lock.acquire(blocking=False):
                            break
                        acquired.append(lock)
                    if len(acquired) != len(locks):
                        for lock in reversed(acquired):
                            lock.release()
                        acquired.clear()
                        profiles_tried.add(profile)
                        continue
                    usage={"requests":1,"items":1,"tokens":max(1,round(len(query)/4))}
                    attempt_id,quota_wait=self._reserve_interactive(pool,profile,request_id,usage)
                    if attempt_id is None:
                        if quota_wait:
                            quota_wakes.append(quota_wait)
                        profiles_tried.add(profile)
                        continue
                    remaining=max(0.05,deadline-time.monotonic())
                    try:
                        result=invoke(profile,remaining)
                    except Exception as exc:  # noqa: BLE001 - provider SDK errors vary.
                        code=_status(exc)
                        timed_out=_is_timeout(exc)
                        configuration_error=isinstance(exc,ProfileError) or getattr(exc,"category",None)=="profile_configuration"
                        category=("profile_configuration" if configuration_error else "rate_limited" if code==429 else "profile_auth" if code==401 else "policy_blocked" if code==403 else "server_error" if code in {500,502,503,504} else "request_timeout" if timed_out else "network_error" if code is None else "request_rejected")
                        retry_after=_retry_after(exc,utc_now())
                        permanent_category=getattr(exc,"category",None)
                        if isinstance(exc,PermanentProviderError):
                            category=permanent_category or "provider_error"
                        observed_retry_after = retry_after
                        if code==429 and retry_after is None:
                            retry_after=self.random_uniform(pool.retry_initial_seconds,pool.retry_initial_seconds*(1+pool.retry_jitter_fraction))
                        uncertain=(timed_out or code is None) and not configuration_error
                        consumed=not configuration_error
                        from kiber_analyst.gemini_quota import quota_details
                        self._finish_interactive(pool,profile,request_id,attempt_id,status_code=code,category=category,retry_after=retry_after,uncertain=uncertain,consumed=consumed,
                                                 quota_diagnostic=quota_details(exc) if code == 429 and pool.provider == "google" else None,
                                                 observed_retry_after=observed_retry_after)
                        reason="quota" if code==429 else "timeout" if timed_out else "provider_unavailable"
                        return {"status":"waiting_quota" if code==429 else "failed","reason":reason,"retry_after_seconds":retry_after,"scope":pool.scope_for(profile),"request_id":request_id}
                    self._finish_interactive(pool,profile,request_id,attempt_id,status_code=None,category="success",retry_after=None,uncertain=False,consumed=True,actual_usage=result.usage)
                    vector=result.value
                    return {"status":"done","vector":vector,"dimensions":len(vector),"model":pool.model,"scope":pool.scope_for(profile),"request_id":request_id}
                finally:
                    for lock in reversed(acquired):
                        lock.release()
            if quota_wakes:
                retry=max(0.0,(min(quota_wakes)-utc_now()).total_seconds())
                self._finish_unattempted_interactive(request_id,"quota",retry)
                return {"status":"waiting_quota","reason":"quota","retry_after_seconds":retry,"request_id":request_id}
            self._finish_unattempted_interactive(request_id,"busy",None)
            return {"status":"failed","reason":"worker_busy","request_id":request_id}
        except Exception:  # noqa: BLE001 - keep RPC failures secret-safe.
            self._finish_unattempted_interactive(request_id,"worker_unavailable",None)
            return {"status":"failed","reason":"worker_unavailable","request_id":request_id}

    def _finish_unattempted_interactive(self,request_id:str,reason:str,retry_after:float|None)->None:
        state="waiting_quota" if reason=="quota" else "failed"
        details={"category":reason}
        if retry_after is not None:
            details["retry_after_seconds"]=retry_after
        now=iso_utc()
        with self.store.transaction() as db:
            db.execute(
                """UPDATE interactive_requests SET state=?,updated_at=?,finished_at=?,diagnostic_json=?
                   WHERE id=? AND state='running'""",
                (state,now,now,canonical_json(details),request_id),
            )

    def _reserve(
        self,
        pool: PoolConfig,
        profile: str,
        job_id: str,
        item_id: str | None,
        usage: dict[str, float],
    ) -> tuple[str, datetime | None]:
        now = utc_now()
        scopes = pool.scopes_for(profile)
        attempt_id = str(uuid.uuid4())
        configured = [limit for limit in pool.limits if limit.scope in scopes]
        reservations: list[tuple[str, str, str, int, float, float | None]] = []
        for metric, amount in usage.items():
            for scope in scopes:
                limits = [item for item in configured if item.scope == scope and item.metric == metric]
                if limits:
                    reservations.extend((scope,metric,item.window,item.window_seconds,amount,item.limit) for item in limits)
                else:
                    reservations.append((scope,metric,"lifetime",0,amount,None))
        with self.store.transaction() as db:
            cooldown_times: list[datetime] = []
            for scope in scopes:
                cooldown = db.execute(
                    """SELECT available_at FROM quota_buckets WHERE scope=? AND provider=? AND model=?
                       AND metric='*' AND window_name='cooldown'""",
                    (scope, pool.provider, pool.model),
                ).fetchone()
                if cooldown and cooldown[0] and parse_utc(cooldown[0]) > now:
                    cooldown_times.append(parse_utc(cooldown[0]))
            if cooldown_times:
                return "", max(cooldown_times)
            ready_at: datetime | None = None
            for scope,metric_name,window,window_seconds,amount,limit in reservations:
                db.execute(
                    """INSERT INTO quota_buckets(scope,provider,model,metric,window_name,window_seconds,quota_limit,updated_at)
                       VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope,provider,model,metric,window_name)
                       DO UPDATE SET quota_limit=COALESCE(excluded.quota_limit,quota_buckets.quota_limit),
                                     window_seconds=excluded.window_seconds,updated_at=excluded.updated_at""",
                    (scope, pool.provider, pool.model, metric_name, window, window_seconds, limit, iso_utc(now)),
                )
                if limit is not None and window_seconds > 0:
                    since = iso_utc(now - timedelta(seconds=window_seconds))
                    used = db.execute(
                        """SELECT COALESCE(sum(amount),0) FROM quota_ledger
                           WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                             AND created_at>=? AND state IN ('consumed','uncertain')""",
                        (scope,pool.provider,pool.model,metric_name,window,since),
                    ).fetchone()[0]
                    reserved = db.execute(
                        """SELECT COALESCE(sum(amount),0) FROM quota_ledger
                           WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                             AND created_at>=? AND state='reserved'""",
                        (scope,pool.provider,pool.model,metric_name,window,since),
                    ).fetchone()[0]
                    db.execute(
                        """UPDATE quota_buckets SET used=?,reserved=?,updated_at=?
                           WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                        (float(used),float(reserved),iso_utc(now),scope,pool.provider,pool.model,metric_name,window),
                    )
                    if float(used) + float(reserved) + amount > limit:
                        oldest = db.execute(
                            """SELECT min(created_at) FROM quota_ledger
                               WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                                 AND created_at>=? AND state IN ('consumed','uncertain')""",
                            (scope,pool.provider,pool.model,metric_name,window,since),
                        ).fetchone()[0]
                        candidate = parse_utc(oldest) + timedelta(seconds=window_seconds) if oldest else now + timedelta(seconds=window_seconds)
                        ready_at = candidate if ready_at is None else max(ready_at, candidate)
            if ready_at:
                return "", ready_at
            db.execute(
                """INSERT INTO attempts(id,job_id,item_id,profile,provider,model,started_at,status)
                   VALUES(?,?,?,?,?,?,?,'started')""",
                (attempt_id,job_id,item_id,profile,pool.provider,pool.model,iso_utc(now)),
            )
            db.execute("UPDATE attempts SET quota_usage_json=? WHERE id=?",(canonical_json(usage),attempt_id))
            for scope,metric_name,window,window_seconds,amount,_limit in reservations:
                reservation_id = str(uuid.uuid4())
                db.execute(
                    """INSERT INTO quota_reservations
                       (id,attempt_id,job_id,scope,provider,model,metric,window_name,window_seconds,amount,created_at,state,expires_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,'reserved',?)""",
                    (reservation_id,attempt_id,job_id,scope,pool.provider,pool.model,metric_name,window,window_seconds,amount,iso_utc(now),iso_utc(now+timedelta(days=30))),
                )
                db.execute(
                    """UPDATE quota_buckets SET reserved=reserved+?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (amount,iso_utc(now),scope,pool.provider,pool.model,metric_name,window),
                )
            self.store.event(db, job_id, "provider_attempt_started", {"attempt_id": attempt_id, "profile": profile, "provider": pool.provider, "model": pool.model, "quota_scopes": list(scopes), "usage_estimate": usage})
        return attempt_id, None

    def _finish(
        self,
        pool: PoolConfig,
        profile: str,
        attempt_id: str,
        job_id: str,
        *,
        status_code: int | None,
        category: str,
        diagnostic: str,
        retry_after: float | None,
        uncertain: bool,
        consumed: bool,
        actual_usage: dict[str, float] | None = None,
        quota_diagnostic: dict | None = None,
    ) -> None:
        now = utc_now()
        scopes = pool.scopes_for(profile)
        with self.store.transaction() as db:
            if status_code == 429 and pool.provider == "google":
                from kiber_analyst.gemini_quota import record_429
                record_429(db, profile=profile, operation=pool.operation, at=iso_utc(now),
                           detail=quota_diagnostic, retry_after=retry_after)
            if actual_usage is not None:
                db.execute("UPDATE attempts SET quota_usage_json=? WHERE id=?",(canonical_json(actual_usage),attempt_id))
                for metric,amount in actual_usage.items():
                    db.execute(
                        "UPDATE quota_reservations SET amount=? WHERE attempt_id=? AND metric=?",
                        (amount,attempt_id,metric),
                    )
            db.execute(
                """UPDATE attempts SET finished_at=?,http_code=?,category=?,retry_info_json=?,
                   outcome_uncertain=?,status=?,diagnostic=? WHERE id=?""",
                (iso_utc(now),status_code,category,canonical_json({"retry_after_seconds": retry_after} if retry_after is not None else {}),int(uncertain),"uncertain" if uncertain else ("succeeded" if consumed and status_code is None else "failed"),diagnostic,attempt_id),
            )
            db.execute(
                "UPDATE quota_reservations SET state=? WHERE attempt_id=? AND state='reserved'",
                ("uncertain" if uncertain else ("consumed" if consumed else "released"),attempt_id),
            )
            reservations=db.execute(
                """SELECT DISTINCT scope,provider,model,metric,window_name,window_seconds
                   FROM quota_reservations WHERE attempt_id=?""",
                (attempt_id,),
            ).fetchall()
            for bucket in reservations:
                since=iso_utc(now-timedelta(seconds=int(bucket[5]))) if int(bucket[5])>0 else None
                totals=db.execute(
                    """SELECT state,COALESCE(sum(amount),0) FROM quota_ledger
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                         AND (? IS NULL OR created_at>=?) GROUP BY state""",
                    (bucket[0],bucket[1],bucket[2],bucket[3],bucket[4],since,since),
                ).fetchall()
                values={row[0]:float(row[1]) for row in totals}
                db.execute(
                    """UPDATE quota_buckets SET used=?,reserved=?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (values.get("consumed",0.0)+values.get("uncertain",0.0),values.get("reserved",0.0),iso_utc(now),bucket[0],bucket[1],bucket[2],bucket[3],bucket[4]),
                )
            if status_code == 401 or category == "profile_configuration":
                reason="401" if status_code==401 else "profile_configuration"
                affected_pools = [
                    configured_pool
                    for configured_pool in self.pools.values()
                    if profile in configured_pool.profiles
                ]
                for configured_pool in affected_pools:
                    row = _state(db, configured_pool.name)
                    disabled = json.loads(row["disabled_profiles_json"]) if row else []
                    if profile not in disabled:
                        disabled.append(profile)
                    _persist_state(db, configured_pool.name, disabled=disabled)
                self.store.event(
                    db,
                    job_id,
                    "profile_disabled",
                    {
                        "profile": profile,
                        "reason": reason,
                        "pools": [configured_pool.name for configured_pool in affected_pools],
                    },
                )
            elif status_code == 429 or (
                status_code in {408,500,502,503,504}
                or category.startswith("network")
                or category in {"request_timeout", "network_timeout"}
            ):
                if retry_after is not None:
                    delay = retry_after
                else:
                    previous = db.execute(
                        "SELECT count(*) FROM attempts WHERE job_id=? AND profile=? AND category=?",
                        (job_id,profile,category),
                    ).fetchone()[0]
                    base = min(pool.retry_initial_seconds * (2 ** min(int(previous), 16)),pool.retry_max_seconds)
                    delay = self.random_uniform(base,base*(1+pool.retry_jitter_fraction))
                until = now + timedelta(seconds=max(0.0,delay))
                row = _state(db,pool.name)
                cooldowns = json.loads(row["cooldowns_json"]) if row else {}
                cooldowns[profile] = iso_utc(until)
                _persist_state(db,pool.name,cooldowns=cooldowns)
                if status_code == 429:
                    for scope in scopes:
                        db.execute(
                            """INSERT INTO quota_buckets(scope,provider,model,metric,window_name,window_seconds,available_at,updated_at)
                               VALUES(?,?,?,'*','cooldown',0,?,?) ON CONFLICT(scope,provider,model,metric,window_name)
                               DO UPDATE SET available_at=max(COALESCE(quota_buckets.available_at,''),excluded.available_at),updated_at=excluded.updated_at""",
                            (scope,pool.provider,pool.model,iso_utc(until),iso_utc(now)),
                        )
                self.store.event(db,job_id,"profile_cooldown",{"profile":profile,"quota_scopes":list(scopes),"available_at":iso_utc(until),"category":category})

    def call(
        self,
        *,
        pool_name: str,
        job_id: str,
        item_id: str | None,
        invoke: Callable[[str], Any],
        usage: dict[str, float],
        explicit_profile: str | None = None,
        strategy_override: str | None = None,
        deadline_at: str | None = None,
        cycles: int | None = None,
        on_attempt: Callable[[str], None] | None = None,
    ) -> Any:
        pool = self.pools[pool_name]
        maximum = cycles or pool.max_cycles
        last_error: Exception | None = None
        next_at: datetime | None = None
        with self.store.connect() as db:
            already_attempted = db.execute(
                "SELECT count(*) FROM attempts WHERE job_id=? AND (? IS NULL OR item_id=?) AND COALESCE(category,'')!='profile_configuration'",
                (job_id,item_id,item_id),
            ).fetchone()[0]
        total_limit = maximum * (1 if explicit_profile else len(pool.profiles))
        if int(already_attempted) >= total_limit:
            raise PermanentProviderError("Provider profile retry cycles exhausted",category="retry_exhausted")
        for _cycle in range(maximum):
            attempted_this_cycle: set[str] = set()
            quota_wakes: list[datetime] = []
            profile, next_at, reason = select_profile(
                self.store,pool,explicit_profile=explicit_profile,strategy_override=strategy_override
            )
            while True:
                if profile is None:
                    if reason == "cycle_exhausted":
                        break
                    raise WaitForQuota(next_run_at=next_at,scope=pool.scope_for(explicit_profile or pool.profiles[0]),reason=reason)
                if deadline_at and utc_now() >= parse_utc(deadline_at):
                    raise PermanentProviderError("Job deadline exceeded",category="deadline")
                with self.store.connect() as db:
                    attempts_so_far = db.execute(
                        "SELECT count(*) FROM attempts WHERE job_id=? AND (? IS NULL OR item_id=?) AND COALESCE(category,'')!='profile_configuration'",
                        (job_id,item_id,item_id),
                    ).fetchone()[0]
                if int(attempts_so_far) >= total_limit:
                    raise PermanentProviderError("Provider profile retry cycles exhausted",category="retry_exhausted")
                failure: tuple[Exception,int|None,str,float|None] | None=None
                with ExitStack() as locks:
                    for scope in sorted(pool.scopes_for(profile)):
                        locks.enter_context(self._scope_lock(scope))
                    # Reserve only after taking all shared-scope locks. A prior
                    # request may have just cooled down this scope.
                    attempt_id, quota_wait = self._reserve(pool,profile,job_id,item_id,usage)
                    if not attempt_id:
                        if explicit_profile:
                            raise WaitForQuota(next_run_at=quota_wait,scope=pool.scope_for(profile),reason="quota_budget_exhausted")
                        if quota_wait is not None:
                            quota_wakes.append(quota_wait)
                        attempted_this_cycle.add(profile)
                        profile,next_at,reason=select_profile(self.store,pool,strategy_override=strategy_override,exclude_profiles=attempted_this_cycle)
                        if profile is None:
                            if reason == "cycle_exhausted":
                                wake=min(quota_wakes) if quota_wakes else quota_wait
                                raise WaitForQuota(next_run_at=wake,scope=pool.scope_for(pool.profiles[0]),reason="quota_budget_exhausted")
                            raise WaitForQuota(next_run_at=next_at,scope=pool.scope_for(pool.profiles[0]),reason=reason)
                        continue
                    if on_attempt:
                        on_attempt(profile)
                    started = time.monotonic()
                    try:
                        result = invoke(profile)
                    except Exception as exc:
                        if isinstance(exc,PermanentProviderError):
                            from kiber_analyst.gemini_quota import quota_details
                            is_google_429 = exc.status_code == 429 and pool.provider == "google"
                            self._finish(pool,profile,attempt_id,job_id,status_code=exc.status_code,category=exc.category,diagnostic=_safe_error(exc),
                                         retry_after=_retry_after(exc, utc_now()) if is_google_429 else None,uncertain=False,consumed=False,
                                         quota_diagnostic=quota_details(exc) if is_google_429 else None)
                            if exc.category!="profile_configuration":
                                raise
                            failure=(exc,exc.status_code,exc.category,None)
                        else:
                            status_code = _status(exc)
                            timed_out = _is_timeout(exc) or bool(getattr(exc,"timed_out",False))
                            category = (
                                "profile_configuration" if isinstance(exc,ProfileError) or getattr(exc,"category",None)=="profile_configuration" else
                                "rate_limited" if status_code == 429 else
                                "profile_auth" if status_code == 401 else
                                "policy_blocked" if status_code == 403 else
                                "server_error" if status_code in {500,502,503,504} else
                                "request_timeout" if timed_out else
                                "request_rejected" if status_code in TERMINAL_CODES else
                                "network_error" if status_code is None else "provider_error"
                            )
                            retry_after = _retry_after(exc,utc_now())
                            ambiguous = timed_out or (status_code is None and category!="profile_configuration")
                            from kiber_analyst.gemini_quota import quota_details
                            detail = None
                            if status_code == 429 and pool.provider == "google":
                                detail = getattr(exc, "quota_diagnostic", None) or quota_details(exc)
                            self._finish(pool,profile,attempt_id,job_id,status_code=status_code,category=category,diagnostic=_safe_error(exc),retry_after=retry_after,uncertain=ambiguous,consumed=category!="profile_configuration",quota_diagnostic=detail)
                            if status_code in TERMINAL_CODES or status_code == 403 or category == "provider_error":
                                raise PermanentProviderError(_safe_error(exc),status_code=status_code,category=category) from None
                            failure=(exc,status_code,category,retry_after)
                    else:
                        if isinstance(result,MeteredResult):
                            actual_usage=result.usage
                            value=result.value
                        else:
                            actual_usage=None
                            value=result
                        self._finish(pool,profile,attempt_id,job_id,status_code=None,category="success",diagnostic=f"completed in {time.monotonic()-started:.3f}s",retry_after=None,uncertain=False,consumed=True,actual_usage=actual_usage)
                        return value
                if failure:
                    exc,status_code,category,retry_after=failure
                    last_error=exc
                    attempted_this_cycle.add(profile)
                    if explicit_profile:
                        raise WaitForQuota(next_run_at=utc_now()+timedelta(seconds=retry_after or pool.retry_initial_seconds),scope=pool.scope_for(profile),reason=category) from None
                    profile,next_at,reason=select_profile(self.store,pool,strategy_override=strategy_override,exclude_profiles=attempted_this_cycle)
                    continue
        if last_error is not None:
            next_run = next_at or utc_now()+timedelta(seconds=pool.retry_initial_seconds)
            raise WaitForQuota(next_run_at=next_run,scope=pool.scope_for(pool.profiles[0]),reason="profile_cycles_exhausted")
        raise PermanentProviderError("No provider profile was available",category="no_profile")
