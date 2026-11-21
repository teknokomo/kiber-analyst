"""Restricted read-only summaries from the durable queue."""
import datetime
import json
import re
import uuid

from kiber_analyst.jobqueue import JobsError, iso_utc

SAFE_EVENTS = {'queued', 'claimed', 'waiting_retry', 'waiting_quota', 'done', 'failed',
               'timed_out', 'cancelled', 'cancel_requested', 'profile_cooldown',
               'provider_attempt_started', 'item_state'}
SAFE_CODES = {
    'success', 'rate_limited', 'server_error', 'network_error', 'request_timeout',
    'network_timeout', 'profile_auth', 'profile_configuration', 'policy_blocked',
    'request_rejected', 'provider_error', 'invalid_response', 'worker_restart',
}


def job_log(store, job_id, limit):
    try:
        if str(uuid.UUID(job_id)) != job_id:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise JobsError("Invalid job ID") from None
    if type(limit) is not int or not 1 <= limit <= 50:
        raise JobsError("Invalid log limit")
    with store.connect() as db:
        events = db.execute(
            'SELECT at,kind FROM job_events WHERE job_id=? ORDER BY seq DESC LIMIT ?',
            (job_id, limit),
        ).fetchall()
        calls = db.execute(
            """SELECT finished_at,http_code,category FROM attempts WHERE job_id=?
               AND finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT ?""",
            (job_id, limit),
        ).fetchall()
    logs = [{'at': row['at'], 'event': row['kind']} for row in events if row['kind'] in SAFE_EVENTS]
    logs.extend({'at': row['finished_at'], 'event': 'provider_result', 'http_code': row['http_code'],
                 'code': row['category'] if row['category'] in SAFE_CODES else None} for row in calls)
    return {'events': sorted(logs, key=lambda event: event['at'], reverse=True)[:limit]}


def blocked_until(db, now):
    results = {}
    for pool, cooldowns in db.execute('SELECT pool_name,cooldowns_json FROM routing_state'):
        if not re.fullmatch(r'[a-z0-9_-]{1,80}', pool):
            continue
        valid = {name: until for name, until in json.loads(cooldowns).items()
                 if re.fullmatch(r'[a-z0-9_-]{1,80}', name) and isinstance(until, str)
                 and re.fullmatch(r'[0-9T:+.Z-]{20,40}', until) and until > now}
        if valid:
            results[pool] = valid
    return results



def health(store, providers_only=False):
    now = iso_utc()
    hour_ago = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1)).isoformat()
    stale_at = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=15)).isoformat()
    with store.connect() as db:
        paused = blocked_until(db, now)
        if providers_only:
            counts = db.execute(
                """SELECT profile,
                       count(*) FILTER (WHERE http_code=429) AS http_429,
                       count(*) FILTER (WHERE http_code BETWEEN 500 AND 599) AS http_5xx
                   FROM (SELECT profile,http_code,finished_at FROM attempts
                         UNION ALL SELECT profile,http_code,finished_at FROM interactive_attempts)
                   WHERE finished_at>=? GROUP BY profile""", (hour_ago,),
            )
            return {'window_seconds': 3600, 'profiles': [
                dict(row) for row in counts if re.fullmatch(r'[a-z0-9_-]{1,80}', row['profile'])
            ], 'blocked_until': paused}
        states = dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state').fetchall())
        stuck = db.execute(
            """SELECT count(*) FROM jobs WHERE
               (state='running' AND updated_at < ?) OR
               (state IN ('queued','waiting_retry','waiting_quota') AND coalesce(next_run_at,updated_at) < ?)""",
            (stale_at, stale_at),
        ).fetchone()[0]
        reservations = dict(db.execute(
            'SELECT state,count(*) FROM quota_reservations WHERE expires_at>? GROUP BY state',
            (now,),
        ).fetchall())
    return {'worker': 'ready', 'queue': states, 'stuck_jobs': stuck,
            'blocked_until': paused, 'quota_reservations': reservations}
