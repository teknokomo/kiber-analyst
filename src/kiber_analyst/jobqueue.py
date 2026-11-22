from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import subprocess
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import data_dir, runtime_path
from kiber_analyst.errors import IngestError

DEFAULT_DATA_ROOT = data_dir()
DEFAULT_SOCKET = runtime_path("control.sock")
DEFAULT_UUID = os.environ.get("KIBER_ANALYST_DATA_UUID")
DEFAULT_MOUNT_TARGET = os.environ.get("KIBER_ANALYST_MOUNT_TARGET")
JOB_STATES = {
    "queued",
    "running",
    "waiting_quota",
    "waiting_retry",
    "done",
    "failed",
    "timed_out",
    "cancelled",
}


class JobsError(IngestError):
    """Safe queue, worker, or control-protocol error."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat(timespec="microseconds")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _safe_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _resolved_within(path: Path, root: Path) -> bool:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = root.resolve(strict=True)
    except (OSError, RuntimeError):
        return False
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  operation TEXT NOT NULL,
  command_json TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL CHECK(state IN (
    'queued','running','waiting_quota','waiting_retry','done','failed','timed_out','cancelled'
  )),
  priority INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deadline_at TEXT NOT NULL,
  next_run_at TEXT,
  checkpoint_json TEXT NOT NULL DEFAULT '{}',
  progress_json TEXT NOT NULL DEFAULT '{}',
  diagnostic_json TEXT NOT NULL DEFAULT '{}',
  result_path TEXT,
  result_sha256 TEXT,
  delivered_at TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1))
);
CREATE INDEX IF NOT EXISTS jobs_ready_idx
  ON jobs(state, priority DESC, next_run_at, created_at);
CREATE TABLE IF NOT EXISTS job_items (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  item_key TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('queued','running','waiting','done','failed','skipped')),
  checkpoint_json TEXT NOT NULL DEFAULT '{}',
  artifact_path TEXT,
  artifact_sha256 TEXT,
  updated_at TEXT NOT NULL,
  UNIQUE(job_id, item_key)
);
CREATE INDEX IF NOT EXISTS job_items_state_idx ON job_items(job_id, state);
CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  item_id TEXT REFERENCES job_items(id) ON DELETE SET NULL,
  profile TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  http_code INTEGER,
  category TEXT,
  retry_info_json TEXT NOT NULL DEFAULT '{}',
  quota_usage_json TEXT NOT NULL DEFAULT '{}',
  outcome_uncertain INTEGER NOT NULL DEFAULT 0 CHECK(outcome_uncertain IN (0,1)),
  status TEXT NOT NULL CHECK(status IN ('started','succeeded','failed','uncertain')),
  diagnostic TEXT
);
CREATE INDEX IF NOT EXISTS attempts_job_idx ON attempts(job_id, started_at);
CREATE TABLE IF NOT EXISTS quota_buckets (
  scope TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  metric TEXT NOT NULL,
  window_name TEXT NOT NULL,
  window_seconds INTEGER NOT NULL,
  quota_limit REAL,
  used REAL NOT NULL DEFAULT 0,
  reserved REAL NOT NULL DEFAULT 0,
  available_at TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(scope, provider, model, metric, window_name)
);
CREATE TABLE IF NOT EXISTS quota_reservations (
  id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  scope TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  metric TEXT NOT NULL,
  window_name TEXT NOT NULL,
  window_seconds INTEGER NOT NULL,
  amount REAL NOT NULL CHECK(amount >= 0),
  created_at TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('reserved','consumed','released','uncertain')),
  expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS quota_reservation_window_idx
  ON quota_reservations(scope, provider, model, metric, window_name, created_at, state);
CREATE TABLE IF NOT EXISTS routing_state (
  pool_name TEXT PRIMARY KEY,
  cursor_index INTEGER NOT NULL DEFAULT 0,
  weighted_current_json TEXT NOT NULL DEFAULT '{}',
  disabled_profiles_json TEXT NOT NULL DEFAULT '[]',
  cooldowns_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
  at TEXT NOT NULL,
  kind TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events(job_id, seq);
"""

QUEUE_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS interactive_requests (
  id TEXT PRIMARY KEY,
  project_slug TEXT NOT NULL,
  model TEXT NOT NULL,
  query_sha256 TEXT NOT NULL CHECK(length(query_sha256)=64),
  state TEXT NOT NULL CHECK(state IN ('running','waiting_quota','done','failed','abandoned')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  finished_at TEXT,
  diagnostic_json TEXT NOT NULL DEFAULT '{}',
  outcome_uncertain INTEGER NOT NULL DEFAULT 0 CHECK(outcome_uncertain IN (0,1))
);
CREATE INDEX IF NOT EXISTS interactive_requests_finished_idx
  ON interactive_requests(state, finished_at);
CREATE TABLE IF NOT EXISTS interactive_attempts (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES interactive_requests(id) ON DELETE CASCADE,
  attempt_no INTEGER NOT NULL CHECK(attempt_no > 0),
  profile TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  http_code INTEGER,
  category TEXT,
  retry_after_seconds REAL,
  outcome_uncertain INTEGER NOT NULL DEFAULT 0 CHECK(outcome_uncertain IN (0,1)),
  status TEXT NOT NULL CHECK(status IN ('started','succeeded','failed','uncertain')),
  UNIQUE(request_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS interactive_attempts_request_idx
  ON interactive_attempts(request_id, started_at);
CREATE TABLE IF NOT EXISTS quota_events (
  id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES interactive_attempts(id) ON DELETE CASCADE,
  request_id TEXT NOT NULL REFERENCES interactive_requests(id) ON DELETE CASCADE,
  scope TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  metric TEXT NOT NULL,
  window_name TEXT NOT NULL,
  window_seconds INTEGER NOT NULL,
  amount REAL NOT NULL CHECK(amount >= 0),
  created_at TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('reserved','consumed','released','uncertain')),
  expires_at TEXT NOT NULL,
  UNIQUE(attempt_id, scope, metric, window_name)
);
CREATE INDEX IF NOT EXISTS quota_events_window_idx
  ON quota_events(scope, provider, model, metric, window_name, created_at, state);
CREATE VIEW IF NOT EXISTS quota_ledger AS
  SELECT scope,provider,model,metric,window_name,window_seconds,amount,created_at,state
    FROM quota_reservations
  UNION ALL
  SELECT scope,provider,model,metric,window_name,window_seconds,amount,created_at,state
    FROM quota_events;
"""


@dataclass(frozen=True)
class JobRecord:
    id: str
    operation: str
    command: dict[str, Any]
    fingerprint: str
    state: str
    created_at: str
    updated_at: str
    deadline_at: str
    next_run_at: str | None
    checkpoint: dict[str, Any]
    progress: dict[str, Any]
    diagnostic: dict[str, Any]
    result_path: str | None
    result_sha256: str | None
    delivered_at: str | None
    priority: int
    cancel_requested: bool


class JobStore:
    """Durable SQLite state. Each mutation is one FULL-synchronous transaction."""

    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        *,
        expected_uuid: str | None = DEFAULT_UUID,
        require_mount: bool | None = None,
    ) -> None:
        self.data_root = data_root
        self.expected_uuid = expected_uuid
        self.require_mount = (
            bool(DEFAULT_MOUNT_TARGET) if require_mount is None else require_mount
        )
        self.db_path = data_root / "queue.db"
        self.jobs_dir = data_root / "jobs"
        self.backups_dir = data_root / "backups"
        self._write_lock = threading.RLock()

    def verify_storage(self) -> None:
        if not self.require_mount:
            return
        if not DEFAULT_MOUNT_TARGET:
            raise JobsError("Required queue mount target is not configured")
        mount_target = Path(DEFAULT_MOUNT_TARGET)
        if not _resolved_within(self.data_root, mount_target):
            raise JobsError("Queue storage must be under the configured mount target")
        try:
            result = subprocess.run(
                ["findmnt", "--noheadings", "--output", "UUID", "--target", str(mount_target)],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise JobsError("Cannot verify the configured queue mount") from None
        actual = result.stdout.strip()
        if result.returncode != 0 or (self.expected_uuid and actual != self.expected_uuid):
            raise JobsError("Queue mount is unavailable or has an unexpected identifier")

    def initialize(self) -> None:
        self.verify_storage()
        for directory in (self.data_root, self.jobs_dir, self.backups_dir):
            if directory.is_symlink():
                raise JobsError("Queue directories must not be symbolic links")
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(directory, 0o700)
        if self.db_path.is_symlink():
            raise JobsError("Queue database must not be a symbolic link")
        with self._write_lock, self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            current_version = int(db.execute("PRAGMA user_version").fetchone()[0])
            if current_version > 2:
                raise JobsError("Queue schema is newer than this worker supports")
            from kiber_analyst.gemini_quota import SCHEMA as GEMINI_QUOTA_SCHEMA

            db.executescript(SCHEMA + GEMINI_QUOTA_SCHEMA)
            if current_version == 0:
                db.execute("PRAGMA user_version=1")
                current_version = 1
            if current_version == 1:
                try:
                    db.executescript(
                        "BEGIN IMMEDIATE;\n"
                        + QUEUE_SCHEMA_V2
                        + "\nPRAGMA user_version=2;\nCOMMIT;"
                    )
                except sqlite3.Error:
                    if db.in_transaction:
                        db.rollback()
                    raise
            db.execute("PRAGMA wal_checkpoint(PASSIVE)")
        os.chmod(self.db_path,0o600)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.verify_storage()
        db = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock, self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except Exception:
                db.rollback()
                raise
            else:
                db.commit()

    def event(
        self,
        db: sqlite3.Connection,
        job_id: str | None,
        kind: str,
        details: Any = None,
    ) -> None:
        db.execute(
            "INSERT INTO job_events(job_id,at,kind,details_json) VALUES(?,?,?,?)",
            (job_id, iso_utc(), kind, canonical_json(details or {})),
        )

    def create_job(
        self,
        *,
        operation: str,
        command: dict[str, Any],
        fingerprint: str,
        deadline_hours: float = 24,
        priority: int = 0,
        items: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[JobRecord, bool]:
        if not 0 < deadline_hours <= 72:
            raise JobsError("Deadline must be greater than 0 and at most 72 hours")
        now = utc_now()
        key = idempotency_key or hashlib.sha256(
            canonical_json({"operation": operation, "fingerprint": fingerprint}).encode()
        ).hexdigest()
        new_id = str(uuid.uuid4())
        with self.transaction() as db:
            db.execute(
                """INSERT INTO jobs
                   (id,operation,command_json,fingerprint,idempotency_key,state,priority,
                    created_at,updated_at,deadline_at)
                   VALUES(?,?,?,?,?,'queued',?,?,?,?)
                   ON CONFLICT(idempotency_key) DO NOTHING""",
                (
                    new_id,
                    operation,
                    canonical_json(command),
                    fingerprint,
                    key,
                    priority,
                    iso_utc(now),
                    iso_utc(now),
                    iso_utc(now + timedelta(hours=deadline_hours)),
                ),
            )
            row = db.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?", (key,)
            ).fetchone()
            if row is None:
                raise JobsError("Could not persist queued job")
            if row["operation"] != operation or row["fingerprint"] != fingerprint:
                raise JobsError("Idempotency key was already used for a different job")
            created = row["id"] == new_id
            if created:
                self.event(db, new_id, "submitted", {"operation": operation})
                job_dir = self.jobs_dir / new_id
                job_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
                os.chmod(job_dir,0o700)
                directory_fd=os.open(self.jobs_dir,os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                for item in items or []:
                    item_id = str(uuid.uuid4())
                    db.execute(
                        """INSERT INTO job_items
                           (id,job_id,item_key,fingerprint,state,updated_at)
                           VALUES(?,?,?,?,'queued',?)""",
                        (item_id, new_id, item["key"], item["fingerprint"], iso_utc(now)),
                    )
                    self.event(db, new_id, "item_created", {"item_key": item["key"]})
            return self._record(row), created

    def _record(self, row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=row["id"],
            operation=row["operation"],
            command=_safe_json(row["command_json"], {}),
            fingerprint=row["fingerprint"],
            state=row["state"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            deadline_at=row["deadline_at"],
            next_run_at=row["next_run_at"],
            checkpoint=_safe_json(row["checkpoint_json"], {}),
            progress=_safe_json(row["progress_json"], {}),
            diagnostic=_safe_json(row["diagnostic_json"], {}),
            result_path=row["result_path"],
            result_sha256=row["result_sha256"],
            delivered_at=row["delivered_at"],
            priority=int(row["priority"]),
            cancel_requested=bool(row["cancel_requested"]),
        )

    def get(self, job_id: str) -> JobRecord:
        try:
            parsed = str(uuid.UUID(job_id))
        except (ValueError, TypeError, AttributeError):
            raise JobsError("Invalid job_id") from None
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (parsed,)).fetchone()
        if row is None:
            raise JobsError("Job not found")
        return self._record(row)

    def job_items(self, job_id: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT * FROM job_items WHERE job_id=? ORDER BY rowid", (job_id,)
            ).fetchall()
        return [
            {
                "id": row["id"],
                "key": row["item_key"],
                "fingerprint": row["fingerprint"],
                "state": row["state"],
                "checkpoint": _safe_json(row["checkpoint_json"], {}),
                "artifact_path": row["artifact_path"],
                "artifact_sha256": row["artifact_sha256"],
            }
            for row in rows
        ]

    def ensure_item(self, job_id: str, item_key: str, fingerprint: str) -> dict[str, Any]:
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM job_items WHERE job_id=? AND item_key=?",
                (job_id,item_key),
            ).fetchone()
            if row is None:
                item_id = str(uuid.uuid4())
                db.execute(
                    """INSERT INTO job_items(id,job_id,item_key,fingerprint,state,updated_at)
                       VALUES(?,?,?,?,'queued',?)""",
                    (item_id,job_id,item_key,fingerprint,iso_utc()),
                )
                row = db.execute("SELECT * FROM job_items WHERE id=?", (item_id,)).fetchone()
                self.event(db, job_id, "item_created", {"item_key": item_key})
            elif row["fingerprint"] != fingerprint:
                raise JobsError("Job item fingerprint changed after submission")
            return {
                "id": row["id"],
                "key": row["item_key"],
                "fingerprint": row["fingerprint"],
                "state": row["state"],
                "checkpoint": _safe_json(row["checkpoint_json"], {}),
                "artifact_path": row["artifact_path"],
                "artifact_sha256": row["artifact_sha256"],
            }

    def update_item(
        self,
        item_id: str,
        state: str,
        *,
        checkpoint: dict[str, Any] | None = None,
        artifact_path: str | None = None,
        artifact_sha256: str | None = None,
    ) -> None:
        if state not in {"queued","running","waiting","done","failed","skipped"}:
            raise JobsError("Invalid job item state")
        with self.transaction() as db:
            row = db.execute(
                "SELECT job_id,item_key,state,checkpoint_json FROM job_items WHERE id=?",
                (item_id,),
            ).fetchone()
            if row is None:
                raise JobsError("Job item not found")
            current = _safe_json(row["checkpoint_json"], {}) if checkpoint is None else checkpoint
            db.execute(
                """UPDATE job_items SET state=?,checkpoint_json=?,artifact_path=COALESCE(?,artifact_path),
                   artifact_sha256=COALESCE(?,artifact_sha256),updated_at=? WHERE id=?""",
                (state,canonical_json(current),artifact_path,artifact_sha256,iso_utc(),item_id),
            )
            if row["state"] != state:
                self.event(
                    db,
                    row["job_id"],
                    "item_state",
                    {"item_key": row["item_key"], "from": row["state"], "to": state},
                )

    def _close_open_items(
        self, db: sqlite3.Connection, job_id: str, terminal_state: str
    ) -> None:
        item_state = (
            "done"
            if terminal_state == "done"
            else "skipped"
            if terminal_state == "cancelled"
            else "failed"
        )
        rows = db.execute(
            "SELECT id,item_key,state FROM job_items WHERE job_id=? AND state IN ('queued','running','waiting')",
            (job_id,),
        ).fetchall()
        for row in rows:
            db.execute(
                "UPDATE job_items SET state=?,updated_at=? WHERE id=?",
                (item_state, iso_utc(), row["id"]),
            )
            self.event(
                db,
                job_id,
                "item_state",
                {"item_key": row["item_key"], "from": row["state"], "to": item_state},
            )

    def attempts_summary(self, job_id: str) -> dict[str, Any]:
        with self.connect() as db:
            rows = db.execute(
                """SELECT provider,profile,model,http_code,category,status,outcome_uncertain,
                          quota_usage_json,retry_info_json,started_at,finished_at FROM attempts
                   WHERE job_id=? ORDER BY started_at""",
                (job_id,),
            ).fetchall()
        return {
            "count": len(rows),
            "items": [
                {
                    "provider":row["provider"],"profile":row["profile"],"model":row["model"],
                    "http_code":row["http_code"],"category":row["category"],"status":row["status"],
                    "outcome_uncertain":bool(row["outcome_uncertain"]),
                    "quota_usage":_safe_json(row["quota_usage_json"],{}),
                    "retry_info":_safe_json(row["retry_info_json"],{}),
                    "started_at":row["started_at"],"finished_at":row["finished_at"],
                }
                for row in rows
            ],
        }

    def claim(self) -> JobRecord | None:
        now = iso_utc()
        expired: list[tuple[str, dict[str, Any]]] = []
        with self.transaction() as db:
            expired_rows = db.execute(
                """SELECT id,diagnostic_json FROM jobs
                   WHERE state IN ('queued','waiting_quota','waiting_retry') AND deadline_at<=?""",
                (now,),
            ).fetchall()
            db.execute(
                """UPDATE jobs SET state='timed_out',updated_at=?,
                   diagnostic_json=? WHERE state IN ('queued','waiting_quota','waiting_retry')
                   AND deadline_at<=?""",
                (now, canonical_json({"reason": "deadline_exceeded"}), now),
            )
            for expired_row in expired_rows:
                diagnostic = _safe_json(expired_row["diagnostic_json"], {})
                if not isinstance(diagnostic, dict):
                    diagnostic = {}
                diagnostic["reason"] = "deadline_exceeded"
                expired.append((expired_row["id"], diagnostic))
                self.event(db, expired_row["id"], "timed_out", diagnostic)
                self._close_open_items(db, expired_row["id"], "timed_out")
            row = db.execute(
                """SELECT * FROM jobs
                   WHERE state IN ('queued','waiting_quota','waiting_retry')
                     AND (next_run_at IS NULL OR next_run_at<=?)
                   ORDER BY priority DESC,created_at LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                result = None
            else:
                updated = db.execute(
                    "UPDATE jobs SET state='running',updated_at=?,next_run_at=NULL WHERE id=? AND state=?",
                    (now, row["id"], row["state"]),
                )
                if updated.rowcount != 1:
                    result = None
                else:
                    self.event(db, row["id"], "claimed", {})
                    claimed = db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
                    result = self._record(claimed)
        for job_id, diagnostic in expired:
            result_path, digest = self._write_result_file(
                job_id,
                {"job_id": job_id, "status": "timed_out", "error": diagnostic},
            )
            with self.transaction() as db:
                db.execute(
                    """UPDATE jobs SET result_path=?,result_sha256=?,updated_at=?
                       WHERE id=? AND state='timed_out' AND result_path IS NULL""",
                    (str(result_path), digest, iso_utc(), job_id),
                )
        return result

    def recover_after_restart(self) -> None:
        now = iso_utc()
        with self.transaction() as db:
            running = db.execute("SELECT id FROM jobs WHERE state='running'").fetchall()
            interactive_unfinished = db.execute(
                """SELECT r.id FROM interactive_requests r
                   WHERE r.state!='running' AND EXISTS (
                     SELECT 1 FROM interactive_attempts a
                     WHERE a.request_id=r.id AND a.status='started'
                   )"""
            ).fetchall()
            db.execute(
                """UPDATE attempts SET status='uncertain',outcome_uncertain=1,
                   category=COALESCE(category,'worker_restart'),finished_at=?
                   WHERE status='started'""",
                (now,),
            )
            db.execute(
                """UPDATE quota_reservations SET state='uncertain'
                   WHERE state='reserved'"""
            )
            db.execute(
                """UPDATE interactive_attempts
                   SET status='uncertain',outcome_uncertain=1,
                       category=COALESCE(category,'worker_restart'),finished_at=?
                   WHERE status='started'""",
                (now,),
            )
            db.execute(
                "UPDATE quota_events SET state='uncertain' WHERE state='reserved'"
            )
            db.execute(
                """UPDATE interactive_requests
                   SET state='abandoned',updated_at=?,finished_at=?,outcome_uncertain=1,
                       diagnostic_json=?
                   WHERE state='running'""",
                (now,now,canonical_json({"reason":"worker_restart","outcome_uncertain":True})),
            )
            for request in interactive_unfinished:
                db.execute(
                    """UPDATE interactive_requests
                       SET state='abandoned',updated_at=?,finished_at=?,outcome_uncertain=1,
                           diagnostic_json=? WHERE id=?""",
                    (
                        now,
                        now,
                        canonical_json({"reason":"worker_restart","outcome_uncertain":True}),
                        request[0],
                    ),
                )
            buckets=db.execute("SELECT scope,provider,model,metric,window_name,window_seconds FROM quota_buckets").fetchall()
            for bucket in buckets:
                since=(iso_utc(utc_now()-timedelta(seconds=int(bucket[5]))) if int(bucket[5])>0 else None)
                rows=db.execute(
                    """SELECT state,COALESCE(sum(amount),0) FROM quota_ledger
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?
                         AND (? IS NULL OR created_at>=?) GROUP BY state""",
                    (bucket[0],bucket[1],bucket[2],bucket[3],bucket[4],since,since),
                ).fetchall()
                totals={row[0]:float(row[1]) for row in rows}
                db.execute(
                    """UPDATE quota_buckets SET used=?,reserved=?,updated_at=?
                       WHERE scope=? AND provider=? AND model=? AND metric=? AND window_name=?""",
                    (totals.get("consumed",0.0)+totals.get("uncertain",0.0),totals.get("reserved",0.0),now,bucket[0],bucket[1],bucket[2],bucket[3],bucket[4]),
                )
            db.execute(
                """UPDATE jobs SET state='queued',updated_at=?,next_run_at=NULL,
                   diagnostic_json=? WHERE state='running'""",
                (now, canonical_json({"reason": "worker_restart; active request outcome may be uncertain"})),
            )
            for row in running:
                self.event(db, row["id"], "recovered_after_restart", {"in_flight_outcome_uncertain": True})
            db.execute(
                """UPDATE job_items SET state='queued',updated_at=?
                   WHERE state='running'""",
                (now,),
            )

    def cleanup_interactive(self, *, now: datetime | None = None) -> int:
        moment = now or utc_now()
        removed = 0
        with self.transaction() as db:
            rows = db.execute(
                """SELECT id,finished_at FROM interactive_requests
                   WHERE state!='running' AND finished_at IS NOT NULL"""
            ).fetchall()
            for row in rows:
                finished_at = parse_utc(row["finished_at"])
                longest_window = db.execute(
                    "SELECT COALESCE(MAX(window_seconds),0) FROM quota_events WHERE request_id=?",
                    (row["id"],),
                ).fetchone()[0]
                retention = max(7 * 24 * 60 * 60, int(longest_window))
                expires_at = moment - timedelta(seconds=retention)
                if finished_at <= expires_at:
                    db.execute("DELETE FROM interactive_requests WHERE id=?", (row["id"],))
                    removed += 1
        return removed

    def update_progress(self, job_id: str, progress: dict[str, Any], checkpoint: dict[str, Any] | None = None) -> None:
        with self.transaction() as db:
            row = db.execute("SELECT checkpoint_json FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise JobsError("Job not found")
            current = _safe_json(row["checkpoint_json"], {}) if checkpoint is None else checkpoint
            db.execute(
                "UPDATE jobs SET progress_json=?,checkpoint_json=?,updated_at=? WHERE id=?",
                (canonical_json(progress), canonical_json(current), iso_utc(), job_id),
            )

    def _write_result_file(self, job_id: str, result: Any) -> tuple[Path, str]:
        try:
            parsed_job_id = str(uuid.UUID(job_id))
        except (ValueError, TypeError, AttributeError):
            raise JobsError("Invalid job_id") from None
        if parsed_job_id != job_id:
            raise JobsError("Invalid job_id")
        job_dir = self.jobs_dir / job_id
        if not job_dir.is_dir() or job_dir.is_symlink():
            raise JobsError("Job artifact directory is unavailable")
        result_path = job_dir / "result.json"
        payload = (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode()
        temporary = result_path.with_name("result.json.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, result_path)
        os.chmod(result_path, 0o600)
        directory_fd=os.open(job_dir,os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        digest = hashlib.sha256(payload).hexdigest()
        return result_path, digest

    def complete(self, job_id: str, result: Any, *, state: str = "done") -> None:
        if state not in {"done", "failed", "timed_out", "cancelled"}:
            raise JobsError("Invalid terminal job state")
        result_path, digest = self._write_result_file(job_id, result)
        with self.transaction() as db:
            db.execute(
                """UPDATE jobs SET state=?,result_path=?,result_sha256=?,updated_at=?,
                          next_run_at=NULL WHERE id=?""",
                (state, str(result_path), digest, iso_utc(), job_id),
            )
            self._close_open_items(db, job_id, state)
            self.event(db, job_id, state, {"result_sha256": digest})

    def postpone(self, job_id: str, state: str, next_run_at: datetime, diagnostic: dict[str, Any]) -> None:
        if state not in {"waiting_quota", "waiting_retry"}:
            raise JobsError("Invalid wait state")
        with self.transaction() as db:
            db.execute(
                """UPDATE jobs SET state=?,next_run_at=?,diagnostic_json=?,updated_at=?
                   WHERE id=? AND state='running'""",
                (state, iso_utc(next_run_at), canonical_json(diagnostic), iso_utc(), job_id),
            )
            self.event(db, job_id, state, diagnostic)

    def cancel(self, job_id: str) -> JobRecord:
        cancelled_now = False
        already_terminal = False
        with self.transaction() as db:
            row = db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise JobsError("Job not found")
            if row["state"] in {"done", "failed", "timed_out", "cancelled"}:
                already_terminal = True
            elif row["state"] == "running":
                db.execute(
                    "UPDATE jobs SET cancel_requested=1,updated_at=? WHERE id=?",
                    (iso_utc(), job_id),
                )
                self.event(db, job_id, "cancel_requested", {})
            else:
                cancelled_now = True
                db.execute(
                    "UPDATE jobs SET state='cancelled',updated_at=? WHERE id=?",
                    (iso_utc(), job_id),
                )
                self._close_open_items(db, job_id, "cancelled")
                self.event(db, job_id, "cancelled", {})
        if cancelled_now:
            result_path, digest = self._write_result_file(
                job_id, {"job_id": job_id, "status": "cancelled"}
            )
            with self.transaction() as db:
                db.execute(
                    """UPDATE jobs SET result_path=?,result_sha256=?,updated_at=?
                       WHERE id=? AND state='cancelled' AND result_path IS NULL""",
                    (str(result_path), digest, iso_utc(), job_id),
                )
        if already_terminal:
            return self.get(job_id)
        return self.get(job_id)

    def cancelled(self, job_id: str) -> bool:
        with self.connect() as db:
            row = db.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
        return row is None or bool(row[0])

    def acknowledge(self, job_id: str) -> JobRecord:
        with self.transaction() as db:
            row = db.execute("SELECT state,delivered_at FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise JobsError("Job not found")
            if row["state"] != "done":
                raise JobsError("Only completed jobs can be acknowledged")
            if not row["delivered_at"]:
                now = iso_utc()
                db.execute("UPDATE jobs SET delivered_at=?,updated_at=? WHERE id=?", (now, now, job_id))
                self.event(db, job_id, "acknowledged", {})
        return self.get(job_id)

    def status(self, job_id: str) -> dict[str, Any]:
        record = self.get(job_id)
        return {
            "job_id": record.id,
            "operation": record.operation,
            "state": record.state,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "deadline_at": record.deadline_at,
            "next_run_at": record.next_run_at,
            "progress": record.progress,
            "checkpoint": record.checkpoint,
            "diagnostic": record.diagnostic,
            "delivered_at": record.delivered_at,
        }

    def result(self, job_id: str) -> dict[str, Any]:
        record = self.get(job_id)
        if record.state not in {"done","failed","timed_out","cancelled"} or not record.result_path:
            if record.state in {"timed_out", "cancelled"} and not record.result_path:
                return {
                    "job_id": record.id,
                    "status": record.state,
                    "error": record.diagnostic or None,
                }
            raise JobsError("Job result is not available")
        path = Path(record.result_path)
        if path.parent != (self.jobs_dir / job_id) or path.name != "result.json":
            raise JobsError("Stored result path is invalid")
        try:
            payload = path.read_bytes()
        except OSError:
            raise JobsError("Stored result is unavailable") from None
        if hashlib.sha256(payload).hexdigest() != record.result_sha256:
            raise JobsError("Stored result checksum mismatch")
        return json.loads(payload)

    def backup(self, *, pre_upgrade: bool = False, now: datetime | None = None) -> Path:
        self.initialize()
        stamp = (now or utc_now()).strftime("%Y%m%dT%H%M%SZ")
        prefix = "preupgrade" if pre_upgrade else "daily"
        destination = self.backups_dir / f"{prefix}-{stamp}-{uuid.uuid4().hex[:8]}.sqlite3"
        with self._write_lock, self.connect() as source:
            target = sqlite3.connect(destination)
            try:
                source.backup(target)
            finally:
                target.close()
        os.chmod(destination, 0o600)
        if not pre_upgrade:
            daily = sorted(self.backups_dir.glob("daily-*.sqlite3"), reverse=True)
            for stale in daily[7:]:
                stale.unlink(missing_ok=True)
        with self.transaction() as db:
            self.event(
                db,
                None,
                "backup_created",
                {"kind": "pre_upgrade" if pre_upgrade else "daily", "name": destination.name},
            )
        return destination

    def restore_from_backup(self, backup_path: Path) -> Path:
        self.verify_storage()
        try:
            if backup_path.is_symlink():
                raise JobsError("Restore source must be a regular queue backup")
            backup=backup_path.resolve(strict=True)
            backups=self.backups_dir.resolve(strict=True)
        except (OSError,RuntimeError):
            raise JobsError("Queue backup does not exist") from None
        if backup.parent!=backups or not backup.name.endswith(".sqlite3") or backup.is_symlink():
            raise JobsError("Restore source must be a queue backup in the backups directory")
        self.data_root.mkdir(mode=0o700,parents=True,exist_ok=True)
        lock_path=self.data_root/"worker.lock"
        descriptor=os.open(lock_path,os.O_CREAT|os.O_RDWR,0o600)
        try:
            try:
                fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                raise JobsError("Stop the Kiber-analyst worker before restoring a queue backup") from None
            source=sqlite3.connect(f"file:{backup}?mode=ro",uri=True)
            try:
                integrity=source.execute("PRAGMA integrity_check").fetchone()
                version=int(source.execute("PRAGMA user_version").fetchone()[0])
                if not integrity or integrity[0]!="ok" or version not in {1,2}:
                    raise JobsError("Queue backup failed integrity or schema-version validation")
                temporary=self.db_path.with_name("queue.db.restore.tmp")
                target=sqlite3.connect(temporary)
                try:
                    source.backup(target)
                    target.execute("PRAGMA journal_mode=DELETE")
                    target.commit()
                finally:
                    target.close()
                os.chmod(temporary,0o600)
                for suffix in ("-wal","-shm"):
                    Path(str(self.db_path)+suffix).unlink(missing_ok=True)
                os.replace(temporary,self.db_path)
                directory_fd=os.open(self.data_root,os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                return self.db_path
            finally:
                source.close()
        finally:
            try:
                fcntl.flock(descriptor,fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def backup_if_due(self) -> None:
        self.backups_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        for path in self.backups_dir.glob("daily-*.sqlite3"):
            if path.name.startswith("daily-" + utc_now().strftime("%Y%m%d")):
                return
        self.backup()

    def extend_deadline(self, job_id: str, hours: float) -> JobRecord:
        if not 0 < hours <= 72:
            raise JobsError("Deadline extension must be between 0 and 72 hours")
        with self.transaction() as db:
            row = db.execute("SELECT deadline_at,created_at,state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise JobsError("Job not found")
            if row["state"] in {"done", "failed", "timed_out", "cancelled"}:
                raise JobsError("A terminal job deadline cannot be extended")
            original = parse_utc(row["created_at"])
            deadline = min(parse_utc(row["deadline_at"]) + timedelta(hours=hours), original + timedelta(hours=72))
            db.execute(
                "UPDATE jobs SET deadline_at=?,updated_at=? WHERE id=?",
                (iso_utc(deadline), iso_utc(), job_id),
            )
            self.event(db, job_id, "deadline_extended", {"hours": hours})
        return self.get(job_id)

    def next_wake_seconds(self, cap: float = 30.0) -> float:
        with self.connect() as db:
            row = db.execute(
                """SELECT min(COALESCE(next_run_at,created_at)) FROM jobs
                   WHERE state IN ('queued','waiting_quota','waiting_retry')"""
            ).fetchone()
        if not row or not row[0]:
            return cap
        try:
            return max(0.1, min(cap, (parse_utc(row[0]) - utc_now()).total_seconds()))
        except ValueError:
            return cap


def mount_ready(
    expected_uuid: str | None = DEFAULT_UUID,
    mount_target: str | Path | None = DEFAULT_MOUNT_TARGET,
) -> bool:
    if mount_target is None:
        return False
    try:
        result = subprocess.run(
            ["findmnt", "--noheadings", "--output", "UUID", "--target", str(mount_target)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and (
        expected_uuid is None or result.stdout.strip() == expected_uuid
    )
