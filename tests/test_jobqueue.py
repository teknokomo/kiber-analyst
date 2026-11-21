from __future__ import annotations

import sqlite3
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

from kiber_analyst.jobqueue import (
    JobsError,
    JobStore,
    _resolved_within,
    iso_utc,
    utc_now,
)


def make_store(tmp_path: Path) -> JobStore:
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    return store


def create_job(store: JobStore, *, key: str = "same", deadline_hours: float = 24):
    return store.create_job(
        operation="search",
        command={"query": "test"},
        fingerprint="f" * 64,
        idempotency_key=key,
        deadline_hours=deadline_hours,
        items=[{"key": "query", "fingerprint": "a" * 64}],
    )


def test_sqlite_pragmas_and_durable_idempotent_submit(tmp_path: Path) -> None:
    store = make_store(tmp_path)

    first, created = create_job(store)
    duplicate, duplicate_created = create_job(store)

    assert created and not duplicate_created
    assert duplicate.id == first.id
    assert store.db_path.stat().st_mode & 0o777 == 0o600
    with store.connect() as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert db.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_idempotency_key_cannot_be_reused_for_different_input(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    create_job(store)

    with pytest.raises(JobsError, match="different job"):
        store.create_job(
            operation="search",
            command={"query": "different"},
            fingerprint="b" * 64,
            idempotency_key="same",
        )


def test_expired_job_has_a_retrievable_persisted_result(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    job, _ = create_job(store)
    with store.transaction() as db:
        db.execute(
            "UPDATE jobs SET deadline_at=? WHERE id=?",
            (iso_utc(utc_now() - timedelta(seconds=1)), job.id),
        )

    assert store.claim() is None
    assert store.status(job.id)["state"] == "timed_out"
    assert store.result(job.id)["status"] == "timed_out"
    assert Path(store.get(job.id).result_path or "").is_file()


def test_queued_cancel_persists_result_and_ack_is_success_only(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    job, _ = create_job(store)

    cancelled = store.cancel(job.id)

    assert cancelled.state == "cancelled"
    assert store.result(job.id)["status"] == "cancelled"
    with store.connect() as db:
        assert db.execute("SELECT state FROM job_items WHERE job_id=?", (job.id,)).fetchone()[0] == "skipped"
        assert db.execute("SELECT count(*) FROM job_events WHERE job_id=? AND kind='item_state'", (job.id,)).fetchone()[0] == 1
    with pytest.raises(JobsError, match="Only completed"):
        store.acknowledge(job.id)


def test_restart_marks_started_calls_uncertain_and_requeues_items(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    job, _ = create_job(store)
    item = store.job_items(job.id)[0]
    assert store.claim().id == job.id  # type: ignore[union-attr]
    attempt_id = str(uuid.uuid4())
    now = iso_utc()
    with store.transaction() as db:
        db.execute(
            """INSERT INTO attempts
               (id,job_id,item_id,profile,provider,model,started_at,status)
               VALUES(?,?,?,?,?,?,?,'started')""",
            (attempt_id, job.id, item["id"], "gemini_embed", "google", "embed", now),
        )
        db.execute(
            """INSERT INTO quota_buckets
               (scope,provider,model,metric,window_name,window_seconds,updated_at)
               VALUES('google:test','google','embed','requests','minute',60,?)""",
            (now,),
        )
        db.execute(
            """INSERT INTO quota_reservations
               (id,attempt_id,job_id,scope,provider,model,metric,window_name,
                window_seconds,amount,created_at,state,expires_at)
               VALUES(?,?,?,?,?,?,?,?,?,1,?,'reserved',?)""",
            (
                str(uuid.uuid4()), attempt_id, job.id, "google:test", "google",
                "embed", "requests", "minute", 60, now, iso_utc(utc_now() + timedelta(days=1)),
            ),
        )
        db.execute("UPDATE job_items SET state='running' WHERE id=?", (item["id"],))

    store.recover_after_restart()

    assert store.status(job.id)["state"] == "queued"
    assert store.job_items(job.id)[0]["state"] == "queued"
    with store.connect() as db:
        assert db.execute("SELECT status FROM attempts WHERE id=?", (attempt_id,)).fetchone()[0] == "uncertain"
        assert db.execute("SELECT state FROM quota_reservations WHERE attempt_id=?", (attempt_id,)).fetchone()[0] == "uncertain"
        assert tuple(db.execute("SELECT used,reserved FROM quota_buckets WHERE scope='google:test'").fetchone()) == (1.0, 0.0)


def test_backup_restore_checks_and_restores_queue_snapshot(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    backup = store.backup(pre_upgrade=True)
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM job_events WHERE job_id IS NULL AND kind='backup_created'").fetchone()[0] == 1
    create_job(store)

    restored = store.restore_from_backup(backup)

    assert restored == store.db_path
    with store.connect() as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0


def test_mount_requirement_fails_closed_without_configured_uuid(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "queue", expected_uuid=None, require_mount=True)

    with pytest.raises(JobsError, match="KIBER_ANALYST_DATA_UUID is required"):
        store.initialize()

    assert not store.data_root.exists()


def test_resolved_queue_path_cannot_escape_the_data_mount(tmp_path: Path) -> None:
    mount = tmp_path / "mnt" / "data"
    mount.mkdir(parents=True)
    outside = tmp_path / "root-fs"
    outside.mkdir()
    redirected = mount / "media"
    redirected.symlink_to(outside, target_is_directory=True)

    assert _resolved_within(mount / "media" / "kiber-analyst", mount) is False
    assert _resolved_within(mount / "media", mount) is False
    assert _resolved_within(mount / "safe" / "queue", mount) is True


def test_queue_initialization_rejects_symlinked_database(tmp_path: Path) -> None:
    root = tmp_path / "queue"
    root.mkdir()
    outside_db = tmp_path / "outside.sqlite3"
    outside_db.write_bytes(b"do not modify")
    (root / "queue.db").symlink_to(outside_db)
    store = JobStore(root, require_mount=False)

    with pytest.raises(JobsError, match="symbolic link"):
        store.initialize()

    assert outside_db.read_bytes() == b"do not modify"


def _downgrade_test_queue_to_v1(store: JobStore) -> None:
    with store.transaction() as db:
        db.execute("DROP VIEW quota_ledger")
        db.execute("DROP TABLE quota_events")
        db.execute("DROP TABLE interactive_attempts")
        db.execute("DROP TABLE interactive_requests")
        db.execute("PRAGMA user_version=1")


def test_sqlite_v1_migration_is_repeatable_and_preserves_jobs(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    job, _ = create_job(store, key="migration-v1")
    _downgrade_test_queue_to_v1(store)

    store.initialize()
    store.initialize()

    with store.connect() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert db.execute("SELECT id FROM jobs").fetchone()[0] == job.id
        assert db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN "
            "('interactive_requests','interactive_attempts','quota_events')"
        ).fetchone()[0] == 3
        assert db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='view' AND name='quota_ledger'"
        ).fetchone()[0] == 1


def test_v1_backup_restore_can_be_upgraded_to_v2(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    job, _ = create_job(store, key="restore-v1")
    _downgrade_test_queue_to_v1(store)
    backup = store.backups_dir / "daily-v1.sqlite3"
    with sqlite3.connect(store.db_path) as source, sqlite3.connect(backup) as target:
        source.backup(target)
    backup.chmod(0o600)

    store.restore_from_backup(backup)
    with store.connect() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT id FROM jobs").fetchone()[0] == job.id

    store.initialize()
    with store.connect() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert db.execute("SELECT id FROM jobs").fetchone()[0] == job.id


def test_interactive_restart_audits_uncertain_quota_and_abandons_request(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    request_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    now = iso_utc()
    with store.transaction() as db:
        db.execute(
            """INSERT INTO interactive_requests
               (id,project_slug,model,query_sha256,state,created_at,updated_at)
               VALUES(?,?,?,?,'running',?,?)""",
            (request_id, "sample-project", "embedding", "a" * 64, now, now),
        )
        db.execute(
            """INSERT INTO interactive_attempts
               (id,request_id,attempt_no,profile,provider,model,started_at,status)
               VALUES(?,?,1,?,'google',? ,?,'started')""",
            (attempt_id, request_id, "gemini_embed", "embedding", now),
        )
        db.execute(
            """INSERT INTO quota_events
               (id,attempt_id,request_id,scope,provider,model,metric,window_name,
                window_seconds,amount,created_at,state,expires_at)
               VALUES(?,?,?,?,?,?,?,?,?,1,?,'reserved',?)""",
            (
                str(uuid.uuid4()), attempt_id, request_id, "google:project-a",
                "google", "embedding", "requests", "minute", 60, now,
                iso_utc(utc_now() + timedelta(days=1)),
            ),
        )
        db.execute(
            """INSERT INTO quota_buckets
               (scope,provider,model,metric,window_name,window_seconds,updated_at)
               VALUES(?,?,?,'requests','minute',60,?)""",
            ("google:project-a", "google", "embedding", now),
        )

    store.recover_after_restart()

    with store.connect() as db:
        assert tuple(db.execute(
            "SELECT state,outcome_uncertain FROM interactive_requests WHERE id=?",
            (request_id,),
        ).fetchone()) == ("abandoned", 1)
        assert tuple(db.execute(
            "SELECT status,outcome_uncertain FROM interactive_attempts WHERE id=?",
            (attempt_id,),
        ).fetchone()) == ("uncertain", 1)
        assert db.execute(
            "SELECT state FROM quota_events WHERE attempt_id=?", (attempt_id,)
        ).fetchone()[0] == "uncertain"
        assert tuple(
            db.execute(
                "SELECT used,reserved FROM quota_buckets WHERE scope='google:project-a'"
            ).fetchone()
        ) == (1.0, 0.0)


def test_interactive_cleanup_keeps_events_through_quota_window(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    request_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    finished = utc_now() - timedelta(days=8)
    now = iso_utc(finished)
    with store.transaction() as db:
        db.execute(
            """INSERT INTO interactive_requests
               (id,project_slug,model,query_sha256,state,created_at,updated_at,finished_at)
               VALUES(?,?,?,?,'done',?,?,?)""",
            (request_id,"sample-project","embedding","b"*64,now,now,now),
        )
        db.execute(
            """INSERT INTO interactive_attempts
               (id,request_id,attempt_no,profile,provider,model,started_at,finished_at,status)
               VALUES(?,?,1,'gemini_embed','google','embedding',?,?,'succeeded')""",
            (attempt_id,request_id,now,now),
        )
        db.execute(
            """INSERT INTO quota_events
               (id,attempt_id,request_id,scope,provider,model,metric,window_name,
                window_seconds,amount,created_at,state,expires_at)
               VALUES(?,?,?,?,?,?,?,?,?,1,?,'consumed',?)""",
            (
                str(uuid.uuid4()),attempt_id,request_id,"google:project-a","google",
                "embedding","requests","monthly",30*24*60*60,now,
                iso_utc(utc_now()+timedelta(days=30)),
            ),
        )

    assert store.cleanup_interactive(now=utc_now()) == 0
    assert store.cleanup_interactive(now=finished + timedelta(days=31)) == 1
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM quota_events").fetchone()[0] == 0


def test_restart_marks_half_finalized_interactive_attempt_abandoned(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    request_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    now = iso_utc()
    with store.transaction() as db:
        db.execute(
            """INSERT INTO interactive_requests
               (id,project_slug,model,query_sha256,state,created_at,updated_at,finished_at)
               VALUES(?,?,?,?,'failed',?,?,?)""",
            (request_id,"sample-project","embedding","c"*64,now,now,now),
        )
        db.execute(
            """INSERT INTO interactive_attempts
               (id,request_id,attempt_no,profile,provider,model,started_at,status)
               VALUES(?,?,1,'gemini_embed','google','embedding',?,'started')""",
            (attempt_id,request_id,now),
        )

    store.recover_after_restart()

    with store.connect() as db:
        assert tuple(db.execute(
            "SELECT state,outcome_uncertain FROM interactive_requests WHERE id=?",
            (request_id,),
        ).fetchone()) == ("abandoned", 1)
        assert tuple(db.execute(
            "SELECT status,outcome_uncertain FROM interactive_attempts WHERE id=?",
            (attempt_id,),
        ).fetchone()) == ("uncertain", 1)
