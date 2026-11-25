from __future__ import annotations

import hashlib
import json
import os
import sqlite3

import pytest

from kiber_analyst.broker import Broker
from kiber_analyst.jobqueue import JobsError, JobStore


def _sqlite_backup(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
    if isinstance(source, sqlite3.Connection):
        with sqlite3.connect(destination) as dst:
            source.backup(dst)
    else:
        with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
            src.backup(dst)


def _restore_sqlite_snapshot(snapshot, target) -> None:
    temporary = target.with_name(target.name + ".restore.tmp")
    with sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True) as src:
        with sqlite3.connect(temporary) as dst:
            src.backup(dst)
    for suffix in ("-wal", "-shm"):
        (target.parent / (target.name + suffix)).unlink(missing_ok=True)
    os.replace(temporary, target)


@pytest.mark.parametrize("terminal_state", ["done", "cancelled"])
def test_rollback_must_reconcile_terminal_job_and_broker_request_before_claim(
    tmp_path, terminal_state,
):
    store = JobStore(tmp_path / "queue", require_mount=False)
    store.initialize()
    (tmp_path / "broker").mkdir(mode=0o700)
    source_id = f"github:example/{terminal_state}"

    def submit(argv, *, idempotency_key):
        record, _created = store.create_job(
            operation="external_ingest", command={"argv": argv},
            fingerprint=hashlib.sha256(json.dumps(argv, sort_keys=True).encode()).hexdigest(),
            idempotency_key=idempotency_key,
        )
        return {"job_id": record.id}

    def worker_api(action, **kwargs):
        if action == "source-overview":
            return {"threads": 1}
        if action == "status":
            record = store.status(kwargs["job_id"])
            return {"state": record["state"], "progress": {}}
        if action == "result":
            return store.result(kwargs["job_id"])
        raise AssertionError(f"unexpected worker API call: {action}")

    broker = Broker(
        tmp_path, {source_id: {"project": terminal_state}}, worker_api, submit,
    )
    request = broker.handle("operator", "ingest", source_id)
    job_id = request["job_id"]
    pre_queue = store.backup(pre_upgrade=True)
    pre_broker = tmp_path / "broker-pre.sqlite3"
    _sqlite_backup(broker.db, pre_broker)

    if terminal_state == "done":
        store.complete(job_id, {"status": "done", "items": 1})
    else:
        store.cancel(job_id)
    broker.status(broker.row("operator", request["request_id"]))
    post_queue = store.backup()
    post_broker = tmp_path / "broker-post.sqlite3"
    _sqlite_backup(broker.db, post_broker)
    with sqlite3.connect(post_queue) as db:
        assert db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()[0] == terminal_state
    with sqlite3.connect(post_broker) as db:
        assert db.execute(
            "SELECT state FROM requests WHERE id=?", (request["request_id"],),
        ).fetchone()[0] == terminal_state
    broker.db.close()

    # A matching pre-upgrade pair is still stale: without reconciliation the
    # completed/cancelled work is claimable again after rollback.
    store.restore_from_backup(pre_queue)
    _restore_sqlite_snapshot(pre_broker, tmp_path / "broker" / "requests.sqlite3")
    restored_broker = Broker(
        tmp_path, {source_id: {"project": terminal_state}}, worker_api, submit,
    )
    assert store.status(job_id)["state"] == "queued"
    assert restored_broker.row("operator", request["request_id"])["state"] == "queued"
    assert store.claim().id == job_id
    restored_broker.db.close()

    # Test-only simulation of an offline reconciliation from the preserved,
    # authoritative post-upgrade pair; production has no automatic reconciler.
    store.restore_from_backup(pre_queue)
    _restore_sqlite_snapshot(pre_broker, tmp_path / "broker" / "requests.sqlite3")
    reconciled_broker = Broker(
        tmp_path, {source_id: {"project": terminal_state}}, worker_api, submit,
    )
    with sqlite3.connect(post_queue) as authoritative_queue, sqlite3.connect(post_broker) as authoritative_broker:
        confirmed_job_state = authoritative_queue.execute(
            "SELECT state FROM jobs WHERE id=?", (job_id,),
        ).fetchone()[0]
        confirmed_request_state = authoritative_broker.execute(
            "SELECT state FROM requests WHERE id=?", (request["request_id"],),
        ).fetchone()[0]
        assert confirmed_job_state == confirmed_request_state == terminal_state
    with store.transaction() as db:
        db.execute(
            "UPDATE jobs SET state=?,next_run_at=NULL WHERE id=?",
            (confirmed_job_state, job_id),
        )
        store.event(db, job_id, "rollback_reconciled", {"confirmed_state": terminal_state})
    with reconciled_broker.db:
        reconciled_broker.db.execute(
            "UPDATE requests SET state=? WHERE id=?",
            (confirmed_request_state, request["request_id"]),
        )
        reconciled_broker.db.execute(
            "UPDATE attempts SET state=? WHERE request_id=? AND attempt=1",
            (confirmed_request_state, request["request_id"]),
        )
    assert store.claim() is None
    assert reconciled_broker.row("operator", request["request_id"])["state"] == terminal_state
    assert post_queue.is_file() and post_broker.is_file(), "post-upgrade evidence must remain available"
    reconciled_broker.db.close()


def test_queue_and_broker_must_be_restored_as_a_consistent_pair(tmp_path):
    queue = JobStore(tmp_path / "queue", require_mount=False)
    queue.initialize()
    (tmp_path / "broker").mkdir(mode=0o700)
    sources = {
        "github:example/preupgrade": {"project": "preupgrade"},
        "github:example/postupgrade": {"project": "postupgrade"},
    }

    def submit(argv, *, idempotency_key):
        fingerprint = hashlib.sha256(json.dumps(argv, sort_keys=True).encode()).hexdigest()
        record, _created = queue.create_job(
            operation="external_ingest", command={"argv": argv},
            fingerprint=fingerprint, idempotency_key=idempotency_key,
        )
        return {"job_id": record.id}

    def worker_api(action, **kwargs):
        if action == "source-overview":
            return {"threads": 1}
        if action == "status":
            record = queue.status(kwargs["job_id"])
            return {"state": record["state"], "progress": {}}
        if action == "result":
            return queue.result(kwargs["job_id"])
        raise AssertionError(f"unexpected worker API call: {action}")

    broker = Broker(tmp_path, sources, worker_api, submit)
    before = broker.handle("operator", "ingest", "github:example/preupgrade")
    queue_preupgrade = queue.backup(pre_upgrade=True)
    broker_preupgrade = tmp_path / "broker-preupgrade.sqlite3"
    _sqlite_backup(broker.db, broker_preupgrade)

    after = broker.handle("operator", "ingest", "github:example/postupgrade")
    queue_postupgrade = queue.backup()
    broker_postupgrade = tmp_path / "broker-postupgrade.sqlite3"
    _sqlite_backup(broker.db, broker_postupgrade)
    broker.db.close()

    # Queue-only restoration leaves the current broker attempt pointing to a
    # worker job absent from the restored queue.
    queue.restore_from_backup(queue_preupgrade)
    with sqlite3.connect(tmp_path / "broker" / "requests.sqlite3") as db:
        dangling = db.execute(
            "SELECT job_id FROM attempts WHERE request_id=?", (after["request_id"],),
        ).fetchone()[0]
    assert dangling == after["job_id"]
    with pytest.raises(JobsError, match="Job not found"):
        queue.get(dangling)

    # The supported manual rollback restores both databases from the same
    # quiesced snapshot pair. Post-upgrade snapshots remain for reconciliation.
    _restore_sqlite_snapshot(
        broker_preupgrade, tmp_path / "broker" / "requests.sqlite3",
    )
    queue.restore_from_backup(queue_preupgrade)
    restored = Broker(tmp_path, sources, worker_api, submit)
    try:
        with restored.db:
            attempts = restored.db.execute(
                "SELECT job_id FROM attempts WHERE request_id=?", (before["request_id"],),
            ).fetchall()
            absent_request = restored.db.execute(
                "SELECT 1 FROM requests WHERE id=?", (after["request_id"],),
            ).fetchone()
        assert [row["job_id"] for row in attempts] == [before["job_id"]]
        assert absent_request is None
        assert queue.get(before["job_id"]).state == "queued"
    finally:
        restored.db.close()

    with sqlite3.connect(broker_postupgrade) as db:
        assert db.execute(
            "SELECT 1 FROM requests WHERE id=?", (after["request_id"],),
        ).fetchone() is not None
    with sqlite3.connect(queue_postupgrade) as db:
        assert db.execute(
            "SELECT 1 FROM jobs WHERE id=?", (after["job_id"],),
        ).fetchone() is not None
