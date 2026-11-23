"""Embeddings for external discussion segments (ТЗ 0.2.0 §9.1).

Same model contract, batch size, retry, and idempotency rules as the Telegram
embed pipeline: empty text is never sent, re-running skips segments that
already have an embedding for the current model, and no vectors are stored
when a batch fails.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.connectors.errors import ConnectorError
from kiber_analyst.connectors.source_ids import parse_source_id
from kiber_analyst.embed import (
    BATCH_SIZE,
    MODEL_KEY,
    _connect,
    _decode_checkpoint,
    _embed_resilient,
    _required_env,
    _safe_db_message,
)
from kiber_analyst.errors import EmbedError
from kiber_analyst.paths import write_json_atomic

_MAX_SOURCE_SEGMENTS = 500_000


def _checkpoint(
    *, source_id: str, last_segment_id: int | None, inserted: int,
    skipped_existing: int, skipped_empty: int, errors: int,
    characters_embedded: int,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "last_segment_id": last_segment_id,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "skipped_empty": skipped_empty,
        "errors": errors,
        "characters_embedded": characters_embedded,
    }


def _get_or_create_run(
    cursor: Any, project_id: str, source_id: str
) -> tuple[str, dict[str, Any], bool]:
    cursor.execute(
        """SELECT id, checkpoint FROM runs
           WHERE project_id = %s::uuid AND kind = 'external_embed'
             AND status IN ('running', 'failed')
             AND checkpoint->>'source_id' = %s
             AND checkpoint->>'model' = %s
           ORDER BY started_at DESC LIMIT 1""",
        (project_id, source_id, MODEL_KEY),
    )
    row = cursor.fetchone()
    if row:
        checkpoint = _decode_checkpoint(row[1])
        return str(row[0]), checkpoint, True
    initial = _checkpoint(
        source_id=source_id,
        last_segment_id=None,
        inserted=0,
        skipped_existing=0,
        skipped_empty=0,
        errors=0,
        characters_embedded=0,
    )
    initial["model"] = MODEL_KEY
    cursor.execute(
        """INSERT INTO runs (project_id, kind, status, checkpoint)
           VALUES (%s::uuid, 'external_embed', 'running', %s::jsonb)
           RETURNING id""",
        (project_id, json.dumps(initial)),
    )
    row = cursor.fetchone()
    if not row:
        raise EmbedError("Could not create an external embedding run")
    return str(row[0]), initial, False


def _write_report(path: Path | None, report: dict[str, Any]) -> None:
    if path is None:
        return
    try:
        write_json_atomic(path, report)
    except OSError:
        # The report is auxiliary: a failed write must not fail a run whose
        # vectors are already committed.
        return


def embed_external(
    source_id: str,
    *,
    project: str,
    env_path: Path = config_path("knowledge.conf"),
    api_env_path: Path = config_path("provider-credentials.env"),
    proxy_env_path: Path = config_path("provider-proxies.env"),
    provider_profile: str = "gemini_embed",
    embedding_call: Callable[[list[str]], tuple[list[list[float]], int | None]] | None = None,
    limit: int | None = None,
    report_path: Path | None = None,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[float], float] = lambda seconds: random.uniform(
        seconds * 0.8, seconds * 1.2
    ),
) -> dict[str, Any]:
    """Embed non-empty text segments of one external source, idempotently."""
    parsed = parse_source_id(source_id)
    if parsed is None:
        raise EmbedError("External source_id is invalid")
    if type(limit) is not int or limit < 0:
        limit = None
    started = time.monotonic()
    client = None
    if embedding_call is None:
        from kiber_analyst.embedding_api import create_client, load_provider_settings

        provider_settings = load_provider_settings(
            api_env_path, proxy_env_path, profile=provider_profile
        )
        client = create_client(provider_settings)
    try:
        db_config = _required_env(env_path)
    except Exception as exc:
        if client is not None:
            client.close()
        if isinstance(exc, EmbedError):
            raise
        raise EmbedError(
            f"Cannot read knowledge database settings ({type(exc).__name__})"
        ) from exc

    counters = {
        "inserted": 0, "skipped_existing": 0, "skipped_empty": 0,
        "skipped_changed": 0, "errors": 0,
    }
    characters = 0
    run_id: str | None = None
    last_segment_id: int | None = None
    report: dict[str, Any] = {
        "status": "running",
        "source_id": source_id,
        "model": MODEL_KEY,
        "project": project,
        **counters,
        "duration_seconds": 0,
        "runs_id": None,
    }
    conn = None
    try:
        conn = _connect(db_config)
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute("SELECT id FROM projects WHERE slug = %s", (project,))
            project_row = cursor.fetchone()
            if not project_row:
                raise EmbedError(f"Project {project!r} does not exist")
            project_id = str(project_row[0])
            # Serialize invocations that share a resumable run so a second
            # worker cannot move the cursor backwards.
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"external-embed:{source_id}:{MODEL_KEY}",),
            )
            run_id, resume, _ = _get_or_create_run(cursor, project_id, source_id)
            last_value = resume.get("last_segment_id")
            last_segment_id = int(last_value) if last_value is not None else None
            for name in counters:
                counters[name] = int(resume.get(name, 0))
            characters = int(resume.get("characters_embedded", 0))
            # The resume cursor is applied in SQL: the limit must cap the rows
            # that are still to be processed, not the already embedded prefix.
            cursor.execute(
                """SELECT s.id, s.external_ref, s.content_hash, s.text,
                          EXISTS (
                            SELECT 1 FROM embeddings e
                            WHERE e.owner_table = 'segments'
                              AND e.owner_id = s.id::text AND e.model = %s
                          ) AS has_embedding
                   FROM segments s
                   JOIN documents d ON d.id = s.document_id
                   JOIN sources so ON so.id = d.source_id
                   JOIN projects p ON p.id = so.project_id
                   WHERE p.slug = %s
                     AND so.kind = %s AND so.external_id = %s
                     AND s.item_kind IS NOT NULL
                     AND s.thread_ref IS NOT NULL
                     AND s.text ~ '[^[:space:]]'
                     AND (%s::bigint IS NULL OR s.id > %s::bigint)
                   ORDER BY has_embedding, s.id
                   LIMIT %s""",
                (
                    MODEL_KEY,
                    project,
                    parsed.provider,
                    parsed.external_id,
                    last_segment_id,
                    last_segment_id,
                    limit if limit is not None else _MAX_SOURCE_SEGMENTS,
                ),
            )
            rows = cursor.fetchall()
            ordered = [
                {
                    "id": int(row[0]),
                    "external_ref": row[1],
                    "content_hash": row[2],
                    "text": row[3],
                    "has_embedding": bool(row[4]),
                }
                for row in rows
            ]

        for chunk_start in range(0, len(ordered), BATCH_SIZE):
            chunk = ordered[chunk_start : chunk_start + BATCH_SIZE]
            pending = [item for item in chunk if not item["has_embedding"]]
            newly_skipped_existing = sum(1 for item in chunk if item["has_embedding"])
            vectors: list[list[float]] = []
            if pending:
                texts = [item["text"] for item in pending]
                if embedding_call is not None:
                    vectors, _ = embedding_call(texts)
                else:
                    vectors, _ = _embed_resilient(
                        client, texts, sleep=sleep, jitter=jitter
                    )
                if len(vectors) != len(pending):
                    raise EmbedError(
                        "Embedding provider returned an unexpected vector count"
                    )
            with conn.transaction(), conn.cursor() as cursor:
                inserted_here = 0
                skipped_conflict = 0
                skipped_changed = 0
                characters_added = 0
                for item, vector in zip(pending, vectors, strict=True):
                    # The provider call ran outside a transaction; skip the
                    # vector if the segment text changed meanwhile.
                    cursor.execute(
                        "SELECT content_hash FROM segments WHERE id = %s FOR UPDATE",
                        (item["id"],),
                    )
                    current = cursor.fetchone()
                    if current is None or current[0] != item["content_hash"]:
                        skipped_changed += 1
                        continue
                    vector_value = json.dumps(vector, separators=(",", ":"))
                    cursor.execute(
                        """INSERT INTO embeddings
                             (project_id, owner_table, owner_id, model, embedding)
                           VALUES (%s::uuid, 'segments', %s, %s, %s::vector)
                           ON CONFLICT (owner_table, owner_id, model) DO NOTHING""",
                        (project_id, str(item["id"]), MODEL_KEY, vector_value),
                    )
                    if cursor.rowcount == 1:
                        inserted_here += 1
                        characters_added += len(item["text"])
                    else:
                        skipped_conflict += 1
                committed = dict(counters)
                committed["inserted"] += inserted_here
                committed["skipped_existing"] += newly_skipped_existing + skipped_conflict
                committed["skipped_changed"] = committed.get("skipped_changed", 0) + skipped_changed
                committed_last_id = (
                    max(item["id"] for item in chunk) if chunk else last_segment_id
                )
                committed_characters = characters + characters_added
                checkpoint = _checkpoint(
                    source_id=source_id,
                    last_segment_id=committed_last_id,
                    inserted=committed["inserted"],
                    skipped_existing=committed["skipped_existing"],
                    skipped_empty=committed["skipped_empty"],
                    errors=committed["errors"],
                    characters_embedded=committed_characters,
                )
                checkpoint["model"] = MODEL_KEY
                cursor.execute(
                    """UPDATE runs SET status = 'running', checkpoint = %s::jsonb,
                              error = NULL, finished_at = NULL WHERE id = %s::uuid""",
                    (json.dumps(checkpoint), run_id),
                )
            counters = committed
            last_segment_id = committed_last_id
            characters = committed_characters
            report.update(
                {
                    **counters,
                    "runs_id": run_id,
                    "last_segment_id": last_segment_id,
                    "characters_embedded": characters,
                    "duration_seconds": round(time.monotonic() - started, 3),
                }
            )
            _write_report(report_path, report)

        if run_id is None:
            raise EmbedError("External embedding run was not initialized")
        final_checkpoint = _checkpoint(
            source_id=source_id,
            last_segment_id=last_segment_id,
            inserted=counters["inserted"],
            skipped_existing=counters["skipped_existing"],
            skipped_empty=counters["skipped_empty"],
            errors=counters["errors"],
            characters_embedded=characters,
        )
        final_checkpoint["model"] = MODEL_KEY
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                """UPDATE runs SET status = 'done', checkpoint = %s::jsonb,
                          finished_at = now(), error = NULL WHERE id = %s::uuid""",
                (json.dumps(final_checkpoint), run_id),
            )
    except ConnectorError as exc:
        counters["errors"] += 1
        report.update(
            {
                "status": "failed",
                **counters,
                "runs_id": run_id,
                "error": f"Connector error ({exc.code})",
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
        _write_report(report_path, report)
        raise EmbedError(f"External embedding failed ({exc.code})") from exc
    except Exception as exc:
        from kiber_analyst.routing import WaitForQuota
        from kiber_analyst.worker import JobCancelled

        if isinstance(exc, (WaitForQuota, JobCancelled)):
            # The worker must postpone/cancel the job, not see a failed embed.
            raise
        counters["errors"] += 1
        safe_message = (
            str(exc) if isinstance(exc, EmbedError) else _safe_db_message(exc)
        )
        if conn is not None and run_id is not None:
            try:
                failed_checkpoint = _checkpoint(
                    source_id=source_id,
                    last_segment_id=last_segment_id,
                    inserted=counters["inserted"],
                    skipped_existing=counters["skipped_existing"],
                    skipped_empty=counters["skipped_empty"],
                    errors=counters["errors"],
                    characters_embedded=characters,
                )
                failed_checkpoint["model"] = MODEL_KEY
                with conn.transaction(), conn.cursor() as cursor:
                    cursor.execute(
                        """UPDATE runs SET status = 'failed', checkpoint = %s::jsonb,
                                  error = %s, finished_at = now() WHERE id = %s::uuid""",
                        (json.dumps(failed_checkpoint), safe_message, run_id),
                    )
            except Exception as checkpoint_error:  # noqa: BLE001
                safe_message += (
                    "; failed-run checkpoint update unavailable "
                    f"({type(checkpoint_error).__name__})"
                )
        report.update(
            {
                "status": "failed",
                **counters,
                "runs_id": run_id,
                "error": safe_message,
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
        _write_report(report_path, report)
        if isinstance(exc, EmbedError):
            raise
        raise EmbedError(_safe_db_message(exc)) from exc
    finally:
        if conn is not None:
            conn.close()
        if client is not None:
            client.close()

    report.update(
        {
            "status": "done",
            **counters,
            "runs_id": run_id,
            "last_segment_id": last_segment_id,
            "characters_embedded": characters,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    )
    _write_report(report_path, report)
    return report
