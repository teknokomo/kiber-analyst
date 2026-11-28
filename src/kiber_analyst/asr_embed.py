from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.embed import (
    BATCH_SIZE,
    FALLBACK_BATCH_SIZE,
    MAX_ATTEMPTS,
    RETRY_DELAYS_SECONDS,
)
from kiber_analyst.embedding_api import (
    DIMENSIONS,
    MODEL_KEY,
    ProviderError,
    create_client,
    embed_texts,
    load_provider_settings,
)
from kiber_analyst.errors import EmbedError, IngestError
from kiber_analyst.load import ENV_PATH, _connect, _required_env
from kiber_analyst.paths import write_json_atomic
from kiber_analyst.telegram_source import parse_telegram_source

_LOGGER = logging.getLogger(__name__)


class ASREmbedError(EmbedError):
    """ASR embedding generation or persistence failed safely."""


ASR_EMBED_SELECT_SQL = """
SELECT a.id, a.run_id::text, a.external_ref, a.segment_no, a.text,
       (e.id IS NOT NULL) AS has_embedding, ar.run_key
FROM asr_runs ar
JOIN asr_results r ON r.run_id = ar.id AND r.status = 'complete'
JOIN asr_segments a ON a.run_id = ar.id AND a.external_ref = r.external_ref
LEFT JOIN embeddings e
  ON e.project_id = ar.project_id
 AND e.owner_table = 'asr_segments'
 AND e.owner_id = a.id::text
 AND e.model = %s
WHERE ar.project_id = %s::uuid
  AND ar.status IN ('done', 'partial')
  AND (%s::text IS NULL OR ar.run_key = %s::text)
  AND (%s::text IS NULL OR EXISTS (
      SELECT 1
      FROM segments s
      JOIN documents d ON d.id = s.document_id
      JOIN sources so ON so.id = d.source_id
      WHERE s.external_ref = a.external_ref
        AND so.project_id = ar.project_id
        AND CASE
              WHEN d.meta->>'export_scope' = 'chat'
                OR (d.meta->>'export_scope' IS NULL AND d.meta->>'topic_id' IS NULL)
                THEN 'group'
              WHEN d.meta->>'topic_id' IS NOT NULL
                THEN ('topic:' || (d.meta->>'topic_id'))
            END = %s::text
  ))
ORDER BY a.id
"""


def _has_text(value: str) -> bool:
    return bool(value.strip())


def _safe_db_error(exc: Exception) -> str:
    sqlstate = getattr(exc, "sqlstate", None)
    suffix = f" SQLSTATE {sqlstate}" if isinstance(sqlstate, str) else ""
    return f"Knowledge database operation failed ({type(exc).__name__}{suffix})"


def _retry_batch(
    client: Any,
    texts: list[str],
    *,
    call_counter: list[int],
    sleep: Callable[[float], None],
    jitter: Callable[[float], float],
) -> tuple[list[list[float]], int | None]:
    for attempt in range(MAX_ATTEMPTS):
        try:
            call_counter[0] += 1
            return embed_texts(client, texts)
        except ProviderError as exc:
            if not exc.retryable or attempt + 1 >= MAX_ATTEMPTS:
                raise
            sleep(jitter(RETRY_DELAYS_SECONDS[attempt]))
    raise AssertionError("ASR embedding retry loop exhausted unexpectedly")


def _embed_resilient(
    client: Any,
    texts: list[str],
    *,
    call_counter: list[int],
    sleep: Callable[[float], None],
    jitter: Callable[[float], float],
) -> tuple[list[list[float]], int | None]:
    try:
        return _retry_batch(
            client,
            texts,
            call_counter=call_counter,
            sleep=sleep,
            jitter=jitter,
        )
    except ProviderError as exc:
        if exc.status_code in {401, 403} or len(texts) <= FALLBACK_BATCH_SIZE:
            raise
    vectors: list[list[float]] = []
    total_tokens = 0
    tokens_known = True
    for offset in range(0, len(texts), FALLBACK_BATCH_SIZE):
        part, token_count = _retry_batch(
            client,
            texts[offset : offset + FALLBACK_BATCH_SIZE],
            call_counter=call_counter,
            sleep=sleep,
            jitter=jitter,
        )
        vectors.extend(part)
        if token_count is None:
            tokens_known = False
        else:
            total_tokens += token_count
    return vectors, total_tokens if tokens_known else None


def _scope_key(project: str, run_key: str | None, source_id: str | None) -> str:
    return f"asr:{project}:{source_id or '*'}:{run_key or 'all'}:{MODEL_KEY}"


def _telegram_scope_key(source_id: str | None) -> str | None:
    if source_id is None:
        return None
    parsed = parse_telegram_source(source_id)
    if parsed is None or not parsed[0].startswith("-"):
        raise ASREmbedError("Invalid Telegram source ID")
    return "group" if parsed[1] is None else f"topic:{parsed[1]}"


def _get_run(
    cursor: Any, project_id: str, scope_key: str
) -> tuple[str, dict[str, Any]]:
    cursor.execute(
        """SELECT id::text, checkpoint FROM runs
           WHERE project_id = %s::uuid AND kind = 'asr_embed'
             AND status IN ('running', 'failed')
             AND checkpoint->>'scope_key' = %s
           ORDER BY started_at DESC LIMIT 1""",
        (project_id, scope_key),
    )
    row = cursor.fetchone()
    if row:
        checkpoint = row[1]
        if isinstance(checkpoint, str):
            try:
                checkpoint = json.loads(checkpoint)
            except json.JSONDecodeError:
                checkpoint = {}
        return str(row[0]), checkpoint if isinstance(checkpoint, dict) else {}
    checkpoint = {
        "scope_key": scope_key,
        "model": MODEL_KEY,
        "last_asr_segment_id": None,
        "inserted": 0,
        "skipped_existing": 0,
        "skipped_empty": 0,
    }
    cursor.execute(
        """INSERT INTO runs (project_id, kind, status, checkpoint)
           VALUES (%s::uuid, 'asr_embed', 'running', %s::jsonb)
           RETURNING id::text""",
        (project_id, json.dumps(checkpoint)),
    )
    row = cursor.fetchone()
    if not row:
        raise ASREmbedError("Could not create ASR embedding run")
    return str(row[0]), checkpoint


def _write_report(report_path: Path | None, report: dict[str, Any]) -> None:
    if report_path is None:
        return
    try:
        write_json_atomic(report_path, report)
    except OSError as exc:
        raise ASREmbedError(
            f"Cannot write ASR embedding report ({type(exc).__name__})"
        ) from exc


def embed_asr(
    *,
    project: str,
    run_key: str | None = None,
    all_runs: bool = False,
    source_id: str | None = None,
    limit: int | None = None,
    env_path: Path = ENV_PATH,
    api_env_path: Path = config_path("provider-credentials.env"),
    proxy_env_path: Path = config_path("provider-proxies.env"),
    provider_profile: str = "gemini_embed",
    embedding_call: Callable[[list[str]], tuple[list[list[float]], int | None]] | None = None,
    report_path: Path | None = None,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[float], float] = lambda seconds: random.uniform(
        seconds * 0.8, seconds * 1.2
    ),
) -> dict[str, Any]:
    if not isinstance(project, str) or not project.strip():
        raise ASREmbedError("Project slug is required")
    if (run_key is None) == (not all_runs):
        raise ASREmbedError("Specify exactly one of run_key or all_runs")
    source_scope = _telegram_scope_key(source_id)
    if limit is not None and limit < 1:
        raise ASREmbedError("--limit must be at least 1")

    client = None
    if embedding_call is None:
        try:
            settings = load_provider_settings(
                api_env_path, proxy_env_path, profile=provider_profile
            )
            client = create_client(settings)
        except Exception as exc:
            if isinstance(exc, ASREmbedError):
                raise
            raise ASREmbedError(
                f"Gemini embedding profile unavailable ({type(exc).__name__})"
            ) from exc
    try:
        config = _required_env(env_path)
        conn = _connect(config)
    except Exception as exc:
        if client is not None:
            client.close()
        if isinstance(exc, ASREmbedError):
            raise
        raise ASREmbedError(
            f"Cannot connect to knowledge database ({type(exc).__name__})"
        ) from exc

    started = time.monotonic()
    call_counter = [0]
    run_id: str | None = None
    counters = {"inserted": 0, "skipped_existing": 0, "skipped_empty": 0}
    try:
        try:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id::text FROM projects WHERE slug = %s", (project.strip(),)
                )
                project_row = cursor.fetchone()
                if not project_row:
                    raise ASREmbedError(f"Project {project!r} does not exist")
                project_id = str(project_row[0])
                cursor.execute(
                    ASR_EMBED_SELECT_SQL,
                    (MODEL_KEY, project_id, run_key, run_key, source_scope, source_scope),
                )
                records = [
                    {
                        "id": int(row[0]),
                        "run_id": str(row[1]),
                        "external_ref": row[2],
                        "segment_no": int(row[3]),
                        "text": row[4],
                        "has_embedding": bool(row[5]),
                        "run_key": row[6],
                    }
                    for row in cursor.fetchall()
                ]
                if run_key is not None and not records:
                    cursor.execute(
                        "SELECT 1 FROM asr_runs WHERE project_id = %s::uuid AND run_key = %s",
                        (project_id, run_key),
                    )
                    if not cursor.fetchone():
                        raise ASREmbedError(f"ASR run {run_key!r} does not exist")
                counters["skipped_empty"] = sum(
                    1 for item in records if not _has_text(item["text"])
                )
                counters["skipped_existing"] = sum(
                    1
                    for item in records
                    if _has_text(item["text"]) and item["has_embedding"]
                )
                pending = [
                    item
                    for item in records
                    if _has_text(item["text"]) and not item["has_embedding"]
                ]
                selected = pending[:limit] if limit is not None else pending
                scope_key = _scope_key(project.strip(), run_key, source_id)
                run_id, checkpoint = _get_run(cursor, project_id, scope_key)
        except ASREmbedError:
            raise
        except Exception as exc:
            raise ASREmbedError(_safe_db_error(exc)) from exc

        inserted = max(0, int(checkpoint.get("inserted", 0)))
        counters["inserted"] = inserted
        for offset in range(0, len(selected), BATCH_SIZE):
            batch = selected[offset : offset + BATCH_SIZE]
            texts = [item["text"] for item in batch]
            if embedding_call is not None:
                call_counter[0] += 1
                vectors, _token_count = embedding_call(texts)
            else:
                vectors, _token_count = _embed_resilient(
                    client,
                    texts,
                    call_counter=call_counter,
                    sleep=sleep,
                    jitter=jitter,
                )
            if len(vectors) != len(batch) or any(
                len(vector) != DIMENSIONS for vector in vectors
            ):
                raise ASREmbedError(
                    "Gemini API returned an unexpected embedding count or dimension"
                )
            batch_inserted = 0
            batch_conflicts = 0
            with conn.transaction(), conn.cursor() as cursor:
                for item, vector in zip(batch, vectors, strict=True):
                    cursor.execute(
                        """INSERT INTO embeddings
                             (project_id, owner_table, owner_id, model, embedding)
                           VALUES (%s::uuid, 'asr_segments', %s, %s, %s::vector)
                           ON CONFLICT (owner_table, owner_id, model) DO NOTHING""",
                        (
                            project_id,
                            str(item["id"]),
                            MODEL_KEY,
                            json.dumps(vector, separators=(",", ":")),
                        ),
                    )
                    if cursor.rowcount == 1:
                        batch_inserted += 1
                    else:
                        batch_conflicts += 1
                inserted += batch_inserted
                counters["inserted"] = inserted
                counters["skipped_existing"] += batch_conflicts
                checkpoint.update(
                    {
                        "last_asr_segment_id": batch[-1]["id"],
                        "inserted": inserted,
                        "skipped_existing": counters["skipped_existing"],
                        "skipped_empty": counters["skipped_empty"],
                    }
                )
                cursor.execute(
                    """UPDATE runs SET status = 'running', checkpoint = %s::jsonb,
                              finished_at = NULL, error = NULL WHERE id = %s::uuid""",
                    (json.dumps(checkpoint), run_id),
                )
        counters["inserted"] = inserted
        remaining = len(pending) - len(selected)
        checkpoint.update(
            {
                "inserted": counters["inserted"],
                "skipped_existing": counters["skipped_existing"],
                "skipped_empty": counters["skipped_empty"],
                "remaining": remaining,
            }
        )
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                """UPDATE runs SET status = 'done', checkpoint = %s::jsonb,
                          finished_at = now(), error = NULL WHERE id = %s::uuid""",
                (json.dumps(checkpoint), run_id),
            )
        report = {
            "status": "done",
            "project": project.strip(),
            "run": run_key if run_key is not None else "all",
            "source_id": source_id,
            "model": MODEL_KEY,
            "dimensions": DIMENSIONS,
            "profile": "gemini_embed",
            "batch_size": BATCH_SIZE,
            "runs_id": run_id,
            "segments_total": len(records),
            "segments_nonempty": sum(1 for item in records if _has_text(item["text"])),
            "selected": len(selected),
            "remaining": remaining,
            **counters,
            "api_calls": call_counter[0],
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        _write_report(report_path, report)
        return report
    except ASREmbedError:
        if run_id is not None:
            try:
                with conn.transaction(), conn.cursor() as cursor:
                    checkpoint["inserted"] = max(
                        int(checkpoint.get("inserted", 0)), counters["inserted"]
                    )
                    cursor.execute(
                        """UPDATE runs SET status = 'failed', checkpoint = %s::jsonb,
                                  finished_at = now(), error = %s WHERE id = %s::uuid""",
                        (json.dumps(checkpoint), "ASR embedding failed", run_id),
                    )
            except Exception as checkpoint_exc:  # noqa: BLE001 - preserve original failure
                _LOGGER.warning(
                    "Could not save failed ASR embedding checkpoint (%s)",
                    type(checkpoint_exc).__name__,
                )
        raise
    except Exception as exc:
        if run_id is not None:
            try:
                with conn.transaction(), conn.cursor() as cursor:
                    checkpoint["inserted"] = max(
                        int(checkpoint.get("inserted", 0)), counters["inserted"]
                    )
                    cursor.execute(
                        """UPDATE runs SET status = 'failed', checkpoint = %s::jsonb,
                                  finished_at = now(), error = %s WHERE id = %s::uuid""",
                        (json.dumps(checkpoint), "ASR embedding failed", run_id),
                    )
            except Exception as checkpoint_exc:  # noqa: BLE001 - preserve original failure
                _LOGGER.warning(
                    "Could not save failed ASR embedding checkpoint (%s)",
                    type(checkpoint_exc).__name__,
                )
        if isinstance(exc, IngestError):
            raise
        raise ASREmbedError(
            str(exc) if isinstance(exc, ProviderError) else _safe_db_error(exc)
        ) from exc
    finally:
        if client is not None:
            client.close()
        conn.close()
