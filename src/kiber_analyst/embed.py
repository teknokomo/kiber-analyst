from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.embedding_api import (
    MODEL_KEY,
    ProviderError,
    create_client,
    embed_texts,
    load_provider_settings,
)
from kiber_analyst.errors import EmbedError, IngestError
from kiber_analyst.load import _connect, _required_env
from kiber_analyst.paths import read_json, write_json_atomic

BATCH_SIZE = 64
FALLBACK_BATCH_SIZE = 16
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (60, 180, 300)


def _manifest_path(job_dir: Path) -> Path:
    for candidate in (
        job_dir / "normalized" / "manifest.json",
        job_dir / "manifest.json",
    ):
        if candidate.is_file():
            return candidate
    raise EmbedError("Job manifest.json was not found")


def _input_path(job_dir: Path) -> tuple[Path, bool]:
    candidates = (
        (job_dir / "segments.jsonl", False),
        (job_dir / "normalized" / "segments.jsonl", False),
        (job_dir / "normalized" / "messages.jsonl", True),
    )
    for path, normalized_messages in candidates:
        if path.is_file():
            return path, normalized_messages
    raise EmbedError("Job has neither segments.jsonl nor normalized/messages.jsonl")


def _read_records(path: Path, *, normalized_messages: bool) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise EmbedError(
                        f"Invalid JSONL at {path.name}:{line_number}"
                    ) from exc
                if not isinstance(item, dict):
                    raise EmbedError(f"JSONL row {line_number} must be an object")
                if normalized_messages:
                    chat_id = item.get("chat_id")
                    message_id = item.get("message_id")
                    if chat_id is None or message_id is None:
                        raise EmbedError(
                            f"Normalized message at row {line_number} lacks identifiers"
                        )
                    external_ref = f"tg:{chat_id}:{message_id}"
                else:
                    external_ref = item.get("external_ref")
                content_hash = item.get("content_hash")
                text = item.get("text")
                if not isinstance(external_ref, str) or not external_ref:
                    raise EmbedError(f"Segment at row {line_number} lacks external_ref")
                if external_ref in seen_refs:
                    raise EmbedError(
                        f"Duplicate external_ref in job input at row {line_number}"
                    )
                if not isinstance(content_hash, str) or not content_hash:
                    raise EmbedError(f"Segment at row {line_number} lacks content_hash")
                if text is None:
                    text = ""
                if not isinstance(text, str):
                    raise EmbedError(
                        f"Segment text at row {line_number} is not a string"
                    )
                seen_refs.add(external_ref)
                records.append(
                    {
                        "external_ref": external_ref,
                        "content_hash": content_hash,
                        "text": text,
                    }
                )
    except OSError as exc:
        raise EmbedError(f"Cannot read job segments ({type(exc).__name__})") from exc
    return records


def _project_slug(manifest: dict[str, Any], override: str | None) -> str:
    load = manifest.get("load")
    loaded_slug = load.get("project_slug") if isinstance(load, dict) else None
    slug = override or manifest.get("project_slug") or loaded_slug or "default"
    if not isinstance(slug, str) or not slug.strip():
        raise EmbedError("Project slug is missing")
    return slug.strip()


def _snapshot_sha256(manifest: dict[str, Any]) -> str:
    value = manifest.get("snapshot_sha256")
    if not isinstance(value, str) or len(value) != 64:
        raise EmbedError("manifest.json has no valid snapshot_sha256")
    return value.lower()


def _decode_checkpoint(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _checkpoint(
    *,
    job_dir: Path,
    snapshot_sha256: str,
    last_segment_id: int | None,
    inserted: int,
    skipped_existing: int,
    skipped_empty: int,
    errors: int,
    input_characters_embedded: int,
) -> dict[str, Any]:
    return {
        "job_dir": str(job_dir.resolve()),
        "snapshot_sha256": snapshot_sha256,
        "last_segment_id": last_segment_id,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "skipped_empty": skipped_empty,
        "errors": errors,
        "input_characters_embedded": input_characters_embedded,
    }


def _get_or_create_run(
    cursor: Any,
    project_id: str,
    job_dir: Path,
    snapshot_sha256: str,
) -> tuple[str, dict[str, Any], bool]:
    resolved = str(job_dir.resolve())
    cursor.execute(
        """SELECT id, checkpoint FROM runs
           WHERE project_id = %s::uuid AND kind = 'telegram_embed'
             AND status IN ('running', 'failed')
             AND checkpoint->>'job_dir' = %s
             AND checkpoint->>'snapshot_sha256' = %s
           ORDER BY started_at DESC LIMIT 1""",
        (project_id, resolved, snapshot_sha256),
    )
    row = cursor.fetchone()
    if row:
        return str(row[0]), _decode_checkpoint(row[1]), True
    initial = _checkpoint(
        job_dir=job_dir,
        snapshot_sha256=snapshot_sha256,
        last_segment_id=None,
        inserted=0,
        skipped_existing=0,
        skipped_empty=0,
        errors=0,
        input_characters_embedded=0,
    )
    cursor.execute(
        """INSERT INTO runs (project_id, kind, status, checkpoint)
           VALUES (%s::uuid, 'telegram_embed', 'running', %s::jsonb)
           RETURNING id""",
        (project_id, json.dumps(initial)),
    )
    row = cursor.fetchone()
    if not row:
        raise EmbedError("Could not create an embedding run")
    return str(row[0]), initial, False


def _retry_embed(
    client: Any,
    texts: list[str],
    *,
    sleep: Callable[[float], None],
    jitter: Callable[[float], float],
) -> tuple[list[list[float]], int | None]:
    for attempt in range(MAX_ATTEMPTS):
        try:
            return embed_texts(client, texts)
        except ProviderError as exc:
            if not exc.retryable or attempt + 1 >= MAX_ATTEMPTS:
                raise
            delay = RETRY_DELAYS_SECONDS[attempt]
            sleep(jitter(delay))
    raise AssertionError("retry loop exhausted unexpectedly")


def _embed_resilient(
    client: Any,
    texts: list[str],
    *,
    sleep: Callable[[float], None],
    jitter: Callable[[float], float],
) -> tuple[list[list[float]], int | None]:
    try:
        return _retry_embed(client, texts, sleep=sleep, jitter=jitter)
    except ProviderError as exc:
        if exc.status_code in {401, 403} or len(texts) <= FALLBACK_BATCH_SIZE:
            raise
        vectors: list[list[float]] = []
        token_count = 0
        token_count_known = True
        for start in range(0, len(texts), FALLBACK_BATCH_SIZE):
            part, part_tokens = _retry_embed(
                client,
                texts[start : start + FALLBACK_BATCH_SIZE],
                sleep=sleep,
                jitter=jitter,
            )
            vectors.extend(part)
            if part_tokens is None:
                token_count_known = False
            else:
                token_count += part_tokens
        return vectors, token_count if token_count_known else None


def _safe_db_message(exc: Exception) -> str:
    sqlstate = getattr(exc, "sqlstate", None)
    suffix = f" SQLSTATE {sqlstate}" if isinstance(sqlstate, str) else ""
    return f"Knowledge database operation failed ({type(exc).__name__}{suffix})"


def _write_report(job_dir: Path, report: dict[str, Any]) -> None:
    try:
        write_json_atomic(job_dir / "embed-report.json", report)
    except OSError as exc:
        raise EmbedError(
            f"Cannot write embed-report.json ({type(exc).__name__})"
        ) from exc


def embed_job(
    job_dir: Path,
    *,
    project: str | None = None,
    env_path: Path = config_path("knowledge.conf"),
    api_env_path: Path = config_path("provider-credentials.env"),
    proxy_env_path: Path = config_path("provider-proxies.env"),
    provider_profile: str = "gemini_embed",
    embedding_call: Callable[[list[str]], tuple[list[list[float]], int | None]] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[float], float] = lambda seconds: random.uniform(
        seconds * 0.8, seconds * 1.2
    ),
) -> dict[str, Any]:
    started = time.monotonic()
    manifest_path = _manifest_path(job_dir)
    try:
        manifest = read_json(manifest_path)
    except (OSError, ValueError, TypeError) as exc:
        raise EmbedError(f"Cannot read job manifest ({type(exc).__name__})") from exc
    input_path, normalized_messages = _input_path(job_dir)
    records = _read_records(input_path, normalized_messages=normalized_messages)
    slug = _project_slug(manifest, project)
    snapshot_sha256 = _snapshot_sha256(manifest)
    client = None
    if embedding_call is None:
        provider_settings = load_provider_settings(
            api_env_path, proxy_env_path, profile=provider_profile
        )
        client = create_client(provider_settings)
    try:
        db_config = _required_env(env_path)
    except Exception as exc:
        if client is not None:
            client.close()
        if isinstance(exc, IngestError):
            raise
        raise EmbedError(
            f"Cannot read knowledge database settings ({type(exc).__name__})"
        ) from exc

    counters = {"inserted": 0, "skipped_existing": 0, "skipped_empty": 0, "errors": 0}
    total_characters = 0
    run_id: str | None = None
    last_segment_id: int | None = None
    report: dict[str, Any] = {
        "status": "running",
        "job_dir": str(job_dir.resolve()),
        "snapshot_sha256": snapshot_sha256,
        "model": MODEL_KEY,
        "segments_total": len(records),
        **counters,
        "duration_seconds": 0,
        "runs_id": None,
        "input_characters_embedded": 0,
        "input_tokens_estimated": 0,
        "token_estimate_method": "Approximation: Unicode character count divided by 4",
    }
    conn = None
    try:
        conn = _connect(db_config)
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute("SELECT id FROM projects WHERE slug = %s", (slug,))
            project_row = cursor.fetchone()
            if not project_row:
                raise EmbedError(f"Project {slug!r} does not exist")
            project_id = str(project_row[0])
            external_refs = [row["external_ref"] for row in records]
            if external_refs:
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
                       WHERE so.project_id = %s::uuid
                         AND s.external_ref = ANY(%s)
                       ORDER BY s.id""",
                    (MODEL_KEY, project_id, external_refs),
                )
                db_rows = cursor.fetchall()
            else:
                db_rows = []
            by_ref = {row[1]: row for row in db_rows}
            if len(by_ref) != len(records):
                raise EmbedError(
                    f"Job/database segment mismatch: input={len(records)}, matched={len(by_ref)}"
                )
            mismatch_count = 0
            ordered: list[dict[str, Any]] = []
            for record in records:
                row = by_ref[record["external_ref"]]
                if row[2] != record["content_hash"]:
                    mismatch_count += 1
                ordered.append(
                    {
                        "id": int(row[0]),
                        "external_ref": row[1],
                        "content_hash": row[2],
                        "text": record["text"],
                        "has_embedding": bool(row[4]),
                    }
                )
            if mismatch_count:
                raise EmbedError(
                    f"Job/database content_hash mismatch on {mismatch_count} segments"
                )
            ordered.sort(key=lambda item: item["id"])
            run_id, resume, _ = _get_or_create_run(
                cursor, project_id, job_dir, snapshot_sha256
            )
            last_value = resume.get("last_segment_id")
            last_segment_id = int(last_value) if last_value is not None else None
            for name in counters:
                counters[name] = int(resume.get(name, 0))
            total_characters = int(resume.get("input_characters_embedded", 0))
            if last_segment_id is not None:
                ordered = [row for row in ordered if row["id"] > last_segment_id]

        for chunk_start in range(0, len(ordered), BATCH_SIZE):
            chunk = ordered[chunk_start : chunk_start + BATCH_SIZE]
            pending = [
                item
                for item in chunk
                if item["text"].strip() and not item["has_embedding"]
            ]
            newly_skipped_existing = sum(
                1 for item in chunk if item["text"].strip() and item["has_embedding"]
            )
            newly_skipped_empty = sum(1 for item in chunk if not item["text"].strip())
            vectors: list[list[float]] = []
            if pending:
                texts = [item["text"] for item in pending]
                if embedding_call is not None:
                    vectors, _ = embedding_call(texts)
                else:
                    vectors, _ = _embed_resilient(
                        client,
                        texts,
                        sleep=sleep,
                        jitter=jitter,
                    )
                if len(vectors) != len(pending):
                    raise EmbedError(
                        "Gemini API returned an unexpected embedding count"
                    )

            with conn.transaction(), conn.cursor() as cursor:
                inserted_here = 0
                skipped_conflict = 0
                characters_added = 0
                for item, vector in zip(pending, vectors, strict=True):
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
                committed_counters = dict(counters)
                committed_counters["inserted"] += inserted_here
                committed_counters["skipped_existing"] += (
                    newly_skipped_existing + skipped_conflict
                )
                committed_counters["skipped_empty"] += newly_skipped_empty
                committed_last_id = chunk[-1]["id"] if chunk else last_segment_id
                committed_characters = total_characters + characters_added
                checkpoint = _checkpoint(
                    job_dir=job_dir,
                    snapshot_sha256=snapshot_sha256,
                    last_segment_id=committed_last_id,
                    inserted=committed_counters["inserted"],
                    skipped_existing=committed_counters["skipped_existing"],
                    skipped_empty=committed_counters["skipped_empty"],
                    errors=committed_counters["errors"],
                    input_characters_embedded=committed_characters,
                )
                cursor.execute(
                    """UPDATE runs SET status = 'running', checkpoint = %s::jsonb,
                              error = NULL, finished_at = NULL WHERE id = %s::uuid""",
                    (json.dumps(checkpoint), run_id),
                )
            counters = committed_counters
            last_segment_id = committed_last_id
            total_characters = committed_characters
            report.update(
                {
                    **counters,
                    "runs_id": run_id,
                    "last_segment_id": last_segment_id,
                    "input_characters_embedded": total_characters,
                    "input_tokens_estimated": round(total_characters / 4),
                    "duration_seconds": round(time.monotonic() - started, 3),
                }
            )
            _write_report(job_dir, report)

        if run_id is None:
            raise EmbedError("Embedding run was not initialized")
        final_checkpoint = _checkpoint(
            job_dir=job_dir,
            snapshot_sha256=snapshot_sha256,
            last_segment_id=last_segment_id,
            inserted=counters["inserted"],
            skipped_existing=counters["skipped_existing"],
            skipped_empty=counters["skipped_empty"],
            errors=counters["errors"],
            input_characters_embedded=total_characters,
        )
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                """UPDATE runs SET status = 'done', checkpoint = %s::jsonb,
                          finished_at = now(), error = NULL WHERE id = %s::uuid""",
                (json.dumps(final_checkpoint), run_id),
            )
    except Exception as exc:
        counters["errors"] += 1
        safe_message = (
            str(exc) if isinstance(exc, EmbedError) else _safe_db_message(exc)
        )
        if conn is not None and run_id is not None:
            try:
                failed_checkpoint = _checkpoint(
                    job_dir=job_dir,
                    snapshot_sha256=snapshot_sha256,
                    last_segment_id=last_segment_id,
                    inserted=counters["inserted"],
                    skipped_existing=counters["skipped_existing"],
                    skipped_empty=counters["skipped_empty"],
                    errors=counters["errors"],
                    input_characters_embedded=total_characters,
                )
                with conn.transaction(), conn.cursor() as cursor:
                    cursor.execute(
                        """UPDATE runs SET status = 'failed', checkpoint = %s::jsonb,
                                  error = %s, finished_at = now() WHERE id = %s::uuid""",
                        (json.dumps(failed_checkpoint), safe_message, run_id),
                    )
            except Exception as checkpoint_error:  # noqa: BLE001 - avoid leaking DB details
                safe_message += (
                    "; failed-run checkpoint update unavailable "
                    f"({type(checkpoint_error).__name__})"
                )
        report.update(
            {
                "status": "failed",
                **counters,
                "runs_id": run_id,
                "last_segment_id": last_segment_id,
                "input_characters_embedded": total_characters,
                "input_tokens_estimated": round(total_characters / 4),
                "duration_seconds": round(time.monotonic() - started, 3),
                "error": safe_message,
            }
        )
        try:
            _write_report(job_dir, report)
        except EmbedError:
            pass
        if isinstance(exc, IngestError):
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
            "input_characters_embedded": total_characters,
            "input_tokens_estimated": round(total_characters / 4),
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    )
    _write_report(job_dir, report)
    return report
