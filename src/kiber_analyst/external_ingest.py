"""external_ingest orchestration: discover → fetch → normalize → load (ТЗ §5.3).

Each stage writes artifacts into the job directory and can be repeated: a
stage re-runs from its own output files when the inputs (source_id + since)
match. The connector does all network work; normalize is offline and
deterministic; load is delegated to external_load in one transaction.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from kiber_analyst.connectors import PROVIDERS, get_connector
from kiber_analyst.connectors.base import Connector, RawItem, RawThread, ThreadRef
from kiber_analyst.connectors.errors import ConnectorCodes, ConnectorError
from kiber_analyst.connectors.normalize import is_nonempty
from kiber_analyst.connectors.source_ids import parse_source_id
from kiber_analyst.errors import IngestError
from kiber_analyst.external_load import load_external
from kiber_analyst.paths import write_json_atomic

DEFAULT_SINCE_DAYS = 90
OVERLAP_SECONDS = 5 * 60
API_VERSIONS = {"github": "2022-11-28", "gitlab": "v4", "gitverse": "v1"}

_EXTERNAL_FILES = (
    "discovered.json",
    "fetched.jsonl",
    "normalized/threads.jsonl",
    "normalized/items.jsonl",
    "normalized/manifest.json",
)


def external_paths(job_dir: Path) -> dict[str, Path]:
    return {
        "root": job_dir,
        "raw": job_dir / "raw",
        "discovered": job_dir / "discovered.json",
        "fetched": job_dir / "fetched.jsonl",
        "normalized": job_dir / "normalized",
        "threads": job_dir / "normalized" / "threads.jsonl",
        "items": job_dir / "normalized" / "items.jsonl",
        "manifest": job_dir / "normalized" / "manifest.json",
    }


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _default_since() -> str:
    return (
        (datetime.now(UTC) - timedelta(days=DEFAULT_SINCE_DAYS))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_since(since: str | None) -> str | None:
    if since is None:
        return None
    try:
        parsed = datetime.fromisoformat(since.replace("Z", "+00:00"))
    except ValueError:
        raise IngestError("--since must be an ISO 8601 date or datetime") from None
    if parsed.tzinfo is None:
        # A date-only value is a UTC cutoff, never the host's local timezone.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _ref_dict(ref: ThreadRef) -> dict[str, Any]:
    return {
        "provider": ref.provider,
        "source_id": ref.source_id,
        "thread_ref": ref.thread_ref,
        "kind": ref.kind,
        "number": ref.number,
        "updated_at": ref.updated_at,
        "state": ref.state,
        "title": ref.title,
    }


def _ref_from_dict(payload: dict[str, Any]) -> ThreadRef:
    return ThreadRef(
        provider=payload["provider"],
        source_id=payload["source_id"],
        thread_ref=payload["thread_ref"],
        kind=payload["kind"],
        number=int(payload["number"]),
        updated_at=payload.get("updated_at"),
        state=payload.get("state"),
        title=payload.get("title"),
    )


def _raw_thread_line(ref: ThreadRef, raw: RawThread) -> str:
    payload = {
        "ref": _ref_dict(ref),
        "provider": ref.provider,
        "source_id": ref.source_id,
        "body": raw.body,
        "items": [
            {
                "item_kind": item.item_kind,
                "external_id": item.external_id,
                "data": item.data,
                "url": item.url,
            }
            for item in raw.items
        ],
        "complete": raw.complete,
        "errors": raw.errors,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compact_jsonl_atomic(path: Path, lines: list[str]) -> None:
    """Replace a completed append journal without risking its last good copy."""
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            for line in lines:
                stream.write(line.encode("utf-8"))
                stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def run_external_ingest(
    *,
    provider: str,
    source_id: str,
    project_slug: str,
    since: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    artifact_dir: Path,
    env_path: Path,
    sleep: Any = None,
    jitter: Any = None,
    transport: Any = None,
    connector: Connector | None = None,
    check_stop: Any = None,
) -> dict[str, Any]:
    """Run the full external ingest pipeline and return its report."""
    if provider not in PROVIDERS:
        raise ConnectorError(
            ConnectorCodes.UNSUPPORTED_PROVIDER,
            f"Provider {provider!r} is not supported",
        )
    parsed = parse_source_id(source_id)
    if parsed is None or parsed.provider != provider:
        raise ConnectorError(
            ConnectorCodes.SEARCH_INVALID_SOURCE,
            f"source_id is not a valid {provider} repository identifier",
        )
    since_effective = _validate_since(since) or _default_since()
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise IngestError("--limit must be a positive integer")

    paths = external_paths(artifact_dir)
    paths["raw"].mkdir(mode=0o700, parents=True, exist_ok=True)
    paths["normalized"].mkdir(mode=0o700, parents=True, exist_ok=True)
    connector = connector or get_connector(
        provider, raw_dir=paths["raw"]
    )
    connector.validate_source(source_id)
    checkpoint_before = {"since": since_effective, "source_id": source_id}
    errors: list[dict[str, str]] = []
    partial_reasons: list[str] = []

    def _check_stop() -> None:
        if check_stop is not None:
            check_stop()

    # Stage 1: discover (no full comment text is downloaded here).
    refs: list[ThreadRef] = []
    for ref in connector.discover(source_id, {"since": since_effective}):
        _check_stop()
        refs.append(ref)
        if limit is not None and len(refs) >= limit:
            break
    discovery_state = {
        "source_id": source_id,
        "provider": provider,
        "since": since_effective,
        "limit": limit,
        "refs": [_ref_dict(ref) for ref in refs],
    }
    write_json_atomic(paths["discovered"], discovery_state)
    discovery_errors = list(getattr(connector, "discovery_errors", []))
    if discovery_errors:
        errors.extend(discovery_errors)
        partial_reasons.append("partial_discovery")

    # Stage 2: fetch (raw answers are dumped to the job directory by the
    # connector itself); per-thread non-retryable failures are recorded, and
    # retryable ones propagate to the worker. Successful threads are appended
    # durably before another request starts, then compacted atomically on success.
    fetched_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    max_updated: datetime | None = None

    def _parse_timestamp(candidate: Any) -> datetime | None:
        if not isinstance(candidate, str) or not candidate:
            return None
        try:
            parsed_date = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=UTC)
        return parsed_date.astimezone(UTC)

    def _track_watermark(*candidates: Any) -> None:
        nonlocal max_updated
        for candidate in candidates:
            parsed_date = _parse_timestamp(candidate)
            if parsed_date is None:
                continue
            if max_updated is None or parsed_date > max_updated:
                max_updated = parsed_date

    existing_lines: dict[str, str] = {}
    existing_payloads: dict[str, dict[str, Any]] = {}
    valid_cache_bytes = 0
    if paths["fetched"].is_file():
        with paths["fetched"].open("rb") as stream:
            while line_bytes := stream.readline():
                if not line_bytes.endswith(b"\n"):
                    continue
                try:
                    line = line_bytes.decode("utf-8").rstrip("\n")
                    if not line.strip():
                        valid_cache_bytes = stream.tell()
                        continue
                    payload = json.loads(line)
                    thread_ref_value = payload["ref"]["thread_ref"]
                except (json.JSONDecodeError, KeyError, TypeError):
                    break
                except UnicodeDecodeError:
                    break
                existing_lines[thread_ref_value] = line
                existing_payloads[thread_ref_value] = payload
                _track_watermark(payload["ref"].get("updated_at"),
                                 (payload.get("body") or {}).get("updated_at"))
                valid_cache_bytes = stream.tell()
        if paths["fetched"].stat().st_size > valid_cache_bytes:
            # Discard a torn or malformed tail left by an interrupted append,
            # preserving every complete JSONL record before it.
            with paths["fetched"].open("r+b") as stream:
                stream.truncate(valid_cache_bytes)
                stream.flush()
                os.fsync(stream.fileno())
    journal_created = not paths["fetched"].exists()
    with paths["fetched"].open("ab") as stream:
        for ref in refs:
            _check_stop()
            cached = existing_payloads.get(ref.thread_ref)
            discovered_updated = _parse_timestamp(ref.updated_at)
            cached_dates = (
                _parse_timestamp((cached.get("ref") or {}).get("updated_at")),
                _parse_timestamp((cached.get("body") or {}).get("updated_at")),
            ) if cached is not None else ()
            cached_updated = max(
                (value for value in cached_dates if value is not None),
                default=None,
            )
            cached_complete = bool(
                cached is not None
                and cached.get("complete", True)
                and not cached.get("errors")
            )
            if cached_complete and not (
                discovered_updated is not None
                and (cached_updated is None or discovered_updated > cached_updated)
            ):
                continue
            try:
                raw = connector.fetch_thread(ref)
            except ConnectorError as exc:
                if exc.retryable:
                    raise
                errors.append(
                    {
                        "code": exc.code,
                        "thread_ref": ref.thread_ref,
                        "detail": "thread fetch failed",
                    }
                )
                partial_reasons.append("thread_fetch_failed")
                continue
            _track_watermark(ref.updated_at, (raw.body or {}).get("updated_at"))
            line = _raw_thread_line(ref, raw)
            existing_lines[ref.thread_ref] = line
            existing_payloads[ref.thread_ref] = json.loads(line)
            stream.write(line.encode("utf-8") + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
            if journal_created:
                directory_fd = os.open(paths["fetched"].parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                journal_created = False
    # A successful pass compacts any replaced cached records. If a retryable
    # fetch fails above, the append-only journal still preserves each completed
    # fetch and the next pass deduplicates by thread_ref, keeping the last line.
    _compact_jsonl_atomic(paths["fetched"], list(existing_lines.values()))

    # Stage 3: normalize (offline, deterministic). Every discovered thread
    # starts as incomplete; only successfully normalized ones flip to True.
    threads_out: list[dict[str, Any]] = []
    items_out: list[dict[str, Any]] = []
    threads_complete: dict[str, bool] = {ref.thread_ref: False for ref in refs}
    nonempty_count = 0
    skipped_empty_count = 0
    with paths["fetched"].open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            payload = json.loads(line)
            ref = _ref_from_dict(payload["ref"])
            raw = RawThread(
                ref=ref,
                body=payload.get("body"),
                items=[
                    RawItem(
                        item_kind=entry["item_kind"],
                        external_id=entry.get("external_id"),
                        data=entry.get("data") or {},
                        url=entry.get("url"),
                    )
                    for entry in payload.get("items", [])
                ],
                complete=bool(payload.get("complete", True)),
                errors=list(payload.get("errors", [])),
            )
            normalized = connector.normalize(raw)
            threads_out.append(normalized.thread)
            is_complete = bool(raw.complete and not raw.errors)
            threads_complete[ref.thread_ref] = is_complete
            if not is_complete:
                partial_reasons.append("incomplete_thread")
            for item in normalized.items:
                items_out.append(item)
                if item["item_kind"] == "event":
                    continue
                if is_nonempty(item):
                    nonempty_count += 1
                else:
                    skipped_empty_count += 1
            errors.extend(raw.errors)

    _check_stop()
    snapshot_sha256 = hashlib.sha256(
        _canonical({"threads": threads_out, "items": items_out})
    ).hexdigest()
    # Completeness and error codes are part of the identity: an identical
    # bundle fetched with a different completeness must still reconcile
    # deletions on reload.
    import_key = hashlib.sha256(
        _canonical({
            "source_id": source_id,
            "snapshot_sha256": snapshot_sha256,
            "threads_complete": threads_complete,
            "error_codes": sorted({str(entry.get("code") or "") for entry in errors}),
        })
    ).hexdigest()
    # Never advance the incremental watermark past threads that were not
    # processed: a failed fetch or a pilot limit keeps the previous cutoff.
    suppressed: str | None = None
    if partial_reasons:
        suppressed = "partial"
    elif limit is not None:
        suppressed = "limit"
    if suppressed is not None or max_updated is None:
        watermark = since_effective
    else:
        watermark = (
            (max_updated - timedelta(seconds=OVERLAP_SECONDS))
            .isoformat()
            .replace("+00:00", "Z")
        )
    checkpoint_after = {
        "since": watermark,
        "fetched_at": fetched_at,
        "threads_discovered": len(refs),
        "threads_fetched": len(threads_out),
    }
    if suppressed is not None:
        checkpoint_after["watermark_suppressed"] = suppressed
    manifest = {
        "source_id": source_id,
        "provider": provider,
        "api_version": API_VERSIONS[provider],
        "snapshot_sha256": snapshot_sha256,
        "external_import_key": import_key,
        "archive_uri": f"external://{source_id}/{snapshot_sha256}",
        "archive_size_bytes": paths["fetched"].stat().st_size if paths["fetched"].is_file() else 0,
        "thread_count": len(threads_out),
        "item_count": len(items_out),
        "nonempty_count": nonempty_count,
        "skipped_empty_count": skipped_empty_count,
        "fetched_at": fetched_at,
        "checkpoint_before": checkpoint_before,
        "checkpoint_after": checkpoint_after,
        "threads_complete": threads_complete,
        "limit_applied": limit,
        "errors": errors,
        "partial_reasons": sorted(set(partial_reasons)),
        "project_slug": project_slug,
        "pipeline_version": "external-ingest/1",
    }
    _write_jsonl(paths["threads"], threads_out)
    _write_jsonl(paths["items"], items_out)
    write_json_atomic(paths["manifest"], manifest)

    # Stage 4: load (one transaction; dry-run rolls back everything).
    _check_stop()
    if dry_run:
        load_result = load_external(
            paths["root"], env_path=env_path, project_slug=project_slug, dry_run=True
        )
    else:
        load_result = load_external(
            paths["root"], env_path=env_path, project_slug=project_slug, dry_run=False
        )
    status = "partial" if partial_reasons else "done"
    return {
        "status": status,
        "provider": provider,
        "source_id": source_id,
        "project_slug": project_slug,
        "threads": len(threads_out),
        "items": len(items_out),
        "nonempty": nonempty_count,
        "skipped_empty": skipped_empty_count,
        "partial_reasons": sorted(set(partial_reasons)),
        "checkpoint_after": checkpoint_after,
        "load": load_result,
        "snapshot_sha256": snapshot_sha256,
        "dry_run": dry_run,
    }


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(
                json.dumps(
                    record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                + "\n"
            )
        stream.flush()
    temporary.replace(path)
    temporary.parent.chmod(0o700)
