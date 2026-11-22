from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from kiber_analyst.errors import LoadError
from kiber_analyst.load import ENV_PATH, _connect, _required_env
from kiber_analyst.paths import read_json, write_json_atomic

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_SAFE_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,99}$")
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_TELEGRAM_REF = re.compile(r"^tg:-?\d+:\d+$")
_STATUSES = {"complete", "failed", "pending"}


class ASRLoadError(LoadError):
    """An ASR manifest or database load failed validation."""


class _DryRunRollback(Exception):
    """Internal signal to roll back a fully executed ASR load."""


def _safe_job_file(root: Path, relative: Any, *, label: str) -> tuple[Path, str]:
    if not isinstance(relative, str) or not relative:
        raise ASRLoadError(f"ASR {label} path must be a non-empty relative path")
    candidate_path = Path(relative)
    if candidate_path.is_absolute() or ".." in candidate_path.parts:
        raise ASRLoadError(f"ASR {label} path must stay inside the job directory")
    candidate = root / candidate_path
    current = root
    for part in candidate_path.parts:
        current = current / part
        if current.is_symlink():
            raise ASRLoadError(f"ASR {label} path must not contain symlinks")
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ASRLoadError(f"ASR {label} file is missing or outside the job") from exc
    if not resolved.is_file():
        raise ASRLoadError(f"ASR {label} path is not a file")
    return resolved, candidate_path.as_posix()


def _selected_manifest_items(
    raw_items: list[Any], external_refs: list[str] | tuple[str, ...] | set[str] | frozenset[str] | None,
) -> list[dict[str, Any]]:
    if external_refs is None:
        if any(not isinstance(item, dict) for item in raw_items):
            raise ASRLoadError("Every ASR manifest item must be an object")
        return raw_items
    if isinstance(external_refs, (str, bytes)) or not isinstance(
        external_refs, (list, tuple, set, frozenset)
    ):
        raise ASRLoadError("ASR external_refs must be a collection of Telegram references")
    requested = list(external_refs)
    if (
        not requested
        or any(
            not isinstance(ref, str) or not _TELEGRAM_REF.fullmatch(ref)
            for ref in requested
        )
        or len(requested) != len(set(requested))
    ):
        raise ASRLoadError("ASR external_refs must contain unique valid Telegram references")
    wanted = set(requested)
    selected: list[dict[str, Any]] = []
    found: set[str] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            raise ASRLoadError("Every ASR manifest item must be an object")
        ref = item.get("external_ref")
        if not isinstance(ref, str) or ref not in wanted:
            continue
        if ref in found:
            raise ASRLoadError(f"Duplicate ASR external_ref: {ref}")
        found.add(ref)
        selected.append(item)
    missing = wanted - found
    if missing:
        raise ASRLoadError(
            f"ASR manifest is missing selected external_ref: {sorted(missing)[0]}"
        )
    return selected


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ASRLoadError(f"Cannot read ASR file ({type(exc).__name__})") from exc
    return digest.hexdigest()


def _find_audio_file(root: Path, relative: Path) -> Path | None:
    unpacked = root / "unpacked"
    if unpacked.is_symlink():
        raise ASRLoadError("ASR unpacked directory must not be a symlink")
    if not unpacked.is_dir():
        return None
    try:
        candidates = [unpacked / relative]
        candidates.extend(
            child / relative
            for child in unpacked.iterdir()
            if not child.is_symlink() and child.is_dir()
        )
    except OSError as exc:
        raise ASRLoadError(
            f"Cannot inspect ASR audio directory ({type(exc).__name__})"
        ) from exc
    matches = [candidate for candidate in candidates if candidate.is_file()]
    if len(matches) > 1:
        raise ASRLoadError("ASR media_path matches multiple files in the job")
    if not matches:
        return None
    candidate = matches[0]
    current = root
    for part in candidate.relative_to(root).parts:
        current = current / part
        if current.is_symlink():
            raise ASRLoadError("ASR audio path must not contain symlinks")
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ASRLoadError("ASR audio path resolves outside the job directory") from exc
    return resolved


def _number(value: Any, *, label: str, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ASRLoadError(f"ASR {label} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ASRLoadError(f"ASR {label} must be finite")
    return number


def _optional_text(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ASRLoadError(f"ASR {label} must be a string or null")
    return value


def _duration_ms(value: float, *, label: str) -> int:
    try:
        result = (Decimal(str(value)) * 1000).quantize(
            Decimal(1), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError) as exc:
        raise ASRLoadError(f"ASR {label} is invalid") from exc
    return int(result)


def _run_key(manifest: dict[str, Any], item: dict[str, Any]) -> str:
    value = item.get("run_key") or manifest.get("run_key") or manifest.get("run_id")
    if not isinstance(value, str) or not _SAFE_LABEL.fullmatch(value.strip()):
        raise ASRLoadError("ASR item is missing run_key")
    return value.strip()


def _attempt_rows(item: dict[str, Any]) -> list[dict[str, Any]]:
    raw_attempts = item.get("attempts")
    if not isinstance(raw_attempts, list):
        raise ASRLoadError("ASR attempts must be an array")
    attempts: list[dict[str, Any]] = []
    for number, raw in enumerate(raw_attempts, start=1):
        if not isinstance(raw, dict):
            raise ASRLoadError("ASR attempt must be an object")
        status = raw.get("status") or raw.get("outcome")
        if not isinstance(status, str) or not _SAFE_LABEL.fullmatch(status):
            raise ASRLoadError("ASR attempt is missing status")
        code = raw.get("http_code", raw.get("status_code"))
        if code is not None and (
            isinstance(code, bool)
            or not isinstance(code, int)
            or not 100 <= code <= 599
        ):
            raise ASRLoadError("ASR attempt http_code must be between 100 and 599")
        error_class = raw.get("error_class")
        if error_class is not None and (
            not isinstance(error_class, str) or not _SAFE_CLASS.fullmatch(error_class)
        ):
            error_class = type(error_class).__name__
        latency = _number(
            raw.get("latency_seconds"), label="latency_seconds", nullable=True
        )
        retry_after = _number(
            raw.get("retry_after_seconds"), label="retry_after_seconds", nullable=True
        )
        profile = _optional_text(raw.get("profile"), label="profile")
        if profile is not None and not _SAFE_LABEL.fullmatch(profile):
            raise ASRLoadError("ASR attempt profile is invalid")
        if (
            latency is not None
            and latency < 0
            or retry_after is not None
            and retry_after < 0
        ):
            raise ASRLoadError("ASR attempt durations must not be negative")
        attempts.append(
            {
                "attempt_no": number,
                "profile": profile,
                "status": status,
                "http_code": code,
                "error_class": error_class,
                "ambiguous_delivery": bool(raw.get("ambiguous_delivery", False)),
                "latency_seconds": latency,
                "retry_after_seconds": retry_after,
            }
        )
    return attempts


def _item_record(
    manifest: dict[str, Any],
    item: dict[str, Any],
    root: Path,
    source_snapshot_sha256: str | None,
    *,
    result_root: Path | None = None,
    input_duration_seconds: float | None = None,
) -> dict[str, Any]:
    job_root = root.resolve(strict=True)
    try:
        result_base = (result_root or job_root).resolve(strict=True)
        result_base.relative_to(job_root)
    except (OSError, ValueError) as exc:
        raise ASRLoadError("ASR result directory must stay inside the job directory") from exc
    run_key = _run_key(manifest, item)
    external_ref = item.get("external_ref")
    if not isinstance(external_ref, str) or not _TELEGRAM_REF.fullmatch(
        external_ref.strip()
    ):
        raise ASRLoadError("ASR item has an invalid Telegram external_ref")
    status = item.get("status")
    if status not in _STATUSES:
        raise ASRLoadError("ASR item status is unsupported")
    audio_sha256 = item.get("audio_sha256") or item.get("sha256")
    if not isinstance(audio_sha256, str) or not _SHA256.fullmatch(audio_sha256):
        raise ASRLoadError("ASR item is missing a valid audio_sha256")
    audio_sha256 = audio_sha256.lower()
    media_path = item.get("media_path")
    if not isinstance(media_path, str) or not media_path:
        raise ASRLoadError("ASR item is missing media_path")
    media_relative = Path(media_path)
    if media_relative.is_absolute() or ".." in media_relative.parts:
        raise ASRLoadError("ASR media_path must stay inside the job directory")
    audio_path = _find_audio_file(root, media_relative)
    audio_verified = False
    if audio_path is not None:
        if _sha256(audio_path) != audio_sha256:
            raise ASRLoadError(f"Audio SHA-256 mismatch for {external_ref}")
        audio_verified = True

    model = item.get("model") or manifest.get("model")
    language = item.get("language") or manifest.get("language")
    provider = item.get("provider") or manifest.get("provider")
    if not isinstance(model, str) or not _SAFE_LABEL.fullmatch(model.strip()):
        raise ASRLoadError("ASR item is missing model")
    if not isinstance(language, str) or not _SAFE_LABEL.fullmatch(language.strip()):
        raise ASRLoadError("ASR item is missing language")
    if not isinstance(provider, str) or not _SAFE_LABEL.fullmatch(provider.strip()):
        raise ASRLoadError("ASR item is missing provider")
    prompt_sha256 = item.get("prompt_sha256", manifest.get("prompt_sha256"))
    if prompt_sha256 is not None and (
        not isinstance(prompt_sha256, str) or not _SHA256.fullmatch(prompt_sha256)
    ):
        raise ASRLoadError("ASR prompt_sha256 must be a SHA-256 or null")

    manifest_duration = _number(
        item.get("duration_seconds"), label="duration_seconds", nullable=True
    )
    input_duration = _number(
        input_duration_seconds, label="input duration_seconds", nullable=True
    )
    if (
        manifest_duration is not None
        and input_duration is not None
        and abs(manifest_duration - input_duration) > 1
    ):
        raise ASRLoadError(
            f"ASR input duration differs from manifest by more than 1 second for {external_ref}"
        )
    duration = manifest_duration if manifest_duration is not None else input_duration
    result_data: dict[str, Any] | None = None
    result_path: str | None = None
    result_digest: str | None = None
    segments: list[dict[str, Any]] = []
    overall_text = ""
    if status == "complete":
        result_file, _result_relative = _safe_job_file(
            result_base, item.get("result"), label="result"
        )
        result_path = result_file.relative_to(job_root).as_posix()
        result_digest = item.get("result_sha256") or item.get("raw_result_sha256")
        if not isinstance(result_digest, str) or not _SHA256.fullmatch(result_digest):
            raise ASRLoadError(f"ASR result_sha256 is missing for {external_ref}")
        result_digest = result_digest.lower()
        actual_result_digest = _sha256(result_file)
        if actual_result_digest != result_digest:
            raise ASRLoadError(f"ASR result SHA-256 mismatch for {external_ref}")
        try:
            result_data = read_json(result_file)
        except (OSError, ValueError, TypeError) as exc:
            raise ASRLoadError(f"Cannot parse ASR JSON for {external_ref}") from exc
        raw_segments = result_data.get("segments")
        if not isinstance(raw_segments, list):
            raise ASRLoadError(f"ASR segments must be an array for {external_ref}")
        actual_duration = _number(
            result_data.get("duration"), label="result duration", nullable=True
        )
        if (
            duration is None
            or actual_duration is None
            or abs(duration - actual_duration) > 1
        ):
            raise ASRLoadError(
                f"ASR duration differs from manifest by more than 1 second for {external_ref}"
            )
        raw_text = result_data.get("text", "")
        if not isinstance(raw_text, str):
            raise ASRLoadError(f"ASR full text must be a string for {external_ref}")
        overall_text = raw_text
        empty_count = 0
        empty_with_tokens_count = 0
        for segment_no, raw_segment in enumerate(raw_segments):
            if not isinstance(raw_segment, dict):
                raise ASRLoadError(f"ASR segment {segment_no} must be an object")
            start = _number(raw_segment.get("start"), label="segment start")
            end = _number(raw_segment.get("end"), label="segment end")
            assert start is not None and end is not None
            if start < 0 or end < start:
                raise ASRLoadError(f"ASR segment {segment_no} has invalid timestamps")
            segment_text = raw_segment.get("text", "")
            if not isinstance(segment_text, str):
                raise ASRLoadError(f"ASR segment {segment_no} text must be a string")
            tokens = raw_segment.get("tokens", [])
            if tokens is None:
                tokens = []
            if not isinstance(tokens, list):
                raise ASRLoadError(f"ASR segment {segment_no} tokens must be an array")
            empty = not segment_text.strip()
            empty_count += int(empty)
            empty_with_tokens_count += int(empty and bool(tokens))
            segments.append(
                {
                    "segment_no": segment_no,
                    "start_ms": _duration_ms(start, label="segment start"),
                    "end_ms": _duration_ms(end, label="segment end"),
                    "text": segment_text,
                    "avg_logprob": _number(
                        raw_segment.get("avg_logprob"),
                        label="avg_logprob",
                        nullable=True,
                    ),
                    "no_speech_prob": _number(
                        raw_segment.get("no_speech_prob"),
                        label="no_speech_prob",
                        nullable=True,
                    ),
                    "compression_ratio": _number(
                        raw_segment.get("compression_ratio"),
                        label="compression_ratio",
                        nullable=True,
                    ),
                    "empty_text_with_tokens": bool(empty and tokens),
                }
            )
        expected_segments = item.get("segments_count")
        if expected_segments is not None and expected_segments != len(segments):
            raise ASRLoadError(f"ASR segments_count mismatch for {external_ref}")
        expected_empty = item.get("empty_text_count")
        if expected_empty is not None and expected_empty != empty_count:
            raise ASRLoadError(f"ASR empty_text_count mismatch for {external_ref}")
        expected_empty_tokens = item.get("empty_text_with_tokens_count")
        if (
            expected_empty_tokens is not None
            and expected_empty_tokens != empty_with_tokens_count
        ):
            raise ASRLoadError(
                f"ASR empty_text_with_tokens_count mismatch for {external_ref}"
            )
    else:
        result_path_value = item.get("result")
        if result_path_value is not None:
            result_file, _result_relative = _safe_job_file(
                result_base, result_path_value, label="result"
            )
            result_path = result_file.relative_to(job_root).as_posix()
        raw_text = item.get("text", "")
        if not isinstance(raw_text, str):
            raise ASRLoadError(f"ASR text must be a string for {external_ref}")
        overall_text = raw_text

    http_code = item.get("http_code")
    if http_code is not None and (
        isinstance(http_code, bool)
        or not isinstance(http_code, int)
        or not 100 <= http_code <= 599
    ):
        raise ASRLoadError(
            f"ASR http_code must be between 100 and 599 for {external_ref}"
        )
    attempts = _attempt_rows(item)
    error_payload = None
    if status == "failed":
        error_class = item.get("error_class")
        if isinstance(error_class, str) and _SAFE_CLASS.fullmatch(error_class):
            error_payload = {"class": error_class}

    profile = item.get("profile")
    if profile is not None and (
        not isinstance(profile, str) or not _SAFE_LABEL.fullmatch(profile)
    ):
        raise ASRLoadError(f"ASR profile is invalid for {external_ref}")
    execution_run_id = item.get("execution_run_id")
    if execution_run_id is not None and (
        not isinstance(execution_run_id, str)
        or not _SAFE_LABEL.fullmatch(execution_run_id)
    ):
        raise ASRLoadError(f"ASR execution_run_id is invalid for {external_ref}")
    safe_meta = {
        "media_path": media_path,
        "file": item.get("file") if isinstance(item.get("file"), str) else None,
        "profile": profile,
        "segments_count": len(segments),
        "empty_text_count": sum(not seg["text"].strip() for seg in segments),
        "empty_text_with_tokens_count": sum(
            seg["empty_text_with_tokens"] for seg in segments
        ),
        "audio_sha256_verified": audio_verified,
        "provider_duration_seconds": (
            result_data.get("duration") if result_data is not None else None
        ),
    }
    return {
        "run_key": run_key,
        "provider": provider.strip(),
        "model": model.strip(),
        "language": language.strip(),
        "prompt_sha256": prompt_sha256.lower()
        if isinstance(prompt_sha256, str)
        else None,
        "source_snapshot_sha256": source_snapshot_sha256,
        "external_ref": external_ref.strip(),
        "audio_sha256": audio_sha256,
        "duration_seconds": duration,
        "text": overall_text,
        "raw_json_path": result_path,
        "raw_json_sha256": result_digest,
        "status": status,
        "http_code": http_code,
        "execution_run_id": execution_run_id,
        "error": error_payload,
        "meta": safe_meta,
        "segments": segments,
        "attempts": attempts,
    }


def _run_status(records: list[dict[str, Any]], manifest_status: Any) -> str:
    statuses = {record["status"] for record in records}
    if statuses == {"complete"}:
        return "done"
    if "complete" in statuses or manifest_status in {"partial", "stopped"}:
        return "partial"
    if "pending" in statuses or manifest_status in {"running", "pending"}:
        return "running"
    return "failed"


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        timestamp = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _upsert_run(
    cursor: Any, project_id: str, record: dict[str, Any]
) -> tuple[str, str]:
    prompt_hashes = {item["prompt_sha256"] for item in record["items"]}
    if len(prompt_hashes) > 1:
        raise ASRLoadError(
            f"ASR run {record['run_key']} has inconsistent prompt hashes"
        )
    prompt_sha256 = next(iter(prompt_hashes))
    meta = {
        "profiles": sorted(
            {
                item["meta"]["profile"]
                for item in record["items"]
                if item["meta"]["profile"]
            }
        ),
        "items": len(record["items"]),
    }
    cursor.execute(
        """INSERT INTO asr_runs
             (project_id, run_key, provider, model, language, prompt_sha256,
              source_snapshot_sha256, status, started_at, finished_at, meta)
           VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
           ON CONFLICT (project_id, run_key) DO UPDATE SET
             provider = EXCLUDED.provider,
             model = EXCLUDED.model,
             language = EXCLUDED.language,
             prompt_sha256 = EXCLUDED.prompt_sha256,
             source_snapshot_sha256 = EXCLUDED.source_snapshot_sha256,
             status = EXCLUDED.status,
             started_at = COALESCE(asr_runs.started_at, EXCLUDED.started_at),
             finished_at = EXCLUDED.finished_at,
             meta = EXCLUDED.meta
           WHERE (asr_runs.provider, asr_runs.model, asr_runs.language,
                  asr_runs.prompt_sha256, asr_runs.source_snapshot_sha256,
                  asr_runs.status, asr_runs.started_at, asr_runs.finished_at, asr_runs.meta)
             IS DISTINCT FROM
                 (EXCLUDED.provider, EXCLUDED.model, EXCLUDED.language,
                  EXCLUDED.prompt_sha256, EXCLUDED.source_snapshot_sha256,
                  EXCLUDED.status, COALESCE(asr_runs.started_at, EXCLUDED.started_at),
                  EXCLUDED.finished_at, EXCLUDED.meta)
           RETURNING id::text, (xmax = 0)""",
        (
            project_id,
            record["run_key"],
            record["provider"],
            record["model"],
            record["language"],
            prompt_sha256,
            record["source_snapshot_sha256"],
            record["status"],
            record["started_at"],
            record["finished_at"],
            json.dumps(meta, ensure_ascii=False),
        ),
    )
    row = cursor.fetchone()
    if row:
        return str(row[0]), "inserted" if row[1] else "updated"
    cursor.execute(
        "SELECT id::text FROM asr_runs WHERE project_id = %s::uuid AND run_key = %s",
        (project_id, record["run_key"]),
    )
    row = cursor.fetchone()
    if not row:
        raise ASRLoadError(f"Could not create ASR run {record['run_key']}")
    return str(row[0]), "skipped"


def _write_result(cursor: Any, run_id: str, item: dict[str, Any]) -> str:
    cursor.execute(
        """INSERT INTO asr_results
             (run_id, external_ref, audio_sha256, duration_seconds, text,
              raw_json_path, raw_json_sha256, status, http_code,
              execution_run_id, error, meta)
           VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
           ON CONFLICT (run_id, external_ref) DO UPDATE SET
             audio_sha256 = EXCLUDED.audio_sha256,
             duration_seconds = EXCLUDED.duration_seconds,
             text = EXCLUDED.text,
             raw_json_path = EXCLUDED.raw_json_path,
             raw_json_sha256 = EXCLUDED.raw_json_sha256,
             status = EXCLUDED.status,
             http_code = EXCLUDED.http_code,
             execution_run_id = EXCLUDED.execution_run_id,
             error = EXCLUDED.error,
             meta = EXCLUDED.meta
           WHERE (asr_results.audio_sha256, asr_results.duration_seconds,
                  asr_results.text, asr_results.raw_json_path,
                  asr_results.raw_json_sha256, asr_results.status,
                  asr_results.http_code, asr_results.execution_run_id,
                  asr_results.error, asr_results.meta)
             IS DISTINCT FROM
                 (EXCLUDED.audio_sha256, EXCLUDED.duration_seconds,
                  EXCLUDED.text, EXCLUDED.raw_json_path,
                  EXCLUDED.raw_json_sha256, EXCLUDED.status,
                  EXCLUDED.http_code, EXCLUDED.execution_run_id,
                  EXCLUDED.error, EXCLUDED.meta)
           RETURNING (xmax = 0)""",
        (
            run_id,
            item["external_ref"],
            item["audio_sha256"],
            item["duration_seconds"],
            item["text"],
            item["raw_json_path"],
            item["raw_json_sha256"],
            item["status"],
            item["http_code"],
            item["execution_run_id"],
            json.dumps(item["error"]) if item["error"] is not None else None,
            json.dumps(item["meta"], ensure_ascii=False),
        ),
    )
    row = cursor.fetchone()
    return "skipped" if row is None else "inserted" if row[0] else "updated"


def _write_segment(
    cursor: Any, run_id: str, external_ref: str, segment: dict[str, Any]
) -> str:
    cursor.execute(
        """INSERT INTO asr_segments
             (run_id, external_ref, segment_no, start_ms, end_ms, text,
              avg_logprob, no_speech_prob, compression_ratio, empty_text_with_tokens)
           VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (run_id, external_ref, segment_no) DO UPDATE SET
             start_ms = EXCLUDED.start_ms,
             end_ms = EXCLUDED.end_ms,
             text = EXCLUDED.text,
             avg_logprob = EXCLUDED.avg_logprob,
             no_speech_prob = EXCLUDED.no_speech_prob,
             compression_ratio = EXCLUDED.compression_ratio,
             empty_text_with_tokens = EXCLUDED.empty_text_with_tokens
           WHERE (asr_segments.start_ms, asr_segments.end_ms, asr_segments.text,
                  asr_segments.avg_logprob, asr_segments.no_speech_prob,
                  asr_segments.compression_ratio, asr_segments.empty_text_with_tokens)
             IS DISTINCT FROM
                 (EXCLUDED.start_ms, EXCLUDED.end_ms, EXCLUDED.text,
                  EXCLUDED.avg_logprob, EXCLUDED.no_speech_prob,
                  EXCLUDED.compression_ratio, EXCLUDED.empty_text_with_tokens)
           RETURNING id, (xmax = 0)""",
        (
            run_id,
            external_ref,
            segment["segment_no"],
            segment["start_ms"],
            segment["end_ms"],
            segment["text"],
            segment["avg_logprob"],
            segment["no_speech_prob"],
            segment["compression_ratio"],
            segment["empty_text_with_tokens"],
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return "skipped"
    return "inserted" if row[1] else "updated"


def _write_attempt(
    cursor: Any, run_id: str, external_ref: str, attempt: dict[str, Any]
) -> str:
    cursor.execute(
        """INSERT INTO asr_attempts
             (run_id, external_ref, attempt_no, profile, status, http_code,
              error_class, ambiguous_delivery, latency_seconds, retry_after_seconds)
           VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (run_id, external_ref, attempt_no) DO NOTHING
           RETURNING id""",
        (
            run_id,
            external_ref,
            attempt["attempt_no"],
            attempt["profile"],
            attempt["status"],
            attempt["http_code"],
            attempt["error_class"],
            attempt["ambiguous_delivery"],
            attempt["latency_seconds"],
            attempt["retry_after_seconds"],
        ),
    )
    return "inserted" if cursor.fetchone() else "skipped"


def load_asr(
    manifest_path: Path,
    job_dir: Path,
    *,
    project: str,
    dry_run: bool = False,
    env_path: Path = ENV_PATH,
    report_path: Path | None = None,
    external_refs: list[str] | tuple[str, ...] | set[str] | frozenset[str] | None = None,
    input_durations: dict[str, float] | None = None,
) -> dict[str, Any]:
    try:
        root = job_dir.resolve(strict=True)
        supplied_manifest_path = Path(manifest_path)
        if supplied_manifest_path.is_absolute():
            manifest_relative = supplied_manifest_path.relative_to(root)
        else:
            manifest_relative = (Path.cwd() / supplied_manifest_path).relative_to(root)
        manifest_file, _manifest_relative = _safe_job_file(
            root, manifest_relative.as_posix(), label="manifest"
        )
        manifest = read_json(manifest_file)
    except (OSError, ValueError, TypeError) as exc:
        raise ASRLoadError(
            f"Cannot read ASR job or manifest ({type(exc).__name__})"
        ) from exc
    if not isinstance(manifest, dict):
        raise ASRLoadError("ASR manifest must be a JSON object")
    manifest_items = manifest.get("items")
    if not isinstance(manifest_items, list) or not manifest_items:
        raise ASRLoadError("ASR manifest items must be a non-empty array")
    raw_items = _selected_manifest_items(manifest_items, external_refs)
    if not isinstance(project, str) or not project.strip():
        raise ASRLoadError("Project slug is required")
    snapshot_sha256 = None
    job_manifest_path = root / "manifest.json"
    if job_manifest_path.is_file() and not job_manifest_path.is_symlink():
        try:
            job_manifest = read_json(job_manifest_path)
        except (OSError, ValueError, TypeError):
            job_manifest = {}
        candidate_hash = (
            job_manifest.get("snapshot_sha256")
            if isinstance(job_manifest, dict)
            else None
        )
        if isinstance(candidate_hash, str) and _SHA256.fullmatch(candidate_hash):
            snapshot_sha256 = candidate_hash.lower()

    normalized: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            raise ASRLoadError("Every ASR manifest item must be an object")
        item = _item_record(
            manifest,
            raw_item,
            root,
            snapshot_sha256,
            result_root=manifest_file.parent,
            input_duration_seconds=(input_durations or {}).get(
                raw_item.get("external_ref")
            ),
        )
        if item["external_ref"] in seen_refs:
            raise ASRLoadError(f"Duplicate ASR external_ref: {item['external_ref']}")
        seen_refs.add(item["external_ref"])
        normalized.append(item)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in normalized:
        grouped[item["run_key"]].append(item)
    run_records: list[dict[str, Any]] = []
    for run_key, items in grouped.items():
        run_attributes = {
            (item["provider"], item["model"], item["language"], item["prompt_sha256"])
            for item in items
        }
        if len(run_attributes) != 1:
            raise ASRLoadError(
                f"ASR run {run_key} has inconsistent provider/model/language/prompt"
            )
        provider, model, language, prompt_sha256 = next(iter(run_attributes))
        status = _run_status(items, manifest.get("status"))
        finished_values = [
            parsed
            for item in items
            if (parsed := _timestamp(item.get("completed_at"))) is not None
        ]
        manifest_run_matches = (
            manifest.get("run_key") == run_key or manifest.get("run_id") == run_key
        )
        run_records.append(
            {
                "run_key": run_key,
                "provider": provider,
                "model": model,
                "language": language,
                "prompt_sha256": prompt_sha256,
                "source_snapshot_sha256": snapshot_sha256,
                "status": status,
                "started_at": _timestamp(manifest.get("created_at"))
                if manifest_run_matches
                else None,
                "finished_at": _timestamp(manifest.get("finished_at"))
                if manifest_run_matches
                else (max(finished_values) if finished_values else None),
                "items": items,
            }
        )
    run_by_key = {record["run_key"]: record for record in run_records}

    try:
        db_config = _required_env(env_path)
        conn = _connect(db_config)
    except Exception as exc:
        if isinstance(exc, ASRLoadError):
            raise
        raise ASRLoadError(
            f"Cannot connect to knowledge database ({type(exc).__name__})"
        ) from exc

    names = (
        "runs_inserted",
        "runs_updated",
        "runs_skipped",
        "results_inserted",
        "results_updated",
        "results_skipped",
        "segments_inserted",
        "segments_updated",
        "segments_skipped",
        "attempts_inserted",
        "attempts_skipped",
    )
    counts: Counter[str] = Counter({name: 0 for name in names})
    counts["skipped_empty"] = sum(
        1
        for item in normalized
        for segment in item["segments"]
        if not segment["text"].strip()
    )
    rolled_back = False
    try:
        try:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id::text FROM projects WHERE slug = %s", (project.strip(),)
                )
                project_row = cursor.fetchone()
                if not project_row:
                    raise ASRLoadError(f"Project {project!r} does not exist")
                project_id = str(project_row[0])
                refs = [item["external_ref"] for item in normalized]
                cursor.execute(
                    """SELECT s.external_ref, s.meta->>'media_path'
                       FROM segments s
                       JOIN documents d ON d.id = s.document_id
                       JOIN sources so ON so.id = d.source_id
                       WHERE so.project_id = %s::uuid AND s.external_ref = ANY(%s)""",
                    (project_id, refs),
                )
                source_rows = {str(row[0]): row[1] for row in cursor.fetchall()}
                missing = sorted(set(refs) - set(source_rows))
                if missing:
                    raise ASRLoadError(
                        f"ASR external_ref is not present in project segments: {missing[0]}"
                    )
                for item in normalized:
                    source_media_path = source_rows[item["external_ref"]]
                    if (
                        source_media_path
                        and source_media_path != item["meta"]["media_path"]
                    ):
                        raise ASRLoadError(
                            f"ASR media_path differs from source segment for {item['external_ref']}"
                        )
                for run_record in run_records:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"{project_id}:{run_record['run_key']}",),
                    )
                    run_id, run_outcome = _upsert_run(cursor, project_id, run_record)
                    counts[f"runs_{run_outcome}"] += 1
                    for item in run_record["items"]:
                        result_outcome = _write_result(cursor, run_id, item)
                        counts[f"results_{result_outcome}"] += 1
                        for segment in item["segments"]:
                            segment_outcome = _write_segment(
                                cursor, run_id, item["external_ref"], segment
                            )
                            counts[f"segments_{segment_outcome}"] += 1
                        for attempt in item["attempts"]:
                            attempt_outcome = _write_attempt(
                                cursor, run_id, item["external_ref"], attempt
                            )
                            counts[f"attempts_{attempt_outcome}"] += 1
                if dry_run:
                    raise _DryRunRollback
        except _DryRunRollback:
            rolled_back = True
    except ASRLoadError:
        raise
    except Exception as exc:
        sqlstate = getattr(exc, "sqlstate", None)
        suffix = f" SQLSTATE {sqlstate}" if isinstance(sqlstate, str) else ""
        raise ASRLoadError(f"ASR load failed ({type(exc).__name__}{suffix})") from exc
    finally:
        conn.close()

    report = {
        "status": "dry_run" if rolled_back else "done",
        "dry_run": dry_run,
        "transaction": "rolled_back" if rolled_back else "committed",
        "rollback_confirmed": rolled_back,
        "project": project.strip(),
        "manifest": manifest_path.name,
        "run_keys": sorted(run_by_key),
        "items": len(normalized),
        "segments_total": sum(len(item["segments"]) for item in normalized),
        "attempts_total": sum(len(item["attempts"]) for item in normalized),
        **{name: counts[name] for name in names},
        "skipped_empty": counts["skipped_empty"],
    }
    target_report = report_path or (root / "asr-load-report.json")
    report["report_path"] = str(target_report)
    try:
        write_json_atomic(target_report, report)
    except OSError as exc:
        raise ASRLoadError(
            f"Cannot write ASR load report ({type(exc).__name__})"
        ) from exc
    return report
