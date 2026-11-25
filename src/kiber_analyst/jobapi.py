from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import data_dir
from kiber_analyst.jobqueue import DEFAULT_SOCKET, JobsError, canonical_json

MAX_REQUEST_BYTES = 1_048_576
MAX_RESPONSE_BYTES = 20 * 1024 * 1024
ALLOWED_OPERATIONS = {
    "telegram_asr",
    "telegram_ingest",
    "embed",
    "asr_embed",
    "search",
    "external_ingest",
    "external_embed",
}
DATA_PREFIX = data_dir().resolve()


class _RestrictedParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise JobsError("Unsupported command arguments")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_input_path(value: str, *, directory: bool = True) -> Path:
    try:
        path = Path(value).resolve(strict=True)
    except (OSError, RuntimeError):
        raise JobsError("Job input path does not exist") from None
    if DATA_PREFIX not in path.parents:
        raise JobsError("Job inputs must be under the configured data directory")
    if directory and not path.is_dir():
        raise JobsError("Job input must be a directory")
    if not directory and not path.is_file():
        raise JobsError("Job input must be a file")
    return path


def parse_job_command(argv: Sequence[str]) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    """Validate supported worker operations; never evaluate a command string."""
    tokens = list(argv)
    if not tokens or any("\x00" in token for token in tokens):
        raise JobsError("A supported Kiber-analyst command is required")
    if tokens[:2] == ["telegram", "asr"]:
        parser = _RestrictedParser(add_help=False)
        parser.add_argument("--job", required=True)
        parser.add_argument("--project")
        parser.add_argument("--load-results", action="store_true")
        parser.add_argument("--run-id")
        parser.add_argument("--external-ref", action="append")
        parser.add_argument("--rotation-config")
        parser.add_argument("--profile", action="append")
        parser.add_argument("--max-cycles", type=int)
        try:
            args = parser.parse_args(tokens[2:])
        except SystemExit:
            raise JobsError("Unsupported telegram asr arguments") from None
        if args.rotation_config or args.profile or args.max_cycles is not None:
            raise JobsError("Worker routing policy cannot be overridden inside a job command")
        if args.load_results and not args.project:
            raise JobsError("--load-results requires --project")
        if args.project and not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.project):
            raise JobsError("Invalid ASR project slug")
        if args.run_id and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.run_id):
            raise JobsError("Invalid ASR run identifier")
        job_dir = _safe_input_path(args.job)
        try:
            from kiber_analyst.asr import _collect_audio

            audio = _collect_audio(job_dir)
        except Exception as exc:  # noqa: BLE001 - keep archive and input diagnostics private.
            raise JobsError(f"Cannot validate ASR inputs ({type(exc).__name__})") from None
        requested_refs = args.external_ref
        if requested_refs is not None:
            if len(requested_refs) != len(set(requested_refs)):
                raise JobsError("Duplicate ASR external reference")
            audio_by_ref = {item.external_ref: item for item in audio}
            if any(reference not in audio_by_ref for reference in requested_refs):
                raise JobsError("Requested ASR item is not present in the job")
            selected = [audio_by_ref[reference] for reference in requested_refs]
        else:
            selected = audio
        if not selected:
            raise JobsError("No ASR items were selected")
        items = [
            {"key": item.external_ref, "fingerprint": item.sha256}
            for item in selected
        ]
        return "telegram_asr", {
            "job_dir": str(job_dir),
            "project": args.project,
            "load_results": bool(args.load_results),
            "run_id": args.run_id,
            "external_refs": [item.external_ref for item in selected],
        }, items

    if tokens[:2] == ["telegram", "ingest"]:
        parser = _RestrictedParser(add_help=False)
        parser.add_argument("--job", required=True)
        parser.add_argument("--project", default="default")
        parser.add_argument("--dry-run", action="store_true")
        try:
            args = parser.parse_args(tokens[2:])
        except SystemExit:
            raise JobsError("Unsupported telegram ingest arguments") from None
        try:
            from kiber_analyst.telegram_ingest import inspect_export

            export = inspect_export(Path(args.job))
        except Exception as exc:  # noqa: BLE001 - do not echo source paths or message data.
            raise JobsError(f"Cannot validate Telegram export ({type(exc).__name__})") from None
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.project):
            raise JobsError("Invalid project slug")
        params = {
            "job_dir": str(export.path),
            "project": args.project,
            "dry_run": bool(args.dry_run),
            "import_key": export.import_key,
            "messages_sha256": export.message_sha256,
            "services_sha256": export.service_sha256,
        }
        items = [{"key": "telegram-export", "fingerprint": export.import_key}]
        return "telegram_ingest", params, items

    if tokens[0] == "embed":
        parser = _RestrictedParser(add_help=False)
        parser.add_argument("--job", required=True)
        parser.add_argument("--project")
        try:
            args = parser.parse_args(tokens[1:])
        except SystemExit:
            raise JobsError("Unsupported embed arguments") from None
        job_dir = _safe_input_path(args.job)
        manifest = job_dir / "normalized" / "manifest.json"
        if not manifest.is_file():
            raise JobsError("Embedding job manifest is missing")
        input_path = next(
            (path for path in (job_dir / "segments.jsonl", job_dir / "normalized" / "messages.jsonl") if path.is_file()),
            None,
        )
        if input_path is None:
            raise JobsError("Embedding job input is missing")
        fingerprint = hashlib.sha256(
            ( _sha256_file(manifest) + _sha256_file(input_path) + (args.project or "") ).encode()
        ).hexdigest()
        return "embed", {"job_dir": str(job_dir), "project": args.project}, [{"key": "embedding-input", "fingerprint": fingerprint}]

    if tokens[0] in {"github", "gitlab", "gitverse"} and len(tokens) > 1 and tokens[1] == "ingest":
        return _parse_external_ingest(tokens)

    if tokens[:2] == ["external", "embed"]:
        parser = _RestrictedParser(add_help=False)
        parser.add_argument("--source", required=True)
        parser.add_argument("--project", required=True)
        parser.add_argument("--limit", type=int)
        try:
            args = parser.parse_args(tokens[2:])
        except SystemExit:
            raise JobsError("Unsupported external embed arguments") from None
        try:
            from kiber_analyst.connectors.source_ids import parse_source_id

            parsed = parse_source_id(args.source)
        except Exception:  # noqa: BLE001 - keep source diagnostics generic
            parsed = None
        if parsed is None:
            raise JobsError("External embed requires a valid provider source ID")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.project):
            raise JobsError("Invalid project slug")
        if args.limit is not None and args.limit < 1:
            raise JobsError("--limit must be positive")
        params = {
            "source_id": args.source,
            "provider": parsed.provider,
            "project": args.project,
            "limit": args.limit,
        }
        fingerprint = hashlib.sha256(canonical_json(params).encode()).hexdigest()
        return "external_embed", params, [{"key": "external-embed", "fingerprint": fingerprint}]

    if tokens[:2] == ["asr", "embed"]:
        parser = _RestrictedParser(add_help=False)
        scope = parser.add_mutually_exclusive_group(required=True)
        scope.add_argument("--run")
        scope.add_argument("--all-runs", action="store_true")
        parser.add_argument("--project", required=True)
        parser.add_argument("--limit", type=int)
        parser.add_argument("--report")
        parser.add_argument("--source-id")
        try:
            args = parser.parse_args(tokens[2:])
        except SystemExit:
            raise JobsError("Unsupported asr embed arguments") from None
        if args.report:
            raise JobsError("Worker chooses the report artifact path")
        if args.limit is not None and args.limit < 1:
            raise JobsError("--limit must be positive")
        if args.source_id and not re.fullmatch(
            r"telegram:-[1-9][0-9]{0,18}:topic:[1-9][0-9]{0,9}", args.source_id
        ):
            raise JobsError("Invalid Telegram source ID")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.project):
            raise JobsError("Invalid project slug")
        params = {
            "run_key": args.run,
            "all_runs": bool(args.all_runs),
            "project": args.project,
            "limit": args.limit,
            "source_id": args.source_id,
        }
        fingerprint = hashlib.sha256(canonical_json(params).encode()).hexdigest()
        return "asr_embed", params, [{"key": "asr-embed", "fingerprint": fingerprint}]

    if tokens[0] == "search":
        parser = _RestrictedParser(add_help=False)
        parser.add_argument("query")
        parser.add_argument("--project", required=True)
        parser.add_argument("--limit", type=int, default=10)
        parser.add_argument(
            "--source-kind",
            choices=("telegram", "document", "web", "video", "github", "gitlab", "gitverse"),
        )
        parser.add_argument("--source", choices=("segment", "asr"), dest="source_filter")
        parser.add_argument("--date-from")
        parser.add_argument("--date-to")
        parser.add_argument("--author")
        parser.add_argument("--topic")
        parser.add_argument("--source-id")
        parser.add_argument("--json", action="store_true")
        try:
            args = parser.parse_args(tokens[1:])
        except SystemExit:
            raise JobsError("Unsupported search arguments") from None
        if not 1 <= args.limit <= 100:
            raise JobsError("Search limit must be between 1 and 100")
        try:
            for item in (args.date_from, args.date_to):
                if item:
                    datetime.fromisoformat(item)
        except ValueError:
            raise JobsError("Search dates must use ISO format YYYY-MM-DD") from None
        params = {
            "query": args.query,
            "project": args.project,
            "limit": args.limit,
            "source_kind": args.source_kind,
            "source": args.source_filter,
            "date_from": args.date_from,
            "date_to": args.date_to,
            "author": args.author,
            "topic": args.topic,
            "source_id": args.source_id,
        }
        fingerprint = hashlib.sha256(canonical_json(params).encode()).hexdigest()
        return "search", params, [{"key": "search-query", "fingerprint": fingerprint}]
    raise JobsError(
        "Supported operations: telegram asr, telegram ingest, embed, asr embed, search,"
        " github/gitlab/gitverse ingest, external embed"
    )


def _parse_external_ingest(tokens: list[str]) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    provider = tokens[0]
    parser = _RestrictedParser(add_help=False)
    if provider == "gitlab":
        parser.add_argument("--host", default="gitlab.com")
        parser.add_argument("--project", required=True, dest="project_path")
        parser.add_argument("--project-slug", required=True)
    else:
        parser.add_argument("--repo", required=True)
        parser.add_argument("--project", required=True)
    parser.add_argument("--since")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    try:
        args = parser.parse_args(tokens[2:])
    except SystemExit:
        raise JobsError(f"Unsupported {provider} ingest arguments") from None
    if provider == "gitlab":
        source_id = f"gitlab:{args.host}/{args.project_path}"
        project_slug = args.project_slug
    else:
        source_id = f"{provider}:{args.repo}"
        project_slug = args.project
    try:
        from kiber_analyst.connectors.source_ids import parse_source_id

        parsed = parse_source_id(source_id)
    except Exception:  # noqa: BLE001 - keep source diagnostics generic
        parsed = None
    if parsed is None or parsed.provider != provider:
        raise JobsError(f"Invalid {provider} repository identifier")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", project_slug):
        raise JobsError("Invalid project slug")
    if args.since is not None:
        try:
            datetime.fromisoformat(args.since.replace("Z", "+00:00"))
        except ValueError:
            raise JobsError("--since must use ISO 8601 format") from None
    if args.limit is not None and args.limit < 1:
        raise JobsError("--limit must be positive")
    params = {
        "provider": provider,
        "source_id": source_id,
        "project": project_slug,
        "since": args.since,
        "limit": args.limit,
        "dry_run": bool(args.dry_run),
    }
    # Idempotency key basis: source_id + since + mode (ТЗ §10.3); the job store
    # derives the idempotency key from the operation fingerprint, which already
    # covers all three plus limit/dry-run.
    fingerprint = hashlib.sha256(canonical_json(params).encode()).hexdigest()
    return "external_ingest", params, [{"key": "external-source", "fingerprint": fingerprint}]


def request(socket_path: Path, payload: dict[str, Any], *, timeout: float = 65.0) -> dict[str, Any]:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise JobsError("Control request is too large")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(timeout)
    try:
        client.connect(str(socket_path))
        client.sendall(encoded + b"\n")
        chunks = bytearray()
        while len(chunks) <= MAX_RESPONSE_BYTES:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.extend(chunk)
            if b"\n" in chunk:
                break
    except (OSError, TimeoutError):
        raise JobsError("Worker control socket is unavailable") from None
    finally:
        client.close()
    if len(chunks) > MAX_RESPONSE_BYTES:
        raise JobsError("Worker response is too large")
    try:
        response = json.loads(bytes(chunks).split(b"\n", 1)[0])
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise JobsError("Worker returned an invalid response") from None
    if not isinstance(response, dict) or not response.get("ok"):
        message = response.get("error", "Worker request failed") if isinstance(response, dict) else "Worker request failed"
        raise JobsError(str(message))
    return response.get("result", {})


def submit_command(
    argv: Sequence[str],
    *,
    pool: str | None = None,
    strategy: str | None = None,
    profile: str | None = None,
    idempotency_key: str | None = None,
    deadline_at: str | None = None,
    next_run_at: str | None = None,
    socket_path: Path = DEFAULT_SOCKET,
) -> dict[str, Any]:
    if profile and (pool or strategy):
        raise JobsError("--profile cannot be combined with --pool or --strategy")
    return request(
        socket_path,
        {
            "action": "submit",
            "argv": list(argv),
            "pool": pool,
            "strategy": strategy,
            "profile": profile,
            "idempotency_key": idempotency_key,
            "deadline_at": deadline_at,
            "next_run_at": next_run_at,
        },
    )


def wait_job(job_id: str, timeout: int = 30, *, socket_path: Path = DEFAULT_SOCKET) -> dict[str, Any]:
    if not 0 <= timeout <= 60:
        raise JobsError("--timeout must be between 0 and 60 seconds")
    return request(socket_path,{"action":"wait","job_id":job_id,"timeout":timeout},timeout=max(65,timeout+5))


def api_call(action: str, *, socket_path: Path = DEFAULT_SOCKET, **payload: Any) -> dict[str, Any]:
    if action not in {"status","result","ack","cancel","backup","extend","set-deadline","reschedule","reset_profile","doctor","source-overview","asr-source-overview","queue-overview","provider-overview","provider-probe","job-log"}:
        raise JobsError("Unsupported worker operation")
    return request(socket_path,{"action":action,**payload},timeout=65)
