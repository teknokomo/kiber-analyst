"""Sequential Telegram audio transcription with bounded Groq profile rotation."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import mimetypes
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.errors import IngestError
from kiber_analyst.paths import job_paths, write_json_atomic
from kiber_analyst.provider_http import ProfileError, client, load_profile

_LOGGER = logging.getLogger(__name__)

ROTATION_CONFIG_PATH = config_path("asr-rotation.yaml")
MODEL = "whisper-large-v3-turbo"
LANGUAGE = "ru"
RESPONSE_FORMAT = "verbose_json"
MAX_FILE_SIZE_BYTES = 25_000_000
GLOSSARY_PROMPT = (
    "Русская речь. Сохраняй точные написания имён собственных и специальных терминов."
)
AUDIO_EXTENSIONS = {
    ".flac",
    ".m4a",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpga",
    ".ogg",
    ".wav",
    ".webm",
}
AUDIO_MEDIA_TYPES = {"audio", "audio_file", "voice", "voice_message"}
RETRYABLE_SERVER_CODES = frozenset(range(500, 600))


class ASRError(IngestError):
    """Safe ASR configuration, input, or response error."""


@dataclass(frozen=True)
class RotationConfig:
    profiles: tuple[str, ...]
    max_cycles: int = 5
    request_timeout_seconds: int = 900
    max_runtime_seconds: int = 2700
    backoff_initial_seconds: float = 10
    backoff_max_seconds: float = 120
    prompt: str = GLOSSARY_PROMPT


@dataclass(frozen=True)
class AudioInput:
    external_ref: str
    media_path: str
    path: Path
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class Failure:
    category: str
    status_code: int | None
    error_class: str
    retry_after_seconds: float | None = None
    ambiguous_delivery: bool = False


def _yaml_document(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        raise ASRError(
            "ASR profile support unavailable; install kiber-analyst[asr]"
        ) from None

    class UniqueLoader(yaml.SafeLoader):
        pass

    def unique_mapping(loader: Any, node: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            if not isinstance(key, str) or key in result:
                raise ASRError("Invalid or duplicate ASR configuration entry")
            result[key] = loader.construct_object(value_node)
        return result

    UniqueLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping
    )
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.load(stream, Loader=UniqueLoader)
    except ASRError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ASRError("Cannot read ASR rotation configuration") from None
    if not isinstance(value, dict):
        raise ASRError("Invalid ASR rotation configuration")
    return value


def load_rotation_config(
    path: Path = ROTATION_CONFIG_PATH,
    *,
    max_cycles_override: int | None = None,
    profiles_override: tuple[str, ...] | None = None,
) -> RotationConfig:
    value = _yaml_document(path)
    allowed = {
        "profiles",
        "max_cycles",
        "request_timeout_seconds",
        "max_runtime_seconds",
        "backoff_initial_seconds",
        "backoff_max_seconds",
        "prompt",
    }
    if value.keys() - allowed:
        raise ASRError("Unknown ASR rotation configuration option")

    profile_values = (
        profiles_override
        if profiles_override is not None
        else value.get("profiles")
    )
    if (
        not isinstance(profile_values, (list, tuple))
        or not profile_values
        or any(
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name)
            for name in profile_values
        )
        or len(set(profile_values)) != len(profile_values)
    ):
        raise ASRError("ASR rotation requires a nonempty list of unique profiles")

    def positive_int(name: str, default: int, *, maximum: int | None = None) -> int:
        item = value.get(name, default)
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise ASRError(f"Invalid ASR rotation option: {name}")
        if maximum is not None and item > maximum:
            raise ASRError(f"ASR rotation option exceeds its limit: {name}")
        return item

    def positive_float(name: str, default: float) -> float:
        item = value.get(name, default)
        if (
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(item)
            or item <= 0
        ):
            raise ASRError(f"Invalid ASR rotation option: {name}")
        return float(item)

    configured_cycles = positive_int("max_cycles", 5, maximum=20)
    max_cycles = (
        configured_cycles if max_cycles_override is None else max_cycles_override
    )
    if (
        isinstance(max_cycles, bool)
        or not isinstance(max_cycles, int)
        or not 1 <= max_cycles <= 20
    ):
        raise ASRError("--max-cycles must be between 1 and 20")

    prompt = value.get("prompt", GLOSSARY_PROMPT)
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 1000:
        raise ASRError("ASR prompt must be nonempty and at most 1000 characters")

    return RotationConfig(
        profiles=tuple(profile_values),
        max_cycles=max_cycles,
        request_timeout_seconds=positive_int(
            "request_timeout_seconds", 900, maximum=3600
        ),
        max_runtime_seconds=positive_int("max_runtime_seconds", 2700, maximum=86400),
        backoff_initial_seconds=positive_float("backoff_initial_seconds", 10),
        backoff_max_seconds=positive_float("backoff_max_seconds", 120),
        prompt=prompt.strip(),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolve_media(unpacked: Path, media_path: str) -> Path:
    relative = PurePosixPath(media_path)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ASRError("Unsafe media_path in normalized job")
    root = unpacked.resolve(strict=True)
    candidates = []
    direct = root.joinpath(*relative.parts)
    if direct.is_file():
        candidates.append(direct)
    else:
        for candidate in root.rglob(relative.name):
            if (
                candidate.is_file()
                and tuple(candidate.relative_to(root).parts[-len(relative.parts) :])
                == relative.parts
            ):
                candidates.append(candidate)
    safe = []
    for candidate in candidates:
        if candidate.is_symlink():
            continue
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            continue
        if resolved.is_relative_to(root) and resolved.is_file():
            safe.append(resolved)
    if len(safe) != 1:
        raise ASRError("Audio media_path is missing or ambiguous in the job archive")
    return safe[0]


def _collect_audio(job_dir: Path) -> list[AudioInput]:
    paths = job_paths(job_dir)
    if not paths["messages"].is_file() or not paths["unpacked"].is_dir():
        raise ASRError(
            "ASR requires a parsed job with messages.jsonl and unpacked media"
        )
    results: list[AudioInput] = []
    seen_refs: set[str] = set()
    with paths["messages"].open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                raise ASRError(
                    f"Invalid normalized message at line {line_number}"
                ) from None
            if not isinstance(record, dict):
                raise ASRError(f"Invalid normalized message at line {line_number}")
            media_type = record.get("media_type")
            media_path = record.get("media_path")
            is_audio_type = (
                isinstance(media_type, str)
                and media_type.lower() in AUDIO_MEDIA_TYPES
            )
            if not isinstance(media_path, str) or not media_path:
                if is_audio_type:
                    raise ASRError(f"Audio media_path is missing at line {line_number}")
                continue
            suffix = PurePosixPath(media_path).suffix.lower()
            if suffix not in AUDIO_EXTENSIONS:
                if is_audio_type:
                    raise ASRError(f"Unsupported audio type at line {line_number}")
                continue
            chat_id = record.get("chat_id")
            message_id = record.get("message_id")
            if not isinstance(chat_id, (str, int)) or isinstance(chat_id, bool):
                raise ASRError(f"Audio message lacks chat_id at line {line_number}")
            if not isinstance(message_id, (str, int)) or isinstance(message_id, bool):
                raise ASRError(f"Audio message lacks message_id at line {line_number}")
            external_ref = f"tg:{chat_id}:{message_id}"
            if external_ref in seen_refs:
                raise ASRError("Duplicate external_ref in normalized audio messages")
            seen_refs.add(external_ref)
            audio_path = _resolve_media(paths["unpacked"], media_path)
            size_bytes = audio_path.stat().st_size
            if not 0 < size_bytes <= MAX_FILE_SIZE_BYTES:
                raise ASRError(
                    f"Audio file size is outside the supported range for {external_ref}"
                )
            results.append(
                AudioInput(
                    external_ref=external_ref,
                    media_path=media_path,
                    path=audio_path,
                    size_bytes=size_bytes,
                    sha256=_sha256(audio_path),
                )
            )
    if not results:
        raise ASRError("No supported audio messages found in the parsed job")
    return results


def _retry_after_seconds(exc: Exception, *, now: datetime) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    value = headers.get("retry-after") if headers is not None else None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - now).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _classify_failure(exc: Exception, *, now: datetime) -> Failure:
    value = getattr(exc, "status_code", None)
    if not isinstance(value, int):
        value = getattr(exc, "code", None)
    status = value if isinstance(value, int) else None
    error_class = type(exc).__name__
    retry_after = _retry_after_seconds(exc, now=now)
    if isinstance(exc,ProfileError):
        return Failure("profile_configuration",None,error_class)
    if status == 401:
        return Failure("profile_auth", status, error_class)
    if status == 403:
        return Failure("policy_blocked", status, error_class)
    if status == 429:
        return Failure("rate_limited", status, error_class, retry_after)
    if status in RETRYABLE_SERVER_CODES:
        return Failure("server_error", status, error_class, retry_after)
    if status == 408:
        return Failure("request_timeout", status, error_class, retry_after, True)
    if status is not None and 400 <= status <= 499:
        return Failure("request_rejected", status, error_class)

    try:
        import httpx
        from groq import APIConnectionError, APITimeoutError
    except ImportError:
        httpx = None
        APIConnectionError = APITimeoutError = ()  # type: ignore[assignment]
    timeout_types = tuple(
        item
        for item in (
            getattr(httpx, "TimeoutException", None) if httpx else None,
            APITimeoutError if isinstance(APITimeoutError, type) else None,
        )
        if isinstance(item, type)
    )
    connection_types = tuple(
        item
        for item in (
            getattr(httpx, "TransportError", None) if httpx else None,
            APIConnectionError if isinstance(APIConnectionError, type) else None,
        )
        if isinstance(item, type)
    )
    if timeout_types and isinstance(exc, timeout_types):
        return Failure("network_timeout", None, error_class, None, True)
    if connection_types and isinstance(exc, connection_types):
        return Failure("network_error", None, error_class, None, True)
    return Failure("unclassified_error", status, error_class)


def _append_event(path: Path, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o600)


def _write_private_bytes(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            raise ASRError("Refusing to overwrite an ASR result")
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def _http_error(status: int, headers: Any = None) -> Exception:
    class Response:
        pass

    class HTTPFailure(Exception):
        def __init__(self) -> None:
            self.status_code = status
            self.response = Response()
            self.response.headers = headers or {}

    return HTTPFailure()


def run_asr_batch(
    job_dir: Path,
    *,
    config_path: Path = ROTATION_CONFIG_PATH,
    max_cycles: int | None = None,
    profiles: tuple[str, ...] | None = None,
    profile_loader: Callable[[str], Any] = load_profile,
    client_factory: Callable[..., Any] = client,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    run_id: str | None = None,
    external_refs: set[str] | None = None,
    request_timeout_seconds: int | None = None,
    _audio_items: list[AudioInput] | None = None,
) -> dict[str, Any]:
    """Transcribe discovered audio sequentially; never reads or writes PostgreSQL."""
    config = load_rotation_config(
        config_path,
        max_cycles_override=max_cycles,
        profiles_override=profiles,
    )
    for profile in config.profiles:
        settings = profile_loader(profile)
        if getattr(settings, "provider", None) != "groq":
            raise ASRError(f"ASR profile {profile} is not a Groq profile")
    audio_items = _collect_audio(job_dir) if _audio_items is None else _audio_items
    selected_audio_items = [
        item for item in audio_items if external_refs is None or item.external_ref in external_refs
    ]
    if external_refs is not None and {item.external_ref for item in selected_audio_items} != external_refs:
        raise ASRError("Requested ASR item is not present in the parsed job")

    run_id = run_id or uuid.uuid4().hex
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", run_id):
        raise ASRError("Invalid ASR run identifier")
    run_dir = job_dir / "asr" / "batches" / run_id
    run_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(run_dir, 0o700)
    attempts_path = run_dir / "attempts.jsonl"
    manifest_path = run_dir / "manifest.json"
    result_dir = run_dir / "results"
    result_dir.mkdir(mode=0o700, exist_ok=True)
    prompt_sha256 = hashlib.sha256(config.prompt.encode("utf-8")).hexdigest()
    existing_manifest: dict[str, Any] | None = None
    if manifest_path.exists():
        try:
            existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise ASRError("Cannot resume the existing ASR job manifest") from None
        if existing_manifest.get("run_id") != run_id or existing_manifest.get("prompt_sha256") != prompt_sha256:
            raise ASRError("Existing ASR job manifest does not match this request")
        existing_items = existing_manifest.get("items")
        if not isinstance(existing_items, list):
            raise ASRError("Existing ASR job manifest has invalid items")
        expected_items = {item.external_ref: item.sha256 for item in audio_items}
        stored_items = {
            item.get("external_ref"): item.get("audio_sha256")
            for item in existing_items if isinstance(item, dict)
        }
        if stored_items != expected_items:
            raise ASRError("ASR job input fingerprint changed after the run began")
        items_state = existing_items
        for state in items_state:
            result_relative=Path(str(state.get("result","")))
            if result_relative.is_absolute() or ".." in result_relative.parts:
                raise ASRError("Saved ASR result path is invalid")
            result_path = run_dir / result_relative
            if state.get("status") != "complete" and result_path.is_file():
                try:
                    cached = json.loads(result_path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError):
                    raise ASRError("A saved ASR result is invalid") from None
                if not isinstance(cached, dict) or not isinstance(cached.get("text"), str) or not isinstance(cached.get("segments"), list):
                    raise ASRError("A saved ASR result is incomplete")
                payload = result_path.read_bytes()
                state["status"] = "complete"
                state["response_bytes"] = len(payload)
                state["result_sha256"] = hashlib.sha256(payload).hexdigest()
                state["recovered_saved_result"] = True
            if state.get("status") == "failed" and state.get("external_ref") in {
                item.external_ref for item in selected_audio_items
            }:
                state["status"] = "pending"
                state.pop("failure", None)
    else:
        items_state = []
        for item in audio_items:
            result_name = hashlib.sha256(item.external_ref.encode("utf-8")).hexdigest()[:24]
            items_state.append(
                {
                    "external_ref": item.external_ref,
                    "media_path": item.media_path,
                    "audio_sha256": item.sha256,
                    "size_bytes": item.size_bytes,
                    "status": "pending",
                    "profile": None,
                    "attempts": [],
                    "result": f"results/{result_name}.verbose.json",
                }
            )

    state_by_ref = {state["external_ref"]: state for state in items_state}
    item_index_by_ref = {item.external_ref: index for index, item in enumerate(audio_items)}
    manifest: dict[str, Any] = existing_manifest or {
        "version": 1,
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "provider": "groq",
        "model": MODEL,
        "language": LANGUAGE,
        "response_format": RESPONSE_FORMAT,
        "prompt_sha256": prompt_sha256,
        "profiles": list(config.profiles),
        "max_cycles": config.max_cycles,
        "max_runtime_seconds": config.max_runtime_seconds,
        "items": items_state,
    }
    manifest["profiles"] = list(config.profiles)

    def save_manifest() -> None:
        write_json_atomic(manifest_path, manifest)
        os.chmod(manifest_path, 0o600)

    save_manifest()
    started_at = clock()
    next_profile_index = 0
    disabled_profiles: set[str] = set()
    not_before: dict[str, float] = {}
    failures_by_profile: dict[str, int] = {}
    attempts_total = 0
    stop_reason: str | None = None

    for audio in selected_audio_items:
        state = state_by_ref[audio.external_ref]
        item_index = item_index_by_ref[audio.external_ref]
        if state.get("status") == "complete":
            continue
        success = False
        for cycle in range(1, config.max_cycles + 1):
            active_at_cycle_start = [
                name for name in config.profiles if name not in disabled_profiles
            ]
            if not active_at_cycle_start:
                stop_reason = "no_usable_profiles"
                state["status"] = "failed"
                break
            ordered_profiles = [
                config.profiles[(next_profile_index + offset) % len(config.profiles)]
                for offset in range(len(config.profiles))
            ]
            statuses_this_cycle: dict[str, int | None] = {}
            permanent_failure = False

            for profile_index, profile in enumerate(ordered_profiles):
                if profile in disabled_profiles:
                    continue
                remaining = config.max_runtime_seconds - (clock() - started_at)
                if remaining <= 0:
                    stop_reason = "max_runtime"
                    state["status"] = "failed"
                    break
                wait_seconds = max(0.0, not_before.get(profile, 0.0) - clock())
                if wait_seconds:
                    if wait_seconds >= remaining:
                        stop_reason = "max_runtime_before_retry"
                        state["status"] = "failed"
                        break
                    sleep_fn(wait_seconds)
                    remaining = config.max_runtime_seconds - (clock() - started_at)
                    if remaining <= 0:
                        stop_reason = "max_runtime"
                        state["status"] = "failed"
                        break

                attempts_total += 1
                attempt_number = len(state["attempts"]) + 1
                attempt_id = f"{item_index + 1}-{attempt_number}"
                started_utc = datetime.now(UTC).isoformat()
                start_event = {
                    "event": "attempt_started",
                    "attempt_id": attempt_id,
                    "at": started_utc,
                    "external_ref": audio.external_ref,
                    "profile": profile,
                    "cycle": cycle,
                    "attempt": attempt_number,
                    "planned_wait_seconds": round(wait_seconds, 3),
                    "model": MODEL,
                }
                _append_event(attempts_path, start_event)
                state["status"] = "running"
                state["profile"] = profile
                save_manifest()

                request_started = clock()
                sdk = None
                status_code: int | None = None
                error_class: str | None = None
                outcome = "success"
                failure: Failure | None = None
                response_size = 0
                try:
                    sdk = client_factory(
                        profile,
                        timeout=min(
                            request_timeout_seconds or config.request_timeout_seconds,
                            max(1, int(remaining)),
                        ),
                        retry_attempts=1,
                    )
                    mime = (
                        mimetypes.guess_type(audio.path.name)[0]
                        or "application/octet-stream"
                    )
                    with audio.path.open("rb") as stream:
                        raw = sdk.audio.transcriptions.with_raw_response.create(
                            file=(audio.path.name, stream, mime),
                            model=MODEL,
                            language=LANGUAGE,
                            response_format=RESPONSE_FORMAT,
                            prompt=config.prompt,
                            temperature=0,
                        )
                    status_code = raw.status_code
                    body = raw.read()
                    headers = getattr(
                        getattr(raw, "http_response", None), "headers", {}
                    )
                    raw.close()
                    response_size = len(body)
                    if status_code != 200:
                        raise _http_error(status_code, headers)
                    try:
                        payload = json.loads(body)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        raise ASRError("Groq returned invalid verbose_json") from None
                    if not isinstance(payload, dict) or not isinstance(
                        payload.get("text"), str
                    ):
                        raise ASRError(
                            "Groq returned an incomplete transcription response"
                        )
                    if not isinstance(payload.get("segments"), list):
                        raise ASRError("Groq verbose_json response has no segments")
                    result_path = run_dir / state["result"]
                    _write_private_bytes(result_path, body)
                    state["status"] = "complete"
                    state["profile"] = profile
                    state["response_bytes"] = response_size
                    state["result_sha256"] = hashlib.sha256(body).hexdigest()
                    state["completed_at"] = datetime.now(UTC).isoformat()
                    state["attempts"].append(
                        {
                            "profile": profile,
                            "cycle": cycle,
                            "status_code": status_code,
                            "outcome": "success",
                            "latency_seconds": round(clock() - request_started, 3),
                            "response_bytes": response_size,
                        }
                    )
                    _append_event(
                        attempts_path,
                        {
                            "event": "attempt_finished",
                            "attempt_id": attempt_id,
                            "at": datetime.now(UTC).isoformat(),
                            "external_ref": audio.external_ref,
                            "profile": profile,
                            "cycle": cycle,
                            "status_code": status_code,
                            "outcome": "success",
                            "latency_seconds": round(clock() - request_started, 3),
                            "response_bytes": response_size,
                        },
                    )
                    save_manifest()
                    next_profile_index = (
                        config.profiles.index(profile) + 1
                    ) % len(config.profiles)
                    success = True
                    break
                except Exception as exc:  # noqa: BLE001 - provider SDK errors vary by transport.
                    if isinstance(exc, ASRError):
                        failure = Failure(
                            "invalid_response", status_code, type(exc).__name__
                        )
                    else:
                        failure = _classify_failure(exc, now=datetime.now(UTC))
                    status_code = failure.status_code
                    error_class = failure.error_class
                    outcome = failure.category
                    statuses_this_cycle[profile] = status_code
                    state["attempts"].append(
                        {
                            "profile": profile,
                            "cycle": cycle,
                            "status_code": status_code,
                            "outcome": outcome,
                            "error_class": error_class,
                            "latency_seconds": round(clock() - request_started, 3),
                            "retry_after_seconds": failure.retry_after_seconds,
                            "ambiguous_delivery": failure.ambiguous_delivery,
                        }
                    )
                    _append_event(
                        attempts_path,
                        {
                            "event": "attempt_finished",
                            "attempt_id": attempt_id,
                            "at": datetime.now(UTC).isoformat(),
                            "external_ref": audio.external_ref,
                            "profile": profile,
                            "cycle": cycle,
                            "status_code": status_code,
                            "outcome": outcome,
                            "error_class": error_class,
                            "latency_seconds": round(clock() - request_started, 3),
                            "retry_after_seconds": failure.retry_after_seconds,
                            "ambiguous_delivery": failure.ambiguous_delivery,
                        },
                    )
                    save_manifest()
                    if failure.category == "profile_auth":
                        disabled_profiles.add(profile)
                        continue
                    if failure.category not in {
                        "rate_limited",
                        "server_error",
                        "request_timeout",
                        "network_timeout",
                        "network_error",
                    }:
                        state["status"] = "failed"
                        state["failure"] = {
                            "profile": profile,
                            "status_code": status_code,
                            "category": failure.category,
                            "error_class": error_class,
                        }
                        stop_reason = failure.category
                        permanent_failure = True
                        save_manifest()
                        break

                    failures_by_profile[profile] = (
                        failures_by_profile.get(profile, 0) + 1
                    )
                    if failure.retry_after_seconds is not None:
                        delay = failure.retry_after_seconds
                    else:
                        exponent = min(failures_by_profile[profile] - 1, 16)
                        delay = min(
                            config.backoff_initial_seconds * (2**exponent),
                            config.backoff_max_seconds,
                        )
                    not_before[profile] = max(
                        not_before.get(profile, 0.0), clock() + delay
                    )
                finally:
                    if sdk is not None:
                        try:
                            sdk.close()
                        except Exception as close_exc:  # noqa: BLE001 - cleanup must not replace request outcome.
                            _LOGGER.debug(
                                "ASR provider client close failed (%s)",
                                type(close_exc).__name__,
                            )

            if success or permanent_failure or stop_reason:
                break
            if (
                len(active_at_cycle_start) > 1
                and len(statuses_this_cycle) == len(active_at_cycle_start)
                and len(set(statuses_this_cycle.values())) == 1
                and next(iter(statuses_this_cycle.values()), None)
                in RETRYABLE_SERVER_CODES
            ):
                stop_reason = "same_server_error_on_all_profiles"
                state["status"] = "failed"
                state["failure"] = {"category": stop_reason}
                break
            if cycle == config.max_cycles:
                stop_reason = "retry_exhausted"
                state["status"] = "failed"
                state["failure"] = {"category": stop_reason}
                break

        if not success:
            if stop_reason is None:
                stop_reason = "retry_exhausted"
            break

    complete = all(item["status"] == "complete" for item in items_state)
    manifest["status"] = "complete" if complete else "stopped"
    manifest["stop_reason"] = stop_reason
    manifest["finished_at"] = datetime.now(UTC).isoformat()
    save_manifest()
    report = {
        "status": "complete" if complete else "stopped",
        "run_id": run_id,
        "run_dir": str(run_dir),
        "profiles": list(config.profiles),
        "max_cycles": config.max_cycles,
        "attempts": attempts_total,
        "completed": sum(item["status"] == "complete" for item in items_state),
        "failed": sum(item["status"] == "failed" for item in items_state),
        "pending": sum(item["status"] == "pending" for item in items_state),
        "stop_reason": stop_reason,
        "items": items_state,
    }
    report_path = run_dir / "report.json"
    write_json_atomic(report_path, report)
    os.chmod(report_path, 0o600)
    return report
