"""Bounded provider checks; never persist or print transcription contents."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from kiber_analyst.provider_http import (
    ProfileError,
    client_from_settings,
    http_client_from_settings,
    load_profile,
)


def preflight(
    profile: str, *, audio: Path | None = None, egress: bool = False, **paths: Any
) -> dict[str, Any]:
    settings = load_profile(profile, **paths)
    if audio is not None and settings.provider != "groq":
        raise ProfileError("Audio preflight requires a Groq profile")
    if audio is not None and (
        not audio.is_file()
        or audio.suffix.lower() != ".ogg"
        or not 0 < audio.stat().st_size <= 25_000_000
    ):
        raise ProfileError("Audio preflight requires one existing OGG file up to 25 MB")
    result: dict[str, Any] = {"profile": profile, "proxy_used": True}
    if egress:
        try:
            # No provider authorization headers are sent to the geo service.
            with http_client_from_settings(settings, timeout=30) as network:
                response = network.get("https://ipinfo.io/json")
                response.raise_for_status()
                geo = response.json()
            match = re.search(r"\bAS\d+\b", str(geo.get("org", "")))
            country = geo.get("country")
            result["country"] = (
                country
                if isinstance(country, str) and re.fullmatch(r"[A-Z]{2}", country)
                else None
            )
            result["asn"] = match.group(0) if match else None
        except Exception:  # noqa: BLE001 - redact all transport diagnostics.
            raise ProfileError(f"Profile {profile} egress check failed") from None
    response_codes: list[int] = []
    sdk = client_from_settings(
        settings,
        timeout=180,
        retry_attempts=1,
        response_codes=response_codes,
    )
    try:
        if settings.provider == "groq":
            models = sdk.models.with_raw_response.list()
            result["models_http"] = models.status_code
            if audio is not None:
                with audio.open("rb") as stream:
                    response = sdk.audio.transcriptions.with_raw_response.create(
                        file=(audio.name, stream, "audio/ogg"),
                        model="whisper-large-v3-turbo",
                        language="ru",
                        response_format="json",
                    )
                result["asr_http"] = response.status_code
                result["transcription_nonempty"] = bool(response.parse().text.strip())
        else:
            from google.genai import types

            response = sdk.models.embed_content(
                model="gemini-embedding-2",
                contents="Проверка профиля подключения.",
                config=types.EmbedContentConfig(output_dimensionality=1536),
            )
            result["embed_http"] = response_codes[-1] if response_codes else None
            vectors = response.embeddings or []
            result["dimensions"] = (
                len(vectors[0].values or []) if len(vectors) == 1 else 0
            )
            if result["dimensions"] != 1536:
                raise ProfileError(
                    f"Profile {profile} returned unexpected embedding dimensions"
                )
    except ProfileError:
        raise
    except Exception as exc:  # noqa: BLE001 - redact provider error bodies.
        status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
        if isinstance(status, int):
            raise ProfileError(f"Profile {profile} API HTTP {status}") from None
        raise ProfileError(
            f"Profile {profile} API request failed ({type(exc).__name__})"
        ) from None
    finally:
        sdk.close()
    return result
