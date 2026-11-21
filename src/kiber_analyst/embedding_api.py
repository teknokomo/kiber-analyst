from __future__ import annotations

from pathlib import Path
from typing import Any

from kiber_analyst.embedding_contract import DIMENSIONS, MODEL
from kiber_analyst.embedding_contract import MODEL_KEY as MODEL_KEY
from kiber_analyst.errors import EmbedError
from kiber_analyst.provider_http import (
    API_ENV_PATH,
    PROFILES_PATH,
    PROXY_ENV_PATH,
    ProfileError,
    ProviderSettings,
    client_from_settings,
    load_profile,
)

TIMEOUT_MS = 120_000
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ProviderError(EmbedError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        timed_out: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.timed_out = timed_out

    @property
    def retryable(self) -> bool:
        return self.timed_out or self.status_code in RETRYABLE_STATUS_CODES


def load_provider_settings(
    api_env_path: Path = API_ENV_PATH,
    proxy_env_path: Path = PROXY_ENV_PATH,
    profiles_path: Path = PROFILES_PATH,
    profile: str = "gemini_embed",
) -> ProviderSettings:
    try:
        return load_profile(
            profile,
            profiles_path=profiles_path,
            api_env_path=api_env_path,
            proxy_env_path=proxy_env_path,
        )
    except ProfileError as exc:
        raise EmbedError(str(exc)) from None


def create_client(settings: ProviderSettings) -> Any:
    if settings.provider != "google":
        raise EmbedError("Embeddings require a Google provider profile")
    try:
        return client_from_settings(settings, timeout=TIMEOUT_MS / 1000)
    except ProfileError as exc:
        raise EmbedError(str(exc)) from None


def _status_code(exc: Exception) -> int | None:
    for value in (
        getattr(exc, "code", None),
        getattr(exc, "status_code", None),
        getattr(getattr(exc, "response", None), "status_code", None),
    ):
        try:
            code = int(value)
        except (TypeError, ValueError):
            continue
        if 100 <= code <= 599:
            return code
    return None


def _is_timeout(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    module = type(exc).__module__.lower()
    return (
        isinstance(exc, TimeoutError)
        or "timeout" in name
        or (module.startswith("httpx") and "timeout" in name)
    )


def _safe_provider_error(exc: Exception) -> ProviderError:
    code = _status_code(exc)
    timed_out = _is_timeout(exc)
    if code is not None:
        error = ProviderError(f"Gemini API HTTP {code}", status_code=code)
        if code == 429:
            from kiber_analyst.gemini_quota import quota_details
            error.quota_diagnostic = quota_details(exc)
            from kiber_analyst.routing import _retry_after, utc_now
            error.retry_after_seconds = _retry_after(exc, utc_now())
        return error
    if timed_out:
        return ProviderError("Gemini API request timed out", timed_out=True)
    return ProviderError(f"Gemini API request failed ({type(exc).__name__})")


def embed_texts(client: Any, texts: list[str]) -> tuple[list[list[float]], int | None]:
    if not texts:
        return [], 0
    try:
        from google.genai import types
    except ImportError as exc:
        raise EmbedError(
            "Gemini SDK or SOCKS support is unavailable; install kiber-analyst[embedding]"
        ) from exc

    contents = [
        types.Content(parts=[types.Part.from_text(text=text)]) for text in texts
    ]
    try:
        response = client.models.embed_content(
            model=MODEL,
            contents=contents,
            config=types.EmbedContentConfig(output_dimensionality=DIMENSIONS),
        )
    except Exception as exc:
        raise _safe_provider_error(exc) from None

    raw_embeddings = getattr(response, "embeddings", None)
    if raw_embeddings is None or len(raw_embeddings) != len(texts):
        raise ProviderError("Gemini API returned an unexpected embedding count")
    vectors: list[list[float]] = []
    for item in raw_embeddings:
        raw_values = getattr(item, "values", None)
        if raw_values is None or len(raw_values) != DIMENSIONS:
            raise ProviderError(
                "Gemini API returned an embedding with unexpected dimensions"
            )
        try:
            vectors.append([float(value) for value in raw_values])
        except (TypeError, ValueError) as exc:
            raise ProviderError(
                "Gemini API returned non-numeric embedding values"
            ) from exc

    usage = getattr(response, "usage_metadata", None)
    token_count = getattr(usage, "prompt_token_count", None)
    return vectors, token_count if isinstance(token_count, int) else None
