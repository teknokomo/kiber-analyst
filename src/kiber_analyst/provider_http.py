"""Explicit provider profiles; every transport requires its profile's SOCKS proxy."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from dotenv import dotenv_values
from dotenv.parser import parse_stream

from kiber_analyst.config_paths import config_path
from kiber_analyst.errors import IngestError

PROFILES_PATH = config_path("providers.yaml")
API_ENV_PATH = config_path("provider-credentials.env")
PROXY_ENV_PATH = config_path("provider-proxies.env")


class ProfileError(IngestError):
    """Safe provider configuration error, without credentials or proxy addresses."""


@dataclass(frozen=True)
class ProviderSettings:
    profile: str
    provider: str
    api_key: str = field(repr=False)
    proxy_url: str = field(repr=False)


def _read_env(path: Path) -> dict[str, str | None]:
    try:
        raw = path.read_text(encoding="utf-8")
        names: set[str] = set()
        for binding in parse_stream(StringIO(raw)):
            if binding.error or (binding.key and binding.key in names):
                raise ProfileError("Invalid or duplicate provider environment entry")
            if binding.key:
                names.add(binding.key)
        return dict(dotenv_values(stream=StringIO(raw), interpolate=False))
    except ProfileError:
        raise
    except (OSError, UnicodeError):
        raise ProfileError("Cannot read provider environment file") from None


def _profiles(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        raise ProfileError(
            "Profile support unavailable; install kiber-analyst[providers]"
        ) from None

    class UniqueLoader(yaml.SafeLoader):
        pass

    def mapping(loader: Any, node: Any) -> dict[str, Any]:
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            if not isinstance(key, str) or key in result:
                raise ProfileError("Invalid or duplicate provider profile entry")
            result[key] = loader.construct_object(value_node)
        return result

    UniqueLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping
    )
    try:
        with path.open(encoding="utf-8") as stream:
            data = yaml.load(stream, Loader=UniqueLoader)
        if not isinstance(data, dict) or set(data) != {"profiles"}:
            raise ProfileError("Invalid provider profiles document")
        profiles = data["profiles"]
        if not isinstance(profiles, dict):
            raise ProfileError("Invalid provider profiles mapping")
        return profiles
    except ProfileError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ProfileError("Cannot read provider profiles") from None


def _validate_proxy(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"socks5", "socks5h"}
            or not parsed.hostname
            or parsed.port is None
            or not 1 <= parsed.port <= 65535
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or any(char.isspace() for char in value)
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise ProfileError(
            "Profile requires a valid SOCKS5 proxy with host and port"
        ) from None
    return value


def load_profile(
    profile: str,
    *,
    profiles_path: Path = PROFILES_PATH,
    api_env_path: Path = API_ENV_PATH,
    proxy_env_path: Path = PROXY_ENV_PATH,
) -> ProviderSettings:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", profile):
        raise ProfileError("Invalid provider profile name")
    spec = _profiles(profiles_path).get(profile)
    required = {"provider", "key_env", "proxy_env"}
    if not isinstance(spec, dict) or not required <= set(spec) <= required | {"project_id"}:
        raise ProfileError(f"Profile {profile} is missing or invalid")
    if not isinstance(spec["provider"], str) or spec["provider"] not in {
        "google",
        "groq",
    }:
        raise ProfileError(f"Profile {profile} has an unsupported provider")
    for name in (spec["key_env"], spec["proxy_env"]):
        if not isinstance(name, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
            raise ProfileError(f"Profile {profile} has an invalid variable name")
    key = _read_env(api_env_path).get(spec["key_env"])
    proxy = _read_env(proxy_env_path).get(spec["proxy_env"])
    if not isinstance(key, str) or not key or any(c.isspace() for c in key):
        raise ProfileError(f"Profile {profile} requires a nonempty API key")
    if not isinstance(proxy, str) or not proxy:
        raise ProfileError(f"Profile {profile} requires an explicit proxy")
    return ProviderSettings(profile, spec["provider"], key, _validate_proxy(proxy))


def http_client_from_settings(
    settings: ProviderSettings,
    *,
    asynchronous: bool = False,
    timeout: float = 120,
    response_codes: list[int] | None = None,
    follow_redirects: bool = False,
) -> Any:
    try:
        import httpx

        # The sole transport is itself proxied. There is no direct transport or
        # environment-derived NO_PROXY mount to fall back to.
        proxy = _validate_proxy(settings.proxy_url)
        transport_type = (
            httpx.AsyncHTTPTransport if asynchronous else httpx.HTTPTransport
        )
        client_type = httpx.AsyncClient if asynchronous else httpx.Client
        transport = transport_type(proxy=proxy, trust_env=False, retries=0)
        hooks = None
        if response_codes is not None:

            def record(response: Any) -> None:
                response_codes.append(response.status_code)

            async def record_async(response: Any) -> None:
                response_codes.append(response.status_code)

            hooks = {"response": [record_async if asynchronous else record]}
        return client_type(
            transport=transport,
            trust_env=False,
            timeout=timeout,
            event_hooks=hooks,
            follow_redirects=follow_redirects,
        )
    except ProfileError:
        raise
    except Exception:  # noqa: BLE001 - SDK/transport errors may contain secrets.
        raise ProfileError(
            f"Cannot initialize HTTP transport for profile {settings.profile}"
        ) from None


def http_client(
    profile: str,
    *,
    asynchronous: bool = False,
    timeout: float = 120,
    follow_redirects: bool = False,
    **paths: Any,
) -> Any:
    """Return a proxied HTTP client without provider authorization headers."""
    return http_client_from_settings(
        load_profile(profile, **paths),
        asynchronous=asynchronous,
        timeout=timeout,
        follow_redirects=follow_redirects,
    )


def client_from_settings(
    settings: ProviderSettings,
    *,
    timeout: float = 120,
    retry_attempts: int = 1,
    response_codes: list[int] | None = None,
) -> Any:
    """Create a synchronous SDK client; Google also exposes its configured .aio."""
    if settings.provider == "google":
        try:
            from google import genai
            from google.genai import types
        except ImportError:
            raise ProfileError(
                "Google SDK unavailable; install kiber-analyst[embedding]"
            ) from None
        import httpx

        proxy = _validate_proxy(settings.proxy_url)
        sync_transport = None
        try:
            sync_transport = httpx.HTTPTransport(
                proxy=proxy, trust_env=False, retries=0
            )
            # Both clients are SDK-owned; close()/aio.aclose() close their pools.
            # A custom async transport prevents selecting aiohttp.
            async_transport = httpx.AsyncHTTPTransport(
                proxy=proxy,
                trust_env=False,
                retries=0,
            )
            sync_args: dict[str, Any] = {
                "transport": sync_transport,
                "trust_env": False,
            }
            if response_codes is not None:

                def record(response: Any) -> None:
                    response_codes.append(response.status_code)

                sync_args["event_hooks"] = {"response": [record]}
            options = types.HttpOptions(
                base_url="https://generativelanguage.googleapis.com",
                timeout=int(timeout * 1000),
                retry_options=types.HttpRetryOptions(attempts=retry_attempts),
                client_args=sync_args,
                async_client_args={"transport": async_transport, "trust_env": False},
            )
            return genai.Client(
                vertexai=False,
                api_key=settings.api_key,
                http_options=options,
            )
        except Exception:  # noqa: BLE001 - SDK/transport errors may contain secrets.
            if sync_transport is not None:
                sync_transport.close()
            raise ProfileError(
                f"Cannot initialize Google profile {settings.profile}"
            ) from None
    if settings.provider == "groq":
        try:
            from groq import Groq
        except ImportError:
            raise ProfileError(
                "Groq SDK unavailable; install kiber-analyst[asr]"
            ) from None
        transport = http_client_from_settings(settings, timeout=timeout)
        try:
            return Groq(
                api_key=settings.api_key,
                base_url="https://api.groq.com",
                http_client=transport,
                max_retries=max(0, retry_attempts - 1),
            )
        except Exception:  # noqa: BLE001 - SDK/transport errors may contain secrets.
            transport.close()
            raise ProfileError(
                f"Cannot initialize Groq profile {settings.profile}"
            ) from None
    raise ProfileError("Unsupported provider")


def client(
    profile: str, *, timeout: float = 120, retry_attempts: int = 1, **paths: Any
) -> Any:
    return client_from_settings(
        load_profile(profile, **paths), timeout=timeout, retry_attempts=retry_attempts
    )


def genai_client(
    profile: str,
    *,
    timeout: float = 120,
    retry_attempts: int = 1,
    **paths: Any,
) -> Any:
    settings = load_profile(profile, **paths)
    if settings.provider != "google":
        raise ProfileError("Google client requires a Google profile")
    return client_from_settings(
        settings, timeout=timeout, retry_attempts=retry_attempts
    )


def groq_client(profile: str, *, timeout: float = 120, **paths: Any) -> Any:
    settings = load_profile(profile, **paths)
    if settings.provider != "groq":
        raise ProfileError("Groq client requires a Groq profile")
    return client_from_settings(settings, timeout=timeout)


def async_groq_client(profile: str, *, timeout: float = 120, **paths: Any) -> Any:
    settings = load_profile(profile, **paths)
    if settings.provider != "groq":
        raise ProfileError("Groq client requires a Groq profile")
    try:
        from groq import AsyncGroq
    except ImportError:
        raise ProfileError("Groq SDK unavailable; install kiber-analyst[asr]") from None
    try:
        return AsyncGroq(
            api_key=settings.api_key,
            base_url="https://api.groq.com",
            max_retries=0,
            http_client=http_client_from_settings(
                settings, asynchronous=True, timeout=timeout
            ),
        )
    except Exception:  # noqa: BLE001 - SDK errors may contain secrets.
        raise ProfileError(
            f"Cannot initialize async Groq profile {settings.profile}"
        ) from None
