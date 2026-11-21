from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

httpx = pytest.importorskip("httpx")
pytest.importorskip("yaml")
pytest.importorskip("groq")
pytest.importorskip("google.genai")

from kiber_analyst import provider_http as providers  # noqa: E402
from kiber_analyst.embedding_api import create_client, load_provider_settings  # noqa: E402
from kiber_analyst.provider_preflight import preflight  # noqa: E402


@pytest.fixture
def profiles(tmp_path: Path) -> dict[str, Path]:
    profiles_path = tmp_path / "providers.yaml"
    profiles_path.write_text(
        "profiles:\n"
        "  groq_asr: {provider: groq, key_env: GROQ_API_KEY, proxy_env: GROQ_PROXY}\n"
        "  groq_other: {provider: groq, key_env: OTHER_API_KEY, proxy_env: OTHER_PROXY}\n"
        "  gemini_embed: {provider: google, key_env: GEMINI_API_KEY, proxy_env: GEMINI_PROXY}\n"
    )
    api = tmp_path / "media.env"
    api.write_text(
        "GROQ_API_KEY=gsk_fake_one\nOTHER_API_KEY=gsk_fake_two\nGEMINI_API_KEY=fake_google\n"
    )
    proxy = tmp_path / "media-proxy.env"
    proxy.write_text(
        "GROQ_PROXY=socks5://alice:fake_one@one.invalid:1080\n"
        "OTHER_PROXY=socks5h://bob:fake_two@two.invalid:1081\n"
        "GEMINI_PROXY=socks5://carol:fake_google@three.invalid:1082\n"
    )
    return {
        "profiles_path": profiles_path,
        "api_env_path": api,
        "proxy_env_path": proxy,
    }


@pytest.fixture
def transports(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[tuple[bool, dict[str, Any]]], list[Any]]:
    constructors: list[tuple[bool, dict[str, Any]]] = []
    requests: list[Any] = []

    def handle(request: Any) -> Any:
        requests.append(request)
        if request.url.host == "ipinfo.io":
            assert "authorization" not in request.headers
            assert "x-goog-api-key" not in request.headers
            return httpx.Response(
                200, json={"country": "US", "org": "AS64512 Test only"}
            )
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"object": "list", "data": []})
        if request.url.path.endswith("/audio/transcriptions"):
            return httpx.Response(
                200, json={"text": "Private fake transcript must not be reported"}
            )
        return httpx.Response(200, json={"embeddings": [{"values": [0.1] * 1536}]})

    def transport(asynchronous: bool, **kwargs: Any) -> Any:
        constructors.append((asynchronous, kwargs))
        return httpx.MockTransport(handle)

    monkeypatch.setattr(
        httpx, "HTTPTransport", lambda **kwargs: transport(False, **kwargs)
    )
    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport", lambda **kwargs: transport(True, **kwargs)
    )
    return constructors, requests


def test_two_groq_profiles_keep_distinct_keys_and_proxies(
    profiles: dict, transports: tuple
) -> None:
    constructors, requests = transports
    with providers.client("groq_asr", **profiles) as one:
        one.models.list()
    with providers.groq_client("groq_other", **profiles) as two:
        two.models.list()
    assert [kwargs["proxy"] for _, kwargs in constructors] == [
        "socks5://alice:fake_one@one.invalid:1080",
        "socks5h://bob:fake_two@two.invalid:1081",
    ]
    assert [r.headers["authorization"] for r in requests] == [
        "Bearer gsk_fake_one",
        "Bearer gsk_fake_two",
    ]
    assert all(
        kwargs["trust_env"] is False and kwargs["retries"] == 0
        for _, kwargs in constructors
    )


@pytest.mark.parametrize("missing", ["profile", "key", "proxy", "empty_proxy"])
def test_missing_profile_key_or_proxy_stops_before_transport(
    missing: str,
    profiles: dict,
    transports: tuple,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIPELINE_PROXY", "socks5://ambient.invalid:1234")
    monkeypatch.setenv("GROQ_PROXY", "socks5://ambient.invalid:1234")
    if missing == "profile":
        profiles["profiles_path"].write_text("profiles: {}\n")
    elif missing == "key":
        profiles["api_env_path"].write_text("GROQ_API_KEY=\n")
    else:
        profiles["proxy_env_path"].write_text(
            "GROQ_PROXY=\n"
            if missing == "empty_proxy"
            else "PIPELINE_PROXY=socks5://ambient.invalid:1234\n"
        )
    with pytest.raises(providers.ProfileError):
        providers.client("groq_asr", **profiles)
    assert transports[0] == []


@pytest.mark.parametrize(
    "proxy",
    [
        "http://fake:secret@proxy.invalid:1080",
        "socks5://:1080",
        "socks5://fake:secret@proxy.invalid:70000",
        "socks5://proxy.invalid:1080/path",
        "socks5://proxy.invalid:1080?bypass=1",
        "socks5://proxy.invalid",
    ],
)
def test_invalid_proxy_is_rejected_without_exposing_credentials(
    proxy: str, profiles: dict, transports: tuple
) -> None:
    profiles["proxy_env_path"].write_text(f"GROQ_PROXY={proxy}\n")
    with pytest.raises(providers.ProfileError) as caught:
        providers.client("groq_asr", **profiles)
    assert proxy not in str(caught.value)
    assert "secret" not in str(caught.value)
    assert transports[0] == []


def test_settings_repr_hides_key_and_proxy(profiles: dict) -> None:
    settings = providers.load_profile("groq_asr", **profiles)
    assert settings.api_key not in repr(settings)
    assert settings.proxy_url not in repr(settings)
    assert "fake_one" not in repr(settings)


@pytest.mark.parametrize("duplicate", ["yaml", "env"])
def test_duplicate_configuration_is_rejected(duplicate: str, profiles: dict) -> None:
    if duplicate == "yaml":
        profiles["profiles_path"].write_text("profiles: {}\nprofiles: {}\n")
    else:
        profiles["api_env_path"].write_text("GROQ_API_KEY=one\nGROQ_API_KEY=two\n")
    with pytest.raises(providers.ProfileError):
        providers.load_profile("groq_asr", **profiles)


def test_profile_values_do_not_expand_from_environment(
    profiles: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FOREIGN_PROXY", "socks5://foreign.invalid:1234")
    profiles["proxy_env_path"].write_text("GROQ_PROXY=${FOREIGN_PROXY}\n")
    with pytest.raises(providers.ProfileError):
        providers.load_profile("groq_asr", **profiles)


def test_google_sync_and_async_force_profile_transport_even_with_aiohttp(
    profiles: dict,
    transports: tuple,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.genai import _api_client

    monkeypatch.setattr(_api_client, "has_aiohttp", True)
    settings = load_provider_settings(**profiles)
    sdk = create_client(settings)
    try:
        assert sdk._api_client._use_aiohttp() is False
        assert sdk._api_client._httpx_client.trust_env is False
        assert sdk._api_client._async_httpx_client.trust_env is False
        response = sdk.models.embed_content(model="gemini-embedding-2", contents="fake")
        assert len(response.embeddings[0].values) == 1536

        async def embed() -> None:
            response = await sdk.aio.models.embed_content(
                model="gemini-embedding-2", contents="fake"
            )
            assert len(response.embeddings[0].values) == 1536
            await sdk.aio.aclose()

        asyncio.run(embed())
    finally:
        sdk.close()
    assert sdk._api_client._httpx_client.is_closed
    assert sdk._api_client._async_httpx_client.is_closed
    constructors, requests = transports
    assert [async_ for async_, _ in constructors] == [False, True]
    assert all(
        kwargs["proxy"] == settings.proxy_url and kwargs["trust_env"] is False
        for _, kwargs in constructors
    )
    assert all(r.headers["x-goog-api-key"] == "fake_google" for r in requests)


def test_async_groq_uses_its_own_profile(profiles: dict, transports: tuple) -> None:
    async def check() -> None:
        async with providers.async_groq_client("groq_other", **profiles) as sdk:
            await sdk.models.list()

    asyncio.run(check())
    constructors, requests = transports
    assert constructors[0][0] is True
    assert constructors[0][1]["proxy"] == "socks5h://bob:fake_two@two.invalid:1081"
    assert requests[0].headers["authorization"] == "Bearer gsk_fake_two"


def test_failed_socks_connection_never_attempts_direct_destination(
    profiles: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    import httpcore
    from httpcore._backends.sync import SyncBackend

    attempts = []

    def fail(_self: Any, host: str, port: int, **kwargs: Any) -> Any:
        attempts.append((host, port))
        raise httpcore.ConnectError("fake proxy unavailable")

    monkeypatch.setattr(SyncBackend, "connect_tcp", fail)
    with (
        providers.http_client("groq_asr", **profiles) as network,
        pytest.raises(httpx.ConnectError),
    ):
        network.get("https://destination.invalid/")
    assert attempts == [("one.invalid", 1080)]


def test_failed_async_socks_connection_never_attempts_direct_destination(
    profiles: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    import httpcore
    from httpcore._backends.anyio import AnyIOBackend

    attempts = []

    async def fail(_self: Any, host: str, port: int, **kwargs: Any) -> Any:
        attempts.append((host, port))
        raise httpcore.ConnectError("fake proxy unavailable")

    monkeypatch.setattr(AnyIOBackend, "connect_tcp", fail)

    async def check() -> None:
        async with providers.http_client(
            "groq_asr", asynchronous=True, **profiles
        ) as network:
            with pytest.raises(httpx.ConnectError):
                await network.get("https://destination.invalid/")

    asyncio.run(check())
    assert attempts == [("one.invalid", 1080)]


def test_asr_preflight_uses_profile_and_hides_transcript(
    profiles: dict, transports: tuple, tmp_path: Path
) -> None:
    audio = tmp_path / "fake.ogg"
    audio.write_bytes(b"fake OGG fixture, not real audio")
    result = preflight("groq_asr", audio=audio, egress=True, **profiles)
    assert result == {
        "profile": "groq_asr",
        "proxy_used": True,
        "country": "US",
        "asn": "AS64512",
        "models_http": 200,
        "asr_http": 200,
        "transcription_nonempty": True,
    }
    assert "Private fake transcript" not in json.dumps(result)
    assert [r.method for r in transports[1]] == ["GET", "GET", "POST"]


def test_google_preflight_uses_profile_without_database(
    profiles: dict, transports: tuple
) -> None:
    result = preflight("gemini_embed", **profiles)
    assert result["profile"] == "gemini_embed"
    assert result["dimensions"] == 1536
    assert result["embed_http"] == 200


def test_sdk_initialization_error_does_not_expose_secrets(
    profiles: dict, transports: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    import groq

    def fail(**kwargs: Any) -> Any:
        raise RuntimeError("gsk_fake_one socks5://alice:fake_one@one.invalid:1080")

    monkeypatch.setattr(groq, "Groq", fail)
    with pytest.raises(providers.ProfileError) as caught:
        providers.groq_client("groq_asr", **profiles)
    assert "fake_one" not in str(caught.value)
    assert caught.value.__suppress_context__ is True


@pytest.mark.parametrize(
    "factory",
    [
        providers.client,
        providers.genai_client,
        providers.groq_client,
        providers.async_groq_client,
    ],
)
def test_factory_requires_explicit_profile(factory: Any, transports: tuple) -> None:
    with pytest.raises(TypeError):
        factory()
    assert transports[0] == []


@pytest.mark.parametrize(
    "factory,profile",
    [
        (providers.genai_client, "groq_asr"),
        (providers.groq_client, "gemini_embed"),
        (providers.async_groq_client, "gemini_embed"),
    ],
)
def test_wrong_provider_profile_stops_before_transport(
    factory: Any, profile: str, profiles: dict, transports: tuple
) -> None:
    with pytest.raises(providers.ProfileError):
        factory(profile, **profiles)
    assert transports[0] == []
