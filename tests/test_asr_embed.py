from __future__ import annotations

from types import SimpleNamespace

import pytest

from kiber_analyst import asr_embed
from kiber_analyst.asr_embed import (
    ASR_EMBED_SELECT_SQL,
    ASREmbedError,
    _embed_resilient,
    _has_text,
)
from kiber_analyst.embedding_api import DIMENSIONS, ProviderError


def test_asr_embed_skips_empty_and_uses_asr_owner_table_contract() -> None:
    assert _has_text("текст")
    assert not _has_text("  \n")
    normalized = " ".join(ASR_EMBED_SELECT_SQL.lower().split())
    assert "owner_table = 'asr_segments'" in normalized
    assert "owner_id = a.id::text" in normalized
    assert "then 'group'" in normalized
    assert "then ('topic:' || (d.meta->>'topic_id'))" in normalized
    assert "%s::text is null or exists" in normalized
    assert "gemini-embedding-2:1536" in asr_embed.MODEL_KEY


def test_asr_embed_source_scopes_checkpoint_and_rejects_invalid_source() -> None:
    first = "telegram:-1001:topic:1"
    second = "telegram:-1001:topic:2"
    assert asr_embed._scope_key("sample-project", None, first) != asr_embed._scope_key(
        "sample-project", None, second
    )
    with pytest.raises(ASREmbedError, match="Invalid Telegram source ID"):
        asr_embed.embed_asr(
            project="sample-project", all_runs=True, source_id="telegram:-1001:topic:../1"
        )


def test_asr_embed_source_scope_accepts_groups_without_crossing_topic_scope():
    topic_scope = asr_embed._telegram_scope_key('telegram:-1001:topic:1')
    group_scope = asr_embed._telegram_scope_key('telegram:-1001:group')
    assert topic_scope == 'topic:1'
    assert group_scope == 'group'
    assert asr_embed._scope_key('sample-project', None, topic_scope) != (
        asr_embed._scope_key('sample-project', None, group_scope)
    )


def test_asr_embed_retry_and_split_match_existing_batch_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizes: list[int] = []
    calls = [0]
    waits: list[float] = []

    def fake_embed(_client: object, texts: list[str]) -> tuple[list[list[float]], int]:
        sizes.append(len(texts))
        if len(texts) == 64:
            raise ProviderError("Gemini API HTTP 503", status_code=503)
        return [[0.0] * DIMENSIONS for _ in texts], len(texts)

    monkeypatch.setattr(asr_embed, "embed_texts", fake_embed)
    vectors, tokens = _embed_resilient(
        SimpleNamespace(),
        [str(index) for index in range(64)],
        call_counter=calls,
        sleep=waits.append,
        jitter=lambda seconds: seconds,
    )
    assert sizes == [64, 64, 64, 16, 16, 16, 16]
    assert calls == [7]
    assert waits == [60, 180]
    assert len(vectors) == 64
    assert tokens == 64


def test_asr_embed_propagates_auth_errors_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = [0]

    def unauthorized(
        _client: object, _texts: list[str]
    ) -> tuple[list[list[float]], None]:
        raise ProviderError("Gemini API HTTP 401", status_code=401)

    monkeypatch.setattr(asr_embed, "embed_texts", unauthorized)
    with pytest.raises(ProviderError):
        _embed_resilient(
            SimpleNamespace(),
            ["text"],
            call_counter=calls,
            sleep=lambda _seconds: None,
            jitter=lambda seconds: seconds,
        )
    assert calls == [1]


def test_asr_embed_requires_one_scope() -> None:
    with pytest.raises(ASREmbedError, match="exactly one"):
        asr_embed.embed_asr(project="sample-project")
