from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from kiber_analyst import embed as embed_module
from kiber_analyst.cli import _parser
from kiber_analyst.embed import _embed_resilient, _read_records, _retry_embed
from kiber_analyst.embedding_api import DIMENSIONS, ProviderError, embed_texts
from kiber_analyst.search import SEARCH_SQL


def test_normalized_messages_are_adapted_to_segment_input(tmp_path: Path) -> None:
    messages = tmp_path / "messages.jsonl"
    messages.write_text(
        json.dumps(
            {
                "chat_id": "-10012345",
                "message_id": "7087",
                "content_hash": "sha256:abc",
                "text": "Текст сообщения",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    result = _read_records(messages, normalized_messages=True)

    assert result == [
        {
            "external_ref": "tg:-10012345:7087",
            "content_hash": "sha256:abc",
            "text": "Текст сообщения",
        }
    ]


def test_embed_texts_sends_one_content_object_per_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class Part:
        @staticmethod
        def from_text(*, text: str) -> str:
            return text

    class Content:
        def __init__(self, *, parts: list[str]) -> None:
            self.parts = parts

    class EmbedContentConfig:
        def __init__(self, *, output_dimensionality: int) -> None:
            self.output_dimensionality = output_dimensionality

    fake_types = SimpleNamespace(
        Part=Part,
        Content=Content,
        EmbedContentConfig=EmbedContentConfig,
    )
    google_module = ModuleType("google")
    google_module.__path__ = []  # type: ignore[attr-defined]
    genai_module = ModuleType("google.genai")
    genai_module.types = fake_types  # type: ignore[attr-defined]
    google_module.genai = genai_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.genai", genai_module)

    class Models:
        def embed_content(self, **kwargs: object) -> object:
            captured.update(kwargs)
            return SimpleNamespace(
                embeddings=[
                    SimpleNamespace(values=[0.25] * DIMENSIONS),
                    SimpleNamespace(values=[0.75] * DIMENSIONS),
                ],
                usage_metadata=None,
            )

    vectors, token_count = embed_texts(SimpleNamespace(models=Models()), ["one", "two"])

    assert len(captured["contents"]) == 2  # type: ignore[arg-type]
    assert [item.parts for item in captured["contents"]] == [["one"], ["two"]]  # type: ignore[union-attr]
    assert captured["model"] == "gemini-embedding-2"
    assert captured["config"].output_dimensionality == DIMENSIONS  # type: ignore[union-attr]
    assert len(vectors) == 2 and all(len(vector) == DIMENSIONS for vector in vectors)
    assert token_count is None


def test_retry_uses_only_transient_statuses_and_bounded_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    waits: list[float] = []

    def flaky(_client: object, texts: list[str]) -> tuple[list[list[float]], int]:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ProviderError("Gemini API HTTP 503", status_code=503)
        return [[0.0] * DIMENSIONS for _ in texts], 4

    monkeypatch.setattr(embed_module, "embed_texts", flaky)
    result, token_count = _retry_embed(
        object(), ["text"], sleep=waits.append, jitter=lambda seconds: seconds
    )

    assert len(result) == 1
    assert token_count == 4
    assert calls == 3
    assert waits == [60, 180]


def test_batch_error_reduces_64_inputs_to_16(monkeypatch: pytest.MonkeyPatch) -> None:
    sizes: list[int] = []
    waits: list[float] = []

    def batch(_client: object, texts: list[str]) -> tuple[list[list[float]], None]:
        sizes.append(len(texts))
        if len(texts) == 64:
            raise ProviderError("Gemini API HTTP 500", status_code=500)
        return [[0.0] * DIMENSIONS for _ in texts], None

    monkeypatch.setattr(embed_module, "embed_texts", batch)
    vectors, tokens = _embed_resilient(
        object(),
        [str(index) for index in range(64)],
        sleep=waits.append,
        jitter=lambda seconds: seconds,
    )

    assert sizes == [64, 64, 64, 16, 16, 16, 16]
    assert waits == [60, 180]
    assert len(vectors) == 64
    assert tokens is None


def test_search_uses_read_only_hybrid_reciprocal_rank_fusion_sql() -> None:
    normalized = " ".join(SEARCH_SQL.lower().split())
    assert "websearch_to_tsquery('russian'" in normalized
    assert "ts_rank_cd" in normalized
    assert "embedding <=> q.query_vector" in normalized
    assert "1.0 / (60 + position)" in normalized
    assert "insert into" not in normalized
    assert "update " not in normalized
    assert "delete " not in normalized


def test_search_sql_uses_group_title_and_preserves_sender_identity() -> None:
    normalized = " ".join(SEARCH_SQL.lower().split())
    assert normalized.count(
        "case when so.kind = 'telegram' then p.title else so.title end as chat_title"
    ) == 2
    assert normalized.count(
        "coalesce(nullif(s.speaker, ''), nullif(s.meta->>'author_id', ''), "
        "nullif(s.meta->>'sender_id', ''), 'unknown') as author"
    ) == 2


def test_cli_registers_embed_and_search_commands() -> None:
    parser = _parser()
    embed_args = parser.parse_args(["embed", "--job", "/tmp/job"])
    search_args = parser.parse_args(
        ["search", "ангар 33 метра", "--project", "sample-project", "--json"]
    )

    assert embed_args.source == "embed"
    assert embed_args.job == Path("/tmp/job")
    assert search_args.source == "search"
    assert search_args.as_json is True


def test_search_cli_accepts_an_exact_source_id_filter() -> None:
    args = _parser().parse_args([
        "search", "query", "--project", "sample-project",
        "--source-id", "telegram:-10012345:topic:1",
    ])
    assert args.source_id == "telegram:-10012345:topic:1"
