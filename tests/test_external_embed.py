from __future__ import annotations

from kiber_analyst.external_embed import _external_embed_lock_key


def test_external_embedding_lock_key_is_scoped_to_source() -> None:
    first = _external_embed_lock_key("github:owner/first")
    second = _external_embed_lock_key("github:owner/second")

    assert first != second
    assert "github:owner/first" in first
    assert "github:owner/second" in second
