from __future__ import annotations

import pytest

from kiber_analyst.routing import PermanentProviderError
from kiber_analyst.search import SEARCH_SQL, _timecode, search


def test_search_union_preserves_source_identity_and_provenance() -> None:
    normalized = " ".join(SEARCH_SQL.lower().split())
    assert "'segment'::text as source" in normalized
    assert "'asr'::text as source" in normalized
    assert "union all" in normalized
    assert "asr_runs ar" in normalized
    assert "r.status = 'complete'" in normalized
    assert "p.id = ar.project_id" in normalized
    assert "e.owner_table = case" in normalized
    assert "when b.source = 'asr' then 'asr_segments'" in normalized
    assert "a.start_ms" in normalized and "a.end_ms" in normalized
    assert "a.run_id as asr_run_id" in normalized
    assert "as source_id" in normalized
    assert "as chat_title" in normalized
    assert "as topic_title" in normalized
    assert "as message_id" in normalized


def test_search_keeps_parameterized_read_only_fts_vector_rrf() -> None:
    normalized = " ".join(SEARCH_SQL.lower().split())
    assert "websearch_to_tsquery('russian'" in normalized
    assert "embedding <=> q.query_vector" in normalized
    assert "1.0 / (60 + position)" in normalized
    assert "%(source)s" in normalized
    assert "%(source_kind)s" in normalized
    assert "%(source_id)s" in normalized
    assert "u.source_id = %(source_id)s::text" in normalized
    assert "insert into" not in normalized
    assert "update " not in normalized
    assert "delete " not in normalized


def test_asr_timecode_is_hh_mm_ss_millis() -> None:
    assert _timecode(268660) == "00:04:28.660"
    assert _timecode(3_661_007) == "01:01:01.007"
    assert _timecode(None) is None


def test_permanent_provider_rejection_does_not_silently_fall_back_to_fts() -> None:
    def rejected(_query: str):
        raise PermanentProviderError("HTTP 403", status_code=403, category="policy_blocked")

    with pytest.raises(PermanentProviderError):
        search("query", project="sample-project", embedding_provider=rejected)
