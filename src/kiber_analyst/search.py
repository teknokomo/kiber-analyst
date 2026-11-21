from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from kiber_analyst.config_paths import config_path
from kiber_analyst.embedding_api import (
    create_client,
    embed_texts,
    load_provider_settings,
)
from kiber_analyst.errors import EmbedError, SearchError
from kiber_analyst.load import _connect, _required_env
from kiber_analyst.search_core import (
    SEARCH_SQL as SEARCH_SQL,
)
from kiber_analyst.search_core import (
    _timecode as _timecode,
)
from kiber_analyst.search_core import (
    execute_search,
)


def _query_vector(
    query: str,
    *,
    embedding_provider: Callable[[str], list[float]] | None = None,
) -> tuple[list[float] | None, str | None]:
    try:
        if embedding_provider is not None:
            return embedding_provider(query), None
        settings = load_provider_settings()
        client = create_client(settings)
        try:
            vectors, _ = embed_texts(client, [query])
        finally:
            client.close()
        return vectors[0], None
    except EmbedError as exc:
        digest = hashlib.sha256(query.encode("utf-8")).hexdigest()
        print(
            f"vector_unavailable query_sha256={digest} query_length={len(query)}",
            file=sys.stderr,
        )
        return None, type(exc).__name__
    except Exception as exc:
        from kiber_analyst.routing import PermanentProviderError

        if isinstance(exc, PermanentProviderError):
            raise
        digest = hashlib.sha256(query.encode("utf-8")).hexdigest()
        print(
            f"vector_unavailable query_sha256={digest} query_length={len(query)}",
            file=sys.stderr,
        )
        return None, type(exc).__name__



def search(
    query: str,
    *,
    project: str,
    limit: int = 10,
    source_kind: str | None = None,
    source: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    author: str | None = None,
    topic: str | None = None,
    source_id: str | None = None,
    env_path: Path = config_path("knowledge.env"),
    embedding_provider: Callable[[str], list[float]] | None = None,
) -> dict[str, Any]:
    normalized_query = query.strip()
    vector, degraded_reason = _query_vector(
        normalized_query, embedding_provider=embedding_provider
    )
    try:
        db_config = _required_env(env_path)
    except Exception as exc:
        raise SearchError(
            f"Cannot read knowledge database settings ({type(exc).__name__})"
        ) from exc

    connection = None
    try:
        connection = _connect(db_config)
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
            return execute_search(
                connection,
                normalized_query,
                project=project,
                vector=vector,
                limit=limit,
                source_kind=source_kind,
                source=source,
                date_from=date_from,
                date_to=date_to,
                author=author,
                topic=topic,
                source_id=source_id,
                degraded_reason=degraded_reason,
            )
    except SearchError:
        raise
    except Exception as exc:
        sqlstate = getattr(exc, "sqlstate", None)
        suffix = f" SQLSTATE {sqlstate}" if isinstance(sqlstate, str) else ""
        raise SearchError(
            f"Knowledge search failed ({type(exc).__name__}{suffix})"
        ) from exc
    finally:
        if connection is not None:
            connection.close()


def print_search_result(result: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    for index, item in enumerate(result["results"], start=1):
        posted_at = item["posted_at"] or "дата не указана"
        author = item["author"] or "автор не указан"
        print(
            f"{index}. [{item['score']:.3f}] {posted_at}, {author} [{item['source']}]"
        )
        if item["source"] == "asr":
            print(f"   {item['timecode']} | «{item['text']}»")
        else:
            print(f"   «{item['text']}»")
        print(f"   {item['url'] or 'ссылка не указана'}")
        print(
            f"   segment_id={item['segment_id']}, "
            f"asr_segment_id={item['asr_segment_id']}, "
            f"document_id={item['document_id']}"
        )
