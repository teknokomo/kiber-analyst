from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import unicodedata
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import ijson
from ijson.common import ObjectBuilder

from kiber_analyst.errors import ParseError
from kiber_analyst.paths import job_paths, read_json, write_json_atomic

MOSCOW = ZoneInfo("Europe/Moscow")
NO_TOPIC_TITLE = "Без установленной темы"


def flatten_text(value: Any) -> tuple[str | None, list | None]:
    if value is None:
        return None, None
    if isinstance(value, str):
        return value, None
    if not isinstance(value, list):
        raise ParseError(f"Unsupported Telegram text value: {type(value).__name__}")
    pieces: list[str] = []
    for entity in value:
        if isinstance(entity, str):
            pieces.append(entity)
        elif isinstance(entity, dict):
            text = entity.get("text")
            if isinstance(text, str):
                pieces.append(text)
        else:
            raise ParseError(
                f"Unsupported Telegram text entity: {type(entity).__name__}"
            )
    return "".join(pieces), value


def normalized_content_hash(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = unicodedata.normalize(
        "NFC", text.replace("\r\n", "\n").replace("\r", "\n")
    )
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_date(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str):
        raise ParseError(
            f"Telegram date must be a string or null, got {type(value).__name__}"
        )
    raw = value
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ParseError(f"Invalid Telegram date {value!r}: {exc}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOSCOW)
    return parsed.isoformat(), raw


def canonical_chat_id(raw_id: Any) -> str | None:
    if raw_id is None:
        return None
    value = str(raw_id)
    return value


def _top_level_layout(path: Path) -> str:
    found: set[str] = set()
    try:
        with path.open("rb") as stream:
            for prefix, event, value in ijson.parse(stream):
                if (
                    prefix == ""
                    and event == "map_key"
                    and value in {"chats", "messages"}
                ):
                    found.add(value)
    except (OSError, ijson.JSONError) as exc:
        raise ParseError(f"Cannot read Telegram result.json: {exc}") from exc
    if found == {"chats"}:
        return "full"
    if found == {"messages"}:
        return "single"
    raise ParseError("result.json must be a single-chat export or contain chats")


def _new_index(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        PRAGMA journal_mode=MEMORY;
        PRAGMA synchronous=OFF;
        CREATE TABLE chats (
            chat_index INTEGER PRIMARY KEY,
            raw_id TEXT,
            title TEXT,
            chat_type TEXT
        );
        CREATE TABLE messages (
            chat_index INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            message_id TEXT,
            reply_to TEXT,
            is_service INTEGER NOT NULL,
            action TEXT,
            action_title TEXT,
            payload TEXT NOT NULL,
            PRIMARY KEY (chat_index, ordinal)
        );
        CREATE UNIQUE INDEX uq_message_id
            ON messages(chat_index, message_id) WHERE message_id IS NOT NULL;
        CREATE INDEX idx_reply_to ON messages(chat_index, reply_to);
        """
    )
    return conn


def _field(message: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in message:
            return message[name]
    return None


def _scalar(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _insert_message(
    conn: sqlite3.Connection,
    chat_index: int,
    ordinal: int,
    message: Any,
) -> None:
    if not isinstance(message, dict):
        raise ParseError(f"Message at index {ordinal} is not an object")
    message_id = _scalar(_field(message, "id", "message_id"))
    reply_to = _scalar(_field(message, "reply_to_message_id", "reply_to"))
    is_service = message.get("type") == "service"
    action = message.get("action") if is_service else None
    title = message.get("title") if action == "topic_created" else None
    try:
        conn.execute(
            """INSERT INTO messages
               (chat_index, ordinal, message_id, reply_to, is_service,
                action, action_title, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                chat_index,
                ordinal,
                message_id,
                reply_to,
                int(is_service),
                action,
                title if isinstance(title, str) else None,
                json.dumps(message, ensure_ascii=False, separators=(",", ":")),
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise ParseError(
            f"Duplicate message id {message_id!r} in chat index {chat_index}"
        ) from exc


def _scan_to_index(path: Path, conn: sqlite3.Connection, layout: str) -> None:
    chat_index = -1
    ordinal_by_chat: Counter[int] = Counter()
    builder: ObjectBuilder | None = None
    builder_depth = 0
    builder_chat_index = -1

    if layout == "single":
        chat_index = 0
        conn.execute("INSERT INTO chats(chat_index) VALUES (0)")
        message_prefix = "messages.item"
    else:
        message_prefix = "chats.list.item.messages.item"

    try:
        with path.open("rb") as stream:
            for prefix, event, value in ijson.parse(stream):
                if (
                    layout == "full"
                    and prefix == "chats.list.item"
                    and event == "start_map"
                ):
                    chat_index += 1
                    conn.execute(
                        "INSERT INTO chats(chat_index) VALUES (?)", (chat_index,)
                    )
                elif layout == "full" and event in {"string", "number", "null"}:
                    parts = prefix.split(".")
                    if len(parts) == 4 and parts[:3] == ["chats", "list", "item"]:
                        field = parts[3]
                        column = {
                            "id": "raw_id",
                            "name": "title",
                            "title": "title",
                            "type": "chat_type",
                        }.get(field)
                        if column:
                            conn.execute(
                                f"UPDATE chats SET {column} = ? WHERE chat_index = ?",
                                (_scalar(value), chat_index),
                            )
                elif (
                    layout == "single"
                    and prefix in {"id", "name", "title", "type"}
                    and event
                    in {
                        "string",
                        "number",
                        "null",
                    }
                ):
                    column = {
                        "id": "raw_id",
                        "name": "title",
                        "title": "title",
                        "type": "chat_type",
                    }[prefix]
                    conn.execute(
                        f"UPDATE chats SET {column} = ? WHERE chat_index = 0",
                        (_scalar(value),),
                    )

                if (
                    builder is None
                    and prefix == message_prefix
                    and event == "start_map"
                ):
                    builder = ObjectBuilder()
                    builder.event(event, value)
                    builder_depth = 1
                    builder_chat_index = chat_index
                    continue
                if builder is not None:
                    builder.event(event, value)
                    if event in {"start_map", "start_array"}:
                        builder_depth += 1
                    elif event in {"end_map", "end_array"}:
                        builder_depth -= 1
                        if builder_depth == 0:
                            ordinal = ordinal_by_chat[builder_chat_index]
                            _insert_message(
                                conn, builder_chat_index, ordinal, builder.value
                            )
                            ordinal_by_chat[builder_chat_index] += 1
                            builder = None
    except (OSError, ijson.JSONError, sqlite3.Error) as exc:
        if isinstance(exc, ParseError):
            raise
        raise ParseError(f"Cannot stream Telegram result.json: {exc}") from exc
    if builder is not None:
        raise ParseError("Unexpected end of result.json inside a message")
    conn.commit()


def _topic_created(conn: sqlite3.Connection, chat_index: int) -> dict[str, str | None]:
    topics: dict[str, str | None] = {}
    for message_id, title in conn.execute(
        """SELECT message_id, action_title FROM messages
           WHERE chat_index = ? AND is_service = 1 AND action = 'topic_created'
           AND message_id IS NOT NULL""",
        (chat_index,),
    ):
        topics[str(message_id)] = title
    return topics


def _resolve_chain(
    conn: sqlite3.Connection,
    chat_index: int,
    reply_to: str | None,
    topic_markers: dict[str, str | None],
    cache: dict[str, tuple[str | None, str | None, bool]],
) -> tuple[str | None, str | None, bool]:
    current = reply_to
    if current is None:
        return None, None, False
    path: list[str] = []
    visited: set[str] = set()
    result: tuple[str | None, str | None, bool]
    while current is not None:
        if current in topic_markers:
            result = (current, topic_markers[current], False)
            break
        if current in cache:
            result = cache[current]
            break
        if current in visited:
            result = (None, None, True)
            break
        visited.add(current)
        path.append(current)
        row = conn.execute(
            """SELECT reply_to, is_service, action, action_title
               FROM messages WHERE chat_index = ? AND message_id = ?""",
            (chat_index, current),
        ).fetchone()
        if row is None:
            result = (None, None, True)
            break
        parent_reply, is_service, action, action_title = row
        if is_service and action == "topic_created":
            result = (current, action_title, False)
            break
        current = parent_reply
        if current is None:
            # A reply chain that bottoms out at an ordinary message did not
            # reach a topic marker; report it as incomplete for diagnostics.
            result = (None, None, bool(path))
            break
    for message_id in path:
        cache[message_id] = result
    return result


def _scope_info(manifest: dict[str, Any]) -> tuple[str, str | None, str | None]:
    scope = manifest.get("scope", "unknown")
    topic_id = manifest.get("scope_topic_id")
    title = manifest.get("scope_topic_title")
    if scope not in {"topic", "chat", "unknown"}:
        raise ParseError(f"Unsupported export scope: {scope!r}")
    if scope == "topic" and not topic_id:
        raise ParseError("topic scope requires scope_topic_id in manifest.json")
    return scope, str(topic_id) if topic_id is not None else None, title


def _make_url(chat_id: str, topic_id: str, message_id: str) -> str:
    path_chat_id = chat_id.removeprefix("-100").removeprefix("-")
    return f"https://t.me/c/{path_chat_id}/{topic_id}/{message_id}"


def _entity_link_count(entities: Any) -> int:
    if not isinstance(entities, list):
        return 0
    return sum(
        1
        for entity in entities
        if isinstance(entity, dict)
        and entity.get("type") in {"text_link", "url"}
        and isinstance(entity.get("href", entity.get("url")), str)
    )


def _message_link_count(message: dict[str, Any]) -> int:
    import re

    text = message.get("text")
    visible = (
        len(re.findall(r"https?://[^\s<>]+", text)) if isinstance(text, str) else 0
    )
    return visible + _entity_link_count(message.get("entities"))


def _media(message: dict[str, Any]) -> tuple[str | None, str | None]:
    explicit = message.get("media_type")
    if isinstance(explicit, str):
        for key in ("media_path", "file", "photo"):
            path = message.get(key)
            if isinstance(path, str) and path:
                return explicit, path
        return explicit, None
    for key in (
        "photo",
        "video_file",
        "video",
        "audio_file",
        "voice_message",
        "file",
        "sticker",
    ):
        value = message.get(key)
        if value is not None:
            if isinstance(value, str):
                return key, value
            if isinstance(value, dict):
                path = value.get("path") or value.get("file")
                return key, path if isinstance(path, str) else None
            return key, None
    return None, None


def _record_message(
    payload: dict[str, Any],
    chat_id: str,
    chat_title: str | None,
    scope: str,
    scope_topic_id: str | None,
    scope_topic_title: str | None,
    topic_markers: dict[str, str | None],
    conn: sqlite3.Connection,
    chat_index: int,
    chain_cache: dict[str, tuple[str | None, str | None, bool]],
) -> tuple[dict[str, Any], bool]:
    message_id_raw = _field(payload, "id", "message_id")
    message_id = _scalar(message_id_raw)
    reply_to = _scalar(_field(payload, "reply_to_message_id", "reply_to"))
    chain_topic_id, chain_title, broken = _resolve_chain(
        conn, chat_index, reply_to, topic_markers, chain_cache
    )
    if chain_topic_id is not None:
        topic_id = chain_topic_id
        topic_title = chain_title
        attribution = "chain"
    elif scope == "topic":
        topic_id = scope_topic_id
        topic_title = scope_topic_title
        attribution = "scope"
    else:
        topic_id = None
        topic_title = None
        attribution = "none"

    raw_text = payload.get("text")
    text, entities = flatten_text(raw_text)
    normalized = (
        unicodedata.normalize("NFC", text.replace("\r\n", "\n").replace("\r", "\n"))
        if text is not None
        else None
    )
    date, date_raw = parse_date(payload.get("date"))
    edited, _ = parse_date(_field(payload, "edited", "edited_at"))
    author = _field(payload, "from", "author")
    author_id = _field(payload, "from_id", "author_id")
    forwarded_from = _field(payload, "forwarded_from", "forwarded_from_name")
    media_type, media_path = _media(payload)
    url = (
        _make_url(chat_id, topic_id, message_id)
        if attribution != "none" and topic_id is not None and message_id is not None
        else None
    )
    meta = {}
    if attribution == "none":
        meta["deep_link"] = (
            f"tg://openmessage?chat_id={chat_id}&message_id={message_id}"
            if message_id is not None
            else None
        )
    record = {
        "chat_id": chat_id,
        "chat_title": chat_title,
        "topic_id": topic_id,
        "topic_title": topic_title,
        "attribution": attribution,
        "message_id": message_id_raw if isinstance(message_id_raw, int) else message_id,
        "date": date,
        "date_raw": date_raw,
        "author": author if isinstance(author, str) else None,
        "author_id": _scalar(author_id),
        "text": normalized,
        "entities": entities,
        "reply_to": int(reply_to)
        if reply_to and reply_to.lstrip("-").isdigit()
        else reply_to,
        "forwarded_from": forwarded_from if isinstance(forwarded_from, str) else None,
        "media_type": media_type,
        "media_path": media_path,
        "edited_at": edited,
        "content_hash": normalized_content_hash(normalized),
        "url": url,
        "meta": meta,
    }
    return record, broken


def parse_job(job_dir: Path) -> dict[str, Any]:
    paths = job_paths(job_dir)
    try:
        manifest = read_json(paths["manifest"])
    except (OSError, ValueError, TypeError) as exc:
        raise ParseError(f"Cannot read job manifest: {exc}") from exc
    scope, scope_topic_id, scope_topic_title = _scope_info(manifest)
    result_path = paths["unpacked"] / manifest.get("result_json", "result.json")
    if not result_path.is_file():
        raise ParseError(f"result.json not found: {result_path}")

    layout = _top_level_layout(result_path)
    index_path = paths["normalized"] / ".telegram-index.sqlite"
    if index_path.exists():
        index_path.unlink()
    index = _new_index(index_path)
    try:
        _scan_to_index(result_path, index, layout)
        target_chat_id = canonical_chat_id(manifest.get("chat_id"))
        candidates: list[tuple[int, str, str | None, str | None]] = []
        for row in index.execute(
            "SELECT chat_index, raw_id, title, chat_type FROM chats ORDER BY chat_index"
        ):
            chat_index, raw_id, title, chat_type = row
            canonical = canonical_chat_id(raw_id)
            if canonical is not None and canonical == target_chat_id:
                candidates.append((chat_index, canonical, title, chat_type))
        if not candidates and layout == "single":
            only_chat = index.execute(
                "SELECT chat_index, raw_id, title, chat_type FROM chats"
            ).fetchone()
            if only_chat and only_chat[1] is None and target_chat_id:
                candidates.append(
                    (only_chat[0], target_chat_id, only_chat[2], only_chat[3])
                )
        if len(candidates) != 1:
            raise ParseError(
                f"Expected exactly one target chat {target_chat_id!r}, found {len(candidates)}"
            )
        chat_index, chat_id, chat_title, _chat_type = candidates[0]
        topic_markers = _topic_created(index, chat_index)
        paths["normalized"].mkdir(parents=True, exist_ok=True)
        temp_messages = paths["messages"].with_name("messages.jsonl.tmp")
        counts: Counter[str] = Counter()
        broken_chains = 0
        links_count = 0
        topics_used: dict[str, dict[str, Any]] = {
            marker_id: {
                "topic_id": marker_id,
                "topic_title": title,
                "attribution": "chain",
            }
            for marker_id, title in topic_markers.items()
        }
        chain_cache: dict[str, tuple[str | None, str | None, bool]] = {}
        parse_started = datetime.now(UTC)
        with temp_messages.open("w", encoding="utf-8", newline="\n") as output:
            rows = index.execute(
                """SELECT payload, message_id, reply_to, is_service
                   FROM messages WHERE chat_index = ? ORDER BY ordinal""",
                (chat_index,),
            )
            for payload_text, message_id, reply_to, is_service in rows:
                if is_service:
                    continue
                payload = json.loads(payload_text)
                record, broken = _record_message(
                    payload,
                    chat_id,
                    chat_title if isinstance(chat_title, str) else None,
                    scope,
                    scope_topic_id,
                    scope_topic_title,
                    topic_markers,
                    index,
                    chat_index,
                    chain_cache,
                )
                counts[record["attribution"]] += 1
                broken_chains += int(broken)
                links_count += _message_link_count(record)
                if record["topic_id"] is not None:
                    tid = str(record["topic_id"])
                    topics_used.setdefault(
                        tid,
                        {
                            "topic_id": tid,
                            "topic_title": record["topic_title"],
                            "attribution": record["attribution"],
                        },
                    )
                output.write(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                )
            output.flush()
            os.fsync(output.fileno())
        temp_messages.replace(paths["messages"])

        if scope == "topic" and scope_topic_id is not None:
            topics_used.setdefault(
                scope_topic_id,
                {
                    "topic_id": scope_topic_id,
                    "topic_title": scope_topic_title,
                    "attribution": "scope",
                },
            )
        if counts["none"]:
            topics_used["__unattributed__"] = {
                "topic_id": None,
                "topic_title": NO_TOPIC_TITLE,
                "attribution": "none",
            }
        topic_list = sorted(
            topics_used.values(),
            key=lambda item: (item["topic_id"] is None, str(item["topic_id"] or "")),
        )
        write_json_atomic(paths["topics"], topic_list)
        total = sum(counts.values())
        manifest["parse"] = {
            "status": "done",
            "layout": layout,
            "chat_title": chat_title if isinstance(chat_title, str) else None,
            "started_at": parse_started.isoformat(),
            "messages_total": total,
            "service_messages_skipped": index.execute(
                "SELECT count(*) FROM messages WHERE chat_index = ? AND is_service = 1",
                (chat_index,),
            ).fetchone()[0],
            "topic_created_count": len(topic_markers),
            "broken_chain_count": broken_chains,
            "attribution_counts": {
                "chain": counts["chain"],
                "scope": counts["scope"],
                "none": counts["none"],
            },
            "links_in_text_count": links_count,
            "topics": topic_list,
            "finished_at": datetime.now(UTC).isoformat(),
        }
        manifest["load"] = manifest.get(
            "load",
            {
                "status": "not_run",
                "inserted": None,
                "updated": None,
                "skipped": None,
                "absent": None,
            },
        )
        write_json_atomic(paths["manifest"], manifest)
        return manifest["parse"]
    except sqlite3.Error as exc:
        raise ParseError(f"Temporary parser index failed: {exc}") from exc
    finally:
        index.close()
        index_path.unlink(missing_ok=True)
        Path(str(index_path) + "-journal").unlink(missing_ok=True)
        paths["messages"].with_name("messages.jsonl.tmp").unlink(missing_ok=True)


def iter_messages(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ParseError(
                    f"Invalid messages.jsonl line {line_number}: {exc}"
                ) from exc
            if not isinstance(item, dict):
                raise ParseError(f"messages.jsonl line {line_number} is not an object")
            yield item
