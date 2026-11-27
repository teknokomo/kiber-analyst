"""Canonical identities for Telegram group and forum-topic exports."""

from __future__ import annotations

import re

_SOURCE = re.compile(
    r"telegram:(?P<chat>-?[1-9][0-9]{0,18}):(?:(?:topic:(?P<topic>[1-9][0-9]{0,9}))|group)\Z"
)


def parse_telegram_source(source_id: str) -> tuple[str, str | None] | None:
    """Return (chat_id, topic_id); group scope uses ``None``."""
    if not isinstance(source_id, str):
        return None
    match = _SOURCE.fullmatch(source_id)
    if match is None:
        return None
    return match.group("chat"), match.group("topic")


def format_telegram_source(chat_id: str, topic_id: int | str | None) -> str:
    if topic_id is None or topic_id == "group":
        return f"telegram:{chat_id}:group"
    return f"telegram:{chat_id}:topic:{topic_id}"


def telegram_scope_directory(topic_id: int | str | None) -> str:
    return "group" if topic_id is None or topic_id == "group" else str(topic_id)
