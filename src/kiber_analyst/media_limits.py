"""Size limits shared by the Telegram import and resume paths."""
from __future__ import annotations

import re

DEFAULT_TELEGRAM_MEDIA_BYTES = 100 * 1024**2
MAX_TELEGRAM_MEDIA_BYTES = 4_000_000_000  # 4000 decimal MB
MAX_TELEGRAM_EXPORT_BYTES = 1024**4  # 1 TiB; also bounded by each deployment's storage quota.
MIB = 1024**2


def validate_telegram_media_limit(value: int) -> int:
    if type(value) is not int or not 0 < value <= MAX_TELEGRAM_MEDIA_BYTES:
        raise ValueError("Telegram media limit is outside the supported range")
    return value


def parse_telegram_media_limit(value: str | None) -> int:
    """Parse a file limit; bare integers retain the legacy MiB interpretation."""
    if value is None:
        return DEFAULT_TELEGRAM_MEDIA_BYTES
    match = re.fullmatch(r"([1-9][0-9]*)(MB|MIB)?", value, re.IGNORECASE)
    if match is None:
        raise ValueError("Use a positive integer with optional MB or MiB suffix")
    amount = int(match.group(1))
    unit = (match.group(2) or "MiB").upper()
    multiplier = 1_000_000 if unit == "MB" else MIB
    return validate_telegram_media_limit(amount * multiplier)
