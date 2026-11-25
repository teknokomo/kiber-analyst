"""Fixed knowledge-project catalog operations for the trusted worker."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

from kiber_analyst.jobqueue import JobsError

SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
OWNER_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,39}\Z")
TITLE_LIMIT = 120


def read_project_policy(path: Path, *, root_uid: int = 0) -> tuple[set[str], bool]:
    """Read the operator-owned allowlist for existing projects; fail closed."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return set(), False
    except OSError:
        raise JobsError("Project access policy is unavailable") from None
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != root_uid
            or info.st_nlink != 1
            or info.st_mode & 0o022
            or info.st_size > 64 * 1024
        ):
            raise JobsError("Project access policy is unsafe")
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            fd = -1
            document = json.load(stream, object_pairs_hook=_unique_object)
    except (OSError, UnicodeError, ValueError):
        raise JobsError("Project access policy is invalid") from None
    finally:
        if fd >= 0:
            os.close(fd)
    if (
        not isinstance(document, dict)
        or set(document) != {"version", "authorized_projects", "allow_project_create"}
        or document.get("version") != 1
        or type(document.get("allow_project_create")) is not bool
    ):
        raise JobsError("Project access policy is invalid")
    projects = document["authorized_projects"]
    if (
        not isinstance(projects, list)
        or any(not isinstance(slug, str) or SLUG_RE.fullmatch(slug) is None for slug in projects)
        or len(projects) != len(set(projects))
    ):
        raise JobsError("Project access policy is invalid")
    return set(projects), document["allow_project_create"]


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _connect():
    from kiber_analyst.load import ENV_PATH, _required_env
    from kiber_analyst.load import _connect as connect

    return connect(_required_env(ENV_PATH))


def list_projects() -> list[dict[str, str]]:
    """Return only project slugs and titles; the broker applies its grants."""
    try:
        with _connect() as conn, conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
            conn.execute("SET LOCAL statement_timeout='10s'")
            rows = conn.execute("SELECT slug, title FROM projects ORDER BY slug").fetchall()
    except Exception as exc:  # noqa: BLE001 - never expose DSNs or driver details.
        if isinstance(exc, JobsError):
            raise
        raise JobsError("Project catalog is unavailable") from None
    return [{"slug": str(row[0]), "title": str(row[1])} for row in rows]


def create_project(slug: str, title: str, owner: str, request_key: str) -> dict[str, Any]:
    """Create a new project with a durable idempotency marker.

    Existing projects are never adopted. A repeated request can recover a
    project created by the same control owner and request key after a broker
    restart.
    """
    if (
        not isinstance(slug, str)
        or SLUG_RE.fullmatch(slug) is None
        or not isinstance(title, str)
        or not title.strip()
        or len(title.strip()) > TITLE_LIMIT
        or any(ord(char) < 32 or ord(char) == 127 for char in title)
        or not isinstance(owner, str)
        or OWNER_RE.fullmatch(owner) is None
        or not isinstance(request_key, str)
        or re.fullmatch(r"[a-f0-9]{64}", request_key) is None
    ):
        raise JobsError("Invalid project request")
    normalized_title = title.strip()
    try:
        with _connect() as conn, conn.transaction():
            conn.execute("SET LOCAL statement_timeout='10s'")
            row = conn.execute(
                """INSERT INTO projects (slug, title, meta)
                   VALUES (%s, %s, jsonb_build_object(
                     'kiber_control_owner', %s, 'kiber_control_request_key', %s))
                   ON CONFLICT (slug) DO NOTHING
                   RETURNING slug, title""",
                (slug, normalized_title, owner, request_key),
            ).fetchone()
            if row is not None:
                return {
                    "slug": str(row[0]), "title": str(row[1]),
                    "created": True, "request_key": request_key,
                }
            existing = conn.execute(
                """SELECT slug, title,
                          meta->>'kiber_control_owner',
                          meta->>'kiber_control_request_key'
                   FROM projects WHERE slug=%s""",
                (slug,),
            ).fetchone()
            if (
                existing is not None
                and existing[1] == normalized_title
                and existing[2] == owner
                and existing[3] == request_key
            ):
                return {
                    "slug": str(existing[0]), "title": str(existing[1]),
                    "created": False, "request_key": request_key,
                }
    except Exception as exc:  # noqa: BLE001 - never expose driver text or credentials.
        if isinstance(exc, JobsError):
            raise
        raise JobsError("Project creation failed") from None
    raise JobsError("PROJECT_ALREADY_EXISTS")


def request_key(owner: str, slug: str, title: str) -> str:
    canonical = json.dumps([owner, slug, title.strip()], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
