"""Validate and apply the repository's self-registering SQL migrations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SOURCE_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
_INSTALLED_MIGRATIONS_DIR = (
    Path(sysconfig.get_path("data")) / "share" / "kiber-analyst" / "migrations"
)
DEFAULT_MIGRATIONS_DIR = Path(
    os.environ.get(
        "KIBER_ANALYST_MIGRATIONS_DIR",
        str(
            _SOURCE_MIGRATIONS_DIR if _SOURCE_MIGRATIONS_DIR.is_dir() else _INSTALLED_MIGRATIONS_DIR
        ),
    )
)
MANIFEST_NAME = "checksums.json"
BASELINE_VERSION = "0001_init"
LOCK_KEY = (1262573121, 1)
_VERSION_RE = re.compile(r"^(\d{4}_[a-z0-9][a-z0-9_]*|0001_init)$")
_REGISTER_RE = re.compile(
    r"INSERT\s+INTO\s+schema_migrations\s*\(\s*version\s*\)\s*"
    r"VALUES\s*\(\s*'([^']+)'\s*\)",
    re.IGNORECASE,
)


class MigrationError(Exception):
    """An expected, safe-to-report migration runner error."""

    def __init__(self, code: str, *, sqlstate: str | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.sqlstate = sqlstate


@dataclass(frozen=True)
class Migration:
    version: str
    filename: str
    sha256: str
    sql: str


@dataclass(frozen=True)
class Catalog:
    versions: tuple[str, ...]
    migrations: tuple[Migration, ...]


def load_catalog(migrations_dir: Path = DEFAULT_MIGRATIONS_DIR) -> Catalog:
    """Load the pinned catalog and reject missing, extra, or changed SQL."""
    manifest_path = migrations_dir / MANIFEST_NAME
    try:
        manifest: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MigrationError("CATALOG_UNREADABLE") from exc

    if not isinstance(manifest, dict) or manifest.get("format_version") != 1:
        raise MigrationError("CATALOG_INVALID")
    baseline = manifest.get("baseline")
    entries = manifest.get("migrations")
    if (
        not isinstance(baseline, dict)
        or baseline != {"version": BASELINE_VERSION, "file": None, "sha256": None}
        or not isinstance(entries, list)
    ):
        raise MigrationError("CATALOG_INVALID")

    versions = [BASELINE_VERSION]
    migrations: list[Migration] = []
    expected_files: set[str] = {MANIFEST_NAME}
    for entry in entries:
        if not isinstance(entry, dict):
            raise MigrationError("CATALOG_INVALID")
        version, filename, expected_hash = (
            entry.get("version"),
            entry.get("file"),
            entry.get("sha256"),
        )
        if (
            not isinstance(version, str)
            or not _VERSION_RE.fullmatch(version)
            or version == BASELINE_VERSION
            or not isinstance(filename, str)
            or Path(filename).name != filename
            or filename != f"{version}.sql"
            or not isinstance(expected_hash, str)
            or not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
            or version in versions
        ):
            raise MigrationError("CATALOG_INVALID")
        path = migrations_dir / filename
        try:
            sql_bytes = path.read_bytes()
        except OSError as exc:
            raise MigrationError("CATALOG_UNREADABLE") from exc
        if hashlib.sha256(sql_bytes).hexdigest() != expected_hash:
            raise MigrationError("CHECKSUM_MISMATCH")
        try:
            sql = sql_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MigrationError("CATALOG_UNREADABLE") from exc
        registrations = _REGISTER_RE.findall(sql)
        if registrations != [version]:
            raise MigrationError("SELF_REGISTRATION_INVALID")

        versions.append(version)
        expected_files.add(filename)
        migrations.append(Migration(version, filename, expected_hash, sql))

    numbers = [int(version[:4]) for version in versions[1:]]
    if numbers != list(range(2, 2 + len(numbers))):
        raise MigrationError("CATALOG_ORDER_INVALID")
    try:
        actual_files = {path.name for path in migrations_dir.glob("*.sql")}
    except OSError as exc:
        raise MigrationError("CATALOG_UNREADABLE") from exc
    if actual_files != expected_files - {MANIFEST_NAME}:
        raise MigrationError("CATALOG_FILE_SET_MISMATCH")
    return Catalog(tuple(versions), tuple(migrations))


def _ledger_prefix(rows: list[tuple[str]]) -> tuple[str, ...]:
    applied = [row[0] for row in rows]
    if not applied or applied[0] != BASELINE_VERSION:
        raise MigrationError("BASELINE_MISSING")
    if len(applied) != len(set(applied)):
        raise MigrationError("LEDGER_DUPLICATE")
    return tuple(applied)


def _planned_versions(
    catalog: Catalog, applied: tuple[str, ...], target: str | None
) -> tuple[str, ...]:
    if any(version not in catalog.versions for version in applied):
        raise MigrationError("LEDGER_UNKNOWN_VERSION")
    if catalog.versions[: len(applied)] != applied:
        raise MigrationError("LEDGER_GAP_OR_ORDER")
    if target is None:
        target_index = len(catalog.versions) - 1
    else:
        if target not in catalog.versions:
            raise MigrationError("TARGET_UNKNOWN")
        target_index = catalog.versions.index(target)
        if target_index < len(applied) - 1:
            raise MigrationError("TARGET_BEHIND_LEDGER")
    current_index = len(applied) - 1
    return catalog.versions[current_index + 1 : target_index + 1]


def _connect(dsn: str):
    try:
        import psycopg
    except ImportError as exc:
        raise MigrationError("PSYCOPG_UNAVAILABLE") from exc
    try:
        return psycopg.connect(dsn, autocommit=True, connect_timeout=5)
    except Exception as exc:
        raise MigrationError(
            "DATABASE_UNAVAILABLE", sqlstate=getattr(exc, "sqlstate", None)
        ) from exc


def run_migrations(
    dsn: str,
    *,
    dry_run: bool = False,
    target: str | None = None,
    migrations_dir: Path = DEFAULT_MIGRATIONS_DIR,
) -> dict[str, Any]:
    """Validate and optionally apply migrations under one session advisory lock."""
    catalog = load_catalog(migrations_dir)
    conn = _connect(dsn)
    locked = False
    try:
        try:
            locked = bool(
                conn.execute("SELECT pg_try_advisory_lock(%s, %s)", LOCK_KEY).fetchone()[0]
            )
        except Exception as exc:
            raise MigrationError(
                "ADVISORY_LOCK_FAILED", sqlstate=getattr(exc, "sqlstate", None)
            ) from exc
        if not locked:
            raise MigrationError("MIGRATION_LOCK_BUSY")

        try:
            database_name = conn.execute("SELECT current_database()").fetchone()[0]
        except Exception as exc:
            raise MigrationError(
                "DATABASE_IDENTITY_UNAVAILABLE", sqlstate=getattr(exc, "sqlstate", None)
            ) from exc
        if database_name != "knowledge":
            raise MigrationError("DATABASE_NAME_UNSUPPORTED")

        try:
            rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
        except Exception as exc:
            raise MigrationError(
                "LEDGER_UNAVAILABLE", sqlstate=getattr(exc, "sqlstate", None)
            ) from exc
        applied = _ledger_prefix(rows)
        pending = _planned_versions(catalog, applied, target)
        result: dict[str, Any] = {
            "status": "dry_run" if dry_run else "ready",
            "baseline": BASELINE_VERSION,
            "applied": list(applied),
            "pending": list(pending),
            "target": target or catalog.versions[-1],
            "checksums_verified": {
                migration.version: migration.sha256 for migration in catalog.migrations
            },
        }
        if dry_run:
            return result

        applied_now: list[str] = []
        by_version = {migration.version: migration for migration in catalog.migrations}
        for version in pending:
            migration = by_version[version]
            try:
                with conn.transaction():
                    conn.execute(migration.sql)
                    count = conn.execute(
                        "SELECT count(*) FROM schema_migrations WHERE version = %s",
                        (version,),
                    ).fetchone()[0]
                    if count != 1:
                        raise MigrationError("SELF_REGISTRATION_FAILED")
            except MigrationError:
                raise
            except Exception as exc:
                raise MigrationError(
                    "MIGRATION_FAILED", sqlstate=getattr(exc, "sqlstate", None)
                ) from exc
            applied_now.append(version)

        result.update(
            status="applied" if applied_now else "current",
            newly_applied=applied_now,
            applied=[*applied, *applied_now],
            pending=[],
        )
        return result
    finally:
        if locked:
            try:
                conn.execute("SELECT pg_advisory_unlock(%s, %s)", LOCK_KEY)
            except Exception:
                pass
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kiber-analyst-migrate")
    parser.add_argument("--dry-run", action="store_true", help="validate and print the plan only")
    parser.add_argument(
        "--target", help="apply through this version, e.g. 0006_external_discussions"
    )
    args = parser.parse_args(argv)
    dsn = os.environ.get("KIBER_ANALYST_MIGRATION_DSN")
    if not dsn:
        print(json.dumps({"error": "MIGRATION_DSN_REQUIRED"}), file=sys.stderr)
        return 2
    try:
        result = run_migrations(dsn, dry_run=args.dry_run, target=args.target)
    except MigrationError as exc:
        error: dict[str, str] = {"error": exc.code}
        if exc.sqlstate:
            error["sqlstate"] = exc.sqlstate
        print(json.dumps(error), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
