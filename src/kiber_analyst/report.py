from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kiber_analyst.errors import ParseError
from kiber_analyst.parse import iter_messages
from kiber_analyst.paths import job_paths, read_json, write_json_atomic


def _timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _directory_size(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file() and not item.is_symlink():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def build_report(job_dir: Path) -> dict[str, Any]:
    paths = job_paths(job_dir)
    try:
        manifest = read_json(paths["manifest"])
    except (OSError, ValueError, TypeError) as exc:
        raise ParseError(f"Cannot read job manifest: {exc}") from exc
    parsed = manifest.get("parse")
    if not isinstance(parsed, dict) or parsed.get("status") != "done":
        raise ParseError("Job has no completed parse step")
    if not paths["messages"].is_file():
        raise ParseError(f"Normalized messages file not found: {paths['messages']}")
    try:
        topics = json.loads(paths["topics"].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ParseError(f"Cannot read normalized topics file: {exc}") from exc
    if not isinstance(topics, list):
        raise ParseError("topics.json must contain a JSON array")

    by_topic: Counter[str] = Counter()
    by_author: Counter[str] = Counter()
    dates: list[datetime] = []
    message_count = 0
    for record in iter_messages(paths["messages"]):
        message_count += 1
        topic_id = record.get("topic_id")
        by_topic[str(topic_id) if topic_id is not None else "none"] += 1
        author = record.get("author")
        if isinstance(author, str) and author:
            by_author[author] += 1
        moment = _timestamp(record.get("date"))
        if moment:
            dates.append(moment)

    attribution = parsed.get("attribution_counts", {})
    if (
        sum(int(attribution.get(key, 0)) for key in ("chain", "scope", "none"))
        != message_count
    ):
        raise ParseError("Attribution counts do not add up to normalized message count")
    load = manifest.get("load", {})
    if not isinstance(load, dict):
        load = {}
    steps = manifest.get("steps", {})
    if not isinstance(steps, dict):
        steps = {}
    parse_timing = parsed.get("started_at") and parsed.get("finished_at")
    if parse_timing:
        steps = dict(steps)
        steps["parse"] = {
            "started_at": parsed["started_at"],
            "finished_at": parsed["finished_at"],
        }
    if load.get("started_at") and load.get("finished_at"):
        steps = dict(steps)
        steps["load"] = {
            "started_at": load["started_at"],
            "finished_at": load["finished_at"],
        }
    steps = dict(steps)
    for timing in steps.values():
        if not isinstance(timing, dict):
            continue
        start = _timestamp(timing.get("started_at"))
        finish = _timestamp(timing.get("finished_at"))
        timing["duration_seconds"] = (
            max(0.0, (finish - start).total_seconds()) if start and finish else None
        )

    total = message_count
    chain = int(attribution.get("chain", 0))
    topic_summary = []
    for item in topics:
        if not isinstance(item, dict):
            continue
        topic_id = item.get("topic_id")
        topic_key = str(topic_id) if topic_id is not None else "none"
        topic_summary.append(
            {
                "topic_id": topic_id,
                "topic_title": item.get("topic_title"),
                "attribution": item.get("attribution"),
                "messages": by_topic.get(topic_key, 0),
            }
        )
    report = {
        "job_dir": str(job_dir.resolve()),
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_sha256": manifest.get("snapshot_sha256"),
        "messages_total": total,
        "topics_count": sum(
            1
            for item in topics
            if isinstance(item, dict) and item.get("topic_id") is not None
        ),
        "topics": topic_summary,
        "messages_by_topic": dict(sorted(by_topic.items())),
        "authors": {
            "count": len(by_author),
            "messages_by_author": dict(by_author.most_common()),
        },
        "attribution": {
            "chain": int(attribution.get("chain", 0)),
            "scope": int(attribution.get("scope", 0)),
            "none": int(attribution.get("none", 0)),
            "chain_share": chain / total if total else 0.0,
        },
        "attribution_interpretation": {
            "chain_share": (
                "Доля сообщений, атрибутированных по явной reply-цепочке до "
                "topic_created; это не оценка полноты или качества экспорта."
            ),
            "topic_scope": (
                "Для экспорта одной темы scope авторитетен: если цепочка не "
                "доходит до topic_created, сообщение относится к scope."
            ),
            "desktop_export": (
                "Непригодность Desktop-экспорта оценивают по экспорту всего "
                "чата; низкая доля chain сама по себе этого не доказывает."
            ),
        },
        "topic_created_count": int(parsed.get("topic_created_count", 0)),
        "broken_chain_count": int(parsed.get("broken_chain_count", 0)),
        "date_range": {
            "first": min(dates).isoformat() if dates else None,
            "last": max(dates).isoformat() if dates else None,
        },
        "links_in_text_count": int(parsed.get("links_in_text_count", 0)),
        "steps": steps,
        "directory_size_bytes": _directory_size(job_dir),
        "load": {
            "status": load.get("status", "not_run"),
            "dry_run": load.get("dry_run", False),
            "transaction": load.get("transaction"),
            "rollback_confirmed": load.get("rollback_confirmed", False),
            "inserted": load.get("inserted"),
            "updated": load.get("updated"),
            "skipped": load.get("skipped"),
            "absent": load.get("absent"),
            "project_slug": load.get("project_slug"),
            "project_id": load.get("project_id"),
        },
    }
    for _ in range(3):
        write_json_atomic(paths["report"], report)
        measured_size = _directory_size(job_dir)
        if report["directory_size_bytes"] == measured_size:
            break
        report["directory_size_bytes"] = measured_size
    write_json_atomic(paths["report"], report)
    return report


def print_report(report: dict[str, Any]) -> None:
    counts = report["attribution"]
    print(f"Job: {report['job_dir']}")
    print(f"Messages: {report['messages_total']}")
    print(
        "Attribution: "
        f"chain={counts['chain']}, scope={counts['scope']}, none={counts['none']} "
        f"(chain share {counts['chain_share']:.1%})"
    )
    load = report["load"]
    if load["status"] != "not_run":
        print(
            "Load: "
            f"status={load['status']}, inserted={load['inserted']}, "
            f"updated={load['updated']}, skipped={load['skipped']}, "
            f"absent={load['absent']}"
        )
    print(f"Report: {report['job_dir']}/report.json")
