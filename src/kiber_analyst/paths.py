from __future__ import annotations

from pathlib import Path


def job_paths(job_dir: Path) -> dict[str, Path]:
    return {
        "root": job_dir,
        "raw": job_dir / "raw",
        "unpacked": job_dir / "unpacked",
        "normalized": job_dir / "normalized",
        "manifest": job_dir / "normalized" / "manifest.json",
        "messages": job_dir / "normalized" / "messages.jsonl",
        "topics": job_dir / "normalized" / "topics.json",
        "report": job_dir / "report.json",
    }


def read_json(path: Path) -> dict:
    import json

    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise TypeError(f"Expected a JSON object in {path}")
    return value


def write_json_atomic(path: Path, value: object) -> None:
    import json
    import os

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
