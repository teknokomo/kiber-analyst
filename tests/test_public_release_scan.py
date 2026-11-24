from __future__ import annotations

import importlib.util
from pathlib import Path

SCAN_PATH = Path(__file__).parents[1] / "scripts" / "public_release_scan.py"
SPEC = importlib.util.spec_from_file_location("public_release_scan", SCAN_PATH)
assert SPEC is not None and SPEC.loader is not None
scanner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scanner)


def test_release_scanner_rejects_literal_credentials_and_public_ips(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    (tmp_path / "config.txt").write_text(
        "API_KEY=not-a-real-but-secret-shaped-value-123\nHOST="
        + ".".join(("8", "8", "8", "8"))
        + "\n",
        encoding="utf-8",
    )

    issues = scanner.scan()

    assert any("credential-like assignment" in issue for issue in issues)
    assert any("non-loopback IPv4 address" in issue for issue in issues)


def test_release_scanner_accepts_placeholders_and_loopback(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    (tmp_path / ".env.example").write_text(
        "API_KEY=replace_me\nHOST=127.0.0.1\n",
        encoding="utf-8",
    )

    assert scanner.scan() == []


def test_release_scanner_does_not_skip_a_checkout_under_a_skipped_name(
    tmp_path: Path, monkeypatch
) -> None:
    # A checkout may live under e.g. ~/.claude/worktrees/repo; exclusions must
    # apply to repository-relative components, not to absolute path parts.
    root = tmp_path / ".claude" / "worktrees" / "repo"
    root.mkdir(parents=True)
    monkeypatch.setattr(scanner, "ROOT", root)
    (root / "config.txt").write_text(
        "API_KEY=not-a-real-but-secret-shaped-value-123\n", encoding="utf-8"
    )
    issues = scanner.scan()
    assert any("credential-like assignment" in issue for issue in issues)


def test_release_scanner_still_skips_relative_skipped_directories(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(scanner, "ROOT", tmp_path)
    (tmp_path / "memory-bank").mkdir()
    (tmp_path / "memory-bank" / "notes.txt").write_text(
        "API_KEY=not-a-real-but-secret-shaped-value-123\n", encoding="utf-8"
    )
    assert scanner.scan() == []
