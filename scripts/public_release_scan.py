from __future__ import annotations

import ipaddress
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Build these patterns from fragments so the scanner does not contain the
# prohibited strings it checks for.
FORBIDDEN_FRAGMENTS = (
    "/" + "home/" + "rimaldiv"[::-1],
    "/opt/" + "asimo"[::-1],
    "/etc/" + "asimo"[::-1],
    "/mnt/data/" + "media",
    "great-" + "ring",
    "-100" + "151" + "219" + "4429",
    "69" + "84",
    "".join(("omi", "sa")),
    "".join(("multi", "ca")),
    "".join(("her", "mes")),
    "rimaldiv"[::-1],
)
FORBIDDEN_NAMES = (
    ".env", ".env.local", "id_rsa", "id_ed25519", "known_hosts",
    "config.json5", "telegram.session",
)
IPV4 = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
LONG_CHAT_ID = re.compile(r"-100[0-9]{8,}")
PRIVATE_KEY = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
SSH_PUBLIC_KEY = re.compile(r"ssh-(?:rsa|ed25519) [A-Za-z0-9+/]{60,}")
KNOWN_TOKENS = (
    re.compile(r"AIza[0-9A-Za-z_-]{30,}"),
    re.compile(r"gsk_[A-Za-z0-9]{24,}"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{30,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{20,}"),
)
ASSIGNMENT = re.compile(
    r"(?im)\b(?:api[_-]?key|token|secret|password|cookie|authorization)\b"
    r"\s*(?:=|:)\s*(?:\"([^\"\r\n]*)\"|'([^'\r\n]*)'|([^\s#]+))"
)
PLACEHOLDER = re.compile(
    r"(?i)^(?:replace|example|fake|test|dummy|placeholder|your[_ -])"
)
FORBIDDEN_FILES = (
    ".sqlite", ".sqlite3", ".db", ".dump", ".zip", ".tar", ".gz",
    ".backup", ".bak", ".wal", ".shm", ".pem", ".key", ".session",
)


SKIP_PARTS = {
    ".git",
    "__pycache__",
    ".ruff_cache",
    ".pytest_cache",
    "build",
    "dist",
    # Local, gitignored working directories (private notes, tool indexes)
    # are never part of a public release.
    "memory-bank",
    ".ontoindex",
    ".venv",
}


def scan() -> list[str]:
    issues: list[str] = []
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        # Exclusions apply to repository-relative components only: a checkout
        # under a directory named e.g. ".claude" must still be scanned fully.
        if any(
            part in SKIP_PARTS or part.endswith(".egg-info")
            for part in relative.parts
        ) or not path.is_file():
            continue
        relative = relative.as_posix()
        lower_name = path.name.lower()
        if path.is_symlink():
            issues.append(f"symlink: {relative}")
            continue
        if any(name in lower_name for name in FORBIDDEN_NAMES) and relative != ".env.example":
            issues.append(f"forbidden file name: {relative}")
        if any(lower_name.endswith(suffix) for suffix in FORBIDDEN_FILES):
            issues.append(f"forbidden artifact: {relative}")
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            issues.append(f"non-text or unreadable file: {relative}")
            continue
        if path == Path(__file__).resolve():
            continue
        low_content = content.lower()
        for fragment in FORBIDDEN_FRAGMENTS:
            if fragment.lower() in low_content:
                issues.append(f"forbidden identifier/path: {relative}")
                break
        if LONG_CHAT_ID.search(content):
            issues.append(f"long Telegram identifier: {relative}")
        for match in IPV4.finditer(content):
            try:
                address = ipaddress.ip_address(match.group())
            except ValueError:
                continue
            documentation_ranges = (
                ipaddress.ip_network("192.0.2.0/24"),
                ipaddress.ip_network("198.51.100.0/24"),
                ipaddress.ip_network("203.0.113.0/24"),
            )
            if address != ipaddress.ip_address("127.0.0.1") and not any(address in network for network in documentation_ranges):
                issues.append(f"non-loopback IPv4 address: {relative}")
                break
        if PRIVATE_KEY.search(content) or SSH_PUBLIC_KEY.search(content):
            issues.append(f"private or SSH key material: {relative}")
        if any(pattern.search(content) for pattern in KNOWN_TOKENS):
            issues.append(f"known credential token pattern: {relative}")
        for match in ASSIGNMENT.finditer(content):
            value = next((part for part in match.groups() if part is not None), "")
            if path.suffix == ".py" and not any(
                part is not None for part in match.groups()[:2]
            ):
                continue
            if not PLACEHOLDER.search(value):
                issues.append(f"credential-like assignment: {relative}")
                break
    return issues


def main() -> int:
    issues = scan()
    if issues:
        for issue in issues:
            print(f"FAIL {issue}")
        return 1
    print("PASS: no forbidden paths, production identifiers, key material, or credential patterns found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
