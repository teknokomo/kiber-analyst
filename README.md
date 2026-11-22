# Kiber-analyst

Kiber-analyst is a Python toolkit for importing structured Telegram Desktop JSON exports into a PostgreSQL knowledge base. It normalizes messages and service events, tracks media references and missing files, creates embeddings, and supports hybrid full-text/vector search. A read-only MCP server, durable job worker, and restricted broker are included for controlled integrations.

## Version 0.1.0 scope

This release covers Telegram JSON ingestion, message and service-event storage, media metadata and `media_missing` reports, embeddings, hybrid/FTS search, read-only MCP, durable jobs, and the allowlisted broker API. The release is limited to those documented workflows; other media processing and external connectors are out of scope.

## Requirements

- Python 3.12 or newer
- PostgreSQL with the schema migrations in `migrations/` for database-backed operations
- Optional provider credentials and network access only for embedding operations

## Install

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
kiber-analyst --version
kiber-analyst --help
```

Install only the extras needed for your workflow:

```bash
pip install -e '.[db]'          # PostgreSQL load and search
pip install -e '.[embedding]'   # embeddings and vector query support
pip install -e '.[mcp]'         # read-only MCP server
pip install -e '.[test]'        # tests and Ruff
```

## Parse a synthetic export

The example contains invented messages and no media or credentials. Copy it before running because parsing writes normalized output beside the input.

```bash
workdir="$(mktemp -d)"
cp -R examples/synthetic/archive-job "$workdir/job"
kiber-analyst telegram parse --job "$workdir/job"
cat "$workdir/job/normalized/manifest.json"
```

The prepared example records are in `examples/synthetic/telegram-topic/`. They demonstrate the message, service-event, and manifest shapes without requiring an account or external service.

## Main workflow

1. Fetch an archive from a location configured for your environment, or unpack it yourself. Fetch requires an explicit archive URI and chat identifier; it has no embedded archive or source defaults.
2. Parse `result.json` into normalized JSONL and a report.
3. Review hashes, source scope, media references, and missing-media counts.
4. Load into PostgreSQL only after applying the matching migrations with an appropriately privileged database role.
5. Optionally create embeddings and run hybrid search.
6. Use the read-only MCP process or restricted broker when an agent needs bounded access.

## Roadmap

Possible future work includes additional input adapters, operational tooling, and usability improvements. This is exploratory only; there are no promised dates or committed scope beyond this release.

Typical commands are available with `kiber-analyst --help`. The input path, project slug, model, and provider settings must be supplied or configured for the local environment; no production account or source is built in.

## Tests and checks

```bash
pip install -e '.[test,db,embedding,mcp]'
pytest -q
ruff check .
python -m compileall -q src
python scripts/public_release_scan.py
```

PostgreSQL integration tests are skipped unless an isolated disposable test database is explicitly configured. Never point those tests at production data.

## Architecture and security

- `src/kiber_analyst/` contains the CLI, parser, loader, search core, job worker, and restricted broker.
- `src/kiber_analyst_mcp/` exposes read-only tools and does not import write paths.
- `migrations/` defines the required database objects and reader permissions.
- `docs/` describes the data flow, schema, installation, search, and security boundaries.
- `examples/synthetic/` and `tests/` contain invented data only.

Provider keys and proxy credentials belong in local configuration files or a secret manager; never commit them. The MCP listener should bind to loopback and use a read-only database role. The broker accepts only its configured source allowlist and fixed operations; do not expose its socket to untrusted users. See [SECURITY.md](SECURITY.md) and [docs/security.md](docs/security.md).

## License

Released under the [Omsk Open License](LICENSE); the Russian text is in [LICENSE-RU.md](LICENSE-RU.md).
