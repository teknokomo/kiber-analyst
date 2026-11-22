# Kiber-analyst

Kiber-analyst is a Python toolkit for importing structured Telegram Desktop exports into a PostgreSQL knowledge base. It supports service-event provenance, media metadata, optional embeddings, hybrid search, speech transcription, a durable job queue, and read-only MCP tools.

## Requirements

- Python 3.12 or newer
- PostgreSQL for database-backed operations
- Optional provider credentials and network access for embeddings and speech transcription

## Install

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[db,embedding,asr,mcp,test]'
kiber-analyst --version
kiber-analyst --help
```

## Import a Telegram export

Supply the archive URI, chat identifier, and an output directory for each run. No source, project, or archive is built into the CLI.

```bash
kiber-analyst telegram fetch \
  --archive <archive-uri> \
  --chat-id <chat-id> \
  --scope chat \
  --work-root <job-directory>
kiber-analyst telegram parse --job <job-directory>/<job-id>
kiber-analyst telegram report --job <job-directory>/<job-id>
```

For a topic-scoped export, use `--scope topic --topic-id <topic-id>`. Review the manifest, hashes, media references, and report before loading data. Apply the matching SQL migrations with an appropriately privileged database role, then run:

```bash
kiber-analyst telegram load --job <job-directory>/<job-id> --dry-run
kiber-analyst telegram load --job <job-directory>/<job-id>
```

`--dry-run` executes the import transaction and rolls it back. PostgreSQL integration tests must use an isolated disposable database.

## Embeddings, search, and ASR

Configure provider profiles, credentials, proxies, and database access in the local configuration directory (`$XDG_CONFIG_HOME/kiber-analyst`, defaulting to `~/.config/kiber-analyst`). Keep credentials out of the repository.

```bash
kiber-analyst embed --job <job-directory>/<job-id>
kiber-analyst search --query "<query>" --project <project-slug>
kiber-analyst telegram asr --job <job-directory>/<job-id>
kiber-analyst asr embed --all-runs --project <project-slug>
```

ASR requires an explicit job and configured provider profiles. Missing, unsupported, or oversized media is reported rather than downloaded without a bound. ASR output is stored separately from the original message text.

## MCP and worker

The optional MCP package exposes read-only knowledge tools. The worker stores queue state under `$XDG_DATA_HOME/kiber-analyst` (default `~/.local/share/kiber-analyst`) and uses `$XDG_RUNTIME_DIR/kiber-analyst` for its control socket when available. These locations can be overridden with `KIBER_ANALYST_DATA_ROOT` and `KIBER_ANALYST_SOCKET`.

See [MCP_READONLY.md](docs/MCP_READONLY.md), [security guidance](docs/security.md), and [installation notes](docs/installation.md) for configuration details.

## Tests and checks

```bash
pytest -q
ruff check .
python -m compileall -q src
python scripts/public_release_scan.py
```

The repository includes synthetic examples that can be copied and parsed without a provider account or database:

```bash
workdir="$(mktemp -d)"
cp -R examples/synthetic/archive-job "$workdir/job"
kiber-analyst telegram parse --job "$workdir/job"
```

## Security

Never commit API keys, proxy credentials, bearer tokens, database passwords, personal data, or production exports. Bind MCP to loopback unless a separately reviewed authenticated gateway is used. Keep provider and database credentials in files outside the repository with restrictive permissions. See [SECURITY.md](SECURITY.md).

## License

Released under the [Omsk Open License](LICENSE); the Russian text is in [LICENSE-RU.md](LICENSE-RU.md).
