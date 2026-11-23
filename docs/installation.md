# Installation

## Local development

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -e '.[test,db,embedding,mcp]'
```

The default configuration directory is `~/.config/kiber-analyst`; override it with `KIBER_ANALYST_CONFIG_DIR`. The data directory defaults to `~/.local/share/kiber-analyst`; override it with `KIBER_ANALYST_DATA_ROOT`. Runtime sockets use `XDG_RUNTIME_DIR` when set and otherwise a per-user runtime directory.

A mounted deployment can set `KIBER_ANALYST_DATA_UUID`; when present, the worker verifies the configured data root's mounted filesystem UUID before opening the queue.

## Database setup

Create an application role and a separate read-only MCP role. Review and apply migrations from `migrations/` using the database owner. Put database connection settings in a private env file readable only by the service account. The MCP role must not be a member of the application or owner role.

## Provider setup

Provider profiles contain environment-variable names, not credential values. Store API keys and proxy URLs in private configuration outside the repository. The default provider-profile files live in the configuration directory; environment variables can point the application at alternate files. Missing or invalid required provider configuration fails closed.

## Discussion connector setup (0.2.0)

Install the connector extra and provide **read-only** tokens through the
environment only:

```bash
pip install -e '.[db,connectors,embedding]'
export KIBER_GITHUB_TOKEN=...    # read-only scope
export KIBER_GITLAB_TOKEN=...    # read_api/read_only scope
export KIBER_GITVERSE_TOKEN=...  # read-only scope
```

Do not put tokens into `.env.example`, documentation, logs, or the repository.
A missing token fails with `AUTH_REQUIRED` before any network call. Tokens are
sent only in `Authorization` headers and are never logged, not even their
length.

Optional API tuning lives in `~/.config/kiber-analyst/connectors.yaml`; copy
`config/connectors.example.yaml` as a starting point. The file is validated at
startup (`api_url` must be https, `page_size` 1–100, `timeout_seconds` 1–300,
`max_retries` 0–10), and an unknown provider is rejected with
`UNSUPPORTED_PROVIDER`. Self-hosted GitLab hosts are part of the source id
(`gitlab:gitlab.example.com/group/project`) and of the configuration; GitHub
Enterprise hosts are part of the source id (`github:github.example.com/team/repo`).

Apply `migrations/0006_external_discussions.sql` as the database owner before
the first external ingest.

## Fetching an archive

Pass an explicit archive URI, chat identifier, and topic scope. The application does not embed a remote, source ID, topic, or checksum. Review all generated manifests before loading data.
