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

## Fetching an archive

Pass an explicit archive URI, chat identifier, and topic scope. The application does not embed a remote, source ID, topic, or checksum. Review all generated manifests before loading data.
