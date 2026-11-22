# Read-only MCP

The `kiber_analyst_mcp` package exposes search, document reading, project listing, and ingestion-status tools. It uses a read-only PostgreSQL role and does not import the package's database write paths.

## Tools

- `search`: hybrid full-text and vector search with project, source, date, author, and topic filters. The worker obtains query embeddings through its configured provider profiles; the MCP process does not read provider credentials.
- `get_document`: returns document metadata and keyset-paginated segments. Linked ASR results are returned separately from original message text.
- `list_projects`: lists projects with independent aggregate counts.
- `ingest_status`: reports import, ASR, embedding, and snapshot counts without exposing internal paths or complete metadata records.

When vector search is unavailable, search can return a full-text result with `degraded=true` and a safe reason. Query text is not written to logs or queue state; only a digest may be retained for operational auditing.

## Database permissions

Apply `migrations/0004_mcp_reader.sql` with a PostgreSQL administrator after creating a database backup. The reader role receives `SELECT` on the documented tables and no write, schema-creation, temporary-table, sequence, or role-membership privileges. Verify these grants independently from the session's read-only setting.

Each MCP database operation uses a read-only transaction. Configure connection details outside the repository in the local configuration directory. Never commit passwords or bearer tokens.

## Queue and provider configuration

The worker persists jobs and quota events in SQLite. The database uses WAL, full synchronous commits, foreign keys, and a busy timeout. Network requests happen outside SQLite write transactions. Interactive query embedding is bounded to one provider dispatch per request and shares the worker's quota ledger.

Provider profile configuration and credentials are read from the local configuration directory (`$XDG_CONFIG_HOME/kiber-analyst`, default `~/.config/kiber-analyst`). Each provider profile must explicitly identify its key and proxy settings; a missing or invalid proxy fails closed.

## Local operation

Install the `mcp` optional dependency, configure the reader database role and bearer token outside the repository, then run the MCP module in the intended user context:

```bash
pip install -e '.[db,mcp]'
python -m kiber_analyst_mcp
```

The listener defaults to loopback. Use an authenticated tunnel or another separately reviewed protected channel for remote clients. Do not expose the unauthenticated health endpoint as a data interface.

## Verification

Use a disposable test database to verify the migration, grants, MCP initialization, tool listing, search modes, and rejected write operations. Do not run positive write tests against production data.
