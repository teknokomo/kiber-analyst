# Security model

## Assets

- Source exports and message text
- Database credentials and stored corpus data
- Provider keys, proxy credentials, and MCP bearer token
- Job queue state and generated reports

## Trust boundaries

- The parser treats ZIP members and JSON fields as untrusted input.
- The database loader receives only validated normalized records and uses parameterized statements.
- The worker is the only component that needs provider credentials and queue write access.
- The MCP service exposes read-only tools and uses a dedicated SELECT-only database role.
- The broker accepts fixed operations, validates source IDs against an operator-owned registry, and confines file access to configured roots.

Do not grant untrusted callers direct access to the queue database, provider configuration, database files, or broker socket. Keep logs free of request bodies, credentials, proxy URLs, and raw provider errors.
