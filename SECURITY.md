# Security policy

Report suspected vulnerabilities privately to the repository maintainers through GitHub Security Advisories. Do not publish credentials, personal data, production exports, or exploit details in a public issue.

## Security boundaries

- Treat database credentials, provider keys, proxy URLs, bearer tokens, and source exports as secrets.
- Keep credentials outside the repository and use restrictive file permissions.
- Bind the MCP HTTP endpoint to loopback unless a separately reviewed authenticated gateway is used. Configure a dedicated database role with read-only table grants.
- The restricted broker is designed for a local Unix socket and a fixed source registry. Do not expose the socket or permit arbitrary commands, paths, or SQL.
- Run PostgreSQL integration tests only against an isolated disposable database.

The release scanner checks public paths and text for production-specific paths, identifiers, and common credential patterns. It is a guardrail, not a substitute for review.
