# Security policy

Report suspected vulnerabilities privately to the repository maintainers through GitHub Security Advisories. Do not publish credentials, personal data, production exports, or exploit details in a public issue.

## Security boundaries

- Treat database credentials, provider keys, proxy URLs, bearer tokens, and source exports as secrets.
- Keep credentials outside the repository and use restrictive file permissions.
- Bind the MCP HTTP endpoint to loopback unless a separately reviewed authenticated gateway is used. Configure a dedicated database role with read-only table grants.
- The restricted broker is designed for a local Unix socket and a fixed source registry. Do not expose the socket or permit arbitrary commands, paths, or SQL.
- Run PostgreSQL integration tests only against an isolated disposable database.

## Discussion connectors (0.2.0)

- The GitHub, GitLab, and GitVerse connectors are read-only by design. Use
  tokens with read-only scopes; never use tokens that can create, edit, or
  merge anything.
- Tokens come only from the environment: `KIBER_GITHUB_TOKEN`,
  `KIBER_GITLAB_TOKEN`, `KIBER_GITVERSE_TOKEN`. Keep them in a service
  environment or a `0600`/`0640` file owned by the service group, never in the
  repository, `.env.example`, documentation, or logs.
- A missing token fails with `AUTH_REQUIRED`; no request is sent without one.
  Tokens and Authorization headers are never written to logs, diagnostics,
  manifests, or job artifacts. Raw provider responses are stored only in the
  job's private artifact directory (`0700`, files `0600`) so a run can be
  reproduced; they are never copied into manifests, diagnostics, or logs.
- Connectors never scrape HTML and never bypass `401/403/429`; they record the
  stable error code and stop.
- Ingested discussion text is stored in PostgreSQL like Telegram data. Apply the
  same access controls and retention policy; `knowledge_reader` keeps
  SELECT-only grants.

The release scanner checks public paths and text for production-specific paths, identifiers, and common credential patterns. It is a guardrail, not a substitute for review.
