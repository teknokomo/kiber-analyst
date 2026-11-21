# Search

Search uses PostgreSQL full-text search and, when configured, a provider-generated query embedding for vector search. The two ranked lists are combined with reciprocal rank fusion. A request can use FTS alone; the result reports its mode and whether vector search was degraded.

The MCP search tool returns a bounded set of explicit provenance fields. It does not return full internal metadata, filesystem paths, or database credentials. MCP database connections use read-only transactions and a role with SELECT-only grants.

For repeatable tests, use the synthetic fixtures in `tests/fixtures/`. Real source text should not be sent to embedding providers unless permitted by the data owner and the provider's terms.
