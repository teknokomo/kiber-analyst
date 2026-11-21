# Architecture

```text
source archive
     │
     ▼
validated parser ──► normalized messages + service events + media report
     │
     ▼
PostgreSQL loader ──► optional embeddings ──► hybrid/FTS search
                                         │
                                read-only MCP tools
                                         │
                             durable worker and broker
```

The parser validates archive members and writes normalized output without downloading media. The loader uses parameterized SQL and idempotent keys. Embeddings are stored separately from message text. Search combines PostgreSQL full-text and vector results; when vector access is unavailable it reports a degraded full-text result.

The job queue persists requests, checkpoints, attempts, and safe diagnostics in SQLite. Network calls run outside SQLite write transactions. The broker accepts fixed operations for sources in a local allowlist; callers do not supply arbitrary SQL or shell commands.
