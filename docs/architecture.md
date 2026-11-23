# Architecture

```text
source archive                    GitHub / GitLab / GitVerse (read-only API)
     │                                        │
     ▼                                        ▼
validated parser ──► normalized     connector discover/fetch/normalize
     │               messages               │
     ▼                                      ▼
PostgreSQL loader ──► optional embeddings ──► hybrid/FTS search
                                         │
                                read-only MCP tools
                                         │
                             durable worker and broker
```

The parser validates archive members and writes normalized output without downloading media. The loader uses parameterized SQL and idempotent keys. Embeddings are stored separately from message text. Search combines PostgreSQL full-text and vector results; when vector access is unavailable it reports a degraded full-text result.

External discussion connectors run only inside the worker: `discover` lists threads without full text, `fetch` writes raw upstream answers into the job directory, `normalize` is offline and deterministic, and `load` reuses the same PostgreSQL model (`documents` + `segments`) in one transaction with an advisory lock per source.

The job queue persists requests, checkpoints, attempts, and safe diagnostics in SQLite. Network calls run outside SQLite write transactions. The broker accepts fixed operations for sources in a local allowlist; callers do not supply arbitrary SQL or shell commands.
