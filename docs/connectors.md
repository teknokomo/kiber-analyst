# Discussion connectors (0.2.0)

Kiber-analyst reads public and private discussions from GitHub, GitLab, and
GitVerse. The connectors are **read-only**: they never create issues, comments,
reviews, labels, or merges, and they never scrape HTML.

## Pipeline

Each ingest is one worker operation (`external_ingest`) with four stages:

1. **discover** — list discussions (no comment text is downloaded yet).
2. **fetch** — download thread bodies, comments, reviews, review comments, and
   timeline events. Raw upstream responses are written to the job directory.
3. **normalize** — offline, deterministic conversion to `threads.jsonl`,
   `items.jsonl`, and `manifest.json`.
4. **load** — one PostgreSQL transaction upserting sources, documents, and
   segments (dry-run executes and rolls back).

Embeddings are a separate operation (`external_embed`) over non-empty text.

Stage artifacts live under `<data root>/jobs/<job id>/external-ingest/`:

```text
raw/                 raw upstream responses per request
discovered.json      discovered thread refs and the effective `since`
fetched.jsonl        one raw thread per line
normalized/threads.jsonl
normalized/items.jsonl
normalized/manifest.json
```

The manifest records `source_id`, `provider`, `snapshot_sha256`, counts
(`thread_count`, `item_count`, `nonempty_count`, `skipped_empty_count`),
`fetched_at`, `checkpoint_before`/`checkpoint_after`, `api_version`, per-thread
`threads_complete` flags, and errors without secrets.

## Coverage

| Provider | Threads | Items |
|---|---|---|
| GitHub | issues, pull requests, discussions (GraphQL) | body, comment, review, review comment, reply, timeline events (last 200 in `documents.meta.events`) |
| GitLab | issues, merge requests | body, note, discussion note, diff note (position), system note as event |
| GitVerse | issues, pull requests | body, comment, review, review comment, timeline events |

GitHub REST `/issues` also returns pull requests; PR entries are excluded from
the issue list by the `pull_request` marker, so an issue and a PR are never
duplicated. The same rule applies to GitVerse.

## Incremental sync and checkpoints

- `--since` accepts an ISO date or datetime; a date without an offset is a UTC
  cutoff. Without it the first run covers the last 90 days.
- Subsequent runs use the stored watermark minus a five-minute overlap window,
  so changes on the boundary are not lost. The watermark is not advanced when a
  run is `partial` or when `--limit` was applied, so unprocessed threads are
  picked up by the next run. Successfully fetched threads survive a job retry:
  the fetch stage resumes from its artifact file.
- Pagination always runs to the end (`Link: rel="next"` for GitHub/GitVerse,
  `X-Next-Page` for GitLab). A run that could not complete pagination or an
  optional endpoint finishes as `partial`, never as `done`.
- Retryable failures are HTTP 429, 5xx, timeouts, and network errors. `429`
  honours `Retry-After`; otherwise exponential backoff with jitter is used, up
  to five attempts. `401/403`, source `404`, `422`, and malformed payloads are
  not retried.

## Conditional endpoint support

When a specific endpoint answers `404` or `501` (for example GitHub Discussions
without a GraphQL-capable token, or a GitVerse review-comment endpoint), the
connector records `unsupported` with the exact method and status, keeps the
rest of the thread, and never substitutes HTML scraping or reports a partial
result as complete. GitHub Issues/PRs continue to work when Discussions are
unavailable.

## Idempotency and history

- Sources are unique by `(project_id, kind, external_id)`, thread documents by
  `(source_id, external_ref)`, and external items by
  `(document_id, external_ref)`; repeated loads create no duplicates. Telegram
  segments keep their global `external_ref` uniqueness.
- The same repository may be ingested into two projects: each project gets its
  own documents and segments, and search/embeddings stay scoped to the project.
  Rows are never moved between projects.
- Edited items bump `meta.revision` and keep `meta.previous_content_hash` plus
  `meta.edited_at`.
- Deleted upstream items are soft-deleted (`meta.deleted`, `meta.deleted_at`)
  only when their thread was re-fetched completely; text is never dropped.
- Embeddings use the same model contract as 0.1.0
  (`gemini-embedding-2:1536`), batches of up to 64 non-empty texts, and
  `ON CONFLICT DO NOTHING`, so re-embedding is idempotent.

## Configuration and errors

See `config/connectors.example.yaml` and `docs/installation.md` for tokens and
per-provider settings. Stable error codes: `UNSUPPORTED_PROVIDER`,
`SOURCE_NOT_FOUND`, `SOURCE_NOT_ALLOWED`, `AUTH_REQUIRED`, `AUTH_FORBIDDEN`,
`RATE_LIMITED`, `UPSTREAM_UNAVAILABLE`, `MALFORMED_RESPONSE`,
`CHECKPOINT_INVALID`, `THREAD_NOT_FOUND`, `NORMALIZE_FAILED`, `LOAD_FAILED`,
`EMBED_FAILED`, and `SEARCH_INVALID_SOURCE`. Error text never contains tokens,
Authorization headers, response bodies, or internal paths.

## Broker allowlist

External sources register in `sources.json` with a project only (no control
hashes, because nothing is staged):

```json
{
  "github:owner/repo": {"project": "sample-project"},
  "gitlab:gitlab.com/group/project": {"project": "sample-project"},
  "gitverse:owner/repo": {"project": "sample-project"}
}
```

Broker commands then work per source: `ingest`, `embed`, `source-status`,
`search <source_id> <query> [limit]` (limit 1–100). Every command is
audit-logged; `media-status` and `reindex` remain Telegram-only.
