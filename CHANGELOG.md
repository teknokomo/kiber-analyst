# Changelog

## 0.3.0 — 2026-11-27

Project-scoped source control and the Telegram export pipeline as product
components.

- Managed source control: the broker exposes project and source discovery,
  project creation, source registration, dry-run, ingest, embeddings, status,
  retry, cancel and search, scoped per project. Existing projects require a
  root-owned policy entry; unregistered projects are refused.
- Queue schema v4 with a documented upgrade and rollback runbook that updates
  worker and broker as a compatible pair and refuses a mixed runtime.
- The Telegram exporter and publisher ship with the package as
  `kiber-analyst-telegram-export` and `kiber-analyst-telegram-publish`:
  ordinary groups, whole forum groups and individual topics, service events,
  and media downloads with a per-file limit of 100 MiB by default and a
  selectable maximum of 4,000,000,000 bytes.
- Resumable media: checkpoints, per-file size and SHA-256 verification,
  storage quota and free-space checks, bounded streaming, and explicit
  refusals instead of silent truncation. Files above the selected limit stay
  reported with their reason and can be fetched later by raising the limit.
- Atomic publication with durable markers, crash recovery that preserves data
  of uncertain provenance, immutable staging revisions, and reindex support
  for both `media/` and `media-resume/` roots.
- Managed Telegram imports accept complete prepared topic exports, attach
  recovered media to existing messages idempotently, and reject cross-project
  topic collisions before writing.
- Search accepts a whole Telegram group scope in addition to a single topic
  and returns results from the same chat only.
- Large histories are read with bounded line and buffer limits; subprocess
  output and JSONL records are size-capped.

Export and storage are independent of ASR: a stored file is not transcribed or
analysed. Audio extraction from video and chunked transcription of large files
are not part of 0.3.0.

Still out of scope: standalone documents (TXT, Markdown, PDF, DOCX, OCR),
write operations against providers, webhooks, diff analysis, cross-provider
deduplication, and UI.

## 0.2.0 — 2026-11-23

Read-only discussion connectors for GitHub, GitLab, and GitVerse.

- `connectors/` package: shared HTTP layer (retry/backoff, `Link` and
  `X-Next-Page` pagination, ETag support, raw dumps) and provider connectors
  for GitHub (issues, pull requests, reviews, review comments, Discussions via
  GraphQL), GitLab (issues, notes, merge requests, discussions), and GitVerse
  (issues, comments, pull requests, reviews).
- `migrations/0006_external_discussions.sql`: `documents.external_ref` with a
  partial unique index, `segments.thread_ref`/`parent_external_ref`/`item_kind`
  with indexes, idempotent registration, and scoped grants.
- External ingest pipeline (discover → fetch → normalize → load) with stage
  checkpoints, deterministic JSONL bundles, revision tracking, and soft
  deletion markers.
- `external_ingest` and `external_embed` worker operations; CLI commands
  `github|gitlab|gitverse ingest`, `external status`, and `external embed`.
- Search and MCP accept `github`, `gitlab`, and `gitverse` source kinds and
  return thread/item provenance (`thread_ref`, `item_ref`, `item_kind`,
  `parent_ref`, `state`, `labels`); Telegram result fields are unchanged.
- Broker allowlist accepts external sources with project-only entries and
  supports `ingest`, `embed`, `source-status`, and `search` (limit 1–100).
- Documentation: `docs/connectors.md`, updated data model, search, installation,
  security, and an acceptance checklist in `docs/acceptance-0.2.0.md`.

Review hardening included in this release:

- Connector tokens are sent only to the configured HTTPS API host (or
  explicitly configured `allowed_hosts`), including on every pagination hop.
- GitHub Discussions use the real GraphQL schema (`closed`, `bodyText`,
  labels, category) and paginate comments and replies.
- The same repository can be ingested into two projects without moving rows;
  external item uniqueness is scoped per source, Telegram uniqueness is
  unchanged.
- Partial runs and pilot limits never advance the incremental watermark, and a
  job retry resumes from already fetched threads.
- Edits keep their provenance, reappearing items clear deletion markers, and
  skipped items refresh their sequence position.
- Embedding runs apply the resume cursor before the limit, skip vectors for
  segments whose text changed during the provider call, and propagate worker
  quota/cancellation signals.
- Vectors record their source text hash: edited text is re-embedded, while
  metadata-only updates keep the vector.
- A partial bundle with failed threads loads its good threads and returns
  `partial` with the previous checkpoint; an explicit ingest after a finished
  run queries the provider again, while an active request stays deduplicated.
- Re-importing a previously seen snapshot restores its text (A → B → A), and
  malformed or non-advancing pagination fails the chain instead of reporting a
  truncated result as complete.
- Repeated external commands return the active rerun instead of creating a
  second job; imports without `--since` continue from the saved checkpoint of
  the last completed import; stale vectors are excluded from semantic ranking
  until the edited text is re-embedded, with full-text search unaffected.
- `segments.tsv` becomes a stored generated column (`0007`), so full-text
  search works for every loader without manual filling; the migration checks
  for existing triggers or generated definitions before rewriting the table.

Still out of scope: write operations, webhooks, diff/file analysis, attachments,
OCR, ASR changes, cross-provider deduplication, and UI.

## 0.1.0 — 2026-11-21

First public release.

- Telegram Desktop JSON parsing and normalized message output.
- Service-event ingestion with provenance and media-reference reporting.
- PostgreSQL migrations, idempotent loading, and optional embeddings.
- Hybrid full-text/vector search with a full-text fallback.
- Read-only MCP tools, a persistent job queue, and a restricted allowlist broker.
- Synthetic examples, tests, package metadata, and GitHub Actions checks.

The supported 0.1.0 workflows do not cover speech transcription, image text extraction, topic-specific export automation, issue-tracker connectors, or production deployment.
