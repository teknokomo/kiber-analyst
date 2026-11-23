# Changelog

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
