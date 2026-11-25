# Acceptance checklist 0.2.0 (real providers)

This checklist is run by the repository owner after the 0.2.0 pull request is
ready. It uses real read-only GitHub, GitLab, and GitVerse access. ZCode does
not run it and never receives production tokens.

## Prerequisites

1. A disposable or dedicated PostgreSQL with the full schema: base objects plus
   migrations `0002`–`0006` applied as the database owner.
2. Read-only tokens in the service environment:
   `KIBER_GITHUB_TOKEN`, `KIBER_GITLAB_TOKEN`, `KIBER_GITVERSE_TOKEN`.
3. At least one public repository per provider with issues and pull requests
   (and, for GitHub, discussions if the token has GraphQL access).
4. The worker is running with the same environment and the `gemini_embed`
   provider configured for `external embed`.

Replace the placeholders below: `<github:owner/repo>`, `<gitlab:host/ns/project>`,
`<gitverse:owner/repo>`, `<project>`.

## 1. Ingest each provider

```bash
kiber-analyst github ingest --repo owner/repo --project <project> --since 2026-01-01
kiber-analyst gitlab ingest --host gitlab.com --project group/project \
  --project-slug <project> --since 2026-01-01
kiber-analyst gitverse ingest --repo owner/repo --project <project> --since 2026-01-01
```

Expected: exit code 0 and `status` = `done`, or `partial` with an explicit
`partial_reasons` entry (for example `partial_discovery` when the token lacks
GraphQL Discussions, or `incomplete_thread` when a GitVerse pull request has
inline review comments that the Public API cannot return). `failed` with
`AUTH_REQUIRED` means the token is missing; `AUTH_FORBIDDEN` means the token has
no access to the repository.

GitVerse is expected to be `partial` when `review_comments > 0`: the current
Public API has no `GET` for inline review comments, and this is not an
authentication failure. Discussion comments, review summaries, and timeline
events are still ingested.

## 2. Source status per provider

```bash
kiber-analyst external status --source github:owner/repo --project <project>
```

Expected counts: `threads`, `items`, `comments`, `reviews`, `review_comments`,
`replies`, `events`, `threads_open`, `threads_closed_or_merged`,
`nonempty_items`, `embeddings` (embeddings only after step 3). On GitVerse,
`review_comments` is `0` even when the upstream pull-request object reports a
non-zero counter, because inline review comments are not readable through the
current Public API.

## 3. Embeddings and search

```bash
kiber-analyst external embed --source github:owner/repo --project <project>
kiber-analyst search "query" --project <project> --source-kind github --source-id github:owner/repo
```

Expected search results contain `thread_ref`, `item_ref`, `item_kind`, `url`,
`author`, `posted_at`, `state`, `labels`. The MCP `search` tool with
`source_kind=github` (and `gitlab`/`gitverse`) returns the same provenance.

## 4. Idempotency

Repeat steps 1 and 3 unchanged. Expected: `external status` counts do not
change; a repeated ingest reports `inserted = 0` and `skipped = <items>`; a
repeated embed reports `inserted = 0` and `skipped_existing = <nonempty>`.

## 5. Not touched

Verify that:

- Telegram sources and `telegram_service_events` rows are unchanged
  (`SELECT count(*)` before/after on the production project).
- ASR tables (`asr_runs`, `asr_results`, `asr_segments`, `asr_attempts`) are
  unchanged.
- The public tag `v0.1.0` still points at `b03263d…`.
- No production configuration files and no secrets were modified or committed.

## 6. Report template

```text
PR: <url>            commit: <sha>
Tests: unit <n> passed / contract <n> passed / database <n> passed / ruff clean
GitHub  <source_id>: threads=<n> items=<n> comments=<n> reviews=<n> nonempty=<n> embeddings=<n>
GitLab  <source_id>: ...
GitVerse <source_id>: ...
Search sample: <query> -> <thread_ref> <url>
Repeat ingest: inserted=0 skipped=<n>; repeat embed: inserted=0 skipped_existing=<n>
NOT TOUCHED: Telegram data, ASR tables, v0.1.0, production configs, secrets
```
