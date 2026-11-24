# Data model

The schema keeps projects, sources, snapshots, documents, segments, service events, embeddings, and job runs separate. Message text and service events have distinct records and provenance. Media metadata is retained with source messages; absent media is reported rather than silently dropped.

The `0002` migration adds Telegram ingestion objects. The `0003` migration adds the optional derived-transcript layer. The `0004` migration adds a read-only database role and grants. The `0005` migration adds service-event storage. The `0006` migration adds the external discussion layer. The `0007` migration turns `segments.tsv` into a stored generated column (`to_tsvector('russian', text)`), so full-text search works for every loader without manual filling. The `0008` migration permits the external connector values `github`, `gitlab`, and `gitverse` in `sources.kind`, and `thread` in `documents.doc_type`. Apply migrations in order with an appropriate database owner, and back up before any schema change. `0007` rewrites `segments` and takes `ACCESS EXCLUSIVE`: check the existing mechanism first (`SELECT attgenerated FROM pg_attribute WHERE attrelid='segments'::regclass AND attname='tsv';` and custom triggers), then apply with a backup and a maintenance window.

Embeddings are associated with their owner table and source record. Repeated loads and embedding requests use stable keys so already stored rows are not duplicated.

## External discussions (0.2.0)

One row per provider repository lives in `sources` with `kind` in
`github`, `gitlab`, or `gitverse` and `external_id` equal to the path part of
the source id (`owner/repo`, or `host/namespace/project` for GitLab).

Each discussion (issue, pull request, merge request, or GitHub discussion) is a
`documents` row with `doc_type='thread'` and a stable `external_ref`:

| Provider | Thread ref |
|---|---|
| GitHub | `github:owner/repo:issue:12`, `:pull:34`, `:discussion:7` |
| GitHub Enterprise | `github:host/owner/repo:issue:12` |
| GitLab | `gitlab:host/namespace/project:issue:12`, `:merge_request:34` |
| GitVerse | `gitverse:owner/repo:issue:5`, `:pull:9` |

`documents.meta` carries `provider`, `kind`, `number`, `state`, `author`,
`labels`, `created_at`, `updated_at`, `url`, `events` (the last 200 timeline
events), and `topic_title` (the thread title, which search exposes as
`topic_title`).

Each item (body, comment, review, review comment, reply, or event) is a
`segments` row. `external_ref` stays globally unique and is built as
`<thread_ref>:body` or `<thread_ref>:<item_kind>:<external_id>`; the new
`thread_ref`, `parent_external_ref`, and `item_kind` columns make thread and
parent links queryable. `content_hash` is the SHA-256 of the normalized text;
`posted_at` is the publication time with a timezone.

Revision and deletion rules:

- An edited upstream item updates `text`, `content_hash`, and `meta.edited_at`;
  the previous hash is kept in `meta.previous_content_hash` and
  `meta.revision` is bumped.
- A deleted upstream item is never removed physically: `meta.deleted=true` and
  `meta.deleted_at` are set, and the text remains available for history.
- Deletion is only inferred when a thread was re-fetched completely (all pages,
  totals verified). Absence from a single page is not a deletion.
- Re-importing an unchanged snapshot creates no duplicates; `runs` rows record
  each pipeline stage with its checkpoint.
