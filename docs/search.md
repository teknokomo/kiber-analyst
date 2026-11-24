# Search

Search uses PostgreSQL full-text search and, when configured, a provider-generated query embedding for vector search. The two ranked lists are combined with reciprocal rank fusion. A request can use FTS alone; the result reports its mode and whether vector search was degraded.

The MCP search tool returns a bounded set of explicit provenance fields. It does not return full internal metadata, filesystem paths, or database credentials. MCP database connections use read-only transactions and a role with SELECT-only grants.

For repeatable tests, use the synthetic fixtures in `tests/fixtures/`. Real source text should not be sent to embedding providers unless permitted by the data owner and the provider's terms.

## Source kinds and identifiers (0.2.0)

`--source-kind` (CLI) and `source_kind` (MCP) accept `telegram`, `github`,
`gitlab`, `gitverse`, `document`, `web`, and `video`. `--source-id` accepts the
Telegram topic form `telegram:<chat_id>:topic:<topic_id>` and the provider source
forms `github:[host/]owner/repo`, `gitlab:host/namespace/project`, and
`gitverse:owner/repo`.

Results keep the existing Telegram fields (`source`, `source_id`, `chat_id`,
`chat_title`, `topic_id`, `topic_title`, `message_id`, `url`, `author`,
`posted_at`) and add provenance for external items:

```json
{
  "provider": "github",
  "source_id": "github:owner/repo",
  "thread_ref": "github:owner/repo:issue:12",
  "item_ref": "github:owner/repo:issue:12:comment:345",
  "parent_ref": null,
  "item_kind": "comment",
  "state": "open",
  "labels": ["bug"],
  "url": "https://github.com/owner/repo/issues/12#issuecomment-345"
}
```

For external sources `chat_title` is the source title or `owner/repo`,
`topic_title` is the thread title, and `message_id` is the thread number for
bodies or the upstream item id for comments and reviews. For Telegram rows the
new keys are `null`, so existing consumers keep working unchanged. `provider`
is always equal to `source_kind`, so Telegram rows report
`"provider": "telegram"` and only the thread/item provenance fields are null.
The `topic` filter keeps Telegram topic semantics; external threads are
filtered through `--source-id`/`--source-kind` and the query text.

`segments.tsv` is a stored generated column (`to_tsvector('russian', text)`),
so full-text search covers every loader without manual filling.

External items are ranked semantically only by a vector built from their
current text: after an edit and before the next `external embed`, the stale
vector is excluded from semantic ranking, while full-text search still finds
the new text. Telegram and ASR ranking are unchanged (their rows have no
`item_kind` and keep the previous behavior).

Broker `search <source_id> <query> [limit]` accepts limits from 1 to 100 for
both Telegram and external sources.
