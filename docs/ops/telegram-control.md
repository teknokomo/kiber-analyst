# Restricted Telegram export channel

`kiber-analyst-telegram-control` is an OpenSSH forced-command endpoint for the
Telegram export service identity. It parses `SSH_ORIGINAL_COMMAND` as a fixed
argument list and calls the package exporter API directly. It never invokes a
shell, accepts paths/flags, reads the worker environment, or returns private
filesystem or Telegram message data.

## Commands

```text
export SOURCE_ID [MAX_FILE_SIZE]
resume SOURCE_ID [MAX_FILE_SIZE]
services SOURCE_ID
status SOURCE_ID
budget SOURCE_ID
transfer SOURCE_ID
transfer-status SOURCE_ID
```

`SOURCE_ID` must be `telegram:<chat_id>:group` or
`telegram:<chat_id>:topic:<topic_id>`. The exporter account must have Telegram
membership and history access for that scope. `MAX_FILE_SIZE` accepts `100MiB`,
`4000MB`, or `4000000000B`; the default is `100MiB` (`104857600` bytes), and
the ceiling is `4000000000` bytes (decimal MB). The `resume` command rejects a
lowered limit that conflicts with already downloaded files before changing the
checkpoint. It retains completed media and reports unresolved media by reason.

The wrapper returns filtered status fields and stable error codes. It omits
source paths, raw publisher output, stdout/stderr, Telegram text, credentials,
and environment values. Unknown commands, extra arguments, shell metacharacters,
traversal-like source values and out-of-range sizes fail closed.

## Server installation contract

Install the package and both configurations through the site's normal
deployment mechanism. Put the Telegram CLI session under the dedicated export
service account; do not copy it to the importer. The exporter config must use
the current schema from `../../config/telegram-export.example.json`; the publisher
config must match its storage and free-space budgets. Protect both configs and
the export tree with the modes and owners in
[`telegram-export.md`](../telegram-export.md).

Create a separate SSH key entry for this service identity and bind it to the
fixed executable. A deployment template is:

```text
restrict,command="<absolute-path-to-kiber-analyst-telegram-control>" ssh-ed25519 <public-key> <comment>
```

Do not grant a shell, PTY, forwarding, a worker socket, database access, or the
import tree to the export account. The destination publisher must remain a
fixed, integrity-checking helper configured by the operator. The current
package runs exporter and publisher under one unprivileged Unix identity; it
does not support using separate Unix UIDs for those two processes. A site that
requires that split needs a separately reviewed transfer service rather than
root execution or relaxed `0700`/`0600` permissions.

The export endpoint verifies Telegram-side access by using the logged-in CLI
identity. Project grants are enforced later by the broker for source
registration, ingest, ASR, embeddings, retry, search and status. Export does
not register or ingest a source. Do not expose control output as proof that a
source has been added to a project.

## Legacy configuration and topic 2049

The earlier topic-only configuration shape containing only
`allowed_topic_ids`, `min_free_bytes` and `storage_limit_bytes` is not the v0.3
configuration schema. The package rejects it as `EXPORT_CONFIG_UNSUPPORTED`
before a Telegram export or transfer attempt; it must be replaced with the
current exporter/publisher pair. The old per-topic list is not a substitute for
the current storage, mount and publisher integrity checks. Preserve existing
export files while migrating configuration and verify `status` and `budget`
before retrying a transfer.

The public code does not configure OpenSSH, systemd, service ownership, or the
site's distinct-UID transfer helper. Installing a new package does not update an
already-installed forced-command wrapper or its environment.
