# Data model

The schema keeps projects, sources, snapshots, documents, message segments, service events, embeddings, and job runs separate. Message text and service events have distinct records and provenance. Media metadata is retained with source messages; absent media is reported rather than silently dropped.

The `0002` migration adds Telegram ingestion objects. The `0003` migration adds the optional derived-transcript layer. The `0004` migration adds a read-only database role and grants. The `0005` migration adds service-event storage. Apply migrations in order with an appropriate database owner, and back up before any schema change.

Embeddings are associated with their owner table and source record. Repeated loads and embedding requests use stable keys so already stored rows are not duplicated.
