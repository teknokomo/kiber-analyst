-- 0006_external_discussions
-- External discussion sources (GitHub, GitLab, GitVerse): thread references on
-- documents and thread/parent/kind references on segments. Telegram and ASR
-- tables are not modified. Requires 0004_mcp_reader (knowledge_reader role).

ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS external_ref text;

CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_source_external_ref
  ON documents (source_id, external_ref)
  WHERE external_ref IS NOT NULL;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS thread_ref text;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS parent_external_ref text;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS item_kind text;

-- External items must be unique per source (project), not globally: the same
-- repository may be ingested into two projects, and each project must keep its
-- own rows. Telegram rows (item_kind IS NULL) keep their global uniqueness.
DROP INDEX IF EXISTS uq_segments_external_ref;

CREATE UNIQUE INDEX IF NOT EXISTS uq_segments_external_ref_telegram
  ON segments (external_ref)
  WHERE external_ref IS NOT NULL AND item_kind IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_segments_document_external_ref
  ON segments (document_id, external_ref)
  WHERE external_ref IS NOT NULL AND item_kind IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_segments_thread_ref
  ON segments (thread_ref);

CREATE INDEX IF NOT EXISTS idx_segments_parent_external_ref
  ON segments (parent_external_ref);

CREATE INDEX IF NOT EXISTS idx_segments_item_kind
  ON segments (item_kind);

GRANT SELECT, INSERT, UPDATE ON documents, segments TO knowledge_app;
GRANT SELECT, INSERT, UPDATE ON snapshots TO knowledge_app;
GRANT SELECT ON documents, segments TO knowledge_reader;
GRANT SELECT ON snapshots TO knowledge_reader;

INSERT INTO schema_migrations(version) VALUES ('0006_external_discussions')
ON CONFLICT DO NOTHING;
