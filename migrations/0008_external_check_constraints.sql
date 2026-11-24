-- 0008_external_check_constraints
-- Permit only the provider and document kinds used by the 0.2.0 discussion
-- connectors. Preserve every existing Telegram/video/document kind.

ALTER TABLE sources
  DROP CONSTRAINT IF EXISTS sources_kind_check;

ALTER TABLE sources
  ADD CONSTRAINT sources_kind_check
  CHECK (kind = ANY (ARRAY[
    'video'::text,
    'telegram'::text,
    'document'::text,
    'web'::text,
    'github'::text,
    'gitlab'::text,
    'gitverse'::text
  ]));

ALTER TABLE documents
  DROP CONSTRAINT IF EXISTS documents_doc_type_check;

ALTER TABLE documents
  ADD CONSTRAINT documents_doc_type_check
  CHECK (doc_type = ANY (ARRAY[
    'transcript'::text,
    'ocr'::text,
    'report'::text,
    'export'::text,
    'thread'::text
  ]));

INSERT INTO schema_migrations(version) VALUES ('0008_external_check_constraints')
ON CONFLICT DO NOTHING;
