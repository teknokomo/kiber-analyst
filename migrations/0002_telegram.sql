ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS meta jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS external_ref text;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS content_hash text;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS meta jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE segments
  ADD COLUMN IF NOT EXISTS posted_at timestamptz;

CREATE TABLE IF NOT EXISTS snapshots (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id  uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  sha256     text NOT NULL,
  size_bytes bigint,
  uri        text,
  scope      text NOT NULL DEFAULT 'unknown',
  created_at timestamptz NOT NULL DEFAULT now(),
  meta       jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (source_id, sha256)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_segments_external_ref
  ON segments (external_ref) WHERE external_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_segments_topic
  ON segments ((meta->>'topic_id'));

CREATE INDEX IF NOT EXISTS idx_segments_author
  ON segments ((meta->>'author_id'));

INSERT INTO schema_migrations(version) VALUES ('0002_telegram')
ON CONFLICT DO NOTHING;
