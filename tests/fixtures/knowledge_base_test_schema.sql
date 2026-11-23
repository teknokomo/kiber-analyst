CREATE EXTENSION vector;
CREATE EXTENSION pg_trgm;

CREATE ROLE knowledge_owner NOLOGIN;
CREATE ROLE knowledge_app LOGIN;

CREATE TABLE schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug text NOT NULL UNIQUE,
  title text NOT NULL,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sources (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  kind text NOT NULL,
  external_id text NOT NULL,
  url text,
  title text,
  author text,
  published_at timestamptz,
  retrieved_at timestamptz NOT NULL DEFAULT now(),
  content_sha256 text,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (project_id, kind, external_id)
);

CREATE TABLE documents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  doc_type text NOT NULL,
  language text,
  raw_path text,
  text_sha256 text,
  created_at timestamptz NOT NULL DEFAULT now(),
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  external_ref text
);
CREATE UNIQUE INDEX uq_documents_source_external_ref
  ON documents (source_id, external_ref) WHERE external_ref IS NOT NULL;

CREATE TABLE segments (
  id bigserial PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  seq integer NOT NULL,
  start_ms integer,
  end_ms integer,
  speaker text,
  text text NOT NULL,
  tsv tsvector,
  external_ref text,
  content_hash text,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  posted_at timestamptz,
  thread_ref text,
  parent_external_ref text,
  item_kind text,
  UNIQUE (document_id, seq)
);
CREATE UNIQUE INDEX uq_segments_external_ref_telegram
  ON segments (external_ref)
  WHERE external_ref IS NOT NULL AND item_kind IS NULL;
CREATE UNIQUE INDEX uq_segments_document_external_ref
  ON segments (document_id, external_ref)
  WHERE external_ref IS NOT NULL AND item_kind IS NOT NULL;
CREATE INDEX idx_segments_tsv ON segments USING gin (tsv);
CREATE INDEX idx_segments_thread_ref ON segments (thread_ref);
CREATE INDEX idx_segments_parent_external_ref ON segments (parent_external_ref);
CREATE INDEX idx_segments_item_kind ON segments (item_kind);

CREATE TABLE embeddings (
  id bigserial PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  owner_table text NOT NULL,
  owner_id text NOT NULL,
  model text NOT NULL,
  embedding vector(1536) NOT NULL,
  content_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner_table, owner_id, model)
);

CREATE TABLE runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  kind text NOT NULL,
  status text NOT NULL,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error text
);

INSERT INTO schema_migrations(version) VALUES ('0001_init'), ('0002_telegram'), ('0006_external_discussions');

GRANT USAGE, CREATE ON SCHEMA public TO knowledge_owner;
GRANT SELECT, INSERT ON schema_migrations TO knowledge_owner;
GRANT REFERENCES ON projects TO knowledge_owner;
GRANT SELECT ON projects, sources, documents, segments, embeddings, runs TO knowledge_owner;

GRANT USAGE ON SCHEMA public TO knowledge_app;
GRANT SELECT ON projects, sources, documents, segments, embeddings, schema_migrations TO knowledge_app;
GRANT SELECT, INSERT, UPDATE ON projects, sources, documents, segments TO knowledge_app;
GRANT SELECT, INSERT, UPDATE ON runs TO knowledge_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON embeddings TO knowledge_app;
GRANT USAGE, SELECT ON SEQUENCE segments_id_seq, embeddings_id_seq TO knowledge_app;
