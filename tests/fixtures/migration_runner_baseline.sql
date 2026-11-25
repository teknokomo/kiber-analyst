-- Disposable PostgreSQL integration-test bootstrap for the external 0001 baseline.
-- The runner itself must never create or record 0001_init.
CREATE EXTENSION vector;
CREATE EXTENSION pg_trgm;

CREATE ROLE knowledge_owner NOLOGIN;
CREATE ROLE knowledge_app LOGIN;

CREATE TABLE schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_migrations(version) VALUES ('0001_init');

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
  kind text NOT NULL CHECK (kind IN ('video', 'telegram', 'document', 'web')),
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
  doc_type text NOT NULL CHECK (doc_type IN ('transcript', 'ocr', 'report', 'export')),
  language text,
  raw_path text,
  text_sha256 text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE segments (
  id bigserial PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  seq integer NOT NULL,
  start_ms integer,
  end_ms integer,
  speaker text,
  text text NOT NULL,
  tsv tsvector,
  UNIQUE (document_id, seq)
);
CREATE TABLE embeddings (
  id bigserial PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  owner_table text NOT NULL,
  owner_id text NOT NULL,
  model text NOT NULL,
  embedding vector(1536) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner_table, owner_id, model)
);
CREATE TABLE runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  kind text NOT NULL,
  status text NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed', 'partial')),
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error text
);

GRANT USAGE, CREATE ON SCHEMA public TO knowledge_owner;
GRANT SELECT, INSERT ON schema_migrations TO knowledge_owner;
GRANT SELECT ON schema_migrations TO knowledge_app;
GRANT REFERENCES ON projects TO knowledge_owner;
GRANT SELECT ON projects, sources, documents, segments, embeddings, runs TO knowledge_owner;
GRANT USAGE ON SCHEMA public TO knowledge_app;
GRANT SELECT, INSERT, UPDATE ON projects, sources, runs TO knowledge_app;
GRANT SELECT, INSERT, UPDATE ON embeddings TO knowledge_app;
GRANT USAGE, SELECT ON SEQUENCE segments_id_seq TO knowledge_app;
GRANT USAGE, SELECT ON SEQUENCE embeddings_id_seq TO knowledge_app;
