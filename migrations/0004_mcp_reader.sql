DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'knowledge_reader') THEN
    CREATE ROLE knowledge_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS NOINHERIT;
  ELSE
    ALTER ROLE knowledge_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS NOINHERIT;
  END IF;
END
$$;

REVOKE knowledge_app, knowledge_owner FROM knowledge_reader;
REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE knowledge FROM knowledge_reader;
REVOKE TEMPORARY ON DATABASE knowledge FROM PUBLIC;
GRANT CONNECT ON DATABASE knowledge TO knowledge_reader;
GRANT TEMPORARY ON DATABASE knowledge TO knowledge_owner, knowledge_app;

REVOKE ALL PRIVILEGES ON SCHEMA public FROM knowledge_reader;
GRANT USAGE ON SCHEMA public TO knowledge_reader;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM knowledge_reader;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM knowledge_reader;
GRANT SELECT ON
  projects, sources, documents, segments, snapshots, embeddings, runs,
  asr_runs, asr_results, asr_segments, asr_attempts
TO knowledge_reader;

ALTER ROLE knowledge_reader SET default_transaction_read_only = on;
ALTER ROLE knowledge_reader SET statement_timeout = '15s';
ALTER ROLE knowledge_reader SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE knowledge_reader SET search_path = pg_catalog, public;

INSERT INTO schema_migrations(version) VALUES ('0004_mcp_reader')
ON CONFLICT DO NOTHING;
