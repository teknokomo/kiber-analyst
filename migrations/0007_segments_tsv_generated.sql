-- 0007_segments_tsv_generated
-- Make segments.tsv a stored generated column so every loader (Telegram,
-- external connectors) gets full-text search for free and no loader has to
-- fill tsv manually. Uses the two-argument to_tsvector with an explicit
-- configuration: the one-argument form depends on default_text_search_config
-- and is not immutable, which generated columns require.
--
-- Pre-checks (implemented in the DO block below):
--   - non-internal triggers on segments abort the migration with an
--     exception (the version stays unrecorded until they are resolved);
--   - an already stored generated column only gets its GIN index and
--     statistics ensured;
--   - any other attgenerated value aborts with an exception;
--   - a plain column (attgenerated IS NULL or '') is converted.
--
-- Production note: this rewrite takes ACCESS EXCLUSIVE on segments and rewrites
-- the table and its indexes. Check the actual mechanism first:
--   SELECT attgenerated FROM pg_attribute
--   WHERE attrelid='segments'::regclass AND attname='tsv';
-- Apply with a backup and a maintenance window.

DO $$
DECLARE
    generated_kind "char";
    custom_triggers integer;
BEGIN
    SELECT attgenerated INTO generated_kind
      FROM pg_attribute
     WHERE attrelid = 'segments'::regclass
       AND attname = 'tsv'
       AND NOT attisdropped;

    SELECT count(*) INTO custom_triggers
      FROM pg_trigger
     WHERE tgrelid = 'segments'::regclass AND NOT tgisinternal;

    IF custom_triggers > 0 THEN
        -- A foreign trigger must not silently disable the migration: the
        -- operator has to resolve the conflict, and the version stays
        -- unrecorded so the migration can be re-applied afterwards.
        RAISE EXCEPTION 'segments has % custom trigger(s); resolve them before converting tsv', custom_triggers;
    END IF;

    IF generated_kind = 's' THEN
        -- Already a stored generated column: only ensure the index.
        CREATE INDEX IF NOT EXISTS idx_segments_tsv ON segments USING gin (tsv);
        ANALYZE segments;
        RETURN;
    END IF;

    IF generated_kind IS NOT NULL AND generated_kind <> '' THEN
        RAISE EXCEPTION 'segments.tsv generation kind % is not supported', generated_kind;
    END IF;

    -- attgenerated IS NULL (column missing) or '' (plain column): convert.
    DROP INDEX IF EXISTS idx_segments_tsv;
    ALTER TABLE segments DROP COLUMN IF EXISTS tsv;
    ALTER TABLE segments
      ADD COLUMN tsv tsvector
      GENERATED ALWAYS AS (
        to_tsvector('russian'::regconfig, coalesce(text, ''))
      ) STORED;
    CREATE INDEX idx_segments_tsv ON segments USING gin (tsv);
    ANALYZE segments;
END $$;

INSERT INTO schema_migrations(version) VALUES ('0007_segments_tsv_generated')
ON CONFLICT DO NOTHING;
