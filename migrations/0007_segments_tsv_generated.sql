-- 0007_segments_tsv_generated
-- Make segments.tsv a stored generated column so every loader (Telegram,
-- external connectors) gets full-text search for free and no loader has to
-- fill tsv manually. Uses the two-argument to_tsvector with an explicit
-- configuration: the one-argument form depends on default_text_search_config
-- and is not immutable, which generated columns require.
--
-- Pre-checks (implemented in the DO block below, per review):
--   - if custom triggers maintain tsv, the existing mechanism is left in place;
--   - if tsv is already a stored generated column, only the GIN index and
--     statistics are ensured;
--   - otherwise the plain column is converted to a stored generated column.
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
        RAISE NOTICE 'segments has custom triggers; tsv mechanism left untouched';
        RETURN;
    END IF;

    IF generated_kind IS NOT NULL AND generated_kind <> 's' THEN
        RAISE NOTICE 'segments.tsv generation kind % is not supported; left untouched', generated_kind;
        RETURN;
    END IF;

    IF generated_kind = 's' THEN
        RAISE NOTICE 'segments.tsv is already a stored generated column';
        CREATE INDEX IF NOT EXISTS idx_segments_tsv ON segments USING gin (tsv);
        ANALYZE segments;
        RETURN;
    END IF;

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
