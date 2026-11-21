CREATE TABLE IF NOT EXISTS asr_runs (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id            uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  run_key               text NOT NULL,
  provider              text NOT NULL,
  model                 text NOT NULL,
  language              text NOT NULL,
  prompt_sha256         text,
  source_snapshot_sha256 text,
  status                text NOT NULL CHECK (status IN ('running','done','partial','failed')),
  started_at            timestamptz,
  finished_at           timestamptz,
  meta                  jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at            timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, run_key)
);

CREATE TABLE IF NOT EXISTS asr_results (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id                uuid NOT NULL REFERENCES asr_runs(id) ON DELETE CASCADE,
  external_ref          text NOT NULL,
  audio_sha256          text NOT NULL CHECK (audio_sha256 ~ '^[0-9a-f]{64}$'),
  duration_seconds      numeric(10,3) CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
  text                  text NOT NULL DEFAULT '',
  raw_json_path         text CHECK (
    raw_json_path IS NULL OR
    (raw_json_path !~ '(^/|(^|/)\.\.(/|$))')
  ),
  raw_json_sha256       text CHECK (raw_json_sha256 IS NULL OR raw_json_sha256 ~ '^[0-9a-f]{64}$'),
  status                text NOT NULL CHECK (status IN ('complete','failed','pending')),
  http_code             integer CHECK (http_code IS NULL OR http_code BETWEEN 100 AND 599),
  execution_run_id      text,
  error                 jsonb,
  meta                  jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at            timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, external_ref)
);

CREATE TABLE IF NOT EXISTS asr_segments (
  id                    bigserial PRIMARY KEY,
  run_id                uuid NOT NULL REFERENCES asr_runs(id) ON DELETE CASCADE,
  external_ref          text NOT NULL,
  segment_no            integer NOT NULL CHECK (segment_no >= 0),
  start_ms              bigint NOT NULL CHECK (start_ms >= 0),
  end_ms                bigint NOT NULL CHECK (end_ms >= start_ms),
  text                  text NOT NULL DEFAULT '',
  avg_logprob           real,
  no_speech_prob        real,
  compression_ratio     real,
  empty_text_with_tokens boolean NOT NULL DEFAULT false,
  tsv                   tsvector GENERATED ALWAYS AS (to_tsvector('russian', coalesce(text,''))) STORED,
  created_at            timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, external_ref, segment_no)
);

CREATE TABLE IF NOT EXISTS asr_attempts (
  id                    bigserial PRIMARY KEY,
  run_id                uuid NOT NULL REFERENCES asr_runs(id) ON DELETE CASCADE,
  external_ref          text NOT NULL,
  attempt_no            integer NOT NULL CHECK (attempt_no > 0),
  profile               text,
  status                text NOT NULL,
  http_code             integer CHECK (http_code IS NULL OR http_code BETWEEN 100 AND 599),
  error_class           text,
  ambiguous_delivery    boolean NOT NULL DEFAULT false,
  latency_seconds       numeric(10,3) CHECK (latency_seconds IS NULL OR latency_seconds >= 0),
  retry_after_seconds   numeric(10,3) CHECK (retry_after_seconds IS NULL OR retry_after_seconds >= 0),
  created_at            timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, external_ref, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_asr_results_external_ref
  ON asr_results (external_ref);
CREATE INDEX IF NOT EXISTS idx_asr_segments_external_ref
  ON asr_segments (external_ref);
CREATE INDEX IF NOT EXISTS idx_asr_segments_run
  ON asr_segments (run_id, external_ref, segment_no);
CREATE INDEX IF NOT EXISTS idx_asr_segments_tsv
  ON asr_segments USING gin (tsv);

GRANT SELECT, INSERT, UPDATE, DELETE ON asr_runs, asr_results, asr_segments, asr_attempts TO knowledge_app;
GRANT USAGE, SELECT ON SEQUENCE asr_segments_id_seq, asr_attempts_id_seq TO knowledge_app;

INSERT INTO schema_migrations(version) VALUES ('0003_asr')
ON CONFLICT DO NOTHING;
