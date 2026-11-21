CREATE TABLE IF NOT EXISTS telegram_service_events (
  id             bigserial PRIMARY KEY,
  source_id      uuid NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  snapshot_id    uuid NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
  external_ref   text NOT NULL,
  event_id       bigint NOT NULL CHECK (event_id > 0),
  topic_id       text NOT NULL,
  event_type     text NOT NULL CHECK (event_type IN (
    'member_added', 'member_joined', 'member_left', 'member_removed',
    'message_pinned', 'message_unpinned', 'topic_created', 'topic_renamed',
    'topic_closed', 'topic_reopened', 'title_changed', 'photo_changed',
    'description_changed', 'permissions_changed', 'video_chat_started',
    'video_chat_ended', 'message_deleted', 'unknown', 'unsupported'
  )),
  occurred_at    timestamptz,
  payload        jsonb NOT NULL DEFAULT '{}'::jsonb,
  provenance     jsonb NOT NULL DEFAULT '{}'::jsonb,
  event_sha256   text NOT NULL CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, external_ref),
  UNIQUE (source_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_telegram_service_events_topic
  ON telegram_service_events (source_id, topic_id, event_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON telegram_service_events TO knowledge_app;
GRANT USAGE, SELECT ON SEQUENCE telegram_service_events_id_seq TO knowledge_app;
GRANT SELECT ON telegram_service_events TO knowledge_reader;

INSERT INTO schema_migrations(version) VALUES ('0005_telegram_services')
ON CONFLICT DO NOTHING;
