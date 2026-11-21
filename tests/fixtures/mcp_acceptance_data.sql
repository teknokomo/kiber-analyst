INSERT INTO projects(id,slug,title)
VALUES ('00000000-0000-0000-0000-000000000001','sample-project','Example Group');

INSERT INTO sources(id,project_id,kind,external_id,title)
VALUES (
  '00000000-0000-0000-0000-000000000010',
  '00000000-0000-0000-0000-000000000001',
  'telegram',
  '-1009000',
  'Example Station'
);

INSERT INTO documents(id,source_id,doc_type,language,created_at,meta)
VALUES (
  '00000000-0000-0000-0000-000000000020',
  '00000000-0000-0000-0000-000000000010',
  'export',
  'ru',
  '2020-01-02 00:00:00+00',
  jsonb_build_object('topic_title','Example topic')
);

INSERT INTO snapshots(source_id,sha256,size_bytes,scope)
VALUES (
  '00000000-0000-0000-0000-000000000010',
  repeat('a',64),
  4096,
  'topic'
);

INSERT INTO segments(document_id,seq,speaker,text,tsv,external_ref,content_hash,meta,posted_at)
SELECT
  '00000000-0000-0000-0000-000000000020',
  n,
  CASE WHEN n <= 2 THEN NULL ELSE 'Example Author' END,
  CASE WHEN n = 1
    THEN 'Участник без имени: идентификатор отправителя для проверки provenance.'
    WHEN n = 2
    THEN 'Участник без имени и идентификатора для проверки значения Unknown.'
    ELSE 'Example Station, сообщение ' || n || ': ангар и Sample Route.'
  END,
  to_tsvector('russian', CASE WHEN n = 1
    THEN 'Участник без имени: идентификатор отправителя для проверки provenance.'
    WHEN n = 2
    THEN 'Участник без имени и идентификатора для проверки значения Unknown.'
    ELSE 'Example Station, сообщение ' || n || ': ангар и Sample Route.'
  END),
  'tg:-1009000:' || n,
  repeat('b',64),
  jsonb_build_object(
    'url','https://t.me/c/9000/1/' || n,
    'topic_id','1',
    'sender_id',CASE WHEN n = 1 THEN 'sender-42' ELSE NULL END
  ),
  '2020-01-02 12:00:00+03'::timestamptz + make_interval(secs => n)
FROM generate_series(1,12) AS n;

INSERT INTO runs(id,project_id,kind,status,started_at,finished_at)
VALUES
  ('00000000-0000-0000-0000-000000000030','00000000-0000-0000-0000-000000000001','telegram_load','done','2020-01-02 10:00:00+00','2020-01-02 10:01:00+00'),
  ('00000000-0000-0000-0000-000000000031','00000000-0000-0000-0000-000000000001','telegram_embed','done','2020-01-02 10:02:00+00','2020-01-02 10:03:00+00');

INSERT INTO asr_runs(id,project_id,run_key,provider,model,language,status,started_at,finished_at)
VALUES
  ('00000000-0000-0000-0000-000000000040','00000000-0000-0000-0000-000000000001','asr-run-previous','groq','whisper-large-v3-turbo','ru','partial','2020-01-02 11:00:00+00','2020-01-02 11:01:00+00'),
  ('00000000-0000-0000-0000-000000000041','00000000-0000-0000-0000-000000000001','asr-run-current','groq','whisper-large-v3-turbo','ru','done','2020-01-02 12:00:00+00','2020-01-02 12:01:00+00');

INSERT INTO asr_results(run_id,external_ref,audio_sha256,duration_seconds,text,status,http_code)
SELECT
  '00000000-0000-0000-0000-000000000041',
  'tg:-1009000:' || n,
  repeat('c',64),
  12.5,
  'Example Station диктует сообщение ' || n,
  'complete',
  200
FROM generate_series(1,3) AS n;

INSERT INTO asr_segments(run_id,external_ref,segment_no,start_ms,end_ms,text,avg_logprob,no_speech_prob)
SELECT
  '00000000-0000-0000-0000-000000000041',
  'tg:-1009000:' || message_no,
  row_number() OVER (PARTITION BY message_no ORDER BY item_no) - 1,
  (item_no - 1) * 1500,
  item_no * 1500,
  CASE
    WHEN item_no <= 3 THEN 'Example Station говорит о Sample Route, фрагмент ' || item_no
    ELSE 'Речь без ключевой фразы, фрагмент ' || item_no
  END,
  -0.1,
  0.01
FROM (
  SELECT n AS item_no, ((n - 1) % 3) + 1 AS message_no
  FROM generate_series(1,8) AS n
) AS generated;

INSERT INTO asr_attempts(run_id,external_ref,attempt_no,profile,status,http_code)
SELECT
  '00000000-0000-0000-0000-000000000041',
  'tg:-1009000:' || (((n - 1) % 3) + 1),
  n,
  'groq_asr',
  'succeeded',
  200
FROM generate_series(1,4) AS n;

INSERT INTO embeddings(project_id,owner_table,owner_id,model,embedding)
SELECT
  '00000000-0000-0000-0000-000000000001',
  'segments',
  s.id::text,
  'gemini-embedding-2:1536',
  array_fill(0.001::real,ARRAY[1536])::vector
FROM segments AS s
WHERE s.document_id='00000000-0000-0000-0000-000000000020'
ORDER BY s.id
LIMIT 8;

INSERT INTO embeddings(project_id,owner_table,owner_id,model,embedding)
SELECT
  '00000000-0000-0000-0000-000000000001',
  'asr_segments',
  a.id::text,
  'gemini-embedding-2:1536',
  array_fill(0.001::real,ARRAY[1536])::vector
FROM asr_segments AS a
ORDER BY a.id
LIMIT 6;
