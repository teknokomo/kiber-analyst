# Применение 0006 External discussions

`0006_external_discussions.sql` — обычная forward-миграция для базы Kiber-analyst `knowledge`. Она добавляет метаданные внешних обсуждений и `embeddings.content_hash`; Telegram- и ASR-строки не преобразует. Запускай её только после проверки целевой базы, из checkout с проверенным файлом миграции, от владельца базы `knowledge_owner`.

Миграция сама записывает `0006_external_discussions` в `schema_migrations`. Не добавляй эту запись вручную. `0007_segments_tsv_generated.sql` отдельно и этим runbook не изменяется.

## 1. Pre-check

Убедись, что подключение ведёт к Kiber-analyst базе `knowledge`, а роли и таблица `snapshots` существуют:

```sql
SELECT current_database(), current_user, current_setting('server_version');
SELECT rolname FROM pg_roles
WHERE rolname IN ('knowledge_owner', 'knowledge_app', 'knowledge_reader')
ORDER BY rolname;
SELECT to_regclass('public.snapshots');
```

Проверь ledger, исходные счётчики, старый индекс и дубликаты ненулевых ссылок сегментов:

```sql
SELECT version FROM schema_migrations ORDER BY version;
SELECT count(*), coalesce(max(id), 0) FROM segments;
SELECT count(*) FROM documents;
SELECT count(*) FROM snapshots;
SELECT count(*) FROM embeddings;

SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname='public' AND tablename='segments'
  AND indexname='uq_segments_external_ref';

SELECT external_ref, count(*) FROM segments
WHERE external_ref IS NOT NULL
GROUP BY external_ref HAVING count(*) > 1;
```

Если `documents.external_ref` уже существует, проверь пары, которые будут покрыты частичным уникальным индексом:

```sql
SELECT source_id, external_ref, count(*) FROM documents
WHERE external_ref IS NOT NULL
GROUP BY source_id, external_ref HAVING count(*) > 1;
```

Если колонка ещё отсутствует, проверь пары после её временного добавления в откатываемой транзакции, до создания индекса:

```sql
BEGIN;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS external_ref text;
SELECT source_id, external_ref, count(*) FROM documents
WHERE external_ref IS NOT NULL
GROUP BY source_id, external_ref HAVING count(*) > 1;
ROLLBACK;
```

Оба запроса на дубликаты должны вернуть ноль строк. При дубликатах, неожиданных объектах или неподходящем владельце остановись и сообщи расхождение. Не удаляй и не исправляй строки вручную.

## 2. Backup и проверка восстановления

Перед DDL создай свежий custom-format dump и зафиксируй его размер и SHA-256:

```bash
pg_dump -Fc -d knowledge -f <backup-dir>/pre-0006-<UTC-stamp>.dump
sha256sum <backup-dir>/pre-0006-<UTC-stamp>.dump
```

Восстанови dump в отдельную scratch-БД. Пример команд (замени имена и пути на временные значения текущего обслуживания):

```bash
createdb -h 127.0.0.1 -U <owner> knowledge_pre0006_check
pg_restore --exit-on-error -h 127.0.0.1 -U <owner> \
  -d knowledge_pre0006_check <backup-dir>/pre-0006-<UTC-stamp>.dump
psql -h 127.0.0.1 -U <owner> -d knowledge_pre0006_check -c \
  "SELECT (SELECT count(*) FROM segments) AS segments,
          (SELECT coalesce(max(id),0) FROM segments) AS max_segment_id,
          (SELECT count(*) FROM documents) AS documents,
          (SELECT count(*) FROM snapshots) AS snapshots,
          (SELECT count(*) FROM embeddings) AS embeddings;"
dropdb -h 127.0.0.1 -U <owner> knowledge_pre0006_check
```

Сверь все пять значений с pre-check. Не продолжай, если архив не восстанавливается или значения различаются. Сохрани backup до завершения post-check.

## 3. Применение

Из checkout с проверенным `migrations/0006_external_discussions.sql`, от `knowledge_owner`:

```bash
psql -1 -v ON_ERROR_STOP=1 -d knowledge \
  -f migrations/0006_external_discussions.sql
```

`-1` обязателен: DDL и запись версии выполняются одной транзакцией. При ошибке транзакция откатится. Не повторяй применение, пока причина ошибки не выяснена; ручной `INSERT` в ledger запрещён.

## 4. Post-check

Проверь наличие колонок `documents.external_ref`, `segments.thread_ref`, `segments.parent_external_ref`, `segments.item_kind`, `embeddings.content_hash`; индексов `uq_documents_source_external_ref`, `uq_segments_external_ref_telegram`, `uq_segments_document_external_ref`, `idx_segments_thread_ref`, `idx_segments_parent_external_ref`, `idx_segments_item_kind`; отсутствие старого `uq_segments_external_ref`; grants `knowledge_app` на SELECT/INSERT/UPDATE и `knowledge_reader` на SELECT для `documents`, `segments`, `snapshots`.

Также проверь:

```sql
SELECT count(*) FROM schema_migrations
WHERE version='0006_external_discussions'; -- ровно 1
SELECT count(*) FROM schema_migrations
WHERE version='0007_segments_tsv_generated'; -- ровно 1, если 0007 уже была применена
SELECT count(*), coalesce(max(id), 0) FROM segments; -- как до миграции
SELECT count(*) FROM segments WHERE item_kind IS NOT NULL;
SELECT count(*) FROM embeddings WHERE content_hash IS NULL;
SELECT count(*) FROM embeddings e
JOIN segments s ON e.owner_table='segments' AND e.owner_id=s.id::text
WHERE s.item_kind IS NOT NULL;
```

Для прежних Telegram/ASR-векторов `content_hash IS NULL` ожидаем. Если найдены сегменты с `item_kind IS NOT NULL` или embeddings, связанные с такими сегментами, остановись и отдельно оцени необходимость backfill/re-embed; не запускай его в рамках миграции. При отсутствии внешних источников ожидаются ноль таких сегментов и векторов.

Сверь полнотекстовый поиск, `segments.tsv IS NULL`, а также рабочие состояния worker, broker и MCP (`/healthz` должен отвечать HTTP 200). Миграция не должна менять счётчики и содержимое Telegram/ASR.

## 5. Полнота ledger

Перед следующей миграцией сверь `schema_migrations` с checkout и контрактом каждой версии. Для production, где 0007 уже применена, после 0006 ожидаются `0001_init` и все версии `0002`–`0007`, каждая ровно один раз. Для более ранних окружений ledger должен содержать непрерывный префикс до последней применённой версии. В этом репозитории `0001_init` — bootstrap baseline без отдельного SQL-файла.

Наличие строки в ledger не заменяет структурную проверку соответствующей миграции. Если запись отсутствует, но вся структура и ограничения совпадают, повторно запусти именно её идемпотентный SQL с `-1`; миграция должна зарегистрировать себя. Если структура не совпадает, остановись и составь diff. Не добавляй версии вручную и не закрывай несколько пропусков массовым `INSERT`.

Для первоначальной диагностики используй:

```bash
find migrations -maxdepth 1 -type f -name '*.sql' -printf '%f\n' | sort
```

```sql
SELECT version FROM schema_migrations ORDER BY version;
```

## 6. Откат

Ошибка до COMMIT откатывается транзакцией `-1`. Если проблема обнаружилась после COMMIT, не исправляй таблицы вручную: сохрани диагностику и следуй проверенной процедуре восстановления из backup.
