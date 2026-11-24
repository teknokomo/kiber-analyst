# Применение 0007 к рабочей базе knowledge

Runbook для миграции `migrations/0007_segments_tsv_generated.sql` на рабочей БД
`knowledge` на рабочем сервере. Выполняется **после merge PR #3**, из чекаута
Kiber-analyst на том же сервере, ролью — владельцем базы `knowledge`.

Все команды ниже — точные; плейсхолдеры только для локальных путей и имён ролей.

## 1. Pre-check (база knowledge)

```sql
SELECT attgenerated::text
FROM pg_attribute
WHERE attrelid='segments'::regclass AND attname='tsv';

SELECT tgname, pg_get_triggerdef(oid)
FROM pg_trigger
WHERE tgrelid='segments'::regclass AND NOT tgisinternal;
```

- Ожидается `attgenerated = ''` и ноль не-внутренних триггеров.
- `attgenerated = 's'` — миграция уже применена: ничего не делать, проверить
  только индекс (`idx_segments_tsv`) и выполнить `ANALYZE segments;`.
- Любой не-внутренний триггер — **остановиться и сообщить**: миграция 0007
  намеренно падает (`RAISE EXCEPTION`), обходить её нельзя.

## 2. Backup

```bash
# counts до миграции — сохранить в отчёт
psql -d knowledge -t -c "SELECT count(*), coalesce(max(id),0) FROM segments;"

pg_dump -Fc -d knowledge -f /var/backups/knowledge/pre-0007-$(date +%Y%m%d-%H%M%S).dump
sha256sum /var/backups/knowledge/pre-0007-*.dump
```

Проверка backup (читается и содержит данные):

```bash
createdb -h 127.0.0.1 -U <owner> knowledge_restore_check
pg_restore -h 127.0.0.1 -U <owner> -d knowledge_restore_check \
  /var/backups/knowledge/pre-0007-<stamp>.dump
psql -d knowledge_restore_check -t -c \
  "SELECT count(*), coalesce(max(id),0) FROM segments;"
# должно совпасть с counts до миграции; после проверки базу удалить:
dropdb -h 127.0.0.1 -U <owner> knowledge_restore_check
```

## 3. Применение (окно обслуживания)

```bash
psql -1 -v ON_ERROR_STOP=1 -d knowledge \
  -f migrations/0007_segments_tsv_generated.sql
```

- `-1` обязателен: DO-блок и запись версии — одна транзакция; при ошибке
  версия 0007 не появляется.
- Миграция сама проверяет триггеры и вид колонки (`attgenerated`): триггеры и
  неподдерживаемые значения дают `RAISE EXCEPTION`.
- Замерить длительность (`time` на команде) для отчёта.

## 4. Post-check

```sql
SELECT attgenerated::text FROM pg_attribute
WHERE attrelid='segments'::regclass AND attname='tsv';          -- ожидается 's'

SELECT count(*) FROM segments WHERE tsv IS NULL;                 -- ожидается 0

SELECT count(*) FROM segments
WHERE tsv @@ websearch_to_tsquery('russian', '<известное слово>');  -- ожидается > 0

SELECT count(*) FROM schema_migrations
WHERE version='0007_segments_tsv_generated';                     -- ожидается 1

SELECT count(*), coalesce(max(id),0) FROM segments;              -- как до миграции
```

Плюс функциональная проверка: поиск Kiber-analyst через broker/CLI находит
известный текст **без вектора** (`mode: fts`).

## 5. Откат

- Ошибка до COMMIT — откат автоматический (`-1`), версия не записана.
- Проблема после COMMIT — восстановить backup
  (`pg_restore -d knowledge /var/backups/…dump` после подтверждения владельца),
  таблицу вручную не править.

## 6. Шаблон отчёта

```text
PR #3: <url>            applied merge commit: <full merge commit SHA>
Backup: <path> (<size>, sha256 <hash>, restore-verified: yes|no)
Before: segments count=<n> max(id)=<n>
Duration: <s> s
Post-check: attgenerated='s'; tsv NULL=0; FTS match=<n>; version=1;
            counts unchanged (count=<n> max(id)=<n>)
CLI/broker search: mode=fts, results=<n>
NOT TOUCHED: Telegram- и ASR-данные, рабочие ASR/embedding jobs
```
