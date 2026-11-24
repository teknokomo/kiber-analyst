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

SELECT attnotnull
FROM pg_attribute
WHERE attrelid='segments'::regclass AND attname='text';

SELECT pg_get_expr(adbin, adrelid)
FROM pg_attrdef
WHERE adrelid='segments'::regclass
  AND adnum=(SELECT attnum FROM pg_attribute
             WHERE attrelid='segments'::regclass AND attname='tsv');

SELECT indexdef
FROM pg_indexes
WHERE schemaname='public' AND tablename='segments'
  AND indexname='idx_segments_tsv';

SELECT count(*)
FROM schema_migrations
WHERE version='0007_segments_tsv_generated';

SELECT tgname, pg_get_triggerdef(oid)
FROM pg_trigger
WHERE tgrelid='segments'::regclass AND NOT tgisinternal;
```

- Ожидаются `segments.text NOT NULL` и ноль не-внутренних триггеров.
- Любой не-внутренний триггер — **остановиться и сообщить**: миграция 0007
  намеренно падает (`RAISE EXCEPTION`), обходить её нельзя.
- Для `segments.tsv` ожидается `attgenerated = ''` (обычный столбец) либо
  `attgenerated = 's'` (stored generated column). Любое другое значение —
  остановиться и выяснить состояние схемы.
- Для generated-столбца выражение должно соответствовать
  `to_tsvector('russian'::regconfig, coalesce(text, ''))`, а индекс
  `idx_segments_tsv` должен быть GIN по `tsv`.

### Если `attgenerated = 's'`

Сравни выражение `pg_get_expr` и определение индекса с ожидаемыми выше.

- Если выражение совпадает, но строки
  `0007_segments_tsv_generated` нет в `schema_migrations`, после backup и
  проверки восстановления повторно запусти миграцию из раздела 3. Она
  проверит/создаст индекс, выполнит `ANALYZE` и зарегистрирует версию.
  **Не добавляй версию ручным `INSERT`.**
- Если выражение отличается, сначала проверь `segments.text NOT NULL`.
  Если `text` nullable — остановись: различие может менять семантику и
  требует отдельного разбора. Перед исправлением сделай и проверь backup,
  затем запланируй maintenance window: изменение generated-выражения
  переписывает `segments` и требует `ACCESS EXCLUSIVE`.
- На PostgreSQL 17 и новее исправь выражение штатной командой ниже; после
  неё проверь `pg_get_expr` и только при точном совпадении повторно запусти
  0007 из раздела 3:

  ```bash
  psql -1 -v ON_ERROR_STOP=1 -d knowledge \
    -c "ALTER TABLE segments ALTER COLUMN tsv SET EXPRESSION AS (to_tsvector('russian'::regconfig, coalesce(text, ''))); ANALYZE segments;"
  ```

  `SET EXPRESSION` документирован в
  [PostgreSQL 17 ALTER TABLE](https://www.postgresql.org/docs/17/sql-altertable.html).
  На PostgreSQL 16 этой команды нет: **не запускай её и не добавляй её в
  миграцию 0007**, которая должна работать на PostgreSQL 16. Подготовь
  отдельную совместимую процедуру и остановись до её согласования.
- Если `attgenerated = ''`, сделай backup и применяй 0007 обычным способом
  из раздела 3.

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

SELECT pg_get_expr(adbin, adrelid)
FROM pg_attrdef
WHERE adrelid='segments'::regclass
  AND adnum=(SELECT attnum FROM pg_attribute
             WHERE attrelid='segments'::regclass AND attname='tsv');
-- ожидается выражение с to_tsvector('russian'::regconfig, coalesce(text, ''))

SELECT count(*) FROM segments WHERE tsv IS NULL;                 -- ожидается 0

SELECT count(*) FROM segments
WHERE tsv @@ websearch_to_tsquery('russian', '<известное слово>');  -- ожидается > 0

SELECT count(*) FROM schema_migrations
WHERE version='0007_segments_tsv_generated';                     -- ожидается 1

SELECT count(*), coalesce(max(id),0) FROM segments;              -- как до миграции
```

Плюс функциональная проверка: поиск Kiber-analyst через broker/CLI находит
известный текст **без вектора** (`mode: fts`).

## 5. Проверка полноты истории перед следующей миграцией

Перед применением любой следующей миграции сверь ledger БД с миграциями в
том же checkout, из которого будет запускаться SQL:

```bash
find migrations -maxdepth 1 -type f -name '*.sql' -printf '%f\n' | sort
```

```sql
SELECT version FROM schema_migrations ORDER BY version;
```

Проверяй каждую версию отдельно: baseline `0001_init` и версии файлов
`migrations/0002_*.sql` — `migrations/0007_*.sql`. В текущем репозитории нет
файла `migrations/0001_*.sql`; `0001_init` — начальный baseline, который
должен подтверждаться bootstrap-схемой установки. Не считай отсутствующую
запись доказательством, что baseline или миграция применены.

- Если версия есть — проверь её соответствующую структуру и данные.
- Если версия отсутствует, но вся структура и ожидаемые ограничения этой
  миграции уже совпадают, повторно запусти именно эту идемпотентную миграцию
  с `-1 -v ON_ERROR_STOP=1 -f <migration.sql>`. Миграция сама зарегистрирует
  версию. **Не добавляй запись вручную и не закрывай пропуски массовым
  `INSERT`.**
- Если структура отличается, остановись и составь точный diff. Не запускай
  миграцию в надежде, что она исправит неизвестный drift, и не записывай её
  версию. Согласуй отдельную процедуру исправления.
- Если отсутствует другая версия, разбирай её отдельно по её SQL и
  структурным pre-checks; не делай вывод о её применении по соседним
  версиям.

## 6. Откат

- Ошибка до COMMIT — откат автоматический (`-1`), версия не записана.
- Проблема после COMMIT — восстановить backup
  (`pg_restore -d knowledge /var/backups/…dump` после подтверждения владельца),
  таблицу вручную не править.

## 7. Шаблон отчёта

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
