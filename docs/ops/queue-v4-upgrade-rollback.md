# Queue v4: согласованное обновление и откат

Этот runbook относится к SQLite-очереди Kiber-analyst и SQLite-базе broker.
Worker и broker используют общий протокол и должны обновляться и
восстанавливаться как одна согласованная пара. Совместимость worker или
broker из релиза 0.2.0 с queue v4 не доказана: запускать старый компонент на
queue v4 запрещено. MCP и база knowledge не входят в обновление очереди.

Автоматического безопасного reconciliation между снимками worker и broker
сейчас нет. После восстановления сервисы остаются остановленными до ручной
сверки по этому документу. Операторский журнал сам по себе не исправляет
восстановленное состояние и не разрешает запуск worker.

## Перед обновлением

Порядок backup важен: `kiber-analyst-queue-backup` делает обычный backup
через worker и не принимает `pre_upgrade`; для этого этапа используется
`kiber-analyst jobs backup --pre-upgrade`. Worker должен оставаться доступен
до завершения этой команды. Broker backup снимается отдельно после остановки
broker, через SQLite Backup API, а не `cp` живого файла.

1. Согласовать окно обслуживания и закрыть приём новых broker-команд. Дождаться
   завершения уже принятых RPC, остановить broker и убедиться, что его процесс
   завершён и больше не принимает запросы. Worker пока оставить работающим.
2. До backup проверить через штатный status/queue интерфейс, что нет
   `queued`, `running`, `waiting_retry` или `waiting_quota` jobs. Если такие
   jobs есть, дождаться их завершения либо отдельно согласовать их удержание;
   не снимать pre-upgrade snapshot, пока worker может поменять состояние job.
   Зафиксировать build-id/commit обоих компонентов, версии схем, SQLite
   `user_version`, полный список request/job ID, состояния, operation,
   attempts, checkpoints, `next_run_at`, связи `requests`/`attempts` и SHA-256
   доступных результатов. Секреты и значения env не включать.
3. Сохранить код, units и конфигурацию broker/worker как один rollback-набор.
   Закрытые env-файлы хранить только в защищённом backup с исходными правами;
   в отчёт включать лишь имена, права и контрольные суммы.
4. Пока broker уже остановлен, а worker доступен и очередь не содержит
   нетерминальных jobs, выполнить от root через его control socket:

   ```bash
   sudo -n env XDG_RUNTIME_DIR=/run \
     /opt/kiber-analyst/.venv/bin/kiber-analyst jobs backup --pre-upgrade
   ```

   Сохранить возвращённый `backup_path`. Этот вызов создаёт queue snapshot
   через SQLite Backup API и требует root. Не заменять его командой
   `kiber-analyst-queue-backup`: она создаёт обычную daily-копию, не
   pre-upgrade. После успешного ответа остановить worker и подтвердить, что
   оба writer-процесса остановлены.
5. Скопировать уже закрытый queue backup в защищённый каталог пары; это файл
   snapshot, не рабочая `queue.db`. Broker `requests.sqlite3` снять после
   остановки broker тем же SQLite Backup API. Ниже пример для оператора:
   `DATA_ROOT` должен быть фактическим корнем из worker doctor/config, а
   `PAIR_DIR` — новый каталог maintenance-снимка. Оба пути проверь вручную;
   не получай их из непроверенного пользовательского ввода.

   ```bash
   install -d -m 0700 "$PAIR_DIR"
   install -m 0600 "$QUEUE_BACKUP" "$PAIR_DIR/queue.sqlite3"
   KIBER_BROKER_DB="$DATA_ROOT/broker/requests.sqlite3" \
   KIBER_BROKER_SNAPSHOT="$PAIR_DIR/requests.sqlite3" \
   python3 - <<'PY'
   import os
   import sqlite3
   from pathlib import Path

   source = Path(os.environ['KIBER_BROKER_DB'])
   snapshot = Path(os.environ['KIBER_BROKER_SNAPSHOT'])
   if source.is_symlink() or not source.is_file() or snapshot.exists():
       raise SystemExit('unsafe broker backup input or destination')
   fd = os.open(snapshot, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
   os.close(fd)
   with sqlite3.connect(source) as src, sqlite3.connect(snapshot) as dst:
       src.backup(dst)
       if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
           raise SystemExit('broker backup integrity check failed')
   os.chmod(snapshot, 0o600)
   PY
   sha256sum "$PAIR_DIR/queue.sqlite3" "$PAIR_DIR/requests.sqlite3"
   PAIR_DIR="$PAIR_DIR" python3 - <<'PY'
   import os
   import sqlite3
   import tempfile
   from pathlib import Path

   pair = Path(os.environ['PAIR_DIR'])
   for name in ('queue.sqlite3', 'requests.sqlite3'):
       snapshot = pair / name
       with tempfile.TemporaryDirectory(dir=pair) as scratch_name:
           restored = Path(scratch_name) / name
           with sqlite3.connect(snapshot) as src, sqlite3.connect(restored) as dst:
               src.backup(dst)
               if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                   raise SystemExit(f'{name}: restore integrity check failed')
               version = dst.execute('PRAGMA user_version').fetchone()[0]
               tables = dst.execute(
                   "SELECT count(*) FROM sqlite_master WHERE type='table'"
               ).fetchone()[0]
           print(f'{name}: restore=ok user_version={version} tables={tables}')
   PY
   ```

   Так как broker остановлен, его database/WAL состояние фиксировано; worker
   уже снял queue snapshot и остановлен. До queue snapshot требуется ноль
   нетерминальных jobs, поэтому между двумя снимками не должно быть переходов
   состояния или новых связей. Сохранить оба файла под одной maintenance
   меткой. Проверить `PRAGMA integrity_check = 'ok'`, schema versions, список
   ID/связей и успешное восстановление каждого файла в отдельный scratch-каталог
   командами выше.
   Если любое условие не выполнено, обновление не начинать. Не копировать
   рабочие SQLite-файлы обычным `cp`.

## Обновление

1. Оставить вход broker закрытым; worker и broker должны быть остановлены.
2. Установить один проверенный build для обоих компонентов. Запуск нового
   worker выполняет предусмотренную миграцию queue v3 → v4. Не запускать
   старый broker с новым worker или наоборот. Если невозможно подтвердить
   одинаковый build и поддержку v4 обоими компонентами, оставить оба
   остановленными.
3. Запустить worker, проверить build-id, `user_version=4`, целостность,
   отсутствие stuck jobs и сохранность инвентаря. Затем запустить broker той
   же версии и проверить его read/status операции. Открыть приём команд лишь
   после этих проверок.
4. Сверить до/после каждую задачу: ID, состояние, operation, checkpoint,
   attempts, расписание, результат и broker request/attempt linkage.

## Откат

### Сохранить доказательства и остановить писателей

1. Закрыть приём broker-команд, остановить broker и worker, затем убедиться,
   что оба процесса завершились. Не восстанавливать одну базу, пока другая
   продолжает принимать записи.
2. До восстановления сохранить отдельную post-upgrade пару SQLite Backup API
   snapshots: queue и broker. Также сохранить полный инвентарь jobs/requests,
   событий, checkpoints, результатов и контрольных сумм. Эти snapshots и
   инвентарь обязательны для разбора и не удаляются при откате.
3. Зафиксировать каждую запись, созданную или изменённую после pre-upgrade
   snapshot. Для внешних вызовов собрать доказательства фактического исхода
   из сохранённых артефактов и провайдера. Состояние `running`, timeout
   клиента или потерянный ответ сами по себе не доказывают, что внешний
   запрос не был обработан.

### Восстановить только согласованную пару

1. Восстановить queue и broker `requests.sqlite3` из одной и той же
   pre-upgrade пары. Восстановление только queue запрещено: broker может
   оставить attempt, указывающий на отсутствующий worker job. Восстановление
   только broker также запрещено: request может потерять соответствующий
   job и его checkpoint.
2. Проверить SHA-256, `integrity_check`, версии обеих схем, ID и связи.
   Не понижать `user_version` вручную и не запускать несовместимую пару.
3. Сохранить post-upgrade пару неизменной. В ней может находиться единственная
   запись о завершённой задаче, отмене, результате внешнего вызова или новом
   job, которого нет в pre-upgrade backup.

### Обязательная сверка до запуска

Восстановленная pre-upgrade пара может содержать `queued` request/job, хотя
после снимка задача завершилась или была отменена. Worker снова захватит такой
job и повторит действие. Поэтому до запуска необходимо сопоставить каждую
восстановленную запись с post-upgrade snapshot и подтверждёнными артефактами:

- подтверждённые `done` и `cancelled` должны быть отражены в восстановленных
  состояниях queue и broker; одного операторского журнала недостаточно;
- каждая broker attempt должна указывать на существующий job, а request,
  attempt и job должны иметь согласованные operation, source, параметры,
  состояние и ID;
- созданные после pre-upgrade snapshot jobs/requests должны быть перечислены
  и индивидуально классифицированы. Не удалять и не терять их молча;
- job с неопределённым внешним исходом нельзя переводить в runnable-состояние
  и нельзя повторно отправлять. Он остаётся на ручном удержании до появления
  проверяемого исхода;
- ни один подтверждённый done/cancelled job не должен оставаться
  `queued`, `waiting_retry` или `waiting_quota`;
- результаты и checkpoints, используемые для reconciliation, должны иметь
  проверенные контрольные суммы и соответствовать request/job.

Автоматического reconciliation и проверенной production-процедуры
offline-repair пока нет. Приведённые выше сравнения — условия допуска, а не
инструкция вручную переписывать SQLite. Не выполнять ad hoc `UPDATE`, не
менять `user_version` и не угадывать исход внешнего запроса.

После восстановления запуск разрешён только если сверка подтверждает, что:

- сохранённые post-upgrade snapshots и inventory читаются, checksum совпадает;
- все jobs/requests/attempts из восстановленной пары согласованы по ID,
  operation, source, параметрам и состоянию, а каждый broker job ID существует
  в queue;
- ни одна задача, завершённая или отменённая после pre-upgrade snapshot, не
  остаётся runnable в восстановленной паре;
- каждая новая или изменённая после snapshot запись классифицирована, её
  результат/checkpoint имеет проверяемый hash, а внешние исходы подтверждены;
- неопределённых внешних попыток нет.

Если для выполнения этих условий нужно менять восстановленные записи, запуск
запрещён до отдельной проверенной offline-repair процедуры/инструмента и её
проверки на копии обеих БД. Сохранить оба сервиса остановленными и передать
случай оператору; один audit-журнал не разрешает запуск. Если неоднозначный
job нельзя гарантированно удержать от исполнения, worker не запускать.

## Проверенные сценарии и ограничения

Изолированные тесты демонстрируют, что:

- восстановление старой пары возвращает `done`/`cancelled` job в `queued`, и
  worker может его захватить; перед запуском требуется сверка и отражение
  подтверждённого исхода и в queue, и в broker;
- восстановление только queue оставляет broker-ссылку на отсутствующий job;
  согласованное восстановление пары убирает dangling reference, сохраняя
  post-upgrade пару для reconciliation.

Тесты моделируют backup, обнаружение расхождений и исправление только на
временных базах. Они не являются автоматическим production-reconciler и не
подтверждают готовность вручную менять production SQLite. Пока нет
проверенного offline-repair инструмента, rollback с обнаруженными
расхождениями требует остановки сервисов и отдельного операторского решения.
Этот runbook не разрешает production-деплой.
