# API расширения

MFUP/3 предоставляет сопоставимые контракты Node и Python. Поля событий и результатов используют camelCase, методы и настройки Python — snake_case. SQL-таблицы, имена staging-файлов и внутренние координаторы не являются API приложения.

## Встраивание

Node createMfup(options) возвращает engine, handle(req,res), attach(server), close(). Prefix применяется к HTTP и WebSocket. Python MfupEngine(MfupConfig(...)) предоставляет engine, router, lifespan, startup(), shutdown().

```python
from fastapi import FastAPI
from mfup_fastapi import MfupConfig, MfupEngine

async def authorize(request):
    user = await resolve_user(request['headers'])
    if user is None:
        return None
    return dict(targetDir=f"{user.id}/uploads", context=dict(userId=user.id))

mfup = MfupEngine(MfupConfig(base_dir='./data', authorize=authorize, prefix='/api'))
app = FastAPI(lifespan=mfup.lifespan)
app.include_router(mfup.router, prefix=mfup.config.prefix)
```

create_app(config) создаёт готовое FastAPI-приложение и применяет prefix. Поддерживаются create_app(base_dir, authorize, **options) и MfupEngine(base_dir, authorize, **options). Жизненный цикл запускается один раз на engine. Resolve_user в примере предоставляет приложение.

## Авторизация и прикладные переменные

authorize(AuthRequest) обязателен, допускает синхронный и асинхронный callback. Выполняется до сохранения сессии и создания staging. Null/None отклоняет создание. Исключение регистрируется и возвращается как 403 denied.

| Поле AuthRequest | Значение                                                                       |
| ---------------- | ------------------------------------------------------------------------------ |
| sessionId        | Назначенный сервером ID                                                        |
| headers          | Заголовки, включая cookie приложения                                           |
| client, query    | Адрес peer и query создания; для прямого core-вызова задаются вызывающим кодом |
| targetDir        | Запрошенное относительное назначение, по умолчанию uploads                     |
| meta             | Произвольный клиентский JSON, по умолчанию null                                |

| Поле AuthResult            | Значение                                             |
| -------------------------- | ---------------------------------------------------- |
| baseDir                    | Абсолютный корень сессии; по умолчанию корень engine |
| targetDir                  | Разрешённое относительное назначение                 |
| context                    | JSON object приложения, остаётся на сервере          |
| maxFiles, maxTotalBytes    | Квоты сессии                                         |
| autoPublish, clientPublish | Политика публикации сессии                           |

Дополнительные переменные находятся в meta. Engine передаёт их значения и структуру в JSON в authorize, onCommitted и mapFile. Поля scope, albumId, projectId не имеют особого смысла для протокола. Приложение проверяет используемые поля и может вернуть производные значения в context. Meta не разворачивается в поля hook и не сливается с context или конфигурацией.

```ts
const session = new MfupSession({
  serverUrl: location.origin,
  meta: {
    projectId: "project-7",
    albumId: "summer",
    options: { original: true },
  },
});
// The application validates projectId/albumId in authorize.
// A configured mapFile can use request.meta and request.context.
```

Результаты, meta и context сохраняются; resume не повторяет authorize. Пределы UTF-8 JSON: maxMetaBytes=16384, maxContextBytes=65536; Python max_meta_bytes/max_context_bytes. Каждый map callback получает сохранённые значения. Изменение аргумента callback приложением не обновляет сессию.

Staging и опубликованные файлы сессии используют выбранный baseDir; SQLite остаётся в корне engine. PublishedDirectory(baseDir,targetDir) / published_directory(base_dir,target_dir) возвращает итоговый каталог. Один процесс владеет БД и всеми назначенными через неё корнями.

[Примеры интеграции](../../examples/README_ru.md) реализуют анонимную cookie и три прикладные зоны через этот контракт. ScopeRoots/scope_roots — настройка примера. Ядро и адаптеры не предоставляют отдельный реестр зон или endpoint scopes.

## Публичные типы и методы

Node экспортирует AuthRequest, AuthResult, FileMapRequest, CommitEvent, StagedFile, Options, Limits. Python экспортирует TypedDict событий/результатов и py.typed. ProtocolError с code/status — публичный тип.

Поля CommitEvent: sessionId, targetDir, baseDir, stagingDir, files, bytes, meta, context. FileMapRequest: sessionId, path, name, size, targetDir, meta, context. StagedFile: path, size, mtime, localPath.

| Node engine                                    | Python engine                 | Назначение                                                    |
| ---------------------------------------------- | ----------------------------- | ------------------------------------------------------------- |
| getSession(id)                                 | get_session(id)               | Серверные сведения, meta, context и processing                |
| listStaged(id)                                 | list_staged(id)               | Перечисление staged-метаданных страницами по 256              |
| openStaged(id,path)                            | open_staged(id,path)          | ReadStream / бинарный файл принятого содержимого              |
| preparePublish(id)                             | prepare_publish(id)           | Проверить сохранённые назначения и разрешение без перемещений |
| publish(id)                                    | publish(id)                   | Доверенная публикация backend                                 |
| retryCommitted(id)                             | retry_committed(id)           | Явный повтор неудачной/прерванной обработки                   |
| setProperties(id,properties)                   | set_properties(id,properties) | Изменить разрешение перезаписи                                |
| snapshot, resume, answer, cancel, sweep, close | те же имена                   | Состояние и жизненный цикл                                    |

Доступ к staging открыт после commit, включая onCommitted. MapFile получает метаданные во время приёма и не читает staging. Потоки нужно закрыть до публикации. Staging не повторяет дерево источника; используйте openStaged или localPath.

## Обработка и политика публикации

OnCommitted(CommitEvent) / on_committed работает вне блокировки сессии, может читать staging и вызывать backend publish. Boolean-результат выбирает автоматическое действие сервера: true — попытаться опубликовать, false — не запускать автоматическую публикацию, undefined/None — использовать серверный autoPublish сессии.

| Настройка                             | Default | Поведение                                      |
| ------------------------------------- | ------- | ---------------------------------------------- |
| SDK/React autoPublish                 | true    | Автоматический publish после commit/разрешения |
| Server autoPublish / auto_publish     | false   | Публикация после успешной обработки сервером   |
| Server clientPublish / client_publish | true    | Разрешение HTTP publish владельцу сессии       |

SDK autoPublish=false оставляет явный session.publish(). False от hook не запрещает клиентский publish; для решения только backend задайте clientPublish=false. HTTP publish тогда даёт 403 server_publish_only; engine.publish доступен. Для отложенной серверной обработки задайте clientPublish=false, autoPublish=false и публикуйте явно после готовности.

Processing сохраняется как none/pending/running/done/failed. Ошибка hook оставляет успешный commit и processing=failed. Повтор commit не повторяет завершённый hook. RetryCommitted повторяет незавершённую/неудачную обработку, объединяет одновременные вызовы и не повторяет успешную. Прерванное running при старте становится failed. Внешние действия требуют идемпотентности приложения по sessionId; гарантия durable exactly-once отсутствует.

До завершения обработки клиентская публикация блокируется 409 processing_required. Server autoPublish может продолжиться после разрешения перезаписи; отложенный backend с autoPublish=false повторяет publish явно.

Синхронный onError/on_error получает hook, sessionId, error; по умолчанию используется серверный logger. Ошибка logger не меняет результат загрузки. Исходные исключения не попадают в клиентские снимки.

Отмена исключает позднюю публикацию callback. Очистка staging дожидается callbacks; sweep пропускает активные callbacks и планирование. Close также дожидается callbacks; их timeout задаёт приложение.

## Маппинг файлов

MapFile(FileMapRequest) / map_file вызывается сразу при появлении нового файла в поступившем манифесте корзины, до чтения тела. Результат — относительное назначение внутри targetDir либо null/None для исходного пути. Консьюмеру не нужны обвязки, дубликат entries или предварительное чтение содержимого. Проверка тела выполняется в onCommitted через listStaged/openStaged.

```ts
mapFile: async ({ path, name, meta, context }) => {
  const category = await lookupCategory(meta, context, path);
  return category ? `${category}/${name}` : null;
};
```

LookupCategory предоставляет приложение. Приём метаданных сериализован внутри сессии. Каждый новый файл маппится один раз за попытку принятия манифеста. Пути и коллизии проверяются по индексам постоянных метаданных и ограниченной текущей корзине. Первый конфликт может вызвать вопрос до завершения маппинга остальных файлов корзины. Ошибка хука или назначения даёт 409 mapping_error. Непринятый манифест может повторить callbacks; маппинг должен допускать повтор.

Успешный маппинг манифеста сохраняется одной SQLite-транзакцией на корзину. Принятые назначения используются для следующих диапазонов, повторов и restart. Нельзя рекурсивно вызывать preparePublish/publish из mapFile. При mapper явные пустые каталоги не публикуются; без него исходные пути и пустые каталоги сохраняются.

Публикация использует перемещения в одной файловой системе. Rename каждого файла атомарен; всё дерево не является одной транзакцией. Копии для отката заменённых файлов не хранятся. Приложение не должно писать в назначения незавершённой публикации.

## Клиент и React

MfupSession предоставляет connect, upload, pause, resume, retry, setOverwrite, answer, publish, cancel, exportTicket, getSnapshot, subscribe, dispose. Источники: массивы File, FileList, handles, entries, AsyncIterable. SourceFromDataTransfer вызывается синхронно внутри drop handler.

React useMfupUpload(options) предоставляет start, session, snapshot, pendingAsks, answer, setOverwrite, pause, resume, retry, cancel. Подписка использует useSyncExternalStore. UseMfupSession(session) подписывается на существующую сессию.

Показывайте не более одного вопроса перезаписи и одной операционной ошибки на загрузку. SetOverwrite(true) сохраняет разрешение для всей сессии. Отмена остаётся cancelling до подтверждения; поздняя отмена может вернуть published. ErrorInfo содержит code/status/phase/retryable. TrackUploadProgress включает XHR-оценки sentBytes рядом с confirmedBytes. Состояния, счётчики, сроки ожидания и повторы: [PROTOCOL.md](PROTOCOL.md).

SDK владеет итератором источника и не более чем 10000 ожидающих записей, включая активные корзины. Обход останавливается на этом пороге и продолжается после сокращения до 5000; ожидание повторов и удерживаемая ошибка также останавливают чтение. `maxReady` уменьшает предел (1–10000), нижний порог равен его половине. Ссылки на подтверждённые File/entries освобождаются. Консьюмер передаёт источник напрямую и не должен собирать или хранить дубликат списка entries/File. После исчерпания попыток или операционной ошибки `upload()` отклоняется, но SDK сохраняет ограниченный незавершённый поток. После устранения причины вызовите `session.retry()` (React `retry()`) без источника. Cancel/dispose освобождают передачу. Перезагрузка страницы уничтожает доступ в памяти; повторный выбор источника нужен только в этом случае.

## Standalone-конфигурация

Node configFromEnv загружает hooks вида ./hooks.mjs#authorize или package#export; без фрагмента берёт default export. Относительные пути считаются от cwd. Запуск — mfup-server из Node-установки.

Python MfupConfig.from_env принимает имена module:attribute. Запуск — python -m mfup_fastapi или mfup-server из Python-установки. Ошибка импорта hook и отсутствие authorize останавливают старт. Исполняемые команды имеют одинаковое имя в соответствующих окружениях.

| Переменная окружения                        | Default                                   |
| ------------------------------------------- | ----------------------------------------- |
| MFUP_BASE_DIR                               | ./data                                    |
| MFUP_AUTHORIZE                              | Обязательно                               |
| MFUP_MAP_FILE, MFUP_ON_COMMITTED            | Не заданы                                 |
| MFUP_PREFIX                                 | Пустой                                    |
| MFUP_AUTO_PUBLISH, MFUP_CLIENT_PUBLISH      | false, true                               |
| MFUP_TTL_MS, MFUP_SWEEP_INTERVAL_MS         | 86400000, 60000; sweep=0 отключает таймер |
| MFUP_MAX_META_BYTES, MFUP_MAX_CONTEXT_BYTES | 16384, 65536                              |
| MFUP_CONCURRENCY, MFUP_MAX_PARTS            | 6, 128                                    |
| MFUP_BATCH_BYTES, MFUP_PART_BYTES           | 33554432, 16777216                        |
| MFUP_HOST, MFUP_PORT                        | 127.0.0.1, 3001                           |

Импорты hooks и абсолютные корни — серверная конфигурация, а не выбор из браузерных метаданных. Неизвестные JSON-поля допускаются; приложению нужен вариант обработки неизвестного error code. [Производительность](PERFORMANCE.md) описывает стоимость metadata, маппинга и payload.

[English](../EXTENDING.md)
