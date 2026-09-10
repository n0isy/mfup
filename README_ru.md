# MFUP/3

[![npm client](https://img.shields.io/npm/v/@mfup/client?label=%40mfup%2Fclient)](https://www.npmjs.com/package/@mfup/client)
[![npm react](https://img.shields.io/npm/v/@mfup/react?label=%40mfup%2Freact)](https://www.npmjs.com/package/@mfup/react)
[![npm server](https://img.shields.io/npm/v/@mfup/server?label=%40mfup%2Fserver)](https://www.npmjs.com/package/@mfup/server)
[![PyPI core](https://img.shields.io/pypi/v/mfup-core?label=mfup-core)](https://pypi.org/project/mfup-core/)
[![PyPI FastAPI](https://img.shields.io/pypi/v/mfup-fastapi?label=mfup-fastapi)](https://pypi.org/project/mfup-fastapi/)
[![CI](https://github.com/n0isy/mfup/actions/workflows/ci.yml/badge.svg)](https://github.com/n0isy/mfup/actions/workflows/ci.yml)

Возобновляемая загрузка файлов из браузера через обычный **multipart/form-data**: до шести параллельных запросов, несколько готовых файлов на запрос, постепенный обход каталогов и одно опциональное разрешение перезаписи на загрузку.

## Пакеты

| Пакет                       | Среда                           |
| --------------------------- | ------------------------------- |
| `@mfup/client`              | Браузерные File/Blob и FormData |
| `@mfup/react`               | React 18+                       |
| `@mfup/server`              | Node 22.13+                     |
| `mfup-core`, `mfup-fastapi` | Python 3.10+                    |

```bash
npm install @mfup/client@^3 @mfup/react@^3
npm install @mfup/server@^3
python -m pip install 'mfup-fastapi>=3,<4'
```

## Клиент браузера

```ts
import { MfupSession, sourceFromInput } from "@mfup/client";

const session = new MfupSession({
  serverUrl: location.origin,
  meta: { albumId: "summer" },
  trackUploadProgress: true,
});
session.subscribe(() => {
  const state = session.getSnapshot();
  renderProgress(state.sentBytes, state.totalBytes, state.confirmedBytes);
  renderOverwritePrompt(
    state.overwriteRequired,
    () => session.setOverwrite(true),
    () => session.cancel(),
  );
});
await session.upload(sourceFromInput(input));
session.dispose();
```

Input и функции отображения предоставляет приложение. Источники: массивы File, FileList, handles, entries, AsyncIterable. SourceFromDataTransfer() вызывается синхронно в drop handler. Браузер читает File/Blob; SDK не копирует payload в JavaScript-буферы.

Meta передаёт произвольный JSON приложения в авторизацию, обработку и маппинг. Поля scope или albumId не требуют отдельного механизма протокола.

SetOverwrite(true) разрешает перезапись для всей сессии, включая будущие конфликты. Приложение показывает один опциональный вопрос. Файлы могут продолжать отправляться до решения; существующие назначения меняются только при публикации. Операционная ошибка отображается одним сообщением из snapshot.errorInfo.

TrackUploadProgress=true включает native XHR events внутри длинных POST. SentBytes оценивает отправленный payload, confirmedBytes считает payload с receipts. Завершение — state=published, а не только отправка всех байтов. Пользовательский fetch имеет приоритет над XHR.

По умолчанию 1000 повторов с задержкой до 36 секунд поддерживают восстановление около десяти часов. SDK удерживает до 10000 ожидающих записей вместе с активными отправками и продолжает обход при 5000. Не собирайте и не храните дубликат entries/File. После окончательной ошибки вызовите session.retry() без нового источника; ограниченный поток остаётся внутри SDK.

## Продолжение и React

Сохраните session.exportTicket() после connect. В текущей странице используйте pause/resume. После reload восстановите ticket и повторно выберите те же файлы:

```ts
const resumed = new MfupSession({ serverUrl: location.origin, ticket });
await resumed.upload(sourceFromInput(input));
resumed.dispose();
```

Сопоставление выполняется по пути, размеру и mtime; принятые диапазоны не передаются повторно. Ticket даёт доступ к сессии и хранится согласно политике приложения. Отмена остаётся cancelling до подтверждения сервера. AutoPublish=false оставляет явный session.publish().

```tsx
import { useMfupUpload } from "@mfup/react";
const { start, snapshot, setOverwrite, pause, resume, cancel } = useMfupUpload({
  serverUrl: location.origin,
  trackUploadProgress: true,
});
```

## Сервер

```ts
import { createServer } from "node:http";
import { createMfup } from "@mfup/server";

const mfup = createMfup({
  baseDir: "./data",
  authorize: async ({ headers }) => {
    const user = await resolveUser(headers);
    return user
      ? { targetDir: `${user.id}/uploads`, context: { userId: user.id } }
      : null;
  },
});
const server = createServer(async (req, res) => {
  if (!(await mfup.handle(req, res))) {
    res.writeHead(404);
    res.end();
  }
});
mfup.attach(server);
server.listen(3000);
// Shutdown: await mfup.close(); server.close();
```

```python
from mfup_fastapi import create_app

async def authorize(request):
    user = await resolve_user(request['headers'])
    if user is None:
        return None
    return dict(targetDir=f"{user.id}/uploads", context=dict(userId=user.id))

app = create_app('./data', authorize=authorize)
```

Приложение реализует resolveUser/resolve_user и правила доступа. Авторизация задаёт корни сессий, квоты и context. MapFile/map_file маппит метаданные сразу при получении манифеста. OnCommitted/on_committed читает принятые файлы и возвращает boolean для управления серверной публикацией. Server autoPublish по умолчанию false; clientPublish=false оставляет публикацию backend. Подробности: [API расширения](docs/ru/EXTENDING.md).

Один процесс владеет SQLite и выданными корнями; Redis не требуется. Receipts поддерживают перезапуск процесса. Rename файлов атомарны по отдельности; публикация дерева не является одной транзакцией и не хранит копии для отката. Fsync payload для гарантии при потере питания не выполняется. Пустые каталоги доступны через handles/entries, но не FileList. Точное поведение задаёт [протокол](docs/ru/PROTOCOL.md).

## Demo

```bash
docker compose up -d
```

Node: http://localhost:20060; Python: http://localhost:20060/python/. Общий React UI предоставляет анонимную cookie, прикладные зоны, список/скачивание собственных файлов, папочный input, drag/drop и resume. Caddy — единый вход; исходники подключены bind mounts. Данные находятся в data/example-node и data/example-python.

Без Docker:

```bash
npm ci --no-audit --no-fund
npm run build
npm run dev
```

Откройте http://localhost:3000. Для Python установите ./server/mfup-core и ./server/mfup-fastapi, задайте MFUP_BACKEND=python. Встраивание и настройки описаны в [примерах](examples/README_ru.md).

## Документация

- [Архитектура](docs/ru/ARCHITECTURE.md)
- [HTTP-протокол, перезапись, отмена и ошибки](docs/ru/PROTOCOL.md)
- [API расширения](docs/ru/EXTENDING.md)
- [Производительность и стоимость транспорта](docs/ru/PERFORMANCE.md)
- [Тестирование](docs/ru/TESTING.md)
- [Выпуск пакетов](docs/ru/RELEASE.md)

CI проверяет Node/Python на Linux, Windows, macOS в Chromium, Firefox, WebKit, Chrome, Edge и мобильной эмуляции. Физические устройства и штатный Safari не подтверждаются эмуляцией. Состав npm/Python-дистрибутивов и использование установленных пакетов проверяются до публикации.

[English](README.md)
