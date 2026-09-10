# @mfup/server

Сервер MFUP/3 для Node 22.13+: потоковый multipart, сохраняемые receipts, вопросы пользователю, корни сессий и hooks приложения. Один процесс владеет каталогом метаданных.

```ts
import { createServer } from "node:http";
import { createMfup } from "@mfup/server";
const mfup = createMfup({
  baseDir: "./state",
  prefix: "/api",
  authorize: async ({ headers }) => {
    const user = await resolveUser(headers);
    return user
      ? { targetDir: `${user.id}/uploads`, context: { uid: user.id } }
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

ResolveUser предоставляет приложение. Браузер использует serverUrl:location.origin+'/api'. Scope и личность принадлежат приложению. PublishedDirectory(baseDir,targetDir) вычисляет каталог для listing/download.

MapFile выполняется после commit, возвращает относительный путь либо null и сохраняет проверенный план до перемещений. OnCommitted получает корни, счётчики, meta/context; engine.listStaged/openStaged открывают принятые файлы. Boolean-результат callback переопределяет server autoPublish (default false). ClientPublish:false оставляет решение engine.publish(id) на backend. SDK autoPublish управляет только автоматическим клиентским запросом. Ошибка onCommitted оставляет успешный commit с processing=failed; engine.retryCommitted(id) повторяет обработку явно.

Экспортируются AuthRequest, AuthResult, FileMapRequest, CommitEvent, StagedFile, Options, ProtocolError(code/status), createMfup, Engine, configFromEnv. Standalone: MFUP_AUTHORIZE='./hooks.mjs#authorize' mfup-server.

[API расширения](https://github.com/n0isy/mfup/blob/main/docs/ru/EXTENDING.md) · [Протокол](https://github.com/n0isy/mfup/blob/main/docs/ru/PROTOCOL.md)

[English](README.md)
