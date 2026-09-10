# MFUP/3

[![npm client](https://img.shields.io/npm/v/@mfup/client?label=%40mfup%2Fclient)](https://www.npmjs.com/package/@mfup/client)
[![npm react](https://img.shields.io/npm/v/@mfup/react?label=%40mfup%2Freact)](https://www.npmjs.com/package/@mfup/react)
[![npm server](https://img.shields.io/npm/v/@mfup/server?label=%40mfup%2Fserver)](https://www.npmjs.com/package/@mfup/server)
[![PyPI core](https://img.shields.io/pypi/v/mfup-core?label=mfup-core)](https://pypi.org/project/mfup-core/)
[![PyPI FastAPI](https://img.shields.io/pypi/v/mfup-fastapi?label=mfup-fastapi)](https://pypi.org/project/mfup-fastapi/)
[![CI](https://github.com/n0isy/mfup/actions/workflows/ci.yml/badge.svg)](https://github.com/n0isy/mfup/actions/workflows/ci.yml)

Resumable browser file uploads using native **multipart/form-data**: up to six concurrent requests, multiple ready files per request, incremental folder discovery and one optional overwrite approval per upload.

## Packages

| Package                     | Runtime                             |
| --------------------------- | ----------------------------------- |
| `@mfup/client`              | Browser File/Blob and FormData APIs |
| `@mfup/react`               | React 18+                           |
| `@mfup/server`              | Node 22.13+                         |
| `mfup-core`, `mfup-fastapi` | Python 3.10+                        |

```bash
npm install @mfup/client@^3 @mfup/react@^3
npm install @mfup/server@^3
python -m pip install 'mfup-fastapi>=3,<4'
```

## Browser client

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

The application supplies the input and rendering functions. Sources include File arrays, FileList, directory handles, entries and AsyncIterable. Call `sourceFromDataTransfer()` synchronously in a drop handler. The browser reads File/Blob data; the SDK does not copy payload into JavaScript buffers.

`meta` carries arbitrary application JSON to authorize, processing and mapping. Fields such as scope or albumId do not require a separate protocol mechanism.

`setOverwrite(true)` grants permission for the entire session, including subsequent conflicts. The application shows one optional prompt. Files can continue uploading while approval is pending; existing destinations change only during publication. Render one operational error from `snapshot.errorInfo`.

`trackUploadProgress: true` enables native XHR events during long POSTs. `sentBytes` estimates sent payload; `confirmedBytes` counts receipt-backed payload. Completion is `state === "published"`, not merely all bytes sent. A custom fetch takes precedence over XHR.

## Resume and React

Save `session.exportTicket()` after connect. Within the same page use pause/resume. After reload, restore the ticket and select the same source files again:

```ts
const resumed = new MfupSession({ serverUrl: location.origin, ticket });
await resumed.upload(sourceFromInput(input));
resumed.dispose();
```

Matching uses path, size and mtime; confirmed ranges are not resent. Tickets grant access to their session and should be stored according to application policy. Cancellation stays cancelling until the server confirms it. `autoPublish: false` allows explicit `session.publish()`.

```tsx
import { useMfupUpload } from "@mfup/react";
const { start, snapshot, setOverwrite, pause, resume, cancel } = useMfupUpload({
  serverUrl: location.origin,
  trackUploadProgress: true,
});
```

## Server

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

The application implements resolveUser/resolve_user and its access policy. Authorization can set session roots, quotas and context. `mapFile` / `map_file` produces a saved destination plan after commit. `onCommitted` / `on_committed` reads accepted files and returns a boolean controlling server publication. Server autoPublish defaults to false; clientPublish=false reserves publication for the backend. See the [extension API](docs/EXTENDING.md).

One process owns SQLite and its assigned roots; Redis is not required. Receipts support process restart. File renames are atomic individually; publishing a tree is not one transaction and does not retain rollback copies. Payload fsync for power-loss durability is not performed. Empty directories are available through handles/entries, but not FileList. Exact behavior is specified by the [protocol](docs/PROTOCOL.md).

## Demo

```bash
docker compose up -d
```

Open `http://localhost:20060` for Node or `http://localhost:20060/python/` for Python. Both use the same React UI with anonymous cookies, application zones, own-file listing/download, folder input, drag/drop and resume. Caddy is the entry point; sources are bind-mounted. Local data is under `data/example-node` and `data/example-python`.

Without Docker:

```bash
npm ci --no-audit --no-fund
npm run build
npm run dev
```

Open `http://localhost:3000`. For the Python demo install `./server/mfup-core` and `./server/mfup-fastapi`, then set MFUP_BACKEND=python. [Examples](examples/README.md) describe embedding and configuration.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [HTTP protocol, overwrite, cancellation and errors](docs/PROTOCOL.md)
- [Extension API](docs/EXTENDING.md)
- [Performance and transport costs](docs/PERFORMANCE.md)
- [Testing](docs/TESTING.md)
- [Package release workflow](docs/RELEASE.md)

CI covers Node/Python on Linux, Windows and macOS, with Chromium, Firefox, WebKit, Chrome, Edge and mobile browser emulation. Physical devices and stock Safari are not represented by browser emulation. npm and Python distribution contents and installed-package consumers are checked before publication.

[Russian](README_ru.md)
