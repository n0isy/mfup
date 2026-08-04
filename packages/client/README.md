# @mfup/client

Browser SDK for MFUP/2 — resumable multi-file upload of whole directory
trees (tens of thousands of files) with interactive conflict handling.

```bash
npm install @mfup/client
```

## Usage

```ts
import { MfupSession, sourceFromDataTransfer } from "@mfup/client";

const session = new MfupSession({
  serverUrl: "https://host/api/uploads",   // wherever mfup-fastapi is mounted
  targetDir: "incoming",
  meta: { album: 42 },                     // handed to the server's hooks
});

session.on("progress", (p) => render(p.fraction));
session.on("ask", (ask) => {
  // Server asks mid-transfer (target conflict) — upload keeps running.
  showDialog(ask.name).then((ok) => ask.respond(ok ? "merge_overwrite" : "cancel"));
});

await session.connect();
dropzone.ondrop = async (e) => {
  e.preventDefault();
  const src = sourceFromDataTransfer(e.dataTransfer!);  // sync, in the handler
  if (!src) return;
  await session.upload(src);          // resolves on COMMIT_OK
  if (await session.settleAsks() !== "cancel") {
    await session.publish();          // atomic renames into targetDir
  }
};
```

## What it handles for you

- **Ingestion**: `getAsFileSystemHandle` (Chrome), `webkitGetAsEntry`
  (Firefox/Safari), `<input webkitdirectory>`, plain file lists — one
  `upload()` for all of them.
- **Transport**: streaming upload (`duplex:"half"`) where supported, atomic
  batched POSTs elsewhere; live probe with cached verdict; CRC-32C per chunk.
- **Resilience**: automatic reconnect + resume with epoch fencing; retry of
  failed POSTs without duplication; metadata re-send so the server's commit
  invariant always converges; per-file NACK budgets.
- **Interactivity**: `ask` events with `respond()` — the user answers
  overwrite/cancel questions *while* the transfer continues.
- **State**: typed events (`file:start/ack/reject`, `committed`, `abort`, …)
  plus a coalesced immutable snapshot store (`subscribe`/`getSnapshot`) —
  plug it straight into `useSyncExternalStore` (that is exactly what
  [`@mfup/react`](https://www.npmjs.com/package/@mfup/react) does).

Server side: [`mfup-fastapi`](https://pypi.org/project/mfup-fastapi/) /
[`mfup-core`](https://pypi.org/project/mfup-core/) on PyPI.

Docs: <https://github.com/n0isy/mfup> (`docs/EXTENDING.md`, `docs/FULL.md`).
