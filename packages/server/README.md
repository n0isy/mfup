# @mfup/server

MFUP/2 upload server for Node — resumable, interactive, atomic multi-file
uploads (`node_modules`-scale trees) with a **universal handler** that plugs
into any Node HTTP stack: express, vite, fastify (raw), Next custom server,
or bare `node:http`.

Wire-compatible with the Python server (`mfup-fastapi`): same protocol, same
on-disk session journal, same Redis key layout — the browser packages
[`@mfup/client`](https://www.npmjs.com/package/@mfup/client) and
[`@mfup/react`](https://www.npmjs.com/package/@mfup/react) work against
either.

**Zero native dependencies.** SQLite comes from `node:sqlite` (Node ≥ 22.13),
CRC-32C is a table-driven JS implementation (V8 runs it at hundreds of MB/s),
and the only runtime dependency is `ws`. `redis` is an optional peer, needed
only if you opt into the Redis session store.

## Quick start

```ts
import { createMfup } from "@mfup/server";

const mfup = createMfup({
  baseDir: "/srv/uploads",
  authorize: async (req) => {
    const user = await authenticate(req.headers.cookie);
    if (!user) return null; // deny → SESSION_ABORT(auth_failed)
    return {
      baseDir: `/srv/uploads/${user.id}`, // per-user home
      targetDir: String((req.meta as any)?.scope ?? "inbox"),
      maxTotalBytes: 512 * 2 ** 20,
      maxFiles: 20_000,
      context: { userId: user.id },
    };
  },
});
```

### The two universal primitives

```ts
await mfup.handle(req, res /*, url? */); // → boolean: HTTP (data/probe/publish/health)
mfup.upgrade(req, socket, head);         // → boolean: the /mfup/control WebSocket
```

`handle` covers every HTTP endpoint; `upgrade` claims the control-channel
WebSocket (an upgrade never reaches ordinary request handlers — this is why
the contract is a pair). Both return `false` untouched for foreign paths.

### Express

```ts
const app = express();
app.use("/api/uploads", mfup.middleware); // (req, res, next) sugar
const server = app.listen(8090);
mfup.attach(server);                      // wires the WebSocket upgrade
```

Browser side: `new MfupSession({ serverUrl: "/api/uploads" })`.

### Vite (dev server as the upload backend)

```ts
// vite.config.ts
import { mfupDev } from "@mfup/server/vite";

export default defineConfig({
  plugins: [mfupDev({ baseDir: "./uploads" })],
});
```

### Bare node:http

```ts
const server = http.createServer(async (req, res) => {
  if (await mfup.handle(req, res)) return;
  res.statusCode = 404;
  res.end();
});
mfup.attach(server);
```

### Standalone

```bash
MFUP_BASE_DIR=/srv/uploads MFUP_PORT=8070 npx mfup-server
```

Env-var parity with `python -m mfup_fastapi` (see `bin/mfup-server.mjs`).

## Session store

```ts
createMfup({ baseDir, store: "memory" });                  // default
createMfup({ baseDir, store: "redis://localhost:6379/0" }); // multi-worker
createMfup({ baseDir, store: myCustomSessionStore });       // your own
```

- **memory** (default) — right for the typical single-process deployment.
  After a restart the engine re-discovers live sessions by scanning
  `baseDir` for staging directories (each carries its own SQLite journal
  with expiry/auth state), so uploads survive restarts without any external
  service.
- **redis://…** — full parity with the Python server: cross-worker lazy
  resume/failover, scan-free cleanup. Same key layout (`mfup:sessions` zset +
  `mfup:meta:{sid}` hashes), so Node and Python servers are interchangeable
  behind one Redis. Requires `npm install redis`.

## Hooks

Identical contracts to the Python server (camelCased):

| Hook | Fires | Return |
|---|---|---|
| `authorize(req)` | once per HELLO, before session creation | `AuthResult` allows (quotas, per-user `baseDir`, `targetDir` rewrite, `context`); `null` denies |
| `mapFile(req)` | once per file at publish | `string` = new path relative to `targetDir`; `null` = keep client layout |
| `onCommitted(ev)` | right after commit | `"publish"` = server publishes immediately; `null` = client-driven |

## Programmatic surface

```ts
mfup.engine.publish(sessionId); // typed errors: SessionNotFound | NotCommitted | …
mfup.engine.sweep();            // manual expired-session sweep
await mfup.ready();             // startup (store connect, recovery, sweeper)
await mfup.close();             // graceful shutdown
```

## Protocol

MFUP/2: a WebSocket control channel (JSON) + HTTP data legs carrying
length-prefixed binary frames with per-chunk CRC-32C, epoch/leg/seq fencing,
resumable sessions journaled in per-session SQLite, staging → atomic
`rename()` publish, and interactive conflict resolution (`ASK`/`ACTION`)
that never pauses the transfer. Full spec: `docs/FULL.md` in the
[repository](https://github.com/n0isy/mfup).

MIT
