#!/usr/bin/env node
/**
 * Standalone MFUP/2 server — config from environment (env-var parity with
 * the Python `python -m mfup_fastapi`):
 *
 *     MFUP_BASE_DIR=/srv/uploads MFUP_PORT=8070 npx mfup-server
 *
 * | Env var                  | Default             |
 * |--------------------------|---------------------|
 * | MFUP_HOST                | 0.0.0.0             |
 * | MFUP_PORT                | 8070                |
 * | MFUP_BASE_DIR            | <tmp>/mfup-uploads  |
 * | MFUP_STORE               | memory              |  ("memory" | redis:// URL)
 * | REDIS_URL                | —                   |  (fallback for MFUP_STORE)
 * | MFUP_SESSION_RESUME_TTL  | 3600                |
 * | MFUP_LEG_IDLE_TIMEOUT    | 60                  |
 * | MFUP_MAX_CHUNK_BYTES     | 262144              |
 * | MFUP_MAX_OPEN_FILES      | 1                   |
 * | MFUP_MAX_PENDING_FILES   | 64                  |
 * | MFUP_SWEEP_INTERVAL      | 300                 |
 * | MFUP_STAGING_PREFIX      | .incoming           |
 * | MFUP_RECONCILE_EVERY     | 4                   |
 * | MFUP_ORPHAN_GRACE        | 600                 |
 * | MFUP_SCAN_DEPTH          | 2                   |
 * | MFUP_MAX_BUFFERED_BODY   | 16777216            |
 * | MFUP_MAX_META_BYTES      | 16384               |
 * | MFUP_ADMIN_TOKEN         | "" (admin disabled) |
 * | MFUP_AUTHORIZE           | —  "./hooks.mjs#authorize" (module#export)   |
 * | MFUP_MAP_FILE            | —                   |
 * | MFUP_ON_COMMITTED        | —                   |
 */

import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";

import { createMfup } from "../dist/index.js";

const env = process.env;

function intEnv(name, def) {
  const v = env[name];
  return v === undefined ? def : Number.parseInt(v, 10);
}

/** Load "path/to/module.mjs#exportName" (or a bare specifier). */
async function loadHook(ref) {
  if (!ref) return undefined;
  const hash = ref.lastIndexOf("#");
  const spec = hash === -1 ? ref : ref.slice(0, hash);
  const name = hash === -1 ? "default" : ref.slice(hash + 1);
  const target =
    spec.startsWith(".") || path.isAbsolute(spec)
      ? pathToFileURL(path.resolve(spec)).href
      : spec;
  const mod = await import(target);
  const fn = mod[name];
  if (typeof fn !== "function") {
    throw new Error(`hook ${ref}: export ${JSON.stringify(name)} is not a function`);
  }
  return fn;
}

const mfup = createMfup({
  baseDir: env.MFUP_BASE_DIR ?? path.join(os.tmpdir(), "mfup-uploads"),
  store: env.MFUP_STORE ?? env.REDIS_URL ?? "memory",
  sessionResumeTtl: intEnv("MFUP_SESSION_RESUME_TTL", 3600),
  legIdleTimeout: intEnv("MFUP_LEG_IDLE_TIMEOUT", 60),
  maxChunkBytes: intEnv("MFUP_MAX_CHUNK_BYTES", 262144),
  maxOpenFiles: intEnv("MFUP_MAX_OPEN_FILES", 1),
  maxPendingFiles: intEnv("MFUP_MAX_PENDING_FILES", 64),
  sweepInterval: intEnv("MFUP_SWEEP_INTERVAL", 300),
  stagingPrefix: env.MFUP_STAGING_PREFIX ?? ".incoming",
  reconcileEvery: intEnv("MFUP_RECONCILE_EVERY", 4),
  orphanGraceSeconds: intEnv("MFUP_ORPHAN_GRACE", 600),
  scanDepth: intEnv("MFUP_SCAN_DEPTH", 2),
  maxBufferedBody: intEnv("MFUP_MAX_BUFFERED_BODY", 16 * 1024 * 1024),
  maxMetaBytes: intEnv("MFUP_MAX_META_BYTES", 16384),
  adminToken: env.MFUP_ADMIN_TOKEN ?? "",
  authorize: await loadHook(env.MFUP_AUTHORIZE),
  mapFile: await loadHook(env.MFUP_MAP_FILE),
  onCommitted: await loadHook(env.MFUP_ON_COMMITTED),
});

const server = http.createServer((req, res) => {
  mfup.handle(req, res).then((handled) => {
    if (!handled) {
      res.statusCode = 404;
      res.setHeader("content-type", "application/json");
      res.end('{"error":"not found"}');
    }
  }).catch((err) => {
    console.error("request failed:", err);
    try {
      res.statusCode = 500;
      res.end('{"error":"internal_error"}');
    } catch {}
  });
});
mfup.attach(server);

const host = env.MFUP_HOST ?? "0.0.0.0";
const port = intEnv("MFUP_PORT", 8070);

await mfup.ready();
server.listen(port, host, () => {
  console.log(`mfup-server listening on http://${host}:${port} (MFUP/2)`);
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    server.close();
    mfup.close().finally(() => process.exit(0));
  });
}
