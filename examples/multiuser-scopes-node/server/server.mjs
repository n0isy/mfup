/**
 * The multiuser-scopes example on the NODE server — same policy as the
 * FastAPI version (examples/multiuser-scopes/server/app.py), written the
 * way an express consumer would:
 *
 *   - users auto-created via a `demo_uid` cookie (GET /api/whoami mints it);
 *   - three upload zones, the SCOPE travels as session meta;
 *   - ONE authorize hook turns (cookie, meta.scope) into a server-owned
 *     destination: files always land at <DATA_DIR>/<uid>/<scope>/…;
 *   - MFUP integration is two lines: app.use(prefix, mfup.middleware) for
 *     HTTP + mfup.attach(server) for the control WebSocket.
 *
 * No Redis: the default in-memory session store. On a restart the engine
 * re-discovers live sessions by scanning DATA_DIR for staging directories.
 */

import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import express from "express";
import { createMfup } from "@mfup/server";

const DATA_DIR = path.resolve(process.env.DEMO_DATA_DIR ?? "./data");
const SCOPES = ["workspace", "scratch", "uploads"];
const COOKIE_NAME = "demo_uid";
const PORT = Number.parseInt(process.env.PORT ?? "8091", 10);

/** Parse the demo_uid cookie off a raw Cookie header (same rules as the
 * Python example: alnum, 8–64 chars). Works for HTTP requests and for the
 * WebSocket handshake headers alike. */
function uidFromCookieHeader(header) {
  if (!header) return null;
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    if (part.slice(0, eq).trim() !== COOKIE_NAME) continue;
    const v = part.slice(eq + 1).trim();
    if (/^[a-zA-Z0-9]{8,64}$/.test(v)) return v;
  }
  return null;
}

// ---------------------------------------------------------------------------
// MFUP: the whole multiuser/scope policy is this one authorize hook.
// ---------------------------------------------------------------------------
const mfup = createMfup({
  baseDir: DATA_DIR,
  store: process.env.REDIS_URL ?? "memory",
  authorize: async (req) => {
    const uid = uidFromCookieHeader(req.headers["cookie"]);
    if (!uid) return null; // the SPA calls /api/whoami first
    const scope =
      req.meta && typeof req.meta === "object" ? /** @type {any} */ (req.meta).scope : undefined;
    if (!SCOPES.includes(scope)) return null;
    return {
      baseDir: path.join(DATA_DIR, uid), // per-user home (staging lives inside)
      targetDir: scope, // SERVER owns the layout: <uid>/<scope>/
      maxTotalBytes: 512 * 2 ** 20, // 512 MiB per session
      maxFiles: 20_000,
      context: { uid, scope },
    };
  },
});

// ---------------------------------------------------------------------------
// The consumer app around it.
// ---------------------------------------------------------------------------
const app = express();

app.get("/api/whoami", async (req, res) => {
  let uid = uidFromCookieHeader(req.headers.cookie);
  if (!uid) {
    uid = crypto.randomBytes(8).toString("hex"); // 16 hex chars — passes the check
    res.cookie(COOKIE_NAME, uid, {
      maxAge: 30 * 24 * 3600 * 1000,
      httpOnly: true,
      sameSite: "lax",
    });
  }
  await fs.mkdir(path.join(DATA_DIR, uid), { recursive: true });
  res.json({ user_id: uid, scopes: SCOPES, server: "@mfup/server (node)" });
});

app.get("/api/files/:scope", async (req, res) => {
  const uid = uidFromCookieHeader(req.headers.cookie);
  const scope = req.params.scope;
  if (!uid || !SCOPES.includes(scope)) {
    res.status(403).json({ error: "unknown user or scope" });
    return;
  }
  const dir = path.join(DATA_DIR, uid, scope);
  let entries = [];
  try {
    const dirents = await fs.readdir(dir, { withFileTypes: true });
    entries = await Promise.all(
      dirents.map(async (d) => ({
        name: d.name,
        dir: d.isDirectory(),
        size: d.isDirectory() ? null : (await fs.stat(path.join(dir, d.name))).size,
      })),
    );
    entries.sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1));
  } catch {
    /* scope dir not created yet — empty listing */
  }
  res.json({ scope, entries });
});

// MFUP/2 under a prefix: HTTP endpoints via middleware…
app.use("/api/mfup", mfup.middleware);

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`multiuser-scopes (node) listening on :${PORT} — data at ${DATA_DIR}`);
});
// …and the control-channel WebSocket via the upgrade hook.
mfup.attach(server);

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    server.close();
    mfup.close().finally(() => process.exit(0));
  });
}
