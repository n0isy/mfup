import { createServer } from "node:http";
import { createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createMfup,
  relativePath,
  ProtocolError,
  publishedDirectory,
} from "@mfup/server";

export const SCOPES = ["workspace", "scratch", "uploads"];
const COOKIE = "mfup3_node_user";
function identity(headers) {
  const token = String(headers.cookie ?? "")
    .split(";")
    .map((p) => p.trim())
    .find((p) => p.startsWith(COOKIE + "="))
    ?.slice(COOKIE.length + 1);
  return /^[a-f0-9]{64}$/.test(token ?? "")
    ? createHash("sha256").update(token).digest("hex").slice(0, 32)
    : null;
}
function json(res, status, value) {
  res.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(value));
}

/** @param {Omit<Partial<import('@mfup/server').Options>, 'authorize'> & {scopeRoots?: Record<string, string>}} [config] */
export function createExample({
  baseDir = process.env.DEMO_DATA_DIR ?? "./data/example-node",
  scopeRoots = {},
  ...options
} = {}) {
  const base = path.resolve(baseDir);
  // Application policy: both uploads and listings resolve the same scope roots.
  const rootFor = (scope) => path.resolve(scopeRoots[scope] ?? base);
  /** @type {import('@mfup/server').Options['authorize']} */
  const authorize = ({ headers, meta }) => {
    const uid = identity(headers),
      scope =
        meta && typeof meta === "object" && "scope" in meta ? meta.scope : null;
    if (!uid || !SCOPES.includes(scope)) return null;
    return {
      baseDir: rootFor(scope),
      targetDir: `${uid}/${scope}`,
      maxTotalBytes: 512 * 2 ** 20,
      maxFiles: 20000,
      context: { uid, scope },
    };
  };
  const mfup = createMfup({
    ...options,
    baseDir: base,
    authorize,
    prefix: "/api",
  });
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, "http://localhost");
      if (req.method === "GET" && url.pathname === "/api/whoami") {
        let uid = identity(req.headers);
        if (!uid) {
          const token = randomBytes(32).toString("hex");
          uid = identity({ cookie: COOKIE + "=" + token });
          res.setHeader(
            "set-cookie",
            `${COOKIE}=${token}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax${process.env.COOKIE_SECURE === "1" ? "; Secure" : ""}`,
          );
        }
        json(res, 200, { user_id: uid, scopes: SCOPES, backend: "node" });
        return;
      }
      const route = /^\/api\/(files|file)\/([^/]+)$/.exec(url.pathname);
      if (req.method === "GET" && route) {
        const uid = identity(req.headers),
          scope = route[2];
        if (!uid || !SCOPES.includes(scope)) {
          json(res, 403, { error: "unknown_user_or_scope" });
          return;
        }
        const rel = url.searchParams.get("path") ?? "";
        if (rel) relativePath(rel);
        const root = publishedDirectory(rootFor(scope), `${uid}/${scope}`),
          target = path.join(root, rel);
        if (route[1] === "file") {
          if (!rel || !(await fs.stat(target)).isFile()) {
            json(res, 404, { error: "not_found" });
            return;
          }
          res.writeHead(200, {
            "content-type": "application/octet-stream",
            "cache-control": "no-store",
            "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(rel))}`,
          });
          const stream = createReadStream(target);
          stream.on("error", () => res.destroy());
          stream.pipe(res);
          return;
        }
        const items = await fs
          .readdir(target, { withFileTypes: true })
          .catch((error) => {
            if (error.code === "ENOENT") return [];
            throw error;
          });
        const entries = await Promise.all(
          items.map(async (item) => ({
            name: item.name,
            dir: item.isDirectory(),
            size: item.isDirectory()
              ? null
              : (await fs.stat(path.join(target, item.name))).size,
          })),
        );
        entries.sort((a, b) =>
          a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1,
        );
        json(res, 200, { scope, path: rel, entries });
        return;
      }
      if (url.pathname.startsWith("/api/mfup/")) {
        await mfup.handle(req, res);
        return;
      }
      json(res, 404, { error: "not_found" });
    } catch (error) {
      if (res.headersSent) {
        res.destroy();
        return;
      }
      json(
        res,
        error instanceof ProtocolError
          ? error.status
          : error.code === "ENOENT"
            ? 404
            : 500,
        {
          error: error instanceof ProtocolError ? error.code : "request_failed",
        },
      );
    }
  });
  mfup.attach(server);
  const close = async () => {
    await mfup.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  };
  return { server, mfup, close };
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const app = createExample();
  const port = Number(process.env.PORT ?? 3001);
  app.server.listen(port, process.env.HOST ?? "0.0.0.0", () =>
    console.log(`MFUP/3 scopes (Node) on :${port}`),
  );
  let closing = false;
  for (const signal of ["SIGINT", "SIGTERM"])
    process.on(signal, async () => {
      if (closing) return;
      closing = true;
      await app.close();
      process.exit(0);
    });
}
