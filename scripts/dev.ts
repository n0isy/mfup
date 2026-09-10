import {
  createServer,
  request as proxyRequest,
  type ServerResponse,
} from "node:http";
import { createReadStream, watch } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { createMfup } from "../packages/server/src/index.js";
import { context } from "esbuild";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

const root = fileURLToPath(new URL("../", import.meta.url));
const base = path.resolve(process.env.MFUP_DATA_DIR ?? path.join(root, "data"));
const python = process.env.MFUP_BACKEND_URL;
const live = process.env.MFUP_TEST !== "1";
const reloadClients = new Set<ServerResponse>();
const reload = () => {
  for (const response of reloadClients) response.write("data: update\n\n");
};
const builder = live
  ? await context({
      entryPoints: [path.join(root, "demo/app.ts")],
      bundle: true,
      format: "esm",
      target: "es2022",
      outfile: path.join(root, "demo/dist/app.js"),
      sourcemap: true,
      plugins: [
        {
          name: "reload",
          setup(build) {
            build.onEnd((result) => {
              if (!result.errors.length) reload();
            });
          },
        },
      ],
    })
  : null;
if (builder) await builder.watch();
const watcher = live
  ? watch(path.join(root, "demo"), async (_event, file) => {
      if (file && ["index.html", "style.css"].includes(String(file))) {
        await fs.copyFile(
          path.join(root, "demo", String(file)),
          path.join(root, "demo/dist", String(file)),
        );
        reload();
      }
    })
  : null;
const app = python
  ? null
  : createMfup({
      baseDir: base,
      authorize: () => ({}),
      limits: process.env.MFUP_PART_BYTES
        ? {
            partBytes: Number(process.env.MFUP_PART_BYTES),
            batchBytes: Number(process.env.MFUP_BATCH_BYTES ?? 33554432),
            maxParts: Number(process.env.MFUP_MAX_PARTS ?? 128),
          }
        : undefined,
    });
const types: Record<string, string> = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".map": "application/json",
};
async function files(
  dir: string,
  parent = "",
): Promise<{ path: string; size: number; sha256: string }[]> {
  const out: { path: string; size: number; sha256: string }[] = [];
  for (const entry of await fs
    .readdir(dir, { withFileTypes: true })
    .catch(() => [])) {
    const rel = parent + entry.name,
      full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await files(full, rel + "/")));
    else {
      const bytes = await fs.readFile(full);
      out.push({
        path: rel,
        size: bytes.length,
        sha256: createHash("sha256").update(bytes).digest("hex"),
      });
    }
  }
  return out.sort((a, b) => a.path.localeCompare(b.path));
}
const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (
      process.env.MFUP_TEST === "1" &&
      req.headers["x-mfup-test-slow"] === "1"
    ) {
      const headers = { ...req.headers };
      delete headers["x-mfup-test-slow"];
      const upstream = proxyRequest(
        new URL(req.url!, `http://127.0.0.1:${process.env.PORT ?? 3000}`),
        { method: req.method, headers },
        (response) => {
          res.writeHead(response.statusCode ?? 502, response.headers);
          response.pipe(res);
        },
      );
      upstream.on("error", () => {
        if (!res.headersSent) res.writeHead(502);
        res.end();
      });
      res.on("close", () => upstream.destroy());
      const paced = new Transform({
        transform(chunk, _encoding, done) {
          setTimeout(() => done(null, chunk), 20);
        },
      });
      void pipeline(req, paced, upstream).catch(() => upstream.destroy());
      return;
    }
    if (live && url.pathname === "/__reload") {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
      });
      res.write("retry: 500\n\n");
      reloadClients.add(res);
      req.on("close", () => reloadClients.delete(res));
      return;
    }
    if (python && url.pathname.startsWith("/mfup/")) {
      const upstream = proxyRequest(
        new URL(req.url!, python),
        { method: req.method, headers: req.headers },
        (response) => {
          res.writeHead(response.statusCode ?? 502, response.headers);
          response.pipe(res);
        },
      );
      upstream.on("error", () => {
        res.writeHead(502);
        res.end();
      });
      req.on("aborted", () => upstream.destroy());
      req.pipe(upstream);
      return;
    }
    if (app && (await app.handle(req, res))) return;
    if (process.env.MFUP_TEST === "1" && url.pathname === "/test/result") {
      const target = url.searchParams.get("target") ?? "";
      if (!/^[A-Za-z0-9_-]+$/.test(target)) {
        res.writeHead(400);
        res.end();
        return;
      }
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify(await files(path.join(base, "published", target))),
      );
      return;
    }
    if (
      process.env.MFUP_TEST === "1" &&
      url.pathname === "/test/seed" &&
      req.method === "POST"
    ) {
      let input = "";
      for await (const chunk of req) {
        input += chunk;
        if (input.length > 262144) throw new Error("Too large");
      }
      const data = JSON.parse(input);
      if (!/^[A-Za-z0-9_-]+$/.test(data.target) || !/^[-\w.]+$/.test(data.name))
        throw new Error("Bad path");
      const dir = path.join(base, "published", data.target);
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(path.join(dir, data.name), data.content);
      res.setHeader("content-type", "application/json");
      res.end("{}");
      return;
    }
    const rel =
      url.pathname === "/"
        ? "index.html"
        : decodeURIComponent(url.pathname.slice(1));
    const filename = path.resolve(root, "demo/dist", rel);
    if (!filename.startsWith(path.join(root, "demo/dist") + path.sep))
      throw new Error("Not found");
    const stat = await fs.stat(filename);
    if (!stat.isFile()) throw new Error("Not found");
    res.setHeader(
      "content-type",
      types[path.extname(filename)] ?? "application/octet-stream",
    );
    if (live && path.extname(filename) === ".html") {
      res.end(
        (await fs.readFile(filename, "utf8")).replace(
          "</body>",
          `<script>new EventSource('/__reload').onmessage=()=>location.reload()</script></body>`,
        ),
      );
    } else createReadStream(filename).pipe(res);
  } catch {
    if (!res.headersSent) res.writeHead(404);
    res.end();
  }
});
if (app) app.attach(server);
else
  server.on("upgrade", async (req, socket, head) => {
    // Native test runner uses this gateway for one origin on every OS.
    const { connect } = await import("node:net");
    const target = new URL(python!);
    const remote = connect(Number(target.port || 80), target.hostname, () => {
      remote.write(
        `${req.method} ${req.url} HTTP/${req.httpVersion}\r\n` +
          req.rawHeaders.reduce(
            (s, v, i) => s + (i % 2 ? `${v}\r\n` : `${v}: `),
            "",
          ) +
          "\r\n",
      );
      if (head.length) remote.write(head);
      socket.pipe(remote);
      remote.pipe(socket);
    });
    remote.on("error", () => socket.destroy());
    socket.on("error", () => remote.destroy());
    socket.on("close", () => remote.destroy());
  });
server.listen(
  Number(process.env.PORT ?? 3000),
  process.env.HOST ?? "0.0.0.0",
  () => console.log(`MFUP/3 demo ready on ${process.env.PORT ?? 3000}`),
);
async function close() {
  for (const response of reloadClients) response.end();
  watcher?.close();
  await builder?.dispose();
  server.close();
  server.closeAllConnections();
  await app?.close();
  process.exit(0);
}
process.on("SIGINT", close);
process.on("SIGTERM", close);
