import type { IncomingMessage, ServerResponse, Server } from "node:http";
import * as fs from "node:fs";
import { Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import Busboy from "busboy";
import { WebSocketServer, WebSocket } from "ws";
import { Engine, type Options, type Batch } from "./engine.js";
import { ProtocolError, check, integer, normalizeError } from "./store.js";

function json(res: ServerResponse, status: number, value: unknown) {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(value));
}
async function body(req: IncomingMessage) {
  let bytes = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    bytes += chunk.length;
    check(bytes <= 262144, "body_too_large", 413);
    chunks.push(chunk);
  }
  try {
    const data = JSON.parse(Buffer.concat(chunks).toString() || "{}");
    check(data && typeof data === "object" && !Array.isArray(data), "bad_json");
    return data;
  } catch {
    throw new ProtocolError(400, "bad_json");
  }
}
export class MfupServer {
  readonly engine: Engine;
  readonly prefix: string;
  readonly sockets = new WebSocketServer({
    noServer: true,
    maxPayload: 262144,
  });
  constructor(options: Options) {
    this.prefix = (options.prefix ?? "").replace(/\/$/, "");
    check(
      this.prefix === "" || /^\/(?:[A-Za-z0-9_-]+\/?)+$/.test(this.prefix),
      "bad_prefix",
    );
    this.engine = new Engine(options);
  }
  async handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (this.prefix && !url.pathname.startsWith(this.prefix + "/"))
      return false;
    url.pathname = url.pathname.slice(this.prefix.length);
    const pieces = url.pathname.split("/").filter(Boolean);
    if (pieces[0] !== "mfup") return false;
    let sessionId: string | undefined;
    let phase = "create";
    try {
      if (url.pathname === "/mfup/health" && req.method === "GET") {
        json(res, 200, { protocol: "MFUP/3", backend: "node" });
        return true;
      }
      if (url.pathname === "/mfup/sessions" && req.method === "POST") {
        const headers = Object.fromEntries(
          Object.entries(req.headers).map(([k, v]) => [
            k,
            Array.isArray(v) ? v.join(",") : (v ?? ""),
          ]),
        );
        json(
          res,
          201,
          await this.engine.create(await body(req), headers, {
            client: `${req.socket.remoteAddress ?? ""}:${req.socket.remotePort ?? ""}`,
            query: Object.fromEntries(url.searchParams),
          }),
        );
        return true;
      }
      check(pieces[1] === "sessions" && pieces.length >= 3, "not_found", 404);
      const id = pieces[2],
        token = (req.headers.authorization ?? "").replace(/^Bearer /, "");
      this.engine.authenticate(id, token);
      sessionId = id;
      const action = pieces[3] ?? "";
      phase = action === "batches" ? "upload" : action || "status";
      if (req.method === "GET") {
        if (!action) json(res, 200, this.engine.snapshot(id));
        else if (action === "files")
          json(
            res,
            200,
            this.engine.resumePage(
              id,
              url.searchParams.get("after") ?? "",
              Number(url.searchParams.get("limit") ?? 256),
            ),
          );
        else if (action === "batches" && pieces[4]) {
          const receipt = this.engine.receipt(id, pieces[4]);
          check(receipt, "not_found", 404);
          json(res, 200, receipt);
        } else throw new ProtocolError(404, "not_found");
      } else if (req.method === "POST") {
        if (action === "batches" && pieces[4])
          json(res, 200, await this.receive(req, id, pieces[4]));
        else if (action === "resume") {
          await body(req);
          json(res, 200, await this.engine.resume(id));
        } else if (action === "commit")
          json(res, 200, await this.engine.commit(id, await body(req)));
        else if (action === "publish") {
          await body(req);
          json(res, 200, await this.engine.publishFromClient(id));
        } else if (action === "cancel") {
          await body(req);
          json(res, 200, await this.engine.cancel(id));
        } else if (action === "properties") {
          json(res, 200, await this.engine.setProperties(id, await body(req)));
        } else if (action === "answers") {
          const answer = await body(req);
          json(
            res,
            200,
            await this.engine.answer(id, answer.id, answer.choice),
          );
        } else throw new ProtocolError(404, "not_found");
      } else throw new ProtocolError(405, "method_not_allowed");
    } catch (error) {
      req.resume();
      const e = this.engine.recordFailure(sessionId ?? "", error, phase);
      json(res, e.status, {
        error: e.code,
        message: e.message,
        retryable: e.retryable,
        phase: e.phase,
      });
    }
    return true;
  }
  private async receive(req: IncomingMessage, id: string, batchId: string) {
    check(/^\d+$/.test(String(req.headers["x-mfup-epoch"] ?? "")), "bad_epoch");
    const batch = await this.engine.begin(
      id,
      integer(Number(req.headers["x-mfup-epoch"])),
      batchId,
      () => req.destroy(),
    );
    req.setTimeout(30000, () => req.destroy(new Error("request_timeout")));
    let parser: ReturnType<typeof Busboy> | undefined;
    const writes: Promise<void>[] = [];
    try {
      // Busboy emits partsLimit on reaching the count, including the final boundary.
      try {
        parser = Busboy({
          headers: req.headers,
          highWaterMark: 256 * 1024,
          fileHwm: 256 * 1024,
          limits: {
            fields: 1,
            fieldSize: 262144,
            files: this.engine.limits.maxParts,
            parts: this.engine.limits.maxParts + 2,
            headerPairs: 32,
          },
        });
      } catch {
        throw new ProtocolError(400, "bad_multipart");
      }
      let prepared: Promise<unknown> | undefined,
        manifestSeen = false,
        count = 0,
        total = 0,
        failure: unknown = null,
        cached: unknown = null;
      const streams = new Set<Writable>();
      const fail = (error: unknown) => {
        failure ??= error;
        for (const stream of streams) stream.destroy(error as Error);
        parser!.destroy(error as Error);
      };
      parser.on("field", (name, value, info) => {
        if (name !== "manifest" || manifestSeen || info.valueTruncated) {
          fail(new ProtocolError(400, "bad_manifest"));
          return;
        }
        manifestSeen = true;
        prepared = (async () => {
          let data: unknown;
          try {
            data = JSON.parse(value);
          } catch {
            throw new ProtocolError(400, "bad_manifest");
          }
          cached = await this.engine.prepare(batch, data);
        })();
        void prepared.catch(fail);
      });
      parser.on("file", (name, file) => {
        // A rejected part can be destroyed before a pipeline is attached.
        file.on("error", () => {});
        const index = count++;
        if (!manifestSeen || name !== String(index)) {
          file.resume();
          fail(new ProtocolError(400, "bad_part"));
          return;
        }
        const work = (async () => {
          await prepared;
          if (cached) {
            for await (const _ of file) {
            }
            return;
          }
          const entry = batch.entries?.[index];
          check(entry, "unexpected_part");
          let length = 0;
          const output = entry.skip
            ? new Writable({
                write(_chunk, _encoding, done) {
                  done();
                },
              })
            : fs.createWriteStream(entry.filePath, {
                flags: "r+",
                start: entry.item[3],
                highWaterMark: 256 * 1024,
              });
          streams.add(output);
          file.on("data", (chunk: Buffer) => {
            length += chunk.length;
            if (length > entry.item[4])
              fail(new ProtocolError(400, "part_size_mismatch"));
          });
          try {
            await pipeline(file, output);
            check(length === entry.item[4], "part_size_mismatch");
          } finally {
            streams.delete(output);
          }
        })();
        writes.push(work);
        void work.catch(fail);
      });
      for (const event of ["partsLimit", "filesLimit", "fieldsLimit"] as const)
        parser.on(event, () => fail(new ProtocolError(413, "too_many_parts")));
      const received = new Promise<void>((resolve, reject) => {
        parser!.on("close", () => (failure ? reject(failure) : resolve()));
        parser!.on("error", reject);
      });
      // Backpressure is provided by the multipart parser and each file stream.
      req.on("data", countBody);
      function countBody(chunk: Buffer) {
        total += chunk.length;
        if (total > thisLimit) fail(new ProtocolError(413, "body_too_large"));
      }
      const thisLimit = this.engine.limits.batchBytes + 1024 * 1024;
      const abort = () => fail(new ProtocolError(400, "incomplete_request"));
      req.once("aborted", abort);
      req.pipe(parser);
      try {
        await received;
        await prepared;
        await Promise.all(writes);
      } finally {
        req.off("data", countBody);
        req.off("aborted", abort);
        req.unpipe(parser);
      }
      check(manifestSeen, "bad_manifest");
      if (cached) return cached;
      check(count === batch.manifest!.files.length, "missing_part");
      return await this.engine.complete(batch);
    } catch (error) {
      parser?.destroy();
      await Promise.allSettled(writes);
      if (error instanceof ProtocolError) throw error;
      if (
        /Unexpected end of|Malformed part header|Missing Content-Type/i.test(
          (error as Error).message,
        )
      )
        throw new ProtocolError(400, "incomplete_multipart");
      throw normalizeError(error, "upload");
    } finally {
      req.setTimeout(0);
      this.engine.end(batch);
    }
  }
  attach(server: Server) {
    const upgrade = (
      req: IncomingMessage,
      socket: import("node:stream").Duplex,
      head: Buffer,
    ) => {
      if (
        new URL(req.url ?? "/", "http://localhost").pathname !==
        `${this.prefix}/mfup/control`
      )
        return;
      this.sockets.handleUpgrade(req, socket, head, (ws) => {
        let unsubscribe: (() => unknown) | undefined;
        const timer = setTimeout(
          () => ws.close(1008, "subscribe_required"),
          5000,
        );
        ws.once("message", (raw) => {
          try {
            const msg = JSON.parse(raw.toString());
            check(msg.type === "subscribe", "bad_message");
            this.engine.authenticate(msg.id, msg.token);
            clearTimeout(timer);
            unsubscribe = this.engine.subscribe(msg.id, (state) => {
              if (ws.readyState === WebSocket.OPEN)
                ws.send(JSON.stringify(state));
            });
          } catch {
            ws.close(1008, "denied");
          }
        });
        ws.once("close", () => {
          clearTimeout(timer);
          unsubscribe?.();
        });
        ws.on("error", () => {});
      });
    };
    server.on("upgrade", upgrade);
    return () => server.off("upgrade", upgrade);
  }
  async close() {
    for (const ws of this.sockets.clients) ws.terminate();
    this.sockets.close();
    await this.engine.close();
  }
}
export function createMfup(options: Options) {
  return new MfupServer(options);
}
