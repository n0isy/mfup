/**
 * createMfup — the universal Node handler for MFUP/2.
 *
 * Two primitives cover every framework that exposes Node's HTTP objects:
 *
 *     const mfup = createMfup({ baseDir: "/srv/uploads", authorize });
 *     await mfup.handle(req, res)          // HTTP: data/probe/publish/admin/health
 *     mfup.upgrade(req, socket, head)      // WS:   /mfup/control
 *
 * Sugar:
 *     app.use("/api/uploads", mfup.middleware)   // connect/express style
 *     mfup.attach(server)                        // wires the upgrade event
 *
 * Routing is mount-agnostic: the handler looks for the protocol's fixed
 * "/mfup/*" namespace inside the request path, so an express mount prefix
 * (stripped from req.url) and a raw prefixed server (pass `basePath`) both
 * line up with the browser client's serverUrl.
 *
 * Port of the HTTP/WS half of server/mfup-fastapi/mfup_fastapi/engine.py.
 */

import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";

import { WebSocketServer, type WebSocket } from "ws";

import { createLogger } from "./logger.js";
import type { AuthRequest, AuthResult } from "./hooks.js";
import {
  MfupEngine,
  MfupOptions,
  NotCommitted,
  SessionNotFound,
  TargetEscapes,
  MapFileHookError,
  isSafeTarget,
  resolveOptions,
} from "./engine.js";
import { ConflictError, MappingError } from "./publish.js";
import {
  FrameReader,
  FrameTag,
  PROTOCOL_VERSION,
  CRC32C_IMPL,
  SessionState,
} from "./protocol.js";
import {
  BadStateError,
  BadTokenError,
  LiveSession,
  SessionUnknownError,
  type ControlSocket,
} from "./session.js";
import { stagingDir } from "./storage.js";

const logger = createLogger("mfup.handler");

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

export interface Mfup {
  /** Handle an HTTP request if its path belongs to MFUP. Returns true when
   * the request was handled (response sent or in flight), false when the
   * path is not ours and req/res were not touched. `url` overrides req.url
   * for callers whose framework rewrites it. */
  handle(req: IncomingMessage, res: ServerResponse, url?: string): Promise<boolean>;
  /** Claim a WebSocket upgrade for the control channel. Synchronous verdict:
   * true = claimed (the socket will be served), false = not ours. Wire it:
   * `server.on("upgrade", (r, s, h) => mfup.upgrade(r, s, h))`. */
  upgrade(req: IncomingMessage, socket: Duplex, head: Buffer): boolean;
  /** Connect/express-style middleware: calls next() for non-MFUP paths. */
  middleware(req: IncomingMessage, res: ServerResponse, next?: (err?: unknown) => void): void;
  /** Convenience: subscribes `upgrade` on the server. */
  attach(server: Server): void;
  /** The engine: programmatic publish(), sweep(), registry access. */
  engine: MfupEngine;
  /** Resolves when startup (store connect, recovery, sweeper) finished.
   * Called lazily by handle/upgrade — explicit awaiting is optional. */
  ready(): Promise<void>;
  /** Stop the sweeper, close sockets, close the store. */
  close(): Promise<void>;
}

export function createMfup(options: MfupOptions): Mfup {
  return new MfupImpl(options);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function json(res: ServerResponse, status: number, body: unknown): void {
  try {
    const data = JSON.stringify(body);
    if (!res.headersSent) {
      res.statusCode = status;
      res.setHeader("content-type", "application/json");
    }
    res.end(data);
  } catch {
    /* client gone — nothing to do */
  }
}

function flattenHeaders(req: IncomingMessage): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === "string") out[k] = v;
    else if (Array.isArray(v)) out[k] = v.join(", ");
  }
  return out;
}

function decodeSegment(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/** FIFO of parsed control messages; `null` value = socket closed. */
class MsgQueue {
  private queue: unknown[] = [];
  private waiters: ((v: unknown | null) => void)[] = [];
  private ended = false;

  push(msg: unknown): void {
    const w = this.waiters.shift();
    if (w) w(msg);
    else this.queue.push(msg);
  }

  end(): void {
    this.ended = true;
    for (const w of this.waiters.splice(0)) w(null);
  }

  next(): Promise<unknown | null> {
    if (this.queue.length > 0) return Promise.resolve(this.queue.shift()!);
    if (this.ended) return Promise.resolve(null);
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class MfupImpl implements Mfup {
  readonly engine: MfupEngine;
  private readonly cfg;
  private readonly wss = new WebSocketServer({ noServer: true });
  private readonly liveSockets = new Set<WebSocket>();
  private readyPromise: Promise<void> | null = null;
  private closed = false;

  constructor(options: MfupOptions) {
    this.cfg = resolveOptions(options);
    this.engine = new MfupEngine(this.cfg);
    // Stable identities so consumers can pass them around directly.
    this.handle = this.handle.bind(this);
    this.upgrade = this.upgrade.bind(this);
    this.middleware = this.middleware.bind(this);
    this.attach = this.attach.bind(this);
  }

  ready(): Promise<void> {
    if (this.readyPromise === null) {
      this.readyPromise = this.engine.startup();
    }
    return this.readyPromise;
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const ws of this.liveSockets) {
      try {
        ws.terminate();
      } catch {
        /* already gone */
      }
    }
    this.liveSockets.clear();
    await this.engine.shutdown();
    this.readyPromise = null;
  }

  // -- routing ---------------------------------------------------------------

  /** Extract the "/mfup/..." (or "/health") tail from a request path. */
  private tailOf(pathname: string): string | null {
    let p = pathname;
    const base = this.cfg.basePath;
    if (base && (p === base || p.startsWith(base + "/"))) {
      p = p.slice(base.length);
      if (!p.startsWith("/")) p = "/" + p;
    }
    const idx = p.indexOf("/mfup/");
    if (idx !== -1) return p.slice(idx);
    if (p === "/health") return "/health";
    return null;
  }

  middleware(req: IncomingMessage, res: ServerResponse, next?: (err?: unknown) => void): void {
    this.handle(req, res)
      .then((handled) => {
        if (!handled && next) next();
      })
      .catch((err) => {
        if (next) next(err);
        else json(res, 500, { error: "internal_error" });
      });
  }

  attach(server: Server): void {
    server.on("upgrade", (req, socket, head) => {
      this.upgrade(req, socket as Duplex, head as Buffer);
    });
  }

  upgrade(req: IncomingMessage, socket: Duplex, head: Buffer): boolean {
    const raw = req.url ?? "/";
    const q = raw.indexOf("?");
    const pathname = q === -1 ? raw : raw.slice(0, q);
    // Upgrade events always carry the FULL path (no express mount stripping),
    // so match the protocol-fixed suffix.
    if (!pathname.endsWith("/mfup/control")) return false;
    if (this.closed) {
      socket.destroy();
      return true;
    }
    void this.ready()
      .then(() => {
        this.wss.handleUpgrade(req, socket, head, (ws) => {
          this.liveSockets.add(ws);
          ws.on("close", () => this.liveSockets.delete(ws));
          void this.controlLoop(ws, req).catch((exc) => {
            logger.error(`Control loop crashed: ${exc}`);
            try {
              ws.close();
            } catch {
              /* already closed */
            }
          });
        });
      })
      .catch((exc) => {
        logger.error(`Upgrade failed during startup: ${exc}`);
        socket.destroy();
      });
    return true;
  }

  async handle(req: IncomingMessage, res: ServerResponse, url?: string): Promise<boolean> {
    const raw = url ?? req.url ?? "/";
    const qIdx = raw.indexOf("?");
    const pathname = qIdx === -1 ? raw : raw.slice(0, qIdx);
    const tail = this.tailOf(pathname);
    if (tail === null) return false;

    await this.ready();
    const query = new URLSearchParams(qIdx === -1 ? "" : raw.slice(qIdx + 1));
    const method = (req.method ?? "GET").toUpperCase();
    const seg = tail.split("/").slice(1).map(decodeSegment); // tail starts with "/"

    // GET /health
    if (tail === "/health" && method === "GET") {
      json(res, 200, { status: "ok", protocol: PROTOCOL_VERSION, crc32c: CRC32C_IMPL });
      return true;
    }

    // POST /mfup/data/{session_id}/{leg_id}
    if (seg[0] === "mfup" && seg[1] === "data" && seg.length === 4 && method === "POST") {
      await this.dataEndpoint(req, res, seg[2], seg[3], query);
      return true;
    }

    // POST /mfup/probe/{session_id}
    if (seg[0] === "mfup" && seg[1] === "probe" && seg.length === 3 && method === "POST") {
      await this.probeEndpoint(req, res, seg[2]);
      return true;
    }

    // /mfup/sessions[...]
    if (seg[0] === "mfup" && seg[1] === "sessions") {
      if (seg.length === 2 && method === "GET") {
        this.listSessions(req, res);
        return true;
      }
      if (seg.length === 3 && method === "GET") {
        this.getSessionStatus(req, res, seg[2]);
        return true;
      }
      if (seg.length === 4 && seg[3] === "publish" && method === "POST") {
        await this.publishEndpoint(req, res, seg[2]);
        return true;
      }
    }

    // POST /mfup/sweep
    if (tail === "/mfup/sweep" && method === "POST") {
      await this.sweepEndpoint(req, res);
      return true;
    }

    // HTTP request to the WebSocket endpoint
    if (tail === "/mfup/control") {
      json(res, 426, { error: "upgrade required" });
      return true;
    }

    json(res, 404, { error: "not found" });
    return true;
  }

  // -- data endpoint -----------------------------------------------------------

  private async dataEndpoint(
    req: IncomingMessage,
    res: ServerResponse,
    sessionId: string,
    legId: string,
    query: URLSearchParams,
  ): Promise<void> {
    const cfg = this.cfg;
    const registry = this.engine.requireRegistry();
    const session = registry.get(sessionId);

    if (session === null) {
      json(res, 410, { error: "session not found" });
      return;
    }

    // Verify the data-channel auth token
    const token = req.headers["x-mfup-token"];
    if (!token || token !== session.resumeToken) {
      json(res, 403, { error: "invalid token" });
      return;
    }

    if (session.legId !== legId) {
      json(res, 409, { error: "stale leg", expected: session.legId });
      return;
    }

    const seq = Number.parseInt(query.get("seq") ?? "", 10);
    if (Number.isNaN(seq)) {
      json(res, 400, { error: "seq_required" });
      return;
    }
    const final = Number.parseInt(query.get("final") ?? "0", 10) || 0;
    const epoch = query.has("epoch") ? Number.parseInt(query.get("epoch") ?? "-1", 10) : -1;

    // Epoch is mandatory: an old client omitting ?epoch= must not silently
    // bypass stale-POST fencing.
    if (Number.isNaN(epoch) || epoch < 0) {
      json(res, 400, { error: "epoch_required" });
      return;
    }

    // Reject requests from stale epochs (old reconnects / retries)
    if (session.epoch !== epoch) {
      logger.warn(
        `Rejected stale epoch for session ${sessionId}: got ${epoch}, current ${session.epoch}`,
      );
      json(res, 409, { error: "stale_epoch", got: epoch, expected: session.epoch });
      return;
    }

    const state = session.state;
    if (state !== SessionState.ACTIVE && state !== SessionState.PAUSED_BY_SERVER) {
      json(res, 409, { error: `session in state ${state}` });
      return;
    }

    // Reject POSTs after final=1 has already been received for this leg
    if (session.finalSeqSeen) {
      logger.warn(`Rejected POST after final for session ${sessionId} leg ${legId} seq=${seq}`);
      json(res, 409, { error: "data_after_final", detail: "final POST already received" });
      return;
    }

    // Validate the sequence number. Advancing is deferred: for buffered
    // (batch) POSTs seq only advances after the body was FULLY processed, so
    // a client may retry a failed POST with the same seq. A duplicate of an
    // already-processed POST then gets seq_mismatch with expected == seq+1,
    // which the client interprets as "already delivered".
    if (!session.validateSeq(seq)) {
      logger.warn(
        `Seq gap/duplicate for session ${sessionId} leg ${legId}: got seq=${seq}, expected=${session.lastDataSeq + 1}`,
      );
      json(res, 409, { error: "seq_mismatch", got: seq, expected: session.lastDataSeq + 1 });
      return;
    }

    // Two body-handling strategies:
    //  - Buffered (atomic): Content-Length known and small enough — read the
    //    whole body first, process after. Either the entire POST is applied
    //    (and seq advances) or none of it is.
    //  - Streaming: chunked/oversized body (the duplex:"half" long POST).
    //    Frames apply as they arrive; a broken stream recovers through
    //    RESUME (new leg, new epoch), never retried by seq.
    const contentLength = req.headers["content-length"];
    const buffered =
      contentLength !== undefined && Number.parseInt(contentLength, 10) <= cfg.maxBufferedBody;

    const reader = new FrameReader();
    let bodyReceived = 0;
    let frameCount = 0;
    let sessionEndSeen = false;
    let errorDetail: string | null = null;

    if (buffered) {
      const chunks: Buffer[] = [];
      let total = 0;
      try {
        for await (const chunk of req) {
          const buf = chunk as Buffer;
          chunks.push(buf);
          total += buf.length;
          if (total > cfg.maxBufferedBody) {
            json(res, 413, { error: "body_too_large" });
            return;
          }
        }
      } catch (exc) {
        // Body never fully arrived — nothing was applied, seq not advanced,
        // the client may retry this POST verbatim.
        logger.warn(
          `Buffered body read failed for session ${sessionId} leg ${legId} seq=${seq}: ${exc}`,
        );
        json(res, 500, { error: "body_read_failed" });
        return;
      }

      // Re-check freshness after the (awaited) body read.
      if (session.legId !== legId || session.epoch !== epoch) {
        json(res, 409, { error: "stale_epoch", got: epoch, expected: session.epoch });
        return;
      }

      const body = Buffer.concat(chunks, total);
      session.db.beginBatch();
      try {
        reader.feed(body);
        const frames = reader.drain();
        for (const frame of frames) {
          await session.processFrame(frame, legId);
          frameCount += 1;
          if (frame.tag === FrameTag.SESSION_END) sessionEndSeen = true;
        }
        bodyReceived = body.length;
      } catch (exc) {
        errorDetail = `${(exc as Error).constructor?.name ?? "Error"}: ${(exc as Error).message ?? exc}`;
        logger.error(
          `Frame processing error for session ${sessionId} leg ${legId} seq=${seq}: ${(exc as Error).stack ?? exc}`,
        );
      } finally {
        session.db.endBatch();
      }

      if (errorDetail === null) {
        session.advanceSeq(seq);
        if (final === 1) session.finalSeqSeen = true;
      }
    } else {
      // Streaming path: advance immediately (no retry-by-seq here).
      session.advanceSeq(seq);
      if (final === 1) session.finalSeqSeen = true;

      session.db.beginBatch();
      let framesSinceFlush = 0;
      try {
        for await (const chunk of req) {
          if (session.legId !== legId || session.epoch !== epoch) {
            logger.warn(`Data stream for stale leg/epoch ${legId} seq=${seq}, aborting read`);
            break;
          }
          reader.feed(chunk as Buffer);
          const frames = reader.drain();
          for (const frame of frames) {
            await session.processFrame(frame, legId);
            frameCount += 1;
            framesSinceFlush += 1;
            if (frame.tag === FrameTag.SESSION_END) sessionEndSeen = true;
          }
          if (framesSinceFlush >= 500) {
            session.db.flush();
            framesSinceFlush = 0;
          }
          bodyReceived += (chunk as Buffer).length;
        }
      } catch (exc) {
        errorDetail = `${(exc as Error).constructor?.name ?? "Error"}: ${(exc as Error).message ?? exc}`;
        logger.error(
          `Data stream error for session ${sessionId} leg ${legId} seq=${seq}: ${(exc as Error).stack ?? exc}`,
        );
      } finally {
        session.db.endBatch();
      }
    }

    logger.info(
      `Data stream ended for session ${sessionId} leg ${legId} seq=${seq}: ` +
        `${bodyReceived} bytes, ${frameCount} frames, final=${final}` +
        (errorDetail ? `, error: ${errorDetail}` : ""),
    );

    // Only attempt commit when final=1 or SESSION_END was in the frames
    let commitResult: { files: number; bytes: number } | null = null;
    if ((final === 1 || sessionEndSeen) && session.state === SessionState.COMMITTING) {
      commitResult = await this.engine.tryCommit(session);
    }

    if (errorDetail) {
      json(res, 500, {
        error: "data_stream_error",
        detail: errorDetail,
        bytes_received: bodyReceived,
      });
      return;
    }

    const resp: Record<string, unknown> = {
      ok: true,
      bytes_received: bodyReceived,
      frames: frameCount,
    };
    if (commitResult !== null) {
      resp.commit = { files: commitResult.files, bytes: commitResult.bytes };
    }
    json(res, 200, resp);
  }

  // -- probe endpoint ----------------------------------------------------------

  private async probeEndpoint(
    req: IncomingMessage,
    res: ServerResponse,
    sessionId: string,
  ): Promise<void> {
    const registry = this.engine.requireRegistry();
    const session = registry.get(sessionId);

    if (session === null) {
      json(res, 410, { error: "session not found" });
      return;
    }

    const token = req.headers["x-mfup-token"];
    if (!token || token !== session.resumeToken) {
      json(res, 403, { error: "invalid token" });
      return;
    }

    const state = session.state;
    if (
      state === SessionState.COMMITTED ||
      state === SessionState.ABORTED ||
      state === SessionState.EXPIRED
    ) {
      json(res, 410, { error: `session in terminal state ${state}` });
      return;
    }

    // Send PROBE_ACK after the first chunk arrives, with its size, so the
    // client can verify real binary data arrived (Firefox stringifies a
    // ReadableStream to 23 bytes of "[object ...]").
    let probeAckSent = false;
    let totalBytes = 0;
    try {
      for await (const chunk of req) {
        totalBytes += (chunk as Buffer).length;
        if (!probeAckSent) {
          probeAckSent = true;
          if (session.ws) {
            try {
              await session.ws.sendJson({
                t: "PROBE_ACK",
                first_chunk_bytes: (chunk as Buffer).length,
              });
            } catch (exc) {
              logger.error(`Failed to send PROBE_ACK for session ${sessionId}: ${exc}`);
            }
          }
        }
      }
    } catch {
      /* client closed the probe stream — fine, report what arrived */
    }

    json(res, 200, { ok: true, total_bytes: totalBytes });
  }

  // -- admin / publish ---------------------------------------------------------

  /** Admin routes require the token to be configured AND presented. */
  private adminDenied(req: IncomingMessage, res: ServerResponse): boolean {
    if (!this.cfg.adminToken) {
      json(res, 403, { error: "admin routes disabled" });
      return true;
    }
    if (req.headers["x-mfup-admin-token"] !== this.cfg.adminToken) {
      json(res, 403, { error: "forbidden" });
      return true;
    }
    return false;
  }

  private listSessions(req: IncomingMessage, res: ServerResponse): void {
    if (this.adminDenied(req, res)) return;
    const registry = this.engine.requireRegistry();
    const result: unknown[] = [];
    for (const [sid, session] of registry.allSessions()) {
      result.push({
        session_id: sid,
        state: session.state,
        epoch: session.epoch,
        leg_id: session.legId,
        expires_at: session.expiresAt,
      });
    }
    json(res, 200, result);
  }

  private getSessionStatus(req: IncomingMessage, res: ServerResponse, sessionId: string): void {
    if (this.adminDenied(req, res)) return;
    const registry = this.engine.requireRegistry();
    const session = registry.get(sessionId);
    if (session === null) {
      json(res, 404, { error: "not found" });
      return;
    }
    json(res, 200, {
      session_id: sessionId,
      state: session.state,
      epoch: session.epoch,
      leg_id: session.legId,
      expires_at: session.expiresAt,
      root_summary: session.db.getRootSummary(),
    });
  }

  private async publishEndpoint(
    req: IncomingMessage,
    res: ServerResponse,
    sessionId: string,
  ): Promise<void> {
    const registry = this.engine.requireRegistry();
    const session = registry.get(sessionId);
    if (session === null) {
      json(res, 404, { error: "not found" });
      return;
    }

    // Publish moves files and destroys staging — same bearer auth as the
    // data plane: the session's resume token.
    const token = req.headers["x-mfup-token"];
    if (!token || token !== session.resumeToken) {
      json(res, 403, { error: "invalid token" });
      return;
    }

    try {
      const published = await this.engine.publish(sessionId);
      json(res, 200, { published });
    } catch (exc) {
      if (exc instanceof SessionNotFound) {
        json(res, 404, { error: "not found" });
      } else if (exc instanceof NotCommitted) {
        json(res, 409, { error: `cannot publish session in state ${exc.state}` });
      } else if (exc instanceof TargetEscapes) {
        json(res, 403, { error: "target_dir escapes base directory" });
      } else if (exc instanceof MapFileHookError) {
        json(res, 500, { error: "map_file_hook_error", path: exc.path });
      } else if (exc instanceof MappingError) {
        // Consumer-hook bug (escape / duplicate destination) — nothing moved.
        logger.error(`Mapping error for session ${sessionId}: ${exc.message}`);
        json(res, 409, { error: "mapping_error", detail: exc.message });
      } else if (exc instanceof ConflictError) {
        json(res, 409, { error: "conflict_files", conflicting_files: exc.conflictingFiles });
      } else if ((exc as NodeJS.ErrnoException).code === "ENOENT") {
        json(res, 404, { error: (exc as Error).message });
      } else {
        logger.error(`Publish failed for session ${sessionId}: ${(exc as Error).stack ?? exc}`);
        json(res, 500, { error: "publish_failed" });
      }
    }
  }

  private async sweepEndpoint(req: IncomingMessage, res: ServerResponse): Promise<void> {
    if (this.adminDenied(req, res)) return;
    const removed = await this.engine.sweep();
    json(res, 200, { removed });
  }

  // -- control channel (WebSocket) ---------------------------------------------

  private async controlLoop(ws: WebSocket, req: IncomingMessage): Promise<void> {
    const cfg = this.cfg;
    const engine = this.engine;
    const registry = engine.requireRegistry();
    const store = engine.requireStore();

    const controlSock: ControlSocket = {
      sendJson: (msg) =>
        new Promise<void>((resolve, reject) => {
          ws.send(JSON.stringify(msg), (err) => (err ? reject(err) : resolve()));
        }),
    };
    const send = (msg: Record<string, unknown>) => controlSock.sendJson(msg);
    const abortAndClose = async (code: string, reason: string) => {
      try {
        await send({ t: "SESSION_ABORT", code, reason });
      } catch {
        /* socket already gone */
      }
      try {
        ws.close();
      } catch {
        /* already closed */
      }
    };

    const q = new MsgQueue();
    ws.on("message", (data) => {
      try {
        q.push(JSON.parse(data.toString()));
      } catch {
        logger.warn("Control WS: unparseable message ignored");
      }
    });
    ws.on("close", () => q.end());
    ws.on("error", (err) => {
      logger.warn(`Control WS error: ${err}`);
      q.end();
    });

    let session: LiveSession | null = null;

    try {
      // First message must be HELLO or RESUME
      const first = (await q.next()) as Record<string, unknown> | null;
      if (first === null) return;
      const t = first.t;

      if (t === "HELLO") {
        if (first.v !== PROTOCOL_VERSION) {
          await abortAndClose("bad_version", `expected ${PROTOCOL_VERSION}`);
          return;
        }

        const sessionId = String(first.session_id);
        const legId = String(first.leg_id);
        let targetDir = typeof first.target_dir === "string" ? first.target_dir : ".";
        // The resume token is SERVER-issued (returned in HELLO_OK) — a
        // client-chosen token would be a self-signed credential, useless as
        // an authenticator for the data/publish endpoints.
        const resumeToken = crypto.randomBytes(32).toString("base64url");

        // Client-attached session metadata (scope/purpose/ids). Untrusted;
        // size-capped; handed to authorize + mapFile.
        const clientMeta = first.meta;
        let metaJson: string | null = null;
        if (clientMeta !== undefined && clientMeta !== null) {
          metaJson = JSON.stringify(clientMeta);
          if (Buffer.byteLength(metaJson) > cfg.maxMetaBytes) {
            await abortAndClose("protocol_error", `meta exceeds ${cfg.maxMetaBytes} bytes`);
            return;
          }
        }

        // Consumer authorization. Runs BEFORE anything is created.
        let authResult: AuthResult | null = null;
        if (cfg.authorize !== null) {
          try {
            const r = await cfg.authorize({
              sessionId,
              targetDir,
              headers: flattenHeaders(req),
              client: req.socket.remoteAddress
                ? `${req.socket.remoteAddress}:${req.socket.remotePort ?? ""}`
                : "",
              query: Object.fromEntries(
                new URLSearchParams(
                  (req.url ?? "").includes("?")
                    ? (req.url ?? "").slice((req.url ?? "").indexOf("?") + 1)
                    : "",
                ),
              ),
              meta: clientMeta,
            } satisfies AuthRequest);
            authResult = r ?? null;
          } catch (exc) {
            logger.error(`Authorize hook raised for session ${sessionId} — denying: ${exc}`);
            authResult = null;
          }
          if (authResult === null) {
            await abortAndClose("auth_failed", "authorization denied");
            return;
          }
          if (authResult.targetDir !== undefined) {
            // The hook may pin or MAP the target (it received the
            // client-requested value in req.targetDir); still subject to the
            // containment check below.
            targetDir = authResult.targetDir;
          }
        }

        // Per-session base directory (e.g. the user's home). Staging is
        // created INSIDE it so publish remains a same-filesystem rename even
        // on separate mounts.
        let sessionBase = registry.baseDir;
        if (authResult !== null && authResult.baseDir !== undefined) {
          if (!path.isAbsolute(authResult.baseDir)) {
            logger.error(
              `Authorize hook returned a relative baseDir ${JSON.stringify(authResult.baseDir)} for session ${sessionId} — denying`,
            );
            await abortAndClose("auth_failed", "authorization misconfigured (relative base_dir)");
            return;
          }
          sessionBase = path.resolve(authResult.baseDir);
          await fsp.mkdir(sessionBase, { recursive: true });
        }

        // Validate target_dir stays within the session's base dir
        if (!isSafeTarget(sessionBase, targetDir)) {
          await abortAndClose("bad_target_dir", "target_dir escapes base directory");
          return;
        }

        const expires = new Date(Date.now() + cfg.sessionResumeTtl * 1000);
        try {
          session = registry.create(
            sessionId,
            resumeToken,
            legId,
            expires.toISOString(),
            targetDir,
            sessionBase !== registry.baseDir ? sessionBase : null,
            metaJson,
          );
        } catch {
          // Session already exists — treat as conflict
          await abortAndClose("conflict", "session already exists");
          return;
        }

        // Register in the store for TTL-based cleanup (with paths). A store
        // failure here must not leave an orphaned in-memory session behind
        // (a retried HELLO would then hit "conflict"), so roll back and
        // abort explicitly.
        const sd = stagingDir(sessionBase, sessionId, cfg.stagingPrefix);
        try {
          // The absolute staging path in store meta is what makes the
          // sweeper / recovery work for per-user base dirs.
          await store.register(sessionId, expires, targetDir, sd);
          session.onExpiryChange = (sid, exp) => store.updateExpiry(sid, exp);
        } catch (exc) {
          logger.error(`Store register failed for session ${sessionId} — rolling back: ${exc}`);
          registry.remove(sessionId);
          // Without a store entry the sweeper would never find this staging
          // dir — remove it now.
          await fsp.rm(sd, { recursive: true, force: true });
          session = null;
          await abortAndClose("server_error", "session index unavailable, retry later");
          return;
        }

        // Apply authorize-hook constraints to the live session (persisted —
        // they must survive restarts / recovery).
        if (authResult !== null) {
          session.applyAuth(
            authResult.maxTotalBytes ?? null,
            authResult.maxFiles ?? null,
            authResult.context ?? {},
          );
        }

        session.ws = controlSock;
        await send({
          t: "HELLO_OK",
          epoch: session.epoch,
          expires_at: session.expiresAt,
          // Server-issued bearer token for data/probe/publish/RESUME.
          resume_token: resumeToken,
          limits: {
            max_chunk_bytes: cfg.maxChunkBytes,
            max_open_files: cfg.maxOpenFiles,
            max_pending_files: cfg.maxPendingFiles,
          },
        });
      } else if (t === "RESUME") {
        const sessionId = String(first.session_id);
        const resumeToken = String(first.resume_token);
        const legId = String(first.leg_id);

        try {
          try {
            session = registry.resume(sessionId, resumeToken, legId);
          } catch (exc) {
            if (!(exc instanceof SessionUnknownError)) throw exc;
            // Lazy recovery: the session is not in THIS process's memory
            // (different worker after a deploy/failover), but its durable
            // state may still exist — store meta points at the staging dir.
            // Ownership transfers to whichever worker holds the WS.
            const meta = await store.getMeta(sessionId);
            if (!meta || !meta.stagingDir) throw exc;
            const recovered = registry.recoverSession(sessionId, meta.stagingDir);
            if (recovered === null) throw exc;
            logger.info(`Lazy-recovered session ${sessionId} for RESUME`);
            session = registry.resume(sessionId, resumeToken, legId);
          }
        } catch (exc) {
          if (exc instanceof SessionUnknownError) {
            await abortAndClose("not_found", "session not found or expired");
          } else if (exc instanceof BadTokenError) {
            await abortAndClose("auth_failed", "invalid resume token");
          } else if (exc instanceof BadStateError) {
            await abortAndClose("invalid_state", exc.message);
          } else {
            logger.error(`RESUME failed for session ${sessionId}: ${exc}`);
            await abortAndClose("server_error", "resume failed");
          }
          return;
        }

        session.ws = controlSock;
        // Ensure the expiry callback is wired for resumed sessions
        session.onExpiryChange = (sid, exp) => store.updateExpiry(sid, exp);
        await send(session.buildResumeOk());
      } else {
        await abortAndClose("protocol_error", `expected HELLO or RESUME, got ${String(t)}`);
        return;
      }

      // Main control loop — CLIENT_ABORT / ACTION
      while (true) {
        const m = (await q.next()) as Record<string, unknown> | null;
        if (m === null) break; // disconnected
        const mt = m.t;

        if (mt === "CLIENT_ABORT") {
          if (session) {
            session.db.setState(SessionState.ABORTED);
            session.detachLeg();
          }
          break;
        }

        if (mt === "ACTION") {
          const action = m.action;
          if (session && (action === "merge_overwrite" || action === "cancel")) {
            session.publishAction = action;
            logger.info(`Session ${session.sessionId}: ACTION=${action}`);
            if (action === "cancel") {
              session.db.setState(SessionState.ABORTED);
              session.detachLeg();
              break;
            }
          }
        }
      }
    } catch (exc) {
      logger.error(
        `Control WS error for session ${session ? session.sessionId : "unknown"}: ${(exc as Error).stack ?? exc}`,
      );
    } finally {
      // The session may have been removed — and its SQLite handle CLOSED —
      // while this socket was still open: publish does exactly that.
      // Touching session.state/db then throws inside this finally after a
      // perfectly good upload. Nothing is left to clean up in that case.
      if (session && registry.get(session.sessionId) !== session) {
        if (session.ws === controlSock) session.ws = null;
        session = null;
      }
      if (session) {
        try {
          // If in COMMITTING state, attempt commit before detaching
          if (session.state === SessionState.COMMITTING) {
            await engine.tryCommit(session);
          }
          if (session.ws === controlSock) session.ws = null;
          if (
            session.legId &&
            session.state !== SessionState.COMMITTED &&
            session.state !== SessionState.ABORTED
          ) {
            session.detachLeg();
          }
          // Clean up aborted sessions immediately: staging + store entry
          if (session.state === SessionState.ABORTED) {
            const sid = session.sessionId;
            const meta = await store.getMeta(sid).catch(() => null);
            // session.baseDir, not the global one: per-user homes.
            const sd =
              meta && meta.stagingDir
                ? meta.stagingDir
                : stagingDir(session.baseDir, sid, cfg.stagingPrefix);
            registry.remove(sid);
            if (fs.existsSync(sd)) {
              await fsp.rm(sd, { recursive: true, force: true });
            }
            await store.remove(sid).catch(() => undefined);
            logger.info(`Cleaned up aborted session ${sid} (staging=${sd})`);
          }
        } catch (exc) {
          logger.error(`Control WS cleanup failed: ${(exc as Error).stack ?? exc}`);
        }
      }
      try {
        ws.close();
      } catch {
        /* already closed */
      }
    }
  }
}
