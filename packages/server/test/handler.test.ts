/**
 * Integration: a real http.Server + createMfup, driven by a real WebSocket
 * (ws) and real HTTP POSTs whose bodies are built with the @mfup/client
 * encoders — the full wire, no mocks.
 */

import * as fs from "node:fs";
import * as http from "node:http";
import * as path from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";

import {
  crc32c as clientCrc32c,
  encodeFileChunkFrame,
  encodeFileCloseFrame,
  encodeFileOpenFrame,
  encodeNodeFrame,
  encodeSessionEndFrame,
  FrameTag as CFrameTag,
} from "@mfup/client";

import { createMfup, stagingDir, type Mfup } from "../src/index.js";
import { mkTmpDir, rmrf } from "./helpers.js";

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

const enc = new TextEncoder();

function chunkFrames(nodeId: number, content: Uint8Array): Uint8Array[] {
  const out: Uint8Array[] = [];
  out.push(
    encodeNodeFrame({
      tag: CFrameTag.NODE,
      nodeId,
      parentId: 0,
      kind: 1,
      name: `f${nodeId}.txt`,
      sizeHint: BigInt(content.length),
      mtimeMs: null,
    }),
  );
  out.push(
    encodeFileOpenFrame({ tag: CFrameTag.FILE_OPEN, nodeId, size: BigInt(content.length), mtimeMs: null }),
  );
  if (content.length > 0) {
    out.push(
      encodeFileChunkFrame({
        tag: CFrameTag.FILE_CHUNK,
        nodeId,
        offset: 0n,
        length: content.length,
        checksumKind: 1,
        checksum: clientCrc32c(content),
        payload: content,
      }),
    );
  }
  out.push(encodeFileCloseFrame({ tag: CFrameTag.FILE_CLOSE, nodeId, sizeSent: BigInt(content.length) }));
  return out;
}

function sessionEnd(scan: number, body: number): Uint8Array {
  return encodeSessionEndFrame({
    tag: CFrameTag.SESSION_END,
    rootSummary: {
      scanDoneUnits: BigInt(scan),
      scanEstUnits: BigInt(scan),
      bodyDoneBytes: BigInt(body),
      bodyEstBytes: BigInt(body),
      sealed: true,
    },
  });
}

function concat(parts: Uint8Array[]): Buffer {
  return Buffer.concat(parts.map((p) => Buffer.from(p)));
}

/** WebSocket client with a message inbox. */
class Ctl {
  ws: WebSocket;
  inbox: Record<string, unknown>[] = [];
  private waiters: ((m: Record<string, unknown> | null) => void)[] = [];
  closed = false;

  constructor(url: string) {
    this.ws = new WebSocket(url);
    this.ws.on("message", (d) => {
      const m = JSON.parse(d.toString()) as Record<string, unknown>;
      this.inbox.push(m);
      for (const w of this.waiters.splice(0)) w(m);
    });
    this.ws.on("close", () => {
      this.closed = true;
      for (const w of this.waiters.splice(0)) w(null);
    });
  }

  open(): Promise<void> {
    return new Promise((res, rej) => {
      this.ws.once("open", res);
      this.ws.once("error", rej);
    });
  }

  send(msg: unknown): void {
    this.ws.send(JSON.stringify(msg));
  }

  /** Wait until a message with t===type is in the inbox. */
  async waitFor(type: string, timeoutMs = 5000): Promise<Record<string, unknown>> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const found = this.inbox.find((m) => m.t === type);
      if (found) return found;
      await new Promise<unknown>((res) => {
        const t = setTimeout(res, 50);
        this.waiters.push(() => {
          clearTimeout(t);
          res(null);
        });
      });
    }
    throw new Error(`timed out waiting for ${type}; inbox: ${JSON.stringify(this.inbox)}`);
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      /* already closed */
    }
  }
}

async function poll(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("poll timed out");
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

let base: string;
let mfup: Mfup;
let server: http.Server;
let port: number;
let origin: string;

beforeAll(async () => {
  base = mkTmpDir("mfup-handler-");
  mfup = createMfup({
    baseDir: base,
    authorize: async (req) => {
      const meta = req.meta as { deny?: boolean; home?: string } | undefined;
      if (meta?.deny) return null;
      if (meta?.home) {
        return { baseDir: path.join(base, "homes", meta.home), context: { home: meta.home } };
      }
      return {};
    },
  });
  server = http.createServer((req, res) => {
    void mfup.handle(req, res).then((handled) => {
      if (!handled) {
        res.statusCode = 404;
        res.end('{"error":"unhandled"}');
      }
    });
  });
  mfup.attach(server);
  await new Promise<void>((res) => server.listen(0, "127.0.0.1", res));
  port = (server.address() as { port: number }).port;
  origin = `http://127.0.0.1:${port}`;
  await mfup.ready();
});

afterAll(async () => {
  await mfup.close();
  await new Promise<void>((res) => server.close(() => res()));
  rmrf(base);
});

interface Opened {
  ctl: Ctl;
  sid: string;
  leg: string;
  token: string;
  epoch: number;
}

async function hello(sid: string, opts: { targetDir?: string; meta?: unknown } = {}): Promise<Opened> {
  const ctl = new Ctl(`ws://127.0.0.1:${port}/mfup/control`);
  await ctl.open();
  const leg = `${sid}-leg1`;
  ctl.send({
    t: "HELLO",
    v: "MFUP/2",
    session_id: sid,
    leg_id: leg,
    target_dir: opts.targetDir ?? ".",
    ...(opts.meta !== undefined ? { meta: opts.meta } : {}),
  });
  const ok = await ctl.waitFor("HELLO_OK");
  return { ctl, sid, leg, token: ok.resume_token as string, epoch: ok.epoch as number };
}

function postData(
  o: Opened,
  body: Buffer,
  seq: number,
  final: number,
  epochOverride?: number,
): Promise<Response> {
  return fetch(
    `${origin}/mfup/data/${o.sid}/${o.leg}?seq=${seq}&final=${final}&epoch=${epochOverride ?? o.epoch}`,
    { method: "POST", headers: { "x-mfup-token": o.token }, body },
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("HTTP surface", () => {
  it("GET /health", async () => {
    const r = await fetch(`${origin}/health`);
    expect(r.status).toBe(200);
    const j = (await r.json()) as Record<string, unknown>;
    expect(j.protocol).toBe("MFUP/2");
    expect(j.crc32c).toBe("js-table");
  });

  it("unknown /mfup/* path → 404, foreign path → untouched", async () => {
    expect((await fetch(`${origin}/mfup/bogus`)).status).toBe(404);
    const r = await fetch(`${origin}/something/else`);
    expect(await r.text()).toBe('{"error":"unhandled"}');
  });

  it("admin routes are disabled without a token", async () => {
    const r = await fetch(`${origin}/mfup/sessions`);
    expect(r.status).toBe(403);
    expect(((await r.json()) as Record<string, unknown>).error).toBe("admin routes disabled");
  });
});

describe("upload lifecycle over the real wire", () => {
  it("HELLO → data POST → commit → publish → bytes on disk", async () => {
    const o = await hello("it-happy");
    const content = enc.encode("hello over the wire");
    const body = concat([...chunkFrames(1, content), sessionEnd(1, content.length)]);

    const resp = await postData(o, body, 0, 1);
    expect(resp.status).toBe(200);
    const j = (await resp.json()) as { ok: boolean; commit?: { files: number; bytes: number } };
    expect(j.ok).toBe(true);
    expect(j.commit).toEqual({ files: 1, bytes: content.length });

    await o.ctl.waitFor("COMMIT_OK");
    expect(o.ctl.inbox.some((m) => m.t === "FILE_ACK")).toBe(true);

    const pub = await fetch(`${origin}/mfup/sessions/${o.sid}/publish`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
    });
    expect(pub.status).toBe(200);
    expect(((await pub.json()) as { published: string[] }).published).toEqual(["f1.txt"]);

    expect(fs.readFileSync(path.join(base, "f1.txt"), "utf-8")).toBe("hello over the wire");
    expect(fs.existsSync(stagingDir(base, o.sid))).toBe(false);
    o.ctl.close();
  });

  it("second upload of the same name → ASK → merge_overwrite → overwritten", async () => {
    const o = await hello("it-conflict");
    const content = enc.encode("NEW CONTENT v2");
    const body = concat([...chunkFrames(1, content), sessionEnd(1, content.length)]);

    const resp = await postData(o, body, 0, 1);
    expect(resp.status).toBe(200);

    // f1.txt already exists in the target from the previous test → ASK
    const ask = await o.ctl.waitFor("ASK");
    expect(ask.code).toBe("target_conflict");
    expect(ask.name).toBe("f1.txt");
    o.ctl.send({ t: "ACTION", action: "merge_overwrite" });

    await o.ctl.waitFor("COMMIT_OK");
    // ACTION travels over WS; give the server a beat to record it
    await poll(() => {
      return true;
    });
    let pub: Response | null = null;
    for (let i = 0; i < 20; i++) {
      pub = await fetch(`${origin}/mfup/sessions/${o.sid}/publish`, {
        method: "POST",
        headers: { "x-mfup-token": o.token },
      });
      if (pub.status === 200) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(pub!.status).toBe(200);
    expect(fs.readFileSync(path.join(base, "f1.txt"), "utf-8")).toBe("NEW CONTENT v2");
    o.ctl.close();
  });

  it("CLIENT_ABORT mid-transfer leaves no staging dir and no store entry", async () => {
    const o = await hello("it-abort");
    const content = enc.encode("partial data that will be discarded");
    // final=0 — transfer still in flight
    const resp = await postData(o, concat(chunkFrames(1, content)), 0, 0);
    expect(resp.status).toBe(200);
    expect(fs.existsSync(stagingDir(base, o.sid))).toBe(true);

    o.ctl.send({ t: "CLIENT_ABORT", code: "client_cancel", reason: "user cancelled" });
    await poll(() => !fs.existsSync(stagingDir(base, o.sid)));
    expect(await mfup.engine.requireStore().isRegistered(o.sid)).toBe(false);
    o.ctl.close();
  });

  it("per-user baseDir from the authorize hook: staging + publish inside the home", async () => {
    const o = await hello("it-homed", { meta: { home: "alice" } });
    const home = path.join(base, "homes", "alice");
    const content = enc.encode("home sweet home");
    const body = concat([...chunkFrames(1, content), sessionEnd(1, content.length)]);
    const resp = await postData(o, body, 0, 1);
    expect(((await resp.json()) as { commit?: unknown }).commit).toBeTruthy();

    const pub = await fetch(`${origin}/mfup/sessions/${o.sid}/publish`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
    });
    expect(pub.status).toBe(200);
    expect(fs.readFileSync(path.join(home, "f1.txt"), "utf-8")).toBe("home sweet home");
    expect(fs.existsSync(path.join(base, "f1.txt.it-homed"))).toBe(false);
    o.ctl.close();
  });

  it("authorize deny → SESSION_ABORT auth_failed", async () => {
    const ctl = new Ctl(`ws://127.0.0.1:${port}/mfup/control`);
    await ctl.open();
    ctl.send({
      t: "HELLO",
      v: "MFUP/2",
      session_id: "it-denied",
      leg_id: "l",
      target_dir: ".",
      meta: { deny: true },
    });
    const abort = await ctl.waitFor("SESSION_ABORT");
    expect(abort.code).toBe("auth_failed");
    ctl.close();
  });

  it("bad protocol version → SESSION_ABORT bad_version", async () => {
    const ctl = new Ctl(`ws://127.0.0.1:${port}/mfup/control`);
    await ctl.open();
    ctl.send({ t: "HELLO", v: "MFUP/1", session_id: "it-old", leg_id: "l", target_dir: "." });
    const abort = await ctl.waitFor("SESSION_ABORT");
    expect(abort.code).toBe("bad_version");
    ctl.close();
  });
});

describe("data-plane fencing", () => {
  it("bad token 403, stale leg 409, stale epoch 409, bad seq 409", async () => {
    const o = await hello("it-fence");
    const body = concat(chunkFrames(1, enc.encode("x")));

    const badToken = await fetch(`${origin}/mfup/data/${o.sid}/${o.leg}?seq=0&final=0&epoch=${o.epoch}`, {
      method: "POST",
      headers: { "x-mfup-token": "wrong" },
      body,
    });
    expect(badToken.status).toBe(403);

    const staleLeg = await fetch(`${origin}/mfup/data/${o.sid}/other-leg?seq=0&final=0&epoch=${o.epoch}`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
      body,
    });
    expect(staleLeg.status).toBe(409);
    expect(((await staleLeg.json()) as { error: string }).error).toBe("stale leg");

    const staleEpoch = await postData(o, body, 0, 0, o.epoch + 5);
    expect(staleEpoch.status).toBe(409);
    expect(((await staleEpoch.json()) as { error: string }).error).toBe("stale_epoch");

    const noEpoch = await fetch(`${origin}/mfup/data/${o.sid}/${o.leg}?seq=0&final=0`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
      body,
    });
    expect(noEpoch.status).toBe(400);

    const badSeq = await postData(o, body, 5, 0);
    expect(badSeq.status).toBe(409);
    expect(((await badSeq.json()) as { error: string }).error).toBe("seq_mismatch");

    // Correct POST still lands after all the rejects
    const ok = await postData(o, body, 0, 0);
    expect(ok.status).toBe(200);

    // Retrying the same seq (already processed) reads as "already delivered"
    const dup = await postData(o, body, 0, 0);
    expect(dup.status).toBe(409);
    const dj = (await dup.json()) as { error: string; expected: number };
    expect(dj.error).toBe("seq_mismatch");
    expect(dj.expected).toBe(1);

    o.ctl.send({ t: "CLIENT_ABORT", code: "client_cancel", reason: "done" });
    await poll(() => !fs.existsSync(stagingDir(base, o.sid)));
    o.ctl.close();
  });

  it("unknown session 410", async () => {
    const r = await fetch(`${origin}/mfup/data/who/leg?seq=0&final=0&epoch=1`, {
      method: "POST",
      body: Buffer.alloc(0),
    });
    expect(r.status).toBe(410);
  });
});

describe("probe endpoint", () => {
  it("streams the body and PROBE_ACKs the first chunk on the control socket", async () => {
    const o = await hello("it-probe");
    const r = await fetch(`${origin}/mfup/probe/${o.sid}`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
      body: Buffer.alloc(2048, 7),
    });
    expect(r.status).toBe(200);
    expect(((await r.json()) as { total_bytes: number }).total_bytes).toBe(2048);
    const ack = await o.ctl.waitFor("PROBE_ACK");
    expect((ack.first_chunk_bytes as number) >= 1).toBe(true);
    o.ctl.send({ t: "CLIENT_ABORT", code: "client_cancel", reason: "done" });
    await poll(() => !fs.existsSync(stagingDir(base, o.sid)));
    o.ctl.close();
  });
});

describe("resume over the wire", () => {
  it("drop the socket mid-upload → RESUME → finish → publish", async () => {
    const o = await hello("it-resume", { targetDir: "resumed" });
    const content = enc.encode("0123456789abcdef");
    // First half of the file, then the socket dies
    const half = content.subarray(0, 8);
    const frames1 = [
      encodeNodeFrame({
        tag: CFrameTag.NODE,
        nodeId: 1,
        parentId: 0,
        kind: 1,
        name: "r.txt",
        sizeHint: BigInt(content.length),
        mtimeMs: null,
      }),
      encodeFileOpenFrame({ tag: CFrameTag.FILE_OPEN, nodeId: 1, size: BigInt(content.length), mtimeMs: null }),
      encodeFileChunkFrame({
        tag: CFrameTag.FILE_CHUNK,
        nodeId: 1,
        offset: 0n,
        length: half.length,
        checksumKind: 1,
        checksum: clientCrc32c(half),
        payload: half,
      }),
    ];
    expect((await postData(o, concat(frames1), 0, 0)).status).toBe(200);
    o.ctl.ws.terminate(); // hard disconnect

    // New socket, RESUME with the server-issued token
    const ctl2 = new Ctl(`ws://127.0.0.1:${port}/mfup/control`);
    await ctl2.open();
    const leg2 = "it-resume-leg2";
    ctl2.send({ t: "RESUME", session_id: o.sid, resume_token: o.token, leg_id: leg2, last_known_epoch: o.epoch });
    const rok = await ctl2.waitFor("RESUME_OK");
    const epoch2 = rok.epoch as number;
    expect(epoch2).toBeGreaterThan(o.epoch);
    const fileState = (rok.files as { node_id: number; accepted_offset: number }[])[0];
    expect(fileState.accepted_offset).toBe(8);

    // Finish from the accepted offset on the new leg/epoch
    const rest = content.subarray(8);
    const frames2 = [
      encodeNodeFrame({
        tag: CFrameTag.NODE,
        nodeId: 1,
        parentId: 0,
        kind: 1,
        name: "r.txt",
        sizeHint: BigInt(content.length),
        mtimeMs: null,
      }),
      encodeFileOpenFrame({ tag: CFrameTag.FILE_OPEN, nodeId: 1, size: BigInt(content.length), mtimeMs: null }),
      encodeFileChunkFrame({
        tag: CFrameTag.FILE_CHUNK,
        nodeId: 1,
        offset: 8n,
        length: rest.length,
        checksumKind: 1,
        checksum: clientCrc32c(rest),
        payload: rest,
      }),
      encodeFileCloseFrame({ tag: CFrameTag.FILE_CLOSE, nodeId: 1, sizeSent: BigInt(content.length) }),
      sessionEnd(1, content.length),
    ];
    const resp = await fetch(
      `${origin}/mfup/data/${o.sid}/${leg2}?seq=0&final=1&epoch=${epoch2}`,
      { method: "POST", headers: { "x-mfup-token": o.token }, body: concat(frames2) },
    );
    expect(resp.status).toBe(200);
    expect(((await resp.json()) as { commit?: { files: number } }).commit?.files).toBe(1);

    const pub = await fetch(`${origin}/mfup/sessions/${o.sid}/publish`, {
      method: "POST",
      headers: { "x-mfup-token": o.token },
    });
    expect(pub.status).toBe(200);
    expect(fs.readFileSync(path.join(base, "resumed", "r.txt"), "utf-8")).toBe("0123456789abcdef");
    ctl2.close();
  });
});
