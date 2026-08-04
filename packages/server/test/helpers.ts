/** Shared fixtures mirroring server/tests/test_edge_cases.py. */

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  ChecksumKind,
  crc32c,
  FrameTag,
  NodeKind,
  SessionState,
  openSessionDb,
  LiveSession,
  type Frame,
  type ControlSocket,
} from "../src/index.js";

/** Records every control message; exposes ofType(t). */
export class FakeWS implements ControlSocket {
  messages: Record<string, unknown>[] = [];

  sendJson(msg: Record<string, unknown>): void {
    this.messages.push(msg);
  }

  ofType(t: string): Record<string, unknown>[] {
    return this.messages.filter((m) => m.t === t);
  }

  last(t: string): Record<string, unknown> | undefined {
    return this.ofType(t).at(-1);
  }
}

export function mkTmpDir(prefix = "mfup-test-"): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

export function rmrf(p: string): void {
  fs.rmSync(p, { recursive: true, force: true });
}

export interface MadeSession {
  session: LiveSession;
  ws: FakeWS;
  base: string;
}

/** Open a real session DB in tmp, attach leg "leg1", swap in a FakeWS. */
export function makeSession(
  base: string,
  sid: string,
  opts: { targetDir?: string; maxChunkBytes?: number } = {},
): MadeSession {
  const db = openSessionDb(base, sid);
  const expires = new Date(Date.now() + 3600_000).toISOString();
  db.initSession(sid, "tok", expires, opts.targetDir ?? ".");
  const session = new LiveSession(sid, "tok", base, db, {
    targetDir: opts.targetDir ?? ".",
    maxChunkBytes: opts.maxChunkBytes,
  });
  session.attachLeg("leg1");
  const ws = new FakeWS();
  session.ws = ws;
  return { session, ws, base };
}

// -- frame builders (decoded shapes, as the Python tests build dataclasses) --

export function nodeFrame(
  nodeId: number,
  parentId: number,
  kind: NodeKind,
  name: string,
  sizeHint: number | null = null,
): Frame {
  return { tag: FrameTag.NODE, nodeId, parentId, kind, name, sizeHint, mtimeMs: null };
}

export function fileOpenFrame(nodeId: number, size: number): Frame {
  return { tag: FrameTag.FILE_OPEN, nodeId, size, mtimeMs: null };
}

export function fileChunkFrame(nodeId: number, offset: number, payload: Uint8Array): Frame {
  return {
    tag: FrameTag.FILE_CHUNK,
    nodeId,
    offset,
    length: payload.length,
    checksumKind: ChecksumKind.CRC32C,
    checksum: crc32c(payload),
    payload,
  };
}

export function fileCloseFrame(nodeId: number, sizeSent: number): Frame {
  return { tag: FrameTag.FILE_CLOSE, nodeId, sizeSent };
}

export function sessionEndFrame(scanDone: number, bodyDone: number): Frame {
  return {
    tag: FrameTag.SESSION_END,
    scanDoneUnits: scanDone,
    scanEstUnits: scanDone,
    bodyDoneBytes: bodyDone,
    bodyEstBytes: bodyDone,
    sealed: true,
  };
}

export interface TreeFile {
  nodeId: number;
  name: string;
  content: Uint8Array;
}

/** Emit NODE/FILE_OPEN/FILE_CHUNK/FILE_CLOSE per entry + SESSION_END. */
export async function sendTree(s: LiveSession, files: TreeFile[]): Promise<void> {
  let bodyDone = 0;
  for (const f of files) {
    await s.processFrame(nodeFrame(f.nodeId, 0, NodeKind.FILE, f.name, f.content.length), "leg1");
    await s.processFrame(fileOpenFrame(f.nodeId, f.content.length), "leg1");
    if (f.content.length > 0) {
      await s.processFrame(fileChunkFrame(f.nodeId, 0, f.content), "leg1");
    }
    await s.processFrame(fileCloseFrame(f.nodeId, f.content.length), "leg1");
    bodyDone += f.content.length;
  }
  await s.processFrame(sessionEndFrame(files.length, bodyDone), "leg1");
}

/** sendTree + assert COMMIT_OK. */
export async function uploadTree(s: LiveSession, files: TreeFile[]): Promise<void> {
  await sendTree(s, files);
  const result = await s.tryCommit();
  if (result === null || result.t !== "COMMIT_OK") {
    throw new Error(`expected COMMIT_OK, got ${JSON.stringify(result)} (state=${s.state})`);
  }
}

export const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

export { SessionState };
