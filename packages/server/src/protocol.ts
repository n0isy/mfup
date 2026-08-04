/**
 * MFUP/2 protocol types and binary frame decoder (server-side).
 *
 * Wire format: [4-byte big-endian length][1-byte tag][payload]
 * The length covers tag + payload. All multi-byte integers are big-endian;
 * strings are [u16 length][UTF-8 bytes].
 *
 * This mirrors server/mfup-core/mfup_core/protocol.py byte for byte.
 */

export const PROTOCOL_VERSION = "MFUP/2";
export const ROOT_NODE_ID = 0;

// ---------------------------------------------------------------------------
// Frame tags — plain const objects (isolatedModules-safe), same as the client.
// ---------------------------------------------------------------------------
export const FrameTag = {
  NODE: 0x01,
  SUMMARY: 0x02,
  FILE_OPEN: 0x03,
  FILE_CHUNK: 0x04,
  FILE_CLOSE: 0x05,
  DIR_CLOSE: 0x06,
  SESSION_END: 0x07,
  CLIENT_ABORT: 0x08,
} as const;
export type FrameTag = (typeof FrameTag)[keyof typeof FrameTag];

export const NodeKind = {
  DIR: 0x00,
  FILE: 0x01,
} as const;
export type NodeKind = (typeof NodeKind)[keyof typeof NodeKind];

export const ChecksumKind = {
  CRC32C: 0x01,
} as const;
export type ChecksumKind = (typeof ChecksumKind)[keyof typeof ChecksumKind];

// ---------------------------------------------------------------------------
// Decoded frame shapes
//
// Sizes/offsets are plain numbers: JS numbers are exact to 2^53-1 (~9 PB),
// far beyond any real payload, and SQLite/fs APIs consume numbers. A u64 on
// the wire that exceeds Number.MAX_SAFE_INTEGER throws at decode.
// ---------------------------------------------------------------------------
export interface NodeFrame {
  tag: typeof FrameTag.NODE;
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  name: string;
  sizeHint: number | null;
  mtimeMs: number | null;
}

export interface SummaryFrame {
  tag: typeof FrameTag.SUMMARY;
  nodeId: number;
  scanDoneUnits: number;
  scanEstUnits: number;
  bodyDoneBytes: number;
  bodyEstBytes: number;
  sealed: boolean;
}

export interface FileOpenFrame {
  tag: typeof FrameTag.FILE_OPEN;
  nodeId: number;
  size: number;
  mtimeMs: number | null;
}

export interface FileChunkFrame {
  tag: typeof FrameTag.FILE_CHUNK;
  nodeId: number;
  offset: number;
  length: number;
  checksumKind: ChecksumKind;
  checksum: number;
  payload: Uint8Array;
}

export interface FileCloseFrame {
  tag: typeof FrameTag.FILE_CLOSE;
  nodeId: number;
  sizeSent: number;
}

export interface DirCloseFrame {
  tag: typeof FrameTag.DIR_CLOSE;
  nodeId: number;
}

export interface SessionEndFrame {
  tag: typeof FrameTag.SESSION_END;
  scanDoneUnits: number;
  scanEstUnits: number;
  bodyDoneBytes: number;
  bodyEstBytes: number;
  sealed: boolean;
}

export interface ClientAbortFrame {
  tag: typeof FrameTag.CLIENT_ABORT;
  code: string;
  reason: string;
}

export type Frame =
  | NodeFrame
  | SummaryFrame
  | FileOpenFrame
  | FileChunkFrame
  | FileCloseFrame
  | DirCloseFrame
  | SessionEndFrame
  | ClientAbortFrame;

// ---------------------------------------------------------------------------
// Session states (persisted as TEXT in the per-session SQLite journal)
// ---------------------------------------------------------------------------
export const SessionState = {
  ACTIVE: "active",
  PAUSED_BY_SERVER: "paused_by_server",
  WAITING_RESUME: "waiting_resume",
  COMMITTING: "committing",
  COMMITTED: "committed",
  ABORTED: "aborted",
  EXPIRED: "expired",
  FAILED: "failed",
} as const;
export type SessionState = (typeof SessionState)[keyof typeof SessionState];

export const NodeStatus = {
  OPEN: "open",
  CLOSED: "closed",
  REJECTED: "rejected",
  PRUNED: "pruned",
} as const;
export type NodeStatus = (typeof NodeStatus)[keyof typeof NodeStatus];

// ---------------------------------------------------------------------------
// Binary frame decoder
// ---------------------------------------------------------------------------

const TEXT_DECODER = new TextDecoder("utf-8", { fatal: true });

class Cursor {
  off = 0;
  constructor(
    readonly view: DataView,
    readonly bytes: Uint8Array,
  ) {}

  u8(): number {
    const v = this.view.getUint8(this.off);
    this.off += 1;
    return v;
  }
  u16(): number {
    const v = this.view.getUint16(this.off);
    this.off += 2;
    return v;
  }
  u32(): number {
    const v = this.view.getUint32(this.off);
    this.off += 4;
    return v;
  }
  u64(): number {
    const v = this.view.getBigUint64(this.off);
    this.off += 8;
    if (v > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new RangeError(`u64 value ${v} exceeds Number.MAX_SAFE_INTEGER`);
    }
    return Number(v);
  }
  string(): string {
    const len = this.u16();
    const s = TEXT_DECODER.decode(this.bytes.subarray(this.off, this.off + len));
    this.off += len;
    return s;
  }
  slice(len: number): Uint8Array {
    // Copy: the caller may hold the payload past buffer compaction.
    const out = this.bytes.slice(this.off, this.off + len);
    this.off += len;
    return out;
  }
}

/** Decode the payload bytes (after the tag byte) into a Frame. */
export function decodeFramePayload(tag: number, payload: Uint8Array): Frame {
  const c = new Cursor(new DataView(payload.buffer, payload.byteOffset, payload.byteLength), payload);

  switch (tag) {
    case FrameTag.NODE: {
      const nodeId = c.u32();
      const parentId = c.u32();
      const kindVal = c.u8();
      if (kindVal !== NodeKind.DIR && kindVal !== NodeKind.FILE) {
        throw new RangeError(`unknown node kind: ${kindVal}`);
      }
      const name = c.string();
      const sizeHint = c.u8() ? c.u64() : null;
      const mtimeMs = c.u8() ? c.u64() : null;
      return { tag: FrameTag.NODE, nodeId, parentId, kind: kindVal, name, sizeHint, mtimeMs };
    }
    case FrameTag.SUMMARY: {
      return {
        tag: FrameTag.SUMMARY,
        nodeId: c.u32(),
        scanDoneUnits: c.u64(),
        scanEstUnits: c.u64(),
        bodyDoneBytes: c.u64(),
        bodyEstBytes: c.u64(),
        sealed: c.u8() !== 0,
      };
    }
    case FrameTag.FILE_OPEN: {
      const nodeId = c.u32();
      const size = c.u64();
      const mtimeMs = c.u8() ? c.u64() : null;
      return { tag: FrameTag.FILE_OPEN, nodeId, size, mtimeMs };
    }
    case FrameTag.FILE_CHUNK: {
      const nodeId = c.u32();
      const offset = c.u64();
      const length = c.u32();
      const ckKind = c.u8();
      if (ckKind !== ChecksumKind.CRC32C) {
        throw new RangeError(`unknown checksum kind: ${ckKind}`);
      }
      const checksum = c.u32();
      const payloadBytes = c.slice(length);
      return {
        tag: FrameTag.FILE_CHUNK,
        nodeId,
        offset,
        length,
        checksumKind: ckKind,
        checksum,
        payload: payloadBytes,
      };
    }
    case FrameTag.FILE_CLOSE: {
      return { tag: FrameTag.FILE_CLOSE, nodeId: c.u32(), sizeSent: c.u64() };
    }
    case FrameTag.DIR_CLOSE: {
      return { tag: FrameTag.DIR_CLOSE, nodeId: c.u32() };
    }
    case FrameTag.SESSION_END: {
      return {
        tag: FrameTag.SESSION_END,
        scanDoneUnits: c.u64(),
        scanEstUnits: c.u64(),
        bodyDoneBytes: c.u64(),
        bodyEstBytes: c.u64(),
        sealed: c.u8() !== 0,
      };
    }
    case FrameTag.CLIENT_ABORT: {
      return { tag: FrameTag.CLIENT_ABORT, code: c.string(), reason: c.string() };
    }
    default:
      throw new RangeError(`unknown frame tag: 0x${tag.toString(16).padStart(2, "0")}`);
  }
}

/**
 * Incremental frame reader that buffers partial data from a streaming body.
 *
 * Feed chunks via `feed(data)` and collect decoded frames via `drain()`.
 *
 * `maxFrameLen` bounds the declared frame length: without it a corrupt or
 * malicious 4-byte prefix claiming a multi-gigabyte frame would grow the
 * buffer without limit while the reader waits for it to "complete".
 */
export class FrameReader {
  // 1 MiB default: comfortably above MAX_CHUNK_BYTES (256 KiB) + headers
  // and any NODE frame with a long UTF-8 name.
  static readonly DEFAULT_MAX_FRAME_LEN = 1024 * 1024;

  private chunks: Uint8Array[] = [];
  private buffered = 0;
  private readonly maxFrameLen: number;

  constructor(maxFrameLen: number = FrameReader.DEFAULT_MAX_FRAME_LEN) {
    this.maxFrameLen = maxFrameLen;
  }

  feed(data: Uint8Array): void {
    if (data.length > 0) {
      this.chunks.push(data);
      this.buffered += data.length;
    }
  }

  drain(): Frame[] {
    // Flatten once per drain; the partial tail is kept as a single chunk.
    const buf =
      this.chunks.length === 1 ? this.chunks[0] : concat(this.chunks, this.buffered);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

    const frames: Frame[] = [];
    let pos = 0;
    const total = buf.length;

    while (pos + 4 <= total) {
      const frameLen = view.getUint32(pos);
      if (frameLen > this.maxFrameLen) {
        throw new RangeError(`frame length ${frameLen} exceeds limit ${this.maxFrameLen}`);
      }
      if (pos + 4 + frameLen > total) break; // incomplete frame
      const tag = buf[pos + 4];
      // Copy payload so nothing pins the accumulation buffer.
      const payload = buf.slice(pos + 5, pos + 4 + frameLen);
      frames.push(decodeFramePayload(tag, payload));
      pos += 4 + frameLen;
    }

    if (pos > 0 || this.chunks.length > 1) {
      const tail = buf.subarray(pos);
      this.chunks = tail.length > 0 ? [tail] : [];
      this.buffered = tail.length;
    }

    return frames;
  }
}

function concat(chunks: Uint8Array[], total: number): Uint8Array {
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

// ---------------------------------------------------------------------------
// CRC-32C (Castagnoli) — table-driven, same algorithm as @mfup/client.
//
// Unlike CPython (~6 MB/s pure-Python, hence the hard C dependency there),
// V8 runs this table loop at hundreds of MB/s — comfortably above any single
// upload stream. No native module needed.
// ---------------------------------------------------------------------------
const CRC32C_TABLE = new Uint32Array(256);
{
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    }
    CRC32C_TABLE[i] = crc >>> 0;
  }
}

export function crc32c(data: Uint8Array, initial = 0): number {
  let crc = (initial ^ 0xffffffff) >>> 0;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ data[i]) & 0xff];
  }
  return (crc ^ 0xffffffff) >>> 0;
}

export const CRC32C_IMPL = "js-table";
