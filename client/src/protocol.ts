// MFUP/2 protocol types and binary frame codec

export const PROTOCOL_VERSION = "MFUP/2";
export const ROOT_NODE_ID = 0;

// ---------------------------------------------------------------------------
// Frame tags (single-byte discriminator in wire format)
// ---------------------------------------------------------------------------
export const enum FrameTag {
  NODE         = 0x01,
  SUMMARY      = 0x02,
  FILE_OPEN    = 0x03,
  FILE_CHUNK   = 0x04,
  FILE_CLOSE   = 0x05,
  DIR_CLOSE    = 0x06,
  SESSION_END  = 0x07,
  CLIENT_ABORT = 0x08,
}

// ---------------------------------------------------------------------------
// Node kind
// ---------------------------------------------------------------------------
export const enum NodeKind {
  DIR  = 0x00,
  FILE = 0x01,
}

// ---------------------------------------------------------------------------
// Checksum / hash kinds
// ---------------------------------------------------------------------------
export const enum ChecksumKind {
  CRC32C = 0x01,
}

export const enum HashKind {
  NONE   = 0x00,
  SHA256 = 0x01,
}

// ---------------------------------------------------------------------------
// Data frame types (logical, before serialisation)
// ---------------------------------------------------------------------------
export interface NodeFrame {
  tag: FrameTag.NODE;
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  name: string;
  sizeHint: bigint | null;
  mtimeMs: bigint | null;
}

export interface SummaryFrame {
  tag: FrameTag.SUMMARY;
  nodeId: number;
  scanDoneUnits: bigint;
  scanEstUnits: bigint;
  bodyDoneBytes: bigint;
  bodyEstBytes: bigint;
  sealed: boolean;
}

export interface FileOpenFrame {
  tag: FrameTag.FILE_OPEN;
  nodeId: number;
  size: bigint;
  mtimeMs: bigint | null;
}

export interface FileChunkFrame {
  tag: FrameTag.FILE_CHUNK;
  nodeId: number;
  offset: bigint;
  length: number;
  checksumKind: ChecksumKind;
  checksum: number;
  payload: Uint8Array;
}

export interface FileCloseFrame {
  tag: FrameTag.FILE_CLOSE;
  nodeId: number;
  sizeSent: bigint;
  strongHashKind: HashKind;
  strongHash: Uint8Array | null;
}

export interface DirCloseFrame {
  tag: FrameTag.DIR_CLOSE;
  nodeId: number;
}

export interface SessionEndFrame {
  tag: FrameTag.SESSION_END;
  rootSummary: {
    scanDoneUnits: bigint;
    scanEstUnits: bigint;
    bodyDoneBytes: bigint;
    bodyEstBytes: bigint;
    sealed: true;
  };
}

export interface ClientAbortFrame {
  tag: FrameTag.CLIENT_ABORT;
  code: string;
  reason: string;
}

export type DataFrame =
  | NodeFrame
  | SummaryFrame
  | FileOpenFrame
  | FileChunkFrame
  | FileCloseFrame
  | DirCloseFrame
  | SessionEndFrame
  | ClientAbortFrame;

// ---------------------------------------------------------------------------
// Control messages (JSON over WebSocket)
// ---------------------------------------------------------------------------
export interface HelloMsg {
  t: "HELLO";
  v: typeof PROTOCOL_VERSION;
  session_id: string;
  resume_token: string;
  leg_id: string;
  target_dir: string;
}

export interface ResumeMsg {
  t: "RESUME";
  session_id: string;
  resume_token: string;
  leg_id: string;
  last_known_epoch: number | null;
}

export interface ClientAbortMsg {
  t: "CLIENT_ABORT";
  code: string;
  reason: string;
}

export type ClientControlMsg = HelloMsg | ResumeMsg | ClientAbortMsg;

// Server → Client
export interface HelloOkMsg {
  t: "HELLO_OK";
  epoch: number;
  expires_at: string;
  limits: ServerLimits;
}

export interface ServerLimits {
  max_chunk_bytes: number;
  max_open_files: number;
  max_pending_files: number;
}

export interface ResumeFileStatus {
  node_id: number;
  accepted_offset: number;
  status: "open" | "closed" | "rejected";
}

export interface RootSummary {
  scan_done_units: number;
  scan_est_units: number;
  body_done_bytes: number;
  body_est_bytes: number;
  sealed: boolean;
}

export interface ResumeOkMsg {
  t: "RESUME_OK";
  epoch: number;
  expires_at: string;
  root_summary: RootSummary;
  files: ResumeFileStatus[];
  pruned_nodes: number[];
  rejected_files: number[];
}

export interface FileAckMsg {
  t: "FILE_ACK";
  node_id: number;
  accepted_offset: number;
}

export interface NackChunkMsg {
  t: "NACK_CHUNK";
  node_id: number;
  expected_offset: number;
  reason: "bad_checksum" | "bad_offset" | "stale_epoch" | "server_policy";
}

export interface FlowMsg {
  t: "FLOW";
  paused: boolean;
  reason: "backpressure" | "maintenance" | "storage_pressure";
}

export interface PruneNodeMsg {
  t: "PRUNE_NODE";
  node_id: number;
  code: string;
  reason: string;
}

export interface RejectFileMsg {
  t: "REJECT_FILE";
  node_id: number;
  code: string;
  reason: string;
}

export interface SessionAbortMsg {
  t: "SESSION_ABORT";
  code: string;
  reason: string;
}

export interface CommitOkMsg {
  t: "COMMIT_OK";
  files: number;
  bytes: number;
}

export interface ProbeAckMsg {
  t: "PROBE_ACK";
  first_chunk_bytes?: number;
}

export type ServerControlMsg =
  | HelloOkMsg
  | ResumeOkMsg
  | FileAckMsg
  | NackChunkMsg
  | FlowMsg
  | PruneNodeMsg
  | RejectFileMsg
  | SessionAbortMsg
  | CommitOkMsg
  | ProbeAckMsg;

// ---------------------------------------------------------------------------
// Session states
// ---------------------------------------------------------------------------
export type SessionState =
  | "active"
  | "paused_by_server"
  | "waiting_resume"
  | "committing"
  | "committed"
  | "aborted"
  | "expired"
  | "failed";

// ---------------------------------------------------------------------------
// Binary frame encoder
//
// Wire format: [4-byte big-endian length][1-byte tag][payload]
// The length prefix covers tag + payload (does NOT include itself).
// ---------------------------------------------------------------------------

const TEXT_ENCODER = new TextEncoder();

/** Encode a UTF-8 string as a length-prefixed segment: [u16 len][bytes] */
function encodeString(s: string): Uint8Array {
  const bytes = TEXT_ENCODER.encode(s);
  const buf = new Uint8Array(2 + bytes.length);
  new DataView(buf.buffer).setUint16(0, bytes.length);
  buf.set(bytes, 2);
  return buf;
}

function allocFrame(tag: FrameTag, payloadSize: number): { buf: Uint8Array; view: DataView } {
  // 4-byte length prefix + 1-byte tag + payload
  const total = 4 + 1 + payloadSize;
  const buf = new Uint8Array(total);
  const view = new DataView(buf.buffer);
  view.setUint32(0, 1 + payloadSize); // length = tag + payload
  buf[4] = tag;
  return { buf, view };
}

export function encodeNodeFrame(f: NodeFrame): Uint8Array {
  const nameBytes = TEXT_ENCODER.encode(f.name);
  // node_id(4) + parent_id(4) + kind(1) + name_len(2) + name + size_hint_flag(1) + [size_hint(8)] + mtime_flag(1) + [mtime(8)]
  let size = 4 + 4 + 1 + 2 + nameBytes.length + 1 + (f.sizeHint !== null ? 8 : 0) + 1 + (f.mtimeMs !== null ? 8 : 0);
  const { buf, view } = allocFrame(FrameTag.NODE, size);
  let off = 5;
  view.setUint32(off, f.nodeId); off += 4;
  view.setUint32(off, f.parentId); off += 4;
  buf[off++] = f.kind;
  view.setUint16(off, nameBytes.length); off += 2;
  buf.set(nameBytes, off); off += nameBytes.length;
  if (f.sizeHint !== null) {
    buf[off++] = 1;
    view.setBigUint64(off, f.sizeHint); off += 8;
  } else {
    buf[off++] = 0;
  }
  if (f.mtimeMs !== null) {
    buf[off++] = 1;
    view.setBigUint64(off, f.mtimeMs); off += 8;
  } else {
    buf[off++] = 0;
  }
  return buf;
}

export function encodeSummaryFrame(f: SummaryFrame): Uint8Array {
  // node_id(4) + 4×u64(32) + sealed(1) = 37
  const { buf, view } = allocFrame(FrameTag.SUMMARY, 37);
  let off = 5;
  view.setUint32(off, f.nodeId); off += 4;
  view.setBigUint64(off, f.scanDoneUnits); off += 8;
  view.setBigUint64(off, f.scanEstUnits); off += 8;
  view.setBigUint64(off, f.bodyDoneBytes); off += 8;
  view.setBigUint64(off, f.bodyEstBytes); off += 8;
  buf[off] = f.sealed ? 1 : 0;
  return buf;
}

export function encodeFileOpenFrame(f: FileOpenFrame): Uint8Array {
  // node_id(4) + size(8) + mtime_flag(1) + [mtime(8)]
  const size = 4 + 8 + 1 + (f.mtimeMs !== null ? 8 : 0);
  const { buf, view } = allocFrame(FrameTag.FILE_OPEN, size);
  let off = 5;
  view.setUint32(off, f.nodeId); off += 4;
  view.setBigUint64(off, f.size); off += 8;
  if (f.mtimeMs !== null) {
    buf[off++] = 1;
    view.setBigUint64(off, f.mtimeMs); off += 8;
  } else {
    buf[off++] = 0;
  }
  return buf;
}

export function encodeFileChunkFrame(f: FileChunkFrame): Uint8Array {
  // node_id(4) + offset(8) + length(4) + checksum_kind(1) + checksum(4) + payload
  const size = 4 + 8 + 4 + 1 + 4 + f.payload.length;
  const { buf, view } = allocFrame(FrameTag.FILE_CHUNK, size);
  let off = 5;
  view.setUint32(off, f.nodeId); off += 4;
  view.setBigUint64(off, f.offset); off += 8;
  view.setUint32(off, f.length); off += 4;
  buf[off++] = f.checksumKind;
  view.setUint32(off, f.checksum); off += 4;
  buf.set(f.payload, off);
  return buf;
}

export function encodeFileCloseFrame(f: FileCloseFrame): Uint8Array {
  // node_id(4) + size_sent(8) + hash_kind(1) + [hash(32 for sha256)]
  const hashLen = f.strongHash ? f.strongHash.length : 0;
  const size = 4 + 8 + 1 + (hashLen > 0 ? 2 + hashLen : 0);
  const { buf, view } = allocFrame(FrameTag.FILE_CLOSE, size);
  let off = 5;
  view.setUint32(off, f.nodeId); off += 4;
  view.setBigUint64(off, f.sizeSent); off += 8;
  buf[off++] = f.strongHashKind;
  if (f.strongHash && hashLen > 0) {
    view.setUint16(off, hashLen); off += 2;
    buf.set(f.strongHash, off);
  }
  return buf;
}

export function encodeDirCloseFrame(f: DirCloseFrame): Uint8Array {
  const { buf, view } = allocFrame(FrameTag.DIR_CLOSE, 4);
  view.setUint32(5, f.nodeId);
  return buf;
}

export function encodeSessionEndFrame(f: SessionEndFrame): Uint8Array {
  // 4×u64(32) + sealed(1) = 33
  const { buf, view } = allocFrame(FrameTag.SESSION_END, 33);
  let off = 5;
  view.setBigUint64(off, f.rootSummary.scanDoneUnits); off += 8;
  view.setBigUint64(off, f.rootSummary.scanEstUnits); off += 8;
  view.setBigUint64(off, f.rootSummary.bodyDoneBytes); off += 8;
  view.setBigUint64(off, f.rootSummary.bodyEstBytes); off += 8;
  buf[off] = 1; // sealed = true
  return buf;
}

export function encodeClientAbortFrame(f: ClientAbortFrame): Uint8Array {
  const codeBytes = encodeString(f.code);
  const reasonBytes = encodeString(f.reason);
  const size = codeBytes.length + reasonBytes.length;
  const { buf } = allocFrame(FrameTag.CLIENT_ABORT, size);
  let off = 5;
  buf.set(codeBytes, off); off += codeBytes.length;
  buf.set(reasonBytes, off);
  return buf;
}

export function encodeFrame(f: DataFrame): Uint8Array {
  switch (f.tag) {
    case FrameTag.NODE:         return encodeNodeFrame(f);
    case FrameTag.SUMMARY:      return encodeSummaryFrame(f);
    case FrameTag.FILE_OPEN:    return encodeFileOpenFrame(f);
    case FrameTag.FILE_CHUNK:   return encodeFileChunkFrame(f);
    case FrameTag.FILE_CLOSE:   return encodeFileCloseFrame(f);
    case FrameTag.DIR_CLOSE:    return encodeDirCloseFrame(f);
    case FrameTag.SESSION_END:  return encodeSessionEndFrame(f);
    case FrameTag.CLIENT_ABORT: return encodeClientAbortFrame(f);
  }
}

// ---------------------------------------------------------------------------
// CRC-32C (Castagnoli) — used for per-chunk checksums
// ---------------------------------------------------------------------------
const CRC32C_TABLE = new Uint32Array(256);
{
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = (crc & 1) ? (crc >>> 1) ^ 0x82F63B78 : crc >>> 1;
    }
    CRC32C_TABLE[i] = crc >>> 0;
  }
}

export function crc32c(data: Uint8Array, initial = 0): number {
  let crc = (initial ^ 0xFFFFFFFF) >>> 0;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ data[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}
