// MFUP/2 structured error system
//
// Every error carries: code, layer, message, action hint, and optional cause chain.

// ---------------------------------------------------------------------------
// Error codes — exhaustive, greppable
// ---------------------------------------------------------------------------
// Plain const objects, not `const enum` — const enums break for consumers
// building with isolatedModules (esbuild/swc/vite), i.e. everyone.
export const MfupErrorCode = {
  // Control channel
  WS_CONNECT_FAILED:    "WS_CONNECT_FAILED",
  WS_HANDSHAKE_FAILED:  "WS_HANDSHAKE_FAILED",
  WS_CLOSED_UNEXPECTED: "WS_CLOSED_UNEXPECTED",
  WS_MESSAGE_PARSE:     "WS_MESSAGE_PARSE",
  WS_SEND_FAILED:       "WS_SEND_FAILED",

  // Data channel
  DATA_OPEN_FAILED:     "DATA_OPEN_FAILED",
  DATA_HTTP_ERROR:      "DATA_HTTP_ERROR",
  DATA_WRITE_FAILED:    "DATA_WRITE_FAILED",
  DATA_STREAM_ERROR:    "DATA_STREAM_ERROR",
  DATA_CHANNEL_CLOSED:  "DATA_CHANNEL_CLOSED",

  // Session
  SESSION_ABORTED_BY_SERVER:   "SESSION_ABORTED_BY_SERVER",
  SESSION_ABORT_FAILED:        "SESSION_ABORT_FAILED",
  SESSION_RECONNECT_FAILED:    "SESSION_RECONNECT_FAILED",
  SESSION_RECONNECT_EXHAUSTED: "SESSION_RECONNECT_EXHAUSTED",
  SESSION_COMMIT_FAILED:       "SESSION_COMMIT_FAILED",
  SESSION_ENDED_BAD_STATE:     "SESSION_ENDED_BAD_STATE",

  // Publish
  PUBLISH_CONFLICT:     "PUBLISH_CONFLICT",
  PUBLISH_FAILED:       "PUBLISH_FAILED",

  // Ingestion
  INGEST_HANDLE_ERROR:  "INGEST_HANDLE_ERROR",
  INGEST_READ_ERROR:    "INGEST_READ_ERROR",

  // Protocol
  NACK_BAD_CHECKSUM:    "NACK_BAD_CHECKSUM",
  NACK_BAD_OFFSET:      "NACK_BAD_OFFSET",
  NACK_STALE_EPOCH:     "NACK_STALE_EPOCH",
  NACK_SERVER_POLICY:   "NACK_SERVER_POLICY",

  // Generic
  UNKNOWN:              "UNKNOWN",
} as const;
export type MfupErrorCode = (typeof MfupErrorCode)[keyof typeof MfupErrorCode];

export const MfupErrorLayer = {
  CONTROL:  "control",
  DATA:     "data",
  SESSION:  "session",
  INGEST:   "ingest",
  PROTOCOL: "protocol",
} as const;
export type MfupErrorLayer = (typeof MfupErrorLayer)[keyof typeof MfupErrorLayer];

// ---------------------------------------------------------------------------
// Structured error
// ---------------------------------------------------------------------------
export class MfupError extends Error {
  readonly code: MfupErrorCode;
  readonly layer: MfupErrorLayer;
  readonly action: string;
  readonly fatal: boolean;
  readonly detail: Record<string, unknown>;
  readonly timestamp: string;

  constructor(opts: {
    code: MfupErrorCode;
    layer: MfupErrorLayer;
    message: string;
    action: string;
    fatal: boolean;
    cause?: unknown;
    detail?: Record<string, unknown>;
  }) {
    super(opts.message, { cause: opts.cause });
    this.name = "MfupError";
    this.code = opts.code;
    this.layer = opts.layer;
    this.action = opts.action;
    this.fatal = opts.fatal;
    this.detail = opts.detail ?? {};
    this.timestamp = new Date().toISOString();
  }

  /** Flat representation for logging / UI display */
  toJSON(): Record<string, unknown> {
    return {
      code: this.code,
      layer: this.layer,
      message: this.message,
      action: this.action,
      fatal: this.fatal,
      detail: this.detail,
      cause: this.cause instanceof Error
        ? { name: this.cause.name, message: this.cause.message, stack: this.cause.stack }
        : this.cause,
      timestamp: this.timestamp,
    };
  }

  /** One-line summary for compact logs */
  summary(): string {
    return `[${this.layer}/${this.code}] ${this.message}`;
  }
}

// ---------------------------------------------------------------------------
// Factory helpers — one per common scenario
// ---------------------------------------------------------------------------

export function wsConnectFailed(url: string, cause?: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.WS_CONNECT_FAILED,
    layer: MfupErrorLayer.CONTROL,
    message: `WebSocket connection to ${url} failed`,
    action: "Check server URL and network connectivity. Verify the server is running.",
    fatal: false,
    cause,
    detail: { url },
  });
}

export function wsHandshakeFailed(code: string, reason: string): MfupError {
  return new MfupError({
    code: MfupErrorCode.WS_HANDSHAKE_FAILED,
    layer: MfupErrorLayer.CONTROL,
    message: `Server rejected handshake: [${code}] ${reason}`,
    action: code === "bad_version" ? "Client/server version mismatch. Update the client."
      : code === "conflict" ? "Session already exists. Reset and try again."
      : code === "auth_failed" ? "Resume token invalid. Start a new session."
      : "Check server logs for details.",
    fatal: true,
    detail: { code, reason },
  });
}

export function wsClosedUnexpected(wsCode: number, wsReason: string): MfupError {
  return new MfupError({
    code: MfupErrorCode.WS_CLOSED_UNEXPECTED,
    layer: MfupErrorLayer.CONTROL,
    message: `WebSocket closed unexpectedly: code=${wsCode} reason="${wsReason || "(none)"}"`,
    action: wsCode === 1006 ? "Network interrupted. Will attempt reconnect."
      : wsCode === 1008 ? "Server policy violation. Check server logs."
      : "Check network and server status.",
    fatal: false,
    detail: { wsCode, wsReason },
  });
}

export function wsMessageParse(rawData: string, cause: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.WS_MESSAGE_PARSE,
    layer: MfupErrorLayer.CONTROL,
    message: "Failed to parse control message from server",
    action: "Protocol error. Check server version compatibility.",
    fatal: false,
    cause,
    detail: { rawData: rawData.slice(0, 200) },
  });
}

export function dataOpenFailed(url: string, cause: unknown): MfupError {
  const msg = cause instanceof Error ? cause.message : String(cause);
  const isDuplex = msg.includes("duplex") || msg.includes("ReadableStream") || msg.includes("body");
  return new MfupError({
    code: MfupErrorCode.DATA_OPEN_FAILED,
    layer: MfupErrorLayer.DATA,
    message: `Data channel POST to ${url} failed: ${msg}`,
    action: isDuplex
      ? "Browser does not support streaming uploads (duplex:half). Use Chrome/Edge 105+, or the server needs HTTP/2 (TLS)."
      : "Check server URL and network. Verify CORS and proxy settings.",
    fatal: true,
    cause,
    detail: { url, isDuplexIssue: isDuplex },
  });
}

export function dataHttpError(url: string, status: number, statusText: string, body?: string): MfupError {
  return new MfupError({
    code: MfupErrorCode.DATA_HTTP_ERROR,
    layer: MfupErrorLayer.DATA,
    message: `Data channel HTTP ${status} ${statusText}`,
    action: status === 409 ? "Session conflict (stale leg or epoch). Reconnect will fix this."
      : status === 410 ? "Session expired. Start a new upload."
      : status === 503 ? "Server overloaded. Retry in a moment."
      : "Check server logs.",
    fatal: status === 410,
    detail: { url, status, statusText, body: body?.slice(0, 500) },
  });
}

export function dataWriteFailed(reason: string, cause?: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.DATA_WRITE_FAILED,
    layer: MfupErrorLayer.DATA,
    message: `Cannot write to data channel: ${reason}`,
    action: "Data channel write failed. Will attempt reconnect.",
    fatal: false,
    cause,
    detail: { reason },
  });
}

export function dataChannelClosed(): MfupError {
  return new MfupError({
    code: MfupErrorCode.DATA_CHANNEL_CLOSED,
    layer: MfupErrorLayer.DATA,
    message: "Data channel is closed, cannot send frames",
    action: "Upload will attempt reconnect.",
    fatal: false,
  });
}

export function sessionAbortedByServer(code: string, reason: string): MfupError {
  return new MfupError({
    code: MfupErrorCode.SESSION_ABORTED_BY_SERVER,
    layer: MfupErrorLayer.SESSION,
    message: `Server aborted session: [${code}] ${reason}`,
    action: "The server terminated this upload. Check server logs for the cause. Start a new upload.",
    fatal: true,
    detail: { serverCode: code, serverReason: reason },
  });
}

export function sessionReconnectExhausted(attempts: number, lastCause?: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.SESSION_RECONNECT_EXHAUSTED,
    layer: MfupErrorLayer.SESSION,
    message: `Reconnect failed after ${attempts} attempts`,
    action: "Check your network connection. You can retry the upload — the server may still have partial progress.",
    fatal: true,
    cause: lastCause,
    detail: { attempts },
  });
}

export function sessionReconnectFailed(attempt: number, cause: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.SESSION_RECONNECT_FAILED,
    layer: MfupErrorLayer.SESSION,
    message: `Reconnect attempt ${attempt} failed: ${cause instanceof Error ? cause.message : cause}`,
    action: "Will retry automatically.",
    fatal: false,
    cause,
    detail: { attempt },
  });
}

export function nackChunk(nodeId: number, expectedOffset: number, reason: string): MfupError {
  const codeMap: Record<string, MfupErrorCode> = {
    bad_checksum: MfupErrorCode.NACK_BAD_CHECKSUM,
    bad_offset: MfupErrorCode.NACK_BAD_OFFSET,
    stale_epoch: MfupErrorCode.NACK_STALE_EPOCH,
    server_policy: MfupErrorCode.NACK_SERVER_POLICY,
  };
  return new MfupError({
    code: codeMap[reason] ?? MfupErrorCode.UNKNOWN,
    layer: MfupErrorLayer.PROTOCOL,
    message: `Server rejected chunk for node ${nodeId}: ${reason} (expected offset ${expectedOffset})`,
    action: reason === "bad_checksum" ? "Data corruption detected. Chunk will be resent."
      : reason === "stale_epoch" ? "Session was resumed elsewhere. This tab's data is stale."
      : "Server rejected the data. Check server logs.",
    fatal: reason === "stale_epoch",
    detail: { nodeId, expectedOffset, reason },
  });
}

export function publishConflict(conflictingFiles: string[]): MfupError {
  return new MfupError({
    code: MfupErrorCode.PUBLISH_CONFLICT,
    layer: MfupErrorLayer.SESSION,
    message: `Publish blocked: ${conflictingFiles.length} entr${conflictingFiles.length === 1 ? "y" : "ies"} already exist in the target directory`,
    action: "Ask the user, then sendAction(\"merge_overwrite\") and publish() again — or sendAction(\"cancel\").",
    fatal: false,
    detail: { conflictingFiles },
  });
}

export function publishFailed(status: number, body: string): MfupError {
  return new MfupError({
    code: MfupErrorCode.PUBLISH_FAILED,
    layer: MfupErrorLayer.SESSION,
    message: `Publish failed: HTTP ${status} ${body.slice(0, 200)}`,
    action: status === 404 ? "Session unknown to the server (already published or cleaned up)."
      : "Check server logs.",
    fatal: status === 404,
    detail: { status, body: body.slice(0, 500) },
  });
}

export function ingestError(source: string, name: string, cause: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.INGEST_HANDLE_ERROR,
    layer: MfupErrorLayer.INGEST,
    message: `Failed to read "${name}" via ${source}: ${cause instanceof Error ? cause.message : cause}`,
    action: "The file or directory could not be read. Check permissions. The upload will skip this item.",
    fatal: false,
    cause,
    detail: { source, name },
  });
}

export function probeError(cause: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.UNKNOWN,
    layer: MfupErrorLayer.DATA,
    message: `Streaming probe failed: ${cause instanceof Error ? cause.message : cause}`,
    action: "Will use batch upload mode. This is normal for Firefox/Safari.",
    fatal: false,
    cause,
  });
}

export function unknownError(layer: MfupErrorLayer, message: string, cause?: unknown): MfupError {
  return new MfupError({
    code: MfupErrorCode.UNKNOWN,
    layer,
    message,
    action: "Unexpected error. Check browser console for details.",
    fatal: false,
    cause,
  });
}
