// MFUP/2 session orchestrator — coordinates control channel, data channel,
// ingestion, and progress into a single upload session.

import {
  FrameTag, NodeKind, ChecksumKind,
  ROOT_NODE_ID,
  crc32c,
  type NodeFrame,
  type SummaryFrame,
  type FileOpenFrame,
  type FileChunkFrame,
  type FileCloseFrame,
  type SessionEndFrame,
  type ClientAbortFrame,
  type DataFrame,
  type ResumeOkMsg,
  type SessionState,
  type ServerLimits,
} from "./protocol.js";
import { ControlChannel } from "./control.js";
import { DataChannel } from "./data-channel.js";
import { ProgressTracker, type ProgressSnapshot, type ProgressListener } from "./progress.js";
import {
  type DiscoveredNode,
  type IngestFilter,
  NodeIdAllocator,
  ingestFromHandles,
  ingestFromEntries,
  ingestFromFileList,
  ingestFromFiles,
} from "./ingestion.js";
import {
  MfupError,
  MfupErrorCode,
  MfupErrorLayer,
  wsConnectFailed,
  dataOpenFailed,
  dataHttpError,
  dataWriteFailed,
  sessionAbortedByServer,
  sessionReconnectExhausted,
  sessionReconnectFailed,
  nackChunk,
  ingestError,
  unknownError,
  probeError,
  publishConflict,
  publishFailed,
} from "./errors.js";
import { probeStreaming } from "./probe.js";
import type { UploadSource } from "./dnd.js";

// ---------------------------------------------------------------------------
// Public config
// ---------------------------------------------------------------------------

export interface MfupSessionConfig {
  /** Server base URL (http(s)://host) */
  serverUrl: string;
  /** Relative target directory on the server where files will be placed */
  targetDir?: string;
  /** Arbitrary JSON attached to the session (upload scope/purpose/ids).
   * Delivered to the server's authorize and map_file hooks via HELLO.meta.
   * Untrusted by the server until the consumer's hook validates it. */
  meta?: unknown;
  /** Existing session_id for resume, or omit for new session */
  sessionId?: string;
  /** Existing resume_token for resume */
  resumeToken?: string;
  /** Last known epoch (for resume) */
  lastKnownEpoch?: number;
  /** Default chunk size in bytes (default 256 KiB) */
  chunkSize?: number;
  /** Auto-reconnect attempts (default: unlimited). Set null/undefined for infinite retry. */
  maxReconnectAttempts?: number | null;
  /** Reconnect delay base in ms (default 1000, caps at 20s) */
  reconnectDelayMs?: number;
}

// ---------------------------------------------------------------------------
// Interactive server questions (ASK)
// ---------------------------------------------------------------------------

export type MfupAskAction = "merge_overwrite" | "cancel";

/**
 * A question the server asked mid-transfer (e.g. the target directory
 * already contains conflicting entries). The UI answers via respond();
 * the transfer keeps running while the question is pending — that is the
 * whole point of the non-blocking control channel.
 */
export interface MfupAsk {
  /** Monotonic per-session id, usable as a React key. */
  readonly id: number;
  /** Question kind. Currently always "target_conflict". */
  readonly code: string;
  /** Node id of the entry that triggered the question, when known. */
  readonly nodeId: number | null;
  /** Basename of the conflicting entry, when known. */
  readonly name: string | null;
  /** The answer given via respond(), or null while pending. */
  readonly answered: MfupAskAction | null;
  /** Answer the question. "cancel" is terminal for the whole session. */
  respond(action: MfupAskAction): void;
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/** Identity of one tracked file, attached to every file:* event. */
export interface MfupFileRef {
  nodeId: number;
  /** Client-relative path ("dir/sub/name.ext"), reconstructed from the scan. */
  path: string;
  size: bigint;
}

export interface MfupSessionEvents {
  /** Aggregate progress. Fires on every mutation — coalesce in UIs
   * (snapshot subscribers get coalescing for free). */
  progress: ProgressSnapshot;
  state: SessionState;
  committed: { files: number; bytes: number };
  /** Result of publish() — files were moved into the target directory. */
  published: { published: string[] };
  /** The server asked an interactive question; answer via ev.respond(). */
  ask: MfupAsk;
  /** A pending ask was answered (from any tab/code path). */
  "ask:answered": MfupAsk;
  /** A file's body started streaming to the server. */
  "file:start": MfupFileRef;
  /** The server durably accepted bytes for a file (FILE_ACK).
   * complete=true → the whole file is on the server. */
  "file:ack": MfupFileRef & { acceptedBytes: bigint; complete: boolean };
  /** The server (or the NACK budget) permanently skipped a file. */
  "file:reject": MfupFileRef & { code: string; reason: string };
  /** Terminal teardown, client- or server-initiated. */
  abort: { by: "client" | "server"; code: string; reason: string };
  error: MfupError;
  reconnecting: { attempt: number; delay: number; maxAttempts: number | null };
}

type Listener<T> = (ev: T) => void;

// ---------------------------------------------------------------------------
// Immutable state snapshot (for useSyncExternalStore-style consumers)
// ---------------------------------------------------------------------------

export interface MfupSessionSnapshot {
  sessionId: string;
  state: SessionState;
  epoch: number;
  /** null until the streaming probe ran. */
  streaming: boolean | null;
  progress: ProgressSnapshot;
  /** Blended 0–1 fraction (copy of progress.fraction for convenience). */
  fraction: number | null;
  /** All asks raised so far, in order. */
  asks: readonly MfupAsk[];
  /** Number of asks still awaiting respond(). */
  pendingAsks: number;
  /** Non-null while a reconnect loop is running. */
  reconnect: { attempt: number; delay: number; maxAttempts: number | null } | null;
  /** Set once COMMIT_OK arrived. */
  committed: { files: number; bytes: number } | null;
  /** Set once publish() succeeded. */
  published: readonly string[] | null;
  /** The file currently streaming, or null. */
  currentFile: MfupFileRef | null;
  /** Last few errors (ring of 20, newest last). */
  recentErrors: readonly MfupError[];
  /** First fatal error, if any — the reason the session died. */
  fatalError: MfupError | null;
}

/** How long snapshot notifications may be coalesced (ms). State-changing
 * events (state/ask/committed/abort) flush immediately; only high-frequency
 * progress ticks ride the timer. */
const SNAPSHOT_THROTTLE_MS = 33;

/**
 * crypto.randomUUID() exists only in secure contexts (https / localhost).
 * Consumers embedding the SDK on plain-HTTP intranets still need ids, so
 * fall back to an RFC 4122 v4 built from getRandomValues (available
 * everywhere crypto is).
 */
function genUUID(): string {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  b[6] = (b[6] & 0x0f) | 0x40; // version 4
  b[8] = (b[8] & 0x3f) | 0x80; // variant 10
  const h = Array.from(b, (x) => x.toString(16).padStart(2, "0"));
  return `${h[0]}${h[1]}${h[2]}${h[3]}-${h[4]}${h[5]}-${h[6]}${h[7]}-${h[8]}${h[9]}-${h.slice(10).join("")}`;
}

// ---------------------------------------------------------------------------
// Per-file tracking
// ---------------------------------------------------------------------------

interface TrackedFile {
  nodeId: number;
  size: bigint;
  acceptedOffset: bigint;
  openBody: (offsetBytes?: number) => AsyncIterable<Uint8Array>;
  status: "pending" | "streaming" | "sent" | "acked" | "rejected";
  /** Set by NACK handler — signals streamFile to abort and requeue. */
  nacked: boolean;
  /** NACKs received for this file — guards against endless requeue storms. */
  nackCount: number;
}

/** Give up on a single file after this many NACKs. */
const MAX_FILE_NACKS = 8;

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

export class MfupSession {
  // Identity
  private sessionId: string;
  private resumeToken: string;
  private legId: string;
  private epoch = 0;

  // Channels
  private control: ControlChannel | null = null;
  private data: DataChannel | null = null;
  private abortCtrl = new AbortController();

  // State
  private _state: SessionState = "active";
  private limits: ServerLimits = { max_chunk_bytes: 262144, max_open_files: 1, max_pending_files: 64 };
  private chunkSize: number;
  private paused = false;
  private pauseResolve: (() => void) | null = null;

  // Node / file tracking
  private ids = new NodeIdAllocator();
  private prunedNodes = new Set<number>();
  private rejectedFiles = new Set<number>();
  private trackedFiles = new Map<number, TrackedFile>();
  private fileQueue: TrackedFile[] = [];
  private activeFile: TrackedFile | null = null;

  // NODE frames by node id, in discovery order (parents before children).
  // Needed to re-send metadata the server never received: a data POST that
  // died in transit takes its NODE frames with it, and files unknown to the
  // server are invisible to its commit invariant. Dir entries are kept for
  // the session lifetime; file entries are dropped once the file is acked.
  private nodeMeta = new Map<number, NodeFrame>();
  // Set by COMMIT_RETRY carrying a node-count mismatch: re-send all metadata.
  private _resendAllMeta = false;

  // Scan tracking
  private scanDone = 0n;
  private scanEst = 0n;
  private bodyEstTotal = 0n;
  private bodyDoneTotal = 0n;
  private scanSealed = false;

  // Progress
  private progress = new ProgressTracker();

  // Events
  private listeners = new Map<string, Set<Listener<any>>>();

  // Snapshot store (useSyncExternalStore contract: subscribe + getSnapshot)
  private _snapVersion = 0;
  private _snapCache: MfupSessionSnapshot | null = null;
  private _snapCacheVersion = -1;
  private _snapSubs = new Set<() => void>();
  private _snapTimer: ReturnType<typeof setTimeout> | null = null;
  private _snapLastFlush = 0;

  // Interactive asks
  private asks: MfupAsk[] = [];
  private _askSeq = 0;

  // Terminal results / diagnostics for the snapshot
  private _committed: { files: number; bytes: number } | null = null;
  private _published: string[] | null = null;
  private _currentFileRef: MfupFileRef | null = null;
  private _reconnectInfo: { attempt: number; delay: number; maxAttempts: number | null } | null = null;
  private _recentErrors: MfupError[] = [];
  private _fatalError: MfupError | null = null;

  // Reconnect
  private maxReconnectAttempts: number | null;
  private reconnectDelayMs: number;
  private reconnectCount = 0;
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  // Config
  private serverUrl: string;
  private targetDir: string;
  private meta: unknown;

  // Data channel mode (detected via probe on first connect)
  private _streamingMode: boolean | null = null; // null = not yet probed

  // Pump synchronization — resolves when pumpFiles() finishes its current run
  private pumpDone: Promise<void> = Promise.resolve();
  private pumping = false;

  // Scan backpressure — pause ingestion when fileQueue grows too large
  private static readonly SCAN_HIGH_WATER = 10_000;
  private static readonly SCAN_LOW_WATER = 5_000;
  private _scanGateResolve: (() => void) | null = null;

  // Metadata frames buffered during disconnect (NODE, SUMMARY, DIR_CLOSE)
  private _pendingMeta: DataFrame[] = [];

  // Reconnect synchronization — resolves when reconnect completes
  private _reconnectPromise: Promise<void> | null = null;
  private _reconnectResolve: (() => void) | null = null;

  // Commit synchronization — resolves when COMMIT_OK arrives
  private _commitResolve: (() => void) | null = null;
  private _commitReject: ((err: Error) => void) | null = null;
  private _commitRetry = false;

  constructor(config: MfupSessionConfig) {
    // Accept page-relative server URLs too ("/api/uploads") — consumers
    // mounting the router under a prefix naturally write that, and the
    // WebSocket URL derivation needs an absolute http(s) base.
    const resolved = typeof location !== "undefined"
      ? new URL(config.serverUrl, location.origin).toString()
      : config.serverUrl;
    this.serverUrl = resolved.replace(/\/$/, "");
    this.targetDir = config.targetDir ?? ".";
    this.meta = config.meta;
    this.sessionId = config.sessionId ?? genUUID();
    // The resume token is SERVER-issued: empty until HELLO_OK delivers it
    // (probe/data/publish all happen after the handshake, so ordering is
    // safe). For a resumed session the caller passes the token it persisted.
    this.resumeToken = config.resumeToken ?? "";
    this.legId = genUUID();
    this.chunkSize = config.chunkSize ?? 262144;
    this.maxReconnectAttempts = config.maxReconnectAttempts ?? null;
    this.reconnectDelayMs = config.reconnectDelayMs ?? 1000;

    if (config.lastKnownEpoch != null) {
      this.epoch = config.lastKnownEpoch;
    }

    // Progress mutations feed both the typed event bus and the snapshot
    // store, so on("progress") and useSyncExternalStore see the same stream.
    this.progress.on((snap) => {
      this.emit("progress", snap);
      this.markSnapshotDirty();
    });
  }

  // -- public accessors ----------------------------------------------------

  get state(): SessionState { return this._state; }
  get id(): string { return this.sessionId; }
  get token(): string { return this.resumeToken; }
  get currentEpoch(): number { return this.epoch; }
  get streamingMode(): boolean | null { return this._streamingMode; }

  sendAction(action: MfupAskAction): void {
    // The ACTION is session-wide — settle every pending ask with it so the
    // snapshot and ask:answered listeners stay coherent regardless of
    // whether the UI answered via ask.respond() or sendAction() directly.
    for (const ask of this.asks) {
      if (ask.answered === null) {
        (ask as { answered: MfupAskAction | null }).answered = action;
        this.emit("ask:answered", ask);
      }
    }
    this.markSnapshotDirty(true);
    this.control?.sendAction(action);
    if (action === "cancel") {
      // A conflict cancel is terminal: the server aborts the session and
      // deletes its staging immediately, so any reconnect would only find
      // not_found. Tear down locally instead of reconnecting into the void.
      // (WebSocket delivery is ordered — ACTION is flushed before close.)
      this.abort("client_cancel", "user rejected overwrite");
    }
  }

  onProgress(fn: ProgressListener): () => void { return this.progress.on(fn); }

  // -- snapshot store (useSyncExternalStore contract) ------------------------

  /** Subscribe to snapshot changes. Notifications are coalesced (~33ms) for
   * high-frequency progress ticks; state-changing events flush immediately. */
  subscribe(fn: () => void): () => void {
    this._snapSubs.add(fn);
    return () => { this._snapSubs.delete(fn); };
  }

  /** Immutable state snapshot. Referentially stable until something changes —
   * safe to hand to React's useSyncExternalStore as getSnapshot. */
  getSnapshot(): MfupSessionSnapshot {
    if (this._snapCacheVersion !== this._snapVersion || this._snapCache === null) {
      const progress = this.progress.snapshot();
      let pendingAsks = 0;
      for (const a of this.asks) if (a.answered === null) pendingAsks++;
      this._snapCache = {
        sessionId: this.sessionId,
        state: this._state,
        epoch: this.epoch,
        streaming: this._streamingMode,
        progress,
        fraction: progress.fraction,
        asks: this.asks.slice(),
        pendingAsks,
        reconnect: this._reconnectInfo,
        committed: this._committed,
        published: this._published,
        currentFile: this._currentFileRef,
        recentErrors: this._recentErrors.slice(),
        fatalError: this._fatalError,
      };
      this._snapCacheVersion = this._snapVersion;
    }
    return this._snapCache;
  }

  private markSnapshotDirty(immediate = false): void {
    this._snapVersion++;
    if (this._snapSubs.size === 0) return;
    const now = Date.now();
    if (immediate || now - this._snapLastFlush >= SNAPSHOT_THROTTLE_MS) {
      this.flushSnapshot();
    } else if (this._snapTimer === null) {
      this._snapTimer = setTimeout(() => {
        this._snapTimer = null;
        this.flushSnapshot();
      }, SNAPSHOT_THROTTLE_MS);
    }
  }

  private flushSnapshot(): void {
    if (this._snapTimer !== null) {
      clearTimeout(this._snapTimer);
      this._snapTimer = null;
    }
    this._snapLastFlush = Date.now();
    for (const fn of this._snapSubs) fn();
  }

  on<K extends keyof MfupSessionEvents>(event: K, fn: Listener<MfupSessionEvents[K]>): () => void {
    let set = this.listeners.get(event);
    if (!set) { set = new Set(); this.listeners.set(event, set); }
    set.add(fn);
    return () => { set!.delete(fn); };
  }

  private emit<K extends keyof MfupSessionEvents>(event: K, data: MfupSessionEvents[K]): void {
    const set = this.listeners.get(event);
    if (set) for (const fn of set) fn(data);
  }

  getProgress(): ProgressSnapshot { return this.progress.snapshot(); }

  getResumeState(): { sessionId: string; resumeToken: string; epoch: number } {
    return { sessionId: this.sessionId, resumeToken: this.resumeToken, epoch: this.epoch };
  }

  // -- lifecycle -----------------------------------------------------------

  async connect(): Promise<void> {
    this.legId = genUUID();
    const isResume = this.epoch > 0;

    // Build WS URL — handle both http:// and https:// origins
    const wsUrl = this.serverUrl.replace(/^http/, "ws") + "/mfup/control";

    this.control = new ControlChannel({
      url: wsUrl,
      sessionId: this.sessionId,
      resumeToken: this.resumeToken,
      legId: this.legId,
      targetDir: this.targetDir,
      meta: this.meta,
      lastKnownEpoch: isResume ? this.epoch : undefined,
    });

    this.wireControlEvents();
    this.control.open();

    try {
      await this.control.ready();
    } catch (err) {
      // Preserve typed fatal rejections (SESSION_ABORT: not_found /
      // auth_failed / bad_version) — the reconnect loop keys on `fatal`.
      const mfupErr = err instanceof MfupError ? err : wsConnectFailed(wsUrl, err);
      this.emitError(mfupErr);
      throw mfupErr;
    }

    // Reset abort controller for this leg
    this.abortCtrl = new AbortController();

    // Probe streaming support on first connect (cached for session lifetime,
    // and across sessions per origin via localStorage — the verdict depends
    // on browser + transport path, neither of which changes between visits).
    if (this._streamingMode === null) {
      this._streamingMode = this.readProbeCache();
    }
    if (this._streamingMode === null) {
      try {
        const probe = await probeStreaming({
          baseUrl: this.serverUrl,
          sessionId: this.sessionId,
          resumeToken: this.resumeToken,
          control: this.control,
          signal: this.abortCtrl.signal,
        });
        this._streamingMode = probe.streaming;
        this.writeProbeCache(probe.streaming);
      } catch (err) {
        this._streamingMode = false;
        this.emitError(probeError(err));
      }
    }

    // Control is established — open data channel
    this.data = new DataChannel({
      baseUrl: this.serverUrl,
      sessionId: this.sessionId,
      legId: this.legId,
      resumeToken: this.resumeToken,
      epoch: this.epoch,
      signal: this.abortCtrl.signal,
      streaming: this._streamingMode,
    });

    // Forward data channel errors — trigger reconnect for network failures
    // Capture reference so stale channels don't trigger reconnect on new channel
    const dc = this.data;
    this.data.onError((err) => {
      if (this.data !== dc) return; // stale channel — ignore
      this.emitError(err);
      if (err.code === MfupErrorCode.DATA_WRITE_FAILED || err.code === MfupErrorCode.DATA_HTTP_ERROR) {
        this.handleDisconnect(err);
      }
    });

    // Fire-and-forget — handle both streaming (Response) and batch (void) results
    const dataUrl = `${this.serverUrl}/mfup/data/${this.sessionId}/${this.legId}`;
    const openDc = this.data;
    this.data.open().then(
      async (result) => {
        if (this.data !== openDc) return; // stale channel
        // Streaming mode returns a Response; batch mode returns void
        if (result && "ok" in result && !result.ok && !this.abortCtrl.signal.aborted) {
          const body = await (result as Response).text().catch(() => "");
          const err = dataHttpError(dataUrl, (result as Response).status, (result as Response).statusText, body);
          this.emitError(err);
          this.handleDisconnect(err);
        }
      },
      (err) => {
        if (this.data !== openDc) return; // stale channel
        if (!this.abortCtrl.signal.aborted) {
          const mfupErr = err instanceof MfupError ? err : dataOpenFailed(dataUrl, err);
          this.emitError(mfupErr);
          this.handleDisconnect(mfupErr);
        }
      },
    );

    this._reconnectInfo = null;
    this.setState("active");
    this.reconnectCount = 0;
  }

  /**
   * Upload from FileSystemHandle roots (drag-and-drop / picker).
   * Resolves when the server returns COMMIT_OK (or rejects on fatal error).
   */
  async uploadHandles(handles: FileSystemHandle[]): Promise<void> {
    const filter = this.makeIngestFilter();
    try {
      await ingestFromHandles(handles, ROOT_NODE_ID, this.ids, this.onDiscover.bind(this), filter);
    } catch (err) {
      const mfupErr = err instanceof MfupError ? err : ingestError("handles", "FileSystemHandle[]", err);
      this.emitError(mfupErr);
      throw mfupErr;
    }
    return this.finalizeScan();
  }

  /**
   * Upload from FileSystemEntry roots (webkitGetAsEntry — Firefox/Safari).
   * Entries persist after the drop handler; files are read lazily via streaming.
   */
  async uploadEntries(entries: FileSystemEntry[]): Promise<void> {
    const filter = this.makeIngestFilter();
    try {
      await ingestFromEntries(entries, ROOT_NODE_ID, this.ids, this.onDiscover.bind(this), filter);
    } catch (err) {
      const mfupErr = err instanceof MfupError ? err : ingestError("entries", "FileSystemEntry[]", err);
      this.emitError(mfupErr);
      throw mfupErr;
    }
    return this.finalizeScan();
  }

  async uploadFileList(files: ArrayLike<File>): Promise<void> {
    const filter = this.makeIngestFilter();
    try {
      await ingestFromFileList(files, ROOT_NODE_ID, this.ids, this.onDiscover.bind(this), filter);
    } catch (err) {
      const mfupErr = err instanceof MfupError ? err : ingestError("filelist", "FileList", err);
      this.emitError(mfupErr);
      throw mfupErr;
    }
    return this.finalizeScan();
  }

  async uploadFiles(files: File[]): Promise<void> {
    const filter = this.makeIngestFilter();
    try {
      await ingestFromFiles(files, ROOT_NODE_ID, this.ids, this.onDiscover.bind(this), filter);
    } catch (err) {
      const mfupErr = err instanceof MfupError ? err : ingestError("files", "File[]", err);
      this.emitError(mfupErr);
      throw mfupErr;
    }
    return this.finalizeScan();
  }

  /**
   * Upload from a normalised source — the output of sourceFromDataTransfer /
   * sourceFromInput (see dnd.ts) — or a plain FileList / File[].
   */
  async upload(source: UploadSource | FileList | File[]): Promise<void> {
    if (Array.isArray(source)) return this.uploadFiles(source);
    if (typeof FileList !== "undefined" && source instanceof FileList) {
      return this.uploadFileList(source);
    }
    const s = source as UploadSource;
    switch (s.kind) {
      case "handles": return this.uploadHandles(await s.handles);
      case "entries": return this.uploadEntries(s.entries);
      case "filelist": return this.uploadFileList(s.files);
      case "files": return this.uploadFiles(s.files);
    }
  }

  /**
   * Wait until every pending ask has been answered (by any code path).
   *
   * Returns "cancel" if any answer was cancel, "merge_overwrite" if asks
   * were raised and all resolved positively, or null when no ask was ever
   * raised. Resolves early (with the current verdict) if the session hits a
   * terminal state while a question is still open — never hangs.
   *
   * Typical flow: `await upload(...); if (await settleAsks() !== "cancel") await publish();`
   */
  async settleAsks(): Promise<MfupAskAction | null> {
    const terminal = () =>
      this._state === "committed" ? false
        : this._state === "aborted" || this._state === "failed";
    while (this.asks.some((a) => a.answered === null) && !terminal()) {
      await new Promise<void>((resolve) => {
        const offs: (() => void)[] = [];
        const done = () => { for (const off of offs) off(); resolve(); };
        offs.push(this.on("ask:answered", done));
        offs.push(this.on("state", done));
      });
    }
    for (const a of this.asks) if (a.answered === "cancel") return "cancel";
    return this.asks.length > 0 ? "merge_overwrite" : null;
  }

  /**
   * Publish the committed session: the server moves staged files into the
   * target directory (atomic renames) and forgets the session.
   *
   * On a 409 target conflict, throws MfupError(PUBLISH_CONFLICT) with
   * detail.conflictingFiles — ask the user, sendAction("merge_overwrite"),
   * and call publish() again (or sendAction("cancel") to drop everything).
   */
  async publish(): Promise<{ published: string[] }> {
    const url = `${this.serverUrl}/mfup/sessions/${this.sessionId}/publish`;
    let resp: Response;
    try {
      resp = await fetch(url, {
        method: "POST",
        headers: { "X-MFUP-Token": this.resumeToken },
      });
    } catch (cause) {
      const err = publishFailed(0, cause instanceof Error ? cause.message : String(cause));
      this.emitError(err);
      throw err;
    }

    if (resp.ok) {
      const data = await resp.json().catch(() => ({}));
      const published: string[] = data.published ?? [];
      this._published = published;
      this.markSnapshotDirty(true);
      this.emit("published", { published });
      return { published };
    }

    const body = await resp.text().catch(() => "");
    if (resp.status === 409) {
      let parsed: any = null;
      try { parsed = JSON.parse(body); } catch { /* not JSON */ }
      if (parsed && parsed.error === "conflict_files") {
        const err = publishConflict(parsed.conflicting_files ?? []);
        this.emitError(err);
        throw err;
      }
    }
    const err = publishFailed(resp.status, body);
    this.emitError(err);
    throw err;
  }

  abort(code = "client_cancel", reason = "user cancelled"): void {
    // Cancel pending reconnect timer
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    // Resolve reconnect promise so finalizeScan unblocks
    this._reconnectResolve?.();
    this._reconnectPromise = null;
    this._reconnectResolve = null;

    // Unblock scan gate so ingestion doesn't hang
    this._scanGateResolve?.();
    this._scanGateResolve = null;

    this.control?.sendAbort(code, reason);
    const frame: ClientAbortFrame = { tag: FrameTag.CLIENT_ABORT, code, reason };
    try { this.data?.write(frame); } catch { /* expected during abort teardown */ }
    this.data?.close();
    this.control?.close();
    this.abortCtrl.abort();
    this.setState("aborted");
    this.emit("abort", { by: "client", code, reason });
    // Reject any pending commit wait
    this._commitReject?.(new Error("aborted"));
  }

  // -- ingestion callback --------------------------------------------------

  private async onDiscover(node: DiscoveredNode): Promise<void> {
    const nodeFrame: NodeFrame = {
      tag: FrameTag.NODE,
      nodeId: node.nodeId,
      parentId: node.parentId,
      kind: node.kind,
      name: node.name,
      sizeHint: node.size,
      mtimeMs: node.mtimeMs,
    };
    this.nodeMeta.set(node.nodeId, nodeFrame);
    this.safeWrite(nodeFrame);

    this.scanDone++;
    this.scanEst++;

    if (node.kind === NodeKind.FILE && node.openBody) {
      const tracked: TrackedFile = {
        nodeId: node.nodeId,
        size: node.size ?? 0n,
        acceptedOffset: 0n,
        openBody: node.openBody,
        status: "pending",
        nacked: false,
        nackCount: 0,
      };
      this.trackedFiles.set(node.nodeId, tracked);
      this.fileQueue.push(tracked);
      this.bodyEstTotal += tracked.size;
      this.progress.addBodyEstimate(tracked.size);
    }

    if (Number(this.scanDone) % 50 === 0) {
      this.emitSummary();
    }

    // Kick the file pump — capture the promise but don't await (scan + pump run concurrently)
    this.kickPump();

    // Backpressure: pause scan when fileQueue exceeds high water mark
    if (this.fileQueue.length >= MfupSession.SCAN_HIGH_WATER) {
      await new Promise<void>((resolve) => {
        this._scanGateResolve = resolve;
      });
    }
  }

  // -- file streaming pump -------------------------------------------------

  private kickPump(): void {
    if (this.pumping) return;
    this.pumpDone = this.pumpFiles();
  }

  private async pumpFiles(): Promise<void> {
    if (this.pumping) return;
    this.pumping = true;

    try {
      while (this.fileQueue.length > 0 && this._state === "active") {
        await this.waitIfPaused();

        // Data channel died — break and let handleDisconnect deal with it
        if (this.data?.failed || this.data?.closed) break;

        const file = this.fileQueue[0];
        if (this.rejectedFiles.has(file.nodeId) || this.isNodePruned(file.nodeId)) {
          this.fileQueue.shift();
          file.status = "rejected";
          this.progress.skipFile();
          continue;
        }

        this.fileQueue.shift();
        // Unblock scan when queue drains below low water mark
        if (this._scanGateResolve && this.fileQueue.length < MfupSession.SCAN_LOW_WATER) {
          this._scanGateResolve();
          this._scanGateResolve = null;
        }
        this.activeFile = file;
        try {
          await this.streamFile(file);
        } catch (err) {
          // If data channel died during streaming, don't throw — just break
          if (this.data?.failed || this.data?.closed) {
            this.activeFile = null;
            break;
          }
          const mfupErr = err instanceof MfupError ? err : ingestError(
            "stream", `node ${file.nodeId}`, err,
          );
          this.emitError(mfupErr);
          throw mfupErr;
        }
        this.activeFile = null;
        this._currentFileRef = null;
        this.markSnapshotDirty();
      }
    } finally {
      this.pumping = false;
    }
  }

  private async streamFile(file: TrackedFile): Promise<void> {
    file.status = "streaming";
    this._currentFileRef = this.fileRef(file.nodeId, file.size);
    this.markSnapshotDirty();
    this.emit("file:start", this._currentFileRef);

    const openFrame: FileOpenFrame = {
      tag: FrameTag.FILE_OPEN,
      nodeId: file.nodeId,
      size: file.size,
      mtimeMs: null,
    };
    this.safeWrite(openFrame);

    const startOffset = Number(file.acceptedOffset);
    const chunks = file.openBody(startOffset);
    let offset = file.acceptedOffset;
    const maxChunk = Math.min(this.chunkSize, this.limits.max_chunk_bytes);

    for await (const raw of chunks) {
      // Split raw chunk into maxChunk-sized pieces
      let pos = 0;
      while (pos < raw.length) {
        await this.waitIfPaused();

        if (this.rejectedFiles.has(file.nodeId)) {
          file.status = "rejected";
          this.progress.skipFile();
          return;
        }

        const end = Math.min(pos + maxChunk, raw.length);
        const piece = pos === 0 && end === raw.length ? raw : raw.subarray(pos, end);
        pos = end;

        const checksum = crc32c(piece);

        const chunkFrame: FileChunkFrame = {
          tag: FrameTag.FILE_CHUNK,
          nodeId: file.nodeId,
          offset,
          length: piece.length,
          checksumKind: ChecksumKind.CRC32C,
          checksum,
          payload: piece,
        };
        this.safeWrite(chunkFrame);
        offset += BigInt(piece.length);

        // Backpressure: in batch mode, flush if buffer exceeds threshold
        if (this.data) {
          await this.data.drain();
          if (this.data.failed) return; // channel died — bail out
        }

        // NACK recovery: server rejected a chunk — restart from its offset.
        if (file.nacked) {
          file.nacked = false;
          file.status = "pending";
          this.fileQueue.push(file);
          return;
        }
      }
    }

    const closeFrame: FileCloseFrame = {
      tag: FrameTag.FILE_CLOSE,
      nodeId: file.nodeId,
      sizeSent: offset,
    };
    this.safeWrite(closeFrame);
    file.status = "sent";
  }

  // -- scan finalization ---------------------------------------------------

  private async finalizeScan(): Promise<void> {
    this.scanSealed = true;
    this.emitSummary();

    // Wait for the file pump to fully drain
    await this.pumpDone;

    // If a disconnect happened during pumping, wait for reconnect then re-drain
    while (this._state === "waiting_resume" || this.fileQueue.length > 0) {
      if (this._reconnectPromise) {
        await this._reconnectPromise;
      }
      if (this._state === "failed" || this._state === "aborted") {
        throw new Error(`session ended in state: ${this._state}`);
      }
      if (this.fileQueue.length > 0) {
        this.kickPump();
        await this.pumpDone;
      }
    }

    // Commit loop: send SESSION_END, wait for COMMIT_OK or COMMIT_RETRY
    while (true) {
      const endFrame: SessionEndFrame = {
        tag: FrameTag.SESSION_END,
        rootSummary: {
          scanDoneUnits: this.scanDone,
          scanEstUnits: this.scanEst,
          bodyDoneBytes: this.bodyDoneTotal,
          bodyEstBytes: this.bodyEstTotal,
          sealed: true,
        },
      };
      this.safeWrite(endFrame);
      this.setState("committing");
      await this.data?.close();

      // In batch mode, the final POST response may already contain the commit result.
      if (this._state === "committing" && this.data?.commitResult) {
        const cr = this.data.commitResult;
        this.markCommitted(cr.files, cr.bytes);
        return;
      }

      // Wait for COMMIT_OK or COMMIT_RETRY from the control channel.
      // _commitRetry may already be true if handleDisconnect fired before we
      // reached this point (race: reconnect sets flag before promise exists).
      if (!this._commitRetry) {
        await new Promise<void>((resolve, reject) => {
          if (this._state === "committed") { resolve(); return; }
          if (this._state === "aborted" || this._state === "failed") {
            reject(new Error(`session ended in state: ${this._state}`)); return;
          }
          this._commitResolve = resolve;
          this._commitReject = reject;
        });
        // Clear resolve/reject so stale calls don't fire later
        this._commitResolve = null;
        this._commitReject = null;
      }

      if (!this._commitRetry) return; // COMMIT_OK — done
      this._commitRetry = false;

      // COMMIT_RETRY — server reverted to ACTIVE, requeue happened in handler.
      // Abort old data channel, open fresh one, pump incomplete files.
      this.data?.abort("commit_retry");
      this.setState("active");
      this.data = new DataChannel({
        baseUrl: this.serverUrl,
        sessionId: this.sessionId,
        legId: this.legId,
        resumeToken: this.resumeToken,
        epoch: this.epoch,
        signal: this.abortCtrl.signal,
        streaming: this._streamingMode ?? false,
      });
      const retryDc = this.data;
      this.data.onError((err) => {
        if (this.data !== retryDc) return; // stale channel
        this.emitError(err);
        if (err.code === MfupErrorCode.DATA_WRITE_FAILED || err.code === MfupErrorCode.DATA_HTTP_ERROR) {
          this.handleDisconnect(err);
        }
      });
      this.data.open().catch(() => {});

      // Node-count mismatch retry: replay all node metadata we still hold
      // (dirs for the whole session + files not yet acked) before bodies.
      if (this._resendAllMeta) {
        this._resendAllMeta = false;
        for (const frame of this.nodeMeta.values()) {
          this.safeWrite(frame);
        }
      }

      this.kickPump();
      await this.pumpDone;

      // Drain any remaining after reconnect (state may change async after awaits)
      while ((this._state as string) === "waiting_resume" || this.fileQueue.length > 0) {
        if (this._reconnectPromise) await this._reconnectPromise;
        if (this._state === "failed" || this._state === "aborted") {
          throw new Error(`session ended in state: ${this._state}`);
        }
        if (this.fileQueue.length > 0) {
          this.kickPump();
          await this.pumpDone;
        }
      }
    }
  }

  // -- helpers -------------------------------------------------------------

  private emitSummary(): void {
    const frame: SummaryFrame = {
      tag: FrameTag.SUMMARY,
      nodeId: ROOT_NODE_ID,
      scanDoneUnits: this.scanDone,
      scanEstUnits: this.scanEst,
      bodyDoneBytes: this.bodyDoneTotal,
      bodyEstBytes: this.bodyEstTotal,
      sealed: this.scanSealed,
    };
    this.safeWrite(frame);
    this.progress.updateScan(this.scanDone, this.scanEst, this.scanSealed);
  }

  private makeIngestFilter(): IngestFilter {
    return {
      shouldDescend: (nodeId) => !this.prunedNodes.has(nodeId),
      shouldInclude: (nodeId) => !this.rejectedFiles.has(nodeId),
    };
  }

  private isNodePruned(nodeId: number): boolean {
    return this.prunedNodes.has(nodeId);
  }

  private async waitIfPaused(): Promise<void> {
    if (!this.paused) return;
    await new Promise<void>((resolve) => { this.pauseResolve = resolve; });
  }

  private markCommitted(files: number, bytes: number): void {
    this._committed = { files, bytes };
    this.setState("committed");
    this.emit("committed", { files, bytes });
  }

  private setState(s: SessionState): void {
    this._state = s;
    if (s === "committed" || s === "aborted" || s === "failed") {
      this._reconnectInfo = null;
      this._currentFileRef = null;
    }
    this.markSnapshotDirty(true);
    this.emit("state", s);
  }

  private emitError(err: MfupError): void {
    this._recentErrors.push(err);
    if (this._recentErrors.length > 20) this._recentErrors.shift();
    if (err.fatal && this._fatalError === null) this._fatalError = err;
    this.markSnapshotDirty(err.fatal);
    this.emit("error", err);
  }

  /** Reconstruct a node's client-relative path from the scan metadata. */
  private nodePath(nodeId: number): string {
    const parts: string[] = [];
    let cur = this.nodeMeta.get(nodeId);
    const seen = new Set<number>();
    while (cur && !seen.has(cur.nodeId)) {
      seen.add(cur.nodeId);
      parts.push(cur.name);
      if (cur.parentId === ROOT_NODE_ID) break;
      cur = this.nodeMeta.get(cur.parentId);
    }
    return parts.reverse().join("/");
  }

  private fileRef(nodeId: number, size: bigint): MfupFileRef {
    const path = this.nodePath(nodeId);
    return { nodeId, path: path || String(nodeId), size };
  }

  /** Write to data channel with error propagation instead of silent drops */
  private safeWrite(frame: Parameters<DataChannel["write"]>[0]): void {
    if (!this.data || this.data.closed || this.data.failed) {
      // If reconnecting, buffer metadata for replay; file frames are requeued separately
      if (this._state === "waiting_resume") {
        const t = frame.tag;
        if (t === FrameTag.NODE || t === FrameTag.SUMMARY || t === FrameTag.DIR_CLOSE) {
          this._pendingMeta.push(frame);
        }
        return;
      }
      const err = dataWriteFailed("data channel not available");
      this.emitError(err);
      return;
    }
    try {
      this.data.write(frame);
    } catch (cause) {
      if (cause instanceof MfupError) {
        this.emitError(cause);
      } else {
        this.emitError(dataWriteFailed("write failed", cause));
      }
    }
  }

  // -- control event wiring ------------------------------------------------

  private wireControlEvents(): void {
    if (!this.control) return;

    this.control.on("hello_ok", (msg) => {
      this.epoch = msg.epoch;
      this.limits = msg.limits;
      // Adopt the server-issued bearer token — used by probe, data POSTs,
      // publish, and any future RESUME.
      this.resumeToken = msg.resume_token;
      this.progress.setExpiresAt(msg.expires_at);
      this.markSnapshotDirty(true);
    });

    this.control.on("resume_ok", (msg) => {
      this.epoch = msg.epoch;
      this.progress.setExpiresAt(msg.expires_at);
      this.progress.setFromRootSummary(msg.root_summary);
      this.applyResumeState(msg);
    });

    this.control.on("file_ack", (msg) => {
      const file = this.trackedFiles.get(msg.node_id);
      if (file) {
        const prev = file.acceptedOffset;
        file.acceptedOffset = BigInt(msg.accepted_offset);
        const delta = file.acceptedOffset - prev;
        if (delta > 0n) {
          this.bodyDoneTotal += delta;
          this.progress.setBodyAccepted(this.bodyDoneTotal);
          // Forward progress — the NACK budget counts CONSECUTIVE failures,
          // so a file that recovers after some NACKs is not falsely dropped.
          file.nackCount = 0;
        }
        const complete = file.status === "sent" && file.acceptedOffset >= file.size;
        // Resolve the path BEFORE nodeMeta forgets a completed file.
        const ref = this.fileRef(msg.node_id, file.size);
        if (complete) {
          file.status = "acked";
          // Fully accepted — the server durably knows this node.
          this.nodeMeta.delete(msg.node_id);
          this.progress.acceptFile();
        }
        this.emit("file:ack", { ...ref, acceptedBytes: file.acceptedOffset, complete });
      }
    });

    this.control.on("nack_chunk", (msg) => {
      const file = this.trackedFiles.get(msg.node_id);
      if (file) {
        file.nackCount++;
        if (file.nackCount > MAX_FILE_NACKS) {
          // Endless NACK/requeue loops must not spin forever — give up on
          // this one file, keep the session alive.
          this.rejectedFiles.add(msg.node_id);
          if (file.status !== "rejected") {
            file.status = "rejected";
            this.progress.skipFile();
            this.emit("file:reject", {
              ...this.fileRef(msg.node_id, file.size),
              code: "nack_budget",
              reason: `gave up after ${file.nackCount} NACKs (${msg.reason})`,
            });
          }
          this.emitError(nackChunk(msg.node_id, msg.expected_offset, msg.reason));
          return;
        }
      }
      // unknown_node: the server never saw this node's NODE frame (lost with
      // a dead POST). Re-send the parent chain + node before the re-stream;
      // frames are ordered within the channel, so NODEs land first.
      if (msg.reason === "unknown_node") {
        this.resendNodeChain(msg.node_id);
      }
      if (file && (file.status === "streaming" || file.status === "sent")) {
        file.acceptedOffset = BigInt(msg.expected_offset);
        if (file.status === "streaming") {
          file.nacked = true; // signal streamFile to abort and requeue
        } else {
          // Already "sent" — requeue directly
          file.status = "pending";
          this.fileQueue.push(file);
          this.kickPump();
        }
      }
      this.emitError(nackChunk(msg.node_id, msg.expected_offset, msg.reason));
    });

    this.control.on("flow", (msg) => {
      this.paused = msg.paused;
      if (!msg.paused && this.pauseResolve) {
        this.pauseResolve();
        this.pauseResolve = null;
      }
      if (msg.paused) {
        this.setState("paused_by_server");
      } else if (this._state === "paused_by_server") {
        this.setState("active");
      }
    });

    this.control.on("prune_node", (msg) => {
      this.prunedNodes.add(msg.node_id);
    });

    this.control.on("reject_file", (msg) => {
      this.rejectedFiles.add(msg.node_id);
      const file = this.trackedFiles.get(msg.node_id);
      if (file) {
        file.status = "rejected";
        this.progress.skipFile();
        this.emit("file:reject", {
          ...this.fileRef(msg.node_id, file.size),
          code: msg.code,
          reason: msg.reason,
        });
      }
    });

    this.control.on("session_abort", (msg) => {
      this.setState("aborted");
      this.data?.abort("server aborted session");
      // Close the control WS so the server's finally-block reclaims the
      // staging dir + Redis entry immediately (e.g. storage_full aborts).
      this.control?.close();
      // Stop any reconnect loop — a server abort is terminal.
      if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
      this._reconnectResolve?.();
      this._reconnectPromise = null;
      this._reconnectResolve = null;
      this._scanGateResolve?.();
      this._scanGateResolve = null;
      this.emit("abort", { by: "server", code: msg.code, reason: msg.reason });
      this.emitError(sessionAbortedByServer(msg.code, msg.reason));
      this._commitReject?.(new Error(`session aborted: ${msg.code}`));
    });

    this.control.on("commit_ok", (msg) => {
      this.markCommitted(msg.files, msg.bytes);
      this._commitResolve?.();
    });

    this.control.on("commit_retry", (msg) => {
      // Epoch stays the same — only attach_leg() (reconnect) bumps epoch.
      // Server says these files are incomplete — requeue them and retry
      for (const entry of msg.incomplete) {
        const file = this.trackedFiles.get(entry.node_id);
        // Do not resurrect a file we already gave up on (NACK storm / rejected)
        // — that would ping-pong COMMIT_RETRY with the server forever.
        if (file && file.status !== "acked" && file.status !== "rejected"
            && !this.rejectedFiles.has(entry.node_id)) {
          file.acceptedOffset = BigInt(entry.accepted_offset);
          file.status = "pending";
          this.fileQueue.push(file);
        }
      }
      // Node-count mismatch: NODE frames were lost — re-send all metadata
      // we still hold, and requeue every not-yet-acked file body.
      if (msg.nodes_expected != null && msg.nodes_seen != null
          && msg.nodes_expected !== msg.nodes_seen) {
        this._resendAllMeta = true;
        for (const file of this.trackedFiles.values()) {
          if (file.status !== "acked" && file.status !== "rejected"
              && !this.fileQueue.includes(file)) {
            file.status = "pending";
            this.fileQueue.push(file);
          }
        }
      }
      // Signal finalizeScan to retry
      this._commitRetry = true;
      this._commitResolve?.();
    });

    this.control.on("ask", (msg) => {
      const ask: MfupAsk = {
        id: this._askSeq++,
        code: msg.code ?? "target_conflict",
        nodeId: msg.node_id ?? null,
        name: msg.name ?? null,
        answered: null,
        // The ACTION is session-wide, so respond() routes through
        // sendAction(), which settles every pending ask coherently.
        respond: (action: MfupAskAction) => {
          if (ask.answered !== null) return;
          this.sendAction(action);
        },
      };
      this.asks.push(ask);
      this.markSnapshotDirty(true);
      this.emit("ask", ask);
    });

    this.control.on("close", () => {
      if (this._state === "active" || this._state === "paused_by_server") {
        this.handleDisconnect(unknownError(MfupErrorLayer.CONTROL, "control channel closed unexpectedly"));
      }
    });

    this.control.on("error", (err) => {
      this.emitError(err);
    });
  }

  // -- resume / reconnect --------------------------------------------------

  private applyResumeState(msg: ResumeOkMsg): void {
    for (const pn of msg.pruned_nodes) this.prunedNodes.add(pn);
    for (const rn of msg.rejected_files) this.rejectedFiles.add(rn);

    let acceptedCount = 0;
    let skippedCount = msg.pruned_nodes.length;

    for (const fs of msg.files) {
      const file = this.trackedFiles.get(fs.node_id);
      if (file) {
        file.acceptedOffset = BigInt(fs.accepted_offset);
        if (fs.status === "rejected") {
          file.status = "rejected";
          this.rejectedFiles.add(fs.node_id);
          skippedCount++;
        } else if (fs.status === "closed") {
          file.status = "acked";
          acceptedCount++;
        }
      } else {
        // File not yet in trackedFiles (scan hasn't re-discovered it yet)
        if (fs.status === "closed") acceptedCount++;
        else if (fs.status === "rejected") skippedCount++;
      }
    }

    this.progress.setFileCounts(acceptedCount, skippedCount);
  }

  private async handleDisconnect(err: unknown): Promise<void> {
    if (this._state === "committed" || this._state === "aborted") return;
    // Prevent duplicate calls (WS close + data error can both fire)
    if (this._state === "waiting_resume") return;

    // If disconnected while committing, unblock finalizeScan so it retries
    const wasCommitting = this._state === "committing";

    this.setState("waiting_resume");
    this.data?.abort("disconnected");

    // Single reconnect promise that stays pending until success or give-up.
    // finalizeScan awaits this — if we resolved+nulled it between retries,
    // finalizeScan's while loop would spin with no await (busy loop → tab freeze).
    this._reconnectPromise = new Promise<void>((resolve) => {
      this._reconnectResolve = resolve;
    });

    // Retry loop (replaces recursive calls that broke the state guard)
    while (true) {
      if (this.maxReconnectAttempts != null && this.reconnectCount >= this.maxReconnectAttempts) {
        this.setState("failed");
        const exhaustedErr = sessionReconnectExhausted(this.reconnectCount, err);
        this.emitError(exhaustedErr);
        this._commitReject?.(exhaustedErr);
        this._reconnectResolve?.();
        this._reconnectPromise = null;
        this._reconnectResolve = null;
        return;
      }

      this.reconnectCount++;
      const delay = Math.min(
        this.reconnectDelayMs * Math.pow(2, this.reconnectCount - 1),
        20_000,
      );

      // Emit reconnecting event BEFORE the delay so UI updates immediately
      this._reconnectInfo = {
        attempt: this.reconnectCount,
        delay,
        maxAttempts: this.maxReconnectAttempts,
      };
      this.markSnapshotDirty(true);
      this.emit("reconnecting", this._reconnectInfo);

      this.emitError(sessionReconnectFailed(this.reconnectCount, err));

      await new Promise((r) => {
        this._reconnectTimer = setTimeout(r, delay);
      });
      this._reconnectTimer = null;

      // Check if abort() was called during the delay
      if ((this._state as SessionState) === "aborted") {
        this._reconnectResolve?.();
        this._reconnectPromise = null;
        this._reconnectResolve = null;
        return;
      }

      try {
        await this.connect();
        this.requeuePendingFiles();
        this.kickPump();
        // Signal reconnect complete — finalizeScan can proceed
        this._reconnectResolve?.();
        this._reconnectPromise = null;
        this._reconnectResolve = null;
        // If disconnect happened during committing, unblock finalizeScan to retry
        if (wasCommitting) {
          this._commitRetry = true;
          this._commitResolve?.();
        }
        return; // success
      } catch (reconnectErr) {
        // Clean up partially-opened channels from failed connect()
        this.data?.abort("reconnect failed");
        this.control?.close();
        // A fatal handshake rejection can never be retried into success —
        // e.g. the server aborted+cleaned the session (cancel path) and
        // RESUME now yields not_found. Without this check the client would
        // reconnect forever into the void.
        if (reconnectErr instanceof MfupError && reconnectErr.fatal) {
          this.setState("failed");
          this._commitReject?.(reconnectErr);
          this._reconnectResolve?.();
          this._reconnectPromise = null;
          this._reconnectResolve = null;
          return;
        }
        err = reconnectErr;
        // Loop continues — next retry with backoff
      }
    }
  }

  /**
   * Re-send the NODE frames for a node's parent chain (root→leaf order).
   * Used when the server reports unknown_node — its copy of the metadata
   * was lost with a failed data POST.
   */
  private resendNodeChain(nodeId: number): void {
    const chain: NodeFrame[] = [];
    let cur = this.nodeMeta.get(nodeId);
    const seen = new Set<number>();
    while (cur && !seen.has(cur.nodeId)) {
      seen.add(cur.nodeId);
      chain.push(cur);
      if (cur.parentId === ROOT_NODE_ID) break;
      cur = this.nodeMeta.get(cur.parentId);
    }
    for (let i = chain.length - 1; i >= 0; i--) {
      this.safeWrite(chain[i]);
    }
  }

  private requeuePendingFiles(): void {
    // Replay buffered metadata frames (NODE, SUMMARY, DIR_CLOSE) first
    for (const frame of this._pendingMeta) {
      this.safeWrite(frame);
    }
    this._pendingMeta = [];

    // Re-send the ENTIRE known node tree, not just requeued files' chains.
    // Frames handed to the dead channel (buffered but never delivered) are
    // gone, and RESUME_OK only lists what the server DID receive. Resending
    // every NODE we still hold — idempotent server-side (INSERT OR REPLACE /
    // OR IGNORE) — guarantees the server has the full tree before any
    // FILE_OPEN, so recovery never triggers unknown_node NACK storms and the
    // commit node-count invariant converges in one round. nodeMeta iterates
    // in discovery order (parents before children); acked files are already
    // durable server-side and were dropped from nodeMeta, which is fine.
    for (const frame of this.nodeMeta.values()) {
      this.safeWrite(frame);
    }

    for (const file of this.trackedFiles.values()) {
      if (file.status === "pending" || file.status === "streaming" || file.status === "sent") {
        if (!this.rejectedFiles.has(file.nodeId) && !this.prunedNodes.has(file.nodeId)) {
          if (file.acceptedOffset < file.size) {
            file.status = "pending";
            this.fileQueue.push(file);
          }
        }
      }
    }
  }

  // -- probe verdict cache ---------------------------------------------------

  private probeCacheKey(): string {
    return `mfup:streaming-probe:${this.serverUrl}`;
  }

  private readProbeCache(): boolean | null {
    try {
      const raw = localStorage.getItem(this.probeCacheKey());
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { v: boolean; ts: number };
      if (Date.now() - parsed.ts > 24 * 3600 * 1000) return null; // stale
      return parsed.v;
    } catch {
      return null; // no localStorage (private mode, workers) — just probe
    }
  }

  private writeProbeCache(v: boolean): void {
    try {
      localStorage.setItem(this.probeCacheKey(), JSON.stringify({ v, ts: Date.now() }));
    } catch { /* best effort */ }
  }
}
