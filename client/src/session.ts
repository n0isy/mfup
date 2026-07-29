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
} from "./errors.js";
import { probeStreaming } from "./probe.js";

// ---------------------------------------------------------------------------
// Public config
// ---------------------------------------------------------------------------

export interface MfupSessionConfig {
  /** Server base URL (http(s)://host) */
  serverUrl: string;
  /** Relative target directory on the server where files will be placed */
  targetDir?: string;
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

export interface MfupSessionEvents {
  progress: ProgressSnapshot;
  state: SessionState;
  committed: { files: number; bytes: number };
  ask: void;
  error: MfupError;
  reconnecting: { attempt: number; delay: number; maxAttempts: number | null };
}

type Listener<T> = (ev: T) => void;

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

  // Reconnect
  private maxReconnectAttempts: number | null;
  private reconnectDelayMs: number;
  private reconnectCount = 0;
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  // Config
  private serverUrl: string;
  private targetDir: string;

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
    this.serverUrl = config.serverUrl.replace(/\/$/, "");
    this.targetDir = config.targetDir ?? ".";
    this.sessionId = config.sessionId ?? crypto.randomUUID();
    this.resumeToken = config.resumeToken ?? crypto.randomUUID();
    this.legId = crypto.randomUUID();
    this.chunkSize = config.chunkSize ?? 262144;
    this.maxReconnectAttempts = config.maxReconnectAttempts ?? null;
    this.reconnectDelayMs = config.reconnectDelayMs ?? 1000;

    if (config.lastKnownEpoch != null) {
      this.epoch = config.lastKnownEpoch;
    }
  }

  // -- public accessors ----------------------------------------------------

  get state(): SessionState { return this._state; }
  get id(): string { return this.sessionId; }
  get token(): string { return this.resumeToken; }
  get currentEpoch(): number { return this.epoch; }
  get streamingMode(): boolean | null { return this._streamingMode; }

  sendAction(action: "merge_overwrite" | "cancel"): void {
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
    this.legId = crypto.randomUUID();
    const isResume = this.epoch > 0;

    // Build WS URL — handle both http:// and https:// origins
    const wsUrl = this.serverUrl.replace(/^http/, "ws") + "/mfup/control";

    this.control = new ControlChannel({
      url: wsUrl,
      sessionId: this.sessionId,
      resumeToken: this.resumeToken,
      legId: this.legId,
      targetDir: this.targetDir,
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

  async uploadFileList(files: FileList): Promise<void> {
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
      }
    } finally {
      this.pumping = false;
    }
  }

  private async streamFile(file: TrackedFile): Promise<void> {
    file.status = "streaming";

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
        this.setState("committed");
        this.emit("committed", { files: cr.files, bytes: cr.bytes });
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

  private setState(s: SessionState): void {
    this._state = s;
    this.emit("state", s);
  }

  private emitError(err: MfupError): void {
    this.emit("error", err);
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
      this.progress.setExpiresAt(msg.expires_at);
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
        }
        if (file.status === "sent" && file.acceptedOffset >= file.size) {
          file.status = "acked";
          // Fully accepted — the server durably knows this node.
          this.nodeMeta.delete(msg.node_id);
          this.progress.acceptFile();
        }
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
      }
    });

    this.control.on("session_abort", (msg) => {
      this.setState("aborted");
      this.data?.abort("server aborted session");
      this.emitError(sessionAbortedByServer(msg.code, msg.reason));
      this._commitReject?.(new Error(`session aborted: ${msg.code}`));
    });

    this.control.on("commit_ok", (msg) => {
      this.setState("committed");
      this.emit("committed", { files: msg.files, bytes: msg.bytes });
      this._commitResolve?.();
    });

    this.control.on("commit_retry", (msg) => {
      // Epoch stays the same — only attach_leg() (reconnect) bumps epoch.
      // Server says these files are incomplete — requeue them and retry
      for (const entry of msg.incomplete) {
        const file = this.trackedFiles.get(entry.node_id);
        if (file && file.status !== "acked") {
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

    this.control.on("ask", () => {
      this.emit("ask", undefined as any);
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
      this.emit("reconnecting", {
        attempt: this.reconnectCount,
        delay,
        maxAttempts: this.maxReconnectAttempts,
      });

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

    const requeued: TrackedFile[] = [];
    for (const file of this.trackedFiles.values()) {
      if (file.status === "pending" || file.status === "streaming" || file.status === "sent") {
        if (!this.rejectedFiles.has(file.nodeId) && !this.prunedNodes.has(file.nodeId)) {
          if (file.acceptedOffset < file.size) {
            file.status = "pending";
            this.fileQueue.push(file);
            requeued.push(file);
          }
        }
      }
    }

    // Re-send NODE chains for everything requeued: frames already handed to
    // the dead channel (buffered but never delivered) are gone, and RESUME_OK
    // only describes what the server DID receive. Re-sending is cheap and
    // idempotent server-side (INSERT OR REPLACE / OR IGNORE).
    const toSend = new Set<number>();
    for (const file of requeued) {
      let cur = this.nodeMeta.get(file.nodeId);
      while (cur && !toSend.has(cur.nodeId)) {
        toSend.add(cur.nodeId);
        if (cur.parentId === ROOT_NODE_ID) break;
        cur = this.nodeMeta.get(cur.parentId);
      }
    }
    // nodeMeta iterates in discovery order → parents always precede children.
    for (const frame of this.nodeMeta.values()) {
      if (toSend.has(frame.nodeId)) {
        this.safeWrite(frame);
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
