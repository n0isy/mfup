/**
 * MFUP/2 session manager — in-memory registry of active sessions backed by
 * per-session SQLite databases in staging directories.
 *
 * One writer per file is enforced. Each session has at most one active leg.
 *
 * Port of server/mfup-core/mfup_core/session_manager.py. One deliberate
 * difference: Python guards registry mutations with an asyncio.Lock because
 * create/resume await mid-mutation; here every registry mutation is fully
 * synchronous (node:sqlite is sync), so on Node's single thread no lock is
 * needed — there is no await point inside any critical section.
 */

import * as fs from "node:fs";
import * as path from "node:path";

import { createLogger } from "./logger.js";
import {
  ChecksumKind,
  crc32c,
  Frame,
  FrameTag,
  NodeKind,
  NodeStatus,
  SessionState,
  ROOT_NODE_ID,
  type FileChunkFrame,
  type FileCloseFrame,
  type FileOpenFrame,
  type NodeFrame,
  type SessionEndFrame,
  type SummaryFrame,
  type ClientAbortFrame,
  type DirCloseFrame,
} from "./protocol.js";
import {
  DEFAULT_STAGING_PREFIX,
  SessionDB,
  openSessionDb,
  resolvePayloadPath,
  validateNodeName,
} from "./storage.js";

const logger = createLogger("mfup.session");

/** Storage errors that cannot be fixed by retrying the same write. */
const FATAL_STORAGE_CODES = new Set(["ENOSPC", "EDQUOT", "EROFS", "EFBIG"]);

function isFsError(e: unknown): e is NodeJS.ErrnoException {
  return e instanceof Error && typeof (e as NodeJS.ErrnoException).code === "string";
}

function isFatalStorageError(e: unknown): boolean {
  return isFsError(e) && FATAL_STORAGE_CODES.has(e.code as string);
}

/** Cap on COMMIT_RETRY round-trips before the server gives up on a session. */
export const MAX_COMMIT_RETRIES = 5;

/** Duck-typed control channel — the session only ever calls sendJson. */
export interface ControlSocket {
  sendJson(msg: Record<string, unknown>): void | Promise<void>;
}

// ---------------------------------------------------------------------------
// Per-session live state (in-memory, not persisted)
// ---------------------------------------------------------------------------

/** Wraps an open file descriptor for a single file being uploaded. */
export class FileWriter {
  readonly path: string;
  readonly nodeId: number;
  acceptedOffset: number;
  private fd: number;
  private closed = false;

  constructor(filePath: string, nodeId: number, acceptedOffset = 0, createExclusive = false) {
    this.path = filePath;
    this.nodeId = nodeId;
    this.acceptedOffset = acceptedOffset;
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    if (acceptedOffset === 0) {
      this.fd = fs.openSync(filePath, createExclusive ? "wx" : "w");
    } else {
      // Resuming: discard any bytes past the last durably-recorded offset.
      this.fd = fs.openSync(filePath, "r+");
      fs.ftruncateSync(this.fd, acceptedOffset);
    }
  }

  /** Write data at the expected offset. Returns the new acceptedOffset.
   * The write goes through the libuv thread pool (positional pwrite). */
  async write(data: Uint8Array, offset: number): Promise<number> {
    if (offset !== this.acceptedOffset) {
      throw new Error(`bad offset: expected ${this.acceptedOffset}, got ${offset}`);
    }
    await writeAll(this.fd, data, offset);
    this.acceptedOffset += data.length;
    return this.acceptedOffset;
  }

  close(): void {
    if (!this.closed) {
      this.closed = true;
      try {
        fs.closeSync(this.fd);
      } catch {
        /* already closed */
      }
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }
}

function writeAll(fd: number, data: Uint8Array, position: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const step = (bufOffset: number, pos: number): void => {
      fs.write(fd, data, bufOffset, data.length - bufOffset, pos, (err, written) => {
        if (err) return reject(err);
        if (bufOffset + written >= data.length) return resolve();
        step(bufOffset + written, pos + written);
      });
    };
    step(0, position);
  });
}

export interface LiveSessionOptions {
  targetDir?: string;
  stagingPrefix?: string;
  sessionResumeTtl?: number; // seconds
  legIdleTimeout?: number; // seconds
  maxChunkBytes?: number;
  conflictCheck?: boolean;
}

/** In-memory state for one active upload session. */
export class LiveSession {
  readonly sessionId: string;
  readonly resumeToken: string;
  readonly baseDir: string;
  readonly db: SessionDB;
  readonly targetDir: string;
  readonly stagingPrefix: string;

  readonly sessionResumeTtl: number;
  readonly legIdleTimeout: number;

  legId: string | null = null;
  epoch: number;

  /** One writer per open file, keyed by nodeId. */
  writers = new Map<number, FileWriter>();

  /** Control channel (set when the WebSocket connects). */
  ws: ControlSocket | null = null;

  private idleTimer: NodeJS.Timeout | null = null;

  // Sequence tracking for multi-POST data legs
  lastDataSeq = -1;
  finalSeqSeen = false;

  /** Conflict FSM: "clean" → "conflict_dir" → "conflict_files" */
  conflictState: "clean" | "conflict_dir" | "conflict_files" = "clean";

  /** Node ids dropped at ingest (illegal name / unknown parent). Counted in
   * the commit node-count invariant so a session with a few bad names can
   * still commit; repopulated on retry streams. */
  droppedNodes = new Set<number>();

  /** COMMIT_RETRY loop guard — counts CONSECUTIVE no-progress retries. */
  commitRetries = 0;
  private commitProgress: [number, number] = [-1, -1]; // [seenNodes, acceptedBytes]

  /** Publish action (set by the client's ACTION message). */
  publishAction: string | null = null;

  /** Advertised chunk limit — ENFORCED: oversized FILE_CHUNKs are NACKed. */
  readonly maxChunkBytes: number;

  /** When a mapFile hook is configured the ingest-time conflict ASK is
   * disabled: it checks the CLIENT's layout, which no longer predicts the
   * final one. Conflicts then surface at publish (409 → action). */
  readonly conflictCheck: boolean;

  // Per-session quotas from the authorize hook. null = unlimited.
  quotaMaxBytes: number | null = null;
  quotaMaxFiles: number | null = null;
  authContext: Record<string, unknown> = {};
  clientMeta: unknown = null;

  // Running totals for quota checks (no per-frame SQL aggregates).
  filesSeen: number;
  bytesAccepted: number;

  /** Callback for expiry changes (set by the engine for store updates). */
  onExpiryChange: ((sessionId: string, expiresAt: Date) => Promise<void>) | null = null;

  constructor(
    sessionId: string,
    resumeToken: string,
    baseDir: string,
    db: SessionDB,
    opts: LiveSessionOptions = {},
  ) {
    this.sessionId = sessionId;
    this.resumeToken = resumeToken;
    this.baseDir = baseDir;
    this.db = db;
    this.targetDir = opts.targetDir ?? ".";
    this.stagingPrefix = opts.stagingPrefix ?? DEFAULT_STAGING_PREFIX;
    this.sessionResumeTtl = opts.sessionResumeTtl ?? 3600;
    this.legIdleTimeout = opts.legIdleTimeout ?? 60;
    this.maxChunkBytes = opts.maxChunkBytes ?? 262144;
    this.conflictCheck = opts.conflictCheck ?? true;
    this.epoch = db.getEpoch();

    // Restore persisted meta / authorize constraints (survive restarts —
    // the mapFile hook at publish and the quota checks depend on them).
    const row = db.getSession();
    if (row !== null) {
      if (row.meta_json) {
        try {
          this.clientMeta = JSON.parse(row.meta_json);
        } catch {
          logger.warn(`Session ${sessionId}: unreadable meta_json ignored`);
        }
      }
      if (row.auth_json) {
        try {
          const auth = JSON.parse(row.auth_json) as {
            max_total_bytes?: number | null;
            max_files?: number | null;
            context?: Record<string, unknown> | null;
          };
          this.quotaMaxBytes = auth.max_total_bytes ?? null;
          this.quotaMaxFiles = auth.max_files ?? null;
          this.authContext = auth.context ?? {};
        } catch {
          logger.warn(`Session ${sessionId}: unreadable auth_json ignored`);
        }
      }
    }
    // Seeded from the DB so recovery/resume keeps quotas accurate.
    const [fc, bts] = db.countCommittedFiles();
    this.filesSeen = fc;
    this.bytesAccepted = bts;
  }

  get state(): SessionState {
    return this.db.getState();
  }

  get expiresAt(): string {
    return this.db.getExpiresAt();
  }

  // -- leg management -------------------------------------------------------

  /** True iff seq is the next expected sequence number. */
  validateSeq(seq: number): boolean {
    return seq === this.lastDataSeq + 1;
  }

  /** Mark seq as processed. Call only after the POST body was fully handled,
   * so a client may safely retry a failed POST with the same seq (a
   * duplicate is then rejected with seq_mismatch = "already delivered"). */
  advanceSeq(seq: number): void {
    this.lastDataSeq = seq;
  }

  /** Attach a new leg. Returns the new epoch. */
  attachLeg(legId: string): number {
    // Close any existing writers (stale leg)
    this.closeAllWriters();
    this.legId = legId;
    this.lastDataSeq = -1;
    this.finalSeqSeen = false;
    this.commitRetries = 0;
    this.commitProgress = [-1, -1];
    // Re-seed quota counters from the DB: after seek+truncate resume the
    // in-memory running totals would otherwise double-count re-sent tails.
    const [fc, bts] = this.db.countCommittedFiles();
    this.filesSeen = fc;
    this.bytesAccepted = bts;
    this.epoch = this.db.incrementEpoch();
    this.db.setState(SessionState.ACTIVE);
    this.resetIdleTimer();
    return this.epoch;
  }

  /**
   * Detach the current leg (disconnect).
   *
   * COMMITTING is included in the WAITING_RESUME transition: commit is
   * client-initiated, so a session whose leg went idle mid-commit must be
   * resumable (resume() rejects COMMITTING) — the client re-sends
   * SESSION_END after RESUME, mirroring recoverSession() semantics.
   */
  detachLeg(): void {
    this.closeAllWriters();
    this.cancelIdleTimer();
    const state = this.state;
    if (
      state === SessionState.ACTIVE ||
      state === SessionState.PAUSED_BY_SERVER ||
      state === SessionState.COMMITTING
    ) {
      this.db.setState(SessionState.WAITING_RESUME);
      const exp = new Date(Date.now() + this.sessionResumeTtl * 1000);
      this.db.setExpiresAt(exp.toISOString());
      if (this.onExpiryChange) {
        void this.onExpiryChange(this.sessionId, exp).catch((e) => {
          logger.error(`Session ${this.sessionId}: expiry update failed: ${e}`);
        });
      }
    }
    this.legId = null;
    this.ws = null;
  }

  // -- frame processing -----------------------------------------------------

  /** Process a decoded data frame. Must match the current legId. */
  async processFrame(frame: Frame, legId: string): Promise<void> {
    if (legId !== this.legId) {
      logger.warn(`Ignoring frame from stale leg ${legId} (current: ${this.legId})`);
      return;
    }

    this.resetIdleTimer();

    switch (frame.tag) {
      case FrameTag.NODE:
        return this.handleNode(frame);
      case FrameTag.FILE_OPEN:
        return this.handleFileOpen(frame);
      case FrameTag.FILE_CHUNK:
        return this.handleFileChunk(frame);
      case FrameTag.FILE_CLOSE:
        return this.handleFileClose(frame);
      case FrameTag.DIR_CLOSE:
        return this.handleDirClose(frame);
      case FrameTag.SUMMARY:
        return this.handleSummary(frame);
      case FrameTag.SESSION_END:
        return this.handleSessionEnd(frame);
      case FrameTag.CLIENT_ABORT:
        return this.handleClientAbort(frame);
    }
  }

  /** Resolve where this node would land in targetDir after publish. */
  private resolveTargetPath(nodeId: number): string | null {
    let target = this.targetDir;
    if (!path.isAbsolute(target)) {
      target = path.join(this.baseDir, target);
    }
    const parts: string[] = [];
    let cur = nodeId;
    while (cur !== ROOT_NODE_ID) {
      const node = this.db.getNode(cur);
      if (node === null) return null;
      parts.push(node.name);
      cur = node.parent_id;
    }
    if (parts.length === 0) return null;
    parts.reverse();
    const result = path.join(target, ...parts);
    // Guard against path traversal via crafted file names.
    const rel = path.relative(path.resolve(this.baseDir), path.resolve(result));
    if (rel.startsWith("..") || path.isAbsolute(rel)) return null;
    return result;
  }

  /** Advance the conflict FSM by checking if the target path exists. */
  private async checkConflict(nodeId: number, isDir: boolean): Promise<void> {
    if (!this.conflictCheck) return; // mapFile hook owns the final layout
    if (this.conflictState === "conflict_files") return; // terminal
    const dest = this.resolveTargetPath(nodeId);
    if (dest === null || !fs.existsSync(dest)) return;
    let destIsDir = false;
    try {
      destIsDir = fs.statSync(dest).isDirectory();
    } catch {
      return;
    }
    if (isDir && destIsDir) {
      // Dir-dir match: auto-accept, advance silently.
      if (this.conflictState === "clean") {
        this.conflictState = "conflict_dir";
        logger.info(`Session ${this.sessionId}: conflict_dir (dir ${path.basename(dest)} exists)`);
      }
    } else {
      // File-file or type mismatch: send ASK once. code/node_id/name are
      // additive fields (older clients ignore them); name is the conflicting
      // basename only — never a server-side path.
      this.conflictState = "conflict_files";
      logger.info(`Session ${this.sessionId}: conflict_files (file ${path.basename(dest)} exists)`);
      await this.sendControl({
        t: "ASK",
        code: "target_conflict",
        node_id: nodeId,
        name: path.basename(dest),
      });
    }
  }

  private async handleNode(f: NodeFrame): Promise<void> {
    if (this.db.isPruned(f.nodeId) || this.db.isRejected(f.nodeId)) return;
    try {
      validateNodeName(f.name);
    } catch {
      logger.warn(`Rejected node ${f.nodeId} with illegal name: ${JSON.stringify(f.name)}`);
      // Count it so the commit node-count invariant still balances (the node
      // is never stored), and REJECT it so the client skips the file
      // immediately instead of provoking an unknown_node NACK storm.
      this.droppedNodes.add(f.nodeId);
      await this.rejectFile(f.nodeId, "illegal_name", `illegal node name: ${JSON.stringify(f.name)}`);
      return;
    }
    // A node whose parent chain is unknown must not be stored: its payload
    // path could not be resolved (or worse, would silently resolve short).
    // NACK with unknown_node so the client re-sends the parent chain.
    if (f.parentId !== 0 && this.db.getNode(f.parentId) === null) {
      logger.warn(`Node ${f.nodeId} references unknown parent ${f.parentId}`);
      await this.sendNack(f.nodeId, 0, "unknown_node");
      return;
    }
    const newFile = this.db.upsertNode(f.nodeId, f.parentId, f.kind, f.name, f.sizeHint, f.mtimeMs);
    if (newFile) {
      this.filesSeen += 1;
      if (this.quotaMaxFiles !== null && this.filesSeen > this.quotaMaxFiles) {
        await this.abortQuota(
          `file count quota exceeded (${this.filesSeen} > ${this.quotaMaxFiles})`,
        );
        return;
      }
    }
    if (f.kind === NodeKind.DIR) {
      // Create the directory in payload. This can fail if a sibling FILE node
      // already claimed the same path (a malformed/adversarial tree — a real
      // filesystem can't hold a file and dir with one name). One bad node
      // must never 500 the whole upload: reject it instead.
      try {
        const p = resolvePayloadPath(
          this.baseDir,
          this.sessionId,
          this.db,
          f.nodeId,
          this.stagingPrefix,
        );
        fs.mkdirSync(p, { recursive: true });
      } catch (exc) {
        logger.warn(`Cannot materialize dir node ${f.nodeId} (${JSON.stringify(f.name)}): ${exc}`);
        await this.rejectFile(f.nodeId, "fs_conflict", String(exc));
        return;
      }
      await this.checkConflict(f.nodeId, true);
    } else {
      await this.checkConflict(f.nodeId, false);
    }
  }

  private async handleFileOpen(f: FileOpenFrame): Promise<void> {
    if (this.db.isRejected(f.nodeId) || this.db.isPruned(f.nodeId)) return;

    const node = this.db.getNode(f.nodeId);
    if (node === null) {
      // The NODE frame for this file never reached us (e.g. lost with a
      // failed data POST). Tell the client to re-send the node chain.
      await this.sendNack(f.nodeId, 0, "unknown_node");
      return;
    }

    const fileRow = this.db.getFile(f.nodeId);
    const accepted = fileRow ? fileRow.accepted_offset : 0;

    let filePath: string;
    try {
      filePath = resolvePayloadPath(
        this.baseDir,
        this.sessionId,
        this.db,
        f.nodeId,
        this.stagingPrefix,
      );
    } catch (exc) {
      // Broken parent chain or traversal attempt — never write blindly.
      logger.warn(`FILE_OPEN node ${f.nodeId}: ${exc}`);
      await this.sendNack(f.nodeId, 0, "unknown_node");
      return;
    }
    // Duplicate FILE_OPEN must not leak the previous file handle.
    const prev = this.writers.get(f.nodeId);
    if (prev !== undefined) {
      this.writers.delete(f.nodeId);
      prev.close();
    }
    let writer: FileWriter;
    try {
      // The existing journal column records ownership, without another index
      // or path map. New files use an exclusive create instead of truncation.
      const createExclusive = accepted === 0 && !fileRow?.local_tmp_path;
      try {
        writer = new FileWriter(filePath, f.nodeId, accepted, createExclusive);
      } catch (exc) {
        if (!createExclusive || (exc as NodeJS.ErrnoException).code !== "EEXIST") throw exc;
        // Recovery may leave an unjournaled file after a batch was interrupted.
        // Inspect existing metadata only on this exceptional path.
        const otherOwner = this.db.getAllFiles().some((file) => {
          if (file.node_id === f.nodeId || file.status === "rejected" || file.status === "pruned") return false;
          return resolvePayloadPath(this.baseDir, this.sessionId, this.db, file.node_id, this.stagingPrefix) === filePath;
        });
        if (otherOwner) throw exc;
        writer = new FileWriter(filePath, f.nodeId, accepted);
      }
      if (!fileRow?.local_tmp_path) this.db.setFilePath(f.nodeId, writer.path);
    } catch (exc) {
      // Opening the payload file failed persistently — e.g. a DIR node
      // already occupies this path (EISDIR), or the disk is full. A capacity
      // error aborts the session; a path/type conflict rejects just this
      // file so the rest of the tree still commits.
      if (isFatalStorageError(exc)) {
        logger.error(`Fatal storage error opening node ${f.nodeId}: ${exc}`);
        await this.abortStorageError(exc as NodeJS.ErrnoException);
        return;
      }
      logger.warn(`Cannot open payload file for node ${f.nodeId}: ${exc}`);
      await this.rejectFile(f.nodeId, "fs_conflict", String(exc));
      return;
    }
    this.writers.set(f.nodeId, writer);
  }

  private async handleFileChunk(f: FileChunkFrame): Promise<void> {
    if (this.db.isRejected(f.nodeId) || this.db.isPruned(f.nodeId)) return;

    const writer = this.writers.get(f.nodeId);
    if (writer === undefined) {
      // No FILE_OPEN succeeded for this node on this leg. If the node itself
      // is unknown, the client must re-send its NODE chain.
      const reason = this.db.getNode(f.nodeId) === null ? "unknown_node" : "bad_offset";
      const fileRow = this.db.getFile(f.nodeId);
      const expected = fileRow ? fileRow.accepted_offset : 0;
      await this.sendNack(f.nodeId, expected, reason);
      return;
    }

    // Enforce the advertised chunk-size limit (honest clients clamp to
    // HELLO_OK.limits; a misbehaving one gets NACKed, bounded by its own
    // NACK budget).
    if (f.length > this.maxChunkBytes) {
      await this.sendNack(f.nodeId, writer.acceptedOffset, "server_policy");
      return;
    }

    // Enforce the byte quota BEFORE writing.
    if (this.quotaMaxBytes !== null && this.bytesAccepted + f.length > this.quotaMaxBytes) {
      await this.abortQuota(
        `byte quota exceeded (${this.bytesAccepted + f.length} > ${this.quotaMaxBytes})`,
      );
      return;
    }

    // Verify checksum
    if (f.checksumKind === ChecksumKind.CRC32C) {
      const computed = crc32c(f.payload);
      if (computed !== f.checksum) {
        await this.sendNack(f.nodeId, writer.acceptedOffset, "bad_checksum");
        return;
      }
    }

    // Verify contiguous offset
    if (f.offset !== writer.acceptedOffset) {
      await this.sendNack(f.nodeId, writer.acceptedOffset, "bad_offset");
      return;
    }

    let newOffset: number;
    try {
      newOffset = await writer.write(f.payload, f.offset);
    } catch (exc) {
      // A storage-capacity error will not clear by retrying — a client that
      // keeps resending would just spin. Abort the whole session with a
      // clear reason and let cleanup reclaim the staging dir.
      if (isFatalStorageError(exc)) {
        logger.error(
          `Fatal storage error for session ${this.sessionId} node ${f.nodeId}: ${exc}`,
        );
        await this.abortStorageError(exc as NodeJS.ErrnoException);
        return;
      }
      // Other errors (transient) — NACK and let the client retry, bounded
      // client-side by MAX_FILE_NACKS.
      logger.error(`Write error for node ${f.nodeId}: ${exc}`);
      await this.sendNack(f.nodeId, writer.acceptedOffset, "server_policy");
      return;
    }

    this.db.setAcceptedOffset(f.nodeId, newOffset);
    this.bytesAccepted += f.length;

    // Update root summary body_done
    const summary = this.db.getRootSummary();
    this.db.updateRootSummary(
      summary.scan_done_units,
      summary.scan_est_units,
      summary.body_done_bytes + f.payload.length,
      summary.body_est_bytes,
      summary.sealed,
    );

    // Cumulative FILE_ACK roughly every 256 KiB.
    if (newOffset % (256 * 1024) < f.length || f.length === 0) {
      await this.sendFileAck(f.nodeId, newOffset);
    }
  }

  private async handleFileClose(f: FileCloseFrame): Promise<void> {
    const writer = this.writers.get(f.nodeId);
    if (writer !== undefined) {
      this.writers.delete(f.nodeId);
      const finalOffset = writer.acceptedOffset;
      writer.close();
      this.db.setFileFinal(f.nodeId, f.sizeSent, writer.path);
      this.db.setNodeStatus(f.nodeId, NodeStatus.CLOSED);
      await this.sendFileAck(f.nodeId, finalOffset);
    }
  }

  private async handleDirClose(f: DirCloseFrame): Promise<void> {
    this.db.setNodeStatus(f.nodeId, NodeStatus.CLOSED);
  }

  private async handleSummary(f: SummaryFrame): Promise<void> {
    const cur = this.db.getRootSummary();
    this.db.updateRootSummary(
      Math.max(f.scanDoneUnits, cur.scan_done_units),
      Math.max(f.scanEstUnits, cur.scan_est_units),
      cur.body_done_bytes, // keep the server-side body count
      Math.max(f.bodyEstBytes, cur.body_est_bytes),
      f.sealed || cur.sealed,
    );
  }

  private async handleSessionEnd(f: SessionEndFrame): Promise<void> {
    const summary = this.db.getRootSummary();
    this.db.updateRootSummary(
      f.scanDoneUnits,
      f.scanEstUnits,
      summary.body_done_bytes,
      f.bodyEstBytes,
      true,
    );
    this.closeAllWriters();
    this.db.setState(SessionState.COMMITTING);
    logger.info(`Session ${this.sessionId} entering COMMITTING state`);
  }

  private async handleClientAbort(f: ClientAbortFrame): Promise<void> {
    this.closeAllWriters();
    this.db.setState(SessionState.ABORTED);
    logger.info(`Session ${this.sessionId} aborted by client: ${f.code} — ${f.reason}`);
  }

  /**
   * Abort the session on an unrecoverable storage error (e.g. disk full).
   *
   * Sets ABORTED so the WS finally-block reclaims the staging dir + store
   * entry, and notifies the client so it stops resending. ENOSPC maps to a
   * distinct code the client surfaces as fatal.
   */
  async abortStorageError(exc: NodeJS.ErrnoException): Promise<void> {
    this.closeAllWriters();
    this.db.setState(SessionState.ABORTED);
    const code = exc.code === "ENOSPC" || exc.code === "EDQUOT" ? "storage_full" : "storage_error";
    await this.sendControl({
      t: "SESSION_ABORT",
      code,
      reason: `server storage error: ${exc.message}`,
    });
  }

  /** Set authorize-hook constraints and persist them (restart-safe). */
  applyAuth(
    maxTotalBytes: number | null,
    maxFiles: number | null,
    context: Record<string, unknown>,
  ): void {
    this.quotaMaxBytes = maxTotalBytes;
    this.quotaMaxFiles = maxFiles;
    this.authContext = { ...context };
    let persistCtx: Record<string, unknown> = this.authContext;
    try {
      JSON.stringify(persistCtx);
    } catch {
      logger.warn(
        `Session ${this.sessionId}: AuthResult.context is not JSON-serializable — ` +
          "it will NOT survive a server restart",
      );
      persistCtx = {};
    }
    this.db.setAuthJson(
      JSON.stringify({
        max_total_bytes: maxTotalBytes,
        max_files: maxFiles,
        context: persistCtx,
      }),
    );
  }

  /** Abort the session on a quota violation (from the authorize hook). */
  async abortQuota(reason: string): Promise<void> {
    this.closeAllWriters();
    this.db.setState(SessionState.ABORTED);
    logger.warn(`Session ${this.sessionId} aborted: ${reason}`);
    await this.sendControl({
      t: "SESSION_ABORT",
      code: "quota_exceeded",
      reason,
    });
  }

  // -- control message senders ---------------------------------------------

  private async sendFileAck(nodeId: number, acceptedOffset: number): Promise<void> {
    await this.sendControl({
      t: "FILE_ACK",
      node_id: nodeId,
      accepted_offset: acceptedOffset,
    });
  }

  private async sendNack(nodeId: number, expectedOffset: number, reason: string): Promise<void> {
    await this.sendControl({
      t: "NACK_CHUNK",
      node_id: nodeId,
      expected_offset: expectedOffset,
      reason,
    });
  }

  /** A dead control channel logs but never raises into the ingest path. */
  async sendControl(msg: Record<string, unknown>): Promise<void> {
    if (this.ws) {
      try {
        await this.ws.sendJson(msg);
      } catch (exc) {
        logger.error(`Failed to send control msg ${String(msg.t ?? "?")}: ${exc}`);
      }
    }
  }

  // -- prune / reject (server-initiated) -------------------------------------

  async pruneNode(nodeId: number, code: string, reason: string): Promise<void> {
    this.db.addPruned(nodeId);
    this.db.setNodeStatus(nodeId, NodeStatus.PRUNED);
    const writer = this.writers.get(nodeId);
    if (writer !== undefined) {
      this.writers.delete(nodeId);
      writer.close();
    }
    await this.sendControl({ t: "PRUNE_NODE", node_id: nodeId, code, reason });
  }

  async rejectFile(nodeId: number, code: string, reason: string): Promise<void> {
    this.db.addRejected(nodeId, code, reason);
    const writer = this.writers.get(nodeId);
    if (writer !== undefined) {
      this.writers.delete(nodeId);
      writer.close();
    }
    await this.sendControl({ t: "REJECT_FILE", node_id: nodeId, code, reason });
  }

  // -- flow control ----------------------------------------------------------

  async sendFlow(paused: boolean, reason: string): Promise<void> {
    if (paused) {
      this.db.setState(SessionState.PAUSED_BY_SERVER);
    } else if (this.state === SessionState.PAUSED_BY_SERVER) {
      this.db.setState(SessionState.ACTIVE);
    }
    await this.sendControl({ t: "FLOW", paused, reason });
  }

  // -- commit ----------------------------------------------------------------

  /**
   * Increment the retry counter, but reset it whenever the session made
   * forward progress (more nodes seen, or more bytes accepted) since the
   * previous COMMIT_RETRY. Returns true only after MAX_COMMIT_RETRIES
   * CONSECUTIVE no-progress retries — so a progressing large-file resume is
   * never falsely aborted, while a genuinely stuck file is.
   */
  private commitRetryExhausted(): boolean {
    const seenNodes = this.db.countNodes();
    const [, acceptedBytes] = this.db.countCommittedFiles();
    const [prevNodes, prevBytes] = this.commitProgress;
    if (seenNodes > prevNodes || (seenNodes === prevNodes && acceptedBytes > prevBytes)) {
      this.commitProgress = [seenNodes, acceptedBytes];
      this.commitRetries = 0;
      return false;
    }
    this.commitRetries += 1;
    return this.commitRetries > MAX_COMMIT_RETRIES;
  }

  /** Attempt to commit: returns the COMMIT_OK payload or null if not ready. */
  async tryCommit(): Promise<{ t: "COMMIT_OK"; files: number; bytes: number } | null> {
    const state = this.db.getState();
    if (state !== SessionState.COMMITTING) return null;

    // Close any stale writers before checking invariants
    if (this.writers.size > 0) {
      logger.warn(
        `Session ${this.sessionId}: closing ${this.writers.size} stale writer(s) before commit`,
      );
      this.closeAllWriters();
    }

    // Invariant 1: the server saw every node the client scanned. SESSION_END
    // carries the client's final scan_done_units; if NODE frames were lost
    // in transit (e.g. a failed data POST), files the server never heard
    // about would be invisible to the completeness check below — this guards
    // exactly that hole.
    const expectedNodes = this.db.getRootSummary().scan_done_units;
    const seenNodes = this.db.countNodes() + this.droppedNodes.size;
    if (expectedNodes && seenNodes !== expectedNodes) {
      const exhausted = this.commitRetryExhausted();
      logger.warn(
        `Session ${this.sessionId}: commit blocked — node count mismatch ` +
          `(expected ${expectedNodes}, seen ${seenNodes}, no-progress attempt ${this.commitRetries})`,
      );
      if (exhausted) {
        this.db.setState(SessionState.FAILED);
        await this.sendControl({
          t: "SESSION_ABORT",
          code: "commit_failed",
          reason: `node count mismatch, no progress: expected ${expectedNodes}, seen ${seenNodes}`,
        });
        return null;
      }
      this.db.setState(SessionState.ACTIVE);
      this.finalSeqSeen = false;
      this.lastDataSeq = -1;
      await this.sendControl({
        t: "COMMIT_RETRY",
        incomplete: [],
        nodes_expected: expectedNodes,
        nodes_seen: seenNodes,
      });
      return null;
    }

    // Invariant 2: all known files fully received (accepted_offset == final_size)
    const incomplete = this.db.getIncompleteFiles();
    if (incomplete.length > 0) {
      const exhausted = this.commitRetryExhausted();
      logger.warn(
        `Session ${this.sessionId}: commit blocked — ${incomplete.length} incomplete file(s), ` +
          `COMMIT_RETRY (no-progress attempt ${this.commitRetries})`,
      );
      if (exhausted) {
        this.db.setState(SessionState.FAILED);
        await this.sendControl({
          t: "SESSION_ABORT",
          code: "commit_failed",
          reason: `${incomplete.length} file(s) incomplete, no progress after ${MAX_COMMIT_RETRIES} retries`,
        });
        return null;
      }
      // Tell the client which files need resending, revert to ACTIVE
      const retryFiles = incomplete.map((f) => ({
        node_id: f.node_id,
        accepted_offset: f.accepted_offset,
      }));
      this.db.setState(SessionState.ACTIVE);
      this.finalSeqSeen = false; // allow a new final POST
      this.lastDataSeq = -1; // reset seq for retry
      await this.sendControl({ t: "COMMIT_RETRY", incomplete: retryFiles });
      return null;
    }

    const [fileCount, totalBytes] = this.db.countCommittedFiles();
    this.db.setState(SessionState.COMMITTED);

    const result = { t: "COMMIT_OK" as const, files: fileCount, bytes: totalBytes };
    await this.sendControl(result);
    return result;
  }

  // -- internal ---------------------------------------------------------------

  closeAllWriters(): void {
    for (const w of this.writers.values()) {
      try {
        w.close();
      } catch {
        /* best effort */
      }
    }
    this.writers.clear();
  }

  private resetIdleTimer(): void {
    this.cancelIdleTimer();
    this.idleTimer = setTimeout(() => {
      logger.info(`Session ${this.sessionId} leg idle timeout`);
      this.detachLeg();
    }, this.legIdleTimeout * 1000);
    this.idleTimer.unref();
  }

  cancelIdleTimer(): void {
    if (this.idleTimer !== null) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
  }

  /** Build a RESUME_OK message from persisted state. */
  buildResumeOk(): Record<string, unknown> {
    const summary = this.db.getRootSummary();
    const files = this.db.getAllFiles().map((row) => ({
      node_id: row.node_id,
      accepted_offset: row.accepted_offset,
      status:
        row.status === NodeStatus.REJECTED
          ? "rejected"
          : row.status === NodeStatus.CLOSED
            ? "closed"
            : "open",
    }));
    return {
      t: "RESUME_OK",
      epoch: this.epoch,
      expires_at: this.expiresAt,
      root_summary: summary,
      files,
      pruned_nodes: this.db.getPrunedNodes(),
      rejected_files: this.db.getRejectedFiles(),
    };
  }
}

// ---------------------------------------------------------------------------
// Global session registry
// ---------------------------------------------------------------------------

export interface SessionRegistryDefaults {
  sessionResumeTtl?: number;
  legIdleTimeout?: number;
  maxChunkBytes?: number;
  conflictCheck?: boolean;
}

/**
 * Global in-memory registry of live sessions.
 *
 * No startup scan here — recovery is driven by the engine (store-driven for
 * durable stores, filesystem scan for the ephemeral MemoryStore).
 */
export class SessionRegistry {
  readonly baseDir: string;
  readonly stagingPrefix: string;
  private defaults: SessionRegistryDefaults;
  private sessions = new Map<string, LiveSession>();

  constructor(
    baseDir: string,
    stagingPrefix: string = DEFAULT_STAGING_PREFIX,
    defaults: SessionRegistryDefaults = {},
  ) {
    this.baseDir = baseDir;
    this.stagingPrefix = stagingPrefix;
    this.defaults = defaults;
  }

  /**
   * Create a session. `baseDir` overrides the registry-wide base for THIS
   * session (per-user home from the authorize hook): staging lives inside
   * it, so publish stays a same-filesystem rename even when homes are
   * separate mounts. `metaJson` is the client-attached session meta
   * (HELLO.meta), persisted for the publish-time mapFile hook.
   */
  create(
    sessionId: string,
    resumeToken: string,
    legId: string,
    expiresAt: string,
    targetDir = ".",
    baseDir: string | null = null,
    metaJson: string | null = null,
  ): LiveSession {
    const base = baseDir ?? this.baseDir;
    if (this.sessions.has(sessionId)) {
      throw new Error(`session ${sessionId} already exists`);
    }
    const db = openSessionDb(base, sessionId, this.stagingPrefix);
    db.initSession(sessionId, resumeToken, expiresAt, targetDir, metaJson);
    const session = new LiveSession(sessionId, resumeToken, base, db, {
      targetDir,
      stagingPrefix: this.stagingPrefix,
      ...this.defaults,
    });
    session.attachLeg(legId);
    this.sessions.set(sessionId, session);
    return session;
  }

  /**
   * Re-attach a leg. Throws:
   *   - SessionUnknownError  — id not in the registry;
   *   - BadTokenError        — token mismatch;
   *   - BadStateError        — non-resumable state.
   */
  resume(sessionId: string, resumeToken: string, legId: string): LiveSession {
    const session = this.sessions.get(sessionId);
    if (session === undefined) {
      throw new SessionUnknownError(`session ${sessionId} not found`);
    }
    if (session.resumeToken !== resumeToken) {
      throw new BadTokenError("invalid resume token");
    }
    const state = session.state;
    if (
      state !== SessionState.WAITING_RESUME &&
      state !== SessionState.ACTIVE &&
      state !== SessionState.PAUSED_BY_SERVER
    ) {
      throw new BadStateError(`cannot resume session in state ${state}`);
    }
    session.attachLeg(legId);
    return session;
  }

  get(sessionId: string): LiveSession | null {
    return this.sessions.get(sessionId) ?? null;
  }

  remove(sessionId: string): void {
    const session = this.sessions.get(sessionId);
    if (session !== undefined) {
      this.sessions.delete(sessionId);
      session.detachLeg();
      session.db.close();
    }
  }

  /**
   * Recover a session from its SQLite DB on disk.
   *
   * Opens the DB, reads the session row, and registers a detached
   * LiveSession (no leg, no WS). Returns null if the DB is missing,
   * unreadable, or the state is terminal.
   */
  recoverSession(sessionId: string, stagingPath: string): LiveSession | null {
    const dbPath = path.join(stagingPath, "state.sqlite");
    if (!fs.existsSync(dbPath)) {
      logger.warn(`Recovery: DB missing for session ${sessionId} at ${dbPath}`);
      return null;
    }

    let db: SessionDB;
    try {
      db = new SessionDB(dbPath);
    } catch (exc) {
      logger.error(`Recovery: failed to open DB for session ${sessionId}: ${exc}`);
      return null;
    }

    const row = db.getSession();
    if (row === null) {
      logger.warn(`Recovery: empty sessions table for ${sessionId}`);
      db.close();
      return null;
    }

    const state = row.state as SessionState;
    // Only recover sessions that can still accept a RESUME
    if (
      state !== SessionState.ACTIVE &&
      state !== SessionState.PAUSED_BY_SERVER &&
      state !== SessionState.WAITING_RESUME &&
      state !== SessionState.COMMITTING
    ) {
      logger.info(`Recovery: skipping session ${sessionId} in terminal state ${state}`);
      db.close();
      return null;
    }

    // Force state to WAITING_RESUME — no leg is attached after restart.
    // COMMITTING included: commit is client-initiated, so after a server
    // restart the client must reconnect and re-send SESSION_END.
    if (
      state === SessionState.ACTIVE ||
      state === SessionState.PAUSED_BY_SERVER ||
      state === SessionState.COMMITTING
    ) {
      db.setState(SessionState.WAITING_RESUME);
    }

    const existing = this.sessions.get(sessionId);
    if (existing !== undefined) {
      db.close();
      return existing;
    }

    // The session's base dir is by construction the PARENT of its staging
    // dir (staging = <base>/<prefix>.<sid>). Deriving it here keeps recovery
    // correct for per-user base_dir sessions (authorize-hook homes) without
    // persisting the base anywhere else.
    const session = new LiveSession(sessionId, row.resume_token, path.dirname(stagingPath), db, {
      targetDir: row.target_dir,
      stagingPrefix: this.stagingPrefix,
      ...this.defaults,
    });
    this.sessions.set(sessionId, session);
    logger.info(
      `Recovery: restored session ${sessionId} (state=${state}, epoch=${session.epoch}, target=${row.target_dir})`,
    );
    return session;
  }

  allSessions(): Map<string, LiveSession> {
    return new Map(this.sessions);
  }
}

export class SessionUnknownError extends Error {}
export class BadTokenError extends Error {}
export class BadStateError extends Error {}
