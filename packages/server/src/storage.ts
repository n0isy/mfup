/**
 * MFUP/2 SQLite storage layer (node:sqlite).
 *
 * Each session lives in:
 *     <base_dir>/.incoming.<session_id>/state.sqlite
 *     <base_dir>/.incoming.<session_id>/payload/...
 *
 * Mirrors server/mfup-core/mfup_core/storage.py: same schema, same pragmas,
 * same batch semantics. node:sqlite is synchronous (like better-sqlite3);
 * batched transactions keep each event-loop hold in the microseconds.
 */

import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";

import { NodeKind, NodeStatus, SessionState, ROOT_NODE_ID } from "./protocol.js";

const _SCHEMA = `
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    resume_token TEXT NOT NULL,
    epoch        INTEGER NOT NULL DEFAULT 1,
    state        TEXT NOT NULL DEFAULT 'active',
    target_dir   TEXT NOT NULL DEFAULT '.',
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id    INTEGER PRIMARY KEY,
    parent_id  INTEGER NOT NULL,
    kind       TEXT NOT NULL,
    name       TEXT NOT NULL,
    size       INTEGER,
    mtime_ms   INTEGER,
    status     TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS files (
    node_id          INTEGER PRIMARY KEY REFERENCES nodes(node_id),
    accepted_offset  INTEGER NOT NULL DEFAULT 0,
    final_size       INTEGER,
    checksum_state   BLOB,
    local_tmp_path   TEXT
);

CREATE TABLE IF NOT EXISTS pruned (
    node_id  INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS rejected (
    node_id  INTEGER PRIMARY KEY,
    code     TEXT NOT NULL,
    reason   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS root_summary (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    scan_done_units  INTEGER NOT NULL DEFAULT 0,
    scan_est_units   INTEGER NOT NULL DEFAULT 0,
    body_done_bytes  INTEGER NOT NULL DEFAULT 0,
    body_est_bytes   INTEGER NOT NULL DEFAULT 0,
    sealed           INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO root_summary (id) VALUES (1);
`;

export interface SessionRow {
  session_id: string;
  resume_token: string;
  epoch: number;
  state: string;
  target_dir: string;
  expires_at: string;
  created_at: string;
  updated_at: string;
  meta_json: string | null;
  auth_json: string | null;
}

export interface NodeRow {
  node_id: number;
  parent_id: number;
  kind: "dir" | "file";
  name: string;
  size: number | null;
  mtime_ms: number | null;
  status: string;
}

export interface FileRow {
  node_id: number;
  accepted_offset: number;
  final_size: number | null;
  checksum_state: Uint8Array | null;
  local_tmp_path: string | null;
}

export interface RootSummaryData {
  scan_done_units: number;
  scan_est_units: number;
  body_done_bytes: number;
  body_est_bytes: number;
  sealed: boolean;
}

function nowIso(): string {
  return new Date().toISOString();
}

/**
 * Thin wrapper around a per-session SQLite database.
 *
 * SQLite autocommits each statement outside an explicit transaction, so the
 * Python `_maybe_commit` dance collapses to: begin_batch() opens a
 * transaction, end_batch()/flush() commit it. For hot paths (a data POST
 * carrying thousands of frames) the batch is 10–100× cheaper for
 * many-small-files uploads (think node_modules).
 */
export class SessionDB {
  readonly path: string;
  private db: DatabaseSync;
  private batch = false;

  constructor(dbPath: string) {
    this.path = dbPath;
    this.db = new DatabaseSync(dbPath);
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec("PRAGMA busy_timeout=3000");
    this.db.exec("PRAGMA synchronous=NORMAL");
    this.db.exec(_SCHEMA);
    // Lightweight migration: sessions are short-lived (TTL), so additive
    // columns are applied in place instead of versioned migrations.
    for (const col of ["meta_json", "auth_json"]) {
      try {
        this.db.exec(`ALTER TABLE sessions ADD COLUMN ${col} TEXT`);
      } catch {
        /* column already exists */
      }
    }
  }

  close(): void {
    if (this.batch) {
      try {
        this.db.exec("COMMIT");
      } catch {
        /* not in a transaction */
      }
      this.batch = false;
    }
    this.db.close();
  }

  // -- batching -------------------------------------------------------------

  /** Enter deferred-commit mode (idempotent). */
  beginBatch(): void {
    if (!this.batch) {
      this.db.exec("BEGIN");
      this.batch = true;
    }
  }

  /** Commit accumulated work and leave deferred-commit mode. */
  endBatch(): void {
    if (this.batch) {
      this.db.exec("COMMIT");
      this.batch = false;
    }
  }

  /** Commit accumulated work but stay in the current mode. */
  flush(): void {
    if (this.batch) {
      this.db.exec("COMMIT");
      this.db.exec("BEGIN");
    }
  }

  // -- session --------------------------------------------------------------

  initSession(
    sessionId: string,
    resumeToken: string,
    expiresAt: string,
    targetDir = ".",
    metaJson: string | null = null,
  ): void {
    const now = nowIso();
    // Explicit column list: the table gains additive columns over time.
    this.db
      .prepare(
        "INSERT OR REPLACE INTO sessions " +
          "(session_id, resume_token, epoch, state, target_dir, expires_at," +
          " created_at, updated_at, meta_json) " +
          "VALUES (?,?,1,?,?,?,?,?,?)",
      )
      .run(sessionId, resumeToken, SessionState.ACTIVE, targetDir, expiresAt, now, now, metaJson);
  }

  /** Persist authorize-hook constraints (quotas + JSON-able context) so
   * they survive restarts / recovery. */
  setAuthJson(authJson: string): void {
    this.db.prepare("UPDATE sessions SET auth_json=?").run(authJson);
  }

  getSession(): SessionRow | null {
    const row = this.db.prepare("SELECT * FROM sessions LIMIT 1").get();
    return (row as unknown as SessionRow) ?? null;
  }

  getTargetDir(): string {
    const row = this.db.prepare("SELECT target_dir FROM sessions LIMIT 1").get() as
      | { target_dir: string }
      | undefined;
    return row ? row.target_dir : ".";
  }

  setState(state: SessionState): void {
    this.db.prepare("UPDATE sessions SET state=?, updated_at=?").run(state, nowIso());
  }

  getState(): SessionState {
    const row = this.db.prepare("SELECT state FROM sessions LIMIT 1").get() as
      | { state: string }
      | undefined;
    return row ? (row.state as SessionState) : SessionState.ACTIVE;
  }

  incrementEpoch(): number {
    this.db.prepare("UPDATE sessions SET epoch = epoch + 1, updated_at = ?").run(nowIso());
    const row = this.db.prepare("SELECT epoch FROM sessions LIMIT 1").get() as { epoch: number };
    return row.epoch;
  }

  getEpoch(): number {
    const row = this.db.prepare("SELECT epoch FROM sessions LIMIT 1").get() as
      | { epoch: number }
      | undefined;
    return row ? row.epoch : 0;
  }

  getExpiresAt(): string {
    const row = this.db.prepare("SELECT expires_at FROM sessions LIMIT 1").get() as {
      expires_at: string;
    };
    return row.expires_at;
  }

  setExpiresAt(expiresAt: string): void {
    this.db.prepare("UPDATE sessions SET expires_at=?, updated_at=?").run(expiresAt, nowIso());
  }

  // -- nodes ----------------------------------------------------------------

  /**
   * Upsert a node. Returns true iff a NEW files-row was created (i.e. this is
   * the first time we see this FILE node) — used by the caller to maintain
   * the file-count quota without SQL aggregates.
   */
  upsertNode(
    nodeId: number,
    parentId: number,
    kind: NodeKind,
    name: string,
    size: number | null = null,
    mtimeMs: number | null = null,
  ): boolean {
    const kindStr = kind === NodeKind.DIR ? "dir" : "file";
    this.db
      .prepare("INSERT OR REPLACE INTO nodes VALUES (?,?,?,?,?,?,?)")
      .run(nodeId, parentId, kindStr, name, size, mtimeMs, NodeStatus.OPEN);
    let newFile = false;
    if (kind === NodeKind.FILE) {
      const res = this.db
        .prepare("INSERT OR IGNORE INTO files (node_id, accepted_offset) VALUES (?, 0)")
        .run(nodeId);
      newFile = res.changes === 1;
    }
    return newFile;
  }

  getNode(nodeId: number): NodeRow | null {
    const row = this.db.prepare("SELECT * FROM nodes WHERE node_id=?").get(nodeId);
    return (row as unknown as NodeRow) ?? null;
  }

  setNodeStatus(nodeId: number, status: NodeStatus): void {
    this.db.prepare("UPDATE nodes SET status=? WHERE node_id=?").run(status, nodeId);
  }

  // -- files ----------------------------------------------------------------

  getFile(nodeId: number): FileRow | null {
    const row = this.db.prepare("SELECT * FROM files WHERE node_id=?").get(nodeId);
    return (row as unknown as FileRow) ?? null;
  }

  setAcceptedOffset(nodeId: number, offset: number): void {
    this.db.prepare("UPDATE files SET accepted_offset=? WHERE node_id=?").run(offset, nodeId);
  }

  setFileFinal(nodeId: number, finalSize: number, localPath: string): void {
    this.db
      .prepare("UPDATE files SET final_size=?, local_tmp_path=? WHERE node_id=?")
      .run(finalSize, localPath, nodeId);
  }

  getAllFiles(): { node_id: number; accepted_offset: number; status: string }[] {
    return this.db
      .prepare(
        "SELECT f.node_id, f.accepted_offset, n.status " +
          "FROM files f JOIN nodes n ON f.node_id = n.node_id",
      )
      .all() as { node_id: number; accepted_offset: number; status: string }[];
  }

  // -- prune / reject -------------------------------------------------------

  addPruned(nodeId: number): void {
    this.db.prepare("INSERT OR IGNORE INTO pruned VALUES (?)").run(nodeId);
  }

  isPruned(nodeId: number): boolean {
    return this.db.prepare("SELECT 1 FROM pruned WHERE node_id=?").get(nodeId) !== undefined;
  }

  getPrunedNodes(): number[] {
    return (this.db.prepare("SELECT node_id FROM pruned").all() as { node_id: number }[]).map(
      (r) => r.node_id,
    );
  }

  addRejected(nodeId: number, code: string, reason: string): void {
    this.db.prepare("INSERT OR IGNORE INTO rejected VALUES (?,?,?)").run(nodeId, code, reason);
    this.setNodeStatus(nodeId, NodeStatus.REJECTED);
  }

  isRejected(nodeId: number): boolean {
    return this.db.prepare("SELECT 1 FROM rejected WHERE node_id=?").get(nodeId) !== undefined;
  }

  getRejectedFiles(): number[] {
    return (this.db.prepare("SELECT node_id FROM rejected").all() as { node_id: number }[]).map(
      (r) => r.node_id,
    );
  }

  // -- root summary ---------------------------------------------------------

  updateRootSummary(
    scanDone: number,
    scanEst: number,
    bodyDone: number,
    bodyEst: number,
    sealed: boolean,
  ): void {
    this.db
      .prepare(
        "UPDATE root_summary SET scan_done_units=?, scan_est_units=?, " +
          "body_done_bytes=?, body_est_bytes=?, sealed=? WHERE id=1",
      )
      .run(scanDone, scanEst, bodyDone, bodyEst, sealed ? 1 : 0);
  }

  getRootSummary(): RootSummaryData {
    const row = this.db.prepare("SELECT * FROM root_summary WHERE id=1").get() as
      | {
          scan_done_units: number;
          scan_est_units: number;
          body_done_bytes: number;
          body_est_bytes: number;
          sealed: number;
        }
      | undefined;
    if (!row) {
      return {
        scan_done_units: 0,
        scan_est_units: 0,
        body_done_bytes: 0,
        body_est_bytes: 0,
        sealed: false,
      };
    }
    return {
      scan_done_units: row.scan_done_units,
      scan_est_units: row.scan_est_units,
      body_done_bytes: row.body_done_bytes,
      body_est_bytes: row.body_est_bytes,
      sealed: !!row.sealed,
    };
  }

  // -- aggregate stats for COMMIT_OK ----------------------------------------

  /** Total nodes seen (dirs + files) — commit node-count invariant. */
  countNodes(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS n FROM nodes").get() as { n: number };
    return row.n;
  }

  /** Return [fileCount, totalBytes] for all non-rejected files. */
  countCommittedFiles(): [number, number] {
    const row = this.db
      .prepare(
        "SELECT COUNT(*) AS n, COALESCE(SUM(accepted_offset), 0) AS b " +
          "FROM files f JOIN nodes n ON f.node_id = n.node_id " +
          "WHERE n.status NOT IN ('rejected', 'pruned')",
      )
      .get() as { n: number; b: number };
    return [row.n, row.b];
  }

  /** Files where accepted_offset != final_size (incomplete transfers). */
  getIncompleteFiles(): { node_id: number; accepted_offset: number; final_size: number | null }[] {
    return this.db
      .prepare(
        "SELECT f.node_id, f.accepted_offset, f.final_size " +
          "FROM files f JOIN nodes n ON f.node_id = n.node_id " +
          "WHERE n.status NOT IN ('rejected', 'pruned') " +
          "  AND (f.final_size IS NULL OR f.accepted_offset != f.final_size)",
      )
      .all() as { node_id: number; accepted_offset: number; final_size: number | null }[];
  }
}

// ---------------------------------------------------------------------------
// Staging directory helpers
// ---------------------------------------------------------------------------

export const DEFAULT_STAGING_PREFIX = ".incoming";

export function stagingDir(
  baseDir: string,
  sessionId: string,
  prefix: string = DEFAULT_STAGING_PREFIX,
): string {
  return path.join(baseDir, `${prefix}.${sessionId}`);
}

export function ensureStaging(
  baseDir: string,
  sessionId: string,
  prefix: string = DEFAULT_STAGING_PREFIX,
): string {
  const sd = stagingDir(baseDir, sessionId, prefix);
  fs.mkdirSync(path.join(sd, "payload"), { recursive: true });
  return sd;
}

export function openSessionDb(
  baseDir: string,
  sessionId: string,
  prefix: string = DEFAULT_STAGING_PREFIX,
): SessionDB {
  const sd = ensureStaging(baseDir, sessionId, prefix);
  return new SessionDB(path.join(sd, "state.sqlite"));
}

/** Reject names that could escape the payload directory. */
export function validateNodeName(name: string): void {
  if (!name) {
    throw new Error("empty node name");
  }
  if (name === "." || name === "..") {
    throw new Error(`illegal node name: ${JSON.stringify(name)}`);
  }
  if (name.includes("/") || name.includes("\\") || name.includes("\x00")) {
    throw new Error(`illegal characters in node name: ${JSON.stringify(name)}`);
  }
}

/**
 * Build the filesystem path for a node inside the payload directory.
 *
 * Walks the parent_id chain in the DB to reconstruct the relative path.
 * Validates each name component to prevent path traversal.
 */
export function resolvePayloadPath(
  baseDir: string,
  sessionId: string,
  db: SessionDB,
  nodeId: number,
  prefix: string = DEFAULT_STAGING_PREFIX,
): string {
  const parts: string[] = [];
  let curId = nodeId;
  const seen = new Set<number>();
  while (curId !== ROOT_NODE_ID) {
    if (seen.has(curId)) {
      throw new Error(`parent cycle detected at node ${curId}`);
    }
    seen.add(curId);
    const node = db.getNode(curId);
    if (node === null) {
      // A silent break here would resolve a SHORTENED path and write the
      // file into the wrong directory. Broken chains are an error.
      throw new Error(`broken parent chain: node ${curId} unknown`);
    }
    validateNodeName(node.name);
    parts.push(node.name);
    curId = node.parent_id;
  }
  parts.reverse();
  const payloadRoot = path.join(stagingDir(baseDir, sessionId, prefix), "payload");
  const result = parts.length > 0 ? path.join(payloadRoot, ...parts) : payloadRoot;
  // Defense in depth: resolved path must stay within the payload dir.
  const rel = path.relative(path.resolve(payloadRoot), path.resolve(result));
  if (rel.startsWith("..") || path.isAbsolute(rel)) {
    throw new Error(`path traversal detected: ${result}`);
  }
  return result;
}
