/**
 * MfupEngine — owns the session registry, the session store, the sweeper and
 * the consumer hooks. The HTTP/WS surface lives in handler.ts; consumer
 * backends can also drive the engine directly (engine.publish(), sweep()).
 *
 * Port of server/mfup-fastapi/mfup_fastapi/engine.py (lifecycle half).
 */

import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";

import { createLogger } from "./logger.js";
import type {
  AuthorizeHook,
  CommitEvent,
  FileMapRequest,
  MapFileHook,
  OnCommittedHook,
} from "./hooks.js";
import { SessionState } from "./protocol.js";
import { LiveSession, SessionRegistry } from "./session.js";
import { DEFAULT_STAGING_PREFIX, stagingDir } from "./storage.js";
import {
  ConflictError,
  MappingError,
  listPayloadFiles,
  publishSession,
  publishSessionMapped,
} from "./publish.js";
import { MemoryStore, resolveStore, type SessionStore } from "./store.js";

const logger = createLogger("mfup.engine");

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface MfupOptions {
  /** Global base directory for staging + publish (a per-session baseDir from
   * the authorize hook overrides it per session). */
  baseDir: string;
  /** Session store: "memory" (default), a "redis://…" URL, or a custom
   * SessionStore. Memory is right for single-process deployments — after a
   * restart the engine re-discovers live sessions by scanning baseDir for
   * staging directories. Redis adds cross-worker lazy resume/failover and
   * scan-free cleanup, wire-compatible with the Python server's index. */
  store?: SessionStore | string;
  /** Seconds a session stays resumable; sets expires_at at HELLO. */
  sessionResumeTtl?: number;
  /** Seconds of leg inactivity before the leg is detached. */
  legIdleTimeout?: number;
  /** Largest accepted chunk; advertised in HELLO_OK.limits. */
  maxChunkBytes?: number;
  /** Concurrently open files per session; advertised in HELLO_OK.limits. */
  maxOpenFiles?: number;
  /** Pending-file window; advertised in HELLO_OK.limits. */
  maxPendingFiles?: number;
  /** Seconds between sweeper passes. */
  sweepInterval?: number;
  /** Prefix of per-session staging dir names ({prefix}.{session_id}). */
  stagingPrefix?: string;
  /** Run the filesystem-orphan reconciliation every Nth sweep. */
  reconcileEvery?: number;
  /** Minimum staging-dir age (seconds) before it may be reconciled away. */
  orphanGraceSeconds?: number;
  /** How many directory levels below baseDir to scan for staging dirs
   * (memory-store restart recovery + orphan reconciliation). Depth 2 covers
   * both `<base>/.incoming.*` and per-user `<base>/<uid>/.incoming.*`. */
  scanDepth?: number;
  /** Buffered-body threshold for atomic batch POSTs; also the hard cap that
   * produces 413 body_too_large. */
  maxBufferedBody?: number;
  /** Cap on the JSON size of HELLO.meta. */
  maxMetaBytes?: number;
  /** Bearer for the x-mfup-admin-token header; empty/unset disables the
   * admin routes entirely. */
  adminToken?: string;
  /** Mount prefix when the handler cannot infer it from req.url (raw
   * http.createServer with a path prefix). With express/vite middleware
   * mounting this is unnecessary. */
  basePath?: string;
  /** Consumer hooks (see hooks.ts). No authorize hook = allow-all (warned). */
  authorize?: AuthorizeHook;
  mapFile?: MapFileHook;
  onCommitted?: OnCommittedHook;
}

export interface ResolvedMfupOptions {
  baseDir: string;
  store: SessionStore | string;
  sessionResumeTtl: number;
  legIdleTimeout: number;
  maxChunkBytes: number;
  maxOpenFiles: number;
  maxPendingFiles: number;
  sweepInterval: number;
  stagingPrefix: string;
  reconcileEvery: number;
  orphanGraceSeconds: number;
  scanDepth: number;
  maxBufferedBody: number;
  maxMetaBytes: number;
  adminToken: string;
  basePath: string;
  authorize: AuthorizeHook | null;
  mapFile: MapFileHook | null;
  onCommitted: OnCommittedHook | null;
}

export function resolveOptions(opts: MfupOptions): ResolvedMfupOptions {
  return {
    baseDir: path.resolve(opts.baseDir),
    store: opts.store ?? "memory",
    sessionResumeTtl: opts.sessionResumeTtl ?? 3600,
    legIdleTimeout: opts.legIdleTimeout ?? 60,
    maxChunkBytes: opts.maxChunkBytes ?? 262144,
    maxOpenFiles: opts.maxOpenFiles ?? 1,
    maxPendingFiles: opts.maxPendingFiles ?? 64,
    sweepInterval: opts.sweepInterval ?? 300,
    stagingPrefix: opts.stagingPrefix ?? DEFAULT_STAGING_PREFIX,
    reconcileEvery: opts.reconcileEvery ?? 4,
    orphanGraceSeconds: opts.orphanGraceSeconds ?? 600,
    scanDepth: opts.scanDepth ?? 2,
    maxBufferedBody: opts.maxBufferedBody ?? 16 * 1024 * 1024,
    maxMetaBytes: opts.maxMetaBytes ?? 16384,
    adminToken: opts.adminToken ?? "",
    basePath: opts.basePath ?? "",
    authorize: opts.authorize ?? null,
    mapFile: opts.mapFile ?? null,
    onCommitted: opts.onCommitted ?? null,
  };
}

// ---------------------------------------------------------------------------
// Typed publish errors
// ---------------------------------------------------------------------------

export class PublishError extends Error {}

export class SessionNotFound extends PublishError {}

export class NotCommitted extends PublishError {
  constructor(readonly state: string) {
    super(`cannot publish session in state ${state}`);
  }
}

export class TargetEscapes extends PublishError {}

export class MapFileHookError extends PublishError {
  constructor(readonly path: string) {
    super(`mapFile hook raised for ${path}`);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Check that targetDir resolves within baseDir (no path traversal). */
export function isSafeTarget(baseDir: string, targetDir: string): boolean {
  const resolved = path.isAbsolute(targetDir)
    ? path.resolve(targetDir)
    : path.resolve(baseDir, targetDir);
  const rel = path.relative(path.resolve(baseDir), resolved);
  return !rel.startsWith("..") && !path.isAbsolute(rel);
}

/**
 * Depth-limited scan of `base` for staging directories `{prefix}.{sid}`.
 * Depth 1 = only direct children of base; depth 2 also checks each child
 * directory's children (per-user homes) — never descends INTO a staging dir.
 */
export async function scanStagingDirs(
  base: string,
  prefix: string,
  depth: number,
): Promise<{ sessionId: string; stagingPath: string }[]> {
  const found: { sessionId: string; stagingPath: string }[] = [];
  const marker = `${prefix}.`;

  const walk = async (dir: string, remaining: number): Promise<void> => {
    let entries: fs.Dirent[];
    try {
      entries = await fsp.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const p = path.join(dir, e.name);
      if (e.name.startsWith(marker)) {
        found.push({ sessionId: e.name.slice(marker.length), stagingPath: p });
      } else if (remaining > 1) {
        await walk(p, remaining - 1);
      }
    }
  };

  await walk(base, Math.max(1, depth));
  return found;
}

/**
 * Remove staging dirs that no live session and no store entry reference.
 *
 * The store-driven sweeper can only clean sessions the store still knows
 * about. A staging dir becomes an unreachable orphan when cleanup was
 * interrupted between rm and store-remove, when rm silently failed, or when
 * the store lost the entry. Such a dir can never be resumed nor swept — so
 * it would accumulate forever. This scan is the retention safety net.
 *
 * An orphan is removed only when ALL hold, to avoid racing a live upload:
 *   - not in the in-memory registry,
 *   - not registered in the store,
 *   - last modified at least graceSeconds ago.
 */
export async function reconcileOrphans(
  base: string,
  registry: SessionRegistry,
  store: SessionStore,
  prefix: string,
  graceSeconds = 600,
  scanDepth = 2,
): Promise<string[]> {
  const removed: string[] = [];
  if (!fs.existsSync(base)) return removed;

  const now = Date.now();
  for (const { sessionId: sid, stagingPath } of await scanStagingDirs(base, prefix, scanDepth)) {
    if (registry.get(sid) !== null) continue; // live in this process
    try {
      if (await store.isRegistered(sid)) continue; // store still tracks it
    } catch {
      // Store unreachable — do not delete anything we cannot verify.
      logger.warn(`Reconcile: cannot verify session ${sid} in store, skipping`);
      continue;
    }
    let ageMs: number;
    try {
      ageMs = now - (await fsp.stat(stagingPath)).mtimeMs;
    } catch {
      continue;
    }
    if (ageMs < graceSeconds * 1000) continue; // might be mid-registration

    await fsp.rm(stagingPath, { recursive: true, force: true });
    // Belt and suspenders: drop any half-written store entry too.
    try {
      await store.remove(sid);
    } catch {
      /* best effort */
    }
    removed.push(sid);
  }

  return removed;
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export class MfupEngine {
  readonly cfg: ResolvedMfupOptions;
  registry: SessionRegistry | null = null;
  store: SessionStore | null = null;

  private sweepTimer: NodeJS.Timeout | null = null;
  private sweeping = false;
  private sweeps = 0;
  private startedUp = false;

  constructor(cfg: ResolvedMfupOptions) {
    this.cfg = cfg;
    if (cfg.authorize === null) {
      logger.warn(
        "no authorize hook configured — running WITHOUT authorization (allow-all). " +
          "Do not do this in production.",
      );
    }
  }

  // -- lifecycle -------------------------------------------------------------

  async startup(): Promise<void> {
    if (this.startedUp) return;
    this.startedUp = true;
    const cfg = this.cfg;
    await fsp.mkdir(cfg.baseDir, { recursive: true });

    this.store = await resolveStore(cfg.store);
    this.registry = new SessionRegistry(cfg.baseDir, cfg.stagingPrefix, {
      sessionResumeTtl: cfg.sessionResumeTtl,
      legIdleTimeout: cfg.legIdleTimeout,
      maxChunkBytes: cfg.maxChunkBytes,
      // With a mapFile hook the client's layout no longer predicts final
      // paths — the ingest-time conflict ASK would be noise; publish-time
      // conflict handling (409 → action) takes over.
      conflictCheck: cfg.mapFile === null,
    });

    try {
      if (this.store.ephemeral) {
        await this.recoverFromFilesystem();
      } else {
        await this.recoverFromStore();
      }
    } catch (exc) {
      logger.error(`Startup: session recovery failed: ${exc}`);
    }

    // One filesystem reconciliation at startup: catches staging dirs
    // orphaned by a crash between rm and store-remove, a silently-failed
    // rm, or a lost store — none of which the store-driven sweeper can find.
    try {
      const removed = await reconcileOrphans(
        cfg.baseDir,
        this.registry,
        this.store,
        cfg.stagingPrefix,
        cfg.orphanGraceSeconds,
        cfg.scanDepth,
      );
      if (removed.length > 0) {
        logger.warn(
          `Startup: reconciled ${removed.length} orphaned staging dir(s): ${removed.join(", ")}`,
        );
      }
    } catch (exc) {
      logger.error(`Startup: orphan reconciliation failed: ${exc}`);
    }

    this.sweepTimer = setInterval(() => {
      void this.sweepTick();
    }, cfg.sweepInterval * 1000);
    this.sweepTimer.unref();
  }

  async shutdown(): Promise<void> {
    if (this.sweepTimer !== null) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    if (this.registry !== null) {
      for (const [, session] of this.registry.allSessions()) {
        session.cancelIdleTimer();
        session.closeAllWriters();
        session.db.close();
      }
    }
    if (this.store !== null) {
      await this.store.close();
    }
    this.startedUp = false;
  }

  requireRegistry(): SessionRegistry {
    if (this.registry === null) throw new Error("MfupEngine.startup() has not run");
    return this.registry;
  }

  requireStore(): SessionStore {
    if (this.store === null) throw new Error("MfupEngine.startup() has not run");
    return this.store;
  }

  // -- recovery ----------------------------------------------------------------

  /** Durable store (Redis): recover live sessions the store knows about. */
  private async recoverFromStore(): Promise<void> {
    const store = this.requireStore();
    const registry = this.requireRegistry();
    const aliveIds = await store.getNotExpired();
    const expiredIds = await store.getExpired();
    logger.info(`Startup: store has ${aliveIds.length} alive + ${expiredIds.length} expired session(s)`);
    let recovered = 0;
    for (const sid of aliveIds) {
      const meta = await store.getMeta(sid);
      if (!meta || !meta.stagingDir) {
        logger.warn(`Startup: no meta for session ${sid}, skipping`);
        continue;
      }
      const session = registry.recoverSession(sid, meta.stagingDir);
      if (session) {
        session.onExpiryChange = (s, e) => store.updateExpiry(s, e);
        recovered += 1;
      }
    }
    logger.info(`Startup: recovered ${recovered} session(s) from disk`);
  }

  /**
   * Ephemeral store (memory): the store is empty after a restart, so
   * re-discover sessions by scanning the base dir for staging directories —
   * each carries its own state.sqlite with expiry, target and auth state.
   * Live ones are recovered AND re-registered in the store; expired ones
   * are reclaimed immediately.
   */
  private async recoverFromFilesystem(): Promise<void> {
    const store = this.requireStore();
    const registry = this.requireRegistry();
    const cfg = this.cfg;
    const dirs = await scanStagingDirs(cfg.baseDir, cfg.stagingPrefix, cfg.scanDepth);
    if (dirs.length === 0) return;
    logger.info(`Startup: found ${dirs.length} staging dir(s) on disk`);
    let recovered = 0;
    let reclaimed = 0;
    for (const { sessionId: sid, stagingPath } of dirs) {
      const session = registry.recoverSession(sid, stagingPath);
      if (session === null) continue; // terminal/unreadable — reconcile handles it
      const expired = new Date(session.expiresAt).getTime() <= Date.now();
      if (expired) {
        registry.remove(sid);
        await fsp.rm(stagingPath, { recursive: true, force: true });
        reclaimed += 1;
        continue;
      }
      await store.register(sid, new Date(session.expiresAt), session.targetDir, stagingPath);
      session.onExpiryChange = (s, e) => store.updateExpiry(s, e);
      recovered += 1;
    }
    logger.info(`Startup: recovered ${recovered} session(s), reclaimed ${reclaimed} expired`);
  }

  // -- sweeper -----------------------------------------------------------------

  private async sweepTick(): Promise<void> {
    if (this.sweeping) return; // a slow pass must not overlap the next tick
    this.sweeping = true;
    try {
      await this.sweep();
      this.sweeps += 1;
      if (this.sweeps % this.cfg.reconcileEvery === 0) {
        const orphans = await reconcileOrphans(
          this.cfg.baseDir,
          this.requireRegistry(),
          this.requireStore(),
          this.cfg.stagingPrefix,
          this.cfg.orphanGraceSeconds,
          this.cfg.scanDepth,
        );
        if (orphans.length > 0) {
          logger.warn(`Sweeper reconciled ${orphans.length} orphaned staging dir(s): ${orphans.join(", ")}`);
        }
      }
    } catch (exc) {
      logger.error(`Sweeper error: ${exc}`);
    } finally {
      this.sweeping = false;
    }
  }

  /** Remove expired sessions (staging dir + store entry). Returns their ids. */
  async sweep(): Promise<string[]> {
    const registry = this.requireRegistry();
    const store = this.requireStore();
    const expiredIds = await store.getExpired();
    const removed: string[] = [];
    for (const sid of expiredIds) {
      const meta = await store.getMeta(sid);
      registry.remove(sid);
      const sd =
        meta && meta.stagingDir
          ? meta.stagingDir
          : stagingDir(this.cfg.baseDir, sid, this.cfg.stagingPrefix);
      if (fs.existsSync(sd)) {
        await fsp.rm(sd, { recursive: true, force: true });
      }
      await store.remove(sid);
      removed.push(sid);
      logger.info(`Sweeper cleaned session ${sid} (staging=${sd})`);
    }
    if (removed.length > 0) {
      logger.info(`Sweeper removed ${removed.length} sessions`);
    }
    return removed;
  }

  // -- programmatic publish ----------------------------------------------------

  /**
   * Publish a committed session server-side (consumer backends and the
   * onCommitted auto-publish path). Throws typed PublishError subclasses;
   * ConflictError/MappingError pass through.
   */
  async publish(sessionId: string): Promise<string[]> {
    const registry = this.requireRegistry();
    const session = registry.get(sessionId);
    if (session === null) {
      throw new SessionNotFound(sessionId);
    }
    if (session.state !== SessionState.COMMITTED) {
      throw new NotCommitted(session.state);
    }

    const target = path.isAbsolute(session.targetDir)
      ? session.targetDir
      : path.join(session.baseDir, session.targetDir);

    // Defense in depth: verify the target stays within the session's base dir
    if (!isSafeTarget(session.baseDir, session.targetDir)) {
      throw new TargetEscapes(session.targetDir);
    }

    let published: string[];
    if (this.cfg.mapFile !== null) {
      // Per-file layout is the consumer's: run the (async) hook per file
      // first, then hand the precomputed plan to the mover.
      const files = await listPayloadFiles(session.baseDir, sessionId, this.cfg.stagingPrefix);
      const mapping: Record<string, string> = {};
      for (const [rel, size] of files) {
        let mapped: string | null | undefined;
        try {
          mapped = await this.cfg.mapFile({
            sessionId,
            path: rel,
            name: rel.split("/").pop() ?? rel,
            size,
            targetDir: session.targetDir,
            meta: session.clientMeta,
            context: session.authContext,
          } satisfies FileMapRequest);
        } catch (exc) {
          logger.error(`mapFile hook raised for ${sessionId} (${rel}): ${exc}`);
          throw new MapFileHookError(rel);
        }
        if (mapped !== null && mapped !== undefined) {
          mapping[rel] = mapped;
        }
      }
      published = await publishSessionMapped(
        session.baseDir,
        sessionId,
        target,
        mapping,
        this.cfg.stagingPrefix,
        session.publishAction,
      );
    } else {
      published = await publishSession(
        session.baseDir,
        sessionId,
        target,
        this.cfg.stagingPrefix,
        session.publishAction,
      );
    }

    registry.remove(sessionId);
    await this.requireStore().remove(sessionId);
    return published;
  }

  // -- onCommitted -------------------------------------------------------------

  /**
   * Fire the consumer's onCommitted hook once per commit. A hook error must
   * never damage the session; a "publish" return triggers server-side
   * publish immediately.
   */
  private async afterCommit(session: LiveSession, result: { files: number; bytes: number }): Promise<void> {
    if (this.cfg.onCommitted === null) return;
    const sd = stagingDir(session.baseDir, session.sessionId, this.cfg.stagingPrefix);
    let verdict: string | null | undefined;
    try {
      verdict = await this.cfg.onCommitted({
        sessionId: session.sessionId,
        targetDir: session.targetDir,
        baseDir: session.baseDir,
        stagingDir: sd,
        files: result.files,
        bytes: result.bytes,
        meta: session.clientMeta,
        context: session.authContext,
      } satisfies CommitEvent);
    } catch (exc) {
      logger.error(`onCommitted hook raised for session ${session.sessionId}: ${exc}`);
      return;
    }
    if (verdict === "publish") {
      try {
        const published = await this.publish(session.sessionId);
        logger.info(`onCommitted auto-published session ${session.sessionId}: ${published.join(", ")}`);
      } catch (exc) {
        logger.error(`onCommitted auto-publish failed for session ${session.sessionId}: ${exc}`);
      }
    }
  }

  /** tryCommit + onCommitted, so no call site can forget the hook. */
  async tryCommit(session: LiveSession): Promise<{ files: number; bytes: number } | null> {
    const result = await session.tryCommit();
    if (result !== null && session.state === SessionState.COMMITTED) {
      await this.afterCommit(session, result);
    }
    return result;
  }
}

export { ConflictError, MappingError };
