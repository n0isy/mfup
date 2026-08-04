/**
 * Session store — the expiry index + path metadata behind the sweeper,
 * recovery and lazy resume. Pluggable; two built-ins:
 *
 *   - MemoryStore (default): a Map. Right for the typical single-process
 *     Node deployment. It is EPHEMERAL — after a restart the engine
 *     re-populates it by scanning the base dir for staging directories
 *     (state.sqlite carries expiry/target), so single-process deployments
 *     get restart recovery without any external service.
 *
 *   - RedisStore (./store-redis.js): full parity with the Python server —
 *     survives restarts on its own, supports lazy resume/failover across
 *     workers, and keeps cleanup O(expired) with no directory scans.
 *     `redis` is an optional peer dependency, imported only when used.
 *
 * Key semantics (mirroring mfup_core/redis_index.py): a session is scored
 * by its expiry timestamp; meta holds the target dir and the ABSOLUTE
 * staging dir (what makes sweeper + recovery work with per-user base dirs).
 */

export interface SessionMeta {
  targetDir: string;
  stagingDir: string;
}

export interface SessionStore {
  /** True when the store's contents do not survive a process restart.
   * The engine runs a filesystem recovery scan at startup for ephemeral
   * stores (and skips it for durable ones, which drive recovery themselves). */
  readonly ephemeral: boolean;

  register(sessionId: string, expiresAt: Date, targetDir: string, stagingDir: string): Promise<void>;
  updateExpiry(sessionId: string, expiresAt: Date): Promise<void>;
  /** Session ids whose expiry is <= now. */
  getExpired(now?: Date): Promise<string[]>;
  /** Session ids whose expiry is > now (still alive). */
  getNotExpired(now?: Date): Promise<string[]>;
  isRegistered(sessionId: string): Promise<boolean>;
  getMeta(sessionId: string): Promise<SessionMeta | null>;
  /** Every id regardless of expiry. */
  allSessions(): Promise<string[]>;
  remove(sessionId: string): Promise<void>;
  close(): Promise<void>;
}

interface MemoryEntry {
  expiresAtMs: number;
  targetDir: string;
  stagingDir: string;
}

/** In-process store. See module docs for the restart story. */
export class MemoryStore implements SessionStore {
  readonly ephemeral = true;
  private entries = new Map<string, MemoryEntry>();

  async register(
    sessionId: string,
    expiresAt: Date,
    targetDir: string,
    stagingDir: string,
  ): Promise<void> {
    this.entries.set(sessionId, { expiresAtMs: expiresAt.getTime(), targetDir, stagingDir });
  }

  async updateExpiry(sessionId: string, expiresAt: Date): Promise<void> {
    const e = this.entries.get(sessionId);
    if (e) {
      e.expiresAtMs = expiresAt.getTime();
    }
  }

  async getExpired(now: Date = new Date()): Promise<string[]> {
    const t = now.getTime();
    const out: string[] = [];
    for (const [sid, e] of this.entries) {
      if (e.expiresAtMs <= t) out.push(sid);
    }
    return out;
  }

  async getNotExpired(now: Date = new Date()): Promise<string[]> {
    const t = now.getTime();
    const out: string[] = [];
    for (const [sid, e] of this.entries) {
      if (e.expiresAtMs > t) out.push(sid);
    }
    return out;
  }

  async isRegistered(sessionId: string): Promise<boolean> {
    return this.entries.has(sessionId);
  }

  async getMeta(sessionId: string): Promise<SessionMeta | null> {
    const e = this.entries.get(sessionId);
    return e ? { targetDir: e.targetDir, stagingDir: e.stagingDir } : null;
  }

  async allSessions(): Promise<string[]> {
    return [...this.entries.keys()];
  }

  async remove(sessionId: string): Promise<void> {
    this.entries.delete(sessionId);
  }

  async close(): Promise<void> {
    this.entries.clear();
  }
}

/**
 * Resolve the `store` option: a SessionStore instance passes through,
 * "memory"/undefined builds a MemoryStore, and a "redis://…" URL lazily
 * imports the Redis adapter (so `redis` stays an optional dependency).
 */
export async function resolveStore(
  store: SessionStore | string | undefined,
): Promise<SessionStore> {
  if (store === undefined || store === "memory") {
    return new MemoryStore();
  }
  if (typeof store === "string") {
    if (store.startsWith("redis://") || store.startsWith("rediss://")) {
      const { RedisStore } = await import("./store-redis.js");
      return RedisStore.connect(store);
    }
    throw new Error(
      `unrecognized store ${JSON.stringify(store)}: expected "memory", a redis:// URL, or a SessionStore instance`,
    );
  }
  return store;
}
