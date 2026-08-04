/**
 * Redis session store — full parity with the Python server's SessionIndex
 * (mfup_core/redis_index.py): the SAME key layout, so a Node and a Python
 * MFUP server pointed at one Redis are interchangeable.
 *
 * Keys:
 *   mfup:sessions            — sorted set: session_id → expires_at (unix ts)
 *   mfup:meta:{session_id}   — hash: target_dir, staging_dir (full paths)
 *
 * Sweeper flow: ZRANGEBYSCORE → HGETALL meta → rm(staging_dir) → ZREM + DEL.
 * No directory scans. No SQLite opens for cleanup.
 *
 * `redis` (node-redis v4+) is an optional peer dependency; this module is
 * imported lazily by resolveStore() only when a redis:// URL is passed.
 */

import type { SessionMeta, SessionStore } from "./store.js";

export const SESSIONS_KEY = "mfup:sessions";
export const META_PREFIX = "mfup:meta:";

// Minimal structural type of the node-redis client we use — avoids a hard
// type-level dependency on the optional package.
interface RedisClientLike {
  connect(): Promise<unknown>;
  quit(): Promise<unknown>;
  zAdd(key: string, member: { score: number; value: string }): Promise<unknown>;
  zRangeByScore(key: string, min: number | string, max: number | string): Promise<string[]>;
  zScore(key: string, member: string): Promise<number | null>;
  zRange(key: string, start: number, stop: number): Promise<string[]>;
  zRem(key: string, member: string): Promise<unknown>;
  hSet(key: string, value: Record<string, string>): Promise<unknown>;
  hGetAll(key: string): Promise<Record<string, string>>;
  del(key: string): Promise<unknown>;
}

export class RedisStore implements SessionStore {
  readonly ephemeral = false;

  private constructor(private redis: RedisClientLike) {}

  static async connect(url: string): Promise<RedisStore> {
    let createClient: (opts: { url: string }) => RedisClientLike;
    try {
      ({ createClient } = (await import("redis")) as unknown as {
        createClient: (opts: { url: string }) => RedisClientLike;
      });
    } catch (cause) {
      throw new Error(
        'store is a redis:// URL but the "redis" package is not installed — npm install redis',
        { cause },
      );
    }
    const client = createClient({ url });
    await client.connect();
    return new RedisStore(client);
  }

  /** Wrap an already-connected node-redis client (consumer-managed). */
  static from(client: unknown): RedisStore {
    return new RedisStore(client as RedisClientLike);
  }

  async register(
    sessionId: string,
    expiresAt: Date,
    targetDir: string,
    stagingDir: string,
  ): Promise<void> {
    await Promise.all([
      this.redis.zAdd(SESSIONS_KEY, { score: expiresAt.getTime() / 1000, value: sessionId }),
      this.redis.hSet(META_PREFIX + sessionId, {
        target_dir: targetDir,
        staging_dir: stagingDir,
      }),
    ]);
  }

  async updateExpiry(sessionId: string, expiresAt: Date): Promise<void> {
    await this.redis.zAdd(SESSIONS_KEY, {
      score: expiresAt.getTime() / 1000,
      value: sessionId,
    });
  }

  async getExpired(now: Date = new Date()): Promise<string[]> {
    return this.redis.zRangeByScore(SESSIONS_KEY, "-inf", now.getTime() / 1000);
  }

  async getNotExpired(now: Date = new Date()): Promise<string[]> {
    return this.redis.zRangeByScore(SESSIONS_KEY, now.getTime() / 1000, "+inf");
  }

  async isRegistered(sessionId: string): Promise<boolean> {
    return (await this.redis.zScore(SESSIONS_KEY, sessionId)) !== null;
  }

  async getMeta(sessionId: string): Promise<SessionMeta | null> {
    const data = await this.redis.hGetAll(META_PREFIX + sessionId);
    if (!data || Object.keys(data).length === 0) return null;
    return {
      targetDir: data.target_dir ?? ".",
      stagingDir: data.staging_dir ?? "",
    };
  }

  async allSessions(): Promise<string[]> {
    return this.redis.zRange(SESSIONS_KEY, 0, -1);
  }

  async remove(sessionId: string): Promise<void> {
    await Promise.all([
      this.redis.zRem(SESSIONS_KEY, sessionId),
      this.redis.del(META_PREFIX + sessionId),
    ]);
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}
