/**
 * MemoryStore semantics + the ephemeral-store restart story: after a
 * process restart the engine re-discovers live sessions by scanning the
 * base dir (glob) — including per-user homes one level down — and reclaims
 * expired ones immediately.
 */

import * as fs from "node:fs";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  MemoryStore,
  MfupEngine,
  SessionRegistry,
  SessionState,
  openSessionDb,
  reconcileOrphans,
  resolveOptions,
  resolveStore,
  stagingDir,
} from "../src/index.js";
import { scanStagingDirs } from "../src/engine.js";
import { mkTmpDir, rmrf } from "./helpers.js";

const tmps: string[] = [];
function tmp(): string {
  const t = mkTmpDir();
  tmps.push(t);
  return t;
}
afterEach(() => {
  for (const t of tmps.splice(0)) rmrf(t);
});

function makeStagedSession(
  base: string,
  sid: string,
  opts: { expiresInMs?: number; state?: SessionState } = {},
): void {
  const db = openSessionDb(base, sid);
  const expires = new Date(Date.now() + (opts.expiresInMs ?? 3600_000)).toISOString();
  db.initSession(sid, `tok-${sid}`, expires, ".");
  if (opts.state) db.setState(opts.state);
  db.close();
}

describe("MemoryStore", () => {
  it("register / expiry / meta / remove roundtrip", async () => {
    const store = new MemoryStore();
    expect(store.ephemeral).toBe(true);
    const soon = new Date(Date.now() + 1000);
    await store.register("a", soon, "target-a", "/staging/a");
    await store.register("b", new Date(Date.now() - 1000), "target-b", "/staging/b");

    expect(await store.isRegistered("a")).toBe(true);
    expect((await store.getMeta("a"))?.stagingDir).toBe("/staging/a");
    expect(await store.getExpired()).toEqual(["b"]);
    expect(await store.getNotExpired()).toEqual(["a"]);
    expect((await store.allSessions()).sort()).toEqual(["a", "b"]);

    await store.updateExpiry("b", new Date(Date.now() + 60_000));
    expect(await store.getExpired()).toEqual([]);

    await store.remove("a");
    expect(await store.isRegistered("a")).toBe(false);
    expect(await store.getMeta("a")).toBeNull();
  });

  it("resolveStore: memory default, unknown string rejected", async () => {
    expect((await resolveStore(undefined)).ephemeral).toBe(true);
    expect((await resolveStore("memory")).ephemeral).toBe(true);
    await expect(resolveStore("postgres://nope")).rejects.toThrow(/unrecognized store/);
  });
});

describe("filesystem recovery (ephemeral store restart)", () => {
  it("scanStagingDirs finds base-level and per-user staging dirs, not deeper", async () => {
    const base = tmp();
    makeStagedSession(base, "top");
    fs.mkdirSync(path.join(base, "user1"), { recursive: true });
    makeStagedSession(path.join(base, "user1"), "nested");
    // depth 3 — must NOT be found at scanDepth 2
    fs.mkdirSync(path.join(base, "a", "b"), { recursive: true });
    makeStagedSession(path.join(base, "a", "b"), "deep");

    const found = await scanStagingDirs(base, ".incoming", 2);
    expect(found.map((f) => f.sessionId).sort()).toEqual(["nested", "top"]);
  });

  it("engine startup recovers live sessions from disk and reclaims expired ones", async () => {
    const base = tmp();
    makeStagedSession(base, "alive");
    makeStagedSession(base, "dead", { expiresInMs: -1000 });
    fs.mkdirSync(path.join(base, "alice"), { recursive: true });
    makeStagedSession(path.join(base, "alice"), "homed");

    const engine = new MfupEngine(resolveOptions({ baseDir: base, authorize: async () => ({}) }));
    await engine.startup();
    try {
      const registry = engine.requireRegistry();
      const store = engine.requireStore();

      // Live sessions recovered into the registry AND re-registered in the store
      expect(registry.get("alive")).not.toBeNull();
      expect(registry.get("alive")!.state).toBe(SessionState.WAITING_RESUME);
      expect(await store.isRegistered("alive")).toBe(true);
      expect(registry.get("homed")).not.toBeNull();
      expect(registry.get("homed")!.baseDir).toBe(path.join(base, "alice"));

      // Expired one reclaimed immediately
      expect(registry.get("dead")).toBeNull();
      expect(fs.existsSync(stagingDir(base, "dead"))).toBe(false);

      // A recovered session accepts RESUME with its persisted token
      const resumed = registry.resume("alive", "tok-alive", "leg2");
      expect(resumed.state).toBe(SessionState.ACTIVE);
    } finally {
      await engine.shutdown();
    }
  });

  it("terminal-state staging is not recovered and falls to reconciliation", async () => {
    const base = tmp();
    makeStagedSession(base, "gone", { state: SessionState.ABORTED });

    const engine = new MfupEngine(resolveOptions({ baseDir: base, authorize: async () => ({}) }));
    await engine.startup();
    try {
      expect(engine.requireRegistry().get("gone")).toBeNull();
      // Still on disk (grace window protects it) …
      expect(fs.existsSync(stagingDir(base, "gone"))).toBe(true);
      // … until reconciliation with zero grace removes it.
      const removed = await reconcileOrphans(
        base,
        engine.requireRegistry(),
        engine.requireStore(),
        ".incoming",
        0,
      );
      expect(removed).toEqual(["gone"]);
      expect(fs.existsSync(stagingDir(base, "gone"))).toBe(false);
    } finally {
      await engine.shutdown();
    }
  });

  it("reconcileOrphans respects the grace window and store registration", async () => {
    const base = tmp();
    const registry = new SessionRegistry(base);
    const store = new MemoryStore();

    makeStagedSession(base, "orphan");
    makeStagedSession(base, "tracked");
    await store.register("tracked", new Date(Date.now() + 3600_000), ".", stagingDir(base, "tracked"));

    // Fresh dirs survive a graced pass
    expect(await reconcileOrphans(base, registry, store, ".incoming", 600)).toEqual([]);
    // Zero grace: the orphan goes, the tracked one stays
    const removed = await reconcileOrphans(base, registry, store, ".incoming", 0);
    expect(removed).toEqual(["orphan"]);
    expect(fs.existsSync(stagingDir(base, "tracked"))).toBe(true);
  });
});
