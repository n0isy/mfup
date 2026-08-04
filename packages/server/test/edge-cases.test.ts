/**
 * Hardening suite — mirrors server/tests/test_edge_cases.py case by case.
 */

import * as fs from "node:fs";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  MAX_COMMIT_RETRIES,
  MemoryStore,
  MfupEngine,
  NodeKind,
  NotCommitted,
  SessionNotFound,
  SessionRegistry,
  SessionState,
  listPayloadFiles,
  publishSession,
  publishSessionMapped,
  resolvePayloadPath,
  stagingDir,
  validateNodeName,
  MappingError,
  openSessionDb,
  resolveOptions,
  type CommitEvent,
} from "../src/index.js";
import {
  FakeWS,
  fileChunkFrame,
  fileCloseFrame,
  fileOpenFrame,
  makeSession,
  mkTmpDir,
  nodeFrame,
  rmrf,
  sendTree,
  sessionEndFrame,
  uploadTree,
  utf8,
} from "./helpers.js";

const tmps: string[] = [];
function tmp(): string {
  const t = mkTmpDir();
  tmps.push(t);
  return t;
}
afterEach(() => {
  for (const t of tmps.splice(0)) rmrf(t);
});

// ---------------------------------------------------------------------------
// Name validation and path traversal
// ---------------------------------------------------------------------------

describe("name validation and path traversal", () => {
  it.each(["", ".", "..", "a/b", "a\\b", "a\x00b", "/", "..\\.."])(
    "validateNodeName rejects %j",
    (name) => {
      expect(() => validateNodeName(name)).toThrow();
    },
  );

  it.each(["file.txt", "a b c", "файл ❤.md", ".hidden", "..foo", "foo.."])(
    "validateNodeName accepts %j",
    (name) => {
      expect(() => validateNodeName(name)).not.toThrow();
    },
  );

  it("illegal-name node is dropped, counted, and rejected", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "..", 10), "leg1");
    expect(session.db.getNode(1)).toBeNull();
    expect(session.droppedNodes.has(1)).toBe(true);
    expect(ws.ofType("REJECT_FILE")).toHaveLength(1);
  });

  it("traversal name never escapes the payload root", async () => {
    const base = tmp();
    const { session } = makeSession(base, "s1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "../evil", 10), "leg1");
    expect(session.db.getNode(1)).toBeNull();
    expect(fs.existsSync(path.join(base, "evil"))).toBe(false);
    expect(fs.existsSync(path.join(path.dirname(base), "evil"))).toBe(false);
  });

  it("resolvePayloadPath raises on a broken parent chain", async () => {
    const base = tmp();
    const { session } = makeSession(base, "s1");
    session.db.upsertNode(1, 99, NodeKind.FILE, "orphan.txt");
    expect(() => resolvePayloadPath(base, "s1", session.db, 1)).toThrow(/broken parent chain/);
  });

  it("resolvePayloadPath resolves a normal nested path", async () => {
    const base = tmp();
    const { session } = makeSession(base, "s1");
    session.db.upsertNode(1, 0, NodeKind.DIR, "src");
    session.db.upsertNode(2, 1, NodeKind.FILE, "app.ts");
    const p = resolvePayloadPath(base, "s1", session.db, 2);
    const payloadRoot = path.join(stagingDir(base, "s1"), "payload");
    expect(p.startsWith(payloadRoot + path.sep)).toBe(true);
    expect(path.basename(p)).toBe("app.ts");
  });
});

// ---------------------------------------------------------------------------
// Type collisions
// ---------------------------------------------------------------------------

describe("type collisions", () => {
  it("dir-on-file collision rejects the node, not the session", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    // File `foo` written to disk first
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "foo", 3), "leg1");
    await session.processFrame(fileOpenFrame(1, 3), "leg1");
    await session.processFrame(fileChunkFrame(1, 0, utf8("abc")), "leg1");
    await session.processFrame(fileCloseFrame(1, 3), "leg1");
    // Then a DIR node also named `foo` (mkdir over a file)
    await session.processFrame(nodeFrame(2, 0, NodeKind.DIR, "foo"), "leg1");
    const rejected = ws.ofType("REJECT_FILE").map((m) => m.node_id);
    expect(rejected).toContain(2);
    expect(session.state).toBe(SessionState.ACTIVE);
  });

  it("file-on-dir collision rejects the node, not the session", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.DIR, "bar"), "leg1");
    await session.processFrame(nodeFrame(2, 0, NodeKind.FILE, "bar", 3), "leg1");
    await session.processFrame(fileOpenFrame(2, 3), "leg1");
    const rejected = ws.ofType("REJECT_FILE").map((m) => m.node_id);
    expect(rejected).toContain(2);
    expect(session.state).toBe(SessionState.ACTIVE);
  });
});

// ---------------------------------------------------------------------------
// Storage failure and commit termination
// ---------------------------------------------------------------------------

describe("storage failure and commit termination", () => {
  it("ENOSPC aborts the session with storage_full", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "big.bin", 10), "leg1");
    await session.processFrame(fileOpenFrame(1, 10), "leg1");
    const writer = session.writers.get(1)!;
    writer.write = () =>
      Promise.reject(Object.assign(new Error("no space left on device"), { code: "ENOSPC" }));
    await session.processFrame(fileChunkFrame(1, 0, utf8("0123456789")), "leg1");
    expect(session.state).toBe(SessionState.ABORTED);
    const abort = ws.last("SESSION_ABORT");
    expect(abort?.code).toBe("storage_full");
  });

  it("commit retries are capped by consecutive no-progress rounds", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    // A file that can never complete: final_size=100, accepted_offset=0
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "stuck.bin", 100), "leg1");
    await session.processFrame(fileOpenFrame(1, 100), "leg1");
    await session.processFrame(fileCloseFrame(1, 100), "leg1");
    await session.processFrame(sessionEndFrame(1, 0), "leg1");

    for (let i = 0; i < MAX_COMMIT_RETRIES + 4; i++) {
      session.db.setState(SessionState.COMMITTING);
      const result = await session.tryCommit();
      expect(result).toBeNull();
      if (session.state === SessionState.FAILED) break;
    }
    expect(session.state).toBe(SessionState.FAILED);
    expect(ws.last("SESSION_ABORT")?.code).toBe("commit_failed");
  });

  it("duplicate FILE_OPEN closes the prior writer", async () => {
    const base = tmp();
    const { session } = makeSession(base, "s1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "f.txt", 3), "leg1");
    await session.processFrame(fileOpenFrame(1, 3), "leg1");
    const first = session.writers.get(1)!;
    await session.processFrame(fileOpenFrame(1, 3), "leg1");
    const second = session.writers.get(1)!;
    expect(second).not.toBe(first);
    expect(first.isClosed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Quotas and chunk policy
// ---------------------------------------------------------------------------

describe("quotas and chunk policy", () => {
  it("byte quota aborts the session", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    session.applyAuth(5, null, {});
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "f.bin", 10), "leg1");
    await session.processFrame(fileOpenFrame(1, 10), "leg1");
    await session.processFrame(fileChunkFrame(1, 0, utf8("0123456789")), "leg1");
    expect(session.state).toBe(SessionState.ABORTED);
    expect(ws.last("SESSION_ABORT")?.code).toBe("quota_exceeded");
  });

  it("file quota aborts on the file over the limit", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1");
    session.applyAuth(null, 2, {});
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "a.txt", 1), "leg1");
    await session.processFrame(nodeFrame(2, 0, NodeKind.FILE, "b.txt", 1), "leg1");
    expect(session.state).toBe(SessionState.ACTIVE);
    await session.processFrame(nodeFrame(3, 0, NodeKind.FILE, "c.txt", 1), "leg1");
    expect(session.state).toBe(SessionState.ABORTED);
    expect(ws.last("SESSION_ABORT")?.code).toBe("quota_exceeded");
  });

  it("a replayed NODE does not double-count the file quota", async () => {
    const base = tmp();
    const { session } = makeSession(base, "s1");
    session.applyAuth(null, 1, {});
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "a.txt", 1), "leg1");
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "a.txt", 1), "leg1");
    expect(session.state).toBe(SessionState.ACTIVE);
    expect(session.filesSeen).toBe(1);
  });

  it("an oversized chunk is NACKed with server_policy, session stays alive", async () => {
    const base = tmp();
    const { session, ws } = makeSession(base, "s1", { maxChunkBytes: 8 });
    await session.processFrame(nodeFrame(1, 0, NodeKind.FILE, "f.bin", 16), "leg1");
    await session.processFrame(fileOpenFrame(1, 16), "leg1");
    await session.processFrame(fileChunkFrame(1, 0, new Uint8Array(16)), "leg1");
    const nack = ws.last("NACK_CHUNK");
    expect(nack?.reason).toBe("server_policy");
    expect(session.state).toBe(SessionState.ACTIVE);
  });
});

// ---------------------------------------------------------------------------
// Per-session base directories and recovery
// ---------------------------------------------------------------------------

describe("per-session base dirs and recovery", () => {
  it("stages inside the per-session base and publishes there", async () => {
    const globalBase = tmp();
    const home = path.join(tmp(), "home-alice");
    const registry = new SessionRegistry(globalBase);
    const expires = new Date(Date.now() + 3600_000).toISOString();
    const session = registry.create("sid1", "tok", "leg1", expires, "incoming", home);
    session.ws = new FakeWS();

    await uploadTree(session, [{ nodeId: 1, name: "hello.txt", content: utf8("hello world") }]);

    // Staging lives in the home, nothing under the global base
    expect(fs.existsSync(path.join(home, ".incoming.sid1", "payload"))).toBe(true);
    expect(fs.readdirSync(globalBase)).toHaveLength(0);

    const published = await publishSession(home, "sid1", path.join(home, "incoming"));
    expect(published).toEqual(["hello.txt"]);
    expect(fs.readFileSync(path.join(home, "incoming", "hello.txt"), "utf-8")).toBe("hello world");
    expect(fs.existsSync(path.join(home, ".incoming.sid1"))).toBe(false);
  });

  it("recoverSession derives the base dir from the staging parent", async () => {
    const home = tmp();
    const r1 = new SessionRegistry(home);
    const expires = new Date(Date.now() + 3600_000).toISOString();
    const s1 = r1.create("sid2", "tok2", "leg1", expires, ".");
    s1.ws = new FakeWS();
    await s1.processFrame(nodeFrame(1, 0, NodeKind.FILE, "a.txt", 1), "leg1");
    s1.detachLeg();
    // Simulate a restart: fresh registry recovering from the staging path
    r1.remove("sid2");
    const r2 = new SessionRegistry(home);
    const recovered = r2.recoverSession("sid2", path.join(home, ".incoming.sid2"));
    expect(recovered).not.toBeNull();
    expect(recovered!.baseDir).toBe(home);
    const resumed = r2.resume("sid2", "tok2", "leg2");
    expect(resumed.state).toBe(SessionState.ACTIVE);
  });

  it("auth constraints and client meta persist across recovery", async () => {
    const home = tmp();
    const r1 = new SessionRegistry(home);
    const expires = new Date(Date.now() + 3600_000).toISOString();
    const meta = JSON.stringify({ scope: "avatars", album: 7 });
    const s1 = r1.create("sid3", "tok3", "leg1", expires, ".", null, meta);
    s1.applyAuth(12345, 10, { user_id: "alice" });
    s1.detachLeg();
    r1.remove("sid3");

    const r2 = new SessionRegistry(home);
    const s2 = r2.recoverSession("sid3", path.join(home, ".incoming.sid3"))!;
    expect(s2.clientMeta).toEqual({ scope: "avatars", album: 7 });
    expect(s2.quotaMaxBytes).toBe(12345);
    expect(s2.quotaMaxFiles).toBe(10);
    expect(s2.authContext).toEqual({ user_id: "alice" });
  });
});

// ---------------------------------------------------------------------------
// Mapped publish
// ---------------------------------------------------------------------------

async function stageThreeFiles(base: string): Promise<void> {
  const db = openSessionDb(base, "sidm");
  db.initSession("sidm", "tok", new Date(Date.now() + 3600_000).toISOString(), ".");
  db.close();
  const payload = path.join(stagingDir(base, "sidm"), "payload");
  fs.mkdirSync(path.join(payload, "shots"), { recursive: true });
  fs.writeFileSync(path.join(payload, "shots", "a.jpg"), "aaa");
  fs.writeFileSync(path.join(payload, "shots", "b.pdf"), "bbb");
  fs.writeFileSync(path.join(payload, "c.jpg"), "ccc");
}

describe("mapped publish", () => {
  it("publishes per-file layout decided by the mapping", async () => {
    const base = tmp();
    await stageThreeFiles(base);

    const files = await listPayloadFiles(base, "sidm");
    expect(files.map(([p]) => p)).toEqual(["c.jpg", "shots/a.jpg", "shots/b.pdf"]);

    const target = path.join(base, "out");
    const published = await publishSessionMapped(base, "sidm", target, {
      "shots/a.jpg": "media/avatars/a.jpg",
      "shots/b.pdf": "docs/b.pdf",
      "c.jpg": "media/avatars/c.jpg",
    });
    expect(published.sort()).toEqual(["docs/b.pdf", "media/avatars/a.jpg", "media/avatars/c.jpg"]);
    expect(fs.readFileSync(path.join(target, "media", "avatars", "a.jpg"), "utf-8")).toBe("aaa");
    expect(fs.readFileSync(path.join(target, "docs", "b.pdf"), "utf-8")).toBe("bbb");
    expect(fs.existsSync(path.join(target, "shots"))).toBe(false); // client layout not replicated
    expect(fs.existsSync(stagingDir(base, "sidm"))).toBe(false); // staging reclaimed
  });

  it("rejects escapes and collisions without moving anything", async () => {
    const base = tmp();
    await stageThreeFiles(base);
    const target = path.join(base, "out");

    await expect(
      publishSessionMapped(base, "sidm", target, { "shots/a.jpg": "../evil" }),
    ).rejects.toThrow(MappingError);

    await expect(
      publishSessionMapped(base, "sidm", target, {
        "shots/a.jpg": "same.bin",
        "shots/b.pdf": "same.bin",
      }),
    ).rejects.toThrow(MappingError);

    // Mapping validation is all-or-nothing: payload intact, target untouched
    expect(fs.existsSync(path.join(stagingDir(base, "sidm"), "payload", "shots", "a.jpg"))).toBe(true);
    expect(!fs.existsSync(target) || fs.readdirSync(target).length === 0).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// onCommitted hook and programmatic publish
// ---------------------------------------------------------------------------

function makeEngine(
  base: string,
  onCommitted: ((ev: CommitEvent) => Promise<string | null>) | null,
): MfupEngine {
  const engine = new MfupEngine(
    resolveOptions({
      baseDir: base,
      onCommitted: onCommitted ?? undefined,
      authorize: async () => ({}), // silence the allow-all warning
    }),
  );
  // Wire registry + store by hand, deliberately skipping startup() —
  // mirroring make_engine in the Python tests (no sweeper, no recovery).
  engine.registry = new SessionRegistry(base);
  engine.store = new MemoryStore();
  return engine;
}

describe("onCommitted hook and programmatic publish", () => {
  it("delivers the full CommitEvent; null verdict = notification only", async () => {
    const base = tmp();
    const events: CommitEvent[] = [];
    const engine = makeEngine(base, async (ev) => {
      events.push(ev);
      return null;
    });
    const registry = engine.requireRegistry();
    const expires = new Date(Date.now() + 3600_000).toISOString();
    const session = registry.create(
      "oc1",
      "tok",
      "leg1",
      expires,
      "out",
      null,
      JSON.stringify({ scope: "docs" }),
    );
    session.applyAuth(null, null, { user_id: "alice" });
    session.ws = new FakeWS();
    await sendTree(session, [{ nodeId: 1, name: "a.txt", content: utf8("hello") }]);
    const result = await engine.tryCommit(session);
    expect(result).not.toBeNull();

    expect(events).toHaveLength(1);
    const ev = events[0];
    expect(ev.sessionId).toBe("oc1");
    expect(ev.targetDir).toBe("out");
    expect(ev.files).toBe(1);
    expect(ev.bytes).toBe(5);
    expect(ev.meta).toEqual({ scope: "docs" });
    expect(ev.context).toEqual({ user_id: "alice" });
    expect(fs.existsSync(ev.stagingDir)).toBe(true);
    // Notification only: session stays, target never created
    expect(registry.get("oc1")).not.toBeNull();
    expect(fs.existsSync(path.join(base, "out"))).toBe(false);
  });

  it('"publish" verdict publishes server-side and removes the session', async () => {
    const base = tmp();
    const engine = makeEngine(base, async () => "publish");
    const registry = engine.requireRegistry();
    const store = engine.requireStore();
    const expires = new Date(Date.now() + 3600_000);
    const session = registry.create("oc2", "tok", "leg1", expires.toISOString(), "out");
    await store.register("oc2", expires, "out", stagingDir(base, "oc2"));
    session.ws = new FakeWS();
    await sendTree(session, [{ nodeId: 1, name: "b.txt", content: utf8("bytes") }]);
    await engine.tryCommit(session);

    expect(fs.readFileSync(path.join(base, "out", "b.txt"), "utf-8")).toBe("bytes");
    expect(registry.get("oc2")).toBeNull();
    expect(await store.isRegistered("oc2")).toBe(false);
    expect(fs.existsSync(stagingDir(base, "oc2"))).toBe(false);
  });

  it("a throwing hook cannot damage a completed commit", async () => {
    const base = tmp();
    const engine = makeEngine(base, async () => {
      throw new Error("consumer bug");
    });
    const registry = engine.requireRegistry();
    const expires = new Date(Date.now() + 3600_000).toISOString();
    const session = registry.create("oc3", "tok", "leg1", expires, "out");
    const ws = new FakeWS();
    session.ws = ws;
    await sendTree(session, [{ nodeId: 1, name: "c.txt", content: utf8("x") }]);
    const result = await engine.tryCommit(session);

    expect(result).not.toBeNull();
    expect(ws.ofType("COMMIT_OK")).toHaveLength(1);
    expect(session.state).toBe(SessionState.COMMITTED);
    expect(registry.get("oc3")).not.toBeNull();
    expect(fs.existsSync(stagingDir(base, "oc3"))).toBe(true);
  });

  it("engine.publish raises typed errors and publishes after commit", async () => {
    const base = tmp();
    const engine = makeEngine(base, null);
    const registry = engine.requireRegistry();

    await expect(engine.publish("nope")).rejects.toThrow(SessionNotFound);

    const expires = new Date(Date.now() + 3600_000).toISOString();
    const session = registry.create("oc4", "tok", "leg1", expires, "out");
    session.ws = new FakeWS();
    await expect(engine.publish("oc4")).rejects.toThrow(NotCommitted);

    await uploadTree(session, [{ nodeId: 1, name: "d.txt", content: utf8("data") }]);
    const published = await engine.publish("oc4");
    expect(published).toEqual(["d.txt"]);
    expect(fs.readFileSync(path.join(base, "out", "d.txt"), "utf-8")).toBe("data");
    expect(registry.get("oc4")).toBeNull();
  });
});
