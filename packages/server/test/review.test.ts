import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { LiveSession, MappingError, NodeKind, publishSession, publishSessionMapped, stagingDir } from "../src/index.js";
import { validateSessionId } from "../src/storage.js";
import { FrameReader, IncompleteFrameError } from "../src/protocol.js";
import { fileChunkFrame, fileCloseFrame, fileOpenFrame, makeSession, mkTmpDir, nodeFrame, rmrf, sessionEndFrame, utf8 } from "./helpers.js";

const dirs: string[] = [];
const sessions: LiveSession[] = [];
function tmp() { const dir = mkTmpDir(); dirs.push(dir); return dir; }
function fixture() {
  const made = makeSession(tmp(), "review");
  sessions.push(made.session);
  return made;
}
afterEach(() => {
  for (const s of sessions.splice(0)) { s.cancelIdleTimer(); s.closeAllWriters(); s.db.close(); }
  for (const dir of dirs.splice(0)) rmrf(dir);
});

describe("session paths", () => {
  it.each(["", "x/../../other", "a/b", "a\\b", ".", "..", "a\0b", "a".repeat(129), null, 1])("rejects invalid id %j before staging", sid => {
    const base = tmp();
    expect(() => stagingDir(base, sid as string)).toThrow(/session_id/);
    expect(fs.readdirSync(base)).toEqual([]);
  });
  it.each(["123", "a".repeat(128), "upload_01.test-2", "01234567-89ab-cdef-0123-456789abcdef"])("accepts id %s", sid => {
    expect(() => validateSessionId(sid)).not.toThrow();
  });
});

describe("file ownership without another index", () => {
  it("rejects another node at the same path and commits only the original", async () => {
    const { session: s, ws, base } = fixture();
    for (const [id, content] of [[1, "AAA"], [2, "BBB"]] as const) {
      await s.processFrame(nodeFrame(id, 0, NodeKind.FILE, "same.txt", 3), "leg1");
      await s.processFrame(fileOpenFrame(id, 3), "leg1");
      await s.processFrame(fileChunkFrame(id, 0, utf8(content)), "leg1");
      await s.processFrame(fileCloseFrame(id, 3), "leg1");
    }
    // A repeated NODE must not restore a rejected node to the commit count.
    await s.processFrame(nodeFrame(2, 0, NodeKind.FILE, "same.txt", 3), "leg1");
    await s.processFrame(sessionEndFrame(2, 6), "leg1");
    expect(await s.tryCommit()).toMatchObject({ files: 1, bytes: 3 });
    expect(ws.last("REJECT_FILE")?.node_id).toBe(2);
    expect(fs.readFileSync(path.join(stagingDir(base, "review"), "payload/same.txt"), "utf8")).toBe("AAA");
  });

  it("detects the same file path through different directory ids", async () => {
    const { session: s } = fixture();
    for (const id of [1, 2]) await s.processFrame(nodeFrame(id, 0, NodeKind.DIR, "folder"), "leg1");
    for (const [id, parent] of [[3, 1], [4, 2]]) {
      await s.processFrame(nodeFrame(id, parent, NodeKind.FILE, "same.txt", 0), "leg1");
      await s.processFrame(fileOpenFrame(id, 0), "leg1");
    }
    expect(s.writers.has(3)).toBe(true);
    expect(s.writers.has(4)).toBe(false);
    expect(s.db.isRejected(4)).toBe(true);
  });

  it("reopens its own zero-offset file on another leg", async () => {
    const { session: s } = fixture();
    await s.processFrame(nodeFrame(1, 0, NodeKind.FILE, "empty", 0), "leg1");
    await s.processFrame(fileOpenFrame(1, 0), "leg1");
    s.detachLeg(); s.attachLeg("leg2");
    await s.processFrame(fileOpenFrame(1, 0), "leg2");
    await s.processFrame(fileCloseFrame(1, 0), "leg2");
    await s.processFrame(sessionEndFrame(1, 0), "leg2");
    expect(await s.tryCommit()).toMatchObject({ files: 1, bytes: 0 });
  });

  it("recovers a zero-offset file left before its path was journaled", async () => {
    const { session: s, base } = fixture();
    await s.processFrame(nodeFrame(1, 0, NodeKind.FILE, "leftover", 3), "leg1");
    fs.writeFileSync(path.join(stagingDir(base, "review"), "payload/leftover"), "unconfirmed");
    await s.processFrame(fileOpenFrame(1, 3), "leg1");
    await s.processFrame(fileChunkFrame(1, 0, utf8("new")), "leg1");
    await s.processFrame(fileCloseFrame(1, 3), "leg1");
    expect(s.db.isRejected(1)).toBe(false);
    expect(fs.readFileSync(path.join(stagingDir(base, "review"), "payload/leftover"), "utf8")).toBe("new");
  });

  it("does not let an existing node change the path of its descendants", async () => {
    const { session: s } = fixture();
    await s.processFrame(nodeFrame(1, 0, NodeKind.DIR, "original"), "leg1");
    await expect(s.processFrame(nodeFrame(1, 0, NodeKind.DIR, "different"), "leg1")).rejects.toThrow(/cannot change/);
    expect(s.db.getNode(1)?.name).toBe("original");
  });
});

describe("publication", () => {
  function tree() {
    const base = tmp(), payload = path.join(stagingDir(base, "s"), "payload"), target = path.join(base, "target");
    fs.mkdirSync(payload, { recursive: true }); fs.mkdirSync(target);
    return { base, payload, target };
  }

  it.each([false, true])("rejects file/parent mappings before moving anything (reversed=%s)", async reversed => {
    const { base, payload, target } = tree();
    fs.writeFileSync(path.join(payload, "a"), "A"); fs.writeFileSync(path.join(payload, "b"), "B");
    const mapping = reversed ? { a: "item/child", b: "item" } : { a: "item", b: "item/child" };
    await expect(publishSessionMapped(base, "s", target, mapping)).rejects.toBeInstanceOf(MappingError);
    expect(fs.readdirSync(target)).toEqual([]);
    expect(fs.readdirSync(payload).sort()).toEqual(["a", "b"]);
  });

  it("merges an existing directory without overwriting its distinct files", async () => {
    const { base, payload, target } = tree();
    for (const dir of [payload, target]) fs.mkdirSync(path.join(dir, "folder"));
    fs.writeFileSync(path.join(payload, "folder/new.txt"), "new");
    fs.writeFileSync(path.join(target, "folder/old.txt"), "old");
    await publishSession(base, "s", target);
    expect(fs.readFileSync(path.join(target, "folder/old.txt"), "utf8")).toBe("old");
    expect(fs.readFileSync(path.join(target, "folder/new.txt"), "utf8")).toBe("new");
  });

  it("replaces a directory with a file only after overwrite was selected", async () => {
    const { base, payload, target } = tree();
    fs.mkdirSync(path.join(target, "item")); fs.writeFileSync(path.join(target, "item/old"), "old");
    fs.writeFileSync(path.join(payload, "item"), "new");
    await expect(publishSession(base, "s", target)).rejects.toThrow(/conflicting/);
    expect(fs.readFileSync(path.join(target, "item/old"), "utf8")).toBe("old");
    await publishSession(base, "s", target, ".incoming", "merge_overwrite");
    expect(fs.readFileSync(path.join(target, "item"), "utf8")).toBe("new");
  });
});

it("checks EOF without consuming the incomplete tail", () => {
  const r = new FrameReader();
  r.feed(Buffer.from([0, 0, 0, 5, 6, 0]));
  expect(r.drain()).toEqual([]);
  expect(() => r.finish()).toThrow(IncompleteFrameError);
  r.feed(Buffer.from([0, 0, 1]));
  expect(r.drain()).toMatchObject([{ nodeId: 1 }]);
  expect(() => r.finish()).not.toThrow();
});
