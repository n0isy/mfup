import { afterEach, beforeEach, describe, it, expect } from "vitest";
import { createServer, request as httpRequest, type Server } from "node:http";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import {
  createMfup,
  Engine,
  relativePath,
  type MfupServer,
} from "../../packages/server/src/index.js";
import { MfupSession } from "../../packages/client/src/index.js";

let app: MfupServer, server: Server, base: string, url: string;
beforeEach(async () => {
  base = await fs.mkdtemp(path.join(os.tmpdir(), "mfup3-"));
  app = createMfup({
    baseDir: base,
    authorize: () => ({}),
    sweepIntervalMs: 0,
    limits: { partBytes: 1024, batchBytes: 8192, maxParts: 16 },
  });
  server = createServer(async (req, res) => {
    if (!(await app.handle(req, res))) {
      res.writeHead(404);
      res.end();
    }
  });
  app.attach(server);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  url = `http://127.0.0.1:${(server.address() as any).port}`;
});
afterEach(async () => {
  await app.close();
  server.closeAllConnections();
  await new Promise<void>((r) => server.close(() => r()));
  await fs.rm(base, { recursive: true, force: true });
});
async function ticket() {
  return (
    await fetch(url + "/mfup/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ protocol: "MFUP/3", targetDir: "test" }),
    })
  ).json();
}
async function call(t: any, suffix: string, data?: unknown) {
  const r = await fetch(`${url}/mfup/sessions/${t.id}${suffix}`, {
    method: data === undefined ? "GET" : "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "Content-Type": "application/json",
    },
    body: data === undefined ? undefined : JSON.stringify(data),
  });
  return { status: r.status, data: await r.json() };
}
async function send(
  t: any,
  files: [string, string, number?, number?][],
  id = crypto.randomUUID(),
) {
  const form = new FormData();
  form.append(
    "manifest",
    JSON.stringify({
      files: files.map(([name, content, size, offset]) => [
        name,
        size ?? content.length,
        1,
        offset ?? 0,
        content.length,
      ]),
      dirs: [],
    }),
  );
  files.forEach(([, content], i) =>
    form.append(String(i), new Blob([content]), "file"),
  );
  const response = await fetch(`${url}/mfup/sessions/${t.id}/batches/${id}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "X-MFUP-Epoch": String(t.epoch),
    },
    body: form,
  });
  return { status: response.status, data: await response.json(), id };
}
describe("HTTP batches and lifecycle", () => {
  it("replaces unconfirmed bytes after a connection breaks inside a part", async () => {
    const t = await ticket();
    const manifest = JSON.stringify({
      files: [["partial", 2048, 1, 0, 1024]],
      dirs: [],
    });
    const req = httpRequest(`${url}/mfup/sessions/${t.id}/batches/broken`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${t.token}`,
        "X-MFUP-Epoch": "1",
        "Content-Type": "multipart/form-data; boundary=partial",
      },
    });
    req.on("error", () => {});
    req.write(
      `--partial\r\nContent-Disposition: form-data; name="manifest"\r\n\r\n${manifest}\r\n--partial\r\nContent-Disposition: form-data; name="0"; filename="partial"\r\nContent-Type: application/octet-stream\r\n\r\n` +
        "A".repeat(500),
    );
    await expect
      .poll(async () => {
        try {
          return (await fs.stat(app.engine.payload(t.id, "partial"))).size;
        } catch {
          return 0;
        }
      })
      .toBeGreaterThan(0);
    req.destroy();
    const resumed = await app.engine.resume(t.id);
    t.epoch = resumed.epoch;
    expect(app.engine.receipt(t.id, "broken")).toBeNull();
    expect(app.engine.resumePage(t.id).files[0].offsets).toEqual([]);
    expect(
      (await send(t, [["partial", "B".repeat(1024), 2048, 0]])).status,
    ).toBe(200);
    expect(
      (await send(t, [["partial", "C".repeat(1024), 2048, 1024]])).status,
    ).toBe(200);
    await call(t, "/commit", { files: 1, dirs: 0, bytes: 2048 });
    expect((await call(t, "/publish", {})).status).toBe(200);
    expect(
      await fs.readFile(path.join(base, "published/test/partial"), "utf8"),
    ).toBe("B".repeat(1024) + "C".repeat(1024));
  });
  it("accepts exactly maxParts files including the manifest field", async () => {
    const t = await ticket();
    expect(
      (
        await send(
          t,
          Array.from({ length: 16 }, (_, i) => [`f${i}`, "x"]),
        )
      ).status,
    ).toBe(200);
  });
  it("rejects incompatible spelling of an implicit directory", async () => {
    const t = await ticket();
    expect(
      (
        await send(t, [
          ["Folder/a", "a"],
          ["folder/b", "b"],
        ])
      ).data.error,
    ).toBe("path_conflict");
  });
  it.each(["overwrite", "cancel"])(
    "handles a file at the parent directory using %s",
    async (choice) => {
      await fs.mkdir(path.join(base, "published/test"), { recursive: true });
      await fs.writeFile(path.join(base, "published/test/folder"), "old");
      const t = await ticket();
      expect((await send(t, [["folder/child", "new"]])).status).toBe(200);
      const q = (await call(t, "")).data.asks[0];
      expect(q.id).toBe("overwrite");
      await call(t, "/answers", { id: q.id, choice });
      if (choice === "cancel") {
        expect((await call(t, "")).data.state).toBe("cancelled");
        expect(
          await fs.readFile(path.join(base, "published/test/folder"), "utf8"),
        ).toBe("old");
        return;
      }
      await call(t, "/commit", { files: 1, dirs: 0, bytes: 3 });
      const result = await call(t, "/publish", {});
      expect(result.status).toBe(200);
      expect(
        await fs.readFile(
          path.join(base, "published/test", "folder/child"),
          "utf8",
        ),
      ).toBe("new");
    },
  );
  it("serializes concurrent publication to the same target", async () => {
    const a = await ticket(),
      b = await ticket();
    await send(a, [["same", "aaa"]]);
    await send(b, [["same", "bbb"]]);
    await call(a, "/commit", { files: 1, dirs: 0, bytes: 3 });
    await call(b, "/commit", { files: 1, dirs: 0, bytes: 3 });
    const results = await Promise.all([
      call(a, "/publish", {}),
      call(b, "/publish", {}),
    ]);
    expect(results.map((r) => r.status).sort()).toEqual([200, 409]);
  });
  it("rejects missing and malformed manifests without creating a receipt", async () => {
    const t = await ticket();
    const form = new FormData();
    form.append("0", new Blob(["a"]), "a");
    const response = await fetch(
      `${url}/mfup/sessions/${t.id}/batches/no-manifest`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${t.token}`, "X-MFUP-Epoch": "1" },
        body: form,
      },
    );
    expect(response.status).toBe(400);
    expect((await call(t, "/batches/no-manifest")).status).toBe(404);
  });
  it("stores multiple files in a request and publishes idempotently", async () => {
    const t = await ticket();
    const sent = await send(t, [
      ["a.txt", "abc"],
      ["folder/b.txt", "def"],
    ]);
    expect(sent.status).toBe(200);
    expect(sent.data.parts).toBe(2);
    expect(
      (await call(t, "/commit", { files: 2, dirs: 0, bytes: 6 })).status,
    ).toBe(200);
    expect((await call(t, "/publish", {})).data.published).toEqual([
      "a.txt",
      "folder/b.txt",
    ]);
    expect((await call(t, "/publish", {})).status).toBe(200);
    expect(
      await fs.readFile(path.join(base, "published/test/folder/b.txt"), "utf8"),
    ).toBe("def");
  });
  it("returns a receipt for a lost response without receiving data again", async () => {
    const t = await ticket(),
      sent = await send(t, [["a", "abc"]]);
    expect((await call(t, "/batches/" + sent.id)).data).toEqual(sent.data);
    const duplicate = await send(t, [["a", "xyz"]], sent.id);
    expect(duplicate.status).toBe(200);
    expect(await fs.readFile(app.engine.payload(t.id, "a"), "utf8")).toBe(
      "abc",
    );
  });
  it("rejects a different manifest using the same batch id", async () => {
    const t = await ticket(),
      sent = await send(t, [["a", "abc"]]);
    expect((await send(t, [["b", "abc"]], sent.id)).status).toBe(409);
  });
  it("rejects bytes not declared by the manifest and does not acknowledge them", async () => {
    const t = await ticket(),
      form = new FormData();
    form.append(
      "manifest",
      JSON.stringify({ files: [["a", 3, 1, 0, 3]], dirs: [] }),
    );
    form.append("0", new Blob(["longer"]), "a");
    const response = await fetch(`${url}/mfup/sessions/${t.id}/batches/x`, {
      method: "POST",
      headers: { Authorization: `Bearer ${t.token}`, "X-MFUP-Epoch": "1" },
      body: form,
    });
    expect(response.status).toBe(400);
    expect((await call(t, "/files")).data.files[0].offsets).toEqual([]);
    expect((await send(t, [["a", "abc"]])).status).toBe(200);
  });
  it("does not permit commit before all ranges arrive", async () => {
    const t = await ticket();
    await send(t, [["a", "x".repeat(1024), 2048]]);
    expect(
      (await call(t, "/commit", { files: 1, dirs: 0, bytes: 2048 })).data.error,
    ).toBe("incomplete");
  });
  it("rejects old epochs after resume and paginates confirmed offsets", async () => {
    const t = await ticket();
    await send(t, [
      ["a", "abc"],
      ["b", "def"],
    ]);
    const resumed = await call(t, "/resume", {});
    expect(resumed.data.epoch).toBe(2);
    expect((await send(t, [["c", "new"]])).data.error).toBe("stale_epoch");
    const page = (await call(t, "/files?limit=1")).data;
    expect(page.files[0].offsets).toEqual([0]);
    expect(page.next).toBe("a");
  });
  it("keeps questions separate from ongoing uploads", async () => {
    await fs.mkdir(path.join(base, "published/test"), { recursive: true });
    await fs.writeFile(path.join(base, "published/test/a"), "old");
    const t = await ticket();
    await send(t, [["a", "new"]]);
    const q = (await call(t, "")).data.asks[0];
    expect(q.answer).toBeNull();
    expect((await send(t, [["b", "other"]])).status).toBe(200);
    await call(t, "/commit", { files: 2, dirs: 0, bytes: 8 });
    expect((await call(t, "/publish", {})).data.error).toBe("answers_required");
    await call(t, "/properties", { overwrite: true });
    await call(t, "/publish", {});
    expect(await fs.readFile(path.join(base, "published/test/a"), "utf8")).toBe(
      "new",
    );
    expect(await fs.readFile(path.join(base, "published/test/b"), "utf8")).toBe(
      "other",
    );
  });
  it("rejects another session token", async () => {
    const a = await ticket(),
      b = await ticket();
    expect((await call({ ...a, token: b.token }, "")).status).toBe(403);
  });
  it("retains cancel state and accepts no more payload", async () => {
    const t = await ticket();
    await send(t, [["a", "abc"]]);
    await call(t, "/cancel", {});
    expect((await call(t, "")).data.state).toBe("cancelled");
    expect((await send(t, [["b", "x"]])).status).toBe(409);
  });
});
describe("browser SDK using native FormData", () => {
  it("groups files, never exceeds six requests, resumes selected files", async () => {
    const files = Array.from(
      { length: 80 },
      (_, i) => new File(["data"], `f${i}`, { lastModified: 1 }),
    );
    const s = new MfupSession({
      serverUrl: url,
      targetDir: "test",
      batchDelayMs: 1,
    });
    await s.upload(files);
    expect(s.getSnapshot().maxActiveRequests).toBeLessThanOrEqual(6);
    expect(s.getSnapshot().batches).toBeLessThan(80);
    expect(s.getSnapshot().confirmedBytes).toBe(320);
    s.dispose();
    const resumed = new MfupSession({
      serverUrl: url,
      ticket: s.exportTicket(),
    });
    await resumed.upload(files);
    expect(resumed.getSnapshot().state).toBe("published");
    resumed.dispose();
  });
  it("sends independent ranges of a large file and preserves exact bytes", async () => {
    const bytes = new Uint8Array(16001).map((_, i) => i % 251);
    const s = new MfupSession({ serverUrl: url, targetDir: "test" });
    await s.upload([new File([bytes], "big", { lastModified: 1 })]);
    expect(
      new Uint8Array(await fs.readFile(path.join(base, "published/test/big"))),
    ).toEqual(bytes);
    s.dispose();
  });
  it("starts uploading before directory discovery ends", async () => {
    const s = new MfupSession({
      serverUrl: url,
      targetDir: "test",
      batchDelayMs: 0,
    });
    let uploadedBeforeEnd = false;
    async function* source() {
      yield {
        kind: "file" as const,
        path: "first",
        file: new File(["abc"], "first", { lastModified: 1 }),
      };
      await new Promise((r) => setTimeout(r, 100));
      uploadedBeforeEnd = s.getSnapshot().batches > 0;
      yield { kind: "directory" as const, path: "empty" };
    }
    await s.upload(source());
    expect(uploadedBeforeEnd).toBe(true);
    expect(
      (await fs.stat(path.join(base, "published/test/empty"))).isDirectory(),
    ).toBe(true);
    s.dispose();
  });
});
describe("coordinated cleanup", () => {
  it("allows only one owner of a data directory", () => {
    expect(
      () =>
        new Engine({
          baseDir: base,
          authorize: () => ({}),
          sweepIntervalMs: 0,
        }),
    ).toThrow(/locked/);
  });
  it("keeps the range size consistent across restarts", async () => {
    await app.close();
    expect(
      () =>
        new Engine({
          baseDir: base,
          authorize: () => ({}),
          sweepIntervalMs: 0,
          limits: { partBytes: 2048 },
        }),
    ).toThrow(/part_size_in_use/);
  });
  it("recovers a publication interrupted after its first rename", async () => {
    const t = await ticket();
    await send(t, [
      ["a", "aaa"],
      ["b", "bbb"],
    ]);
    await call(t, "/commit", { files: 2, dirs: 0, bytes: 6 });
    await fs.mkdir(path.join(base, "published/test"), { recursive: true });
    await fs.rename(
      app.engine.payload(t.id, "a"),
      path.join(base, "published/test/a"),
    );
    app.engine.store.run(
      "UPDATE sessions SET state='publishing' WHERE id=?",
      t.id,
    );
    await app.close();
    app = createMfup({
      baseDir: base,
      authorize: () => ({}),
      sweepIntervalMs: 0,
      limits: { partBytes: 1024, batchBytes: 8192, maxParts: 16 },
    });
    expect((await app.engine.resume(t.id)).state).toBe("publishing");
    expect((await app.engine.publish(t.id)).published).toEqual(["a", "b"]);
    expect(await fs.readFile(path.join(base, "published/test/a"), "utf8")).toBe(
      "aaa",
    );
    expect(await fs.readFile(path.join(base, "published/test/b"), "utf8")).toBe(
      "bbb",
    );
  });
  it("runs committed hooks outside the lifecycle lock", async () => {
    app.engine.options.onCommitted = async (event) => {
      await app.engine.publish(event.sessionId);
    };
    const t = await ticket();
    await send(t, [["a", "aaa"]]);
    expect(
      (await call(t, "/commit", { files: 1, dirs: 0, bytes: 3 })).status,
    ).toBe(200);
    expect((await call(t, "")).data.state).toBe("published");
  });
  it("resume refreshes expiry before an older candidate can be removed", async () => {
    const t = await ticket();
    app.engine.store.run("UPDATE sessions SET expires=0 WHERE id=?", t.id);
    await app.engine.resume(t.id);
    expect(await app.engine.sweep()).toBe(0);
    expect(app.engine.row(t.id).epoch).toBe(2);
  });
  it("an active request is excluded from cleanup; cleanup wins after it ends", async () => {
    const t = await ticket();
    const batch = await app.engine.begin(t.id, 1, "pending", () => {});
    app.engine.store.run("UPDATE sessions SET expires=0 WHERE id=?", t.id);
    expect(await app.engine.sweep()).toBe(0);
    app.engine.end(batch);
    expect(await app.engine.sweep()).toBe(1);
    await expect(app.engine.resume(t.id)).rejects.toMatchObject({
      code: "not_found",
    });
  });
  it("two sweepers remove a session only once", async () => {
    const t = await ticket();
    app.engine.store.run("UPDATE sessions SET expires=0 WHERE id=?", t.id);
    expect(
      (await Promise.all([app.engine.sweep(), app.engine.sweep()])).reduce(
        (a, b) => a + b,
        0,
      ),
    ).toBe(1);
  });
});
it.each(["../a", "/a", "a\\b", "CON", "a/..", "x.", "a:b", "a\0b"])(
  "portable paths reject %j",
  (value) => expect(() => relativePath(value)).toThrow(),
);
