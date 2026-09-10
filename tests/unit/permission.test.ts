import { beforeEach, afterEach, expect, it } from "vitest";
import { createServer, request as requestHTTP, type Server } from "node:http";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { Writable } from "node:stream";
import { syncBuiltinESMExports } from "node:module";
import {
  createMfup,
  type MfupServer,
} from "../../packages/server/src/index.js";
import { MfupSession } from "../../packages/client/src/index.js";
let app: MfupServer, server: Server, base: string, url: string;
beforeEach(async () => {
  base = await fsp.mkdtemp(path.join(os.tmpdir(), "mfup-permission-"));
  app = createMfup({
    baseDir: base,
    authorize: () => ({}),
    sweepIntervalMs: 0,
    onError: () => {},
    maxMetaBytes: 65536,
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
  await fsp.rm(base, { recursive: true, force: true });
});
async function ticket(overwrite = false) {
  return (
    await fetch(url + "/mfup/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        protocol: "MFUP/3",
        targetDir: "test",
        overwrite,
      }),
    })
  ).json();
}
async function call(t: any, action = "", data?: unknown) {
  const r = await fetch(`${url}/mfup/sessions/${t.id}${action}`, {
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
  files: [string, string][],
  bid = crypto.randomUUID(),
) {
  const form = new FormData();
  form.append(
    "manifest",
    JSON.stringify({
      files: files.map(([name, body]) => [
        name,
        body.length,
        1,
        0,
        body.length,
      ]),
      dirs: [],
    }),
  );
  files.forEach(([name, body], i) =>
    form.append(String(i), new Blob([body]), name),
  );
  const r = await fetch(`${url}/mfup/sessions/${t.id}/batches/${bid}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "X-MFUP-Epoch": String(t.epoch),
    },
    body: form,
  });
  return { status: r.status, data: await r.json(), bid };
}
async function seed() {
  await fsp.mkdir(path.join(base, "published/test"), { recursive: true });
  for (const name of ["a", "later"])
    await fsp.writeFile(path.join(base, "published/test", name), "old");
}
it.each(["during", "after"])(
  "one approval covers the whole session: %s upload",
  async (phase) => {
    await seed();
    const t = await ticket();
    await send(t, [["a", "new"]]);
    expect((await call(t)).data).toMatchObject({
      overwrite: false,
      overwriteRequired: true,
      asks: [{ id: "overwrite" }],
    });
    if (phase === "during") {
      expect((await call(t, "/properties", { overwrite: true })).status).toBe(
        200,
      );
      expect(
        await fsp.readFile(path.join(base, "published/test/a"), "utf8"),
      ).toBe("old");
    }
    await send(t, [
      ["later", "new"],
      ["unique", "new"],
    ]);
    await call(t, "/commit", { files: 3, dirs: 0, bytes: 9 });
    if (phase === "after") {
      expect((await call(t, "/publish", {})).data.error).toBe(
        "answers_required",
      );
      expect(
        await fsp.readFile(path.join(base, "published/test/a"), "utf8"),
      ).toBe("old");
      await call(t, "/properties", { overwrite: true });
    }
    expect((await call(t)).data).toMatchObject({
      overwrite: true,
      overwriteRequired: false,
      asks: [],
    });
    expect((await call(t, "/properties", { overwrite: true })).status).toBe(
      200,
    );
    expect((await call(t, "/properties", { overwrite: false })).status).toBe(
      409,
    );
    const resumed = await call(t, "/resume", {});
    expect(resumed.data.overwrite).toBe(true);
    expect((await call(t, "/publish", {})).status).toBe(200);
    expect(
      await fsp.readFile(path.join(base, "published/test/later"), "utf8"),
    ).toBe("new");
  },
);
it.each(["during", "after"])(
  "cancel preserves every destination: %s upload",
  async (phase) => {
    await seed();
    const t = await ticket();
    await send(t, [
      ["a", "new"],
      ["unique", "new"],
    ]);
    if (phase === "after")
      await call(t, "/commit", { files: 2, dirs: 0, bytes: 6 });
    expect((await call(t, "/cancel", {})).data).toMatchObject({
      state: "cancelled",
      asks: [],
      overwriteRequired: false,
    });
    expect((await call(t, "/cancel", {})).status).toBe(200);
    expect((await call(t, "/properties", { overwrite: true })).status).toBe(
      409,
    );
    expect((await call(t, "/publish", {})).status).toBe(409);
    expect(
      await fsp.readFile(path.join(base, "published/test/a"), "utf8"),
    ).toBe("old");
    expect(fs.existsSync(path.join(base, "published/test/unique"))).toBe(false);
    expect(fs.existsSync(app.engine.staging(t.id))).toBe(false);
  },
);
it("accepts approval during a long POST and suppresses later conflicts", async () => {
  await seed();
  const t = await ticket();
  const manifest = JSON.stringify({
    files: [["a", 4096, 1, 0, 4096]],
    dirs: [],
  });
  const request = requestHTTP(`${url}/mfup/sessions/${t.id}/batches/long`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "X-MFUP-Epoch": "1",
      "Content-Type": "multipart/form-data; boundary=part",
    },
  });
  const response = new Promise<number>((resolve, reject) => {
    request.on("error", reject);
    request.on("response", (res) => {
      res.resume();
      res.on("end", () => resolve(res.statusCode!));
    });
  });
  request.write(
    `--part\r\nContent-Disposition: form-data; name="manifest"\r\n\r\n${manifest}\r\n--part\r\nContent-Disposition: form-data; name="0"; filename="a"\r\n\r\n` +
      "x".repeat(512),
  );
  await expect
    .poll(() => app.engine.snapshot(t.id).overwriteRequired)
    .toBe(true);
  await call(t, "/properties", { overwrite: true });
  request.end("x".repeat(3584) + "\r\n--part--\r\n");
  expect(await response).toBe(200);
  await send(t, [["later", "new"]]);
  expect(app.engine.snapshot(t.id).asks).toEqual([]);
  await call(t, "/commit", { files: 2, dirs: 0, bytes: 4099 });
  expect((await call(t, "/publish", {})).status).toBe(200);
});
it("pre-approved upload never asks and a late cancel reports published", async () => {
  await seed();
  const t = await ticket(true);
  await send(t, [["a", "new"]]);
  expect((await call(t)).data.asks).toEqual([]);
  await call(t, "/commit", { files: 1, dirs: 0, bytes: 3 });
  await call(t, "/publish", {});
  expect((await call(t, "/cancel", {})).data.state).toBe("published");
});
it("maps a real SQLite allocation failure to 507 and rolls back session creation", async () => {
  const db = app.engine.store.db;
  const count = (db.prepare("PRAGMA page_count").get() as any).page_count;
  db.exec(`PRAGMA max_page_count=${count}`);
  const r = await fetch(url + "/mfup/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ protocol: "MFUP/3", meta: "m".repeat(32768) }),
  });
  expect(r.status).toBe(507);
  expect(await r.json()).toMatchObject({
    error: "storage_full",
    retryable: false,
    phase: "create",
  });
  expect(await fsp.readdir(path.join(base, "staging"))).toEqual([]);
  db.exec("PRAGMA max_page_count=100000");
  expect((await ticket()).id).toBeTruthy();
});
it("keeps EIO distinct from malformed multipart and releases the request slot", async () => {
  const t = await ticket();
  const original = fs.createWriteStream;
  fs.createWriteStream = ((..._args: any[]) =>
    new Writable({
      write(_c, _e, done) {
        done(Object.assign(new Error("test disk failure"), { code: "EIO" }));
      },
    })) as any;
  syncBuiltinESMExports();
  let failed: any;
  try {
    failed = await send(t, [["a", "abc"]]);
  } finally {
    fs.createWriteStream = original;
    syncBuiltinESMExports();
  }
  expect(failed).toMatchObject({
    status: 503,
    data: { error: "storage_unavailable", retryable: false, phase: "upload" },
  });
  expect((await call(t, "/batches/" + failed.bid)).status).toBe(404);
  expect((await call(t)).data.error.code).toBe("storage_unavailable");
  t.epoch = (await call(t, "/resume", {})).data.epoch;
  expect((await send(t, [["a", "abc"]])).status).toBe(200);
});
it("does not announce cancellation when the server response is lost", async () => {
  const s = new MfupSession({
    serverUrl: url,
    fetch: async (input, init) => {
      if (String(input).endsWith("/cancel")) throw new TypeError("offline");
      return fetch(input, init);
    },
  });
  try {
    await s.connect();
    await expect(s.cancel()).rejects.toMatchObject({ code: "network_error" });
    expect(s.getSnapshot().state).toBe("failed");
    expect(app.engine.snapshot(s.exportTicket().id).state).toBe("uploading");
  } finally {
    s.dispose();
  }
});
it("keeps only the first storage error and does not automatically retry its body", async () => {
  let posts = 0;
  const s = new MfupSession({
    serverUrl: url,
    fetch: async (input, init) => {
      if (String(input).includes("/batches/") && init?.method === "POST") {
        posts++;
        return new Response(
          JSON.stringify({
            error: "storage_full",
            message: "storage_full",
            retryable: false,
            phase: "upload",
          }),
          { status: 507, headers: { "Content-Type": "application/json" } },
        );
      }
      return fetch(input, init);
    },
  });
  try {
    await expect(s.upload([new File(["data"], "a")])).rejects.toMatchObject({
      code: "storage_full",
    });
    expect(posts).toBe(1);
    expect(s.getSnapshot().errorInfo?.code).toBe("storage_full");
  } finally {
    s.dispose();
  }
});
it("recovers a real SQLite full error after file moves without repeating upload", async () => {
  const t = await ticket(true);
  const files: [string, string][] = Array.from({ length: 80 }, (_, i) => [
    `f${i}-` + "x".repeat(180),
    "v",
  ]);
  expect((await send(t, files)).status).toBe(200);
  await call(t, "/commit", { files: 80, dirs: 0, bytes: 80 });
  const db = app.engine.store.db;
  const pages = (db.prepare("PRAGMA page_count").get() as any).page_count;
  db.exec(`PRAGMA max_page_count=${pages}`);
  const failed = await call(t, "/publish", {});
  expect(failed.status).toBe(507);
  expect(failed.data.error).toBe("storage_full");
  expect(
    await fsp.readFile(path.join(base, "published/test", files[0][0]), "utf8"),
  ).toBe("v");
  db.exec("PRAGMA max_page_count=100000");
  const resumed = await call(t, "/resume", {});
  expect(resumed.data.state).toBe("publishing");
  expect((await call(t, "/publish", {})).data.published).toHaveLength(80);
  expect((await call(t)).data.error).toBeNull();
});
it("cancel during a committed callback prevents its later automatic publish", async () => {
  let entered!: () => void, release!: () => void;
  const ready = new Promise<void>((r) => (entered = r)),
    gate = new Promise<void>((r) => (release = r));
  app.engine.options.onCommitted = async () => {
    entered();
    await gate;
    return true;
  };
  const t = await ticket();
  const committed = call(t, "/commit", { files: 0, dirs: 0, bytes: 0 });
  await ready;
  try {
    expect((await call(t, "/cancel", {})).data.state).toBe("cancelled");
  } finally {
    release();
    await committed;
  }
  expect((await call(t)).data.state).toBe("cancelled");
});
it("approval remains effective after restarting the backend", async () => {
  await seed();
  const t = await ticket();
  await send(t, [["a", "new"]]);
  await call(t, "/properties", { overwrite: true });
  await app.close();
  app = createMfup({
    baseDir: base,
    authorize: () => ({}),
    sweepIntervalMs: 0,
  });
  t.epoch = (await call(t, "/resume", {})).data.epoch;
  await send(t, [["later", "new"]]);
  expect((await call(t)).data.asks).toEqual([]);
  await call(t, "/commit", { files: 2, dirs: 0, bytes: 6 });
  expect((await call(t, "/publish", {})).status).toBe(200);
});
it("reports a missing staged file instead of treating the old destination as published", async () => {
  await seed();
  const t = await ticket(true);
  await send(t, [["a", "new"]]);
  await call(t, "/commit", { files: 1, dirs: 0, bytes: 3 });
  await fsp.unlink(app.engine.payload(t.id, "a"));
  const result = await call(t, "/publish", {});
  expect(result.status).toBe(503);
  expect(result.data.error).toBe("storage_unavailable");
  expect((await call(t)).data.state).toBe("committed");
  expect(await fsp.readFile(path.join(base, "published/test/a"), "utf8")).toBe(
    "old",
  );
});
it("concurrent cancellation during a long POST drains it before cleanup", async () => {
  await seed();
  const t = await ticket();
  const manifest = JSON.stringify({
    files: [["a", 4096, 1, 0, 4096]],
    dirs: [],
  });
  const req = requestHTTP(`${url}/mfup/sessions/${t.id}/batches/cancel-long`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "X-MFUP-Epoch": "1",
      "Content-Type": "multipart/form-data; boundary=part",
    },
  });
  const ended = new Promise<void>((resolve) => {
    req.on("error", () => resolve());
    req.on("response", (res) => {
      res.resume();
      res.on("end", resolve);
    });
  });
  req.write(
    `--part\r\nContent-Disposition: form-data; name="manifest"\r\n\r\n${manifest}\r\n--part\r\nContent-Disposition: form-data; name="0"; filename="a"\r\n\r\n` +
      "x".repeat(512),
  );
  await expect
    .poll(() => app.engine.snapshot(t.id).overwriteRequired)
    .toBe(true);
  const responses = await Promise.all([
    call(t, "/cancel", {}),
    call(t, "/cancel", {}),
  ]);
  expect(responses.map((r) => r.data.state)).toEqual([
    "cancelled",
    "cancelled",
  ]);
  await ended;
  expect(fs.existsSync(app.engine.staging(t.id))).toBe(false);
  expect((await call(t, "/batches/cancel-long")).status).toBe(404);
  expect(await fsp.readFile(path.join(base, "published/test/a"), "utf8")).toBe(
    "old",
  );
});
it("checks the durable receipt after a network error instead of repeating the body", async () => {
  let posts = 0;
  const s = new MfupSession({
    serverUrl: url,
    fetch: async (input, init) => {
      const response = await fetch(input, init);
      if (
        String(input).includes("/batches/") &&
        init?.method === "POST" &&
        ++posts === 1
      )
        throw new TypeError("response lost");
      return response;
    },
  });
  try {
    await s.upload([new File(["retained"], "a")]);
    expect(posts).toBe(1);
    expect(s.getSnapshot().state).toBe("published");
  } finally {
    s.dispose();
  }
});
it("preserves SQLite FULL during mapping-plan rollback", async () => {
  app.engine.options.mapFile = ({ path: name }) =>
    "d".repeat(180) + "/" + "e".repeat(180) + "/" + name;
  const t = await ticket();
  const files: [string, string][] = Array.from({ length: 80 }, (_, i) => [
    `f${i}`,
    "v",
  ]);
  await send(t, files);
  await call(t, "/commit", { files: 80, dirs: 0, bytes: 80 });
  const db = app.engine.store.db;
  const pages = (db.prepare("PRAGMA page_count").get() as any).page_count;
  db.exec(`PRAGMA max_page_count=${pages}`);
  expect((await call(t, "/publish", {})).status).toBe(507);
  expect([...app.engine.listStaged(t.id)]).toHaveLength(80);
  db.exec("PRAGMA max_page_count=100000");
  app.engine.options.mapFile = () => null;
  expect((await call(t, "/publish", {})).status).toBe(200);
});
it("does not demote server publication when an older commit response arrives", async () => {
  app.engine.options.clientPublish = false;
  const s = new MfupSession({
    serverUrl: url,
    fetch: async (input, init) => {
      const response = await fetch(input, init);
      if (String(input).endsWith("/commit")) {
        await app.engine.publish(s.exportTicket().id);
        await s.refresh();
      }
      return response;
    },
  });
  try {
    await s.upload([new File(["a"], "a")]);
    expect(s.getSnapshot().state).toBe("published");
    expect(s.getSnapshot().errorInfo).toBeNull();
  } finally {
    s.dispose();
  }
});
it("ignores an older failure snapshot after confirmed cancellation", async () => {
  let release!: (r: Response) => void,
    holding = false;
  const s = new MfupSession({
    serverUrl: url,
    fetch: async (input, init) => {
      if (holding && (!init?.method || init.method === "GET"))
        return new Promise<Response>((r) => (release = r));
      return fetch(input, init);
    },
  });
  try {
    await s.connect();
    const old = {
      ...app.engine.snapshot(s.exportTicket().id),
      error: {
        code: "storage_full",
        status: 507,
        phase: "upload",
        retryable: false,
      },
    };
    holding = true;
    const pending = s.refresh();
    await s.cancel();
    release(
      new Response(JSON.stringify(old), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await pending;
    expect(s.getSnapshot().state).toBe("cancelled");
    expect(s.getSnapshot().errorInfo).toBeNull();
  } finally {
    s.dispose();
  }
});
