import { afterEach, beforeEach, expect, it } from "vitest";
import { createServer, type Server } from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { WebSocket } from "ws";
import {
  createMfup,
  type Options,
  type MfupServer,
} from "../../packages/server/src/index.js";
import { MfupSession } from "../../packages/client/src/index.js";
let base: string;
const apps: { app: MfupServer; server: Server }[] = [];
beforeEach(async () => {
  base = await fs.mkdtemp(path.join(os.tmpdir(), "mfup3-extension-"));
});
afterEach(async () => {
  for (const { app, server } of apps.splice(0)) {
    await app.close();
    server.closeAllConnections();
    await new Promise<void>((r) => server.close(() => r()));
  }
  await fs.rm(base, { recursive: true, force: true });
});
async function open(options: Partial<Options> = {}) {
  const app = createMfup({
    baseDir: base,
    authorize: () => ({}),
    sweepIntervalMs: 0,
    onError: () => {},
    ...options,
  });
  const server = createServer(async (req, res) => {
    if (!(await app.handle(req, res))) {
      res.writeHead(404);
      res.end();
    }
  });
  app.attach(server);
  apps.push({ app, server });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const origin = `http://127.0.0.1:${(server.address() as any).port}`;
  const url = origin + (options.prefix ?? "");
  return { app, server, origin, url };
}
async function create(url: string, meta: unknown = null, query = "") {
  return fetch(url + "/mfup/sessions" + query, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ protocol: "MFUP/3", meta }),
  });
}
function call(url: string, t: any, action: string, data: unknown = {}) {
  return fetch(`${url}/mfup/sessions/${t.id}/${action}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
}
const totals = { files: 0, dirs: 0, bytes: 0 };
it("passes request context, enforces metadata limits, and denies a failed authorize", async () => {
  const seen: any[] = [];
  const { url, app } = await open({
    maxMetaBytes: 20,
    authorize: (r) => {
      seen.push(r);
      return {};
    },
  });
  expect((await create(url, { tooLong: "x".repeat(30) })).status).toBe(413);
  expect(seen).toHaveLength(0);
  const t = await (await create(url, { a: 1 }, "?album=42")).json();
  expect(seen[0]).toMatchObject({
    sessionId: t.id,
    query: { album: "42" },
    meta: { a: 1 },
  });
  expect(seen[0].client).toContain("127.0.0.1");
  app.engine.options.authorize = () => {
    throw Error("consumer");
  };
  expect((await create(url)).status).toBe(403);
});
it("serves HTTP and WebSocket under the configured prefix", async () => {
  const { url, origin } = await open({ prefix: "/api/uploads" });
  const t = await (await create(url)).json();
  expect((await fetch(origin + "/mfup/health")).status).toBe(404);
  const ws = new WebSocket(url.replace("http:", "ws:") + "/mfup/control");
  try {
    const state = await new Promise<any>((resolve, reject) => {
      ws.on("error", reject);
      ws.on("open", () => ws.send(JSON.stringify({ type: "subscribe", ...t })));
      ws.once("message", (data) => resolve(JSON.parse(String(data))));
    });
    expect(state.id).toBe(t.id);
  } finally {
    ws.close();
  }
});
it("maps metadata before body receipt and reads accepted content in onCommitted", async () => {
  const mapped: any[] = [];
  const home = path.join(base, "home");
  const { url, app } = await open({
    limits: { partBytes: 2, batchBytes: 4 },
    authorize: () => ({
      baseDir: home,
      targetDir: "scope",
      context: { uid: 7 },
    }),
    mapFile: async (r) => {
      mapped.push(r);
      expect(() => app.engine.openStaged(r.sessionId, r.path)).toThrow();
      return "mapped/" + r.name;
    },
    onCommitted: async (event) => {
      let content = "";
      for await (const chunk of app.engine.openStaged(event.sessionId, "a.txt"))
        content += chunk;
      expect(content).toBe("abcdef");
    },
  });
  const session = new MfupSession({ serverUrl: url, autoPublish: false });
  try {
    await session.upload([new File(["abcdef"], "a.txt", { lastModified: 1 })]);
    const t = session.exportTicket();
    expect(mapped).toHaveLength(1);
    expect([...app.engine.listStaged(t.id)]).toMatchObject([
      { path: "a.txt", size: 6 },
    ]);
    expect(app.engine.getSession(t.id).stagingDir).toBe(
      path.join(home, "staging", t.id),
    );
    await Promise.all([
      app.engine.preparePublish(t.id),
      app.engine.preparePublish(t.id),
    ]);
    expect(mapped).toHaveLength(1);
    expect(mapped[0]).toMatchObject({
      name: "a.txt",
      targetDir: "scope",
      context: { uid: 7 },
    });
    app.engine.options.mapFile = () => {
      throw Error("plan is already saved");
    };
    await session.publish();
    expect(
      await fs.readFile(
        path.join(home, "published/scope/mapped/a.txt"),
        "utf8",
      ),
    ).toBe("abcdef");
  } finally {
    session.dispose();
  }
});
it.each(["same", "parent", "case"])(
  "validates the complete mapping before moving any file: %s",
  async (kind) => {
    const { url, app } = await open({
      mapFile: (r) =>
        kind === "same"
          ? "same"
          : kind === "parent"
            ? r.path === "a"
              ? "p"
              : "p/b"
            : r.path === "a"
              ? "Folder/a"
              : "folder/b",
    });
    const session = new MfupSession({ serverUrl: url, autoPublish: false });
    try {
      await expect(
        session.upload([new File(["a"], "a"), new File(["b"], "b")]),
      ).rejects.toMatchObject({ code: "mapping_error" });
      const t = session.exportTicket();
      expect((await call(url, t, "publish")).status).toBe(409);
      expect(app.engine.snapshot(t.id).state).toBe("uploading");
      app.engine.options.mapFile = () => null;
      await session.retry();
      expect((await call(url, t, "publish")).status).toBe(200);
      expect(
        await fs.readFile(path.join(base, "published/uploads/a"), "utf8"),
      ).toBe("a");
    } finally {
      session.dispose();
    }
  },
);
it("keeps commit successful on hook failure and permits an explicit backend retry", async () => {
  let calls = 0;
  const { url, app } = await open({
    onCommitted: () => {
      if (++calls === 1) throw Error("temporary");
      return false;
    },
  });
  const t = await (await create(url)).json();
  let r = await call(url, t, "commit", totals);
  expect(r.status).toBe(200);
  expect(await r.json()).toMatchObject({
    state: "committed",
    processing: "failed",
  });
  r = await call(url, t, "publish");
  expect(r.status).toBe(409);
  await call(url, t, "commit", totals);
  expect(calls).toBe(1);
  expect((await app.engine.retryCommitted(t.id)).processing).toBe("done");
  expect(calls).toBe(2);
  expect((await call(url, t, "publish")).status).toBe(200);
});
it("blocks browser publication and cleanup while the committed hook is running", async () => {
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((r) => (release = r)),
    ready = new Promise<void>((r) => (entered = r));
  const { url, app } = await open({
    onCommitted: async () => {
      entered();
      await gate;
      return false;
    },
  });
  const t = await (await create(url)).json(),
    commit = call(url, t, "commit", totals);
  await ready;
  try {
    expect((await call(url, t, "publish")).status).toBe(409);
    expect(await app.engine.sweep(Date.now() + 86400001)).toBe(0);
  } finally {
    release();
    await commit;
  }
});
it("separates browser autoPublish from server-only publication", async () => {
  const { url, app } = await open({ clientPublish: false });
  const session = new MfupSession({ serverUrl: url });
  try {
    await session.upload([new File(["a"], "a")]);
    const t = session.exportTicket();
    expect(session.getSnapshot().state).toBe("committed");
    expect((await call(url, t, "publish")).status).toBe(403);
    expect((await app.engine.publish(t.id)).state).toBe("published");
  } finally {
    session.dispose();
  }
});
it("auto-publishes on the server and continues after a conflict answer", async () => {
  const { url } = await open({ clientPublish: false, autoPublish: true });
  await fs.mkdir(path.join(base, "published/uploads"), { recursive: true });
  await fs.writeFile(path.join(base, "published/uploads/a"), "old");
  const session = new MfupSession({ serverUrl: url });
  try {
    await session.upload([new File(["new"], "a")]);
    const t = session.exportTicket();
    expect(session.getSnapshot().state).toBe("committed");
    const ask = session.getSnapshot().asks[0];
    const answer = await call(url, t, "answers", {
      id: ask.id,
      choice: "overwrite",
    });
    expect(await answer.json()).toMatchObject({ state: "published" });
    expect(
      await fs.readFile(path.join(base, "published/uploads/a"), "utf8"),
    ).toBe("new");
  } finally {
    session.dispose();
  }
});
it("accepts a boolean committed decision and preserves client autoPublish false", async () => {
  const { url, app } = await open({
    onCommitted: () => true,
    clientPublish: false,
  });
  const s = new MfupSession({ serverUrl: url, autoPublish: false });
  try {
    await s.upload([new File(["a"], "a")]);
    expect(s.getSnapshot().state).toBe("published");
  } finally {
    s.dispose();
  }
  app.engine.options.onCommitted = () => false;
  const t = await (await create(url)).json();
  expect(await (await call(url, t, "commit", totals)).json()).toMatchObject({
    state: "committed",
    processing: "done",
  });
});
it("recovers failed processing and per-session policy in a different root after restart", async () => {
  const home = path.join(base, "separate-root");
  const first = await open({
    authorize: () => ({
      baseDir: home,
      targetDir: "area",
      clientPublish: false,
      context: { job: 42 },
    }),
    onCommitted: () => {
      throw Error("interrupted");
    },
  });
  const s = new MfupSession({ serverUrl: first.url });
  await s.upload([new File(["restart"], "a")]);
  const ticket = s.exportTicket();
  s.dispose();
  expect(s.getSnapshot().processing).toBe("failed");
  await first.app.close();
  first.server.closeAllConnections();
  await new Promise<void>((r) => first.server.close(() => r()));
  const second = await open({
    onCommitted: (event) => {
      expect(event.context).toEqual({ job: 42 });
      expect(event.baseDir).toBe(home);
      return true;
    },
  });
  expect((await call(second.url, ticket, "publish")).status).toBe(403);
  expect((await second.app.engine.retryCommitted(ticket.id)).state).toBe(
    "published",
  );
  expect(await fs.readFile(path.join(home, "published/area/a"), "utf8")).toBe(
    "restart",
  );
});

it("asks after the first mapped destination while the next map and all bodies are pending", async () => {
  let release!: () => void;
  const hold = new Promise<void>((r) => {
    release = r;
  });
  const { app } = await open({
    mapFile: async (r) => {
      if (r.name === "b") await hold;
      return r.name;
    },
  });
  await fs.mkdir(path.join(base, "published/uploads"), { recursive: true });
  await fs.writeFile(path.join(base, "published/uploads/a"), "old");
  const t = await app.engine.create({ protocol: "MFUP/3" }, {});
  const batch = await app.engine.begin(t.id, t.epoch, "early", () => {});
  const prepare = app.engine
    .prepare(batch, {
      files: [
        ["a", 3, 1, 0, 3],
        ["b", 3, 1, 0, 3],
      ],
      dirs: [],
    })
    .finally(() => app.engine.end(batch));
  try {
    await expect
      .poll(() => app.engine.snapshot(t.id).overwriteRequired)
      .toBe(true);
    expect(app.engine.snapshot(t.id).confirmedBytes).toBe(0);
    await app.engine.setProperties(t.id, { overwrite: true });
    release();
    await prepare;
    expect(app.engine.snapshot(t.id).asks).toEqual([]);
  } finally {
    release();
    await prepare;
  }
});

it("acknowledges cancellation during metadata mapping and waits for callback cleanup", async () => {
  let release!: () => void,
    entered = false;
  const hold = new Promise<void>((r) => {
    release = r;
  });
  const { app } = await open({
    mapFile: async (r) => {
      entered = true;
      await hold;
      return r.path;
    },
  });
  const t = await app.engine.create({ protocol: "MFUP/3" }, {});
  const batch = await app.engine.begin(t.id, t.epoch, "early", () => {});
  const prepare = app.engine
    .prepare(batch, { files: [["a", 3, 1, 0, 3]], dirs: [] })
    .catch((e) => e)
    .finally(() => app.engine.end(batch));
  try {
    await expect.poll(() => entered).toBe(true);
    expect((await app.engine.cancel(t.id)).state).toBe("cancelled");
    expect((await prepare).code).toBe("bad_state");
    expect(await app.engine.sweep(Number.MAX_SAFE_INTEGER)).toBe(0);
  } finally {
    release();
  }
});

it("handles a large persisted inventory without materializing all nodes or published paths", async () => {
  const { app } = await open();
  const t = await app.engine.create({ protocol: "MFUP/3" }, {});
  const store = app.engine.store;
  const insert = store.db.prepare(
    "INSERT INTO nodes(sid,path,kind,size,mtime,destination,source_key,destination_key,mapped,done) VALUES(?,?,'file',0,1,?,?,?,1,1)",
  );
  store.transaction(() => {
    for (let i = 0; i < 100000; i++) {
      const name = `f${String(i).padStart(6, "0")}`;
      insert.run(t.id, name, name, name, name);
    }
  });
  const original = store.all.bind(store);
  store.all = ((sql: string, ...args: any[]) => {
    if (sql.includes("FROM nodes")) expect(sql).toMatch(/LIMIT (256|\?)/);
    return original(sql, ...args);
  }) as typeof store.all;
  store.run("UPDATE sessions SET state='committed' WHERE id=?", t.id);
  const state = await app.engine.publish(t.id);
  expect(state.files).toBe(100000);
  expect(state.publishedCount).toBe(100000);
  expect(state.published).toHaveLength(256);
  expect(state.publishedNext).toBe("f000255");
  expect(app.engine.publishedPage(t.id, "f099990").files).toHaveLength(9);
}, 30000);
