import { test, expect } from "@playwright/test";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
const target = () => `t-${randomUUID()}`;
const hash = (bytes: Uint8Array | string) =>
  createHash("sha256").update(bytes).digest("hex");
test("native multipart reports progress while a long POST is still unconfirmed", async ({
  page,
  request,
}) => {
  const name = target();
  const result = await page.evaluate(async (target) => {
    const s = new (window as any).mfup3.MfupSession({
      targetDir: target,
      trackUploadProgress: true,
      headers: { "x-mfup-test-slow": "1" },
    });
    const samples: number[] = [];
    let invalid = false;
    s.subscribe(() => {
      const snap = s.getSnapshot();
      if (
        snap.sentBytes > snap.totalBytes ||
        snap.sentBytes < snap.confirmedBytes
      )
        invalid = true;
      if (
        snap.confirmedBytes === 0 &&
        snap.sentBytes > 0 &&
        snap.sentBytes < snap.totalBytes
      )
        samples.push(snap.sentBytes);
    });
    const size = 48 * 1048576;
    try {
      await s.upload([
        new File([new Uint8Array(size).fill(81)], "progress.bin", {
          lastModified: 1,
        }),
      ]);
      return {
        samples: [...new Set(samples)],
        invalid,
        snapshot: s.getSnapshot(),
      };
    } finally {
      s.dispose();
    }
  }, name);
  expect(result.invalid).toBe(false);
  expect(result.samples.length).toBeGreaterThan(1);
  expect(result.snapshot.sentBytes).toBe(48 * 1048576);
  expect(result.snapshot.confirmedBytes).toBe(result.snapshot.sentBytes);
  expect(result.snapshot.state).toBe("published");
  const output = await (
    await request.get(`/test/result?target=${name}`)
  ).json();
  expect(output[0].sha256).toBe(hash(new Uint8Array(48 * 1048576).fill(81)));
});
test.beforeEach(async ({ page, request }) => {
  expect((await (await request.get("/mfup/health")).json()).backend).toBe(
    process.env.MFUP_BACKEND ?? "node",
  );
  await page.goto("/");
  await page.waitForFunction(() => !!(window as any).mfup3);
});
test("React StrictMode starts one upload and reports completion", async ({
  page,
  request,
}) => {
  const name = target();
  const result = await page.evaluate(async (target) => {
    const testModule = await import("/react-test.js");
    return testModule.runReact(target);
  }, name);
  expect(result).toEqual({ sessions: 1, state: "published", bytes: 5 });
  expect(
    (await (await request.get(`/test/result?target=${name}`)).json())[0].sha256,
  ).toBe(hash("react"));
});

test("native files are grouped into up to six requests without JS body reads", async ({
  page,
  request,
}) => {
  const name = target();
  const result = await page.evaluate(async (target) => {
    const { MfupSession } = (window as any).mfup3;
    const files = Array.from(
      { length: 180 },
      (_, i) => new File([`file-${i}`], `f${i}.txt`, { lastModified: 1 }),
    );
    const old = Blob.prototype.arrayBuffer;
    Blob.prototype.arrayBuffer = () => {
      throw new Error("SDK must not read file content");
    };
    const session = new MfupSession({ targetDir: target });
    try {
      await session.upload(files);
      return session.getSnapshot();
    } finally {
      Blob.prototype.arrayBuffer = old;
      session.dispose();
    }
  }, name);
  expect(result.state).toBe("published");
  expect(result.batches).toBeLessThan(30);
  expect(result.maxActiveRequests).toBeLessThanOrEqual(6);
  expect(result.maxActiveRequests).toBeGreaterThan(1);
  const output = await (
    await request.get(`/test/result?target=${name}`)
  ).json();
  expect(output).toHaveLength(180);
  expect(output.find((f: any) => f.path === "f42.txt").sha256).toBe(
    hash("file-42"),
  );
});

test("large file ranges and empty files are byte exact", async ({
  page,
  request,
}) => {
  const name = target(),
    size = 8 * 1048576 + 791;
  const result = await page.evaluate(
    async ({ target, size }) => {
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i % 251;
      const s = new (window as any).mfup3.MfupSession({ targetDir: target });
      try {
        await s.upload([
          new File([data], "big.bin", { lastModified: 1 }),
          new File([], "empty", { lastModified: 1 }),
        ]);
        return s.getSnapshot();
      } finally {
        s.dispose();
      }
    },
    { target: name, size },
  );
  expect(result.confirmedBytes).toBe(size);
  const bytes = new Uint8Array(size).map((_, i) => i % 251);
  const output = await (
    await request.get(`/test/result?target=${name}`)
  ).json();
  expect(output).toContainEqual({ path: "big.bin", size, sha256: hash(bytes) });
  expect(output).toContainEqual({ path: "empty", size: 0, sha256: hash("") });
});

test("directory input preserves relative paths", async ({
  page,
  request,
}, info) => {
  const dir = info.outputPath("folder");
  await fs.mkdir(path.join(dir, "nested"), { recursive: true });
  await fs.writeFile(path.join(dir, "top.txt"), "top");
  await fs.writeFile(path.join(dir, "nested", "child.txt"), "child");
  const name = target();
  await page.evaluate(() => {
    const i = document.createElement("input");
    i.id = "test-folder";
    i.type = "file";
    i.setAttribute("webkitdirectory", "");
    document.body.append(i);
  });
  await page.locator("#test-folder").setInputFiles(dir);
  await page.evaluate(async (target) => {
    const s = new (window as any).mfup3.MfupSession({ targetDir: target });
    try {
      await s.upload(
        (window as any).mfup3.sourceFromInput(
          document.getElementById("test-folder"),
        ),
      );
    } finally {
      s.dispose();
    }
  }, name);
  const output = await (
    await request.get(`/test/result?target=${name}`)
  ).json();
  expect(output.map((f: any) => f.path).sort()).toEqual([
    "folder/nested/child.txt",
    "folder/top.txt",
  ]);
});

test("upload begins while paginated discovery is still running", async ({
  page,
  request,
}) => {
  const name = target();
  const early = await page.evaluate(async (target) => {
    const s = new (window as any).mfup3.MfupSession({
      targetDir: target,
      batchDelayMs: 0,
    });
    let early = false;
    async function* source() {
      yield { kind: "directory", path: "empty" };
      yield {
        kind: "file",
        path: "first",
        file: new File(["first"], "first", { lastModified: 1 }),
      };
      await new Promise((r) => setTimeout(r, 500));
      early = s.getSnapshot().batches > 0 && !s.getSnapshot().scanDone;
      yield {
        kind: "file",
        path: "last",
        file: new File(["last"], "last", { lastModified: 1 }),
      };
    }
    try {
      await s.upload(source());
      return early;
    } finally {
      s.dispose();
    }
  }, name);
  expect(early).toBe(true);
  expect(
    await (await request.get(`/test/result?target=${name}`)).json(),
  ).toHaveLength(2);
});

test("user answers remain available while other files are transferred", async ({
  page,
  request,
}) => {
  const name = target();
  await request.post("/test/seed", {
    data: { target: name, name: "existing.txt", content: "old" },
  });
  await page.evaluate((target) => {
    const s = new (window as any).mfup3.MfupSession({ targetDir: target });
    (window as any).session = s;
    (window as any).upload = s.upload(
      Array.from(
        { length: 100 },
        (_, i) =>
          new File([i ? "new" : "replacement"], i ? `f${i}` : "existing.txt", {
            lastModified: 1,
          }),
      ),
    );
  }, name);
  await expect
    .poll(() =>
      page.evaluate(() => (window as any).session.getSnapshot().state),
    )
    .toBe("waiting");
  const pending = await page.evaluate(() => {
    const s = (window as any).session;
    return { asks: s.getSnapshot().asks, batches: s.getSnapshot().batches };
  });
  expect(pending.batches).toBeGreaterThan(1);
  expect(pending.asks).toHaveLength(1);
  await page.evaluate(async () => {
    const s = (window as any).session;
    await s.setOverwrite(true);
    await (window as any).upload;
    s.dispose();
  });
  const output = await (
    await request.get(`/test/result?target=${name}`)
  ).json();
  expect(output).toHaveLength(100);
  expect(output.find((f: any) => f.path === "existing.txt").sha256).toBe(
    hash("replacement"),
  );
});

test("lost receipt is recovered without a second data POST", async ({
  page,
  request,
}) => {
  const name = target();
  const state = await page.evaluate(async (target) => {
    let lost = false,
      posts = 0;
    const transport = async (input: any, init: any) => {
      const result = await fetch(input, init);
      if (init?.method === "POST" && String(input).includes("/batches/")) {
        posts++;
        if (!lost) {
          lost = true;
          throw new TypeError("simulated response loss");
        }
      }
      return result;
    };
    const s = new (window as any).mfup3.MfupSession({
      targetDir: target,
      fetch: transport,
    });
    try {
      await s.upload([new File(["retained"], "a", { lastModified: 1 })]);
      return { posts, state: s.getSnapshot().state };
    } finally {
      s.dispose();
    }
  }, name);
  expect(state).toEqual({ posts: 1, state: "published" });
  expect(
    (await (await request.get(`/test/result?target=${name}`)).json())[0].sha256,
  ).toBe(hash("retained"));
});

test("page reload and backend restart preserve confirmed data", async ({
  page,
  request,
}) => {
  const name = target();
  const ticket = await page.evaluate(async (target) => {
    const s = new (window as any).mfup3.MfupSession({
      targetDir: target,
      autoPublish: false,
    });
    await s.upload([new File(["survives"], "a", { lastModified: 1 })]);
    const ticket = s.exportTicket();
    s.dispose();
    return ticket;
  }, name);
  const restarted = await request.post("http://127.0.0.1:20065/restart");
  expect(restarted.ok()).toBe(true);
  await page.reload();
  await page.waitForFunction(() => !!(window as any).mfup3);
  const state = await page.evaluate(async (ticket) => {
    const s = new (window as any).mfup3.MfupSession({ ticket });
    try {
      await s.upload([new File(["survives"], "a", { lastModified: 1 })]);
      return s.getSnapshot();
    } finally {
      s.dispose();
    }
  }, ticket);
  expect(state.batches).toBe(0);
  expect(state.state).toBe("published");
  expect(
    (await (await request.get(`/test/result?target=${name}`)).json())[0].sha256,
  ).toBe(hash("survives"));
});

test("pause resumes outstanding ranges and cancellation stops publication", async ({
  page,
  request,
}) => {
  const name = target();
  const result = await page.evaluate(async (target) => {
    const s = new (window as any).mfup3.MfupSession({
      targetDir: target,
      trackUploadProgress: true,
    });
    let paused = false;
    s.subscribe(() => {
      if (!paused && s.getSnapshot().activeRequests > 0) {
        paused = true;
        queueMicrotask(() => s.pause());
      }
    });
    const data = new Uint8Array(12 * 1048576);
    data.fill(73);
    const upload = s.upload([
      new File([data], "pause.bin", { lastModified: 1 }),
    ]);
    while (s.getSnapshot().state !== "paused")
      await new Promise((r) => setTimeout(r, 10));
    await s.resume();
    await upload;
    const snapshot = s.getSnapshot();
    s.dispose();
    const cancelled = new (window as any).mfup3.MfupSession({
      targetDir: target + "-cancel",
      autoPublish: false,
    });
    await cancelled.upload([new File(["a"], "a", { lastModified: 1 })]);
    await cancelled.cancel();
    const cancelState = cancelled.getSnapshot().state;
    cancelled.dispose();
    return { snapshot, cancelState };
  }, name);
  expect(result.snapshot.state).toBe("published");
  expect(result.snapshot.confirmedBytes).toBe(12 * 1048576);
  expect(result.cancelState).toBe("cancelled");
  expect(
    await (await request.get(`/test/result?target=${name}-cancel`)).json(),
  ).toEqual([]);
});
