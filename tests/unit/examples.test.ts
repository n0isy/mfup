import { afterEach, beforeEach, expect, it } from "vitest";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { createExample } from "../../examples/multiuser-scopes-node/server/server.mjs";
import { MfupSession } from "@mfup/client";

let app: ReturnType<typeof createExample>, base: string, url: string;
async function open(options = {}) {
  app = createExample({ baseDir: base, sweepIntervalMs: 0, ...options });
  await new Promise<void>((resolve) =>
    app.server.listen(0, "127.0.0.1", resolve),
  );
  url = `http://127.0.0.1:${(app.server.address() as any).port}`;
}
beforeEach(async () => {
  base = await fs.mkdtemp(path.join(os.tmpdir(), "mfup3-scope-"));
  await open();
});
afterEach(async () => {
  await app.close();
  await fs.rm(base, { recursive: true, force: true });
});
async function user() {
  const response = await fetch(url + "/api/whoami");
  return {
    cookie: response.headers.get("set-cookie")!.split(";")[0],
    ...(await response.json()),
  };
}
it("keeps the anonymous identity across visits and assigns the requested scope on the server", async () => {
  const a = await user(),
    b = await user();
  expect(a.user_id).not.toBe(b.user_id);
  const again = await (
    await fetch(url + "/api/whoami", { headers: { Cookie: a.cookie } })
  ).json();
  expect(again.user_id).toBe(a.user_id);
  const s = new MfupSession({
    serverUrl: url + "/api",
    headers: { Cookie: a.cookie },
    targetDir: `${b.user_id}/scratch`,
    meta: { scope: "workspace" },
  });
  try {
    await s.upload([new File(["owned"], "a.txt", { lastModified: 1 })]);
  } finally {
    s.dispose();
  }
  const listing = async (cookie: string, scope: string) =>
    (
      await (
        await fetch(`${url}/api/files/${scope}`, {
          headers: { Cookie: cookie },
        })
      ).json()
    ).entries;
  expect(await listing(a.cookie, "workspace")).toEqual([
    { name: "a.txt", dir: false, size: 5 },
  ]);
  expect(await listing(a.cookie, "scratch")).toEqual([]);
  expect(await listing(b.cookie, "workspace")).toEqual([]);
  expect(await listing(b.cookie, "scratch")).toEqual([]);
  expect(
    await fs.readFile(
      path.join(base, "published", a.user_id, "workspace/a.txt"),
      "utf8",
    ),
  ).toBe("owned");
  expect(
    (
      await fetch(url + "/api/file/workspace?path=a.txt", {
        headers: { Cookie: b.cookie },
      })
    ).status,
  ).toBe(404);
});
it("requires a known scope and cookie for session creation and listings", async () => {
  const a = await user();
  for (const [cookie, meta] of [
    ["", { scope: "workspace" }],
    [a.cookie, { scope: "unknown" }],
    [a.cookie, { scope: ["workspace"] }],
    [a.cookie, null],
  ]) {
    expect(
      (
        await fetch(url + "/api/mfup/sessions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Cookie: cookie as string,
          },
          body: JSON.stringify({ protocol: "MFUP/3", meta }),
        })
      ).status,
    ).toBe(403);
  }
  expect((await fetch(url + "/api/files/workspace")).status).toBe(403);
});
it("persists scope context and per-file mapping across a backend restart", async () => {
  await app.close();
  const mapped: any[] = [];
  await open({
    mapFile: (request: any) => {
      mapped.push(request);
      return "documents/" + request.path;
    },
  });
  const a = await user();
  const s = new MfupSession({
    serverUrl: url + "/api",
    headers: { Cookie: a.cookie },
    meta: { scope: "uploads" },
    autoPublish: false,
  });
  await s.upload([new File(["mapped"], "a.txt", { lastModified: 1 })]);
  const ticket = s.exportTicket();
  s.dispose();
  expect(mapped).toHaveLength(1);
  await app.mfup.engine.preparePublish(ticket.id);
  expect(mapped).toHaveLength(1);
  expect(mapped[0].context).toEqual({ uid: a.user_id, scope: "uploads" });
  await app.close();
  await open({
    mapFile: () => {
      throw Error("Mapping must remain persisted");
    },
  });
  const resumed = new MfupSession({ serverUrl: url + "/api", ticket });
  try {
    await resumed.upload([]);
  } finally {
    resumed.dispose();
  }
  expect(
    await fs.readFile(
      path.join(base, "published", a.user_id, "uploads/documents/a.txt"),
      "utf8",
    ),
  ).toBe("mapped");
});
it("resolves an example scope root through authorize and uses it for listings", async () => {
  await app.close();
  const scopeRoot = path.join(base, "scope-storage");
  await open({ scopeRoots: { uploads: scopeRoot } });
  const a = await user();
  const s = new MfupSession({
    serverUrl: url + "/api",
    headers: { Cookie: a.cookie },
    meta: { scope: "uploads" },
    targetDir: "other/scope",
  });
  try {
    await s.upload([new File(["scoped"], "root.txt")]);
    const info = app.mfup.engine.getSession(s.exportTicket().id);
    expect(info.baseDir).toBe(scopeRoot);
    expect(info.targetDir).toBe(`${a.user_id}/uploads`);
    const response = await fetch(url + "/api/file/uploads?path=root.txt", {
      headers: { Cookie: a.cookie },
    });
    expect(await response.text()).toBe("scoped");
    const listing = await (
      await fetch(url + "/api/files/workspace", {
        headers: { Cookie: a.cookie },
      })
    ).json();
    expect(listing.entries).toEqual([]);
  } finally {
    s.dispose();
  }
});
