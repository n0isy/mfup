import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { MfupSession } from "@mfup/client";
import { useMfupUpload } from "@mfup/react";
import { configFromEnv, publishedDirectory } from "@mfup/server";
import { createExample } from "./example-server.mjs";

assert.equal(typeof useMfupUpload, "function");
const base = path.resolve("data"),
  home = path.resolve("scope-home");
let calls = 0;
const app = createExample({
  baseDir: base,
  scopeRoots: { uploads: home },
  clientPublish: false,
  mapFile: (request) => {
    calls++;
    assert.equal(request.context.scope, "uploads");
    return "mapped/" + request.name;
  },
  onCommitted: async (event) => {
    assert.equal(event.baseDir, home);
    assert.equal(event.context.scope, "uploads");
    const files = [...app.mfup.engine.listStaged(event.sessionId)];
    assert.deepEqual(
      files.map((f) => f.path),
      ["a.txt"],
    );
    let content = "";
    for await (const chunk of app.mfup.engine.openStaged(
      event.sessionId,
      "a.txt",
    ))
      content += chunk;
    assert.equal(content, "consumer");
    return true;
  },
});
await new Promise((r) => app.server.listen(0, "127.0.0.1", r));
const url = `http://127.0.0.1:${app.server.address().port}`;
const who = await fetch(url + "/api/whoami");
const cookie = who.headers.get("set-cookie").split(";")[0];
const identity = await who.json();
const session = new MfupSession({
  serverUrl: url + "/api",
  meta: { scope: "uploads" },
  headers: { Cookie: cookie },
  autoPublish: false,
});
try {
  await session.upload([new File(["consumer"], "a.txt", { lastModified: 1 })]);
  assert.equal(session.getSnapshot().state, "published");
  assert.equal(calls, 1);
  const ticket = session.exportTicket();
  assert.equal(app.mfup.engine.getSession(ticket.id).baseDir, home);
  assert.equal((await app.mfup.engine.publish(ticket.id)).state, "published");
  const file = await fetch(url + "/api/file/uploads?path=mapped/a.txt", {
    headers: { Cookie: cookie },
  });
  assert.equal(await file.text(), "consumer");
  assert.equal(
    await fs.readFile(
      path.join(
        publishedDirectory(home, `${identity.user_id}/uploads`),
        "mapped/a.txt",
      ),
      "utf8",
    ),
    "consumer",
  );
  const other = await fetch(url + "/api/whoami");
  const otherCookie = other.headers.get("set-cookie").split(";")[0];
  assert.deepEqual(
    (
      await (
        await fetch(url + "/api/files/uploads", {
          headers: { Cookie: otherCookie },
        })
      ).json()
    ).entries,
    [],
  );
} finally {
  session.dispose();
  await app.close();
}
await fs.writeFile("hooks.mjs", "export const authorize = () => ({});");
const config = await configFromEnv({
  MFUP_AUTHORIZE: "./hooks.mjs#authorize",
  MFUP_AUTO_PUBLISH: "1",
  MFUP_CLIENT_PUBLISH: "false",
});
assert.equal(config.autoPublish, true);
assert.equal(config.clientPublish, false);
assert.deepEqual(await config.authorize({}), {});
console.log(
  "Installed packages: public hooks, scoped example, staged reads, boolean publication and env config pass.",
);
