/** Linux integration test: run unprivileged with /storage tmpfs and /readonly ro tmpfs. */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { createMfup } from "../packages/server/dist/index.js";
const base = await fs.mkdtemp(path.join(os.tmpdir(), "mfup-storage-"));
let root = "/storage/home";
const app = createMfup({
  baseDir: base,
  authorize: () => ({ baseDir: root }),
  onError: () => {},
  sweepIntervalMs: 0,
});
const server = createServer((req, res) => void app.handle(req, res));
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const url = `http://127.0.0.1:${server.address().port}`;
const create = () =>
  fetch(url + "/mfup/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ protocol: "MFUP/3" }),
  });
const call = (t, action, data = {}) =>
  fetch(`${url}/mfup/sessions/${t.id}/${action}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
const payload = Buffer.alloc(1024 * 1024, 120);
async function send(t, bid) {
  const form = new FormData();
  form.append(
    "manifest",
    JSON.stringify({
      files: [["a", payload.length, 1, 0, payload.length]],
      dirs: [],
    }),
  );
  form.append("0", new Blob([payload]), "a");
  return fetch(`${url}/mfup/sessions/${t.id}/batches/${bid}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "X-MFUP-Epoch": String(t.epoch),
    },
    body: form,
  });
}
try {
  const t = await (await create()).json();
  await fs.mkdir("/storage/home/published/uploads", { recursive: true });
  await fs.writeFile("/storage/home/published/uploads/a", "old");
  await fs.writeFile("/storage/filler", Buffer.alloc(3584 * 1024));
  const failed = await send(t, "full");
  assert.equal(failed.status, 507);
  assert.equal((await failed.json()).error, "storage_full");
  const receipt = await fetch(`${url}/mfup/sessions/${t.id}/batches/full`, {
    headers: { Authorization: `Bearer ${t.token}` },
  });
  assert.equal(receipt.status, 404);
  assert.equal(
    await fs.readFile("/storage/home/published/uploads/a", "utf8"),
    "old",
  );
  await fs.unlink("/storage/filler");
  t.epoch = (await (await call(t, "resume")).json()).epoch;
  assert.equal((await send(t, "retry")).status, 200);
  assert.equal(
    (await call(t, "commit", { files: 1, dirs: 0, bytes: payload.length }))
      .status,
    200,
  );
  assert.equal((await call(t, "properties", { overwrite: true })).status, 200);
  assert.equal((await call(t, "publish")).status, 200);
  assert.deepEqual(
    await fs.readFile("/storage/home/published/uploads/a"),
    payload,
  );
  root = "/readonly";
  let r = await create();
  assert.equal(r.status, 503);
  assert.equal((await r.json()).error, "storage_unavailable");
  root = base + "/no-access";
  await fs.mkdir(root);
  await fs.chmod(root, 0o500);
  try {
    r = await create();
    assert.equal(r.status, 503);
    assert.equal((await r.json()).error, "storage_unavailable");
  } finally {
    await fs.chmod(root, 0o700);
  }
  console.log(
    "Node real ENOSPC, EROFS, EACCES: classified, no false receipt, original destination retained, resume verified.",
  );
} finally {
  await app.close();
  server.closeAllConnections();
  await new Promise((r) => server.close(r));
  await fs.rm(base, { recursive: true, force: true });
}
