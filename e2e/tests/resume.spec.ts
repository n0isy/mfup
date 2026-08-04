/**
 * Resume path: restart the backend container mid-upload.
 * Expect: WS reconnect → RESUME → byte-exact continuation → COMMIT_OK,
 * and the published file matches the source byte-for-byte.
 * This exercises: Redis-based recovery, COMMITTING/ACTIVE revert,
 * FileWriter seek+truncate, epoch fencing, client backoff.
 */
import { test, expect } from "@playwright/test";
import { execSync } from "node:child_process";
import * as path from "node:path";
import { standardManifest, totalBytes, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
// compose commands run against the TEST stack: e2e/docker-compose.yaml
const REPO = path.resolve(import.meta.dirname, "..");

test("backend restart mid-upload → reconnect → resume → verified bytes @chromium-only", async ({ page }) => {
  test.setTimeout(420_000);

  await page.goto("/e2e.html");
  const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
  test.skip(!caps.opfs || !caps.createWritable, "OPFS unsupported");

  const rootName = `res-${Date.now().toString(36)}`;
  const manifest = standardManifest({ bigFileBytes: 60_000_000, smallFiles: 8 });

  const resultPromise = page.evaluate(
    (args) => (window as any).mfupE2E.run(args),
    { rootName, targetDir: "e2e-resume", manifest },
  );

  // Wait until the server has accepted a few MB, then kill the backend
  await expect
    .poll(async () => {
      const p = await page.evaluate(() => (window as any).mfupE2E.progress());
      return p?.bodyDoneBytes ?? 0;
    }, { timeout: 180_000, intervals: [250] })
    .toBeGreaterThan(2_000_000);

  // Hard kill: `compose restart` sends SIGTERM and uvicorn drains in-flight
  // requests gracefully — the upload can win that race. SIGKILL cannot be raced.
  // (`compose kill/start` is CI-portable: container names depend on the
  // project directory name.)
  console.log("killing backend mid-upload...");
  execSync("docker compose kill backend", { cwd: REPO, stdio: "inherit", timeout: 60_000 });
  execSync("docker compose start backend", { cwd: REPO, stdio: "inherit", timeout: 60_000 });

  const result = await resultPromise;
  console.log(`reconnects=${result.reconnects} epoch=${result.epoch} ` +
    `committed=${JSON.stringify(result.committed)} log=${JSON.stringify(result.log.slice(-10))}`);

  expect(result.committed).not.toBeNull();
  expect(result.committed.bytes).toBe(totalBytes(manifest));
  expect(result.reconnects).toBeGreaterThanOrEqual(1);
  expect(result.publishStatus).toBe(200);

  const problems = verifyTree(path.join(UPLOADS, "e2e-resume", rootName), rootName, manifest);
  expect(problems).toEqual([]);
});

/**
 * Regression: GRACEFUL restart (SIGTERM) with a SINGLE big file. This is the
 * interleaving the kill-based test above got lucky on: after RESUME the
 * requeued file is shifted into activeFile immediately, the queue reads
 * empty, and a buggy finalizeScan entered the commit loop mid-stream —
 * SESSION_END closed the data channel under the writer, the writer chewed
 * the rest of the file into the closed channel and marked it "sent", and
 * the COMMIT_RETRY rounds ping-ponged data-free until commit_failed.
 */
test("graceful restart mid-upload of a single big file → resumes and commits @chromium-only", async ({ page }) => {
  test.setTimeout(420_000);

  await page.goto("/e2e.html");
  const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
  test.skip(!caps.opfs || !caps.createWritable, "OPFS unsupported");

  const rootName = `resg-${Date.now().toString(36)}`;
  const manifest = [{ path: "big.bin", size: 120_000_000 }];

  const resultPromise = page.evaluate(
    (args) => (window as any).mfupE2E.run(args),
    { rootName, targetDir: "e2e-resume", manifest },
  );

  await expect
    .poll(async () => {
      const p = await page.evaluate(() => (window as any).mfupE2E.progress());
      return p?.bodyDoneBytes ?? 0;
    }, { timeout: 180_000, intervals: [250] })
    .toBeGreaterThan(5_000_000);

  console.log("gracefully restarting backend mid-upload...");
  execSync("docker compose restart backend", { cwd: REPO, stdio: "inherit", timeout: 120_000 });

  const result = await resultPromise;
  console.log(`reconnects=${result.reconnects} committed=${JSON.stringify(result.committed)} ` +
    `log=${JSON.stringify(result.log.slice(-10))}`);

  expect(result.committed).not.toBeNull();
  expect(result.committed.bytes).toBe(totalBytes(manifest));
  expect(result.publishStatus).toBe(200);

  const problems = verifyTree(path.join(UPLOADS, "e2e-resume", rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
