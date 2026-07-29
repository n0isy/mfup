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
const REPO = path.resolve(import.meta.dirname, "../..");

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
