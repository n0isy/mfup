/**
 * Regression: lost NODE frames must never produce COMMIT_OK on an
 * incomplete tree.
 *
 * Original bug (caught live through a flaky proxy): a batch POST carrying
 * NODE frames died in transit → the server never learned those files
 * existed → get_incomplete_files() saw nothing missing → COMMIT_OK for 16
 * of 65 files.
 *
 * This test kills the backend EARLY, while metadata for hundreds of small
 * files is still in flight, then verifies every single file was published
 * byte-for-byte. Defense layers exercised: batch POST transport retry
 * (same seq), NODE-chain re-send on reconnect, unknown_node NACKs, and the
 * server's node-count commit invariant.
 */
import { test, expect } from "@playwright/test";
import { execSync } from "node:child_process";
import * as path from "node:path";
import { type ManifestEntry, totalBytes, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
const REPO = path.resolve(import.meta.dirname, "../..");

function manySmallFiles(count: number): ManifestEntry[] {
  // ~14 KB average × 600 ≈ 8.4 MB → several 2 MiB batch POSTs, so a kill
  // after the first ACK burst reliably catches later NODE frames in flight.
  const entries: ManifestEntry[] = [];
  for (let i = 0; i < count; i++) {
    const dir = `d${i % 20}/s${i % 5}`;
    entries.push({ path: `${dir}/f-${i}.bin`, size: 10_000 + ((i * 37) % 8000) });
  }
  return entries;
}

test("backend killed mid-scan of many small files → full tree still published", async ({ page, browserName }) => {
  test.skip(browserName !== "chromium", "run once; protocol path is engine-independent");
  test.setTimeout(420_000);

  await page.goto("/e2e.html");
  const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
  test.skip(!caps.opfs || !caps.createWritable, "OPFS unsupported");

  const rootName = `ml-${Date.now().toString(36)}`;
  const manifest = manySmallFiles(600); // ~2.1 MB of pure metadata+small bodies

  const resultPromise = page.evaluate(
    (args) => (window as any).mfupE2E.run(args),
    { rootName, targetDir: "e2e-metaloss", manifest },
  );

  // Kill as soon as the server has accepted SOMETHING but far from everything
  // — metadata frames for later files are then guaranteed to be in flight.
  await expect
    .poll(async () => {
      const p = await page.evaluate(() => (window as any).mfupE2E.progress());
      return p?.bodyDoneBytes ?? 0;
    }, { timeout: 120_000, intervals: [100] })
    .toBeGreaterThan(100_000);

  console.log("killing backend mid-scan...");
  execSync("docker compose kill backend", { cwd: REPO, stdio: "inherit", timeout: 60_000 });
  execSync("docker compose start backend", { cwd: REPO, stdio: "inherit", timeout: 60_000 });

  const result = await resultPromise;
  console.log(`reconnects=${result.reconnects} epoch=${result.epoch} ` +
    `committed=${JSON.stringify(result.committed)} errors=${result.errors.length} ` +
    `log=${JSON.stringify(result.log.slice(-8))}`);

  expect(result.committed).not.toBeNull();
  // THE assertion the original bug would fail: every file counted.
  expect(result.committed.files).toBe(manifest.length);
  expect(result.committed.bytes).toBe(totalBytes(manifest));
  expect(result.publishStatus).toBe(200);

  const problems = verifyTree(path.join(UPLOADS, "e2e-metaloss", rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
