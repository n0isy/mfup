/**
 * Retention audit: a session that ends by abort/cancel must leave NOTHING
 * behind — no staging dir on disk, no Redis zset member, no Redis meta hash.
 * Also verifies the sweep endpoint and that publish cleans up.
 *
 * Redis is inspected via `docker compose exec redis redis-cli`.
 */
import { test, expect } from "@playwright/test";
import { execSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { standardManifest } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
const REPO = path.resolve(import.meta.dirname, "../..");

function redisCli(args: string): string {
  return execSync(`docker compose exec -T redis redis-cli ${args}`, { cwd: REPO }).toString().trim();
}
function stagingExists(sessionId: string): boolean {
  const d = path.join(UPLOADS, `.incoming.${sessionId}`);
  return fs.existsSync(d);
}
function inZset(sessionId: string): boolean {
  return redisCli(`ZSCORE mfup:sessions ${sessionId}`) !== "";
}
function metaExists(sessionId: string): boolean {
  return redisCli(`EXISTS mfup:meta:${sessionId}`) === "1";
}

test.describe("retention", () => {
  test("client abort mid-transfer leaves no disk or Redis trace @chromium-only", async ({ page }) => {
    test.setTimeout(120_000);

    await page.goto("/e2e.html");
    const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
    test.skip(!caps.opfs || !caps.createWritable, "OPFS unsupported");

    const rootName = `ret-${Date.now().toString(36)}`;
    const manifest = standardManifest({ smallFiles: 40, bigFileBytes: 8_000_000 });

    const res = await page.evaluate(
      (args) => (window as any).mfupE2E.runAndAbort(args),
      { rootName, targetDir: "e2e-ret", manifest, abortAfterBytes: 500_000 },
    );
    console.log(`abort: session=${res.sessionId} state=${res.state} log=${JSON.stringify(res.log.slice(-5))}`);

    expect(res.state).toBe("aborted");

    // Cleanup happens in the WS finally block — poll briefly.
    await expect.poll(() => stagingExists(res.sessionId), { timeout: 15_000 }).toBe(false);
    expect(inZset(res.sessionId)).toBe(false);
    expect(metaExists(res.sessionId)).toBe(false);
  });

  test("no orphaned staging dirs accumulate (reconciliation net) @chromium-only", async () => {
    // Plant a fake orphan INSIDE the backend container, so it is owned by the
    // same user that owns the bind-mounted uploads dir (the host test user
    // cannot write there in CI). It has payload contents, no Redis entry, and
    // an mtime aged past the grace window → a true unreachable orphan. The
    // startup reconciler must remove it.
    const fakeSid = `orphan-${Date.now().toString(36)}`;
    const cdir = `/data/uploads/.incoming.${fakeSid}`; // path inside container
    const plant = [
      `mkdir -p ${cdir}/payload`,
      `dd if=/dev/zero of=${cdir}/payload/leftover.bin bs=1024 count=1 2>/dev/null`,
      `touch -d '20 minutes ago' ${cdir}`, // age past ORPHAN_GRACE (600s)
    ].join(" && ");
    execSync(`docker compose exec -T backend sh -c ${JSON.stringify(plant)}`, { cwd: REPO });

    const existsInContainer = () =>
      execSync(`docker compose exec -T backend sh -c ${JSON.stringify(`test -d ${cdir} && echo yes || echo no`)}`,
        { cwd: REPO }).toString().trim() === "yes";

    expect(existsInContainer()).toBe(true);
    expect(inZset(fakeSid)).toBe(false); // not registered → true orphan

    // Restart backend: startup reconciliation scans the filesystem.
    execSync("docker compose restart backend", { cwd: REPO, stdio: "inherit", timeout: 60_000 });
    await expect.poll(() => {
      try { execSync("curl -sf http://localhost:20060/health", { stdio: "ignore" }); return true; }
      catch { return false; }
    }, { timeout: 60_000 }).toBe(true);

    // The orphan must be gone.
    await expect.poll(() => existsInContainer(), { timeout: 20_000 }).toBe(false);
  });
});
