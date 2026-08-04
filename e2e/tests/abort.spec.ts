/**
 * Cancelling a running upload must be terminal and INSTANT:
 *   - the session goes straight to "aborted" and stays there;
 *   - it is never resurrected by an in-flight reconnect
 *     (aborted → active → committing was a real bug);
 *   - the upload promise settles instead of hanging on a commit
 *     that can never arrive.
 */
import { test, expect } from "@playwright/test";

test("cancel mid-upload → aborted immediately, never resurrected @chromium-only", async ({ page }) => {
  await page.goto("/e2e.html");
  await page.waitForFunction(() => (window as any).mfupE2E !== undefined);

  const result = await page.evaluate(() => (window as any).mfupE2E.runAndAbort({
    rootName: `abort-${Date.now().toString(36)}`,
    targetDir: ".",
    manifest: [{ path: "big.bin", size: 48 * 1024 * 1024 }],
    abortAfterBytes: 4 * 1024 * 1024,     // cancel mid-flight
  }), undefined, { timeout: 120_000 } as any);

  expect(result.state).toBe("aborted");
  const idx = result.log.indexOf("state:aborted");
  expect(idx).toBeGreaterThan(-1);
  // Nothing after the abort may flip the session back to life.
  expect(result.log.slice(idx + 1)).toEqual(
    result.log.slice(idx + 1).filter((l: string) => !/^state:(active|committing|committed)$/.test(l)),
  );
});
