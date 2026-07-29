/**
 * Conflict flow: upload the same tree twice into the same target.
 * Second upload must trigger ASK → modal → Overwrite → publish succeeds.
 */
import { test, expect } from "@playwright/test";
import * as path from "node:path";
import { standardManifest, writeTreeToDisk, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
const TMP = path.resolve(import.meta.dirname, "../.tmp");

test("second upload into same target → ASK → merge_overwrite → published", async ({ page, browserName }) => {
  test.skip(browserName !== "chromium", "flow is engine-independent; run once");

  const rootName = `cfl-${Date.now().toString(36)}`;
  const manifest = standardManifest({ smallFiles: 20, bigFileBytes: 200_000 });
  const dirPath = writeTreeToDisk(TMP, rootName, manifest);

  // First upload — clean path
  await page.goto("/");
  await page.setInputFiles("#folder-input", dirPath);
  await expect(page.locator("#log")).toContainText("Published:", { timeout: 120_000 });

  // Reset UI, upload again into the same target
  await page.click("#btn-reset");
  await page.setInputFiles("#folder-input", dirPath);

  // Server must send ASK → conflict modal appears
  await expect(page.locator("#conflict-overlay")).toBeVisible({ timeout: 60_000 });
  await page.click("#modal-overwrite");

  await expect(page.locator("#log")).toContainText("Published:", { timeout: 120_000 });

  const problems = verifyTree(path.join(UPLOADS, rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
