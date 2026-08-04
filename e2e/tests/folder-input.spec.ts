/**
 * Upload a real on-disk directory through the demo page's
 * <input webkitdirectory> (ingestFromFileList adapter) and verify bytes.
 */
import { test, expect } from "@playwright/test";
import * as path from "node:path";
import { standardManifest, writeTreeToDisk, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
const TMP = path.resolve(import.meta.dirname, "../.tmp");

test("webkitdirectory input upload via demo page", async ({ page, browserName }) => {
  const rootName = `fld-${browserName}-${Date.now().toString(36)}`;
  const manifest = standardManifest();
  const dirPath = writeTreeToDisk(TMP, rootName, manifest);

  await page.goto("/");
  await page.setInputFiles("#folder-input", dirPath);

  await expect(page.locator("#log")).toContainText("Published:", { timeout: 180_000 });
  // On failure, surface WHAT error card flipped the badge — a bare
  // "Received: FAILED" hides the actual fatal (learned the hard way).
  const diag = await page.locator("#error-list").innerText().catch(() => "(no error cards)");
  await expect(page.locator("#state-badge"), `error cards:\n${diag}`).toHaveText("COMMITTED");

  // demo uses targetDir "." → files land at uploads/<rootName>/
  const problems = verifyTree(path.join(UPLOADS, rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
