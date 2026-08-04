/**
 * Smoke for the @mfup/react bindings: the react.html demo is built purely on
 * useMfupUpload + useMfupDropzone (autoPublish flow). A folder upload through
 * the hook must commit, auto-publish, and land byte-exact on disk.
 */
import { test, expect, type Page } from "@playwright/test";
import * as path from "node:path";
import { standardManifest, writeTreeToDisk, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");
const TMP = path.resolve(import.meta.dirname, "../.tmp");

/** Open react.html and wait for React to mount — with diagnostics: a bare
 * setInputFiles timeout hides WHY the page did not render (JS error, asset
 * 404, slow cold start). Surfaces pageerrors/console errors instead. */
async function openReactPage(page: Page): Promise<void> {
  const problems: string[] = [];
  page.on("pageerror", (e) => problems.push(`pageerror: ${e.message}`));
  page.on("console", (m) => { if (m.type() === "error") problems.push(`console: ${m.text()}`); });
  await page.goto("/react.html");
  try {
    await page.locator("[data-testid=dropzone]").waitFor({ timeout: 30_000 });
  } catch {
    throw new Error(`react page failed to render; ${problems.join(" | ") || "(no page errors captured)"}`);
  }
}

test("react hooks: folder upload → auto-publish → bytes on disk", async ({ page, browserName }) => {
  const rootName = `react-${browserName}-${Date.now().toString(36)}`;
  const manifest = standardManifest();
  const dirPath = writeTreeToDisk(TMP, rootName, manifest);

  await openReactPage(page);
  await page.setInputFiles("[data-testid=folder-input]", dirPath);

  await expect(page.locator("[data-testid=log]")).toContainText("published:", { timeout: 180_000 });
  await expect(page.locator("[data-testid=state]")).toHaveText("committed");
  await expect(page.locator("[data-testid=published]")).toContainText(rootName);

  const problems = verifyTree(path.join(UPLOADS, rootName), rootName, manifest);
  expect(problems).toEqual([]);
});

test("react hooks: conflict ask dialog → overwrite → published @chromium-only", async ({ page, browserName }) => {
  const rootName = `reactc-${browserName}-${Date.now().toString(36)}`;
  const manifest = standardManifest();
  const dirPath = writeTreeToDisk(TMP, rootName, manifest);

  // Round 1: clean upload.
  await openReactPage(page);
  await page.setInputFiles("[data-testid=folder-input]", dirPath);
  await expect(page.locator("[data-testid=log]")).toContainText("published:", { timeout: 180_000 });

  // Round 2: same target → server ASKs → answer via the hook-rendered dialog.
  await page.locator("[data-testid=reset]").click();
  await page.setInputFiles("[data-testid=folder-input]", dirPath);
  await expect(page.locator("[data-testid=ask]")).toBeVisible({ timeout: 60_000 });
  await page.locator("[data-testid=ask-overwrite]").click();
  await expect(page.locator("[data-testid=log]")).toContainText("published:", { timeout: 180_000 });

  const problems = verifyTree(path.join(UPLOADS, rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
