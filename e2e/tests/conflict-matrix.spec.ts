/**
 * Conflict interaction matrix — the non-blocking design's reason to exist:
 * the user answers the overwrite question WHILE (or after) the transfer runs.
 *
 *   ┌──────────────┬───────────────────────────┬──────────────────────────────┐
 *   │              │ answered BEFORE commit    │ answered AFTER commit        │
 *   ├──────────────┼───────────────────────────┼──────────────────────────────┤
 *   │ Overwrite    │ publish OK, bytes = v2    │ publish OK, bytes = v2       │
 *   │ Cancel       │ session aborted,          │ committed session discarded, │
 *   │              │ target untouched (= v1),  │ target untouched (= v1),     │
 *   │              │ staging cleaned           │ staging cleaned              │
 *   └──────────────┴───────────────────────────┴──────────────────────────────┘
 */
import { test, expect } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import { standardManifest, totalBytes, verifyTree, type ManifestEntry } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");

type When = "on_ask" | "after_commit";
type Action = "merge_overwrite" | "cancel";

async function runUpload(page: any, opts: {
  rootName: string; targetDir: string; manifest: ManifestEntry[];
  contentSeed?: string; conflict?: { action: Action; when: When };
}) {
  return page.evaluate((args: any) => (window as any).mfupE2E.run(args), opts);
}

function stagingDirs(): string[] {
  if (!fs.existsSync(UPLOADS)) return [];
  return fs.readdirSync(UPLOADS).filter((n) => n.startsWith(".incoming."));
}

async function expectStagingGone(sessionId: string) {
  await expect
    .poll(() => stagingDirs().some((d) => d.includes(sessionId)), { timeout: 15_000 })
    .toBe(false);
}

const CASES: { name: string; action: Action; when: When }[] = [
  { name: "overwrite answered mid-transfer", action: "merge_overwrite", when: "on_ask" },
  { name: "overwrite answered after commit", action: "merge_overwrite", when: "after_commit" },
  { name: "cancel answered mid-transfer", action: "cancel", when: "on_ask" },
  { name: "cancel answered after commit", action: "cancel", when: "after_commit" },
];

for (const c of CASES) {
  // @chromium-only: engine-independent protocol flow; run once on chromium.
  test(`conflict: ${c.name} @chromium-only`, async ({ page }) => {
    test.setTimeout(240_000);

    await page.goto("/e2e.html");
    const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
    test.skip(!caps.opfs || !caps.createWritable, "OPFS unsupported");

    const rootName = `cm-${c.action === "cancel" ? "c" : "o"}${c.when === "on_ask" ? "b" : "a"}-${Date.now().toString(36)}`;
    const targetDir = `e2e-cm/${rootName}`;
    const manifest = standardManifest({ smallFiles: 30, bigFileBytes: 1_500_000 });
    const targetRoot = path.join(UPLOADS, targetDir, rootName);

    // --- Round 1: clean upload (v1) + publish -----------------------------
    const r1 = await runUpload(page, { rootName, targetDir, manifest });
    expect(r1.publishStatus).toBe(200);
    expect(verifyTree(targetRoot, rootName, manifest)).toEqual([]);

    // --- Round 2: same tree, different bytes (v2) → server sends ASK ------
    const v2seed = rootName + "#v2";
    const r2 = await runUpload(page, {
      rootName, targetDir, manifest,
      contentSeed: v2seed,
      conflict: { action: c.action, when: c.when },
    });

    console.log(`[${c.name}] ask=${r2.askSeen} state=${r2.state} ` +
      `committed=${JSON.stringify(r2.committed)} publish=${r2.publishStatus} ` +
      `uploadError=${r2.uploadError} log=${JSON.stringify(r2.log.slice(-6))}`);

    expect(r2.askSeen).toBe(true);

    if (c.action === "merge_overwrite") {
      expect(r2.committed).not.toBeNull();
      expect(r2.committed.bytes).toBe(totalBytes(manifest));
      expect(r2.publishStatus).toBe(200);
      // Bytes must be the NEW content — proof the overwrite actually happened.
      expect(verifyTree(targetRoot, rootName, manifest, v2seed)).toEqual([]);
    } else {
      // Cancel: no publish attempted, target still holds v1 byte-for-byte.
      expect(r2.publishStatus).toBeNull();
      expect(verifyTree(targetRoot, rootName, manifest, rootName)).toEqual([]);
      // Session must be terminal locally (no headless reconnect loop)...
      expect(["aborted", "failed"]).toContain(r2.state);
      // ...and the server must have cleaned the staging directory.
      await expectStagingGone(r2.sessionId);
    }
  });
}
