/**
 * Upload a deterministic OPFS tree via MfupSession.uploadHandles()
 * (real FileSystemDirectoryHandle objects) and verify bytes on disk.
 */
import { test, expect } from "@playwright/test";
import * as path from "node:path";
import { standardManifest, totalBytes, verifyTree } from "../lib/gen.js";

const UPLOADS = path.resolve(import.meta.dirname, "../../uploads");

test("OPFS handles upload → commit → publish → bytes verified", async ({ page, browserName }) => {
  await page.goto("/e2e.html");

  const caps = await page.evaluate(() => (window as any).mfupE2E.detect());
  test.skip(!caps.opfs || !caps.createWritable, `OPFS/createWritable unsupported in ${browserName}`);

  const rootName = `opfs-${browserName}-${Date.now().toString(36)}`;
  const manifest = standardManifest();

  const result = await page.evaluate(
    (args) => (window as any).mfupE2E.run(args),
    { rootName, targetDir: "e2e", manifest },
  );

  console.log(`[${browserName}] streaming=${result.streaming} epoch=${result.epoch} ` +
    `committed=${JSON.stringify(result.committed)} errors=${JSON.stringify(result.errors)}`);

  expect(result.errors.filter((e: any) => e.fatal)).toEqual([]);
  expect(result.committed).not.toBeNull();
  expect(result.committed.bytes).toBe(totalBytes(manifest));
  expect(result.publishStatus).toBe(200);

  const problems = verifyTree(path.join(UPLOADS, "e2e", rootName), rootName, manifest);
  expect(problems).toEqual([]);
});
