/**
 * Deterministic test-tree generator + on-disk verifier.
 * bytesFor() MUST match example/src/e2e.ts byte-for-byte.
 */
import * as fs from "node:fs";
import * as path from "node:path";

export interface ManifestEntry {
  path: string; // relative, "/"-separated
  size: number;
}

export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

export function bytesFor(key: string, size: number): Uint8Array {
  const buf = new Uint8Array(size);
  let x = fnv1a(key) || 1;
  for (let i = 0; i < size; i++) {
    x ^= (x << 13) >>> 0; x >>>= 0;
    x ^= x >>> 17;
    x ^= (x << 5) >>> 0; x >>>= 0;
    buf[i] = x & 0xff;
  }
  return buf;
}

/** Standard project-like manifest: nested dirs, unicode, empty file, one big file. */
export function standardManifest(opts?: { bigFileBytes?: number; smallFiles?: number }): ManifestEntry[] {
  const entries: ManifestEntry[] = [];
  const small = opts?.smallFiles ?? 60;
  const dirs = ["src", "src/components", "src/utils", "assets", "assets/img", "deep/a/b/c/d"];
  for (let i = 0; i < small; i++) {
    const dir = dirs[i % dirs.length];
    entries.push({ path: `${dir}/file-${i}.ts`, size: 512 + ((i * 997) % 7000) });
  }
  entries.push({ path: "README.md", size: 3000 });
  entries.push({ path: "empty.txt", size: 0 });
  entries.push({ path: "файл — тест ❤.txt", size: 2048 });
  entries.push({ path: "assets/img/photo.bin", size: 300_000 });
  entries.push({ path: "bundle.bin", size: opts?.bigFileBytes ?? 2_000_000 });
  return entries;
}

export function totalBytes(manifest: ManifestEntry[]): number {
  return manifest.reduce((s, e) => s + e.size, 0);
}

/** Write the manifest tree to disk (for <input webkitdirectory> tests). */
export function writeTreeToDisk(baseDir: string, rootName: string, manifest: ManifestEntry[]): string {
  const root = path.join(baseDir, rootName);
  fs.rmSync(root, { recursive: true, force: true });
  for (const e of manifest) {
    const p = path.join(root, ...e.path.split("/"));
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, bytesFor(rootName + "/" + e.path, e.size));
  }
  return root;
}

/** Verify uploaded tree on disk matches the manifest. Returns list of problems.
 * `contentSeed` defaults to rootName; pass the seed used at generation time
 * (e.g. a "#v2" variant) to verify an overwrite actually replaced bytes. */
export function verifyTree(uploadedRoot: string, rootName: string, manifest: ManifestEntry[], contentSeed?: string): string[] {
  const seed = contentSeed ?? rootName;
  const problems: string[] = [];
  for (const e of manifest) {
    const p = path.join(uploadedRoot, ...e.path.split("/"));
    if (!fs.existsSync(p)) {
      problems.push(`MISSING: ${e.path}`);
      continue;
    }
    const actual = fs.readFileSync(p);
    const expected = Buffer.from(bytesFor(seed + "/" + e.path, e.size));
    if (actual.length !== expected.length) {
      problems.push(`SIZE: ${e.path} expected ${expected.length} got ${actual.length}`);
    } else if (!actual.equals(expected)) {
      let firstDiff = -1;
      for (let i = 0; i < actual.length; i++) {
        if (actual[i] !== expected[i]) { firstDiff = i; break; }
      }
      problems.push(`BYTES: ${e.path} first diff at offset ${firstDiff}`);
    }
  }
  return problems;
}
