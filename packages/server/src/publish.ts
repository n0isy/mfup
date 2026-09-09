/**
 * MFUP/2 publish (rename from staging to target).
 *
 * Port of server/mfup-core/mfup_core/publish.py. All filesystem work is
 * async (fs/promises → libuv thread pool), which keeps the event loop free —
 * the Python original needs asyncio.to_thread for the same effect.
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";

import { createLogger } from "./logger.js";
import { DEFAULT_STAGING_PREFIX, stagingDir, validateNodeName } from "./storage.js";

const logger = createLogger("mfup.publish");

/** Publish detected file conflicts requiring user action. */
export class ConflictError extends Error {
  constructor(readonly conflictingFiles: number) {
    super(`${conflictingFiles} conflicting file(s)`);
  }
}

/** The consumer's mapFile hook produced an unusable path (escape attempt,
 * illegal segment, or two files mapped to one target). */
export class MappingError extends Error {}

async function exists(p: string): Promise<boolean> {
  try {
    await fsp.access(p);
    return true;
  } catch {
    return false;
  }
}

async function isDir(p: string): Promise<boolean> {
  try {
    return (await fsp.stat(p)).isDirectory();
  } catch {
    return false;
  }
}

async function isFile(p: string): Promise<boolean> {
  try {
    return (await fsp.stat(p)).isFile();
  } catch {
    return false;
  }
}

/** All files in the session's payload as ["/"-relative path, size]. */
export async function listPayloadFiles(
  baseDir: string,
  sessionId: string,
  prefix: string = DEFAULT_STAGING_PREFIX,
): Promise<[string, number][]> {
  const payload = path.join(stagingDir(baseDir, sessionId, prefix), "payload");
  if (!(await exists(payload))) {
    throw Object.assign(new Error(`no payload directory for session ${sessionId}`), {
      code: "ENOENT",
    });
  }
  const out: [string, number][] = [];
  const walk = async (dir: string): Promise<void> => {
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        await walk(p);
      } else if (e.isFile()) {
        const st = await fsp.stat(p);
        out.push([path.relative(payload, p).split(path.sep).join("/"), st.size]);
      }
    }
  };
  await walk(payload);
  out.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return out;
}

/**
 * Reject mapped destinations that could escape targetDir.
 *
 * Same per-segment rules as ingest names (no "..", separators inside a
 * segment, NUL), plus: must be relative and non-empty.
 */
export function validateMappedPath(rel: string): void {
  if (!rel || rel.startsWith("/") || rel.startsWith("\\")) {
    throw new MappingError(`mapped path must be relative and non-empty: ${JSON.stringify(rel)}`);
  }
  for (const segment of rel.split("/")) {
    try {
      validateNodeName(segment);
    } catch (exc) {
      throw new MappingError(`illegal mapped path ${JSON.stringify(rel)}: ${(exc as Error).message}`);
    }
  }
}

/**
 * Publish with a per-file layout decided by the consumer's mapFile hook.
 *
 * `mapping` is {payload-relative source → target-relative destination};
 * files absent from the mapping keep their client layout. Materializes
 * FILES only (directories are implied; empty client dirs are not preserved
 * in mapped mode). Same conflict semantics as the plain path: existing
 * destinations require action === "merge_overwrite", otherwise
 * ConflictError is raised before anything moves.
 */
export async function publishSessionMapped(
  baseDir: string,
  sessionId: string,
  targetDir: string,
  mapping: Record<string, string>,
  prefix: string = DEFAULT_STAGING_PREFIX,
  action: string | null = null,
): Promise<string[]> {
  const sd = stagingDir(baseDir, sessionId, prefix);
  const payload = path.join(sd, "payload");
  if (!(await exists(payload))) {
    throw Object.assign(new Error(`no payload directory for session ${sessionId}`), {
      code: "ENOENT",
    });
  }

  const files = await listPayloadFiles(baseDir, sessionId, prefix);

  // Resolve, validate and collision-check the full plan BEFORE moving
  // anything — publish must not stop halfway on a consumer-hook bug.
  const plan: [string, string][] = [];
  const seen = new Map<string, string>();
  const targetRes = path.resolve(targetDir);
  for (const [rel] of files) {
    const destRel = mapping[rel] ?? rel;
    validateMappedPath(destRel);
    const dest = path.join(targetDir, destRel);
    // Defense in depth after segment validation.
    const relCheck = path.relative(targetRes, path.resolve(dest));
    if (relCheck.startsWith("..") || path.isAbsolute(relCheck)) {
      throw new MappingError(`mapped path escapes target: ${JSON.stringify(destRel)}`);
    }
    const prev = seen.get(destRel);
    if (prev !== undefined) {
      throw new MappingError(
        `two files map to ${JSON.stringify(destRel)}: ${JSON.stringify(prev)} and ${JSON.stringify(rel)}`,
      );
    }
    seen.set(destRel, rel);
    plan.push([path.join(payload, rel), destRel]);
  }

  let conflicts = 0;
  for (const [, destRel] of plan) {
    // Reuse the complete destination map. No extra filesystem reads or index.
    for (let slash = destRel.lastIndexOf("/"); slash !== -1; slash = destRel.lastIndexOf("/", slash - 1)) {
      if (seen.has(destRel.slice(0, slash))) {
        throw new MappingError(`mapped file is also a parent directory: ${JSON.stringify(destRel)}`);
      }
    }
    if (await exists(path.join(targetDir, destRel))) conflicts += 1;
  }
  if (conflicts > 0 && action === null) {
    throw new ConflictError(conflicts);
  }

  await fsp.mkdir(targetDir, { recursive: true });
  const published: string[] = [];
  for (const [src, destRel] of plan) {
    const dest = path.join(targetDir, destRel);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    // fs.rename overwrites an existing destination file on POSIX (like
    // os.replace); conflicts were gated on `action` above.
    await fsp.rename(src, dest);
    published.push(destRel);
    logger.info(`Published (mapped) session ${sessionId}: ${path.basename(src)} -> ${destRel}`);
  }

  await cleanupStaging(sd);
  return published;
}

/**
 * Count files in payload that already exist in targetDir.
 *
 * Dirs are auto-merged (not conflicts). Only file-vs-file collisions count;
 * a type mismatch (file vs dir) counts as a conflict too.
 */
export async function detectConflicts(targetDir: string, payload: string): Promise<number> {
  let count = 0;
  const entries = await fsp.readdir(payload, { withFileTypes: true });
  for (const entry of entries) {
    const src = path.join(payload, entry.name);
    const dest = path.join(targetDir, entry.name);
    if (!(await exists(dest))) continue;
    if (entry.isDirectory() && (await isDir(dest))) {
      count += await detectConflicts(dest, src);
    } else if (entry.isFile() && (await isFile(dest))) {
      count += 1;
    } else {
      count += 1; // type mismatch
    }
  }
  return count;
}

/** Recursively merge src into dst, overwriting files. Returns published names. */
async function mergeTree(src: string, dst: string): Promise<string[]> {
  const published: string[] = [];
  const entries = await fsp.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    const s = path.join(src, entry.name);
    const dest = path.join(dst, entry.name);
    if (entry.isDirectory()) {
      if (await isDir(dest)) {
        published.push(...(await mergeTree(s, dest)));
      } else if (await exists(dest)) {
        // Type conflict: replace file with dir
        await fsp.unlink(dest);
        await fsp.rename(s, dest);
        published.push(entry.name);
      } else {
        await fsp.rename(s, dest);
        published.push(entry.name);
      }
    } else {
      // File: overwrite or create (rename overwrites like os.replace)
      if ((await exists(dest)) && (await isDir(dest))) {
        await fsp.rm(dest, { recursive: true, force: true });
      }
      await fsp.rename(s, dest);
      published.push(entry.name);
    }
  }
  return published;
}

/**
 * Publish payload entries into targetDir.
 *
 * If conflicts exist and action is null, throws ConflictError. If action is
 * "merge_overwrite", merges dirs and overwrites files. Returns the list of
 * published entry names.
 */
export async function publishSession(
  baseDir: string,
  sessionId: string,
  targetDir: string,
  prefix: string = DEFAULT_STAGING_PREFIX,
  action: string | null = null,
): Promise<string[]> {
  const sd = stagingDir(baseDir, sessionId, prefix);
  const payload = path.join(sd, "payload");

  if (!(await exists(payload))) {
    throw Object.assign(new Error(`no payload directory for session ${sessionId}`), {
      code: "ENOENT",
    });
  }

  await fsp.mkdir(targetDir, { recursive: true });

  const conflicts = await detectConflicts(targetDir, payload);

  if (conflicts > 0 && action === null) {
    throw new ConflictError(conflicts);
  }

  // Existing directories need merging even when all file names are distinct.
  // mergeTree still renames a whole subtree when its destination is absent.
  const published = await mergeTree(payload, targetDir);

  for (const name of published) {
    logger.info(`Published session ${sessionId}: ${name}`);
  }

  await cleanupStaging(sd);
  return published;
}

/** Remove the leftover staging directory after publish. */
async function cleanupStaging(sd: string): Promise<void> {
  try {
    await fsp.rm(sd, { recursive: true, force: true });
  } catch (exc) {
    logger.error(`Failed to clean staging dir ${sd}: ${exc}`);
  }
}
