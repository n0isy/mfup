/**
 * E2E test harness — exposes `window.mfupE2E` for Playwright-driven tests.
 * Not part of the demo UX. Builds deterministic file trees in OPFS and
 * uploads them through the real MfupSession → exercises the
 * ingestFromHandles adapter with genuine FileSystemDirectoryHandle objects.
 */

import { MfupSession, type ProgressSnapshot } from "@mfup/client/index.js";

interface ManifestEntry {
  path: string; // relative path inside the root, "/"-separated
  size: number;
}

interface ConflictOpts {
  action: "merge_overwrite" | "cancel";
  /** Answer the ASK immediately, or only after COMMIT_OK arrived. */
  when: "on_ask" | "after_commit";
}

interface RunOpts {
  rootName: string;   // top-level dir name (unique per test run)
  targetDir: string;  // server-side target dir
  manifest: ManifestEntry[];
  chunkSize?: number;
  /** Seed for deterministic content; defaults to rootName. Use a different
   * seed on a second upload to verify overwrite actually replaced bytes. */
  contentSeed?: string;
  /** How to answer a server ASK (conflict). Absent → never answer. */
  conflict?: ConflictOpts;
}

interface RunResult {
  sessionId: string;
  streaming: boolean | null;
  epoch: number;
  state: string;
  committed: { files: number; bytes: number } | null;
  published: unknown;
  publishStatus: number | null;
  uploadError: string | null;
  askSeen: boolean;
  errors: { code: string; msg: string; fatal: boolean }[];
  log: string[];
  reconnects: number;
}

// ---------------------------------------------------------------------------
// Deterministic content generator — MUST match e2e/lib/gen.ts byte-for-byte.
// ---------------------------------------------------------------------------

function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

function bytesFor(key: string, size: number): Uint8Array {
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

// ---------------------------------------------------------------------------
// OPFS tree builder
// ---------------------------------------------------------------------------

async function buildOpfsTree(rootName: string, manifest: ManifestEntry[], seed: string): Promise<FileSystemDirectoryHandle> {
  const opfs = await navigator.storage.getDirectory();
  try { await opfs.removeEntry(rootName, { recursive: true }); } catch { /* absent */ }
  const root = await opfs.getDirectoryHandle(rootName, { create: true });
  for (const e of manifest) {
    const parts = e.path.split("/");
    let dir: FileSystemDirectoryHandle = root;
    for (let i = 0; i < parts.length - 1; i++) {
      dir = await dir.getDirectoryHandle(parts[i], { create: true });
    }
    const fh = await dir.getFileHandle(parts[parts.length - 1], { create: true });
    const w = await (fh as any).createWritable();
    await w.write(bytesFor(seed + "/" + e.path, e.size));
    await w.close();
  }
  return root;
}

// ---------------------------------------------------------------------------
// Harness API
// ---------------------------------------------------------------------------

const statusEl = document.getElementById("status")!;
let lastProgress: ProgressSnapshot | null = null;
let currentSession: MfupSession | null = null;

async function detect() {
  const out: Record<string, boolean> = { opfs: false, createWritable: false };
  try {
    const opfs = await navigator.storage.getDirectory();
    out.opfs = true;
    const fh = await opfs.getFileHandle(".probe", { create: true });
    out.createWritable = typeof (fh as any).createWritable === "function";
    await opfs.removeEntry(".probe");
  } catch { /* unsupported */ }
  return out;
}

async function run(opts: RunOpts): Promise<RunResult> {
  const log: string[] = [];
  const errors: RunResult["errors"] = [];
  let reconnects = 0;
  let askSeen = false;
  let uploadError: string | null = null;

  statusEl.textContent = "building OPFS tree...";
  const root = await buildOpfsTree(opts.rootName, opts.manifest, opts.contentSeed ?? opts.rootName);

  const session = new MfupSession({
    serverUrl: location.origin,
    targetDir: opts.targetDir,
    chunkSize: opts.chunkSize ?? 256 * 1024,
  });
  currentSession = session;

  session.on("error", (e) => errors.push({ code: String(e.code), msg: e.message, fatal: e.fatal }));
  session.on("state", (s) => log.push("state:" + s));
  session.on("reconnecting", (ev) => { reconnects = ev.attempt; log.push(`reconnecting:${ev.attempt}:${ev.delay}`); });

  let committed: { files: number; bytes: number } | null = null;
  let committedResolve!: () => void;
  const committedPromise = new Promise<void>((r) => { committedResolve = r; });
  session.on("committed", (ev) => {
    committed = ev;
    log.push(`committed:${ev.files}:${ev.bytes}`);
    committedResolve();
  });

  // Conflict answering — the crux of the interactive-transfer design: the
  // upload keeps running while the user decides; the answer may land before
  // or after COMMIT_OK.
  let conflictAnswered!: () => void;
  const conflictDone = new Promise<void>((r) => { conflictAnswered = r; });
  session.on("ask", async () => {
    askSeen = true;
    log.push("ask");
    const c = opts.conflict;
    if (!c) return;
    if (c.when === "after_commit") {
      await committedPromise;
    }
    log.push("answer:" + c.action);
    session.sendAction(c.action);
    conflictAnswered();
  });

  session.onProgress((snap) => { lastProgress = snap; });

  statusEl.textContent = "connecting...";
  await session.connect();
  log.push("streaming:" + session.streamingMode);

  statusEl.textContent = "uploading...";
  try {
    await session.uploadHandles([root]);
  } catch (e: any) {
    uploadError = e?.message ?? String(e);
    log.push("uploadError:" + uploadError);
  }

  // If a conflict answer is configured and the ASK arrived, make sure the
  // answer was actually sent before deciding about publish.
  if (opts.conflict && askSeen) {
    await conflictDone;
  }

  let published: unknown = null;
  let publishStatus: number | null = null;
  const cancelled = opts.conflict?.action === "cancel";
  if (committed && !cancelled) {
    statusEl.textContent = "publishing...";
    // ACTION travels over WS while publish is HTTP — a merge_overwrite
    // answer may still be in flight. Retry a conflict_files 409 briefly.
    for (let attempt = 0; attempt < 4; attempt++) {
      const resp = await fetch(`/mfup/sessions/${session.id}/publish`, {
        method: "POST",
        headers: { "X-MFUP-Token": session.token },
      });
      publishStatus = resp.status;
      published = await resp.json().catch(() => null);
      if (resp.status !== 409) break;
      await new Promise((r) => setTimeout(r, 300));
    }
  }

  statusEl.textContent = "done";
  currentSession = null;
  return {
    sessionId: session.id,
    streaming: session.streamingMode,
    epoch: session.currentEpoch,
    state: session.state,
    committed,
    published,
    publishStatus,
    uploadError,
    askSeen,
    errors,
    log,
    reconnects,
  };
}

function progress() {
  if (!lastProgress) return null;
  return {
    bodyDoneBytes: Number(lastProgress.bodyDoneBytes),
    bodyEstBytes: Number(lastProgress.bodyEstBytes),
    scanDoneUnits: Number(lastProgress.scanDoneUnits),
    acceptedFiles: lastProgress.acceptedFiles,
    fraction: lastProgress.fraction,
    state: currentSession?.state ?? null,
  };
}

// Start an upload but abort it partway (after `abortAfterBytes` accepted).
// Returns the session id so a test can assert disk/Redis cleanup.
async function runAndAbort(opts: RunOpts & { abortAfterBytes: number }): Promise<{ sessionId: string; state: string; log: string[] }> {
  const log: string[] = [];
  const root = await buildOpfsTree(opts.rootName, opts.manifest, opts.contentSeed ?? opts.rootName);
  const session = new MfupSession({
    serverUrl: location.origin,
    targetDir: opts.targetDir,
    chunkSize: opts.chunkSize ?? 256 * 1024,
  });
  currentSession = session;
  session.on("state", (s) => log.push("state:" + s));
  session.onProgress((snap) => { lastProgress = snap; });

  await session.connect();
  // Kick the upload but do not await it — abort once enough bytes landed.
  const up = session.uploadHandles([root]).catch((e) => log.push("uploadErr:" + (e?.message ?? e)));
  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      const done = lastProgress ? Number(lastProgress.bodyDoneBytes) : 0;
      if (done >= opts.abortAfterBytes) {
        clearInterval(timer);
        session.abort("client_cancel", "test abort");
        resolve();
      }
    }, 50);
  });
  await up;
  currentSession = null;
  return { sessionId: session.id, state: session.state, log };
}

(window as any).mfupE2E = { detect, run, runAndAbort, progress, bytesFor, fnv1a };
