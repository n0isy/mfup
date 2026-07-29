/**
 * MFUP/2 vs Traditional — side-by-side upload comparison.
 *
 * Drops files onto a shared dropzone, then simultaneously:
 *   - Lane 1: uploads via MFUP/2 (streaming protocol)
 *   - Lane 2: uploads via sequential multipart POST (file by file)
 *
 * Both lanes show a live timer (0.1s precision) + progress bar.
 */

import { MfupSession, type ProgressSnapshot } from "@mfup/client/index.js";

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const dropzone     = document.getElementById("dropzone")!;
const fileInput    = document.getElementById("file-input") as HTMLInputElement;
const folderInput  = document.getElementById("folder-input") as HTMLInputElement;
const race         = document.getElementById("race")!;
const logEl        = document.getElementById("log")!;
const btnReset     = document.getElementById("btn-reset") as HTMLButtonElement;
const conflictOverlay = document.getElementById("conflict-overlay")!;
const modalOverwrite  = document.getElementById("modal-overwrite")!;
const modalCancel     = document.getElementById("modal-cancel")!;

// Lane elements
const mfupTimer   = document.getElementById("mfup-timer")!;
const mfupBar     = document.getElementById("mfup-bar")!;
const mfupBadge   = document.getElementById("mfup-badge")!;
const mfupFiles   = document.getElementById("mfup-files")!;
const mfupBytes   = document.getElementById("mfup-bytes")!;

const trivTimer   = document.getElementById("trivial-timer")!;
const trivBar     = document.getElementById("trivial-bar")!;
const trivBadge   = document.getElementById("trivial-badge")!;
const trivFiles   = document.getElementById("trivial-files")!;
const trivBytes   = document.getElementById("trivial-bytes")!;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let running = false;
const loc = window.location;
const serverUrl = `${loc.protocol}//${loc.host}`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function fmtTime(ms: number): string {
  return `${(ms / 1000).toFixed(1)} sec`;
}

function log(msg: string, cls: "info" | "ok" | "warn" | "err" = "info") {
  logEl.classList.add("visible");
  const ts = new Date().toLocaleTimeString("en-US", { hour12: false, fractionalSecondDigits: 3 });
  const div = document.createElement("div");
  div.className = `entry ${cls}`;
  div.textContent = `${ts}  ${msg}`;
  logEl.appendChild(div);
  logEl.scrollTop = logEl.scrollHeight;
}

function setBadge(el: HTMLElement, state: "waiting" | "running" | "done" | "error") {
  el.className = `lane-badge ${state}`;
  el.textContent = state === "running" ? "Uploading" : state === "done" ? "Done" : state === "error" ? "Failed" : "Waiting";
}

// ---------------------------------------------------------------------------
// Timer — ticks every 100ms, updates the display
// ---------------------------------------------------------------------------
class LaneTimer {
  private el: HTMLElement;
  private startMs = 0;
  private endMs = 0;
  private interval: number | null = null;

  constructor(el: HTMLElement) { this.el = el; }

  start() {
    this.startMs = performance.now();
    this.endMs = 0;
    this.interval = window.setInterval(() => this.render(), 100);
    this.render();
  }

  stop() {
    this.endMs = performance.now();
    if (this.interval) { clearInterval(this.interval); this.interval = null; }
    this.render();
  }

  render() {
    const now = this.endMs || performance.now();
    this.el.textContent = fmtTime(now - this.startMs);
  }

  elapsed(): number { return (this.endMs || performance.now()) - this.startMs; }

  reset() {
    if (this.interval) { clearInterval(this.interval); this.interval = null; }
    this.el.textContent = "0.0 sec";
    this.el.classList.remove("done");
  }
}

// ---------------------------------------------------------------------------
// Flatten files from entries (for Firefox support)
// ---------------------------------------------------------------------------
interface FlatFile {
  file: File;
  path: string;  // relative path like "dir/sub/file.txt"
}

async function readEntryFile(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

async function flattenEntry(entry: FileSystemEntry, prefix: string): Promise<FlatFile[]> {
  if (entry.isFile) {
    const file = await readEntryFile(entry as FileSystemFileEntry);
    return [{ file, path: prefix + entry.name }];
  }
  if (entry.isDirectory) {
    const dirReader = (entry as FileSystemDirectoryEntry).createReader();
    const children = await new Promise<FileSystemEntry[]>((resolve, reject) => {
      const all: FileSystemEntry[] = [];
      const readBatch = () => {
        dirReader.readEntries((entries) => {
          if (entries.length === 0) { resolve(all); return; }
          all.push(...entries);
          readBatch();
        }, reject);
      };
      readBatch();
    });
    const results: FlatFile[] = [];
    for (const child of children) {
      results.push(...await flattenEntry(child, prefix + entry.name + "/"));
    }
    return results;
  }
  return [];
}

async function flattenHandles(handles: FileSystemHandle[], prefix = ""): Promise<FlatFile[]> {
  const results: FlatFile[] = [];
  for (const h of handles) {
    if (h.kind === "file") {
      const file = await (h as FileSystemFileHandle).getFile();
      results.push({ file, path: prefix + h.name });
    } else if (h.kind === "directory") {
      const dir = h as FileSystemDirectoryHandle;
      const children: FileSystemHandle[] = [];
      for await (const [, child] of (dir as any).entries()) {
        children.push(child);
      }
      results.push(...await flattenHandles(children, prefix + h.name + "/"));
    }
  }
  return results;
}

// ---------------------------------------------------------------------------
// MFUP lane
// ---------------------------------------------------------------------------
async function runMfupLane(
  mode: "handles" | "entries" | "files",
  source: FileSystemHandle[] | FileSystemEntry[] | File[],
  timer: LaneTimer,
): Promise<void> {
  const session = new MfupSession({ serverUrl, targetDir: "mfup-target", chunkSize: 256 * 1024 });

  // Conflict handling
  let conflictResolve: ((a: string) => void) | null = null;
  let conflictPromise: Promise<string> | null = null;

  session.onProgress((snap: ProgressSnapshot) => {
    const pct = snap.fraction != null ? Math.round(snap.fraction * 100) : 0;
    mfupBar.style.width = `${pct}%`;
    mfupFiles.textContent = String(snap.acceptedFiles);
    mfupBytes.textContent = fmtBytes(Number(snap.bodyDoneBytes));
  });

  session.on("ask", () => {
    log("[MFUP] Conflict detected — showing modal", "warn");
    conflictOverlay.classList.add("visible");
    conflictPromise = new Promise((r) => { conflictResolve = r; });

    const onOverwrite = () => { cleanup(); session.sendAction("merge_overwrite"); conflictResolve?.("merge_overwrite"); };
    const onCancel = () => { cleanup(); session.sendAction("cancel"); conflictResolve?.("cancel"); };
    const cleanup = () => {
      conflictOverlay.classList.remove("visible");
      modalOverwrite.removeEventListener("click", onOverwrite);
      modalCancel.removeEventListener("click", onCancel);
    };
    modalOverwrite.addEventListener("click", onOverwrite);
    modalCancel.addEventListener("click", onCancel);
  });

  session.on("committed", async (ev) => {
    if (conflictPromise) {
      const action = await conflictPromise;
      if (action === "cancel") { log("[MFUP] Cancelled", "warn"); return; }
    }
    try {
      const resp = await fetch(`${serverUrl}/mfup/sessions/${session.id}/publish`, {
        method: "POST",
        headers: { "X-MFUP-Token": session.token },
      });
      if (resp.ok) {
        const data = await resp.json();
        log(`[MFUP] Published: ${data.published.join(", ")}`, "ok");
      } else {
        log(`[MFUP] Publish failed: ${resp.status}`, "warn");
      }
    } catch (err: any) {
      log(`[MFUP] Publish error: ${err.message}`, "err");
    }
  });

  setBadge(mfupBadge, "running");
  timer.start();
  log("[MFUP] Starting upload...", "info");

  try {
    await session.connect();
    switch (mode) {
      case "handles": await session.uploadHandles(source as FileSystemHandle[]); break;
      case "entries": await session.uploadEntries(source as FileSystemEntry[]); break;
      case "files":   await session.uploadFiles(source as File[]); break;
    }
    timer.stop();
    mfupBar.classList.add("done");
    mfupTimer.classList.add("done");
    setBadge(mfupBadge, "done");
    log(`[MFUP] Complete in ${fmtTime(timer.elapsed())}`, "ok");
  } catch (err: any) {
    timer.stop();
    setBadge(mfupBadge, "error");
    log(`[MFUP] Error: ${err.message}`, "err");
  }
}

// ---------------------------------------------------------------------------
// Trivial lane — sequential multipart POST, one file at a time
// ---------------------------------------------------------------------------
async function runTrivialLane(flatFiles: FlatFile[], timer: LaneTimer): Promise<void> {
  setBadge(trivBadge, "running");
  timer.start();
  log(`[Traditional] Starting upload of ${flatFiles.length} file(s)...`, "info");

  let doneFiles = 0;
  let doneBytes = 0;
  const totalBytes = flatFiles.reduce((s, f) => s + f.file.size, 0);

  try {
    for (const ff of flatFiles) {
      const form = new FormData();
      form.append("file", ff.file, ff.file.name);
      // Send directory path so the server recreates structure
      const dir = ff.path.includes("/") ? ff.path.substring(0, ff.path.lastIndexOf("/")) : "";
      form.append("path", dir);

      const resp = await fetch(`${serverUrl}/trivial/upload`, {
        method: "POST",
        body: form,
      });

      if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        log(`[Traditional] Failed: ${ff.path} — ${data.error ?? resp.statusText}`, "err");
        continue;
      }

      doneFiles++;
      doneBytes += ff.file.size;
      trivFiles.textContent = String(doneFiles);
      trivBytes.textContent = fmtBytes(doneBytes);
      const pct = totalBytes > 0 ? Math.round((doneBytes / totalBytes) * 100) : 0;
      trivBar.style.width = `${pct}%`;
    }

    timer.stop();
    trivBar.classList.add("done");
    trivTimer.classList.add("done");
    setBadge(trivBadge, "done");
    log(`[Traditional] Complete in ${fmtTime(timer.elapsed())}`, "ok");
  } catch (err: any) {
    timer.stop();
    setBadge(trivBadge, "error");
    log(`[Traditional] Error: ${err.message}`, "err");
  }
}

// ---------------------------------------------------------------------------
// Start both lanes simultaneously
// ---------------------------------------------------------------------------
async function startRace(
  mode: "handles" | "entries" | "files",
  source: FileSystemHandle[] | FileSystemEntry[] | File[],
  flatFiles: FlatFile[],
) {
  if (running) return;
  running = true;
  dropzone.classList.add("uploading");
  race.style.display = "grid";
  btnReset.disabled = true;

  const mfupT = new LaneTimer(mfupTimer);
  const trivT = new LaneTimer(trivTimer);

  // Run both lanes in parallel
  await Promise.allSettled([
    runMfupLane(mode, source, mfupT),
    runTrivialLane(flatFiles, trivT),
  ]);

  btnReset.disabled = false;
  log("Both uploads finished.", "ok");
}

// ---------------------------------------------------------------------------
// Drop handler
// ---------------------------------------------------------------------------
dropzone.addEventListener("dragover", (e) => { e.preventDefault(); dropzone.classList.add("dragover"); });
dropzone.addEventListener("dragleave", () => { dropzone.classList.remove("dragover"); });

dropzone.addEventListener("drop", async (e) => {
  e.preventDefault();
  dropzone.classList.remove("dragover");
  if (running) return;

  const items = e.dataTransfer?.items;
  if (!items || items.length === 0) return;

  // --- 1. Try getAsFileSystemHandle (Chrome/Edge) ---
  const handles: FileSystemHandle[] = [];
  let hasHandles = true;
  for (let i = 0; i < items.length; i++) {
    const h = (items[i] as any).getAsFileSystemHandle?.();
    if (h) handles.push(h);
    else { hasHandles = false; break; }
  }

  if (hasHandles && handles.length > 0) {
    const resolved = await Promise.all(handles);
    const flat = await flattenHandles(resolved);
    log(`Drop: ${resolved.length} handle(s), ${flat.length} file(s) total`, "info");
    startRace("handles", resolved, flat);
    return;
  }

  // --- 2. Try webkitGetAsEntry (Firefox/Safari) ---
  const entries: FileSystemEntry[] = [];
  for (let i = 0; i < items.length; i++) {
    const entry = items[i].webkitGetAsEntry?.();
    if (entry) entries.push(entry);
  }

  if (entries.length > 0) {
    const flat: FlatFile[] = [];
    for (const entry of entries) {
      flat.push(...await flattenEntry(entry, ""));
    }
    log(`Drop: ${entries.length} entry(ies), ${flat.length} file(s) total`, "info");
    startRace("entries", entries, flat);
    return;
  }

  // --- 3. Fallback: plain files ---
  const rawFiles = Array.from(e.dataTransfer?.files ?? []);
  if (rawFiles.length > 0) {
    const flat = rawFiles.map(f => ({ file: f, path: f.name }));
    log(`Drop: ${rawFiles.length} file(s) via fallback`, "warn");
    startRace("files", rawFiles, flat);
  }
});

dropzone.addEventListener("click", () => { if (!running) fileInput.click(); });

fileInput.addEventListener("change", () => {
  if (running || !fileInput.files?.length) return;
  const files = Array.from(fileInput.files);
  const flat = files.map(f => ({ file: f, path: f.name }));
  log(`Browse: ${files.length} file(s)`, "info");
  startRace("files", files, flat);
});

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------
btnReset.addEventListener("click", () => {
  running = false;
  dropzone.classList.remove("uploading");
  race.style.display = "none";
  logEl.classList.remove("visible");
  logEl.innerHTML = "";
  mfupTimer.textContent = "0.0 sec";
  mfupTimer.classList.remove("done");
  mfupBar.style.width = "0%";
  mfupBar.classList.remove("done");
  mfupFiles.textContent = "0";
  mfupBytes.textContent = "0 B";
  setBadge(mfupBadge, "waiting");
  trivTimer.textContent = "0.0 sec";
  trivTimer.classList.remove("done");
  trivBar.style.width = "0%";
  trivBar.classList.remove("done");
  trivFiles.textContent = "0";
  trivBytes.textContent = "0 B";
  setBadge(trivBadge, "waiting");
  btnReset.disabled = true;
  fileInput.value = "";
  conflictOverlay.classList.remove("visible");
});
