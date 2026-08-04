/**
 * MFUP/2 Demo App — wires the client library to a drag-and-drop UI.
 *
 * Drop handler priority:
 *   1. getAsFileSystemHandle()  — Chrome/Edge (persistent handles)
 *   2. webkitGetAsEntry()       — Firefox/Safari (persistent FileSystemEntry)
 *   3. <input> file picker      — all browsers (File objects from inputs persist)
 *
 * DataTransferItem.getAsFile() / e.dataTransfer.files are NOT used for
 * drag-and-drop because Firefox invalidates the underlying blob data after
 * the handler returns. The FileSystemEntry API (webkitGetAsEntry) gives
 * persistent references that survive the handler and support lazy streaming.
 */

import {
  MfupSession,
  MfupError,
  sourceFromDataTransfer,
  sourceFromInput,
  type UploadSource,
  type ProgressSnapshot,
  type SessionState,
} from "@mfup/client";

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const dropzone      = document.getElementById("dropzone")!;
const fileInput     = document.getElementById("file-input") as HTMLInputElement;
const folderInput   = document.getElementById("folder-input") as HTMLInputElement;
const browseBtn     = document.getElementById("browse-btn")!;
const progressPanel = document.getElementById("progress")!;
const bar           = document.getElementById("bar")!;
const stateBadge    = document.getElementById("state-badge")!;
const sessionLabel  = document.getElementById("session-id-label")!;
const statBytes     = document.getElementById("stat-bytes")!;
const statFiles     = document.getElementById("stat-files")!;
const statScanned   = document.getElementById("stat-scanned")!;
const statSkipped   = document.getElementById("stat-skipped")!;
const btnAbort      = document.getElementById("btn-abort") as HTMLButtonElement;
const btnReset      = document.getElementById("btn-reset") as HTMLButtonElement;
const logEl         = document.getElementById("log")!;
const errorsPanel   = document.getElementById("errors")!;
const errorList     = document.getElementById("error-list")!;
const errorCountEl  = document.getElementById("error-count")!;
const conflictOverlay = document.getElementById("conflict-overlay")!;
const modalOverwrite  = document.getElementById("modal-overwrite")!;
const modalCancel     = document.getElementById("modal-cancel")!;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let session: MfupSession | null = null;
let cancelled = false;
let conflictResolve: ((action: "merge_overwrite" | "cancel") => void) | null = null;
let conflictPromise: Promise<"merge_overwrite" | "cancel"> | null = null;

// Determine server URL from current page location
const loc = window.location;
const serverUrl = `${loc.protocol}//${loc.host}`;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------
const MAX_LOG_ENTRIES = 200;
const MAX_ERROR_CARDS = 50;

function log(msg: string, cls: "info" | "ok" | "warn" | "err" = "info") {
  logEl.classList.add("visible");
  const ts = new Date().toLocaleTimeString("en-US", { hour12: false, fractionalSecondDigits: 3 });
  const div = document.createElement("div");
  div.className = `entry ${cls}`;
  div.textContent = `${ts}  ${msg}`;
  logEl.appendChild(div);
  while (logEl.children.length > MAX_LOG_ENTRIES) logEl.removeChild(logEl.firstChild!);
  logEl.scrollTop = logEl.scrollHeight;
}

// ---------------------------------------------------------------------------
// Error display — full detail with stack traces and cause chains
// ---------------------------------------------------------------------------
let errorCount = 0;

function formatCauseChain(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const parts: string[] = [];
  let cur: unknown = err;
  let depth = 0;
  while (cur instanceof Error && depth < 5) {
    const prefix = depth === 0 ? "" : "Caused by: ";
    const stack = cur.stack ?? `${cur.name}: ${cur.message}`;
    parts.push(`${prefix}${stack}`);
    cur = cur.cause;
    depth++;
  }
  return parts.join("\n\n");
}

function renderError(err: MfupError) {
  errorsPanel.classList.add("visible");
  errorCount++;
  errorCountEl.textContent = String(errorCount);

  const card = document.createElement("div");
  card.className = `error-card${err.fatal ? "" : " warning"}`;

  const ts = new Date(err.timestamp).toLocaleTimeString("en-US", { hour12: false, fractionalSecondDigits: 3 });

  const causeChain = err.cause ? formatCauseChain(err.cause) : null;
  const ownStack = err.stack ?? "";
  const detailJson = Object.keys(err.detail).length > 0 ? JSON.stringify(err.detail, null, 2) : null;

  card.innerHTML = `
    <div class="error-code">
      <span>${err.code}</span>
      <span class="error-layer">${err.layer} / ${err.fatal ? "FATAL" : "recoverable"}</span>
    </div>
    <div class="error-msg">${escapeHtml(err.message)}</div>
    <div class="error-action">${escapeHtml(err.action)}</div>
    ${detailJson ? `<pre class="error-detail">${escapeHtml(detailJson)}</pre>` : ""}
    <details class="error-stack-details">
      <summary>Stack trace</summary>
      <pre class="error-stack">${escapeHtml(ownStack)}</pre>
      ${causeChain ? `<pre class="error-stack cause">${escapeHtml(causeChain)}</pre>` : ""}
    </details>
    <div class="error-time">${ts}</div>
  `;

  errorList.prepend(card);
  while (errorList.children.length > MAX_ERROR_CARDS) errorList.removeChild(errorList.lastChild!);
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------
function fmtBytes(n: bigint | number): string {
  const v = Number(n);
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  if (v < 1024 * 1024 * 1024) return `${(v / (1024 * 1024)).toFixed(1)} MB`;
  return `${(v / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

// ---------------------------------------------------------------------------
// Progress & state rendering
// ---------------------------------------------------------------------------
function renderProgress(snap: ProgressSnapshot) {
  const pct = snap.fraction != null ? Math.round(snap.fraction * 100) : 0;
  bar.style.width = `${pct}%`;
  statBytes.textContent = fmtBytes(snap.bodyDoneBytes);
  statFiles.textContent = String(snap.acceptedFiles);
  statScanned.textContent = String(snap.scanDoneUnits);
  statSkipped.textContent = String(snap.skippedFiles);
}

// ProgressTracker emits synchronously on every mutation — for many-small-file
// uploads (node_modules...) that is tens of thousands of DOM updates.
// Coalesce to one render per animation frame.
let pendingSnap: ProgressSnapshot | null = null;
let progressRaf: number | null = null;
function scheduleProgress(snap: ProgressSnapshot) {
  pendingSnap = snap;
  if (progressRaf == null) {
    progressRaf = requestAnimationFrame(() => {
      progressRaf = null;
      if (pendingSnap) renderProgress(pendingSnap);
      pendingSnap = null;
    });
  }
}

function renderState(state: SessionState) {
  stateBadge.textContent = state.toUpperCase().replace(/_/g, " ");
  stateBadge.className = `badge ${state}`;

  if (state === "committed") {
    bar.classList.add("done");
    bar.style.width = "100%";
    btnAbort.disabled = true;
  } else if (state === "aborted" || state === "failed") {
    bar.classList.add("error");
    btnAbort.disabled = true;
  } else if (state === "active" || state === "paused_by_server" || state === "waiting_resume") {
    btnAbort.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Upload launcher
// ---------------------------------------------------------------------------
async function startUpload(source: UploadSource) {
  // Show progress panel
  progressPanel.classList.add("visible");
  dropzone.classList.add("uploading");

  cancelled = false;
  session = new MfupSession({ serverUrl, targetDir: ".", chunkSize: 256 * 1024 });
  sessionLabel.textContent = `session ${session.id.slice(0, 8)}...`;
  btnAbort.disabled = false;

  log(`Session created: ${session.id}`, "info");

  session.onProgress(scheduleProgress);
  session.on("state", (s) => { renderState(s); log(`State -> ${s}`, "info"); });
  session.on("ask", (ask) => {
    log(`Server: conflict in target directory${ask.name ? ` ("${ask.name}")` : ""}`, "warn");
    conflictOverlay.classList.add("visible");

    conflictPromise = new Promise((resolve) => { conflictResolve = resolve; });

    const onOverwrite = () => {
      cleanup();
      ask.respond("merge_overwrite");
      log("User chose: merge & overwrite", "info");
      conflictResolve?.("merge_overwrite");
    };
    const onCancel = () => {
      cleanup();
      cancelled = true;
      ask.respond("cancel");
      log("User chose: cancel", "warn");
      conflictResolve?.("cancel");
    };
    const cleanup = () => {
      conflictOverlay.classList.remove("visible");
      modalOverwrite.removeEventListener("click", onOverwrite);
      modalCancel.removeEventListener("click", onCancel);
    };

    modalOverwrite.addEventListener("click", onOverwrite);
    modalCancel.addEventListener("click", onCancel);
  });
  session.on("committed", async (ev) => {
    log(`COMMITTED: ${ev.files} files, ${fmtBytes(ev.bytes)}`, "ok");

    // If conflict modal is open, wait for user to respond before publishing
    if (conflictPromise) {
      log("Waiting for conflict resolution...", "info");
      const action = await conflictPromise;
      conflictPromise = null;
      conflictResolve = null;
      if (action === "cancel") {
        log("Session was cancelled — skipping publish", "warn");
        return;
      }
    }

    if (cancelled) {
      log("Session was cancelled — skipping publish", "warn");
      return;
    }
    try {
      const { published } = await session!.publish();
      log(`Published: ${published.join(", ")}`, "ok");
    } catch (err: any) {
      log(`Publish failed: ${err.message}`, "warn");
    }
  });
  session.on("reconnecting", (ev) => {
    const maxLabel = ev.maxAttempts != null ? String(ev.maxAttempts) : "∞";
    log(`Reconnecting: attempt ${ev.attempt}/${maxLabel} (waiting ${(ev.delay / 1000).toFixed(1)}s)`, "warn");
    stateBadge.textContent = `RECONNECTING ${ev.attempt}/${maxLabel}`;
    stateBadge.className = "badge waiting_resume";
    btnAbort.disabled = false;
  });
  session.on("error", (err) => {
    log(`[${err.layer}/${err.code}] ${err.message}`, err.fatal ? "err" : "warn");
    if (err.cause instanceof Error) {
      log(`  Cause: ${err.cause.name}: ${err.cause.message}`, "err");
      if (err.cause.stack) {
        log(`  ${err.cause.stack.split("\n").slice(1, 4).join("\n  ")}`, "err");
      }
    }
    renderError(err);
    if (err.fatal && session?.state !== "aborted") {
      renderState("failed");
    }
  });

  try {
    log("Connecting...", "info");
    await session.connect();
    const mode_label = session.streamingMode ? "streaming (duplex:half)" : "batch (sequential POST)";
    log(`Connected  epoch=${session.currentEpoch}  data=${mode_label}`, "ok");

    await session.upload(source);
    log("Upload complete.", "ok");
  } catch (err: any) {
    // Don't treat abort as a fatal error
    if (session?.state === "aborted") {
      log("Upload aborted.", "warn");
      return;
    }
    const msg = err instanceof Error
      ? `${err.message}\n${(err.stack ?? "").split("\n").slice(1, 5).join("\n")}`
      : String(err);
    log(`FATAL: ${msg}`, "err");
    renderState("failed");
  }
}

// ---------------------------------------------------------------------------
// Drop handler
//
// Priority:
//   1. getAsFileSystemHandle() — Chrome/Edge, returns persistent handles
//   2. webkitGetAsEntry()      — Firefox/Safari, returns persistent entries
//   Both must be called SYNCHRONOUSLY in the drop handler (same tick).
// ---------------------------------------------------------------------------
dropzone.addEventListener("dragover", (e) => { e.preventDefault(); dropzone.classList.add("dragover"); });
dropzone.addEventListener("dragleave", () => { dropzone.classList.remove("dragover"); });

dropzone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropzone.classList.remove("dragover");
  if (session) return; // already uploading

  // sourceFromDataTransfer encodes the browser priority chain
  // (getAsFileSystemHandle → webkitGetAsEntry → DataTransfer.files) and MUST
  // run synchronously in the drop handler — it does.
  const src = e.dataTransfer ? sourceFromDataTransfer(e.dataTransfer) : null;
  if (!src) return;
  log(`Drop: source via ${src.kind}`, "info");
  startUpload(src);
});

// ---------------------------------------------------------------------------
// Browse buttons — <input> File objects always persist, no DataTransfer issues
// ---------------------------------------------------------------------------
browseBtn.addEventListener("click", (e) => {
  e.stopPropagation();
  fileInput.click();
});

// Folder picker — previously unreachable: the input existed but nothing
// ever clicked it, so the uploadFileList path had no UI entry point.
const browseFolderBtn = document.getElementById("browse-folder-btn");
browseFolderBtn?.addEventListener("click", (e) => {
  e.stopPropagation();
  folderInput.click();
});

dropzone.addEventListener("click", () => {
  if (!session) fileInput.click();
});

fileInput.addEventListener("change", () => {
  if (session) return;
  const src = sourceFromInput(fileInput);
  if (!src) return;
  log(`Browse: ${fileInput.files!.length} file(s)`, "info");
  startUpload(src);
});

folderInput.addEventListener("change", () => {
  if (session) return;
  const src = sourceFromInput(folderInput);
  if (!src) return;
  log(`Browse: folder with ${folderInput.files!.length} file(s)`, "info");
  startUpload(src);
});

// ---------------------------------------------------------------------------
// Action buttons
// ---------------------------------------------------------------------------
btnAbort.addEventListener("click", () => {
  if (session) {
    session.abort();
    log("Abort requested", "warn");
  }
});

btnReset.addEventListener("click", () => {
  // Reset must not orphan a live session: without this, clearing the guard
  // left the previous upload running headless in the background.
  if (session && !["committed", "aborted", "failed"].includes(session.state)) {
    session.abort("client_reset", "user reset the UI");
  }
  session = null;
  progressPanel.classList.remove("visible");
  dropzone.classList.remove("uploading");
  logEl.classList.remove("visible");
  logEl.innerHTML = "";
  errorsPanel.classList.remove("visible");
  errorList.innerHTML = "";
  errorCount = 0;
  errorCountEl.textContent = "0";
  bar.style.width = "0%";
  bar.classList.remove("done", "error");
  cancelled = false;
  conflictPromise = null;
  conflictResolve = null;
  conflictOverlay.classList.remove("visible");
  statBytes.textContent = "0 B";
  statFiles.textContent = "0";
  statScanned.textContent = "0";
  statSkipped.textContent = "0";
  stateBadge.textContent = "ACTIVE";
  stateBadge.className = "badge active";
  btnAbort.disabled = true;
  fileInput.value = "";
  folderInput.value = "";
});
