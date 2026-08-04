// MFUP/2 drop-source extraction — framework-free helpers that turn browser
// input events (drag-and-drop, <input type=file>) into something
// MfupSession.upload() accepts.
//
// Browser rules this encodes (learned the hard way, see demo history):
//   1. getAsFileSystemHandle()  — Chrome/Edge; persistent handles.
//   2. webkitGetAsEntry()       — Firefox/Safari; persistent FileSystemEntry.
//   3. DataTransfer.files       — last resort; Firefox invalidates the blobs
//      after the drop handler returns, so lazy streaming may fail.
// Both (1) and (2) MUST be called synchronously inside the drop handler —
// the DataTransferItem list is dead by the next microtask. That is why
// sourceFromDataTransfer is synchronous and returns unresolved promises for
// the handle path.

/** A normalised upload source, accepted by MfupSession.upload().
 * "filelist" preserves the directory tree via webkitRelativePath;
 * "files" is a flat list. */
export type UploadSource =
  | { kind: "handles"; handles: Promise<FileSystemHandle[]> }
  | { kind: "entries"; entries: FileSystemEntry[] }
  | { kind: "filelist"; files: ArrayLike<File> }
  | { kind: "files"; files: File[] };

/**
 * Extract an upload source from a drop event's DataTransfer.
 *
 * MUST be called synchronously in the `drop` handler. Returns null when the
 * DataTransfer carries no files.
 */
export function sourceFromDataTransfer(dt: DataTransfer): UploadSource | null {
  const items = dt.items;
  if (!items || items.length === 0) {
    const files = Array.from(dt.files ?? []);
    return files.length > 0 ? { kind: "files", files } : null;
  }

  // 1. File System Access API (Chrome/Edge). All items must support it —
  // a mixed list means an unsupported browser quirk; fall through.
  const handlePromises: Promise<FileSystemHandle | null>[] = [];
  let allHandles = true;
  for (let i = 0; i < items.length; i++) {
    const p = (items[i] as any).getAsFileSystemHandle?.();
    if (p) handlePromises.push(p);
    else { allHandles = false; break; }
  }
  if (allHandles && handlePromises.length > 0) {
    return {
      kind: "handles",
      handles: Promise.all(handlePromises).then(
        (hs) => hs.filter((h): h is FileSystemHandle => h != null),
      ),
    };
  }

  // 2. webkitGetAsEntry (Firefox/Safari) — persistent, supports directories.
  const entries: FileSystemEntry[] = [];
  for (let i = 0; i < items.length; i++) {
    const entry = items[i].webkitGetAsEntry?.();
    if (entry) entries.push(entry);
  }
  if (entries.length > 0) {
    return { kind: "entries", entries };
  }

  // 3. Fallback: plain File objects.
  const files = Array.from(dt.files ?? []);
  return files.length > 0 ? { kind: "files", files } : null;
}

/**
 * Extract an upload source from a file input's change event.
 * Handles both plain <input type=file multiple> and <input webkitdirectory>.
 */
export function sourceFromInput(input: HTMLInputElement): UploadSource | null {
  const files = input.files;
  if (!files || files.length === 0) return null;
  // Copy out of the live FileList so the caller may reset input.value
  // immediately; File objects (and their webkitRelativePath) stay valid.
  const copy = Array.from(files);
  // webkitdirectory inputs carry webkitRelativePath — preserve the tree.
  const isDirectory = (input as any).webkitdirectory
    || ((copy[0] as any).webkitRelativePath ?? "") !== "";
  if (isDirectory) {
    return { kind: "filelist", files: copy };
  }
  return { kind: "files", files: copy };
}
