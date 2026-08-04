// MFUP/2 ingestion — unified interface for drag-and-drop, <input webkitdirectory>,
// and File System Access API pickers. Each adapter yields lightweight metadata
// and a lazy body accessor.

import { NodeKind } from "./protocol.js";

// ---------------------------------------------------------------------------
// Portable file reading — async generator (avoids ReadableStream entirely)
// ---------------------------------------------------------------------------

const READ_SLICE_SIZE = 65536; // 64 KiB slices

/**
 * Yield Uint8Array chunks from a Blob using slice + arrayBuffer.
 *
 * Uses an async generator instead of ReadableStream to avoid Firefox issues:
 * - `Blob.stream()` → "Error in input stream" for multi-chunk files
 * - `ReadableStream({ pull })` → "The operation was aborted" in some cases
 *
 * `Blob.slice()` + `arrayBuffer()` is the most universally reliable path.
 */
async function* blobChunks(blob: Blob, offset = 0): AsyncGenerator<Uint8Array> {
  let pos = offset;
  const size = blob.size;
  while (pos < size) {
    const end = Math.min(pos + READ_SLICE_SIZE, size);
    const buf = await blob.slice(pos, end).arrayBuffer();
    yield new Uint8Array(buf);
    pos = end;
  }
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

/** Lightweight metadata discovered during scan. */
export interface DiscoveredNode {
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  name: string;
  /** For files — exact size. For dirs — null. */
  size: bigint | null;
  mtimeMs: bigint | null;
  /**
   * For files: returns an async iterable of body chunks.
   * Callers may pass a byte offset to resume mid-file (implemented via slice).
   * For directories this is null.
   */
  openBody: ((offsetBytes?: number) => AsyncIterable<Uint8Array>) | null;
}

/** Called for each discovered node during ingestion. */
export type IngestCallback = (node: DiscoveredNode) => void | Promise<void>;

/** Controls ingestion: check before descending / emitting. */
export interface IngestFilter {
  /** Return false to prune this directory subtree. */
  shouldDescend?(nodeId: number, name: string): boolean;
  /** Return false to skip this file entirely. */
  shouldInclude?(nodeId: number, name: string): boolean;
}

// ---------------------------------------------------------------------------
// ID allocator
// ---------------------------------------------------------------------------
export class NodeIdAllocator {
  private next = 1; // 0 is root
  alloc(): number { return this.next++; }
  get current(): number { return this.next - 1; }
}

// ---------------------------------------------------------------------------
// Adapter: FileSystemHandle (drag-and-drop or picker)
// ---------------------------------------------------------------------------

/**
 * Ingest from an array of FileSystemHandle roots.
 * Call `getAsFileSystemHandle()` synchronously in the drop handler, then pass
 * handles here.
 */
export async function ingestFromHandles(
  handles: FileSystemHandle[],
  rootParentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  for (const h of handles) {
    await walkHandle(h, rootParentId, ids, cb, filter);
  }
}

async function walkHandle(
  handle: FileSystemHandle,
  parentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  const nodeId = ids.alloc();

  if (handle.kind === "directory") {
    if (filter?.shouldDescend && !filter.shouldDescend(nodeId, handle.name)) return;

    await cb({
      nodeId, parentId, kind: NodeKind.DIR, name: handle.name,
      size: null, mtimeMs: null, openBody: null,
    });

    const dirHandle = handle as FileSystemDirectoryHandle;
    for await (const [, child] of (dirHandle as any).entries()) {
      await walkHandle(child as FileSystemHandle, nodeId, ids, cb, filter);
    }
  } else {
    if (filter?.shouldInclude && !filter.shouldInclude(nodeId, handle.name)) return;

    const fileHandle = handle as FileSystemFileHandle;
    // Fetch metadata lazily — only need size + mtime for the NODE frame.
    const file = await fileHandle.getFile();
    const size = BigInt(file.size);
    const mtimeMs = BigInt(file.lastModified);

    await cb({
      nodeId, parentId, kind: NodeKind.FILE, name: handle.name,
      size, mtimeMs,
      openBody: (offset = 0) => {
        // Re-get the file to avoid holding a stale reference across resumes.
        // For the initial pass we can use the already-fetched `file`.
        return blobChunks(file, offset);
      },
    });
  }
}

// ---------------------------------------------------------------------------
// Adapter: File list from <input type="file" webkitdirectory>
// ---------------------------------------------------------------------------

/**
 * Ingest from a FileList obtained via `<input webkitdirectory>`.
 *
 * The File objects carry `webkitRelativePath` which we split to reconstruct
 * the directory tree.
 */
export async function ingestFromFileList(
  files: ArrayLike<File>,
  rootParentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  // Build a virtual tree from relative paths.
  interface VNode {
    nodeId: number;
    name: string;
    children: Map<string, VNode>;
    file: File | null;
  }

  const root: VNode = { nodeId: -1, name: "", children: new Map(), file: null };

  // First pass: build tree structure and allocate IDs
  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const rel = (file as any).webkitRelativePath as string;
    const parts = rel.split("/");
    let cur = root;
    for (let p = 0; p < parts.length; p++) {
      const seg = parts[p];
      let child = cur.children.get(seg);
      if (!child) {
        child = { nodeId: ids.alloc(), name: seg, children: new Map(), file: null };
        cur.children.set(seg, child);
      }
      if (p === parts.length - 1) {
        child.file = file;
      }
      cur = child;
    }
  }

  // Second pass: emit nodes DFS
  async function emit(vnode: VNode, parentId: number): Promise<void> {
    if (vnode.file) {
      if (filter?.shouldInclude && !filter.shouldInclude(vnode.nodeId, vnode.name)) return;
      const f = vnode.file;
      await cb({
        nodeId: vnode.nodeId, parentId, kind: NodeKind.FILE, name: vnode.name,
        size: BigInt(f.size), mtimeMs: BigInt(f.lastModified),
        openBody: (offset = 0) => {
          return blobChunks(f, offset);
        },
      });
    } else {
      if (filter?.shouldDescend && !filter.shouldDescend(vnode.nodeId, vnode.name)) return;
      await cb({
        nodeId: vnode.nodeId, parentId, kind: NodeKind.DIR, name: vnode.name,
        size: null, mtimeMs: null, openBody: null,
      });
      for (const child of vnode.children.values()) {
        await emit(child, vnode.nodeId);
      }
    }
  }

  for (const child of root.children.values()) {
    await emit(child, rootParentId);
  }
}

// ---------------------------------------------------------------------------
// Adapter: FileSystemEntry (webkitGetAsEntry — Firefox/Safari drag-and-drop)
//
// Unlike getAsFileSystemHandle() (Chrome-only), webkitGetAsEntry() is
// supported in Firefox, Chrome, and Safari. The returned FileSystemEntry
// objects persist after the drop handler returns (they are NOT tied to the
// DataTransfer lifecycle), so files can be read lazily during streaming.
// ---------------------------------------------------------------------------

/**
 * Ingest from FileSystemEntry roots obtained via webkitGetAsEntry().
 * Must be called synchronously in the drop handler; entries persist after.
 */
export async function ingestFromEntries(
  entries: FileSystemEntry[],
  rootParentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  for (const entry of entries) {
    await walkEntry(entry, rootParentId, ids, cb, filter);
  }
}

async function walkEntry(
  entry: FileSystemEntry,
  parentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  const nodeId = ids.alloc();

  if (entry.isDirectory) {
    if (filter?.shouldDescend && !filter.shouldDescend(nodeId, entry.name)) return;

    await cb({
      nodeId, parentId, kind: NodeKind.DIR, name: entry.name,
      size: null, mtimeMs: null, openBody: null,
    });

    const dirEntry = entry as FileSystemDirectoryEntry;
    const children = await readAllEntries(dirEntry);
    for (const child of children) {
      await walkEntry(child, nodeId, ids, cb, filter);
    }
  } else {
    if (filter?.shouldInclude && !filter.shouldInclude(nodeId, entry.name)) return;

    const fileEntry = entry as FileSystemFileEntry;
    const file = await entryGetFile(fileEntry);
    const size = BigInt(file.size);
    const mtimeMs = BigInt(file.lastModified);

    await cb({
      nodeId, parentId, kind: NodeKind.FILE, name: entry.name,
      size, mtimeMs,
      openBody: (offset = 0) => blobChunks(file, offset),
    });
  }
}

/**
 * Read all entries from a directory.
 * readEntries() may not return all results in one call — must loop until empty.
 */
function readAllEntries(dir: FileSystemDirectoryEntry): Promise<FileSystemEntry[]> {
  return new Promise((resolve, reject) => {
    const reader = dir.createReader();
    const all: FileSystemEntry[] = [];

    function readBatch() {
      reader.readEntries(
        (batch) => {
          if (batch.length === 0) {
            resolve(all);
          } else {
            all.push(...batch);
            readBatch();
          }
        },
        reject,
      );
    }

    readBatch();
  });
}

/** Promisify FileSystemFileEntry.file() callback API. */
function entryGetFile(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

// ---------------------------------------------------------------------------
// Adapter: explicit File array (e.g. from file picker without directories)
// ---------------------------------------------------------------------------

export async function ingestFromFiles(
  files: File[],
  rootParentId: number,
  ids: NodeIdAllocator,
  cb: IngestCallback,
  filter?: IngestFilter,
): Promise<void> {
  for (const file of files) {
    const nodeId = ids.alloc();
    if (filter?.shouldInclude && !filter.shouldInclude(nodeId, file.name)) continue;
    await cb({
      nodeId, parentId: rootParentId, kind: NodeKind.FILE, name: file.name,
      size: BigInt(file.size), mtimeMs: BigInt(file.lastModified),
      openBody: (offset = 0) => {
        return blobChunks(file, offset);
      },
    });
  }
}
