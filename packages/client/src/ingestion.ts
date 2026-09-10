import { MfupError, type Entry, type Source } from "./types.js";

/** Files stay native Files. No body reads, full directory materialization or binary framing. */
export async function* fromFiles(
  files: ArrayLike<File>,
): AsyncGenerator<Entry> {
  for (let i = 0; i < files.length; i++) {
    const file = files[i],
      name = (file.webkitRelativePath || file.name).normalize("NFC");
    yield { kind: "file", path: name, file };
  }
}
export async function* fromHandles(
  handles: Iterable<FileSystemHandle>,
  parent = "",
): AsyncGenerator<Entry> {
  for (const handle of handles) {
    const name = parent + handle.name.normalize("NFC");
    if (handle.kind === "file")
      yield {
        kind: "file",
        path: name,
        file: await (handle as FileSystemFileHandle).getFile(),
      };
    else {
      yield { kind: "directory", path: name };
      const dir = handle as FileSystemDirectoryHandle & {
        values(): AsyncIterable<FileSystemHandle>;
      };
      for await (const child of dir.values())
        yield* fromHandles([child], name + "/");
    }
  }
}
export async function* fromEntries(
  entries: Iterable<FileSystemEntry>,
  parent = "",
): AsyncGenerator<Entry> {
  for (const entry of entries) {
    const name = parent + entry.name.normalize("NFC");
    if (entry.isFile) {
      const file = await new Promise<File>((resolve, reject) =>
        (entry as FileSystemFileEntry).file(resolve, reject),
      );
      yield { kind: "file", path: name, file };
    } else {
      yield { kind: "directory", path: name };
      const reader = (entry as FileSystemDirectoryEntry).createReader();
      while (true) {
        const page = await new Promise<FileSystemEntry[]>((resolve, reject) =>
          reader.readEntries(resolve, reject),
        );
        if (!page.length) break;
        // readEntries is paginated by the browser; each page is consumed before the next.
        yield* fromEntries(page, name + "/");
      }
    }
  }
}
export function sourceFromInput(input: HTMLInputElement): Source {
  const files = input.files;
  return (async function* () {
    try {
      yield* fromFiles(files ?? []);
    } finally {
      if (input.files === files) input.value = "";
    }
  })();
}
/** Invoke synchronously inside drop; browser-owned DataTransfer items expire afterwards. */
export function sourceFromDataTransfer(data: DataTransfer): Source {
  if (data.items.length > 10000)
    throw new MfupError(
      "selection_too_large",
      "Drop at most 10000 roots; directories are read incrementally",
    );
  const items = Array.from(data.items ?? []).filter(
    (item) => item.kind === "file",
  );
  if (items.length && items.every((item) => "getAsFileSystemHandle" in item)) {
    const handles = items.map((item) =>
      (
        item as DataTransferItem & {
          getAsFileSystemHandle(): Promise<FileSystemHandle | null>;
        }
      ).getAsFileSystemHandle(),
    );
    return (async function* () {
      yield* fromHandles(
        (await Promise.all(handles)).filter(
          (x): x is FileSystemHandle => x !== null,
        ),
      );
    })();
  }
  const entries = items
    .map((item) => item.webkitGetAsEntry?.())
    .filter((x): x is FileSystemEntry => !!x);
  return entries.length ? fromEntries(entries) : fromFiles(data.files);
}
