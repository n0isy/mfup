# @mfup/react

React hooks for MFUP/2 resumable multi-file uploads
([`@mfup/client`](https://www.npmjs.com/package/@mfup/client)).

```bash
npm install @mfup/react @mfup/client react
```

## Usage

```tsx
import { MfupProvider, useMfupUpload, useMfupDropzone } from "@mfup/react";

function Uploader() {
  const { snapshot, pendingAsks, busy, start, abort } = useMfupUpload({
    targetDir: "incoming",
    onPublished: (files) => toast(`Uploaded ${files.length} entries`),
  });
  const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
    disabled: busy,
    onSource: start,
  });

  return (
    <div {...getRootProps()} className={isDragActive ? "drop active" : "drop"}>
      <input {...getInputProps({ directory: true })} />
      <progress value={(snapshot?.fraction ?? 0) * 100} max={100} />
      {pendingAsks.map((ask) => (
        <ConfirmDialog key={ask.id} name={ask.name}
          onYes={() => ask.respond("merge_overwrite")}
          onNo={() => ask.respond("cancel")} />
      ))}
      {busy && <button onClick={abort}>Cancel</button>}
    </div>
  );
}

// App root:
<MfupProvider config={{ serverUrl: "/api/uploads" }}>
  <Uploader />
</MfupProvider>
```

## Design

- State comes through `useSyncExternalStore` over the client's coalesced
  snapshot store — tear-free under concurrent rendering, one subscription
  per component, no re-render storms from 50k-file progress ticks.
- Sessions are created in event handlers (never effects) — StrictMode-safe.
- Uploads survive route changes by default (`abortOnUnmount: false`).
- `autoPublish` (default on) publishes after commit once every server
  question is answered — and never after a "cancel".
- `useMfupDropzone` wraps the browser DnD priority chain
  (`getAsFileSystemHandle` → `webkitGetAsEntry` → files) which must run
  synchronously in the drop handler.

Peer deps: `react >= 18`, `@mfup/client`.

Docs: <https://github.com/n0isy/mfup>
