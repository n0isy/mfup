/**
 * MFUP/2 React demo — the whole upload UX in ~100 lines of JSX, built only
 * on the public @mfup/react surface. Doubles as the e2e smoke target for the
 * react package (data-testid attributes).
 */

import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { MfupProvider, useMfupDropzone, useMfupUpload } from "@mfup/react";

function fmtBytes(n: bigint | number): string {
  const v = Number(n);
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / (1024 * 1024)).toFixed(1)} MB`;
}

function UploadApp() {
  const [log, setLog] = useState<string[]>([]);
  const push = (m: string) => setLog((l) => [...l.slice(-49), m]);

  const { snapshot, pendingAsks, busy, start, abort, reset } = useMfupUpload({
    targetDir: ".",
    meta: { app: "react-demo" },
    onCommitted: (ev) => push(`committed: ${ev.files} files, ${fmtBytes(ev.bytes)}`),
    onPublished: (files) => push(`published: ${files.join(", ")}`),
    onError: (e) => { if (e.fatal) push(`fatal: [${e.code}] ${e.message}`); },
  });

  const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
    disabled: busy,
    onSource: (src) => {
      push(`source: ${src.kind}`);
      start(src).catch((e) => push(`start failed: ${e?.message ?? e}`));
    },
  });

  const pct = snapshot?.fraction != null ? Math.round(snapshot.fraction * 100) : 0;

  return (
    <div className="react-demo">
      <h1>MFUP/2 · @mfup/react</h1>

      <div
        {...getRootProps()}
        className={"dropzone" + (isDragActive ? " dragover" : "")}
        data-testid="dropzone"
      >
        <p>Drop files or folders here</p>
        <label>Pick files<input data-testid="file-input" {...getInputProps()} /></label>
        <label>Pick folder<input data-testid="folder-input" {...getInputProps({ directory: true })} /></label>
      </div>

      <div>
        state: <span className="badge" data-testid="state">{snapshot?.state ?? "idle"}</span>
        {snapshot?.reconnect && (
          <span data-testid="reconnect"> · reconnecting #{snapshot.reconnect.attempt}</span>
        )}
      </div>

      <progress max={100} value={pct} data-testid="bar" />
      <div data-testid="stats">
        {snapshot
          ? `${snapshot.progress.acceptedFiles} files · ${fmtBytes(snapshot.progress.bodyDoneBytes)}`
          + (snapshot.currentFile ? ` · sending ${snapshot.currentFile.path}` : "")
          : "idle"}
      </div>
      <div data-testid="published">
        {snapshot?.published ? snapshot.published.join(", ") : ""}
      </div>

      {pendingAsks.map((ask) => (
        <div className="ask" key={ask.id} data-testid="ask">
          <span>“{ask.name ?? "target"}” already exists. Overwrite?</span>
          <button data-testid="ask-overwrite" onClick={() => ask.respond("merge_overwrite")}>Overwrite</button>
          <button data-testid="ask-cancel" onClick={() => ask.respond("cancel")}>Cancel</button>
        </div>
      ))}

      <div>
        {busy && <button data-testid="abort" onClick={abort}>Abort</button>}
        {!busy && snapshot && <button data-testid="reset" onClick={reset}>Reset</button>}
      </div>

      <pre data-testid="log">{log.join("\n")}</pre>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MfupProvider config={{ serverUrl: location.origin }}>
      <UploadApp />
    </MfupProvider>
  </StrictMode>,
);
