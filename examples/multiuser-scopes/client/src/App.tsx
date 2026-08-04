/**
 * Three drop zones — workspace / scratch / uploads — each doubling as a
 * plain viewer of that zone's top-level server directory. Built ONLY on the
 * published @mfup/react surface.
 *
 * What each zone demonstrates besides the transfer itself:
 *   - the INTERACTIVE conflict dialog: drop the same folder twice and the
 *     server ASKs mid-flight — the transfer keeps running while you decide;
 *     "No" cancels the whole session (nothing is published), "Overwrite"
 *     publishes over the old files;
 *   - cancelling a running upload (abort);
 *   - live status from the snapshot store: state, reconnect attempts, the
 *     file currently streaming, accepted bytes/files.
 *
 * The scope travels as session meta; the SERVER decides the final layout
 * (data/<user_id>/<scope>/...).
 */

import { useCallback, useEffect, useState } from "react";
import { MfupProvider, useMfupDropzone, useMfupUpload } from "@mfup/react";

const SCOPES = ["workspace", "scratch", "uploads"] as const;
type Scope = (typeof SCOPES)[number];

interface Entry {
  name: string;
  dir: boolean;
  size: number | null;
}

function fmtBytes(n: number | bigint): string {
  const v = Number(n);
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / (1024 * 1024)).toFixed(1)} MB`;
}

function ScopeZone({ scope }: { scope: Scope }) {
  const [entries, setEntries] = useState<Entry[] | null>(null);

  const refresh = useCallback(async () => {
    try {
      const r = await fetch(`/api/files/${scope}`);
      if (r.ok) setEntries((await r.json()).entries);
    } catch {
      /* transient (dev-server restart etc.) — keep the last listing */
    }
  }, [scope]);
  useEffect(() => { void refresh(); }, [refresh]);

  const { snapshot, pendingAsks, busy, start, abort } = useMfupUpload({
    meta: { scope },                 // → AuthRequest.meta on the server
    onPublished: () => void refresh(),
  });

  const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
    disabled: busy,
    onSource: (src) => { start(src).catch(() => {/* surfaced via snapshot */}); },
  });

  const st = snapshot?.state;
  const pct = snapshot?.fraction != null ? Math.round(snapshot.fraction * 100) : 0;
  const cancelled = st === "aborted";
  const failed = st === "failed";
  const done = st === "committed" && snapshot?.published != null;

  return (
    <section className={"zone" + (isDragActive ? " drag" : "")} {...getRootProps()}>
      <header>
        <h2>{scope}</h2>
        <label className="pick">files<input {...getInputProps()} /></label>
        <label className="pick">folder<input {...getInputProps({ directory: true })} /></label>
      </header>

      {/* live transfer status, straight from the snapshot store */}
      {busy && (
        <div className="status">
          <div className="progress"><div style={{ width: `${pct}%` }} /></div>
          <div className="statline">
            <span className="badge">{snapshot?.reconnect
              ? `reconnecting #${snapshot.reconnect.attempt}`
              : st}</span>
            <span className="counts">
              {snapshot ? `${snapshot.progress.acceptedFiles} files · ${fmtBytes(snapshot.progress.bodyDoneBytes)}` : ""}
            </span>
            <button className="abort" onClick={abort}>Cancel upload</button>
          </div>
          {snapshot?.currentFile && (
            <div className="cur" title={snapshot.currentFile.path}>
              ↑ {snapshot.currentFile.path}
            </div>
          )}
        </div>
      )}

      {/* the interactive part: the server asks, the transfer keeps running */}
      {pendingAsks.map((ask) => (
        <div className="ask" key={ask.id}>
          <p>
            <b>&ldquo;{ask.name ?? "entry"}&rdquo; already exists here.</b><br />
            The upload is still streaming while you decide.
          </p>
          <div>
            <button onClick={() => ask.respond("merge_overwrite")}>Overwrite</button>
            <button className="no" onClick={() => ask.respond("cancel")}>
              No — cancel everything
            </button>
          </div>
        </div>
      ))}

      {done && <div className="note ok">✓ published {snapshot!.published!.length} top-level entr{snapshot!.published!.length === 1 ? "y" : "ies"}</div>}
      {cancelled && <div className="note warn">upload cancelled — nothing was published, the old files are untouched</div>}
      {failed && snapshot?.fatalError && (
        <div className="note err">[{snapshot.fatalError.code}] {snapshot.fatalError.message}</div>
      )}

      <ul className="listing">
        {entries === null && <li className="hint">loading…</li>}
        {entries?.length === 0 && <li className="hint">empty — drop files or a folder here</li>}
        {entries?.map((e) => (
          <li key={e.name}>
            <span>{e.dir ? "📁" : "📄"} {e.name}</span>
            {e.size != null && <span className="size">{fmtBytes(e.size)}</span>}
          </li>
        ))}
      </ul>

      <footer>
        Top-level listing straight from the server&rsquo;s disk (a demo
        viewer, not a file manager). Tip: drop the <b>same folder twice</b>
        to see the mid-transfer overwrite dialog.
      </footer>
    </section>
  );
}

export default function App() {
  const [uid, setUid] = useState<string | null>(null);

  useEffect(() => {
    // First visit auto-creates the user: the server mints an id cookie.
    fetch("/api/whoami")
      .then((r) => r.json())
      .then((d) => setUid(d.user_id))
      .catch(() => setUid("(server unreachable — is examples/…/server running?)"));
  }, []);

  if (!uid) return <p className="boot">creating your demo user…</p>;

  return (
    <MfupProvider config={{ serverUrl: `${location.origin}/api/mfup` }}>
      <h1>MFUP/2 · multiuser scopes</h1>
      <p className="who">
        You are <code>{uid}</code> (auto-created via cookie). Everything you
        drop lands under <code>data/{uid}/&lt;scope&gt;/</code> on the server
        — the scope travels as session meta, the layout is the server&rsquo;s
        decision.
      </p>
      <div className="zones">
        {SCOPES.map((s) => <ScopeZone key={s} scope={s} />)}
      </div>
    </MfupProvider>
  );
}
