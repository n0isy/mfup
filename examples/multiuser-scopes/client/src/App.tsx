/**
 * Three drop zones — workspace / scratch / uploads — each doubling as a
 * plain viewer of that zone's top-level server directory. Built ONLY on the
 * published @mfup/react surface: one useMfupUpload + one useMfupDropzone
 * per zone; the scope travels as session meta and the SERVER decides the
 * final layout (data/<user_id>/<scope>/...).
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

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function ScopeZone({ scope }: { scope: Scope }) {
  const [entries, setEntries] = useState<Entry[] | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const r = await fetch(`/api/files/${scope}`);
      if (r.ok) setEntries((await r.json()).entries);
    } catch {
      /* transient (dev-server restart etc.) — keep the last listing */
    }
  }, [scope]);
  useEffect(() => { void refresh(); }, [refresh]);

  const { snapshot, pendingAsks, busy, start } = useMfupUpload({
    meta: { scope },                 // → AuthRequest.meta on the server
    onPublished: () => { setLastError(null); void refresh(); },
    onError: (e) => { if (e.fatal) setLastError(`[${e.code}] ${e.message}`); },
  });

  const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
    disabled: busy,
    onSource: (src) => {
      start(src).catch((e) => setLastError(e?.message ?? String(e)));
    },
  });

  const pct = snapshot?.fraction != null ? Math.round(snapshot.fraction * 100) : 0;

  return (
    <section className={"zone" + (isDragActive ? " drag" : "")} {...getRootProps()}>
      <header>
        <h2>{scope}</h2>
        <label className="pick">files<input {...getInputProps()} /></label>
        <label className="pick">folder<input {...getInputProps({ directory: true })} /></label>
      </header>

      {busy && (
        <div className="progress" title={snapshot?.currentFile?.path}>
          <div style={{ width: `${pct}%` }} />
        </div>
      )}

      {pendingAsks.map((ask) => (
        <div className="ask" key={ask.id}>
          <span>&ldquo;{ask.name ?? "entry"}&rdquo; exists — overwrite?</span>
          <button onClick={() => ask.respond("merge_overwrite")}>Yes</button>
          <button className="no" onClick={() => ask.respond("cancel")}>No</button>
        </div>
      ))}

      {lastError && <div className="error">{lastError}</div>}

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
        Top-level listing straight from the server&rsquo;s disk — a demo
        viewer, not a file manager.
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
