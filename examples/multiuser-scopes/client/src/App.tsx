/// <reference types="vite/client" />
import { errorMessage, failureMessage } from "./errors";
import { useCallback, useEffect, useRef, useState } from "react";
import { useMfupUpload } from "@mfup/react";
import {
  sourceFromDataTransfer,
  sourceFromInput,
  type Source,
  type Ticket,
} from "@mfup/client";

const API = new URL(import.meta.env.BASE_URL + "api", location.origin).href;
const SCOPES = ["workspace", "scratch", "uploads"] as const;
type Scope = (typeof SCOPES)[number];
interface Entry {
  name: string;
  dir: boolean;
  size: number | null;
}
interface Identity {
  user_id: string;
  scopes: Scope[];
  backend: string;
}
let identityRequest: Promise<Identity> | undefined;
function whoami() {
  // StrictMode and rerenders share the initial request, so only one cookie is minted.
  return (identityRequest ??= fetch(API + "/whoami")
    .then(async (response) => {
      if (!response.ok) throw new Error("Could not open the session");
      return response.json();
    })
    .catch((error) => {
      identityRequest = undefined;
      throw error;
    }));
}
function readTicket(key: string): Ticket | undefined {
  try {
    return JSON.parse(localStorage.getItem(key) ?? "null") ?? undefined;
  } catch {
    return undefined;
  }
}
const bytes = (n: number) =>
  n < 1024
    ? `${n} B`
    : n < 1048576
      ? `${(n / 1024).toFixed(1)} KiB`
      : `${(n / 1048576).toFixed(1)} MiB`;
const labels: Record<string, string> = {
  idle: "Ready to upload",
  connecting: "Connecting",
  uploading: "Uploading",
  paused: "Pause",
  waiting: "Your approval is needed",
  committed: "Publishing",
  published: "Upload complete",
  cancelling: "Waiting for cancellation…",
  cancelled: "Upload cancelled",
  failed: "Upload stopped",
};

function ScopeZone({ scope, uid }: { scope: Scope; uid: string }) {
  const key = `mfup3:${API}:${uid}:${scope}`;
  const [ticket, setTicket] = useState(() => readTicket(key));
  const [entries, setEntries] = useState<Entry[] | null>(null);
  const [folder, setFolder] = useState("");
  const [after, setAfter] = useState("");
  const [nextPage, setNextPage] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [drag, setDrag] = useState(false);
  const listingVersion = useRef(0);
  const refresh = useCallback(async () => {
    const version = ++listingVersion.current;
    try {
      const response = await fetch(
        `${API}/files/${scope}?path=${encodeURIComponent(folder)}&after=${encodeURIComponent(after)}`,
      );
      if (!response.ok) throw new Error("Could not refresh the file list");
      const data = await response.json();
      if (version === listingVersion.current) {
        setEntries(data.entries);
        setNextPage(data.next ?? null);
      }
    } catch (e) {
      if (version === listingVersion.current) setError((e as Error).message);
    }
  }, [scope, folder, after]);
  useEffect(() => {
    void refresh();
    return () => {
      listingVersion.current++;
    };
  }, [refresh]);

  const {
    session,
    snapshot,
    start,
    setOverwrite,
    pause,
    resume,
    retry,
    cancel,
  } = useMfupUpload({
    serverUrl: API,
    meta: { scope },
    ticket,
    trackUploadProgress: true,
  });
  const state = snapshot?.state ?? "idle";
  const busy = [
    "connecting",
    "uploading",
    "paused",
    "waiting",
    "committed",
    "cancelling",
    "failed",
  ].includes(state);
  useEffect(() => {
    if (["published", "cancelled"].includes(state)) {
      setError("");
      localStorage.removeItem(key);
      setTicket(undefined);
    } else if (session?.ticket) {
      const saved = session.exportTicket();
      localStorage.setItem(key, JSON.stringify(saved));
      setTicket(saved);
    }
  }, [session, snapshot, key, state]);
  useEffect(() => {
    if (state === "published") void refresh();
  }, [state, refresh]);
  async function upload(source: Source) {
    setError("");
    try {
      await start(source);
    } catch (e) {
      if (
        (e as { code?: string }).code !== "cancelled" &&
        (e as Error).name !== "AbortError"
      )
        setError(errorMessage(e));
    }
  }
  function picked(input: HTMLInputElement) {
    const source = sourceFromInput(input);
    void upload(source);
  }
  const total = snapshot?.totalBytes ?? 0;
  const sent = snapshot?.sentBytes ?? 0;
  const confirmed = snapshot?.confirmedBytes ?? 0;
  const percent =
    state === "published"
      ? 100
      : total
        ? Math.min(99, (100 * sent) / total)
        : 0;
  const confirmedPercent = total
    ? Math.min(percent, (100 * confirmed) / total)
    : percent;
  const reportError = (e: unknown) => setError(errorMessage(e));
  const hasProcessingError =
    snapshot?.processing === "failed" &&
    !["cancelled", "published"].includes(state);
  const displayError = snapshot?.errorInfo
    ? failureMessage(snapshot.errorInfo)
    : hasProcessingError
      ? failureMessage({ code: "processing_failed", phase: "commit" })
      : error;
  const [approving, setApproving] = useState(false);
  async function approveOverwrite() {
    setApproving(true);
    setError("");
    try {
      await setOverwrite(true);
    } catch (e) {
      if (
        !["cancelling", "cancelled", "published"].includes(
          session?.getSnapshot().state ?? "",
        )
      )
        reportError(e);
    } finally {
      setApproving(false);
    }
  }
  return (
    <section
      data-testid={`zone-${scope}`}
      className={"zone" + (drag ? " drag" : "")}
      onDragOver={(event) => {
        event.preventDefault();
        if (!busy) setDrag(true);
      }}
      onDragLeave={() => setDrag(false)}
      onDrop={(event) => {
        event.preventDefault();
        setDrag(false);
        if (!busy) void upload(sourceFromDataTransfer(event.dataTransfer));
      }}
    >
      <header>
        <h2>{scope}</h2>
        <div className="pickers">
          <label className="pick">
            Files
            <input
              aria-label={`Files ${scope}`}
              type="file"
              multiple
              disabled={busy}
              onChange={(event) => picked(event.target)}
            />
          </label>
          <label className="pick">
            Folder
            <input
              aria-label={`Folder ${scope}`}
              type="file"
              multiple
              {...{ webkitdirectory: "" }}
              disabled={busy}
              onChange={(event) => picked(event.target)}
            />
          </label>
        </div>
      </header>
      <div className="status">
        <div className="statline">
          <span data-testid="status" className="badge">
            {hasProcessingError
              ? "Server processing failed"
              : snapshot?.processing === "running"
                ? "Server processing"
                : state === "committed"
                  ? "Awaiting publication"
                  : labels[state]}
          </span>
          <span className="counts">{snapshot?.discovered ?? 0} files</span>
        </div>
        <div
          className="progress"
          role="progressbar"
          aria-label={`Progress ${scope}`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.floor(percent)}
        >
          <div className="sent" style={{ width: `${percent}%` }} />
          <div
            className="confirmed"
            style={{ width: `${confirmedPercent}%` }}
          />
        </div>
        <div className="counts" data-testid="bytes">
          {bytes(sent)} sent · {bytes(confirmed)} confirmed
          {total ? ` of ${bytes(total)}` : ""}
        </div>
        {busy && (
          <div className="statline">
            <span className="counts">
              {snapshot?.scanDone
                ? `${snapshot.activeRequests} of 6 requests`
                : "Scanning folders…"}
            </span>
            {state === "paused" ? (
              <button onClick={() => void resume()?.catch(reportError)}>
                Resume
              </button>
            ) : (
              <button disabled={state !== "uploading"} onClick={pause}>
                Pause
              </button>
            )}
            <button
              className="abort"
              disabled={state === "cancelling"}
              onClick={() => void cancel()?.catch(reportError)}
            >
              Cancel
            </button>
          </div>
        )}
      </div>
      {snapshot?.overwriteRequired &&
        !displayError &&
        !["cancelling", "cancelled", "failed"].includes(state) && (
          <div className="ask" data-testid="overwrite-prompt" role="status">
            <p>
              <b>Allow overwriting existing files?</b>
              <br />
              Approval applies to this entire upload, including files that are
              still being transferred.
            </p>
            <p>
              {["committed", "waiting"].includes(state)
                ? "The files were received. Publication needs your approval."
                : "Files are replaced only when the upload is published."}
            </p>
            <div className="actions">
              <button
                disabled={approving}
                onClick={() => void approveOverwrite()}
              >
                Allow overwrite
              </button>
              <button
                className="no"
                onClick={() => void cancel()?.catch(reportError)}
              >
                Cancel upload
              </button>
            </div>
          </div>
        )}
      {ticket && !session && !busy && (
        <div className="note warn">
          An unfinished upload is saved. Select the same files to resume.
          <button
            onClick={() => {
              localStorage.removeItem(key);
              setTicket(undefined);
            }}
          >
            Start new upload
          </button>
        </div>
      )}
      {displayError && (
        <div className="note err" role="alert">
          {displayError}
          {state === "failed" && snapshot?.errorInfo?.phase !== "cancel" && (
            <button
              onClick={() => {
                setError("");
                void retry()?.catch(reportError);
              }}
            >
              Retry upload
            </button>
          )}
          {ticket && state === "failed" && (
            <button
              onClick={() => {
                setError("");
                void cancel()?.catch(reportError);
              }}
            >
              {snapshot?.errorInfo?.phase === "cancel"
                ? "Retry cancellation"
                : "Cancel upload"}
            </button>
          )}
        </div>
      )}
      <nav className="folder">
        <button
          disabled={!folder}
          onClick={() => {
            setFolder(folder.split("/").slice(0, -1).join("/"));
            setEntries(null);
            setAfter("");
          }}
        >
          ↑ Back
        </button>
        <span>{folder || "My files"}</span>
        <button onClick={() => void refresh()}>Refresh</button>
      </nav>
      <ul className="listing" data-testid="listing">
        {entries === null && <li className="hint">Loading files…</li>}
        {entries?.length === 0 && (
          <li className="hint">Empty for now — drop files or a folder</li>
        )}
        {entries?.map((entry) => {
          const rel = (folder ? folder + "/" : "") + entry.name;
          return (
            <li key={entry.name}>
              {entry.dir ? (
                <button
                  className="directory"
                  onClick={() => {
                    setFolder(rel);
                    setEntries(null);
                    setAfter("");
                  }}
                >
                  ▸ {entry.name}
                </button>
              ) : (
                <a
                  href={`${API}/file/${scope}?path=${encodeURIComponent(rel)}`}
                  download
                >
                  {entry.name}
                </a>
              )}
              {entry.size !== null && (
                <span className="size">{bytes(entry.size)}</span>
              )}
            </li>
          );
        })}
      </ul>
      {(after || nextPage) && (
        <nav aria-label="File list pages">
          <button
            disabled={!after}
            onClick={() => {
              setAfter("");
              setEntries(null);
            }}
          >
            First page
          </button>
          <button
            disabled={!nextPage}
            onClick={() => {
              setAfter(nextPage!);
              setEntries(null);
            }}
          >
            Next page
          </button>
        </nav>
      )}
      <footer>
        The bar shows sent and confirmed bytes. Completion follows publication.
      </footer>
    </section>
  );
}

export default function App() {
  const [identity, setIdentity] = useState<Identity | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    whoami()
      .then((value) => {
        if (active) setIdentity(value);
      })
      .catch((e) => {
        if (active) setError(e.message);
      });
    return () => {
      active = false;
    };
  }, []);
  if (!identity)
    return (
      <p className="boot" role="status">
        {error || "Opening your session…"}
      </p>
    );
  return (
    <main>
      <div className="page-head">
        <div>
          <h1>MFUP/3 · my uploads</h1>
          <p className="who">
            Anonymous session{" "}
            <code data-testid="user-id">{identity.user_id}</code>
          </p>
        </div>
        <span className="badge" data-testid="backend">
          {identity.backend}
        </span>
      </div>
      <p className="intro">
        Three spaces for your files. Drop a folder, select files, or resume an
        unfinished upload.
      </p>
      <div className="zones">
        {SCOPES.map((scope) => (
          <ScopeZone key={scope} scope={scope} uid={identity.user_id} />
        ))}
      </div>
      <p className="who bottom">
        Your session is saved in this browser cookie. Another browser has its
        own upload list.
      </p>
    </main>
  );
}
